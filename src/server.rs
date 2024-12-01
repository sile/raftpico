use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use jsonlrpc::{ErrorObject, RequestId, ResponseObject};
use jsonlrpc_mio::ClientId;
use raftbare::{Action, ClusterConfig, Node};

use crate::{
    broker::MessageBroker,
    command::{Command, Commands},
    machine::{ApplyContext, Machine},
    machines::Machines,
    messages::{
        AppendEntriesCallParams, AppendEntriesReplyParams, ApplyParams, Caller,
        CreateClusterParams, ErrorReason, InstallSnapshotParams, NotifyCommitParams,
        ProposeQueryParams, Request,
    },
    storage::FileStorage,
    types::{LogIndex, LogPosition, NodeId, PendingQueue},
    ApplyKind,
};

// TODO: struct ServerState

/// Raft server.
#[derive(Debug)]
pub struct Server<M> {
    process_id: u32,
    broker: MessageBroker,
    node: Node,
    election_timeout_deadline: Instant,
    last_applied: LogIndex,
    commands: Commands,
    storage: Option<FileStorage>,
    pending_commands: PendingQueue<Caller>,
    pending_queries: PendingQueue<(serde_json::Value, Caller)>,
    machines: Machines<M>,
}

impl<M: Machine> Server<M> {
    pub fn start(
        listen_addr: SocketAddr,
        mut storage: Option<FileStorage>,
    ) -> std::io::Result<Self> {
        let mut broker = MessageBroker::start(listen_addr)?;
        let mut commands = Commands::new();
        let mut machines = Machines::default();
        let mut node = Node::start(NodeId::UNINIT.into());

        if let Some(storage) = &mut storage {
            if let Some(loaded) = storage.load(&mut commands)? {
                node = loaded.0;
                machines = loaded.1;
                broker.update_peers(machines.system.peers(node.id().into()));
            }
        }

        let last_applied = node.log().snapshot_position().index.into();
        Ok(Self {
            process_id: std::process::id(),
            broker,
            node,
            storage,
            commands,
            election_timeout_deadline: Instant::now() + Duration::from_secs(365 * 24 * 60 * 60),
            last_applied,
            pending_commands: PendingQueue::new(true),
            pending_queries: PendingQueue::new(false),
            machines,
        })
    }

    pub fn addr(&self) -> SocketAddr {
        self.broker.listen_addr()
    }

    pub fn node(&self) -> Option<&Node> {
        (self.node.id() != NodeId::UNINIT.into()).then_some(&self.node)
    }

    pub fn machine(&self) -> &M {
        &self.machines.user
    }

    pub fn machine_mut(&mut self) -> &mut M {
        &mut self.machines.user
    }

    pub fn poll(&mut self, timeout: Option<Duration>) -> std::io::Result<()> {
        let timeout = self
            .election_timeout_deadline
            .saturating_duration_since(Instant::now())
            .min(timeout.unwrap_or(Duration::MAX));
        if !self.broker.poll(timeout)? && self.election_timeout_deadline <= Instant::now() {
            self.node.handle_election_timeout();
        }

        while let Some((addr, response)) = self.broker.try_recv_response() {
            self.handle_response(addr, response)?;
        }

        while let Some((from, request)) = self.broker.try_recv_request() {
            self.handle_request(from, request)?;
        }

        self.handle_committed_entries()?;

        while let Some(action) = self.node.actions_mut().next() {
            self.handle_action(action)?;
        }

        Ok(())
    }

    fn handle_response(
        &mut self,
        addr: SocketAddr,
        response: ResponseObject,
    ) -> std::io::Result<()> {
        let data = match response {
            ResponseObject::Err {
                error:
                    ErrorObject {
                        code: ErrorReason::ERROR_CODE_UNKNOWN_MEMBER,
                        data: Some(data),
                        ..
                    },
                ..
            } => data,
            response => {
                panic!("Unexpected RPC response: {response:?}");
            }
        };

        let mut params: AppendEntriesReplyParams = serde_json::from_value(data)?;
        if params.header.from == NodeId::UNINIT {
            let Some(node_id) = self.machines.system.get_node_id_by_addr(addr) else {
                return Ok(());
            };
            params.header.from = node_id;
        }

        // This call will generate an `Action::InstallSnapshot`
        // to inform the latest cluster members to the follower.
        self.node.handle_message(params.into_raftbare());

        Ok(())
    }

    fn handle_committed_entries(&mut self) -> std::io::Result<()> {
        for index in u64::from(self.last_applied) + 1..=self.node.commit_index().get() {
            self.handle_committed_log_entry(index.into())?;
        }

        if self.last_applied < self.node.commit_index().into() {
            if self.is_leader() {
                // Notify followers of the latest commit index as quickly as possible.
                self.node.heartbeat();
            }

            self.last_applied = self.node.commit_index().into();

            self.handle_committed_queries()?;
        }

        Ok(())
    }

    fn handle_committed_log_entry(&mut self, index: LogIndex) -> std::io::Result<()> {
        let entry = self.node.log().entries().get_entry(index.0).expect("bug");
        match entry {
            raftbare::LogEntry::Term(_) => {}
            raftbare::LogEntry::ClusterConfig(_) => self.maybe_sync_cluster_config(),
            raftbare::LogEntry::Command => self.handle_committed_command(index)?,
        }
        Ok(())
    }

    fn handle_committed_command(&mut self, index: LogIndex) -> std::io::Result<()> {
        let command = self.commands.get(&index).expect("bug").clone();
        let will_change_member = command.will_change_member();

        let mut caller = None;
        while let Some((popped_caller, error)) =
            self.pending_commands.pop_committed(&self.node, index)
        {
            if let Some(error) = error {
                self.broker.reply_error(popped_caller, error)?;
            } else if caller.is_some() {
                unreachable!("bug");
            } else {
                caller = Some(popped_caller);
            }
        }

        if matches!(command, Command::TakeSnapshot) {
            self.take_snapshot(index)?;
        }

        self.apply(ApplyKind::Command, index, caller, command)?;

        if will_change_member {
            // Ensure that the snapshot consistently includes the latest member information for simplicity.
            self.take_snapshot(index)?;
            self.maybe_sync_cluster_config();
            self.broker
                .update_peers(self.machines.system.peers(self.node.id().into()));
        }

        Ok(())
    }

    fn handle_committed_queries(&mut self) -> std::io::Result<()> {
        while let Some(((input, caller), error)) = self
            .pending_queries
            .pop_committed(&self.node, self.last_applied)
        {
            if let Some(error) = error {
                self.broker.reply_error(caller, error)?;
            } else {
                let command = Command::apply(input);
                self.apply(ApplyKind::Query, self.last_applied, Some(caller), command)?;
            }
        }

        Ok(())
    }

    fn apply(
        &mut self,
        kind: ApplyKind,
        commit_index: LogIndex,
        caller: Option<Caller>,
        command: Command,
    ) -> std::io::Result<()> {
        let mut ctx = ApplyContext {
            kind,
            node: &self.node,
            commit_index,
            output: None,
            caller,
        };
        self.machines.apply(&mut ctx, command);

        if let Some(caller) = ctx.caller {
            self.broker.reply_output(caller, ctx.output)?;
        }

        Ok(())
    }

    fn maybe_sync_cluster_config(&mut self) {
        if !self.is_leader() || self.node.config().is_joint_consensus() {
            return;
        }

        let mut adding = Vec::new();
        let mut removing = Vec::new();
        for &id in self.machines.system.members.keys() {
            if !self.node.config().voters.contains(&id.into()) {
                adding.push(id.into());
            }
        }
        for &id in &self.node.config().voters {
            if !self.machines.system.members.contains_key(&id.into()) {
                removing.push(id);
            }
        }
        if adding.is_empty() && removing.is_empty() {
            return;
        }

        let new_config = self.node.config().to_joint_consensus(&adding, &removing);
        self.node.propose_config(new_config); // Always succeeds
    }

    fn handle_action(&mut self, action: Action) -> std::io::Result<()> {
        match action {
            Action::SetElectionTimeout => {
                let timeout = self.machines.system.gen_election_timeout(self.node.role());
                self.election_timeout_deadline = Instant::now() + timeout;
            }
            Action::SaveCurrentTerm => {
                if let Some(storage) = &mut self.storage {
                    storage.save_current_term(self.node.current_term().into())?;
                }
            }
            Action::SaveVotedFor => {
                if let Some(storage) = &mut self.storage {
                    storage.save_voted_for(self.node.voted_for().map(NodeId::from))?;
                }
            }
            Action::AppendLogEntries(entries) => {
                if let Some(storage) = &mut self.storage {
                    storage.append_entries(&entries, &self.commands)?;
                }
            }
            Action::BroadcastMessage(message) => {
                let request = Request::from_raftbare(message, &self.commands);
                self.broker.broadcast(&request)?;
            }
            Action::SendMessage(dst, message) => {
                let request = Request::from_raftbare(message, &self.commands);
                self.broker.send_to(dst.into(), &request)?;
            }
            Action::InstallSnapshot(dst) => self.send_install_snapshot_request(dst.into())?,
        }
        Ok(())
    }

    fn send_install_snapshot_request(&mut self, dst: NodeId) -> std::io::Result<()> {
        let snapshot = self.get_snapshot(self.node.commit_index().into())?;
        let request = Request::install_snapshot(dst, snapshot);
        self.broker.send_to(dst, &request)?;

        // Send a heartbeat to update the follower's `voted_for`.
        // (For simplicity, I use broadcast instead of a P2P message here)
        self.node.heartbeat();

        Ok(())
    }

    fn caller(&self, client_id: ClientId, request_id: RequestId) -> Caller {
        Caller {
            node_id: self.node.id().into(),
            process_id: self.process_id,
            client_id,
            request_id,
        }
    }

    fn handle_request(&mut self, from: ClientId, request: Request) -> std::io::Result<()> {
        match request {
            Request::CreateCluster { id, params, .. } => {
                self.handle_create_cluster_request(self.caller(from, id), params)?
            }
            Request::AddServer { id, params, .. } => {
                self.propose_command(self.caller(from, id), Command::add_server(params.addr))?;
            }
            Request::RemoveServer { id, params, .. } => {
                self.propose_command(self.caller(from, id), Command::remove_server(params.addr))?;
            }
            Request::TakeSnapshot { id, .. } => {
                self.propose_command(self.caller(from, id), Command::TakeSnapshot)?
            }
            Request::Apply { id, params, .. } => {
                self.handle_apply_request(self.caller(from, id), params)?
            }
            Request::ProposeCommand { params, .. } => {
                self.propose_command(params.caller, params.command)?
            }
            Request::ProposeQuery { params, .. } => self.handle_propose_query_request(params)?,
            Request::NotifyCommit { params, .. } => self.handle_notify_commit_request(params)?,
            Request::InstallSnapshot { params, .. } => {
                self.handle_install_snapshot_request(params)?
            }
            Request::AppendEntriesCall { params, .. } => {
                let caller = self.caller(from, Caller::DUMMY_REQUEST_ID);
                self.handle_append_entries_call_request(caller, params)?;
            }
            Request::AppendEntriesReply { params, .. } => {
                self.node.handle_message(params.into_raftbare());
            }
            Request::RequestVoteCall { params, .. } => {
                self.node.handle_message(params.into_raftbare());
            }
            Request::RequestVoteReply { params, .. } => {
                self.node.handle_message(params.into_raftbare());
            }
        }
        Ok(())
    }

    fn handle_append_entries_call_request(
        &mut self,
        caller: Caller,
        params: AppendEntriesCallParams,
    ) -> std::io::Result<()> {
        let sender = params.header.from;
        self.node
            .handle_message(params.into_raftbare(&mut self.commands));
        if !self.machines.system.is_known_node(sender) {
            // The reply message cannot be sent through the usual path.
            // Therefore, send the reply as the RPC response.
            // The leader will send a snapshot to synchronize the follower's state,
            // including the latest cluster members.
            let Some(raftbare::Message::AppendEntriesReply {
                header,
                last_position,
            }) = self.node.actions_mut().send_messages.remove(&sender.into())
            else {
                return Ok(());
            };
            let reply = AppendEntriesReplyParams::from_raftbare(header, last_position);
            self.broker
                .reply_error(caller, ErrorReason::UnknownMember { reply })?;
        }
        Ok(())
    }

    fn handle_install_snapshot_request(
        &mut self,
        params: InstallSnapshotParams,
    ) -> std::io::Result<()> {
        if !self.is_initialized() {
            self.node = Node::start(params.node_id.into());
        }

        if params.last_included.index <= self.node.commit_index().into() {
            return Ok(());
        }

        let config = ClusterConfig {
            voters: params.voters.iter().copied().map(From::from).collect(),
            new_voters: params.new_voters.iter().copied().map(From::from).collect(),
            ..Default::default()
        };
        self.install_snapshot(params.last_included.into(), config, Some(&params))?;

        self.machines = serde_json::from_value(params.machine)?;
        self.broker
            .update_peers(self.machines.system.peers(self.node.id().into()));

        let timeout = self.machines.system.gen_election_timeout(self.node.role());
        self.election_timeout_deadline = Instant::now() + timeout;

        Ok(())
    }

    fn get_snapshot(&self, LogIndex(index): LogIndex) -> std::io::Result<InstallSnapshotParams> {
        let (position, config) = self.node.log().get_position_and_config(index).expect("bug");
        let snapshot = InstallSnapshotParams {
            node_id: self.node.id().into(),
            last_included: position.into(),
            voters: config.voters.iter().map(|n| NodeId::from(*n)).collect(),
            new_voters: config.new_voters.iter().map(|n| NodeId::from(*n)).collect(),
            machine: serde_json::to_value(&self.machines)?,
        };
        Ok(snapshot)
    }

    fn install_snapshot(
        &mut self,
        position: raftbare::LogPosition,
        config: raftbare::ClusterConfig,
        snapshot: Option<&InstallSnapshotParams>,
    ) -> std::io::Result<()> {
        let success = self
            .node
            .handle_snapshot_installed(position, config.clone());
        assert!(success);
        self.commands = self.commands.split_off(&(position.index.get() + 1).into());
        self.last_applied = self.last_applied.max(position.index.into());

        if let Some(snapshot) = snapshot {
            if let Some(storage) = &mut self.storage {
                storage.save_snapshot(snapshot)?;
                storage.save_current_term(self.node.current_term().into())?;
                storage.save_voted_for(self.node.voted_for().map(NodeId::from))?;
                storage.append_entries(self.node.log().entries(), &self.commands)?;
            }
        }

        Ok(())
    }

    fn take_snapshot(&mut self, LogIndex(index): LogIndex) -> std::io::Result<()> {
        let snapshot = if self.storage.is_some() {
            Some(self.get_snapshot(LogIndex(index))?)
        } else {
            None
        };
        let (position, config) = self.node.log().get_position_and_config(index).expect("bug");
        self.install_snapshot(position, config.clone(), snapshot.as_ref())?;
        Ok(())
    }

    fn is_initialized(&self) -> bool {
        self.node().is_some()
    }

    fn handle_propose_query_request(&mut self, params: ProposeQueryParams) -> std::io::Result<()> {
        if !self.is_leader() {
            todo!("redirect if possible");
        }

        // TODO: factor out
        let position = self.propose_command_leader(Command::Query);
        self.broker.send_to(
            params.caller.node_id,
            &Request::notify_commit(position, Some(params.input), params.caller),
        )?;
        Ok(())
    }

    fn handle_notify_commit_request(&mut self, params: NotifyCommitParams) -> std::io::Result<()> {
        if self.process_id != params.caller.process_id {
            // This notification is irrelevant as the server has been restarted.
            //
            // [NOTE]
            // Theoretically, the restarted server could have the same process ID as before.
            // However, in practice, I believe the likelihood of this occurring is extremely low.
        } else if let Some(input) = params.input {
            self.pending_queries
                .push(params.commit, (input, params.caller));
        } else {
            self.pending_commands.push(params.commit, params.caller);
        }
        Ok(())
    }

    fn handle_apply_request(&mut self, caller: Caller, params: ApplyParams) -> std::io::Result<()> {
        match params.kind {
            ApplyKind::Command => {
                self.propose_command(caller, Command::apply(params.input))?;
            }
            ApplyKind::Query => {
                self.handle_apply_query_request(caller, params.input)?;
            }
            ApplyKind::LocalQuery => {
                let kind = ApplyKind::LocalQuery;
                let command = Command::apply(params.input);
                self.apply(kind, self.last_applied, Some(caller), command)?;
            }
        }
        Ok(())
    }

    fn handle_create_cluster_request(
        &mut self,
        mut caller: Caller,
        params: CreateClusterParams,
    ) -> std::io::Result<()> {
        if self.is_initialized() {
            self.broker
                .reply_error(caller, ErrorReason::ClusterAlreadyCreated)?;
            return Ok(());
        }

        caller.node_id = NodeId::SEED;
        self.node = Node::start(NodeId::SEED.into());
        self.node.create_cluster(&[self.node.id()]); // Always succeeds
        self.propose_command(caller, Command::create_cluster(self.addr(), &params))?;

        Ok(())
    }

    pub fn is_leader(&self) -> bool {
        self.node.role().is_leader()
    }

    fn propose_command(&mut self, caller: Caller, command: Command) -> std::io::Result<()> {
        if !self.is_initialized() {
            if caller.node_id == self.node.id().into() {
                self.broker
                    .reply_error(caller, ErrorReason::NotClusterMember)?;
            } else {
                todo!("notify 'leader not found' error to the origin node");
            }
            return Ok(());
        }

        if !self.is_leader() {
            let Some(maybe_leader) = self.node.voted_for().map(NodeId::from) else {
                todo!("notify this error to the origin node");
            };
            if maybe_leader == self.node.id().into() {
                todo!("notify this error to the origin node");
            }

            let request = Request::propose_command(command, caller);
            self.broker.send_to(maybe_leader, &request)?;
            return Ok(());
        }

        let commit_position = self.propose_command_leader(command);
        if caller.node_id == self.node.id().into() {
            self.pending_commands.push(commit_position, caller);
        } else {
            // TODO: add note doc about message reordering
            let node_id = caller.node_id;
            let request = Request::notify_commit(commit_position, None, caller);
            self.broker.send_to(node_id, &request)?;
        }

        Ok(())
    }

    fn propose_command_leader(&mut self, command: Command) -> LogPosition {
        if let Some(promise) = matches!(command, Command::Query)
            .then_some(())
            .and_then(|()| self.node.actions().append_log_entries.as_ref())
            .and_then(|entries| {
                if entries.is_empty() {
                    None
                } else {
                    Some(entries.last_position())
                }
            })
        {
            // TODO: note comment
            return promise.into();
        }

        let position = LogPosition::from(self.node.propose_command()); // Always succeeds
        self.commands.insert(position.index, command);

        position
    }

    // TODO: remove
    fn handle_apply_query_request(
        &mut self,
        caller: Caller,
        input: serde_json::Value,
    ) -> std::io::Result<()> {
        // TODO: use propose_command()?
        if self.is_leader() {
            let commit_position = self.propose_command_leader(Command::Query);
            self.pending_queries.push(commit_position, (input, caller));
            return Ok(());
        }

        let Some(maybe_leader) = self.node.voted_for().map(NodeId::from) else {
            todo!("error response");
        };
        let request = Request::propose_query(input, caller);
        self.broker.send_to(maybe_leader, &request)?;

        Ok(())
    }
}
