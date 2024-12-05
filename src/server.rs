use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use jsonlrpc::{RequestId, ResponseObject};
use jsonlrpc_mio::ClientId;
use raftbare::{Action, ClusterConfig, Node};

use crate::{
    broker::MessageBroker,
    command::{Command, Commands},
    machine::{ApplyContext, Machine},
    machines::Machines,
    messages::{
        AppendEntriesCallParams, AppendEntriesReplyParams, ApplyParams, Caller,
        CreateClusterParams, ErrorReason, GetServerStateResult, InstallSnapshotParams,
        NotifyPendingCommitParams, ReplyErrorParams, Request,
    },
    storage::FileStorage,
    types::{LogIndex, LogPosition, NodeId, PendingQueue, Term},
    ApplyKind,
};

const YEAR: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// Raft server.
///
/// Please use the following JSON-RPC requests to interact with this server:
/// - [`Request::CreateCluster`]
/// - [`Request::AddServer`]
/// - [`Request::RemoveServer`]
/// - [`Request::Apply`]
/// - [`Request::TakeSnapshot`]
/// - [`Request::GetServerState`]
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
    /// Starts a Raft server.
    ///
    /// If `storage` holds previous data, the server will load that state.
    pub fn start(
        listen_addr: SocketAddr,
        mut storage: Option<FileStorage>,
    ) -> std::io::Result<Self> {
        let mut broker = MessageBroker::start(listen_addr)?;
        let mut commands = Commands::new();
        let mut machines = Machines::default();
        let mut node = Node::start(NodeId::UNINIT.0);

        if let Some(storage) = &mut storage {
            if let Some(loaded) = storage.load(&mut commands)? {
                node = loaded.0;
                machines = loaded.1;
                broker.update_peers(machines.system.peers(NodeId(node.id())));
            }
        }

        let last_applied = LogIndex(node.log().snapshot_position().index);
        Ok(Self {
            process_id: std::process::id(),
            broker,
            node,
            storage,
            commands,
            election_timeout_deadline: Instant::now() + YEAR,
            last_applied,
            pending_commands: PendingQueue::new(true),
            pending_queries: PendingQueue::new(false),
            machines,
        })
    }

    /// Returns the address of this server.
    pub fn addr(&self) -> SocketAddr {
        self.broker.listen_addr()
    }

    /// Returns a reference to the replicated state machine.
    pub fn machine(&self) -> &M {
        &self.machines.user
    }

    /// Returns an iterator that traverses through the cluster members.
    pub fn members(&self) -> impl '_ + Iterator<Item = SocketAddr> {
        self.machines.system.members.values().map(|m| m.addr)
    }

    /// Returns `true` if this server is part of a cluster.
    ///
    /// Note that this method will continue to return `true` even if the server is removed from the cluster.
    /// To completely clear the server's state, you must stop the process and delete the storage data.
    pub fn is_initialized(&self) -> bool {
        self.node.id() != NodeId::UNINIT.0
    }

    /// Returns a reference to the internal raftbare node.
    pub fn node(&self) -> &Node {
        &self.node
    }

    /// Polls and handles I/O events for this server.
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
        let ResponseObject::Ok { result, .. } = response else {
            panic!("Unexpected RPC response: {response:?}");
        };

        // The sender's `node_id` is not trusted because their knowledge might be outdated.
        let Some(node_id) = self.machines.system.get_node_id_by_addr(addr) else {
            return Ok(());
        };

        let mut params: AppendEntriesReplyParams = serde_json::from_value(result)?;
        params.header.from = node_id;

        // This call will generate an `Action::InstallSnapshot`
        // to inform the latest cluster members to the follower.
        self.node.handle_message(params.into_raftbare());

        Ok(())
    }

    fn handle_committed_entries(&mut self) -> std::io::Result<()> {
        for index in u64::from(self.last_applied) + 1..=self.node.commit_index().get() {
            self.handle_committed_log_entry(index.into())?;
        }

        if self.last_applied.0 < self.node.commit_index() {
            if self.is_leader() {
                // Notify followers of the latest commit index as quickly as possible.
                self.node.heartbeat();
            }

            self.last_applied = LogIndex(self.node.commit_index());

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
                .update_peers(self.machines.system.peers(NodeId(self.node.id())));
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
            if !self.node.config().voters.contains(&id.0) {
                adding.push(id.0);
            }
        }
        for &id in &self.node.config().voters {
            if !self.machines.system.members.contains_key(&NodeId(id)) {
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
                let timeout = if self.machines.system.is_known_node(NodeId(self.node.id())) {
                    self.machines.system.gen_election_timeout(self.node.role())
                } else {
                    // Non-member nodes aren't required to initiate a new election timeout.
                    YEAR
                };
                self.election_timeout_deadline = Instant::now() + timeout;
            }
            Action::SaveCurrentTerm => {
                if let Some(storage) = &mut self.storage {
                    storage.save_current_term(Term(self.node.current_term()))?;
                }
            }
            Action::SaveVotedFor => {
                if let Some(storage) = &mut self.storage {
                    storage.save_voted_for(self.node.voted_for().map(NodeId))?;
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
                self.broker.send_to(NodeId(dst), &request)?;
            }
            Action::InstallSnapshot(dst) => self.send_install_snapshot_request(NodeId(dst))?,
        }
        Ok(())
    }

    fn send_install_snapshot_request(&mut self, dst: NodeId) -> std::io::Result<()> {
        let snapshot = self.get_snapshot(LogIndex(self.node.commit_index()))?;
        let request = Request::install_snapshot(dst, snapshot);
        self.broker.send_to(dst, &request)?;

        // Send a heartbeat to update the follower's `voted_for`.
        // (For simplicity, I use broadcast instead of a P2P message here)
        self.node.heartbeat();

        Ok(())
    }

    fn caller(&self, client_id: ClientId, request_id: RequestId) -> Caller {
        Caller {
            node_id: NodeId(self.node.id()),
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
                let command = Command::add_server(params.addr);
                self.propose_command(self.caller(from, id), command, None)?;
            }
            Request::RemoveServer { id, params, .. } => {
                let command = Command::remove_server(params.addr);
                self.propose_command(self.caller(from, id), command, None)?;
            }
            Request::TakeSnapshot { id, .. } => {
                self.propose_command(self.caller(from, id), Command::TakeSnapshot, None)?
            }
            Request::GetServerState { id, .. } => {
                self.handle_get_server_state_request(self.caller(from, id))?
            }
            Request::Apply { id, params, .. } => {
                self.handle_apply_request(self.caller(from, id), params)?
            }
            Request::ProposeCommand { params, .. } => {
                self.propose_command(params.caller, params.command, params.query_input)?
            }
            Request::NotifyPendingCommit { params, .. } => {
                self.handle_notify_pending_commit_request(params)?
            }
            Request::ReplyError { params, .. } => self.handle_reply_error_request(params)?,
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
            }) = self.node.actions_mut().send_messages.remove(&sender.0)
            else {
                return Ok(());
            };
            let params = AppendEntriesReplyParams::from_raftbare(header, last_position);
            self.broker.reply_ok(caller, params)?;
        }
        Ok(())
    }

    fn handle_install_snapshot_request(
        &mut self,
        params: InstallSnapshotParams,
    ) -> std::io::Result<()> {
        if !self.is_initialized() {
            self.node = Node::start(params.node_id.0);
        }

        if params.last_included.index.0 <= self.node.commit_index() {
            return Ok(());
        }

        let config = ClusterConfig {
            voters: params.voters.iter().copied().map(|n| n.0).collect(),
            new_voters: params.new_voters.iter().copied().map(|n| n.0).collect(),
            ..Default::default()
        };
        self.install_snapshot(params.last_included.into(), config, Some(&params))?;

        self.machines = serde_json::from_value(params.machines)?;
        self.broker
            .update_peers(self.machines.system.peers(NodeId(self.node.id())));

        let timeout = self.machines.system.gen_election_timeout(self.node.role());
        self.election_timeout_deadline = Instant::now() + timeout;

        Ok(())
    }

    fn get_snapshot(&self, LogIndex(index): LogIndex) -> std::io::Result<InstallSnapshotParams> {
        let (position, config) = self.node.log().get_position_and_config(index).expect("bug");
        let snapshot = InstallSnapshotParams {
            node_id: NodeId(self.node.id()),
            last_included: position.into(),
            voters: config.voters.iter().copied().map(NodeId).collect(),
            new_voters: config.new_voters.iter().copied().map(NodeId).collect(),
            machines: serde_json::to_value(&self.machines)?,
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
        self.last_applied = self.last_applied.max(LogIndex(position.index));

        if let Some(snapshot) = snapshot {
            if let Some(storage) = &mut self.storage {
                storage.save_snapshot(snapshot)?;
                storage.save_current_term(Term(self.node.current_term()))?;
                storage.save_voted_for(self.node.voted_for().map(NodeId))?;
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

    fn handle_notify_pending_commit_request(
        &mut self,
        params: NotifyPendingCommitParams,
    ) -> std::io::Result<()> {
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

    fn handle_reply_error_request(&mut self, params: ReplyErrorParams) -> std::io::Result<()> {
        if self.process_id == params.caller.process_id {
            self.broker.reply_error(params.caller, params.error)?;
        }
        Ok(())
    }

    fn handle_get_server_state_request(&mut self, caller: Caller) -> std::io::Result<()> {
        let role = match self.node.role() {
            raftbare::Role::Follower => "FOLLOWER",
            raftbare::Role::Candidate => "CANDIDATE",
            raftbare::Role::Leader => "LEADER",
        };
        let state = GetServerStateResult {
            addr: self.addr(),
            node_id: self.is_initialized().then_some(NodeId(self.node.id())),
            current_term: Term(self.node.current_term()),
            voted_for: self.node.voted_for().map(NodeId),
            role,
            commit_index: LogIndex(self.node.commit_index()),
            snapshot: self.node.log().snapshot_position().into(),
            machines: &self.machines,
        };
        self.broker.reply_ok(caller, &state)?;
        Ok(())
    }

    fn handle_apply_request(&mut self, caller: Caller, params: ApplyParams) -> std::io::Result<()> {
        match params.kind {
            ApplyKind::Command => {
                self.propose_command(caller, Command::apply(params.input), None)?;
            }
            ApplyKind::Query => {
                self.propose_command(caller, Command::Query, Some(params.input))?;
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
        self.node = Node::start(NodeId::SEED.0);
        self.node.create_cluster(&[self.node.id()]); // Always succeeds
        self.propose_command(caller, Command::create_cluster(self.addr(), &params), None)?;

        Ok(())
    }

    fn is_leader(&self) -> bool {
        self.node.role().is_leader()
    }

    fn propose_command(
        &mut self,
        caller: Caller,
        command: Command,
        query_input: Option<serde_json::Value>,
    ) -> std::io::Result<()> {
        if !self.is_initialized()
            || (!matches!(command, Command::CreateCluster { .. })
                && !self.machines.system.is_known_node(caller.node_id))
        {
            return self.reply_error(caller, ErrorReason::NotClusterMember);
        }

        if self.is_leader() {
            self.propose_command_leader(caller, command, query_input)
        } else {
            self.propose_command_non_leader(caller, command, query_input)
        }
    }

    fn propose_command_leader(
        &mut self,
        caller: Caller,
        command: Command,
        query_input: Option<serde_json::Value>,
    ) -> std::io::Result<()> {
        let commit_position = if let Some(position) = matches!(command, Command::Query)
            .then_some(())
            .and_then(|()| self.node.actions().append_log_entries.as_ref())
            .and_then(|entries| (!entries.is_empty()).then(|| entries.last_position()))
        {
            // If there are other commands present,
            // proposing the additional `Command::Query` command is unnecessary.
            position.into()
        } else {
            let commit_position = LogPosition::from(self.node.propose_command()); // Always succeeds
            self.commands.insert(commit_position.index, command);
            commit_position
        };

        if caller.node_id.0 == self.node.id() {
            if let Some(input) = query_input {
                self.pending_queries.push(commit_position, (input, caller));
            } else {
                self.pending_commands.push(commit_position, caller);
            }
        } else {
            // Inform the node that received the RPC request about the commit position.
            //
            // Note that in the absence of leader re-elections, this notification is guaranteed
            // to reach the destination before the log entry is committed.
            let node_id = caller.node_id;
            let request = Request::notify_commit(commit_position, query_input, caller);
            self.broker.send_to(node_id, &request)?;
        }

        Ok(())
    }

    fn propose_command_non_leader(
        &mut self,
        caller: Caller,
        command: Command,
        query_input: Option<serde_json::Value>,
    ) -> std::io::Result<()> {
        let Some(maybe_leader) = self.node.voted_for().take_if(|id| *id != self.node.id()) else {
            return self.reply_error(caller, ErrorReason::NoLeader);
        };

        let request = Request::propose_command(command, query_input, caller);
        self.broker.send_to(NodeId(maybe_leader), &request)?;
        Ok(())
    }

    fn reply_error(&mut self, caller: Caller, error: ErrorReason) -> std::io::Result<()> {
        if self.node.id() == caller.node_id.0 {
            self.broker.reply_error(caller, error)
        } else {
            let node_id = caller.node_id;
            self.broker
                .send_to(node_id, &Request::reply_error(error, caller))
        }
    }
}
