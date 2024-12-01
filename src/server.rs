use std::{
    collections::BinaryHeap,
    net::SocketAddr,
    time::{Duration, Instant},
};

use jsonlrpc::{RequestId, ResponseObject};
use jsonlrpc_mio::ClientId;
use raftbare::{Action, ClusterConfig, CommitStatus, Node, Role};
use rand::Rng;
use serde::Deserialize;

use crate::{
    broker::MessageBroker,
    command::{Command, Commands},
    machine::{ApplyContext, Machine},
    machines::Machines,
    messages::{
        AddServerParams, AppendEntriesCallParams, AppendEntriesReplyParams, ApplyParams, Caller,
        CreateClusterParams, ErrorKind, InstallSnapshotParams, NotifyCommitParams,
        ProposeCommandParams, ProposeQueryParams, RemoveServerParams, Request,
        RequestVoteCallParams, RequestVoteReplyParams, TakeSnapshotResult,
    },
    storage::FileStorage,
    types::{LogIndex, LogPosition, NodeId, QueueItem},
    ApplyKind,
};

// TODO: struct ServerState

/// Raft server.
#[derive(Debug)]
pub struct Server<M> {
    process_id: u32,
    broker: MessageBroker,
    node: Node,
    election_abs_timeout: Instant,
    last_applied_index: LogIndex,
    local_commands: Commands,
    storage: Option<FileStorage>, // TODO: local_storage
    pendings: BinaryHeap<QueueItem<LogPosition, (serde_json::Value, Caller)>>,
    machines: Machines<M>,
}

impl<M: Machine> Server<M> {
    pub fn start(
        listen_addr: SocketAddr,
        mut storage: Option<FileStorage>,
    ) -> std::io::Result<Self> {
        let mut local_commands = Commands::new();
        let mut machines = Machines::default();
        let mut node = Node::start(NodeId::UNINIT.into());
        if let Some(storage) = &mut storage {
            if let Some(loaded) = storage.load(&mut local_commands)? {
                node = loaded.0;
                machines = loaded.1;
            }
        }

        let last_applied_index = node.log().snapshot_position().index.into();
        let mut this = Self {
            process_id: std::process::id(),
            broker: MessageBroker::start(listen_addr)?,
            node,
            storage,
            local_commands,
            election_abs_timeout: Instant::now() + Duration::from_secs(365 * 24 * 60 * 60), // sentinel value
            last_applied_index,
            pendings: BinaryHeap::new(),
            machines,
        };
        this.update_rpc_clients(); // TODO

        Ok(this)
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
            .election_abs_timeout
            .saturating_duration_since(Instant::now())
            .min(timeout.unwrap_or(Duration::MAX));
        if !self.broker.poll(timeout)? && self.election_abs_timeout <= Instant::now() {
            self.node.handle_election_timeout();
        }

        while let Some(response) = self.broker.try_recv_response() {
            self.handle_response(response)?;
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

    fn handle_response(&mut self, response: ResponseObject) -> std::io::Result<()> {
        match response {
            ResponseObject::Ok { result, id, .. } => todo!("{result:?}, {id:?}"),
            ResponseObject::Err { error, .. }
                if error.code == ErrorKind::NotClusterMember.code() =>
            {
                let data = error.data.ok_or(std::io::ErrorKind::Other)?;

                #[derive(Deserialize)]
                struct Data {
                    addr: SocketAddr,
                }
                let Data { addr } = serde_json::from_value(data)?;

                let Some((&node_id, _)) = self
                    .machines
                    .system
                    .members
                    .iter()
                    .find(|(_, m)| m.addr == addr)
                else {
                    return Ok(());
                };

                // TODO: note doc
                self.handle_install_snapshot_action(node_id)?;
            }
            e @ ResponseObject::Err { .. } => {
                // TODO
                dbg!(e);
                todo!("unexpected error response");
            }
        }
        Ok(())
    }

    fn handle_committed_entries(&mut self) -> std::io::Result<()> {
        for index in (u64::from(self.last_applied_index) + 1..=self.node.commit_index().get())
            .map(LogIndex::from)
        {
            self.handle_committed_entry(index)?;
        }

        if self.last_applied_index < self.node.commit_index().into() {
            if self.is_leader() {
                // TODO: doc  (Quickly notify followers about the latest commit index)
                self.node.heartbeat();
            }

            self.last_applied_index = self.node.commit_index().into();
        }

        Ok(())
    }

    fn pop_pending(&mut self, commit_index: LogIndex) -> Option<(serde_json::Value, Caller)> {
        while let Some(pending) = self.pendings.peek() {
            match self.node.get_commit_status(pending.key.into()) {
                CommitStatus::InProgress => {
                    break;
                }
                CommitStatus::Rejected | CommitStatus::Unknown => {
                    todo!("error response");
                }
                CommitStatus::Committed => {
                    if pending.key.index < commit_index {
                        return None;
                    }

                    let pending = self.pendings.pop().expect("unreachable");
                    return Some(pending.item);
                }
            }
        }
        None
    }

    fn handle_committed_entry(&mut self, index: LogIndex) -> std::io::Result<()> {
        let Some(entry) = self.node.log().entries().get_entry(index.into()) else {
            unreachable!("Bug: {index:?}");
        };
        match entry {
            raftbare::LogEntry::Term(_) => {}
            raftbare::LogEntry::ClusterConfig(_) => {
                self.maybe_update_cluster_config();
            }
            raftbare::LogEntry::Command => self.handle_committed_command(index)?,
        }

        Ok(())
    }

    fn handle_committed_command(&mut self, index: LogIndex) -> std::io::Result<()> {
        // TODO: while loop for merged commands / queries
        let pending = self.pop_pending(index);
        let caller = pending.as_ref().map(|p| p.1.clone()); // TODO: remove clone

        let mut command = self.local_commands.get(&index).expect("bug").clone(); // TODO: remove clone
        let mut kind = ApplyKind::Command;
        if matches!(command, Command::Query) {
            let Some(input) = pending.map(|p| p.0) else {
                return Ok(());
            };
            kind = ApplyKind::Query;
            command = Command::Apply { input };
        }

        // TODO: rename
        let needs_snapshot = matches!(
            command,
            Command::CreateCluster { .. }
                | Command::AddServer { .. }
                | Command::RemoveServer { .. }
        );

        if matches!(command, Command::TakeSnapshot { .. }) {
            self.take_snapshot(index)?;

            if let Some(caller) = caller {
                self.broker.reply_ok(
                    caller,
                    &TakeSnapshotResult {
                        snapshot_index: index,
                    },
                )?;
            }

            return Ok(());
        }

        let mut ctx = ApplyContext {
            kind,
            node: &self.node,
            commit_index: index,
            output: None,
            caller,
        };
        self.machines.apply(&mut ctx, command);

        if let Some(caller) = ctx.caller {
            self.broker.reply_output(caller, ctx.output)?;
        }

        if needs_snapshot {
            // TODO: note comment
            self.take_snapshot(index)?;
            self.maybe_update_cluster_config();
            self.update_rpc_clients();
        }

        Ok(())
    }

    fn update_rpc_clients(&mut self) {
        self.broker.update_peers(
            self.machines
                .system
                .members
                .iter()
                .filter(|(&id, _)| id != self.node.id().into())
                .map(|(&id, m)| (id, m.token, m.addr)),
        );
    }

    fn maybe_update_cluster_config(&mut self) {
        if !self.is_leader() {
            return;
        }

        if self.node.config().is_joint_consensus() {
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
            Action::SetElectionTimeout => self.handle_set_election_timeout_action(),
            Action::SaveCurrentTerm => {
                if let Some(storage) = &mut self.storage {
                    storage.save_current_term(self.node.current_term().into())?;
                }
            }
            Action::SaveVotedFor => {
                if let Some(storage) = &mut self.storage {
                    storage.save_voted_for(self.node.voted_for().map(From::from))?;
                }
            }
            Action::AppendLogEntries(entries) => {
                if let Some(storage) = &mut self.storage {
                    storage.append_entries(&entries, &self.local_commands)?;
                }
            }
            Action::BroadcastMessage(message) => self.handle_broadcast_message_action(message)?,
            Action::SendMessage(dst, message) => {
                self.handle_send_message_action(dst.into(), message)?
            }
            Action::InstallSnapshot(dst) => self.handle_install_snapshot_action(dst.into())?,
        }
        Ok(())
    }

    fn handle_install_snapshot_action(&mut self, dst: NodeId) -> std::io::Result<()> {
        let mut snapshot = self.snapshot(self.node.commit_index().into())?;
        snapshot.node_id = dst;
        let request = Request::InstallSnapshot {
            jsonrpc: jsonlrpc::JsonRpcVersion::V2,
            params: snapshot,
        };
        self.broker.send_to(dst, &request)?;
        Ok(())
    }

    fn handle_send_message_action(
        &mut self,
        dst: NodeId,
        message: raftbare::Message,
    ) -> std::io::Result<()> {
        let request = Request::from_raftbare(message, &self.local_commands)
            .ok_or(std::io::ErrorKind::Other)?;
        self.broker.send_to(dst, &request)?;
        Ok(())
    }

    fn handle_broadcast_message_action(
        &mut self,
        message: raftbare::Message,
    ) -> std::io::Result<()> {
        let request = Request::from_raftbare(message, &self.local_commands)
            .ok_or(std::io::ErrorKind::Other)?;
        self.broker.broadcast(&request)?;
        Ok(())
    }

    fn handle_set_election_timeout_action(&mut self) {
        let min = self.machines.system.min_election_timeout;
        let max = self.machines.system.max_election_timeout.max(min);
        let timeout = match self.node.role() {
            Role::Follower => max,
            Role::Candidate => rand::thread_rng().gen_range(min..=max),
            Role::Leader => min,
        };
        self.election_abs_timeout = Instant::now() + timeout;
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
                self.handle_create_cluster_request(self.caller(from, id), params)
            }
            Request::AddServer { id, params, .. } => {
                self.handle_add_server_request(self.caller(from, id), params)
            }
            Request::RemoveServer { id, params, .. } => {
                self.handle_remove_server_request(self.caller(from, id), params)
            }
            Request::TakeSnapshot { id, .. } => {
                self.handle_take_snapshot_request(self.caller(from, id))
            }
            Request::Apply { id, params, .. } => {
                self.handle_apply_request(self.caller(from, id), params)
            }
            Request::ProposeCommand { params, .. } => {
                self.propose_command(params.caller, params.command)
            }
            Request::ProposeQuery { params, .. } => self.handle_propose_query_request(params),
            Request::NotifyCommit { params, .. } => self.handle_notify_commit_request(params),
            Request::InstallSnapshot { params, .. } => self.handle_install_snapshot_request(params),
            Request::AppendEntriesCall { params, .. } => self
                .handle_append_entries_request(self.caller(from, Caller::DUMMY_REQUEST_ID), params),
            Request::AppendEntriesReply { params, .. } => {
                self.handle_append_entries_result_request(params)
            }
            Request::RequestVoteCall { params, .. } => self.handle_request_vote_request(params),
            Request::RequestVoteReply { params, .. } => {
                self.handle_request_vote_result_request(params)
            }
        }
    }

    fn handle_install_snapshot_request(
        &mut self,
        params: InstallSnapshotParams,
    ) -> std::io::Result<()> {
        if !self.is_initialized() {
            self.node = Node::start(params.node_id.into());
        }
        if self.node.id() != params.node_id.into() {
            return Ok(());
        }

        if params.last_included_position.index <= self.node.commit_index().into() {
            return Ok(());
        }

        self.machines = serde_json::from_value(params.machine.clone())?; // TODO: remove clone

        let last_included = params.last_included_position.into();
        let config = ClusterConfig {
            voters: params.voters.iter().copied().map(From::from).collect(),
            new_voters: params.new_voters.iter().copied().map(From::from).collect(),
            ..Default::default()
        };

        let ok = self.node.handle_snapshot_installed(last_included, config);
        assert!(ok); // TODO: error handling
        self.last_applied_index = last_included.index.into();

        if let Some(storage) = &mut self.storage {
            storage.save_snapshot(params)?;
            storage.save_current_term(self.node.current_term().into())?;
            storage.save_voted_for(self.node.voted_for().map(NodeId::from))?;
            storage.append_entries(self.node.log().entries(), &self.local_commands)?;
        }

        self.update_rpc_clients();

        Ok(())
    }

    // TODO
    fn snapshot(&self, index: LogIndex) -> std::io::Result<InstallSnapshotParams> {
        let (last_included, config) = self
            .node
            .log()
            .get_position_and_config(index.into())
            .expect("unreachable");
        let snapshot = InstallSnapshotParams {
            node_id: self.node.id().into(),
            last_included_position: last_included.into(),
            voters: config.voters.iter().copied().map(|n| n.into()).collect(),
            new_voters: config
                .new_voters
                .iter()
                .copied()
                .map(|n| n.into())
                .collect(),

            // TODO: impl Clone for Machine ?
            machine: serde_json::to_value(&self.machines)?,
        };
        Ok(snapshot)
    }

    fn take_snapshot(&mut self, index: LogIndex) -> std::io::Result<()> {
        let (position, config) = self
            .node
            .log()
            .get_position_and_config(index.into())
            .expect("unreachable");
        let success = self
            .node
            .handle_snapshot_installed(position, config.clone());
        assert!(success); // TODO:
        self.local_commands = self
            .local_commands
            .split_off(&(u64::from(index) + 1).into());

        // TODO: factor out
        if self.storage.is_some() {
            let snapshot = self.snapshot(index)?;
            if let Some(storage) = &mut self.storage {
                storage.save_snapshot(snapshot)?;
                storage.save_current_term(self.node.current_term().into())?;
                storage.save_voted_for(self.node.voted_for().map(NodeId::from))?;
                storage.append_entries(self.node.log().entries(), &self.local_commands)?;
            }
        }

        Ok(())
    }

    fn handle_take_snapshot_request(&mut self, caller: Caller) -> std::io::Result<()> {
        self.propose_command(caller, Command::TakeSnapshot)?;
        Ok(())
    }

    fn handle_append_entries_result_request(
        &mut self,
        params: AppendEntriesReplyParams,
    ) -> std::io::Result<()> {
        if !self.is_initialized() {
            return Ok(());
        }

        let message = params.into_raftbare();
        self.node.handle_message(message);
        Ok(())
    }

    fn handle_request_vote_request(
        &mut self,
        params: RequestVoteCallParams,
    ) -> std::io::Result<()> {
        if !self.is_initialized() {
            return Ok(());
        }

        let message = params.into_raftbare();
        self.node.handle_message(message);
        Ok(())
    }

    fn handle_request_vote_result_request(
        &mut self,
        params: RequestVoteReplyParams,
    ) -> std::io::Result<()> {
        if !self.is_initialized() {
            return Ok(());
        }

        let message = params.into_raftbare();
        self.node.handle_message(message);
        Ok(())
    }

    fn is_initialized(&self) -> bool {
        self.node().is_some()
    }

    fn handle_propose_query_request(&mut self, params: ProposeQueryParams) -> std::io::Result<()> {
        if !self.is_leader() {
            todo!("redirect if possible");
        }

        let position = self.propose_command_leader(Command::Query);
        self.broker.send_to(
            params.caller.node_id,
            &Request::NotifyCommit {
                jsonrpc: jsonlrpc::JsonRpcVersion::V2,
                params: NotifyCommitParams {
                    commit: position,
                    input: params.input,
                    caller: params.caller,
                },
            },
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
            return Ok(());
        }

        self.pendings.push(QueueItem {
            key: params.commit,
            item: (params.input, params.caller),
        });
        Ok(())
    }

    fn handle_append_entries_request(
        &mut self,
        caller: Caller,
        params: AppendEntriesCallParams,
    ) -> std::io::Result<()> {
        if self.node().is_none()
            || !self
                .machines
                .system
                .members
                .contains_key(&params.header.from)
        {
            self.broker.reply_error(
                caller,
                ErrorKind::NotClusterMember
                    .object_with_data(serde_json::json!({"addr": self.addr()})),
            )?;
            return Ok(());
        }

        let message = params.into_raftbare(&mut self.local_commands);
        self.node.handle_message(message);

        Ok(())
    }

    fn handle_add_server_request(
        &mut self,
        caller: Caller,
        params: AddServerParams,
    ) -> std::io::Result<()> {
        if !self.is_initialized() {
            self.broker
                .reply_error(caller, ErrorKind::NotClusterMember.object())?;
            return Ok(());
        }
        let command = Command::AddServer { addr: params.addr };
        self.propose_command(caller, command)?;

        Ok(())
    }

    fn handle_apply_request(&mut self, caller: Caller, params: ApplyParams) -> std::io::Result<()> {
        if !self.is_initialized() {
            self.broker
                .reply_error(caller, ErrorKind::NotClusterMember.object())?;
            return Ok(());
        }
        match params.kind {
            ApplyKind::Command => {
                let command = Command::Apply {
                    input: params.input,
                };
                self.propose_command(caller, command)?;
            }
            ApplyKind::Query => {
                self.handle_apply_query_request(caller, params.input)?;
            }
            ApplyKind::LocalQuery => {
                self.apply_local_query(caller, params.input)?;
            }
        }
        Ok(())
    }

    fn handle_apply_query_request(
        &mut self,
        caller: Caller,
        input: serde_json::Value,
    ) -> std::io::Result<()> {
        if self.is_leader() {
            let commit_position = self.propose_command_leader(Command::Query);
            self.pendings.push(QueueItem {
                key: commit_position,
                item: (input, caller),
            });
            return Ok(());
        }

        let Some(maybe_leader) = self.node.voted_for().map(NodeId::from) else {
            todo!("error response");
        };
        let request = Request::ProposeQuery {
            jsonrpc: jsonlrpc::JsonRpcVersion::V2,
            params: ProposeQueryParams { input, caller },
        };

        self.broker.send_to(maybe_leader, &request)?;

        Ok(())
    }

    fn apply_local_query(
        &mut self,
        caller: Caller,
        input: serde_json::Value,
    ) -> std::io::Result<()> {
        let input = serde_json::from_value(input).expect("TODO: reply error response");

        let mut ctx = ApplyContext {
            kind: ApplyKind::LocalQuery,
            node: &self.node,
            commit_index: self.last_applied_index,
            output: None,
            caller: Some(caller),
        };

        self.machines.user.apply(&mut ctx, input);
        let caller = ctx.caller.expect("unreachale");
        self.broker.reply_output(caller, ctx.output)?;

        Ok(())
    }

    fn handle_remove_server_request(
        &mut self,
        caller: Caller,
        params: RemoveServerParams,
    ) -> std::io::Result<()> {
        if !self.is_initialized() {
            self.broker
                .reply_error(caller, ErrorKind::NotClusterMember.object())?;
            return Ok(());
        }

        let command = Command::RemoveServer { addr: params.addr };
        self.propose_command(caller, command)?;

        Ok(())
    }

    fn handle_create_cluster_request(
        &mut self,
        mut caller: Caller,
        params: CreateClusterParams,
    ) -> std::io::Result<()> {
        if self.is_initialized() {
            self.broker
                .reply_error(caller, ErrorKind::ClusterAlreadyCreated.object())?;
            return Ok(());
        }

        self.node = Node::start(NodeId::SEED.into());
        caller.node_id = NodeId::SEED;
        let snapshot = self.snapshot(LogIndex::from(0))?;
        if let Some(storage) = &mut self.storage {
            storage.save_snapshot(snapshot)?;
        }

        self.node.create_cluster(&[self.node.id()]); // Always succeeds

        let command = Command::CreateCluster {
            seed_addr: self.addr(),
            min_election_timeout: Duration::from_millis(params.min_election_timeout_ms as u64),
            max_election_timeout: Duration::from_millis(params.max_election_timeout_ms as u64),
        };
        self.propose_command(caller, command)?;

        Ok(())
    }

    pub fn is_leader(&self) -> bool {
        self.node.role().is_leader()
    }

    fn propose_command(&mut self, caller: Caller, command: Command) -> std::io::Result<()> {
        assert!(self.is_initialized());

        if !self.is_leader() {
            let Some(maybe_leader) = self.node.voted_for().map(NodeId::from) else {
                todo!("notify this error to the origin node");
            };
            if maybe_leader == self.node.id().into() {
                todo!("notify this error to the origin node");
            }

            let request = Request::ProposeCommand {
                jsonrpc: jsonlrpc::JsonRpcVersion::V2,
                params: ProposeCommandParams { command, caller },
            };
            self.broker.send_to(maybe_leader, &request)?;
            return Ok(());
        }

        let commit_position = self.propose_command_leader(command);
        if caller.node_id == self.node.id().into() {
            let pending = QueueItem {
                key: commit_position,
                item: (serde_json::Value::Null, caller),
            };
            self.pendings.push(pending);
        } else {
            // TODO: add note doc about message reordering
            let node_id = caller.node_id;
            let request = Request::NotifyCommit {
                jsonrpc: jsonlrpc::JsonRpcVersion::V2,
                params: NotifyCommitParams {
                    commit: commit_position,
                    input: serde_json::Value::Null,
                    caller,
                },
            };
            self.broker.send_to(node_id, &request)?;
        }

        Ok(())
    }

    fn propose_command_leader(&mut self, command: Command) -> LogPosition {
        if let Some(promise) = matches!(command, Command::Query { .. })
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
        self.local_commands.insert(position.index, command);

        position
    }
}
