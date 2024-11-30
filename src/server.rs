use std::{
    collections::{BTreeMap, BinaryHeap, HashMap, HashSet},
    net::SocketAddr,
    time::{Duration, Instant},
};

use jsonlrpc::{ErrorObject, RequestId, ResponseObject};
use jsonlrpc_mio::{ClientId, RpcClient, RpcServer};
use mio::{Events, Poll};
use raftbare::{Action, ClusterConfig, CommitStatus, LogEntries, Node, Role, Term};
use rand::Rng;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    command::Command,
    machine::{ApplyContext, Machine},
    machines::Machines,
    rpc::{
        AddServerParams, AppendEntriesCallParams, AppendEntriesReplyParams, ApplyParams, Caller,
        CreateClusterParams, ErrorKind, InstallSnapshotParams, NotifyCommitParams, ProposeParams,
        ProposeQueryParams, RemoveServerParams, Request, RequestVoteCallParams,
        RequestVoteReplyParams, TakeSnapshotResult,
    },
    storage::FileStorage,
    types::{LogIndex, LogPosition, NodeId, Token},
    ApplyKind,
};

pub const EVENTS_CAPACITY: usize = 1024;

// TODO: move
pub type Commands = BTreeMap<LogIndex, Command>;

// TODO: struct or remove or use timestamp (or pid) instead
pub type ServerInstanceId = Uuid;

// TODO: rename
#[derive(Debug)]
pub struct Pending {
    pub commit_position: LogPosition,
    pub input: serde_json::Value,
    pub caller: Caller,
}

impl PartialEq for Pending {
    fn eq(&self, other: &Self) -> bool {
        self.commit_position == other.commit_position
    }
}

impl Eq for Pending {}

impl PartialOrd for Pending {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Pending {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.commit_position.cmp(&other.commit_position).reverse()
    }
}

#[derive(Debug)]
pub struct Server<M> {
    instance_id: ServerInstanceId,
    poller: Poll,
    events: Events,
    rpc_server: RpcServer<Request>,
    rpc_clients: HashMap<Token, RpcClient>,
    node: Node,
    election_abs_timeout: Instant,
    last_applied_index: LogIndex,
    local_commands: Commands,
    storage: Option<FileStorage>, // TODO: local_storage
    dirty_cluster_config: bool,
    pendings: BinaryHeap<Pending>,
    machines: Machines<M>,
}

impl<M: Machine> Server<M> {
    pub fn start(
        listen_addr: SocketAddr,
        mut storage: Option<FileStorage>,
    ) -> std::io::Result<Self> {
        let mut poller = Poll::new()?;
        let events = Events::with_capacity(EVENTS_CAPACITY);
        let rpc_server = RpcServer::start(
            &mut poller,
            listen_addr,
            Token::SERVER_MIN.into(),
            Token::SERVER_MAX.into(),
        )?;

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
            instance_id: Uuid::new_v4(),
            poller,
            events,
            rpc_server,
            rpc_clients: HashMap::new(),
            node,
            storage,
            local_commands,
            election_abs_timeout: Instant::now() + Duration::from_secs(365 * 24 * 60 * 60), // sentinel value
            last_applied_index,
            dirty_cluster_config: false,
            pendings: BinaryHeap::new(),
            machines,
        };
        this.update_rpc_clients(); // TODO

        Ok(this)
    }

    pub fn addr(&self) -> SocketAddr {
        self.rpc_server.listen_addr()
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
        // I/O event handling.
        let timeout = self
            .election_abs_timeout
            .saturating_duration_since(Instant::now())
            .min(timeout.unwrap_or(Duration::MAX));
        self.poller.poll(&mut self.events, Some(timeout))?;
        if self.election_abs_timeout <= Instant::now() {
            // TODO: stats per role
            self.node.handle_election_timeout();
        }

        let mut responses = Vec::new();
        for event in self.events.iter() {
            if let Some(client) = self.rpc_clients.get_mut(&event.token().into()) {
                let _ = client.handle_event(&mut self.poller, event); // TODO: note doc
                while let Some(response) = client.try_recv() {
                    responses.push(response);
                }
            } else {
                self.rpc_server.handle_event(&mut self.poller, event)?;
            }
        }
        for response in responses {
            self.handle_response(response)?;
        }

        // RPC request handling.
        while let Some((from, request)) = self.rpc_server.try_recv() {
            self.handle_request(from, request)?;
        }

        // Commit handling.
        self.handle_committed_entries()?;

        // Raft action handling.
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

    fn pop_pending(&mut self, commit_index: LogIndex) -> Option<Pending> {
        while let Some(pending) = self.pendings.peek() {
            match self.node.get_commit_status(pending.commit_position.into()) {
                CommitStatus::InProgress => {
                    break;
                }
                CommitStatus::Rejected | CommitStatus::Unknown => {
                    todo!("error response");
                }
                CommitStatus::Committed => {
                    if pending.commit_position.index < commit_index {
                        return None;
                    }

                    let pending = self.pendings.pop().expect("unreachable");
                    return Some(pending);
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
            raftbare::LogEntry::Term(x) => self.handle_committed_term(x),
            raftbare::LogEntry::ClusterConfig(x) => self.handle_committed_cluster_config(x),
            raftbare::LogEntry::Command => self.handle_committed_command(index)?,
        }
        self.maybe_update_cluster_config();

        Ok(())
    }

    fn handle_committed_command(&mut self, index: LogIndex) -> std::io::Result<()> {
        let pending = self.pop_pending(index);
        let caller = pending.as_ref().map(|p| p.caller.clone()); // TODO: remove clone

        let mut command = self.local_commands.get(&index).expect("bug").clone(); // TODO: remove clone
        let mut kind = ApplyKind::Command;
        if let Command::Query { input } = &mut command {
            *input = pending.map(|p| p.input);
            kind = ApplyKind::Query;
        }

        let member_change = matches!(command, Command::AddServer { .. });
        let needs_snapshot = matches!(
            command,
            Command::CreateCluster { .. }
                | Command::AddServer { .. }
                | Command::RemoveServer { .. }
        );

        if matches!(command, Command::TakeSnapshot { .. }) {
            self.take_snapshot(index)?;

            if let Some(caller) = caller {
                self.reply_ok(
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
        self.machines.apply(&mut ctx, &command); // TODO: use value passing

        if let Some(caller) = ctx.caller {
            self.reply_output(caller, ctx.output)?;
        }

        if member_change {
            self.dirty_cluster_config = true;

            // TODO: call when node.latest_config() is changed
            self.update_rpc_clients();
        }

        if needs_snapshot {
            // TODO: note comment
            self.take_snapshot(index)?;
        }

        Ok(())
    }

    // TOOD: lazy update
    fn update_rpc_clients(&mut self) {
        // Removed server handling.
        let addrs = self
            .machines
            .system
            .members
            .values()
            .map(|m| m.addr)
            .collect::<HashSet<_>>();
        self.rpc_clients
            .retain(|_, client| addrs.contains(&client.server_addr()));
        // TODO: Remove from node_id_to_token too (or don't remove rpc_clients at all)

        // Added server handling.
        for member in self.machines.system.members.values() {
            if member.addr == self.addr() {
                continue;
            }

            if self.rpc_clients.contains_key(&member.token) {
                continue;
            }

            let rpc_client = RpcClient::new(member.token.into(), member.addr);
            self.rpc_clients.insert(member.token, rpc_client);
        }
    }

    fn reply_output(
        &mut self,
        caller: Caller,
        output: Option<Result<serde_json::Value, ErrorObject>>,
    ) -> std::io::Result<()> {
        let output = output.unwrap_or_else(|| Err(ErrorKind::NoMachineOutput.object()));
        match output {
            Err(e) => {
                self.reply_error(caller, e)?;
            }
            Ok(value) => {
                self.reply_ok(caller, value)?;
            }
        }

        Ok(())
    }

    fn maybe_update_cluster_config(&mut self) {
        if !self.dirty_cluster_config {
            return;
        }

        if !self.is_leader() {
            return;
        }

        if self.node.config().is_joint_consensus() {
            return;
        }

        let mut adding = Vec::new();
        let mut removing = Vec::new();
        for &id in self.machines.system.members.keys() {
            // TODO: if !m.evicting && !self.node.config().voters.contains(id) {
            if !self.node.config().voters.contains(&id.into()) {
                adding.push(id.into());
            }
        }
        for &id in &self.node.config().voters {
            // TODO: if self.members.get(id).map_or(true, |m| m.evicting) {
            if !self.machines.system.members.contains_key(&id.into()) {
                removing.push(id);
            }
        }
        if adding.is_empty() && removing.is_empty() {
            self.dirty_cluster_config = false;
            return;
        }

        let new_config = self.node.config().to_joint_consensus(&adding, &removing);
        self.node.propose_config(new_config); // Always succeeds
    }

    fn handle_committed_term(&mut self, _term: Term) {
        // TOOD: update dirty_cluster_config flag
    }

    fn handle_committed_cluster_config(&mut self, _config: ClusterConfig) {
        // dbg!(self.node.id().get());
        // dbg!(self.is_leader());
        // dbg!(&config);

        // TODO: evict handling
        // if c.is_joint_consensus() {
        //     let mut evicted = Vec::new();
        //     for m in self.members.values() {
        //         if m.evicting && !c.new_voters.contains(&m.node_id) {
        //             evicted.push(m.node_id);
        //         }
        //     }
        //     for id in evicted {
        //         self.handle_evicted(id)?;
        //     }
        // }
    }

    fn handle_action(&mut self, action: Action) -> std::io::Result<()> {
        match action {
            Action::SetElectionTimeout => self.handle_set_election_timeout_action(),
            Action::SaveCurrentTerm => self.handle_save_current_term_action()?,
            Action::SaveVotedFor => self.handle_save_voted_for_action()?,
            Action::AppendLogEntries(entries) => self.handle_append_log_entries_action(entries)?,
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
        self.send_to(dst, &request)?;
        Ok(())
    }

    fn handle_send_message_action(
        &mut self,
        dst: NodeId,
        message: raftbare::Message,
    ) -> std::io::Result<()> {
        let request = Request::from_raftbare(message, &self.local_commands)
            .ok_or(std::io::ErrorKind::Other)?;
        let request = serde_json::to_value(&request)?;
        self.send_to(dst, &request)?;
        Ok(())
    }

    fn handle_broadcast_message_action(
        &mut self,
        message: raftbare::Message,
    ) -> std::io::Result<()> {
        let request = Request::from_raftbare(message, &self.local_commands)
            .ok_or(std::io::ErrorKind::Other)?;
        let request = serde_json::value::to_value(&request)?;
        for client in self.rpc_clients.values_mut() {
            // TODO: Drop if sending queue is full
            if let Err(e) = client.send(&mut self.poller, &request) {
                // Not a critial error.
                todo!("{e:?}");
            }
        }
        Ok(())
    }

    fn handle_append_log_entries_action(&mut self, entries: LogEntries) -> std::io::Result<()> {
        if let Some(storage) = &mut self.storage {
            storage.append_entries(&entries, &self.local_commands)?;
        }
        Ok(())
    }

    fn handle_save_current_term_action(&mut self) -> std::io::Result<()> {
        // TODO: stats
        if let Some(storage) = &mut self.storage {
            storage.save_current_term(self.node.current_term().into())?;
        }
        Ok(())
    }

    fn handle_save_voted_for_action(&mut self) -> std::io::Result<()> {
        // TODO: stats
        if let Some(storage) = &mut self.storage {
            storage.save_voted_for(self.node.voted_for().map(From::from))?;
        }
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
            server_id: self.instance_id,
            node_id: self.node.id().into(),
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
            Request::Propose { params, .. } => self.handle_propose_request(params),
            Request::ProposeQuery { params, .. } => self.handle_propose_query_request(params),
            Request::NotifyCommit { params, .. } => self.handle_notify_commit_request(params),
            Request::InstallSnapshot { params, .. } => self.handle_install_snapshot_request(params),
            Request::AppendEntriesCall { params, .. } => {
                let caller = self.caller(from, jsonlrpc::RequestId::Number(0)); // TODO: remove dummy id
                self.handle_append_entries_request(caller, params)
            }
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
            // TODO: stats
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
            // TODO: stats
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
            // TODO: stats
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
            // TODO: stats
            return Ok(());
        }

        let message = params.into_raftbare();
        self.node.handle_message(message);
        Ok(())
    }

    fn is_initialized(&self) -> bool {
        self.node().is_some()
    }

    fn handle_propose_request(&mut self, params: ProposeParams) -> std::io::Result<()> {
        self.propose_command(params.caller, params.command)?;
        Ok(())
    }

    fn handle_propose_query_request(&mut self, params: ProposeQueryParams) -> std::io::Result<()> {
        if !self.is_leader() {
            todo!("redirect if possible");
        }

        let position = self.propose_command_leader(Command::Query { input: None });
        self.send_to(
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

    fn send_to<T: Serialize>(&mut self, node_id: NodeId, message: &T) -> std::io::Result<()> {
        let Some(token) = self.machines.system.members.get(&node_id).map(|m| m.token) else {
            return Ok(());
        };

        let Some(client) = self.rpc_clients.get_mut(&token) else {
            // TODO: use entry
            return Ok(());
        };

        client.send(&mut self.poller, message)?;
        Ok(())
    }

    fn handle_notify_commit_request(&mut self, params: NotifyCommitParams) -> std::io::Result<()> {
        self.pendings.push(Pending {
            commit_position: params.commit,
            input: params.input,
            caller: params.caller,
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
            self.reply_error(
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
            self.reply_error(caller, ErrorKind::NotClusterMember.object())?;
            return Ok(());
        }
        let command = Command::AddServer { addr: params.addr };
        self.propose_command(caller, command)?;

        Ok(())
    }

    fn handle_apply_request(&mut self, caller: Caller, params: ApplyParams) -> std::io::Result<()> {
        if !self.is_initialized() {
            self.reply_error(caller, ErrorKind::NotClusterMember.object())?;
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
            let command = Command::Query { input: None };
            let promise = self.propose_command_leader(command);
            self.pendings.push(Pending {
                commit_position: promise,
                input,
                caller,
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

        // TODO: optimize
        let Some(addr) = self
            .machines
            .system
            .members
            .iter()
            .find(|x| *x.0 == maybe_leader)
            .map(|x| x.1.addr)
        else {
            todo!();
        };
        let Some(client) = self
            .rpc_clients
            .values_mut()
            .find(|c| c.server_addr() == addr)
        else {
            todo!("error response");
        };
        client.send(&mut self.poller, &request)?;

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

        self.machines.user.apply(&mut ctx, &input);
        let caller = ctx.caller.expect("unreachale");
        self.reply_output(caller, ctx.output)?;

        Ok(())
    }

    fn handle_remove_server_request(
        &mut self,
        caller: Caller,
        params: RemoveServerParams,
    ) -> std::io::Result<()> {
        if !self.is_initialized() {
            self.reply_error(caller, ErrorKind::NotClusterMember.object())?;
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
            self.reply_error(caller, ErrorKind::ClusterAlreadyCreated.object())?;
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

            let request = Request::Propose {
                jsonrpc: jsonlrpc::JsonRpcVersion::V2,
                params: ProposeParams { command, caller },
            };
            self.send_to(maybe_leader, &request)?;
            return Ok(());
        }

        let commit_position = self.propose_command_leader(command);
        if caller.node_id == self.node.id().into() {
            let pending = Pending {
                commit_position,
                input: serde_json::Value::Null, // Always null for non-query commands
                caller,
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
            self.send_to(node_id, &request)?;
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

    fn reply_ok<T: Serialize>(&mut self, caller: Caller, value: T) -> std::io::Result<()> {
        let response = serde_json::json!({
            "jsonrpc": jsonlrpc::JsonRpcVersion::V2,
            "id": caller.request_id,
            "result": value
        });
        self.rpc_server
            .reply(&mut self.poller, caller.client_id, &response)?;
        Ok(())
    }

    fn reply_error(&mut self, caller: Caller, error: ErrorObject) -> std::io::Result<()> {
        let response = ResponseObject::Err {
            jsonrpc: jsonlrpc::JsonRpcVersion::V2,
            error,
            id: Some(caller.request_id),
        };
        self.rpc_server
            .reply(&mut self.poller, caller.client_id, &response)?;
        Ok(())
    }
}
