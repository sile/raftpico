use std::{
    collections::{BTreeMap, BinaryHeap, HashMap, HashSet},
    net::SocketAddr,
    time::{Duration, Instant},
};

use jsonlrpc::{ErrorCode, ErrorObject, ResponseObject};
use jsonlrpc_mio::{ClientId, RpcClient, RpcServer};
use mio::{Events, Poll, Token};
use raftbare::{
    Action, ClusterConfig, CommitPromise, LogEntries, LogIndex, Node, NodeId, Role, Term,
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use uuid::Uuid;

use crate::{
    command::{Caller, Command2},
    machine::{Context2, Machine2},
    message::{
        AddServerParams, AppendEntriesParams, AppendEntriesResultParams, ApplyParams,
        CreateClusterOutput, InitNodeParams, NotifyServerAddrParams, ProposeParams, Proposer,
        RemoveServerParams, Request, RequestVoteParams, RequestVoteResultParams,
    },
    request::CreateClusterParams,
    storage::FileStorage,
    InputKind,
};

const SERVER_TOKEN_MIN: Token = Token(usize::MAX / 2);
const SERVER_TOKEN_MAX: Token = Token(usize::MAX);

const CLIENT_TOKEN_MIN: Token = Token(0);
const CLIENT_TOKEN_MAX: Token = Token(SERVER_TOKEN_MIN.0 - 1);

const EVENTS_CAPACITY: usize = 1024;

const SEED_NODE_ID: NodeId = NodeId::new(0);
const UNINIT_NODE_ID: NodeId = NodeId::new(u64::MAX);

// TODO: move
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "CreateClusterParams", try_from = "CreateClusterParams")]
pub struct ClusterSettings {
    pub min_election_timeout: Duration,
    pub max_election_timeout: Duration,
    pub max_local_log_entries: usize,
}

impl Default for ClusterSettings {
    fn default() -> Self {
        Self {
            min_election_timeout: Duration::from_millis(100),
            max_election_timeout: Duration::from_millis(1000),
            max_local_log_entries: 100000,
        }
    }
}

impl From<ClusterSettings> for CreateClusterParams {
    fn from(value: ClusterSettings) -> Self {
        Self {
            min_election_timeout_ms: value.min_election_timeout.as_millis() as usize,
            max_election_timeout_ms: value.max_election_timeout.as_millis() as usize,
            max_log_entries_hint: value.max_local_log_entries,
        }
    }
}

impl TryFrom<CreateClusterParams> for ClusterSettings {
    type Error = &'static str;

    fn try_from(value: CreateClusterParams) -> Result<Self, Self::Error> {
        if value.min_election_timeout_ms >= value.max_election_timeout_ms {
            return Err("Empty election timeout range");
        }

        Ok(Self {
            min_election_timeout: Duration::from_millis(value.min_election_timeout_ms as u64),
            max_election_timeout: Duration::from_millis(value.max_election_timeout_ms as u64),
            max_local_log_entries: value.max_log_entries_hint,
        })
    }
}

// TODO: move
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ErrorKind {
    ClusterAlreadyCreated = 1,
    NoMachineOutput,
    MalformedMachineOutput,
    ServerAlreadyAdded,
    NotClusterMember,
    UnknownServer,
}

impl ErrorKind {
    pub const fn code(self) -> ErrorCode {
        ErrorCode::new(self as i32)
    }

    pub const fn message(&self) -> &'static str {
        match self {
            ErrorKind::ClusterAlreadyCreated => "Cluster already created",
            ErrorKind::NoMachineOutput => "No machine output",
            ErrorKind::MalformedMachineOutput => "Malformed machin",
            ErrorKind::ServerAlreadyAdded => "Server already added",
            ErrorKind::NotClusterMember => "Not a cluster member",
            ErrorKind::UnknownServer => "Unknown server",
        }
    }

    pub fn object(self) -> ErrorObject {
        ErrorObject {
            code: self.code(),
            message: self.message().to_owned(),
            data: None,
        }
    }

    pub fn object_with_reason<T: std::fmt::Display>(self, reason: T) -> ErrorObject {
        self.object_with_data(serde_json::json!({"reason": reason.to_string()}))
    }

    pub fn object_with_data(self, data: serde_json::Value) -> ErrorObject {
        ErrorObject {
            code: self.code(),
            message: self.message().to_owned(),
            data: Some(data),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Member {
    pub addr: SocketAddr,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SystemMachine<M> {
    settings: ClusterSettings,
    members: BTreeMap<u64, Member>, // TODO: key type
    user_machine: M,
}

impl<M: Machine2> SystemMachine<M> {
    fn new(user_machine: M) -> Self {
        Self {
            settings: ClusterSettings::default(),
            members: BTreeMap::new(),
            user_machine,
        }
    }

    fn apply_create_cluster_command(
        &mut self,
        ctx: &mut Context2,
        seed_server_addr: SocketAddr,
        settings: &ClusterSettings,
    ) {
        self.settings = settings.clone();
        self.members.insert(
            SEED_NODE_ID.get(),
            Member {
                addr: seed_server_addr,
            },
        );
        ctx.output(&CreateClusterOutput {
            members: self.members.values().cloned().collect(),
        });
    }

    fn apply_add_server_command(&mut self, ctx: &mut Context2, server_addr: SocketAddr) {
        if self.members.values().any(|m| m.addr == server_addr) {
            ctx.error(ErrorKind::ServerAlreadyAdded.object());
            return;
        }

        let node_id = NodeId::new(ctx.commit_index.get());
        self.members
            .insert(node_id.get(), Member { addr: server_addr });
        ctx.output(&CreateClusterOutput {
            members: self.members.values().cloned().collect(),
        });
    }

    fn apply_remove_server_command(&mut self, ctx: &mut Context2, server_addr: SocketAddr) {
        let Some((&node_id, _member)) = self.members.iter().find(|(_, m)| m.addr == server_addr)
        else {
            ctx.error(ErrorKind::NotClusterMember.object());
            return;
        };

        let node_id = NodeId::new(node_id);
        self.members.remove(&node_id.get());
        ctx.output(&CreateClusterOutput {
            members: self.members.values().cloned().collect(),
        });
    }
}

impl<M: Machine2> Machine2 for SystemMachine<M> {
    type Input = Command2;

    fn apply(&mut self, ctx: &mut Context2, input: &Self::Input) {
        match input {
            Command2::CreateCluster {
                seed_server_addr,
                settings,
                ..
            } => self.apply_create_cluster_command(ctx, *seed_server_addr, settings),
            Command2::AddServer { server_addr, .. } => {
                self.apply_add_server_command(ctx, *server_addr)
            }
            Command2::RemoveServer { server_addr, .. } => {
                self.apply_remove_server_command(ctx, *server_addr)
            }
            Command2::ApplyCommand { input, .. } => {
                let input = serde_json::from_value(input.clone()).expect("TODO: error response");
                self.user_machine.apply(ctx, &input)
            }
            Command2::ApplyQuery => todo!(),
        }
    }
}

pub type Commands = BTreeMap<LogIndex, Command2>;

// TODO: struct
pub type ServerInstanceId = Uuid;

#[derive(Debug)]
pub struct PendingQuery {
    pub promise: CommitPromise,
    pub input: serde_json::Value,
    pub caller: Caller,
}

impl PartialEq for PendingQuery {
    fn eq(&self, other: &Self) -> bool {
        self.promise.log_position() == other.promise.log_position()
    }
}

impl Eq for PendingQuery {}

impl PartialOrd for PendingQuery {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PendingQuery {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let p0 = self.promise.log_position();
        let p1 = other.promise.log_position();
        (p0.term, p0.index).cmp(&(p1.term, p1.index)).reverse()
    }
}

#[derive(Debug)]
pub struct RaftServer<M> {
    instance_id: ServerInstanceId,
    poller: Poll,
    events: Events,
    rpc_server: RpcServer<Request>,
    rpc_clients: HashMap<Token, RpcClient>,
    node_id_to_token: HashMap<NodeId, Token>,
    node: Node,
    rng: StdRng,
    storage: Option<FileStorage>,
    election_abs_timeout: Instant,
    last_applied_index: LogIndex,
    local_commands: Commands,
    dirty_cluster_config: bool,
    queries: BinaryHeap<PendingQuery>,
    next_token: Token,
    machine: SystemMachine<M>,
}

impl<M: Machine2> RaftServer<M> {
    pub fn start(listen_addr: SocketAddr, machine: M) -> std::io::Result<Self> {
        let mut poller = Poll::new()?;
        let events = Events::with_capacity(EVENTS_CAPACITY);
        let rpc_server =
            RpcServer::start(&mut poller, listen_addr, SERVER_TOKEN_MIN, SERVER_TOKEN_MAX)?;
        Ok(Self {
            instance_id: Uuid::new_v4(),
            poller,
            events,
            rpc_server,
            rpc_clients: HashMap::new(),
            node_id_to_token: HashMap::new(),
            node: Node::start(UNINIT_NODE_ID),
            rng: StdRng::from_entropy(),
            storage: None, // TODO
            local_commands: Commands::new(),
            election_abs_timeout: Instant::now() + Duration::from_secs(365 * 24 * 60 * 60), // sentinel value
            last_applied_index: LogIndex::ZERO,
            dirty_cluster_config: false,
            queries: BinaryHeap::new(),
            next_token: CLIENT_TOKEN_MIN,
            machine: SystemMachine::new(machine),
        })
    }

    // TODO: rename (server_addr for naming consistency)
    pub fn listen_addr(&self) -> SocketAddr {
        self.rpc_server.listen_addr()
    }

    pub fn node(&self) -> Option<&Node> {
        (self.node.id() != UNINIT_NODE_ID).then(|| &self.node)
    }

    pub fn machine(&self) -> &M {
        &self.machine.user_machine
    }

    pub fn machine_mut(&mut self) -> &mut M {
        &mut self.machine.user_machine
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
            if let Some(client) = self.rpc_clients.get_mut(&event.token()) {
                client.handle_event(&mut self.poller, event)?;
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

                let Some(client) = self
                    .rpc_clients
                    .values_mut()
                    .find(|c| c.server_addr() == addr)
                else {
                    return Ok(());
                };
                let Some((&node_id, _)) = self.machine.members.iter().find(|(_, m)| m.addr == addr)
                else {
                    return Ok(());
                };
                let request = Request::InitNode {
                    jsonrpc: jsonlrpc::JsonRpcVersion::V2,
                    params: InitNodeParams { node_id },
                };
                let _ = client.send(&mut self.poller, &request);
            }
            ResponseObject::Err { error, .. } if error.code == ErrorKind::UnknownServer.code() => {
                let data = error.data.ok_or(std::io::ErrorKind::Other)?;

                #[derive(Deserialize)]
                struct Data {
                    node_id: u64,
                }
                let Data { node_id } = serde_json::from_value(data)?;
                let Some(token) = self.node_id_to_token.get(&NodeId::new(node_id)) else {
                    return Ok(());
                };

                let node_id = self.node.id().get();
                let addr = self.listen_addr();
                let Some(client) = self.rpc_clients.get_mut(&token) else {
                    return Ok(());
                };

                let request = Request::NotifyServerAddr {
                    jsonrpc: jsonlrpc::JsonRpcVersion::V2,
                    params: NotifyServerAddrParams { node_id, addr },
                };
                let _ = client.send(&mut self.poller, &request);
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
        for index in
            (self.last_applied_index.get() + 1..=self.node.commit_index().get()).map(LogIndex::new)
        {
            self.handle_committed_entry(index)?;
        }

        if self.last_applied_index < self.node.commit_index() {
            if self.is_leader() {
                // TODO: doc  (Quickly notify followers about the latest commit index)
                self.node.heartbeat();
            }

            self.last_applied_index = self.node.commit_index();

            // TODO: snapshot handling
            // if self.commands.len() > self.max_log_entries_hint {
            //     self.install_snapshot();
            // }
        }

        Ok(())
    }

    fn handle_committed_entry(&mut self, index: LogIndex) -> std::io::Result<()> {
        let Some(entry) = self.node.log().entries().get_entry(index) else {
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
        let command = self.local_commands.get(&index).expect("bug");
        let kind = if matches!(command, Command2::ApplyQuery) {
            InputKind::Query
        } else {
            InputKind::Command
        };

        let member_change = matches!(command, Command2::AddServer { .. });

        // TODO: remove clone
        let caller = command
            .proposer()
            .filter(|p| p.server == self.instance_id)
            .map(|p| p.client.clone());
        let mut ctx = Context2 {
            kind,
            node: &self.node,
            commit_index: index,
            output: None,
            caller,
        };
        self.machine.apply(&mut ctx, command);

        if let Some(caller) = ctx.caller {
            self.reply_output(caller, ctx.output)?;
        }

        if member_change {
            self.dirty_cluster_config = true;

            // TODO: call when node.latest_config() is changed
            self.update_rpc_clients();
        }

        Ok(())
    }

    fn update_rpc_clients(&mut self) {
        // Removed server handling.
        let addrs = self
            .machine
            .members
            .values()
            .map(|m| m.addr)
            .collect::<HashSet<_>>();
        self.rpc_clients
            .retain(|_, client| addrs.contains(&client.server_addr()));
        // TODO: Remove from node_id_to_token too (or don't remove rpc_clients at all)

        // Added server handling.
        for (&node_id, member) in &self.machine.members {
            if member.addr == self.listen_addr() {
                continue;
            }

            // TODO: conflict check or leave note comment
            let token = self.next_token;
            if self.next_token == CLIENT_TOKEN_MAX {
                self.next_token = CLIENT_TOKEN_MIN;
            } else {
                self.next_token.0 += 1;
            }

            let rpc_client = RpcClient::new(token, member.addr);
            self.rpc_clients.insert(token, rpc_client);
            self.node_id_to_token.insert(NodeId::new(node_id), token);
        }
    }

    fn reply_output(
        &mut self,
        caller: Caller,
        output: Option<Result<Box<RawValue>, ErrorObject>>,
    ) -> std::io::Result<()> {
        let Some(output) = output else {
            self.reply_error(caller, ErrorKind::NoMachineOutput.object())?;
            return Ok(());
        };

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
        for (id, _m) in &self.machine.members {
            let id = NodeId::new(*id);
            // TODO: if !m.evicting && !self.node.config().voters.contains(id) {
            if !self.node.config().voters.contains(&id) {
                adding.push(id);
            }
        }
        for &id in &self.node.config().voters {
            // TODO: if self.members.get(id).map_or(true, |m| m.evicting) {
            if !self.machine.members.contains_key(&id.get()) {
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
            Action::SendMessage(dst, message) => self.handle_send_message_action(dst, message)?,
            Action::InstallSnapshot(_) => todo!(),
        }
        Ok(())
    }

    fn handle_send_message_action(
        &mut self,
        dst: NodeId,
        message: raftbare::Message,
    ) -> std::io::Result<()> {
        let request = Request::from_raft_message(message, &self.local_commands)
            .ok_or(std::io::ErrorKind::Other)?;
        let request = serde_json::value::to_raw_value(&request)?;
        let Some(token) = self.node_id_to_token.get(&dst) else {
            return Ok(());
        };
        let Some(client) = self.rpc_clients.get_mut(token) else {
            return Ok(());
        };
        client.send(&mut self.poller, &request)?;
        Ok(())
    }

    fn handle_broadcast_message_action(
        &mut self,
        message: raftbare::Message,
    ) -> std::io::Result<()> {
        let request = Request::from_raft_message(message, &self.local_commands)
            .ok_or(std::io::ErrorKind::Other)?;
        let request = serde_json::value::to_raw_value(&request)?;
        for client in self.rpc_clients.values_mut() {
            // TODO: Drop if sending queue is full
            if let Err(e) = client.send(&mut self.poller, &request) {
                // Not a critial error.
                todo!("{e:?}");
            }
        }
        Ok(())
    }

    fn handle_append_log_entries_action(&mut self, _entries: LogEntries) -> std::io::Result<()> {
        // TODO: stats
        if let Some(_storage) = &mut self.storage {
            // storage.append_entries(&entries, &self.commands)?;
            todo!()
        }
        Ok(())
    }

    fn handle_save_current_term_action(&mut self) -> std::io::Result<()> {
        // TODO: stats
        if let Some(storage) = &mut self.storage {
            storage.save_current_term(self.node.current_term())?;
        }
        Ok(())
    }

    fn handle_save_voted_for_action(&mut self) -> std::io::Result<()> {
        // TODO: stats
        if let Some(storage) = &mut self.storage {
            storage.save_voted_for(self.node.voted_for())?;
        }
        Ok(())
    }

    fn handle_set_election_timeout_action(&mut self) {
        // TODO: self.stats.election_timeout_set_count += 1;
        let min = self.machine.settings.min_election_timeout;
        let max = self.machine.settings.max_election_timeout;
        let timeout = match self.node.role() {
            Role::Follower => max,
            Role::Candidate => self.rng.gen_range(min..=max),
            Role::Leader => min,
        };
        self.election_abs_timeout = Instant::now() + timeout;
    }

    fn handle_request(&mut self, from: ClientId, request: Request) -> std::io::Result<()> {
        match request {
            Request::CreateCluster { id, params, .. } => {
                self.handle_create_cluster_request(Caller::new(from, id), params)
            }
            Request::AddServer { id, params, .. } => {
                self.handle_add_server_request(Caller::new(from, id), params)
            }
            Request::RemoveServer { id, params, .. } => {
                self.handle_remove_server_request(Caller::new(from, id), params)
            }
            Request::Apply { id, params, .. } => {
                self.handle_apply_request(Caller::new(from, id), params)
            }
            Request::Propose { params, .. } => self.handle_propose_request(params),
            Request::InitNode { params, .. } => self.handle_init_node_request(params),
            Request::NotifyServerAddr { params, .. } => {
                self.handle_notify_server_addr_request(params)
            }
            Request::AppendEntries { id, params, .. } => {
                self.handle_append_entries_request(Caller::new(from, id), params)
            }
            Request::AppendEntriesResult { id, params, .. } => {
                self.handle_append_entries_result_request(Caller::new(from, id), params)
            }
            Request::RequestVote { id, params, .. } => {
                self.handle_request_vote_request(Caller::new(from, id), params)
            }
            Request::RequestVoteResult { id, params, .. } => {
                self.handle_request_vote_result_request(Caller::new(from, id), params)
            }
        }
    }

    fn handle_append_entries_result_request(
        &mut self,
        caller: Caller,
        params: AppendEntriesResultParams,
    ) -> std::io::Result<()> {
        if !self.is_initialized() {
            // TODO: stats
            return Ok(());
        }

        let message = params.into_raft_message(&caller);
        self.node.handle_message(message);
        Ok(())
    }

    fn handle_request_vote_request(
        &mut self,
        caller: Caller,
        params: RequestVoteParams,
    ) -> std::io::Result<()> {
        if !self.is_initialized() {
            // TODO: stats
            return Ok(());
        }

        let message = params.into_raft_message(&caller);
        self.node.handle_message(message);
        Ok(())
    }

    fn handle_request_vote_result_request(
        &mut self,
        caller: Caller,
        params: RequestVoteResultParams,
    ) -> std::io::Result<()> {
        if !self.is_initialized() {
            // TODO: stats
            return Ok(());
        }

        let message = params.into_raft_message(&caller);
        self.node.handle_message(message);
        Ok(())
    }

    fn handle_init_node_request(&mut self, params: InitNodeParams) -> std::io::Result<()> {
        if self.is_initialized() {
            return Ok(());
        }

        let node_id = NodeId::new(params.node_id);
        self.node = Node::start(node_id);
        Ok(())
    }

    fn handle_notify_server_addr_request(
        &mut self,
        params: NotifyServerAddrParams,
    ) -> std::io::Result<()> {
        let node_id = NodeId::new(params.node_id);
        if self.node_id_to_token.contains_key(&node_id) {
            return Ok(());
        }

        // TODO: self.next_token()
        let token = self.next_token;
        self.next_token.0 += 1;

        let client = RpcClient::new(token, params.addr);
        self.node_id_to_token.insert(node_id, token);
        self.rpc_clients.insert(token, client);

        Ok(())
    }

    fn is_initialized(&self) -> bool {
        self.node().is_some()
    }

    fn handle_propose_request(&mut self, params: ProposeParams) -> std::io::Result<()> {
        self.propose_command(params.command)?;
        Ok(())
    }

    fn handle_append_entries_request(
        &mut self,
        caller: Caller,
        params: AppendEntriesParams,
    ) -> std::io::Result<()> {
        if self.node().is_none() {
            self.reply_error(
                caller,
                ErrorKind::NotClusterMember
                    .object_with_data(serde_json::json!({"addr": self.listen_addr()})),
            )?;
            return Ok(());
        }
        if !self
            .node_id_to_token
            .contains_key(&NodeId::new(params.from))
        {
            self.reply_error(
                caller,
                ErrorKind::UnknownServer
                    .object_with_data(serde_json::json!({"node_id": self.node.id().get()})),
            )?;
            return Ok(());
        }

        let message = params
            .into_raft_message(&caller, &mut self.local_commands)
            .ok_or(std::io::ErrorKind::Other)?;
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
        let command = Command2::AddServer {
            server_addr: params.server_addr,
            proposer: Proposer {
                server: self.instance_id,
                client: caller,
            },
        };
        self.propose_command(command)?;

        Ok(())
    }

    fn handle_apply_request(&mut self, caller: Caller, params: ApplyParams) -> std::io::Result<()> {
        if !self.is_initialized() {
            self.reply_error(caller, ErrorKind::NotClusterMember.object())?;
            return Ok(());
        }
        match params.kind {
            InputKind::Command => {
                let command = Command2::ApplyCommand {
                    input: params.input,
                    proposer: Proposer {
                        server: self.instance_id,
                        client: caller,
                    },
                };
                self.propose_command(command)?;
            }
            InputKind::Query => {
                self.handle_apply_query_request(caller, params.input)?;
            }
            InputKind::LocalQuery => {
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
            let command = Command2::ApplyQuery;
            let promise = self.propose_command_leader(command);
            self.queries.push(PendingQuery {
                promise,
                input,
                caller,
            });
            return Ok(());
        }
        todo!()
    }

    fn apply_local_query(
        &mut self,
        caller: Caller,
        input: serde_json::Value,
    ) -> std::io::Result<()> {
        let input = serde_json::from_value(input).expect("TODO: reply error response");

        let mut ctx = Context2 {
            kind: InputKind::LocalQuery,
            node: &self.node,
            commit_index: self.last_applied_index,
            output: None,
            caller: Some(caller),
        };

        self.machine.user_machine.apply(&mut ctx, &input);
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

        let command = Command2::RemoveServer {
            server_addr: params.server_addr,
            proposer: Proposer {
                server: self.instance_id,
                client: caller,
            },
        };
        self.propose_command(command)?;

        Ok(())
    }

    fn handle_create_cluster_request(
        &mut self,
        caller: Caller,
        settings: ClusterSettings,
    ) -> std::io::Result<()> {
        if self.is_initialized() {
            self.reply_error(caller, ErrorKind::ClusterAlreadyCreated.object())?;
            return Ok(());
        }

        self.node = Node::start(SEED_NODE_ID);
        if let Some(storage) = &mut self.storage {
            storage.save_node_id(self.node.id())?;
        }

        self.node.create_cluster(&[self.node.id()]); // Always succeeds

        let command = Command2::CreateCluster {
            seed_server_addr: self.listen_addr(),
            settings,
            proposer: Proposer {
                server: self.instance_id,
                client: caller,
            },
        };
        self.propose_command(command)?;

        Ok(())
    }

    pub fn is_leader(&self) -> bool {
        self.node.role().is_leader()
    }

    fn propose_command(&mut self, command: Command2) -> std::io::Result<()> {
        assert!(self.is_initialized());

        if !self.is_leader() {
            let Some(maybe_leader) = self.node.voted_for() else {
                todo!("notify this error to the origin node");
            };
            if maybe_leader == self.node.id() {
                todo!("notify this error to the origin node");
            }

            let request = Request::Propose {
                jsonrpc: jsonlrpc::JsonRpcVersion::V2,
                params: ProposeParams { command },
            };

            // TODO: optimize
            let Some(addr) = self
                .machine
                .members
                .iter()
                .find(|(&id, _m)| id == maybe_leader.get())
                .map(|(_, m)| m.addr)
            else {
                todo!();
            };
            let Some(client) = self
                .rpc_clients
                .values_mut()
                .find(|c| c.server_addr() == addr)
            else {
                todo!();
            };
            client.send(&mut self.poller, &request)?;
            return Ok(());
        }

        self.propose_command_leader(command);
        Ok(())
    }

    fn propose_command_leader(&mut self, command: Command2) -> CommitPromise {
        if let Some(promise) = matches!(command, Command2::ApplyQuery)
            .then_some(())
            .and_then(|()| self.node.actions().append_log_entries.as_ref())
            .and_then(|entries| {
                if entries.is_empty() {
                    None
                } else {
                    Some(CommitPromise::Pending(entries.last_position()))
                }
            })
        {
            // TODO: note comment
            return promise;
        }

        let promise = self.node.propose_command(); // Always succeeds
        self.local_commands
            .insert(promise.log_position().index, command);

        promise
    }

    fn reply_ok<T: Serialize>(&mut self, caller: Caller, value: T) -> std::io::Result<()> {
        let response = serde_json::json!({
            "jsonrpc": jsonlrpc::JsonRpcVersion::V2,
            "id": caller.request_id,
            "result": value
        });
        self.rpc_server
            .reply(&mut self.poller, caller.from, &response)?;
        Ok(())
    }

    fn reply_error(&mut self, caller: Caller, error: ErrorObject) -> std::io::Result<()> {
        let response = ResponseObject::Err {
            jsonrpc: jsonlrpc::JsonRpcVersion::V2,
            error,
            id: Some(caller.request_id),
        };
        self.rpc_server
            .reply(&mut self.poller, caller.from, &response)?;
        Ok(())
    }
}
