use std::{
    collections::{BTreeMap, BinaryHeap, HashMap, HashSet},
    net::SocketAddr,
    time::{Duration, Instant},
};

use jsonlrpc::{ErrorCode, ErrorObject, ResponseObject};
use jsonlrpc_mio::{From, RpcClient, RpcServer};
use mio::{Events, Poll, Token};
use raftbare::{
    Action, ClusterConfig, CommitPromise, LogEntries, LogIndex, Node, NodeId, Role, Term,
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::{
    command::{Caller, Command2},
    machine::{Context2, Machine2},
    message::{AddServerParams, CreateClusterOutput, Request},
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

// TODO: rename jsonlrpc_mio::From
impl std::convert::From<ClusterSettings> for CreateClusterParams {
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
        ErrorObject {
            code: self.code(),
            message: self.message().to_owned(),
            data: Some(serde_json::json!({"reason": reason.to_string()})),
        }
    }
}

#[derive(Debug)]
pub struct OngoingProposal {
    promise: CommitPromise,
    caller: Caller,
}

impl PartialEq for OngoingProposal {
    fn eq(&self, other: &Self) -> bool {
        self.promise.log_position() == other.promise.log_position()
    }
}

impl Eq for OngoingProposal {}

impl PartialOrd for OngoingProposal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(other.cmp(self))
    }
}

impl Ord for OngoingProposal {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let p0 = self.promise.log_position();
        let p1 = other.promise.log_position();
        (p1.term, p1.index).cmp(&(p0.term, p0.index))
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
}

impl<M: Machine2> Machine2 for SystemMachine<M> {
    type Input = Command2;

    fn apply(&mut self, ctx: &mut Context2, input: &Self::Input) {
        match input {
            Command2::CreateCluster {
                seed_server_addr,
                settings,
            } => self.apply_create_cluster_command(ctx, *seed_server_addr, settings),
            Command2::AddServer { server_addr } => self.apply_add_server_command(ctx, *server_addr),
            Command2::ApplyCommand { input } => todo!(),
            Command2::ApplyQuery => todo!(),
        }
    }
}

pub type Commands = BTreeMap<LogIndex, Command2>;

#[derive(Debug)]
pub struct RaftServer<M> {
    poller: Poll,
    events: Events,
    rpc_server: RpcServer<Request>,
    rpc_clients: HashMap<Token, RpcClient>,
    node: Node,
    rng: StdRng,
    storage: Option<FileStorage>,
    election_abs_timeout: Instant,
    last_applied_index: LogIndex,
    local_commands: Commands,
    ongoing_proposals: BinaryHeap<OngoingProposal>,
    dirty_cluster_config: bool,
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
            poller,
            events,
            rpc_server,
            rpc_clients: HashMap::new(),
            node: Node::start(UNINIT_NODE_ID),
            rng: StdRng::from_entropy(),
            storage: None, // TODO
            local_commands: Commands::new(),
            ongoing_proposals: BinaryHeap::new(),
            election_abs_timeout: Instant::now() + Duration::from_secs(365 * 24 * 60 * 60), // sentinel value
            last_applied_index: LogIndex::ZERO,
            dirty_cluster_config: false,
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
        if self.election_abs_timeout >= Instant::now() {
            // TODO: stats per role
            self.node.handle_election_timeout();
        }

        for event in self.events.iter() {
            if let Some(client) = self.rpc_clients.get_mut(&event.token()) {
                client.handle_event(&mut self.poller, event)?;
                while let Some(response) = client.try_recv() {
                    todo!("[{}] {response:?}", self.node.id().get());
                }
            } else {
                self.rpc_server.handle_event(&mut self.poller, event)?;
            }
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

    fn handle_committed_entries(&mut self) -> std::io::Result<()> {
        self.handle_rejected_proposals()?;

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

    fn handle_rejected_proposals(&mut self) -> std::io::Result<()> {
        while let Some(proposal) = self.ongoing_proposals.peek() {
            // TODO: remove `.clone()`
            if !proposal.promise.clone().poll(&self.node).is_rejected() {
                break;
            }
            todo!();
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
        let caller = if self
            .ongoing_proposals
            .peek()
            .map_or(false, |p| p.promise.clone().poll(&self.node).is_accepted())
        {
            self.ongoing_proposals.pop().map(|p| p.caller)
        } else {
            None
        };
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

        // Added server handling.
        for member in self.machine.members.values() {
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
            removing.push(id);
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

    fn handle_committed_cluster_config(&mut self, config: ClusterConfig) {
        dbg!(&config);

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
            Action::BroadcastMessage(message) => self.handle_broadcast_message(message)?,
            Action::SendMessage(_, _) => todo!(),
            Action::InstallSnapshot(_) => todo!(),
        }
        Ok(())
    }

    fn handle_broadcast_message(&mut self, message: raftbare::Message) -> std::io::Result<()> {
        let request = Request::from_raft_message(message, &self.local_commands)
            .ok_or(std::io::ErrorKind::Other)?;
        let request = serde_json::value::to_raw_value(&request)?;
        for client in self.rpc_clients.values_mut() {
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

    fn handle_request(&mut self, from: From, request: Request) -> std::io::Result<()> {
        match request {
            Request::CreateCluster { id, params, .. } => {
                self.handle_create_cluster_request(Caller::new(from, id), params)
            }
            Request::AddServer { id, params, .. } => {
                self.handle_add_server_request(Caller::new(from, id), params)
            }
            Request::AppendEntries {
                jsonrpc,
                id,
                params,
            } => todo!(),
        }
    }

    fn handle_add_server_request(
        &mut self,
        caller: Caller,
        params: AddServerParams,
    ) -> std::io::Result<()> {
        if self.node().is_none() {
            self.reply_error(caller, ErrorKind::NotClusterMember.object())?;
            return Ok(());
        }

        assert!(self.is_leader()); // TODO: remote handling
        let command = Command2::AddServer {
            server_addr: params.server_addr,
        };
        let promise = self.propose_command(command); // Always succeeds
        self.ongoing_proposals
            .push(OngoingProposal { promise, caller });

        Ok(())
    }

    fn handle_create_cluster_request(
        &mut self,
        caller: Caller,
        settings: ClusterSettings,
    ) -> std::io::Result<()> {
        if self.node().is_some() {
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
        };
        let promise = self.propose_command(command); // Always succeeds
        self.ongoing_proposals
            .push(OngoingProposal { promise, caller });

        Ok(())
    }

    pub fn is_leader(&self) -> bool {
        self.node.role().is_leader()
    }

    fn propose_command(&mut self, command: Command2) -> CommitPromise {
        if matches!(command, Command2::ApplyQuery) && self.is_leader() {
            if let Some(entries) = &self.node.actions().append_log_entries {
                if !entries.is_empty() {
                    // TODO: note comment (there are concurrent proposals)
                    return CommitPromise::Pending(entries.last_position());
                }
            }
        }

        let promise = self.node.propose_command();
        if !promise.is_rejected() {
            self.local_commands
                .insert(promise.log_position().index, command);
        }
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
