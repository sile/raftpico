use std::{
    collections::BinaryHeap,
    net::SocketAddr,
    time::{Duration, Instant},
};

use jsonlrpc::{ErrorCode, ErrorObject, ResponseObject};
use jsonlrpc_mio::{From, RpcServer};
use mio::{Events, Poll, Token};
use raftbare::{Action, CommitPromise, LogEntries, LogIndex, Node, NodeId, Role};
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};

use crate::{
    command::{Caller, Command2},
    machine::Machine2,
    message::Request,
    request::CreateClusterParams,
    storage::FileStorage,
};

const SERVER_TOKEN_MIN: Token = Token(usize::MAX / 2);
const SERVER_TOKEN_MAX: Token = Token(usize::MAX);

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
}

impl ErrorKind {
    pub const fn code(self) -> ErrorCode {
        ErrorCode::new(self as i32)
    }

    pub const fn message(&self) -> &'static str {
        match self {
            ErrorKind::ClusterAlreadyCreated => "Cluster already created",
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

// TODO: SystemMachine?
#[derive(Debug)]
pub struct ReplicatedState<M> {
    settings: ClusterSettings,
    machine: M,
}

#[derive(Debug)]
pub struct RaftServer<M> {
    poller: Poll,
    events: Events,
    rpc_server: RpcServer<Request>,
    node: Node,
    rng: StdRng,
    storage: Option<FileStorage>,
    election_abs_timeout: Instant,
    last_applied_index: LogIndex,
    ongoing_proposals: BinaryHeap<OngoingProposal>,
    state: ReplicatedState<M>,
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
            node: Node::start(UNINIT_NODE_ID),
            rng: StdRng::from_entropy(),
            storage: None, // TODO
            ongoing_proposals: BinaryHeap::new(),
            election_abs_timeout: Instant::now() + Duration::from_secs(365 * 24 * 60 * 60), // sentinel value
            last_applied_index: LogIndex::ZERO,
            state: ReplicatedState {
                settings: ClusterSettings::default(),
                machine,
            },
        })
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.rpc_server.listen_addr()
    }

    pub fn node(&self) -> Option<&Node> {
        (self.node.id() != UNINIT_NODE_ID).then(|| &self.node)
    }

    pub fn machine(&self) -> &M {
        &self.state.machine
    }

    pub fn machine_mut(&mut self) -> &mut M {
        &mut self.state.machine
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
            self.rpc_server.handle_event(&mut self.poller, event)?;
        }

        // RPC request handling.
        while let Some((from, request)) = self.rpc_server.try_recv() {
            self.handle_request(from, request)?;
        }

        // Commit handling.
        self.handle_commit()?;

        // Raft action handling.
        while let Some(action) = self.node.actions_mut().next() {
            self.handle_action(action)?;
        }

        Ok(())
    }

    fn handle_commit(&mut self) -> std::io::Result<()> {
        for index in
            (self.last_applied_index.get() + 1..=self.node.commit_index().get()).map(LogIndex::new)
        {
            self.apply_committed_command(index)?;
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

    fn apply_committed_command(&mut self, index: LogIndex) -> std::io::Result<()> {
        todo!()
    }

    fn handle_action(&mut self, action: Action) -> std::io::Result<()> {
        match action {
            Action::SetElectionTimeout => self.handle_set_election_timeout_action(),
            Action::SaveCurrentTerm => self.handle_save_current_term_action()?,
            Action::SaveVotedFor => self.handle_save_voted_for_action()?,
            Action::AppendLogEntries(entries) => self.handle_append_log_entries_action(entries)?,
            Action::BroadcastMessage(_) => todo!(),
            Action::SendMessage(_, _) => todo!(),
            Action::InstallSnapshot(_) => todo!(),
        }
        Ok(())
    }

    fn handle_append_log_entries_action(&mut self, entries: LogEntries) -> std::io::Result<()> {
        // TODO: stats
        if let Some(storage) = &mut self.storage {
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

        let min = self.state.settings.min_election_timeout;
        let max = self.state.settings.max_election_timeout;
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
        }
    }

    fn handle_create_cluster_request(
        &mut self,
        caller: Caller,
        settings: ClusterSettings,
    ) -> std::io::Result<()> {
        if self.node().is_some() {
            self.reply_error(caller, ErrorKind::ClusterAlreadyCreated, None)?;
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
            // TODO
            // self.commands.insert(promise.log_position().index, command);
        }
        promise
    }

    fn reply_error(
        &mut self,
        caller: Caller,
        kind: ErrorKind,
        data: Option<serde_json::Value>,
    ) -> std::io::Result<()> {
        let error = ErrorObject {
            code: kind.code(),
            message: kind.message().to_owned(),
            data,
        };
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
