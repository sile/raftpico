use std::{collections::HashMap, net::SocketAddr, time::Duration};

use jsonlrpc::{ErrorCode, ErrorObject, ResponseObject};
use jsonlrpc_mio::{From, RpcServer};
use mio::{Events, Poll, Token};
use raftbare::{CommitPromise, LogIndex, LogPosition, Node, NodeId};
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
    storage: Option<FileStorage>,
    //    ongoing_proposals: HashMap<LogPosition, Caller>,
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
            storage: None, // TODO
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
        self.poller.poll(&mut self.events, timeout)?;
        for event in self.events.iter() {
            self.rpc_server.handle_event(&mut self.poller, event)?;
        }

        while let Some((from, request)) = self.rpc_server.try_recv() {
            self.handle_request(from, request)?;
        }

        Ok(())
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

        let command = Command2::<M>::CreateCluster {
            seed_server_addr: self.listen_addr(),
            settings,
        };
        let promise = self.propose_command(command); // Always succeeds

        Ok(())
    }

    pub fn is_leader(&self) -> bool {
        self.node.role().is_leader()
    }

    fn propose_command(&mut self, command: Command2<M>) -> CommitPromise {
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
