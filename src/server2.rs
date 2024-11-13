use std::{net::SocketAddr, time::Duration};

use jsonlrpc::{ErrorCode, ErrorObject, RequestId, ResponseObject};
use jsonlrpc_mio::{From, RpcServer};
use mio::{Events, Poll, Token};
use raftbare::{Node, NodeId};
use serde::{Deserialize, Serialize};

use crate::{message::Request, request::CreateClusterParams};

const SERVER_TOKEN_MIN: Token = Token(usize::MAX / 2);
const SERVER_TOKEN_MAX: Token = Token(usize::MAX);

const EVENTS_CAPACITY: usize = 1024;

const UNINIT_NODE_ID: NodeId = NodeId::new(u64::MAX);

#[derive(Debug)]
pub struct RaftServer<M> {
    poller: Poll,
    events: Events,
    rpc_server: RpcServer<Request>,
    cluster_settings: ClusterSettings,
    node: Node,
    machine: M,
}

impl<M> RaftServer<M> {
    pub fn start(listen_addr: SocketAddr, machine: M) -> std::io::Result<Self> {
        let mut poller = Poll::new()?;
        let events = Events::with_capacity(EVENTS_CAPACITY);
        let rpc_server =
            RpcServer::start(&mut poller, listen_addr, SERVER_TOKEN_MIN, SERVER_TOKEN_MAX)?;
        Ok(Self {
            poller,
            events,
            rpc_server,
            cluster_settings: ClusterSettings::default(),
            node: Node::start(UNINIT_NODE_ID),
            machine,
        })
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.rpc_server.listen_addr()
    }

    pub fn node(&self) -> Option<&Node> {
        (self.node.id() != UNINIT_NODE_ID).then(|| &self.node)
    }

    pub fn machine(&self) -> &M {
        &self.machine
    }

    pub fn machine_mut(&mut self) -> &mut M {
        &mut self.machine
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
                self.handle_create_cluster_request(from, id, params)
            }
        }
    }

    fn handle_create_cluster_request(
        &mut self,
        from: From,
        id: RequestId,
        params: ClusterSettings,
    ) -> std::io::Result<()> {
        if self.node().is_some() {
            self.reply_error(from, id, ErrorKind::ClusterAlreadyCreated, None)?;
            return Ok(());
        }

        self.cluster_settings = params;
        todo!()
    }

    fn reply_error(
        &mut self,
        from: From,
        id: RequestId,
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
            id: Some(id),
        };
        self.rpc_server.reply(&mut self.poller, from, &response)?;
        Ok(())
    }
}

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
