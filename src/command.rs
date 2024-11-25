use std::net::SocketAddr;

use jsonlrpc::RequestId;
use jsonlrpc_mio::ClientId;
use raftbare::LogIndex;
use serde::{Deserialize, Serialize};

use crate::{
    rpc::{ClusterSettings, Proposer},
    server::Commands,
    types::{NodeId, Term},
};

// TODO: Remove default type value
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Command<INPUT = serde_json::Value> {
    CreateCluster {
        seed_server_addr: SocketAddr,
        settings: ClusterSettings,
        proposer: Proposer,
    },
    AddServer {
        server_addr: SocketAddr,
        proposer: Proposer,
    },
    RemoveServer {
        server_addr: SocketAddr,
        proposer: Proposer,
    },
    TakeSnapshot {
        proposer: Proposer,
    },
    Apply {
        input: INPUT,
        proposer: Proposer,
    },
    Query,

    // TODO: doc
    StartLeaderTerm {
        term: Term,
    },
    UpdateClusterConfig {
        voters: Vec<NodeId>,
        new_voters: Vec<NodeId>,
    },
}

impl Command {
    pub fn proposer(&self) -> Option<&Proposer> {
        match self {
            Command::CreateCluster { proposer, .. } => Some(proposer),
            Command::AddServer { proposer, .. } => Some(proposer),
            Command::RemoveServer { proposer, .. } => Some(proposer),
            Command::TakeSnapshot { proposer, .. } => Some(proposer),
            Command::Apply { proposer, .. } => Some(proposer),
            Command::Query
            | Command::StartLeaderTerm { .. }
            | Command::UpdateClusterConfig { .. } => None,
        }
    }

    // TODO
    pub fn new(index: LogIndex, entry: &raftbare::LogEntry, commands: &Commands) -> Option<Self> {
        match entry {
            raftbare::LogEntry::Term(term) => Some(Self::StartLeaderTerm {
                term: Term::from(*term),
            }),
            raftbare::LogEntry::ClusterConfig(cluster_config) => Some(Self::UpdateClusterConfig {
                voters: cluster_config
                    .voters
                    .iter()
                    .copied()
                    .map(NodeId::from)
                    .collect(),
                new_voters: cluster_config
                    .new_voters
                    .iter()
                    .copied()
                    .map(NodeId::from)
                    .collect(),
            }),
            raftbare::LogEntry::Command => commands.get(&index.into()).cloned(),
        }
    }
}

// TODO: move
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Caller {
    pub from: ClientId,
    pub request_id: RequestId,
}

impl Caller {
    pub fn new(from: ClientId, request_id: RequestId) -> Self {
        Self { from, request_id }
    }
}
