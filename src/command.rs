use std::net::SocketAddr;

use jsonlrpc::RequestId;
use jsonlrpc_mio::ClientId;
use raftbare::LogIndex;
use serde::{Deserialize, Serialize};

use crate::{
    rpc::{ClusterSettings, Proposer},
    server::Commands,
};

// TODO: default type
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
}

impl Command {
    pub fn proposer(&self) -> Option<&Proposer> {
        match self {
            Command::CreateCluster { proposer, .. } => Some(proposer),
            Command::AddServer { proposer, .. } => Some(proposer),
            Command::RemoveServer { proposer, .. } => Some(proposer),
            Command::TakeSnapshot { proposer, .. } => Some(proposer),
            Command::Apply { proposer, .. } => Some(proposer),
            Command::Query => None,
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

// TODO: move and rename
#[derive(Debug, Serialize, Deserialize)]
pub enum LogEntry {
    Term(u64),
    ClusterConfig {
        voters: Vec<u64>,
        new_voters: Vec<u64>,
    },
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
    ApplyCommand {
        // TODO: Cow? or Rc
        // TODO: input: Box<RawValue>,
        input: serde_json::Value,
        proposer: Proposer,
    },
    ApplyQuery,
}

impl LogEntry {
    pub fn new(index: LogIndex, entry: &raftbare::LogEntry, commands: &Commands) -> Option<Self> {
        match entry {
            raftbare::LogEntry::Term(term) => Some(Self::Term(term.get())),
            raftbare::LogEntry::ClusterConfig(cluster_config) => Some(Self::ClusterConfig {
                voters: cluster_config.voters.iter().map(|x| x.get()).collect(),
                new_voters: cluster_config.new_voters.iter().map(|x| x.get()).collect(),
            }),
            raftbare::LogEntry::Command => {
                let command = commands.get(&index.into()).cloned()?;
                Some(match command {
                    Command::CreateCluster {
                        seed_server_addr,
                        settings,
                        proposer,
                    } => Self::CreateCluster {
                        seed_server_addr,
                        settings,
                        proposer,
                    },
                    Command::AddServer {
                        server_addr,
                        proposer,
                    } => Self::AddServer {
                        server_addr,
                        proposer,
                    },
                    Command::TakeSnapshot { proposer } => Self::TakeSnapshot { proposer },
                    Command::RemoveServer {
                        server_addr,
                        proposer,
                    } => Self::RemoveServer {
                        server_addr,
                        proposer,
                    },

                    Command::Apply { input, proposer } => Self::ApplyCommand { input, proposer },
                    Command::Query => Self::ApplyQuery,
                })
            }
        }
    }
}
