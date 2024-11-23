use std::net::SocketAddr;

use jsonlrpc::RequestId;
use jsonlrpc_mio::ClientId;
use raftbare::LogIndex;
use serde::{Deserialize, Serialize};

use crate::{
    message::Proposer,
    server::{ClusterSettings, Commands},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command2 {
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
        // [NOTE] Cannot use RawValue here: https://github.com/serde-rs/json/issues/545
        // TODO: consider workaround
        // input: Box<RawValue>,
        input: serde_json::Value,
        proposer: Option<Proposer>, // TODO: command_id: Uuid (?)
    },
    ApplyQuery,
}

impl Command2 {
    pub fn proposer(&self) -> Option<&Proposer> {
        match self {
            Command2::CreateCluster { proposer, .. } => Some(proposer),
            Command2::AddServer { proposer, .. } => Some(proposer),
            Command2::RemoveServer { proposer, .. } => Some(proposer),
            Command2::TakeSnapshot { proposer, .. } => Some(proposer),
            Command2::ApplyCommand { proposer, .. } => proposer.as_ref(),
            Command2::ApplyQuery => None,
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
        proposer: Option<Proposer>,
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
                let command = commands.get(&index).cloned()?;
                Some(match command {
                    Command2::CreateCluster {
                        seed_server_addr,
                        settings,
                        proposer,
                    } => Self::CreateCluster {
                        seed_server_addr,
                        settings,
                        proposer,
                    },
                    Command2::AddServer {
                        server_addr,
                        proposer,
                    } => Self::AddServer {
                        server_addr,
                        proposer,
                    },
                    Command2::TakeSnapshot { proposer } => Self::TakeSnapshot { proposer },
                    Command2::RemoveServer {
                        server_addr,
                        proposer,
                    } => Self::RemoveServer {
                        server_addr,
                        proposer,
                    },

                    Command2::ApplyCommand { input, proposer } => {
                        Self::ApplyCommand { input, proposer }
                    }
                    Command2::ApplyQuery => Self::ApplyQuery,
                })
            }
        }
    }
}
