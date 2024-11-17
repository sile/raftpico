use std::{net::SocketAddr, time::Duration};

use jsonlrpc::RequestId;
use jsonlrpc_mio::ClientId;
use raftbare::LogIndex;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::{
    message::Proposer,
    server2::{ClusterSettings, Commands},
};

// TODO: delete
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    InitCluster {
        server_addr: SocketAddr,
        min_election_timeout: Duration,
        max_election_timeout: Duration,
        max_log_entries_hint: usize,
    },
    InviteServer {
        server_addr: SocketAddr,
    },
    EvictServer {
        server_addr: SocketAddr,
    },
    Command(serde_json::Value),
    Query,
}

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
    ApplyCommand {
        input: Box<RawValue>,
        proposer: Proposer,
    },
    ApplyQuery,
}

impl Command2 {
    pub fn proposer(&self) -> Option<&Proposer> {
        match self {
            Command2::CreateCluster { proposer, .. } => Some(proposer),
            Command2::AddServer { proposer, .. } => Some(proposer),
            Command2::ApplyCommand { proposer, .. } => Some(proposer),
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
    ApplyCommand {
        // TODO: Cow? or Rc
        input: Box<RawValue>,
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
                    Command2::ApplyCommand { input, proposer } => {
                        Self::ApplyCommand { input, proposer }
                    }
                    Command2::ApplyQuery => Self::ApplyQuery,
                })
            }
        }
    }
}
