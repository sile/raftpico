use std::{net::SocketAddr, time::Duration};

use jsonlrpc::RequestId;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::server2::ClusterSettings;

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

#[derive(Debug, Serialize, Deserialize)]
pub enum Command2 {
    CreateCluster {
        seed_server_addr: SocketAddr,
        settings: ClusterSettings,
    },
    ApplyCommand {
        input: Box<RawValue>,
    },
    ApplyQuery,
}

// TODO: move
#[derive(Debug)]
pub struct Caller {
    pub from: jsonlrpc_mio::From,
    pub request_id: RequestId,
}

impl Caller {
    pub fn new(from: jsonlrpc_mio::From, request_id: RequestId) -> Self {
        Self { from, request_id }
    }
}
