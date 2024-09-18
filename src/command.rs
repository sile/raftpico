use std::{net::SocketAddr, time::Duration};

use serde::{Deserialize, Serialize};

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
