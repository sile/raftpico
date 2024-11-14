use std::{net::SocketAddr, time::Duration};

use serde::{Deserialize, Serialize};

use crate::{machine::Machine2, server2::ClusterSettings};

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
pub enum Command2<M: Machine2> {
    CreateCluster {
        seed_server_addr: SocketAddr,
        settings: ClusterSettings,
    },
    Command(M::Input),
    Query,
}
