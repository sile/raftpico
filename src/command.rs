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
    InstallSnapshot,
    Command(serde_json::Value),
    // AddMember {
    //     node_id: NodeIdJson,
    //     server_addr: SocketAddr,
    // },

    // Join {
    //     id: NodeIdJson,
    //     // TODO: instance_id
    //     addr: SocketAddr,
    //     caller: RpcCaller,
    // },
    // Apply {
    //     command: serde_json::Value,
    //     caller: RpcCaller,
    // },

    // // Note that this is merged if there are concurrent (but not yet proposed) apply and ask commands
    // Ask,
}

// // TODO: rename
// //
// // TODO: Make it possible to save this instance to reply after at the time a condition is met
// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct RpcCaller {
//     pub callee_id: NodeIdJson,

//     // TODO: the following two fields could be merged into a single field (e.g., 'FooId' for a map)
//     pub token: TokenJson,

//     pub request_id: RequestId,
// }
