use std::net::SocketAddr;

use jsonlrpc::RequestId;
use serde::{Deserialize, Serialize};

use crate::remote_types::{NodeIdJson, TokenJson};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    Join {
        id: NodeIdJson,
        // TODO: instance_id
        addr: SocketAddr,
        caller: RpcCaller,
    },
    Apply {
        command: serde_json::Value,
        caller: RpcCaller,
    },

    // Note that this is merged if there are concurrent (but not yet proposed) apply and ask commands
    Ask,
}

// TODO: rename
//
// TODO: Make it possible to save this instance to reply after at the time a condition is met
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcCaller {
    pub callee_id: NodeIdJson,

    // TODO: the following two fields could be merged into a single field (e.g., 'FooId' for a map)
    pub token: TokenJson,

    pub request_id: RequestId,
}
