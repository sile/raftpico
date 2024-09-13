use std::net::SocketAddr;

use jsonlrpc::RequestId;
use mio::Token;
use raftbare::NodeId;
use serde::{Deserialize, Serialize};

use crate::remote_types::{NodeIdDef, TokenDef};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    Join {
        #[serde(with = "NodeIdDef")]
        id: NodeId,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcCaller {
    #[serde(with = "NodeIdDef")]
    pub callee_id: NodeId,

    // TODO: the following two fields could be merged into a single field (e.g., 'FooId' for a map)
    #[serde(with = "TokenDef")]
    pub token: Token,

    pub request_id: RequestId,
}
