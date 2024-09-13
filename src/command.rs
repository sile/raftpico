use jsonlrpc::RequestId;
use mio::Token;
use raftbare::NodeId;
use serde::{Deserialize, Serialize};

use crate::remote_types::{NodeIdDef, TokenDef};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    Apply {
        command: serde_json::Value,
        caller: RpcCaller,
    },

    // Note that this is merged if there are concurrent apply and ask commands
    Ask,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcCaller {
    #[serde(with = "NodeIdDef")]
    pub callee_id: NodeId,

    #[serde(with = "TokenDef")]
    pub token: Token,

    pub request_id: RequestId,
}
