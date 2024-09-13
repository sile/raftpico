use jsonlrpc::RequestId;
use mio::Token;
use raftbare::NodeId;
use serde::{Deserialize, Serialize};

use crate::remote_types::{NodeIdDef, TokenDef};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    System(SystemCommand),
    User(serde_json::Value),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SystemCommand {
    // InstallSnapshot
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcCaller {
    #[serde(with = "NodeIdDef")]
    pub callee_id: NodeId,

    #[serde(with = "TokenDef")]
    pub token: Token,

    pub request_id: RequestId,
}
