use std::net::SocketAddr;

use jsonlrpc::{JsonRpcVersion, RequestId};
use serde::{Deserialize, Serialize};

use crate::command::Command;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "method")]
pub enum Request {
    // API messages
    CreateCluster {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        params: Option<CreateClusterParams>,
    },
    Join {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        params: JoinParams,
    },
    Kick {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        params: KickParams,
    },
    Apply {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        params: ApplyParams,
    },
    // TODO: GetMembers, etc

    // Internal messages
    Propose {
        jsonrpc: JsonRpcVersion,
        id: u32,
        params: ProposeParams,
    },
    GetSnapshot {
        jsonrpc: JsonRpcVersion,
        id: u32,
    }, //  Raft
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateClusterParams {
    pub min_election_timeout: Seconds,
    pub max_election_timeout: Seconds,
}

// TODO: validate

impl Default for CreateClusterParams {
    fn default() -> Self {
        Self {
            min_election_timeout: Seconds::new(100.0),
            max_election_timeout: Seconds::new(1000.0),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Seconds(f64);

impl Seconds {
    pub const fn new(seconds: f64) -> Self {
        Self(seconds)
    }

    pub const fn get(self) -> f64 {
        self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinParams {
    pub contact_addr: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KickParams {
    pub target_addr: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposeParams {
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AskParams {
    pub query: serde_json::Value,

    #[serde(default)]
    pub local: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyParams {
    #[serde(default)]
    pub kind: ApplyKind,
    pub args: serde_json::Value,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ApplyKind {
    #[default]
    Command,
    Query,
    LocalQuery,
}
