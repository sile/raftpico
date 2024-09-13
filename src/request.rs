use std::net::SocketAddr;

use jsonlrpc::{JsonRpcVersion, RequestId, RequestParams};
use serde::{Deserialize, Serialize};

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
    Command {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        params: RequestParams,
    },
    Query {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        params: RequestParams,
    },
    LocalQuery {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        params: RequestParams,
    },
    //
    // Internal messages
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
