use jsonlrpc::{ErrorCode, ErrorObject, JsonRpcVersion, RequestId};
use serde::{Deserialize, Serialize};

pub fn is_known_external_method(method: &str) -> bool {
    matches!(method, "CreateCluster")
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IncomingMessage {
    External(Request),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "method")]
pub enum Request {
    CreateCluster {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        #[serde(default)]
        params: CreateClusterParams,
    },
}

impl Request {
    pub fn create_cluster(id: RequestId, params: Option<CreateClusterParams>) -> Self {
        Self::CreateCluster {
            jsonrpc: JsonRpcVersion::V2,
            id,
            params: params.unwrap_or_default(),
        }
    }

    pub fn id(&self) -> &RequestId {
        match self {
            Request::CreateCluster { id, .. } => id,
        }
    }

    pub fn validate(&self) -> Option<ErrorObject> {
        match self {
            Request::CreateCluster { params, .. } => params.validate(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateClusterParams {
    pub min_election_timeout_ms: usize,
    pub max_election_timeout_ms: usize,
}

impl CreateClusterParams {
    // TODO: -> Option<ErrorObject>
    pub fn validate(&self) -> Option<ErrorObject> {
        if self.min_election_timeout_ms <= self.max_election_timeout_ms {
            return None;
        }

        Some(ErrorObject {
            code: ErrorCode::INVALID_PARAMS,
            message:
                "`min_election_timeout_ms` must be less than or equal to `max_election_timeout_ms`"
                    .to_string(),
            data: None,
        })
    }
}

impl Default for CreateClusterParams {
    fn default() -> Self {
        Self {
            min_election_timeout_ms: 100,
            max_election_timeout_ms: 1000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response<T> {
    Ok {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        result: T,
    },
    Err {
        jsonrpc: JsonRpcVersion,
        id: Option<RequestId>,
        error: ErrorObject,
    },
}

impl<T> Response<T> {
    pub fn into_std_result(self) -> Result<T, ErrorObject> {
        match self {
            Response::Ok { result, .. } => Ok(result),
            Response::Err { error, .. } => Err(error),
        }
    }
}

impl Response<CreateClusterResult> {
    pub fn create_cluster(id: RequestId, success: bool) -> Self {
        Self::Ok {
            jsonrpc: JsonRpcVersion::V2,
            id,
            result: CreateClusterResult { success },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateClusterResult {
    pub success: bool,
}

// use std::net::SocketAddr;

// use crate::command::Command;

// #[derive(Debug, Clone, Serialize, Deserialize)]
// #[serde(tag = "method")]
// pub enum Request {
//     // API messages
//     CreateCluster {
//         jsonrpc: JsonRpcVersion,
//         id: RequestId,
//         #[serde(default, skip_serializing_if = "Option::is_none")]
//         params: Option<CreateClusterParams>,
//     },
//     Join {
//         jsonrpc: JsonRpcVersion,
//         id: RequestId,
//         params: JoinParams,
//     },
//     Kick {
//         jsonrpc: JsonRpcVersion,
//         id: RequestId,
//         params: KickParams,
//     },
//     Apply {
//         jsonrpc: JsonRpcVersion,
//         id: RequestId,
//         params: ApplyParams,
//     },
//     // TODO: GetMembers, etc

//     // Internal messages
//     // TODO: Handshake
//     Propose {
//         jsonrpc: JsonRpcVersion,
//         id: u32,
//         params: ProposeParams,
//     },
//     GetSnapshot {
//         jsonrpc: JsonRpcVersion,
//         id: u32,
//     }, //  Raft
// }

// // TODO: validate

// impl Default for CreateClusterParams {
//     fn default() -> Self {
//         Self {
//             min_election_timeout: Seconds::new(100.0),
//             max_election_timeout: Seconds::new(1000.0),
//         }
//     }
// }

// #[derive(Debug, Clone, Copy, Serialize, Deserialize)]
// pub struct Seconds(f64);

// impl Seconds {
//     pub const fn new(seconds: f64) -> Self {
//         Self(seconds)
//     }

//     pub const fn get(self) -> f64 {
//         self.0
//     }
// }

// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct JoinParams {
//     pub contact_addr: SocketAddr,
// }

// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct KickParams {
//     pub target_addr: SocketAddr,
// }

// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct ProposeParams {
//     pub command: Command,
// }

// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct AskParams {
//     pub query: serde_json::Value,

//     #[serde(default)]
//     pub local: bool,
// }

// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct ApplyParams {
//     #[serde(default)]
//     pub kind: ApplyKind,
//     pub args: serde_json::Value,
// }

// #[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
// #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
// pub enum ApplyKind {
//     #[default]
//     Command,
//     Query,
//     LocalQuery,
// }
