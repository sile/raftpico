use std::net::SocketAddr;

use jsonlrpc::{ErrorCode, ErrorObject, JsonRpcVersion, RequestId};
use raftbare::{Message, MessageHeader};
use serde::{Deserialize, Serialize};

use crate::{command::Command, raft_server::Commands};

pub fn is_known_external_method(method: &str) -> bool {
    matches!(method, "CreateCluster")
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IncomingMessage {
    ExternalRequest(Request),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "method")]
pub enum InternalRequest {
    Handshake {
        jsonrpc: JsonRpcVersion,
        params: HandshakeParams,
    },
    AppendEntriesCall {
        jsonrpc: JsonRpcVersion,
        params: AppendEntriesCallParams,
    },
}

impl InternalRequest {
    pub fn from_raft_message(msg: Message, commands: &Commands) -> Self {
        match msg {
            Message::RequestVoteCall {
                header,
                last_position,
            } => todo!(),
            Message::RequestVoteReply {
                header,
                vote_granted,
            } => todo!(),
            Message::AppendEntriesCall {
                header: MessageHeader { from, term, seqno },
                commit_index,
                entries,
            } => Self::AppendEntriesCall {
                jsonrpc: JsonRpcVersion::V2,
                params: AppendEntriesCallParams {
                    from: from.get(),
                    term: term.get(),
                    seqno: seqno.get(),
                    commit_index: commit_index.get(),
                    prev_log_term: entries.prev_position().term.get(),
                    prev_log_index: entries.prev_position().index.get(),
                    entries: entries
                        .iter_with_positions()
                        .map(|(position, entry)| match entry {
                            raftbare::LogEntry::Term(t) => LogEntry::Term(t.get()),
                            raftbare::LogEntry::ClusterConfig(c) => LogEntry::Config {
                                voters: c.voters.iter().map(|v| v.get()).collect(),
                                new_voters: c.new_voters.iter().map(|v| v.get()).collect(),
                            },
                            raftbare::LogEntry::Command => {
                                let command = commands.get(&position.index).expect("bug");
                                LogEntry::Command(command.clone())
                            }
                        })
                        .collect(),
                },
            },
            Message::AppendEntriesReply {
                header,
                last_position,
            } => todo!(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesCallParams {
    from: u64,
    term: u64,
    seqno: u64,
    commit_index: u64,
    prev_log_term: u64,
    prev_log_index: u64,
    entries: Vec<LogEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogEntry {
    Term(u64),
    Config {
        voters: Vec<u64>,
        new_voters: Vec<u64>,
    },
    Command(Command),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeParams {
    pub src_node_id: u64,
    pub dst_node_id: u64,
    pub inviting: bool,
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
    AddServer {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        params: AddServerParams,
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

    pub fn add_server(id: RequestId, contact_server_addr: SocketAddr) -> Self {
        Self::AddServer {
            jsonrpc: JsonRpcVersion::V2,
            id,
            params: AddServerParams {
                server_addr: contact_server_addr,
            },
        }
    }

    pub fn id(&self) -> &RequestId {
        match self {
            Self::CreateCluster { id, .. } => id,
            Self::AddServer { id, .. } => id,
        }
    }

    pub fn validate(&self) -> Option<ErrorObject> {
        match self {
            Self::CreateCluster { params, .. } => params.validate(),
            Self::AddServer { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateClusterParams {
    pub min_election_timeout_ms: usize,
    pub max_election_timeout_ms: usize,
    pub max_log_entries_hint: usize,
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
            max_log_entries_hint: 100000,
        }
    }
}

// TODO: remove T?
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
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
    pub fn ok(id: RequestId, result: T) -> Self {
        Self::Ok {
            jsonrpc: JsonRpcVersion::V2,
            id,
            result,
        }
    }

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

impl Response<AddServerResult> {
    pub fn add_server(id: RequestId, result: Result<(), AddServerError>) -> Self {
        let result = AddServerResult {
            success: result.is_ok(),
            error: result.err(),
        };
        Self::Ok {
            jsonrpc: JsonRpcVersion::V2,
            id,
            result,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateClusterResult {
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddServerParams {
    pub server_addr: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddServerResult {
    pub success: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<AddServerError>,
}

impl AddServerResult {
    pub fn ok() -> Self {
        Self {
            success: true,
            error: None,
        }
    }

    pub fn err(e: AddServerError) -> Self {
        Self {
            success: false,
            error: Some(e),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AddServerError {
    // TODO
    ServerNotReady,
    ProposalRejected,
    AlreadyInCluster,
}

// #[derive(Debug, Clone, Serialize, Deserialize)]
// #[serde(tag = "method")]
// pub enum Request {
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
