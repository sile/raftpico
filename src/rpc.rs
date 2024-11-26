//! RPC messages.
use std::net::SocketAddr;

use jsonlrpc::{ErrorCode, ErrorObject, JsonRpcVersion, RequestId};
use jsonlrpc_mio::ClientId;
use raftbare::{ClusterConfig, LogEntries, MessageHeader, MessageSeqNo};
use serde::{Deserialize, Serialize};

use crate::{
    command::Command,
    server::{Commands, ServerInstanceId},
    types::{LogIndex, LogPosition, NodeId, Term},
    ApplyKind,
};

/// RPC request.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "method")]
#[allow(missing_docs)]
pub enum Request {
    /// **\[EXTERNAL (API)\]** Create a cluster.
    CreateCluster {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        #[serde(default)]
        params: CreateClusterParams,
    },

    /// **\[EXTERNAL (API)\]** Add a server to a cluster.
    AddServer {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        params: AddServerParams,
    },

    /// **\[EXTERNAL (API)\]** Remove a server from a cluster.
    RemoveServer {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        params: RemoveServerParams,
    },
    Apply {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        params: ApplyParams,
    },
    TakeSnapshot {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
    },
    // TODO: GetServerState

    // Internal APIs
    Propose {
        // TODO: ProposeCommand (?)
        jsonrpc: JsonRpcVersion,
        params: ProposeParams,
    },
    ProposeQuery {
        jsonrpc: JsonRpcVersion,
        params: ProposeQueryParams,
    },
    NotifyQueryPromise {
        jsonrpc: JsonRpcVersion,
        params: NotifyQueryPromiseParams,
    },
    InitNode {
        jsonrpc: JsonRpcVersion,
        params: InitNodeParams,
    },
    Snapshot {
        jsonrpc: JsonRpcVersion,
        params: SnapshotParams,
    },
    // Raft messages
    AppendEntries {
        jsonrpc: JsonRpcVersion,
        id: RequestId, // TODO: remove
        params: AppendEntriesParams,
    },
    AppendEntriesResult {
        jsonrpc: JsonRpcVersion,
        id: RequestId, // TODO: remove
        params: AppendEntriesResultParams,
    },
    RequestVote {
        jsonrpc: JsonRpcVersion,
        id: RequestId, // TODO: remove
        params: RequestVoteParams,
    },
    RequestVoteResult {
        jsonrpc: JsonRpcVersion,
        id: RequestId, // TODO: remove
        params: RequestVoteResultParams,
    },
}

impl Request {
    pub(crate) fn from_raft_message(
        message: raftbare::Message,
        commands: &Commands,
    ) -> Option<Self> {
        Some(match message {
            raftbare::Message::RequestVoteCall {
                header,
                last_position,
            } => Self::RequestVote {
                jsonrpc: JsonRpcVersion::V2,
                id: RequestId::Number(header.seqno.get() as i64),
                params: RequestVoteParams {
                    from: header.from.into(),
                    term: header.term.into(),
                    last_log_position: last_position.into(),
                },
            },
            raftbare::Message::AppendEntriesCall {
                header,
                commit_index,
                entries,
            } => {
                let params =
                    AppendEntriesParams::new(header, commit_index.into(), entries, commands)?;
                Self::AppendEntries {
                    jsonrpc: JsonRpcVersion::V2,
                    id: RequestId::Number(header.seqno.get() as i64),
                    params,
                }
            }
            raftbare::Message::RequestVoteReply {
                header,
                vote_granted,
            } => Self::RequestVoteResult {
                jsonrpc: JsonRpcVersion::V2,
                id: RequestId::Number(header.seqno.get() as i64),
                params: RequestVoteResultParams {
                    from: header.from.into(),
                    term: header.term.into(),
                    vote_granted,
                },
            },
            raftbare::Message::AppendEntriesReply {
                header,
                last_position,
            } => Self::AppendEntriesResult {
                jsonrpc: JsonRpcVersion::V2,
                id: RequestId::Number(header.seqno.get() as i64),
                params: AppendEntriesResultParams {
                    from: header.from.into(),
                    term: header.term.into(),
                    last_log_position: last_position.into(),
                },
            },
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AppendEntriesResultParams {
    pub from: NodeId,
    pub term: Term,
    pub last_log_position: LogPosition,
}

impl AppendEntriesResultParams {
    pub fn into_raft_message(self, caller: &Caller) -> raftbare::Message {
        let RequestId::Number(request_id) = caller.request_id else {
            todo!("make this branch unreachable");
        };

        raftbare::Message::AppendEntriesReply {
            header: MessageHeader {
                from: self.from.into(),
                term: self.term.into(),
                seqno: MessageSeqNo::new(request_id as u64),
            },
            last_position: self.last_log_position.into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RequestVoteParams {
    pub from: NodeId,
    pub term: Term,
    pub last_log_position: LogPosition,
}

impl RequestVoteParams {
    pub fn into_raft_message(self, caller: &Caller) -> raftbare::Message {
        let RequestId::Number(request_id) = caller.request_id else {
            todo!("make this branch unreachable");
        };

        raftbare::Message::RequestVoteCall {
            header: MessageHeader {
                from: self.from.into(),
                term: self.term.into(),
                seqno: MessageSeqNo::new(request_id as u64),
            },
            last_position: self.last_log_position.into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RequestVoteResultParams {
    pub from: NodeId,
    pub term: Term,
    pub vote_granted: bool,
}

impl RequestVoteResultParams {
    pub fn into_raft_message(self, caller: &Caller) -> raftbare::Message {
        let RequestId::Number(request_id) = caller.request_id else {
            todo!("make this branch unreachable");
        };

        raftbare::Message::RequestVoteReply {
            header: MessageHeader {
                from: self.from.into(),
                term: self.term.into(),
                seqno: MessageSeqNo::new(request_id as u64),
            },
            vote_granted: self.vote_granted,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AppendEntriesParams {
    pub from: NodeId,
    pub term: Term,
    pub commit_index: LogIndex,
    pub prev_position: LogPosition,
    pub entries: Vec<Command>,
}

impl AppendEntriesParams {
    fn new(
        header: MessageHeader,
        commit_index: LogIndex,
        entries: LogEntries,
        commands: &Commands,
    ) -> Option<Self> {
        Some(Self {
            from: header.from.into(),
            term: header.term.into(),
            commit_index,
            prev_position: entries.prev_position().into(),
            entries: entries
                .iter_with_positions()
                .map(|(p, x)| Command::new(p.index, &x, commands))
                .collect::<Option<Vec<_>>>()?,
        })
    }

    pub fn into_raft_message(
        self,
        caller: &Caller,
        commands: &mut Commands,
    ) -> Option<raftbare::Message> {
        let RequestId::Number(request_id) = caller.request_id else {
            return None;
        };

        let prev_position = raftbare::LogPosition::from(self.prev_position);
        let prev_index = u64::from(self.prev_position.index);
        let entries = (1..)
            .map(|i| LogIndex::from(prev_index + i))
            .zip(self.entries)
            .map(|(i, x)| match x {
                Command::StartTerm { term: v } => raftbare::LogEntry::Term(v.into()),
                Command::UpdateClusterConfig { voters, new_voters } => {
                    raftbare::LogEntry::ClusterConfig(ClusterConfig {
                        voters: voters.into_iter().map(From::from).collect(),
                        new_voters: new_voters.into_iter().map(From::from).collect(),
                        ..ClusterConfig::default()
                    })
                }
                command => {
                    commands.insert(i, command);
                    raftbare::LogEntry::Command
                }
            });
        let entries = LogEntries::from_iter(prev_position, entries);

        Some(raftbare::Message::AppendEntriesCall {
            header: MessageHeader {
                from: self.from.into(),
                term: self.term.into(),
                seqno: MessageSeqNo::new(request_id as u64),
            },
            commit_index: self.commit_index.into(),
            entries,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddServerParams {
    pub addr: SocketAddr,
}

#[derive(Debug, Serialize, Deserialize)]
// TODO: #[serde(rename_all = "camelCase")]
pub struct RemoveServerParams {
    pub addr: SocketAddr,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApplyParams {
    pub kind: ApplyKind,
    pub input: serde_json::Value,
    // [NOTE] Cannot use RawValue here: https://github.com/serde-rs/json/issues/545
    //
    // TODO: struct { jsonrpc, method, id, params: RawValue } then serde_json::from_str(RawValue.get())
    //       (use RawValue in jsonlrpc_mio(?))
    // pub input: Box<RawValue>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProposeParams {
    pub command: Command,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProposeQueryParams {
    pub origin: NodeId,
    pub input: serde_json::Value, // TODO: remove
    pub caller: Caller,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NotifyQueryPromiseParams {
    pub commit_position: LogPosition,
    pub input: serde_json::Value, // TODO: remove
    pub caller: Caller,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InitNodeParams {
    pub node_id: NodeId,
    pub snapshot: SnapshotParams, // TODO
}

// TODO: move
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Proposer {
    pub server: ServerInstanceId,
    pub(crate) client: Caller, // TODO: rename
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Caller {
    pub from: ClientId,
    pub request_id: RequestId,
}

impl Caller {
    pub fn new(from: ClientId, request_id: RequestId) -> Self {
        Self { from, request_id }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateClusterOutput {
    pub members: Vec<SocketAddr>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddServerOutput {
    pub members: Vec<SocketAddr>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RemoveServerOutput {
    pub members: Vec<SocketAddr>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TakeSnapshotOutput {
    pub snapshot_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotParams<M = serde_json::Value> {
    // position and config
    pub last_included_position: LogPosition,
    pub voters: Vec<u64>,
    pub new_voters: Vec<u64>,

    // TODO: doc
    pub machine: M,
}

/// Parameters of [`Request::CreateCluster`].
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateClusterParams {
    /// Minimum value for the Raft election timeout (default: `100` milliseconds).
    ///
    /// See also: [`raftbare::Action::SetElectionTimeout`]
    pub min_election_timeout_ms: u32,

    /// Maximum value for the Raft election timeout (default: `1000` milliseconds).
    ///
    /// See also: [`raftbare::Action::SetElectionTimeout`]
    pub max_election_timeout_ms: u32,
}

impl Default for CreateClusterParams {
    fn default() -> Self {
        Self {
            min_election_timeout_ms: 100,
            max_election_timeout_ms: 1000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ErrorKind {
    ClusterAlreadyCreated = 1,
    InvalidMachineInput,
    InvalidMachineOutput,
    NoMachineOutput,
    ServerAlreadyAdded,
    NotClusterMember,
    UnknownServer,
}

impl ErrorKind {
    pub const fn code(self) -> ErrorCode {
        ErrorCode::new(self as i32)
    }

    pub const fn message(&self) -> &'static str {
        match self {
            ErrorKind::ClusterAlreadyCreated => "Cluster already created",
            ErrorKind::NoMachineOutput => "No machine output",
            ErrorKind::InvalidMachineInput => "Invalid machine input",
            ErrorKind::InvalidMachineOutput => "Invalid machine output",
            ErrorKind::ServerAlreadyAdded => "Server already added",
            ErrorKind::NotClusterMember => "Not a cluster member",
            ErrorKind::UnknownServer => "Unknown server",
        }
    }

    pub fn object(self) -> ErrorObject {
        ErrorObject {
            code: self.code(),
            message: self.message().to_owned(),
            data: None,
        }
    }

    pub fn object_with_reason<E: std::fmt::Display>(self, reason: E) -> ErrorObject {
        self.object_with_data(serde_json::json!({"reason": reason.to_string()}))
    }

    pub fn object_with_data(self, data: serde_json::Value) -> ErrorObject {
        ErrorObject {
            code: self.code(),
            message: self.message().to_owned(),
            data: Some(data),
        }
    }
}
