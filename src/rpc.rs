//! RPC messages.
use std::net::SocketAddr;

use jsonlrpc::{ErrorCode, ErrorObject, JsonRpcVersion, RequestId};
use jsonlrpc_mio::ClientId;
use serde::{Deserialize, Serialize};

use crate::{
    command::Command,
    server::{Commands, ServerInstanceId},
    types::{LogIndex, LogPosition, NodeId, Term},
    ApplyKind,
};

/// JSON-RPC request message.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "method")]
#[allow(missing_docs)]
pub enum Request {
    /// **\[API\]** Create a cluster.
    CreateCluster {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        #[serde(default)]
        params: CreateClusterParams,
    },

    /// **\[API\]** Add a server to a cluster.
    AddServer {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        params: AddServerParams,
    },

    /// **\[API\]** Remove a server from a cluster.
    RemoveServer {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        params: RemoveServerParams,
    },

    /// **\[API\]** Call [`Machine::apply()`][crate::Machine::apply()].
    Apply {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        params: ApplyParams,
    },

    /// **\[API\]** Take a snapshot and remove old log entries preceding the snapshot.
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
    NotifyCommit {
        jsonrpc: JsonRpcVersion,
        params: NotifyCommitParams,
    },

    /// **\[INTERNAL:raftbare\]** See: [`raftbare::Message::AppendEntriesCall`].
    AppendEntriesCall {
        jsonrpc: JsonRpcVersion,
        params: AppendEntriesCallParams,
    },

    /// **\[INTERNAL:raftbare\]** See: [`raftbare::Message::AppendEntriesReply`].
    AppendEntriesReply {
        jsonrpc: JsonRpcVersion,
        params: AppendEntriesReplyParams,
    },

    /// **\[INTERNAL:raftbare\]** See: [`raftbare::Message::RequestVoteCall`].
    RequestVoteCall {
        jsonrpc: JsonRpcVersion,
        params: RequestVoteCallParams,
    },

    /// **\[INTERNAL:raftbare\]** See: [`raftbare::Message::RequestVoteReply`].
    RequestVoteReply {
        jsonrpc: JsonRpcVersion,
        params: RequestVoteReplyParams,
    },

    /// **\[INTERNAL:raftbare\]** See: [`raftbare::Action::InstallSnapshot`].
    InstallSnapshot {
        jsonrpc: JsonRpcVersion,
        params: InstallSnapshotParams,
    },
}

impl Request {
    pub(crate) fn from_raftbare(message: raftbare::Message, commands: &Commands) -> Option<Self> {
        Some(match message {
            raftbare::Message::RequestVoteCall {
                header,
                last_position,
            } => Self::RequestVoteCall {
                jsonrpc: JsonRpcVersion::V2,
                params: RequestVoteCallParams {
                    header: MessageHeader::from_raftbare(header),
                    last_log_position: last_position.into(),
                },
            },
            raftbare::Message::AppendEntriesCall {
                header,
                commit_index,
                entries,
            } => Self::AppendEntriesCall {
                jsonrpc: JsonRpcVersion::V2,
                params: AppendEntriesCallParams::from_raftbare(
                    header,
                    commit_index.into(),
                    entries,
                    commands,
                )?,
            },
            raftbare::Message::RequestVoteReply {
                header,
                vote_granted,
            } => Self::RequestVoteReply {
                jsonrpc: JsonRpcVersion::V2,
                params: RequestVoteReplyParams {
                    header: MessageHeader::from_raftbare(header),
                    vote_granted,
                },
            },
            raftbare::Message::AppendEntriesReply {
                header,
                last_position,
            } => Self::AppendEntriesReply {
                jsonrpc: JsonRpcVersion::V2,
                params: AppendEntriesReplyParams {
                    header: MessageHeader::from_raftbare(header),
                    last_log_position: last_position.into(),
                },
            },
        })
    }
}

/// Serializable version of [`raftbare::MessageHeader`].
#[derive(Debug, Serialize, Deserialize)]
#[allow(missing_docs)]
pub struct MessageHeader {
    pub from: NodeId,
    pub term: Term,
    pub seqno: u64,
}

impl MessageHeader {
    fn from_raftbare(header: raftbare::MessageHeader) -> Self {
        Self {
            from: header.from.into(),
            term: header.term.into(),
            seqno: header.seqno.get(),
        }
    }

    fn to_raftbare(&self) -> raftbare::MessageHeader {
        raftbare::MessageHeader {
            from: self.from.into(),
            term: self.term.into(),
            seqno: raftbare::MessageSeqNo::new(self.seqno),
        }
    }
}

/// Serializable version of [`raftbare::LogEntries`].
#[derive(Debug, Serialize, Deserialize)]
#[allow(missing_docs)]
pub struct LogEntries {
    pub prev: LogPosition,
    pub commands: Vec<Command>,
}

impl LogEntries {
    pub(crate) fn from_raftbare(
        entries: &raftbare::LogEntries,
        commands: &Commands,
    ) -> Option<Self> {
        Some(Self {
            prev: entries.prev_position().into(),
            commands: entries
                .iter_with_positions()
                .map(|(p, x)| Command::from_log_entry(p.index.into(), &x, commands))
                .collect::<Option<Vec<_>>>()?,
        })
    }

    pub(crate) fn into_raftbare(self, commands: &mut Commands) -> raftbare::LogEntries {
        let prev_position = raftbare::LogPosition::from(self.prev);
        let entries = (u64::from(self.prev.index) + 1..)
            .map(LogIndex::from)
            .zip(self.commands)
            .map(|(i, x)| x.into_log_entry(i, commands));
        raftbare::LogEntries::from_iter(prev_position, entries)
    }
}

/// Parameters of [`Request::AppendEntriesCall`].
///
/// See also: [`raftbare::Message::AppendEntriesCall`]
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(missing_docs)]
pub struct AppendEntriesCallParams {
    pub header: MessageHeader,
    pub commit_index: LogIndex,
    pub entries: LogEntries,
}

impl AppendEntriesCallParams {
    pub(crate) fn from_raftbare(
        header: raftbare::MessageHeader,
        commit_index: LogIndex,
        entries: raftbare::LogEntries,
        commands: &Commands,
    ) -> Option<Self> {
        Some(Self {
            header: MessageHeader::from_raftbare(header),
            commit_index,
            entries: LogEntries::from_raftbare(&entries, commands)?,
        })
    }

    pub(crate) fn into_raftbare(self, commands: &mut Commands) -> raftbare::Message {
        raftbare::Message::AppendEntriesCall {
            header: self.header.to_raftbare(),
            commit_index: self.commit_index.into(),
            entries: self.entries.into_raftbare(commands),
        }
    }
}

/// Parameters of [`Request::AppendEntriesReply`].
///
/// See also: [`raftbare::Message::AppendEntriesReply`]
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(missing_docs)]
pub struct AppendEntriesReplyParams {
    pub header: MessageHeader,
    pub last_log_position: LogPosition,
}

impl AppendEntriesReplyParams {
    pub(crate) fn into_raftbare(self) -> raftbare::Message {
        raftbare::Message::AppendEntriesReply {
            header: self.header.to_raftbare(),
            last_position: self.last_log_position.into(),
        }
    }
}

/// Parameters of [`Request::RequestVoteCall`].
///
/// See also: [`raftbare::Message::RequestVoteCall`]
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(missing_docs)]
pub struct RequestVoteCallParams {
    pub header: MessageHeader,
    pub last_log_position: LogPosition,
}

impl RequestVoteCallParams {
    pub(crate) fn into_raftbare(self) -> raftbare::Message {
        raftbare::Message::RequestVoteCall {
            header: self.header.to_raftbare(),
            last_position: self.last_log_position.into(),
        }
    }
}

/// Parameters of [`Request::RequestVoteReply`].
///
/// See also: [`raftbare::Message::RequestVoteReply`]
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(missing_docs)]
pub struct RequestVoteReplyParams {
    pub header: MessageHeader,
    pub vote_granted: bool,
}

impl RequestVoteReplyParams {
    pub(crate) fn into_raftbare(self) -> raftbare::Message {
        raftbare::Message::RequestVoteReply {
            header: self.header.to_raftbare(),
            vote_granted: self.vote_granted,
        }
    }
}

/// Parameters of [`Request::InstallSnapshot`].
///
/// See also: [`raftbare::Action::InstallSnapshot`]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(missing_docs)]
pub struct InstallSnapshotParams {
    // [NOTE] Unlike the other fields, this information is specific to a node.
    pub node_id: NodeId,

    pub last_included_position: LogPosition,
    pub voters: Vec<NodeId>,
    pub new_voters: Vec<NodeId>,
    pub machine: serde_json::Value,
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

/// Successful result of [`Request::CreateCluster`].
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateClusterResult {
    /// Latest cluster members.
    pub members: Vec<SocketAddr>,
}

/// Parameters of [`Request::AddServer`].
#[derive(Debug, Serialize, Deserialize)]
pub struct AddServerParams {
    /// Address of the server to be added.
    pub addr: SocketAddr,
}

/// Successful result of [`Request::AddServer`].
#[derive(Debug, Serialize, Deserialize)]
pub struct AddServerResult {
    /// Latest cluster members.
    pub members: Vec<SocketAddr>,
}

/// Parameters of [`Request::RemoveServer`].
#[derive(Debug, Serialize, Deserialize)]
pub struct RemoveServerParams {
    /// Address of the server to be removed.
    pub addr: SocketAddr,
}

/// Successful result of [`Request::RemoveServer`].
#[derive(Debug, Serialize, Deserialize)]
pub struct RemoveServerResult {
    /// Latest cluster members.
    pub members: Vec<SocketAddr>,
}

/// Parameters of [`Request::Apply`].
#[derive(Debug, Serialize, Deserialize)]
pub struct ApplyParams {
    /// The value of [ApplyContext::kind()][crate::ApplyContext::kind()].
    pub kind: ApplyKind,

    // [Note]
    // Considered using `serde_json::value::RawValue` here
    // but were unable to do so due to issue https://github.com/serde-rs/json/issues/545.
    /// The value of the `input` parameter in [Machine::apply()][crate::Machine::apply()].
    pub input: serde_json::Value,
}

/// Successful result of [`Request::TakeSnapshot`].
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TakeSnapshotResult {
    pub snapshot_index: LogIndex,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProposeParams {
    pub command: Command,
    pub caller: Caller,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProposeQueryParams {
    pub input: serde_json::Value, // TODO: remove
    pub caller: Caller,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NotifyCommitParams {
    pub commit: LogPosition,
    pub input: serde_json::Value, // TODO: remove
    pub caller: Caller,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InitNodeParams {
    pub node_id: NodeId,
    pub snapshot: InstallSnapshotParams, // TODO
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Caller {
    pub server_id: ServerInstanceId,
    pub node_id: NodeId,
    pub client_id: ClientId,
    pub request_id: RequestId,
}

/// RPC error kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(missing_docs)]
pub enum ErrorKind {
    ClusterAlreadyCreated = 1,
    InvalidMachineInput,
    InvalidMachineOutput,
    NoMachineOutput,
    ServerAlreadyAdded,
    NotClusterMember,
}

impl ErrorKind {
    /// Returns JSON-RPC error code.
    pub const fn code(self) -> ErrorCode {
        ErrorCode::new(self as i32)
    }

    /// Returns JSON-RPC error message.
    pub const fn message(&self) -> &'static str {
        match self {
            ErrorKind::ClusterAlreadyCreated => "Cluster already created",
            ErrorKind::NoMachineOutput => "No machine output",
            ErrorKind::InvalidMachineInput => "Invalid machine input",
            ErrorKind::InvalidMachineOutput => "Invalid machine output",
            ErrorKind::ServerAlreadyAdded => "Server already added",
            ErrorKind::NotClusterMember => "Not a cluster member",
        }
    }

    pub(crate) fn object(self) -> ErrorObject {
        ErrorObject {
            code: self.code(),
            message: self.message().to_owned(),
            data: None,
        }
    }

    pub(crate) fn object_with_reason<E: std::fmt::Display>(self, reason: E) -> ErrorObject {
        self.object_with_data(serde_json::json!({"reason": reason.to_string()}))
    }

    pub(crate) fn object_with_data(self, data: serde_json::Value) -> ErrorObject {
        ErrorObject {
            code: self.code(),
            message: self.message().to_owned(),
            data: Some(data),
        }
    }
}
