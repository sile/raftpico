//! RPC messages.
use std::net::SocketAddr;

use jsonlrpc::{ErrorCode, ErrorObject, JsonRpcVersion, RequestId};
use jsonlrpc_mio::ClientId;
use serde::{Deserialize, Serialize};

use crate::{
    command::{Command, Commands},
    machines::Machines,
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

    /// **\[API\]** Get the server state.
    GetServerState {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
    },

    /// **\[INTERNAL:raftpico\]** Propose a command.
    ProposeCommand {
        jsonrpc: JsonRpcVersion,
        params: ProposeCommandParams,
    },

    /// **\[INTERNAL:raftpico\]** Notify the commit position associated with an ongoing proposal.
    NotifyPendingCommit {
        jsonrpc: JsonRpcVersion,
        params: NotifyPendingCommitParams,
    },

    /// **\[INTERNAL:raftpico\]** Reply an error response to a client connecting to a remote server.
    ReplyError {
        jsonrpc: JsonRpcVersion,
        params: ReplyErrorParams,
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
    pub(crate) fn install_snapshot(node_id: NodeId, mut params: InstallSnapshotParams) -> Self {
        params.node_id = node_id;
        Request::InstallSnapshot {
            jsonrpc: jsonlrpc::JsonRpcVersion::V2,
            params,
        }
    }

    pub(crate) fn notify_commit(
        commit: LogPosition,
        input: Option<serde_json::Value>,
        caller: Caller,
    ) -> Self {
        Self::NotifyPendingCommit {
            jsonrpc: jsonlrpc::JsonRpcVersion::V2,
            params: NotifyPendingCommitParams {
                commit,
                input,
                caller,
            },
        }
    }

    pub(crate) fn reply_error(error: ErrorReason, caller: Caller) -> Self {
        Self::ReplyError {
            jsonrpc: JsonRpcVersion::V2,
            params: ReplyErrorParams { error, caller },
        }
    }

    pub(crate) fn propose_command(
        command: Command,
        query_input: Option<serde_json::Value>,
        caller: Caller,
    ) -> Self {
        Self::ProposeCommand {
            jsonrpc: jsonlrpc::JsonRpcVersion::V2,
            params: ProposeCommandParams {
                command,
                query_input,
                caller,
            },
        }
    }

    pub(crate) fn from_raftbare(message: raftbare::Message, commands: &Commands) -> Self {
        match message {
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
                    LogIndex(commit_index),
                    entries,
                    commands,
                ),
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
                params: AppendEntriesReplyParams::from_raftbare(header, last_position),
            },
        }
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
            from: NodeId(header.from),
            term: Term(header.term),
            seqno: header.seqno.get(),
        }
    }

    fn to_raftbare(&self) -> raftbare::MessageHeader {
        raftbare::MessageHeader {
            from: self.from.0,
            term: self.term.0,
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
    pub(crate) fn from_raftbare(entries: &raftbare::LogEntries, commands: &Commands) -> Self {
        Self {
            prev: entries.prev_position().into(),
            commands: entries
                .iter_with_positions()
                .map(|(p, x)| Command::from_log_entry(LogIndex(p.index), &x, commands))
                .collect(),
        }
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
    ) -> Self {
        Self {
            header: MessageHeader::from_raftbare(header),
            commit_index,
            entries: LogEntries::from_raftbare(&entries, commands),
        }
    }

    pub(crate) fn into_raftbare(self, commands: &mut Commands) -> raftbare::Message {
        raftbare::Message::AppendEntriesCall {
            header: self.header.to_raftbare(),
            commit_index: self.commit_index.0,
            entries: self.entries.into_raftbare(commands),
        }
    }
}

/// Parameters of [`Request::AppendEntriesReply`].
///
/// See also: [`raftbare::Message::AppendEntriesReply`]
#[derive(Debug, Serialize, Deserialize)]
#[allow(missing_docs)]
pub struct AppendEntriesReplyParams {
    pub header: MessageHeader,
    pub last_position: LogPosition,
}

impl AppendEntriesReplyParams {
    pub(crate) fn into_raftbare(self) -> raftbare::Message {
        raftbare::Message::AppendEntriesReply {
            header: self.header.to_raftbare(),
            last_position: self.last_position.into(),
        }
    }

    pub(crate) fn from_raftbare(
        header: raftbare::MessageHeader,
        last_position: raftbare::LogPosition,
    ) -> Self {
        AppendEntriesReplyParams {
            header: MessageHeader::from_raftbare(header),
            last_position: last_position.into(),
        }
    }
}

/// Parameters of [`Request::RequestVoteCall`].
///
/// See also: [`raftbare::Message::RequestVoteCall`]
#[derive(Debug, Serialize, Deserialize)]
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
#[allow(missing_docs)]
pub struct InstallSnapshotParams {
    // [NOTE] Unlike the other fields, this information is specific to a node.
    pub node_id: NodeId,

    pub last_included: LogPosition,
    pub voters: Vec<NodeId>,
    pub new_voters: Vec<NodeId>,
    pub machine: serde_json::Value,
}

/// Parameters of [`Request::CreateCluster`].
#[derive(Debug, Serialize, Deserialize)]
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
pub struct TakeSnapshotResult {
    /// Log index where the snapshot was taken.
    pub snapshot_index: LogIndex,
}

/// Successful result of [`Request::GetServerState`].
#[derive(Debug, Serialize)]
#[allow(missing_docs)]
pub struct GetServerStateResult<'a, M> {
    pub addr: SocketAddr,
    pub node_id: Option<NodeId>,
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub role: &'static str,
    pub commit_index: LogIndex,
    pub snapshot: LogPosition,
    pub machines: &'a Machines<M>,
}

/// Parameters of [`Request::ProposeCommand`].
#[derive(Debug, Serialize, Deserialize)]
pub struct ProposeCommandParams {
    /// Proposed command.
    pub command: Command,

    // [NOTE]
    // This field can be omitted if the `Server` handles additional state for this,
    // albeit with a slight increase in complexity.
    /// Input for the associated query (if a command is associated, this value becomes `None`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_input: Option<serde_json::Value>,

    /// RPC caller that receives the output of the command result.
    pub caller: Caller,
}

/// Parameters of [`Request::NotifyPendingCommit`].
#[derive(Debug, Serialize, Deserialize)]
pub struct NotifyPendingCommitParams {
    /// Commit position.
    pub commit: LogPosition,

    // [NOTE]
    // This field can be omitted if the `Server` handles additional state for this,
    // albeit with a slight increase in complexity.
    /// Input for the associated query (if a command is associated, this value becomes `None`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<serde_json::Value>,

    /// RPC caller that receives the output of the associated command / query result.
    pub caller: Caller,
}

/// Parameters of [`Request::ReplyError`].
#[derive(Debug, Serialize, Deserialize)]
pub struct ReplyErrorParams {
    /// Error.
    pub error: ErrorReason,

    /// RPC caller that receives the output of the associated command / query result.
    pub caller: Caller,
}

/// Caller of an RPC request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(missing_docs)]
pub struct Caller {
    pub node_id: NodeId,
    pub process_id: u32,
    pub client_id: ClientId,
    pub request_id: RequestId,
}

impl Caller {
    pub(crate) const DUMMY_REQUEST_ID: RequestId = RequestId::Number(-1);
}

/// RPC error reason.
#[derive(Debug, Serialize, Deserialize)]
#[allow(missing_docs)]
pub enum ErrorReason {
    ClusterAlreadyCreated,
    ServerAlreadyAdded,
    NotClusterMember,
    InvalidMachineInput { reason: String },
    InvalidMachineOutput { reason: String },
    NoMachineOutput,
    RequestRejected,
    RequestResultUnknown,
    NoLeader,
}

#[allow(missing_docs)]
impl ErrorReason {
    pub const ERROR_CODE_CLUSTER_ALREADY_CREATED: ErrorCode = ErrorCode::new(100);
    pub const ERROR_CODE_SERVER_ALREADY_ADDED: ErrorCode = ErrorCode::new(101);
    pub const ERROR_CODE_NOT_CLUSTER_MEMBER: ErrorCode = ErrorCode::new(102);
    pub const ERROR_CODE_INVALID_MACHINE_INPUT: ErrorCode = ErrorCode::new(103);
    pub const ERROR_CODE_INVALID_MACHINE_OUTPUT: ErrorCode = ErrorCode::new(104);
    pub const ERROR_CODE_NO_MACHINE_OUTPUT: ErrorCode = ErrorCode::new(105);
    pub const ERROR_CODE_REQUEST_REJECTED: ErrorCode = ErrorCode::new(106);
    pub const ERROR_CODE_REQUEST_RESULT_UNKNOWN: ErrorCode = ErrorCode::new(107);
    pub const ERROR_CODE_NO_LEADER: ErrorCode = ErrorCode::new(108);

    fn code(&self) -> ErrorCode {
        match self {
            ErrorReason::ClusterAlreadyCreated => Self::ERROR_CODE_CLUSTER_ALREADY_CREATED,
            ErrorReason::ServerAlreadyAdded => Self::ERROR_CODE_SERVER_ALREADY_ADDED,
            ErrorReason::NotClusterMember => Self::ERROR_CODE_NOT_CLUSTER_MEMBER,
            ErrorReason::InvalidMachineInput { .. } => Self::ERROR_CODE_INVALID_MACHINE_INPUT,
            ErrorReason::InvalidMachineOutput { .. } => Self::ERROR_CODE_INVALID_MACHINE_OUTPUT,
            ErrorReason::NoMachineOutput => Self::ERROR_CODE_NO_MACHINE_OUTPUT,
            ErrorReason::RequestRejected => Self::ERROR_CODE_REQUEST_REJECTED,
            ErrorReason::RequestResultUnknown => Self::ERROR_CODE_REQUEST_RESULT_UNKNOWN,
            ErrorReason::NoLeader => Self::ERROR_CODE_NO_LEADER,
        }
    }

    fn message(&self) -> &'static str {
        match self {
            ErrorReason::ClusterAlreadyCreated => "Cluster already created",
            ErrorReason::ServerAlreadyAdded => "Server already added",
            ErrorReason::NotClusterMember => "Not a cluster member",
            ErrorReason::NoMachineOutput => "No machine output",
            ErrorReason::InvalidMachineInput { .. } => "Invalid machine input",
            ErrorReason::InvalidMachineOutput { .. } => "Invalid machine output",
            ErrorReason::RequestRejected => "Request rejected due to leader change",
            ErrorReason::RequestResultUnknown => {
                "Request result unknown (it could either be accepted or rejected)"
            }
            ErrorReason::NoLeader => "No leader in the cluster or not known by the server",
        }
    }

    pub(crate) fn object(&self) -> ErrorObject {
        ErrorObject {
            code: self.code(),
            message: self.message().to_owned(),
            data: self.data(),
        }
    }

    fn data(&self) -> Option<serde_json::Value> {
        match self {
            ErrorReason::InvalidMachineInput { reason }
            | ErrorReason::InvalidMachineOutput { reason } => {
                Some(serde_json::json!({"reason": reason}))
            }
            _ => None,
        }
    }
}
