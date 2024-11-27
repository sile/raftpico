//! RPC messages.
use std::net::SocketAddr;

use jsonlrpc::{ErrorCode, ErrorObject, JsonRpcVersion, RequestId};
use jsonlrpc_mio::ClientId;
use raftbare::{ClusterConfig, LogEntries};
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
            } => Self::RequestVoteCall {
                jsonrpc: JsonRpcVersion::V2,
                params: RequestVoteCallParams {
                    header: RaftMessageHeader::from_raftbare_header(header),
                    last_log_position: last_position.into(),
                },
            },
            raftbare::Message::AppendEntriesCall {
                header,
                commit_index,
                entries,
            } => Self::AppendEntriesCall {
                jsonrpc: JsonRpcVersion::V2,
                params: AppendEntriesCallParams::new(
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
                    header: RaftMessageHeader::from_raftbare_header(header),
                    vote_granted,
                },
            },
            raftbare::Message::AppendEntriesReply {
                header,
                last_position,
            } => Self::AppendEntriesReply {
                jsonrpc: JsonRpcVersion::V2,
                params: AppendEntriesReplyParams {
                    header: RaftMessageHeader::from_raftbare_header(header),
                    last_log_position: last_position.into(),
                },
            },
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AppendEntriesReplyParams {
    pub header: RaftMessageHeader,
    pub last_log_position: LogPosition,
}

impl AppendEntriesReplyParams {
    pub fn into_raft_message(self) -> raftbare::Message {
        raftbare::Message::AppendEntriesReply {
            header: self.header.to_raftbare_header(),
            last_position: self.last_log_position.into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RequestVoteCallParams {
    pub header: RaftMessageHeader,
    pub last_log_position: LogPosition,
}

impl RequestVoteCallParams {
    pub fn into_raft_message(self) -> raftbare::Message {
        raftbare::Message::RequestVoteCall {
            header: self.header.to_raftbare_header(),
            last_position: self.last_log_position.into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RequestVoteReplyParams {
    pub header: RaftMessageHeader,
    pub vote_granted: bool,
}

impl RequestVoteReplyParams {
    pub fn into_raft_message(self) -> raftbare::Message {
        raftbare::Message::RequestVoteReply {
            header: self.header.to_raftbare_header(),
            vote_granted: self.vote_granted,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RaftMessageHeader {
    pub from: NodeId,
    pub term: Term,
    pub seqno: u64,
}

impl RaftMessageHeader {
    fn from_raftbare_header(header: raftbare::MessageHeader) -> Self {
        Self {
            from: header.from.into(),
            term: header.term.into(),
            seqno: header.seqno.get(),
        }
    }

    fn to_raftbare_header(&self) -> raftbare::MessageHeader {
        raftbare::MessageHeader {
            from: self.from.into(),
            term: self.term.into(),
            seqno: raftbare::MessageSeqNo::new(self.seqno),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AppendEntriesCallParams {
    pub header: RaftMessageHeader,
    pub commit_index: LogIndex,
    pub prev_position: LogPosition,
    pub entries: Vec<Command>,
}

impl AppendEntriesCallParams {
    fn new(
        header: raftbare::MessageHeader,
        commit_index: LogIndex,
        entries: LogEntries,
        commands: &Commands,
    ) -> Option<Self> {
        Some(Self {
            header: RaftMessageHeader::from_raftbare_header(header),
            commit_index,
            prev_position: entries.prev_position().into(),
            entries: entries
                .iter_with_positions()
                .map(|(p, x)| Command::new(p.index, &x, commands))
                .collect::<Option<Vec<_>>>()?,
        })
    }

    pub fn into_raft_message(self, commands: &mut Commands) -> Option<raftbare::Message> {
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
            header: self.header.to_raftbare_header(),
            commit_index: self.commit_index.into(),
            entries,
        })
    }
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
