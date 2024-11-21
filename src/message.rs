use std::net::SocketAddr;

use jsonlrpc::{JsonRpcVersion, RequestId};
use raftbare::{
    ClusterConfig, LogEntries, LogIndex, LogPosition, MessageHeader, MessageSeqNo, NodeId, Term,
};
use serde::{Deserialize, Serialize};

use crate::{
    command::{Caller, Command2, LogEntry},
    server2::{ClusterSettings, Commands, Member, ServerInstanceId},
    InputKind,
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "method")]
pub enum Request {
    // External APIs
    CreateCluster {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        #[serde(default)]
        params: ClusterSettings,
    },
    AddServer {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        params: AddServerParams,
    },
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
    NotifyServerAddr {
        jsonrpc: JsonRpcVersion,
        params: NotifyServerAddrParams,
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
                    from: header.from.get(),
                    term: header.term.get(),
                    last_log_term: last_position.term.get(),
                    last_log_index: last_position.index.get(),
                },
            },
            raftbare::Message::AppendEntriesCall {
                header,
                commit_index,
                entries,
            } => {
                let params = AppendEntriesParams::new(header, commit_index, entries, commands)?;
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
                    from: header.from.get(),
                    term: header.term.get(),
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
                    from: header.from.get(),
                    term: header.term.get(),
                    last_log_term: last_position.term.get(),
                    last_log_index: last_position.index.get(),
                },
            },
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AppendEntriesResultParams {
    pub from: u64,
    pub term: u64,
    pub last_log_term: u64,
    pub last_log_index: u64,
}

impl AppendEntriesResultParams {
    pub fn into_raft_message(self, caller: &Caller) -> raftbare::Message {
        let RequestId::Number(request_id) = caller.request_id else {
            todo!("make this branch unreachable");
        };

        raftbare::Message::AppendEntriesReply {
            header: MessageHeader {
                from: NodeId::new(self.from),
                term: Term::new(self.term),
                seqno: MessageSeqNo::new(request_id as u64),
            },
            last_position: LogPosition {
                term: Term::new(self.last_log_term),
                index: LogIndex::new(self.last_log_index),
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RequestVoteParams {
    pub from: u64,
    pub term: u64,
    pub last_log_term: u64,
    pub last_log_index: u64,
}

impl RequestVoteParams {
    pub fn into_raft_message(self, caller: &Caller) -> raftbare::Message {
        let RequestId::Number(request_id) = caller.request_id else {
            todo!("make this branch unreachable");
        };

        raftbare::Message::RequestVoteCall {
            header: MessageHeader {
                from: NodeId::new(self.from),
                term: Term::new(self.term),
                seqno: MessageSeqNo::new(request_id as u64),
            },
            last_position: LogPosition {
                term: Term::new(self.last_log_term),
                index: LogIndex::new(self.last_log_index),
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RequestVoteResultParams {
    pub from: u64,
    pub term: u64,
    pub vote_granted: bool,
}

impl RequestVoteResultParams {
    pub fn into_raft_message(self, caller: &Caller) -> raftbare::Message {
        let RequestId::Number(request_id) = caller.request_id else {
            todo!("make this branch unreachable");
        };

        raftbare::Message::RequestVoteReply {
            header: MessageHeader {
                from: NodeId::new(self.from),
                term: Term::new(self.term),
                seqno: MessageSeqNo::new(request_id as u64),
            },
            vote_granted: self.vote_granted,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AppendEntriesParams {
    pub from: u64,
    pub term: u64,
    pub commit_index: u64,
    pub prev_term: u64,
    pub prev_log_index: u64,
    pub entries: Vec<LogEntry>,
}

impl AppendEntriesParams {
    fn new(
        header: MessageHeader,
        commit_index: LogIndex,
        entries: LogEntries,
        commands: &Commands,
    ) -> Option<Self> {
        Some(Self {
            from: header.from.get(),
            term: header.term.get(),
            commit_index: commit_index.get(),
            prev_term: entries.prev_position().term.get(),
            prev_log_index: entries.prev_position().index.get(),
            entries: entries
                .iter_with_positions()
                .map(|(p, x)| LogEntry::new(p.index, &x, commands))
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

        let prev_position = LogPosition {
            term: Term::new(self.prev_term),
            index: LogIndex::new(self.prev_log_index),
        };
        let entries = (1..)
            .map(|i| prev_position.index + LogIndex::new(i))
            .zip(self.entries.into_iter())
            .map(|(i, x)| match x {
                LogEntry::Term(v) => raftbare::LogEntry::Term(Term::new(v)),
                LogEntry::ClusterConfig { voters, new_voters } => {
                    raftbare::LogEntry::ClusterConfig(ClusterConfig {
                        voters: voters.into_iter().map(NodeId::new).collect(),
                        new_voters: new_voters.into_iter().map(NodeId::new).collect(),
                        ..ClusterConfig::default()
                    })
                }
                LogEntry::CreateCluster {
                    seed_server_addr,
                    settings,
                    proposer,
                } => {
                    commands.insert(
                        i,
                        Command2::CreateCluster {
                            seed_server_addr,
                            settings,
                            proposer,
                        },
                    );
                    raftbare::LogEntry::Command
                }
                LogEntry::AddServer {
                    server_addr,
                    proposer,
                } => {
                    commands.insert(
                        i,
                        Command2::AddServer {
                            server_addr,
                            proposer,
                        },
                    );
                    raftbare::LogEntry::Command
                }
                LogEntry::RemoveServer {
                    server_addr,
                    proposer,
                } => {
                    commands.insert(
                        i,
                        Command2::RemoveServer {
                            server_addr,
                            proposer,
                        },
                    );
                    raftbare::LogEntry::Command
                }
                LogEntry::ApplyCommand { input, proposer } => {
                    commands.insert(i, Command2::ApplyCommand { input, proposer });
                    raftbare::LogEntry::Command
                }
                LogEntry::ApplyQuery => {
                    commands.insert(i, Command2::ApplyQuery);
                    raftbare::LogEntry::Command
                }
            });
        let entries = LogEntries::from_iter(prev_position, entries);

        Some(raftbare::Message::AppendEntriesCall {
            header: MessageHeader {
                from: NodeId::new(self.from),
                term: Term::new(self.term),
                seqno: MessageSeqNo::new(request_id as u64),
            },
            commit_index: LogIndex::new(self.commit_index),
            entries,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
// TODO: #[serde(rename_all = "camelCase")]
pub struct AddServerParams {
    pub server_addr: SocketAddr,
}

#[derive(Debug, Serialize, Deserialize)]
// TODO: #[serde(rename_all = "camelCase")]
pub struct RemoveServerParams {
    pub server_addr: SocketAddr,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApplyParams {
    pub kind: InputKind,
    pub input: serde_json::Value,
    // [NOTE] Cannot use RawValue here: https://github.com/serde-rs/json/issues/545
    //
    // TODO: struct { jsonrpc, method, id, params: RawValue } then serde_json::from_str(RawValue.get())
    //       (use RawValue in jsonlrpc_mio(?))
    // pub input: Box<RawValue>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProposeParams {
    pub command: Command2,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProposeQueryParams {
    pub origin_node_id: u64,
    pub caller: Caller,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NotifyQueryPromiseParams {
    pub promise_term: u64,
    pub promise_log_index: u64,
    pub caller: Caller,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InitNodeParams {
    pub node_id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NotifyServerAddrParams {
    pub node_id: u64,
    pub addr: SocketAddr,
}

// TODO: move
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Proposer {
    pub server: ServerInstanceId,
    pub client: Caller, // TODO: rename
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateClusterOutput {
    pub members: Vec<Member>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddServerOutput {
    pub members: Vec<Member>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RemoveServerOutput {
    pub members: Vec<Member>,
}
