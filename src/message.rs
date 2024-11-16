use std::net::SocketAddr;

use jsonlrpc::{JsonRpcVersion, RequestId};
use raftbare::{LogEntries, LogIndex, MessageHeader};
use serde::{Deserialize, Serialize};

use crate::{
    command::LogEntry,
    server2::{ClusterSettings, Commands, Member},
};

// TODO: note doc
const RAFT_REQUEST_ID: RequestId = RequestId::Number(-1);

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
    // Internal messages
    // Raft messages
    AppendEntries {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        params: AppendEntriesParams,
    },
}

impl Request {
    pub(crate) fn from_raft_message(
        message: raftbare::Message,
        commands: &Commands,
    ) -> Option<Self> {
        match message {
            raftbare::Message::RequestVoteCall {
                header,
                last_position,
            } => todo!(),
            raftbare::Message::AppendEntriesCall {
                header,
                commit_index,
                entries,
            } => {
                let params = AppendEntriesParams::new(header, commit_index, entries, commands)?;
                Some(Self::AppendEntries {
                    jsonrpc: JsonRpcVersion::V2,
                    id: RAFT_REQUEST_ID,
                    params,
                })
            }
            _ => {
                unreachable!();
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AppendEntriesParams {
    pub from: u64,
    pub term: u64,
    pub seqno: u64,
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
            seqno: header.seqno.get(),
            commit_index: commit_index.get(),
            prev_term: entries.prev_position().term.get(),
            prev_log_index: entries.prev_position().index.get(),
            entries: entries
                .iter_with_positions()
                .map(|(p, x)| LogEntry::new(p.index, &x, commands))
                .collect::<Option<Vec<_>>>()?,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
// TODO: #[serde(rename_all = "camelCase")]
pub struct AddServerParams {
    pub server_addr: SocketAddr,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateClusterOutput {
    pub members: Vec<Member>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddServerOutput {
    pub members: Vec<Member>,
}
