//! Raft command.
use std::{collections::BTreeMap, net::SocketAddr};

use serde::{Deserialize, Serialize};

#[cfg(doc)]
use crate::messages::Request;
use crate::{
    messages::CreateClusterParams,
    types::{LogIndex, NodeId, Term},
};

pub(crate) type Commands = BTreeMap<LogIndex, Command>;

/// Commmand that can be proposed to [`Server`][crate::Server].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(missing_docs)]
pub enum Command {
    /// A command proposed via [`Request::CreateCluster`] API.
    CreateCluster {
        seed_addr: SocketAddr,
        min_election_timeout_ms: u32,
        max_election_timeout_ms: u32,
    },

    /// A command proposed via [`Request::AddServer`] API.
    AddServer { addr: SocketAddr },

    /// A command proposed via [`Request::RemoveServer`] API.
    RemoveServer { addr: SocketAddr },

    /// A command proposed via [`Request::TakeSnapshot`] API.
    TakeSnapshot,

    /// A command proposed via [`Request::Apply`] API.
    Apply { input: serde_json::Value },

    /// A command proposed via [`Request::Apply`] API.
    Query,

    /// A command proposed by `raftbare` (see: [`raftbare::LogEntry::Term`])
    StartTerm { term: Term },

    /// A command proposed by `raftbare` (see: [`raftbare::LogEntry::ClusterConfig`])
    UpdateClusterConfig {
        voters: Vec<NodeId>,
        new_voters: Vec<NodeId>,
    },
}

impl Command {
    pub(crate) fn create_cluster(seed_addr: SocketAddr, params: &CreateClusterParams) -> Self {
        Self::CreateCluster {
            seed_addr,
            min_election_timeout_ms: params.min_election_timeout_ms,
            max_election_timeout_ms: params.max_election_timeout_ms,
        }
    }

    pub(crate) fn add_server(addr: SocketAddr) -> Self {
        Self::AddServer { addr }
    }

    pub(crate) fn remove_server(addr: SocketAddr) -> Self {
        Self::RemoveServer { addr }
    }

    pub(crate) fn apply(input: serde_json::Value) -> Self {
        Self::Apply { input }
    }

    pub(crate) fn will_change_member(&self) -> bool {
        matches!(
            self,
            Self::CreateCluster { .. } | Self::AddServer { .. } | Self::RemoveServer { .. }
        )
    }

    pub(crate) fn from_log_entry(
        index: LogIndex,
        entry: &raftbare::LogEntry,
        commands: &Commands,
    ) -> Self {
        match entry {
            raftbare::LogEntry::Term(term) => Self::StartTerm { term: Term(*term) },
            raftbare::LogEntry::ClusterConfig(config) => Self::UpdateClusterConfig {
                voters: config.voters.iter().copied().map(NodeId).collect(),
                new_voters: config.new_voters.iter().copied().map(NodeId).collect(),
            },
            raftbare::LogEntry::Command => commands.get(&index).expect("bug").clone(),
        }
    }

    pub(crate) fn into_log_entry(
        self,
        index: LogIndex,
        commands: &mut Commands,
    ) -> raftbare::LogEntry {
        match self {
            Command::StartTerm { term } => raftbare::LogEntry::Term(term.0),
            Command::UpdateClusterConfig { voters, new_voters } => {
                raftbare::LogEntry::ClusterConfig(raftbare::ClusterConfig {
                    voters: voters.into_iter().map(|n| n.0).collect(),
                    new_voters: new_voters.into_iter().map(|n| n.0).collect(),
                    ..Default::default()
                })
            }
            command => {
                commands.insert(index, command);
                raftbare::LogEntry::Command
            }
        }
    }
}
