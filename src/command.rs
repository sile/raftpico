//! Raft command.
use std::net::SocketAddr;

use raftbare::LogIndex;
use serde::{Deserialize, Serialize};

#[cfg(doc)]
use crate::rpc::Request;
use crate::{
    rpc::{ClusterSettings, Proposer},
    server::Commands,
    types::{NodeId, Term},
};

/// List of commands that can be proposed to [`Server`][crate::Server].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(missing_docs)]
pub enum Command {
    /// A command proposed via [`Request::Apply`] API.
    Apply {
        input: serde_json::Value,
        proposer: Proposer,
    },

    /// A command proposed via [`Request::Apply`] API.
    Query,

    /// A command proposed via [`Request::CreateCluster`] API.
    CreateCluster {
        seed_addr: SocketAddr,
        settings: ClusterSettings,
        proposer: Proposer,
    },

    /// A command proposed via [`Request::AddServer`] API.
    AddServer {
        addr: SocketAddr,
        proposer: Proposer,
    },

    /// A command proposed via [`Request::RemoveServer`] API.
    RemoveServer {
        addr: SocketAddr,
        proposer: Proposer,
    },

    /// A command proposed via [`Request::TakeSnapshot`] API.
    TakeSnapshot { proposer: Proposer },

    /// A command proposed by `raftbare` (see: [`raftbare::LogEntry::Term`])
    StartTerm { term: Term },

    /// A command proposed by `raftbare` (see: [`raftbare::LogEntry::ClusterConfig`])
    UpdateClusterConfig {
        voters: Vec<NodeId>,
        new_voters: Vec<NodeId>,
    },
}

impl Command {
    pub(crate) fn proposer(&self) -> Option<&Proposer> {
        match self {
            Command::CreateCluster { proposer, .. } => Some(proposer),
            Command::AddServer { proposer, .. } => Some(proposer),
            Command::RemoveServer { proposer, .. } => Some(proposer),
            Command::TakeSnapshot { proposer, .. } => Some(proposer),
            Command::Apply { proposer, .. } => Some(proposer),
            Command::Query | Command::StartTerm { .. } | Command::UpdateClusterConfig { .. } => {
                None
            }
        }
    }

    // TODO: rename
    pub(crate) fn new(
        index: LogIndex,
        entry: &raftbare::LogEntry,
        commands: &Commands,
    ) -> Option<Self> {
        match entry {
            raftbare::LogEntry::Term(term) => Some(Self::StartTerm {
                term: Term::from(*term),
            }),
            raftbare::LogEntry::ClusterConfig(cluster_config) => Some(Self::UpdateClusterConfig {
                voters: cluster_config
                    .voters
                    .iter()
                    .copied()
                    .map(NodeId::from)
                    .collect(),
                new_voters: cluster_config
                    .new_voters
                    .iter()
                    .copied()
                    .map(NodeId::from)
                    .collect(),
            }),
            raftbare::LogEntry::Command => commands.get(&index.into()).cloned(),
        }
    }
}
