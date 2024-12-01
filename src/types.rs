//! Basic types.
use std::collections::BinaryHeap;

use raftbare::{CommitStatus, Node};
use serde::{Deserialize, Serialize};

use crate::messages::ErrorReason;

/// Raft node identifier.
///
/// This struct is the same as [`raftbare::NodeId`], except that it is serializable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(into = "u64", from = "u64")]
pub struct NodeId(raftbare::NodeId);

impl NodeId {
    pub(crate) const SEED: Self = Self(raftbare::NodeId::new(0));
    pub(crate) const UNINIT: Self = Self(raftbare::NodeId::new(u64::MAX));
}

impl From<u64> for NodeId {
    fn from(value: u64) -> Self {
        Self(raftbare::NodeId::new(value))
    }
}

impl From<NodeId> for u64 {
    fn from(value: NodeId) -> Self {
        value.0.get()
    }
}

impl From<raftbare::NodeId> for NodeId {
    fn from(value: raftbare::NodeId) -> Self {
        Self(value)
    }
}

impl From<NodeId> for raftbare::NodeId {
    fn from(value: NodeId) -> Self {
        value.0
    }
}

/// `mio` Token.
///
/// This struct is the same as [`mio::Token`], except that it is serializable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(into = "usize", from = "usize")]
pub struct Token(mio::Token);

impl Token {
    pub(crate) const CLIENT_MIN: Self = Self(mio::Token(0));
    pub(crate) const CLIENT_MAX: Self = Self(mio::Token(1_000_000 - 1));

    pub(crate) const SERVER_MIN: Self = Self(mio::Token(Self::CLIENT_MAX.0 .0 + 1));
    pub(crate) const SERVER_MAX: Self = Self(mio::Token(usize::MAX));

    pub(crate) fn next_client_token(&mut self) -> Self {
        let token = *self;
        self.0 .0 += 1;
        if *self == Self::CLIENT_MAX {
            *self = Self::CLIENT_MIN;
        }
        token
    }
}

impl Default for Token {
    fn default() -> Self {
        Self::CLIENT_MIN
    }
}

impl From<usize> for Token {
    fn from(value: usize) -> Self {
        Self(mio::Token(value))
    }
}

impl From<Token> for usize {
    fn from(value: Token) -> Self {
        value.0 .0
    }
}

impl From<mio::Token> for Token {
    fn from(value: mio::Token) -> Self {
        Self(value)
    }
}

impl From<Token> for mio::Token {
    fn from(value: Token) -> Self {
        value.0
    }
}

/// Raft term.
///
/// This struct is the same as [`raftbare::Term`], except that it is serializable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(into = "u64", from = "u64")]
pub struct Term(raftbare::Term);

impl From<u64> for Term {
    fn from(value: u64) -> Self {
        Self(raftbare::Term::new(value))
    }
}

impl From<Term> for u64 {
    fn from(value: Term) -> Self {
        value.0.get()
    }
}

impl From<raftbare::Term> for Term {
    fn from(value: raftbare::Term) -> Self {
        Self(value)
    }
}

impl From<Term> for raftbare::Term {
    fn from(value: Term) -> Self {
        value.0
    }
}

/// Raft log index.
///
/// This struct is the same as [`raftbare::LogIndex`], except that it is serializable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(into = "u64", from = "u64")]
pub struct LogIndex(pub(crate) raftbare::LogIndex);

impl From<u64> for LogIndex {
    fn from(value: u64) -> Self {
        Self(raftbare::LogIndex::new(value))
    }
}

impl From<LogIndex> for u64 {
    fn from(value: LogIndex) -> Self {
        value.0.get()
    }
}

// TODO: remove
impl From<raftbare::LogIndex> for LogIndex {
    fn from(value: raftbare::LogIndex) -> Self {
        Self(value)
    }
}

impl From<LogIndex> for raftbare::LogIndex {
    fn from(value: LogIndex) -> Self {
        value.0
    }
}

/// Raft log position.
///
/// This struct is the same as [`raftbare::LogPosition`], except that it is serializable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LogPosition {
    pub term: Term,
    pub index: LogIndex,
}

impl From<raftbare::LogPosition> for LogPosition {
    fn from(value: raftbare::LogPosition) -> Self {
        Self {
            term: value.term.into(),
            index: value.index.into(),
        }
    }
}

impl From<LogPosition> for raftbare::LogPosition {
    fn from(value: LogPosition) -> Self {
        Self {
            term: value.term.into(),
            index: value.index.into(),
        }
    }
}

#[derive(Debug)]
struct PendingQueueItem<T> {
    commit: LogPosition,
    item: T,
}

impl<T> PartialEq for PendingQueueItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.commit == other.commit
    }
}

impl<T> Eq for PendingQueueItem<T> {}

impl<T> PartialOrd for PendingQueueItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for PendingQueueItem<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.commit.cmp(&other.commit).reverse()
    }
}

#[derive(Debug)]
pub(crate) struct PendingQueue<T> {
    queue: BinaryHeap<PendingQueueItem<T>>,
    is_command: bool,
}

impl<T> PendingQueue<T> {
    pub fn new(is_command: bool) -> Self {
        Self {
            queue: BinaryHeap::new(),
            is_command,
        }
    }

    pub fn push(&mut self, commit: LogPosition, item: T) {
        self.queue.push(PendingQueueItem { commit, item });
    }

    pub fn pop_committed(
        &mut self,
        node: &Node,
        commit_index: LogIndex,
    ) -> Option<(T, Option<ErrorReason>)> {
        let pending = self.queue.peek()?;
        match node.get_commit_status(pending.commit.into()) {
            CommitStatus::InProgress => None,
            CommitStatus::Rejected => {
                let item = self.queue.pop().expect("unreachable").item;
                Some((item, Some(ErrorReason::RequestRejected)))
            }
            CommitStatus::Unknown => {
                let item = self.queue.pop().expect("unreachable").item;
                Some((item, Some(ErrorReason::RequestResultUnknown)))
            }
            CommitStatus::Committed if self.is_command && pending.commit.index < commit_index => {
                // This commit (for a command) has already been applied.
                // Re-use `RequestResultUnknown` here, although it may be slightly inappropriate.
                let item = self.queue.pop().expect("unreachable").item;
                Some((item, Some(ErrorReason::RequestResultUnknown)))
            }
            CommitStatus::Committed if commit_index < pending.commit.index => None,
            CommitStatus::Committed => {
                let item = self.queue.pop().expect("unreachable").item;
                Some((item, None))
            }
        }
    }
}
