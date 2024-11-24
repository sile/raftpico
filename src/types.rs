//! Basic types.
use serde::{Deserialize, Serialize};

/// Node identifier.
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
/// This struct is the same as [`mio::NodeId`], except that it is serializable.
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
