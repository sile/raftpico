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
