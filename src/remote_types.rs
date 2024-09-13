use mio::Token;
use raftbare::NodeId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(remote = "Token")]
pub struct TokenDef(usize);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(remote = "NodeId")]
pub struct NodeIdDef(#[serde(getter = "NodeIdDef::remote_id")] u64);

impl NodeIdDef {
    fn remote_id(remote: &NodeId) -> u64 {
        remote.get()
    }
}

impl From<NodeIdDef> for NodeId {
    fn from(node_id: NodeIdDef) -> Self {
        NodeId::new(node_id.0)
    }
}

// TODO
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeIds(pub Vec<u64>);

impl NodeIds {
    pub fn into_iter(self) -> impl Iterator<Item = NodeId> {
        self.0.into_iter().map(NodeId::new)
    }

    pub fn iter(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.0.iter().copied().map(NodeId::new)
    }
}

impl FromIterator<NodeId> for NodeIds {
    fn from_iter<T: IntoIterator<Item = NodeId>>(iter: T) -> Self {
        NodeIds(iter.into_iter().map(NodeId::get).collect())
    }
}
