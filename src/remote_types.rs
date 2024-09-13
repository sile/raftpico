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
