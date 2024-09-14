use raftbare::NodeId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(into = "u64", from = "u64")]
pub struct NodeIdJson(pub NodeId);

impl From<NodeIdJson> for u64 {
    fn from(node_id: NodeIdJson) -> Self {
        node_id.0.get()
    }
}

impl From<u64> for NodeIdJson {
    fn from(node_id: u64) -> Self {
        NodeIdJson(NodeId::new(node_id))
    }
}
