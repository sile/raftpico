use mio::Token;
use raftbare::NodeId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(into = "usize", from = "usize")]
pub struct TokenJson(pub Token);

impl From<TokenJson> for usize {
    fn from(token: TokenJson) -> Self {
        token.0 .0
    }
}

impl From<usize> for TokenJson {
    fn from(token: usize) -> Self {
        TokenJson(Token(token))
    }
}

// TODO: remove
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
