use std::net::SocketAddr;

use jsonlrpc::{JsonRpcVersion, RequestId};
use serde::{Deserialize, Serialize};

use crate::server2::{ClusterSettings, Member};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "method")]
pub enum Request {
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
