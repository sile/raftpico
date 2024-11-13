use jsonlrpc::{JsonRpcVersion, RequestId};
use serde::{Deserialize, Serialize};

use crate::request::CreateClusterParams;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "method")]
pub enum Request {
    CreateCluster {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        #[serde(default)]
        params: CreateClusterParams,
    },
}
