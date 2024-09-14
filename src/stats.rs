use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Stats {
    pub server: ServerStats,
    pub node: Option<RaftNodeStats>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ServerStats {
    pub poll_count: u64,
    pub election_timeout_set_count: u64,
    pub election_timeout_expired_count: u64,
    pub node: Option<RaftNodeStats>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RaftNodeStats {
    pub id: u64,
    pub current_term: u64,
    pub commit_index: u64,
    pub last_applied_index: u64,
    // etc
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ConnectionStats {
    pub recv_bytes: u64,
    pub sent_bytes: u64,
}
