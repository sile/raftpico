use serde::{Deserialize, Serialize};

// TODO: ServerStats?
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Stats {
    // Counters.
    pub poll_count: u64,
    pub election_timeout_set_count: u64,
    pub election_timeout_expired_count: u64,

    // Guages.
    pub node_available: bool,
}

impl Stats {
    pub fn new() -> Self {
        Self::default()
    }
}
