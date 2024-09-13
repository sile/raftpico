use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    System(SystemCommand),
    User(serde_json::Value),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SystemCommand {
    // InstallSnapshot
}
