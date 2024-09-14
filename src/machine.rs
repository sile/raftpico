use std::sync::mpsc;

use mio::Token;
use raftbare::{LogIndex, Node};
use serde::{Deserialize, Serialize};

pub trait Machine: Serialize + for<'de> Deserialize<'de> {
    type Input: Serialize + for<'de> Deserialize<'de>;

    fn handle_input(&mut self, ctx: &Context, from: From, input: &Self::Input);
}

#[derive(Debug)]
pub struct Context<'a> {
    kind: InputKind,
    node: &'a Node,
    machine_version: LogIndex,
}

impl<'a> Context<'a> {
    pub fn kind(&self) -> InputKind {
        self.kind
    }

    pub fn node(&self) -> &Node {
        self.node
    }

    pub fn machine_version(&self) -> LogIndex {
        self.machine_version
    }
}

type OutputSender = mpsc::Sender<(Token, serde_json::Result<serde_json::Value>)>;

#[derive(Debug)]
pub struct From(Option<(Token, OutputSender)>);

impl From {
    pub fn send_output<T: Serialize>(mut self, output: &T) {
        let Some((token, tx)) = self.0.take() else {
            // There is no need to send the output to a client from this Raft server.
            return;
        };

        let result = serde_json::to_value(output);
        let _ = tx.send((token, result));
    }

    pub fn is_null(&self) -> bool {
        self.0.is_none()
    }
}

impl Drop for From {
    fn drop(&mut self) {
        if let Some((token, tx)) = self.0.take() {
            let result = serde_json::to_value(&serde_json::Value::Null);
            let _ = tx.send((token, result));
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum InputKind {
    #[default]
    Command,
    Query,
    LocalQuery,
}

impl InputKind {
    pub fn is_command(self) -> bool {
        matches!(self, InputKind::Command)
    }

    pub fn is_query(self) -> bool {
        !matches!(self, InputKind::Command)
    }

    pub fn is_local(self) -> bool {
        matches!(self, InputKind::LocalQuery)
    }
}
