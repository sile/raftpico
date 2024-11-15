use raftbare::{LogIndex, Node};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::command::Caller;

pub trait Machine2: Serialize + for<'de> Deserialize<'de> {
    type Input: Serialize + for<'de> Deserialize<'de>;

    fn apply(&mut self, ctx: &mut Context2, input: Self::Input);
}

pub trait Machine: Serialize + for<'de> Deserialize<'de> {
    type Input: Serialize + for<'de> Deserialize<'de>;

    fn handle_input(&mut self, ctx: &mut Context, input: Self::Input);
}

#[derive(Debug)]
pub struct Context2<'a> {
    pub kind: InputKind, // TODO: private
    pub node: &'a Node,
    pub commit_index: LogIndex,

    pub(crate) output: Option<serde_json::Result<Box<RawValue>>>,
    pub(crate) caller: Option<Caller>,
}

impl<'a> Context2<'a> {
    pub fn output<T: Serialize>(&mut self, output: &T) {
        if self.caller.is_some() {
            self.output = Some(serde_json::value::to_raw_value(output));
        }
    }
}

#[derive(Debug)]
pub struct Context<'a> {
    pub(crate) kind: InputKind,
    pub(crate) node: &'a Node,
    pub(crate) machine_version: LogIndex,

    pub(crate) output: Option<serde_json::Result<serde_json::Value>>,
    pub(crate) ignore_output: bool,
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

    pub fn output<T: Serialize>(&mut self, output: &T) {
        if !self.ignore_output && self.output.is_none() {
            self.output = Some(serde_json::to_value(output));
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
