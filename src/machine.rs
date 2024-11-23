use jsonlrpc::ErrorObject;
use raftbare::{LogIndex, Node};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::{command::Caller, server::ErrorKind};

pub trait Machine: Default + Serialize + for<'de> Deserialize<'de> {
    type Input: Serialize + for<'de> Deserialize<'de>;

    fn apply(&mut self, ctx: &mut Context, input: &Self::Input);
}

#[derive(Debug)]
pub struct Context<'a> {
    pub kind: InputKind, // TODO: private
    pub node: &'a Node,
    pub commit_index: LogIndex,

    pub(crate) output: Option<Result<Box<RawValue>, ErrorObject>>,
    pub(crate) caller: Option<Caller>,
}

impl<'a> Context<'a> {
    pub fn output<T: Serialize>(&mut self, output: &T) {
        if self.caller.is_some() {
            match serde_json::value::to_raw_value(output) {
                Ok(t) => self.output = Some(Ok(t)),
                Err(e) => {
                    self.output = Some(Err(ErrorKind::MalformedMachineOutput.object_with_reason(e)))
                }
            }
        }
    }

    pub fn error(&mut self, error: ErrorObject) {
        if self.caller.is_some() {
            self.output = Some(Err(error));
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
