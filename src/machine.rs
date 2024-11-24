use jsonlrpc::ErrorObject;
use raftbare::Node;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::{command::Caller, rpc::ErrorKind, types::LogIndex};

pub trait Machine: Default + Serialize + for<'de> Deserialize<'de> {
    type Input: Serialize + for<'de> Deserialize<'de>;

    fn apply(&mut self, ctx: &mut Context, input: &Self::Input);
}

#[derive(Debug)]
pub struct Context<'a> {
    pub kind: ApplyKind, // TODO: private
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

/// This enum specifies how to execute [`Machine::apply()`].
///
/// If the kind is not [`ApplyKind::Command`],
/// the state machine must not be updated while executing [`Machine::apply()`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ApplyKind {
    /// Commands are executed on all servers in the cluster.
    Command,

    /// Queries, also known as consistent queries, are executed on the server that received the request.
    Query,

    /// Local queries are executed immediately on the server that received the request without guaranteeing that the state machine is up-to-date.
    LocalQuery,
}

impl ApplyKind {
    /// Returns `true` if the kind is [`ApplyKind::Command`].
    pub const fn is_command(self) -> bool {
        matches!(self, Self::Command)
    }

    /// Returns `true` if the kind is [`ApplyKind::Query`].
    pub const fn is_query(self) -> bool {
        matches!(self, Self::Query)
    }

    /// Returns `true` if the kind is [`ApplyKind::LocalQuery`].
    pub const fn is_local_query(self) -> bool {
        matches!(self, Self::LocalQuery)
    }
}
