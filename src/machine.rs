use jsonlrpc::ErrorObject;
use raftbare::{Node, Role};
use serde::{Deserialize, Serialize};

use crate::{command::Caller, rpc::ErrorKind, types::LogIndex};

/// This trait represents a replicated state machine.
pub trait Machine: Default + Serialize + for<'de> Deserialize<'de> {
    /// Type of input that the machine can process.
    type Input: Serialize + for<'de> Deserialize<'de>;

    /// Applies the given input to the machine within the provided context,
    /// potentially altering its state if [`ApplyContext::kind()`] is [`ApplyKind::Command`].
    ///
    /// It is important to note that during the execution of this method,
    /// [`ApplyContext::output()`] or [`ApplyContext::error()`] must be called to return the output to the caller.
    fn apply(&mut self, ctx: &mut ApplyContext, input: &Self::Input);
}

#[derive(Debug)]
pub struct ApplyContext<'a> {
    pub(crate) kind: ApplyKind,
    pub(crate) node: &'a Node,
    pub(crate) commit_index: LogIndex,
    pub(crate) output: Option<Result<serde_json::Value, ErrorObject>>,
    pub(crate) caller: Option<Caller>,
}

impl<'a> ApplyContext<'a> {
    pub fn kind(&self) -> ApplyKind {
        self.kind
    }

    pub fn role(&self) -> Role {
        self.node.role()
    }

    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }

    pub fn output<T: Serialize>(&mut self, output: &T) {
        if self.caller.is_some() {
            match serde_json::to_value(output) {
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
