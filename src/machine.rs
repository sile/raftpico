use raftbare::{Node, Role};
use serde::{Deserialize, Serialize};

use crate::{
    messages::{Caller, ErrorReason},
    types::LogIndex,
};

/// This trait represents a replicated state machine.
pub trait Machine: Default + Serialize + for<'de> Deserialize<'de> {
    /// Type of input that the machine can process.
    type Input: Serialize + for<'de> Deserialize<'de>;

    /// Applies the given input to the machine within the provided context,
    /// potentially altering its state deterministically if [`ApplyContext::kind()`] is [`ApplyKind::Command`].
    ///
    /// It is important to note that during the execution of this method,
    /// [`ApplyContext::output()`] must be called to return the output to the caller.
    fn apply(&mut self, ctx: &mut ApplyContext, input: Self::Input);
}

/// Context of a [`Machine::apply()`] call.
#[derive(Debug)]
pub struct ApplyContext<'a> {
    pub(crate) kind: ApplyKind,
    pub(crate) node: &'a Node,
    pub(crate) commit_index: LogIndex,
    pub(crate) output: Option<Result<serde_json::Value, ErrorReason>>,
    pub(crate) caller: Option<Caller>,
}

impl ApplyContext<'_> {
    /// Returns the kind of the apply call.
    pub fn kind(&self) -> ApplyKind {
        self.kind
    }

    /// Current role of the raft server.
    pub fn role(&self) -> Role {
        self.node.role()
    }

    /// Index of the committed log entry that contains the input.
    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }

    /// Notifies the output of the apply call to the raft server.
    pub fn output<T: Serialize>(&mut self, output: &T) {
        if self.caller.is_some() {
            self.output =
                Some(
                    serde_json::to_value(output).map_err(|e| ErrorReason::InvalidMachineOutput {
                        reason: e.to_string(),
                    }),
                )
        }
    }

    pub(crate) fn error(&mut self, error: ErrorReason) {
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
pub enum ApplyKind {
    /// Commands are executed on all servers in the cluster.
    Command,

    /// Queries, also known as consistent queries, are executed on the server that received the request.
    Query,

    /// Local queries are executed immediately on the server that received the request without guaranteeing that the state machine is up-to-date.
    ///
    /// However, it is guaranteed that the commit index is equal to or higher than
    /// the index associated with a previous command or consistent query proposed to the same server.
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
