use raftbare::{NodeId, Role};
use serde::{Deserialize, Serialize};

pub trait Machine: Serialize + for<'a> Deserialize<'a> {
    type Command: Serialize + for<'a> Deserialize<'a>;

    fn apply(&mut self, command: Self::Command);
}

#[derive(Debug)]
pub struct Node<M> {
    inner: raftbare::Node,
    machine: M,
}

impl<M: Machine> Node<M> {
    pub const UNINIT_NODE_ID: NodeId = NodeId::new(u64::MAX);

    pub fn new(machine: M) -> Self {
        Self {
            inner: raftbare::Node::start(Self::UNINIT_NODE_ID),
            machine,
        }
    }

    pub fn id(&self) -> NodeId {
        self.inner.id()
    }

    pub fn role(&self) -> Role {
        self.inner.role()
    }

    // TODO: current_term(), etc

    pub fn machine(&self) -> &M {
        &self.machine
    }
}
