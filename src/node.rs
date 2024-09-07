use std::net::{SocketAddr, TcpListener};

use mio::Poll;
use raftbare::{NodeId, Role};
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct MachineContext {}

impl MachineContext {
    pub fn reply<T>(&self, _reply: &T)
    where
        T: Serialize,
    {
    }
}

pub trait Machine: Default + Serialize + for<'a> Deserialize<'a> {
    type Command: Serialize + for<'a> Deserialize<'a>;

    fn apply(&mut self, ctx: &MachineContext, command: Self::Command);
}

#[derive(Debug)]
pub struct Node<M> {
    inner: raftbare::Node,
    machine: M,
    listener: TcpListener,
    poll: Poll,
}

impl<M: Machine> Node<M> {
    pub const UNINIT_NODE_ID: NodeId = NodeId::new(u64::MAX);

    pub fn new(addr: SocketAddr) -> serde_json::Result<Self> {
        let listener = TcpListener::bind(addr).map_err(serde_json::Error::io)?;
        listener
            .set_nonblocking(true)
            .map_err(serde_json::Error::io)?;

        Ok(Self {
            inner: raftbare::Node::start(Self::UNINIT_NODE_ID),
            machine: M::default(),
            listener,
            poll: Poll::new().map_err(serde_json::Error::io)?,
        })
    }

    pub fn id(&self) -> NodeId {
        self.inner.id()
    }

    pub fn role(&self) -> Role {
        self.inner.role()
    }

    pub fn listener(&self) -> &TcpListener {
        &self.listener
    }

    pub fn poll(&self) -> &Poll {
        &self.poll
    }

    pub fn poll_mut(&mut self) -> &mut Poll {
        &mut self.poll
    }

    // TODO: current_term(), etc

    pub fn machine(&self) -> &M {
        &self.machine
    }
}
