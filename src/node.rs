use std::{net::SocketAddr, time::Duration};

use mio::{net::TcpListener, Events, Interest, Poll, Token};
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

pub trait Machine: Serialize + for<'a> Deserialize<'a> {
    type Command: Serialize + for<'a> Deserialize<'a>;

    fn apply(&mut self, ctx: &MachineContext, command: Self::Command);
}

#[derive(Debug)]
pub struct Node<M> {
    inner: raftbare::Node,
    machine: M,
    listener: TcpListener,
    poller: Poll,
    events: Option<Events>,
    next_token: Token,
}

impl<M: Machine> Node<M> {
    pub const UNINIT_NODE_ID: NodeId = NodeId::new(u64::MAX);

    const SERVER_TOKEN: Token = Token(0);

    // TODO: io result ?
    pub fn new(addr: SocketAddr, machine: M) -> serde_json::Result<Self> {
        let mut listener = TcpListener::bind(addr).map_err(serde_json::Error::io)?;

        let poller = Poll::new().map_err(serde_json::Error::io)?;
        let events = Events::with_capacity(256); // TODO: configurable
        poller
            .registry()
            .register(&mut listener, Self::SERVER_TOKEN, Interest::READABLE)
            .map_err(serde_json::Error::io)?;
        Ok(Self {
            inner: raftbare::Node::start(Self::UNINIT_NODE_ID),
            machine,
            listener,
            poller,
            events: Some(events),
            next_token: Token(1),
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

    pub fn poller(&self) -> &Poll {
        &self.poller
    }

    pub fn poller_mut(&mut self) -> &mut Poll {
        &mut self.poller
    }

    // TODO: current_term(), etc

    pub fn machine(&self) -> &M {
        &self.machine
    }

    pub fn create_cluster(&mut self) -> bool {
        if self.id() != Self::UNINIT_NODE_ID {
            return false;
        }

        let node_id = NodeId::new(0);
        self.inner = raftbare::Node::start(node_id);
        let mut promise = self.inner.create_cluster(&[node_id]);
        promise.poll(&mut self.inner);
        assert!(!promise.is_pending());

        promise.is_accepted()
    }

    pub fn poll_one(&mut self, timeout: Option<Duration>) -> std::io::Result<bool> {
        let Some(mut events) = self.events.take() else {
            todo!();
        };
        self.poller.poll(&mut events, timeout)?;
        let mut did_something = false;

        for event in events.iter() {
            did_something = true;
            match event.token() {
                Self::SERVER_TOKEN => self.handle_listener()?,
                _ => todo!(),
            }
        }

        self.events = Some(events);
        Ok(did_something)
    }

    fn handle_listener(&mut self) -> std::io::Result<()> {
        loop {
            let Some((stream, _addr)) = would_block(self.listener.accept())? else {
                return Ok(());
            };
            todo!()
        }
    }
}

fn would_block<T>(result: std::io::Result<T>) -> std::io::Result<Option<T>> {
    match result {
        Ok(v) => Ok(Some(v)),
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orfail::OrFail;

    impl Machine for () {
        type Command = ();

        fn apply(&mut self, _ctx: &MachineContext, _command: Self::Command) {}
    }

    #[test]
    fn create_cluster() -> orfail::Result<()> {
        let mut node = Node::new(auto_addr(), ()).or_fail()?;
        assert_eq!(node.id(), Node::<()>::UNINIT_NODE_ID);

        node.create_cluster().or_fail()?;
        assert_eq!(node.id(), NodeId::new(0));

        Ok(())
    }

    fn auto_addr() -> SocketAddr {
        addr("127.0.0.1:0")
    }

    fn addr(s: &str) -> SocketAddr {
        s.parse().expect("parse addr")
    }
}
