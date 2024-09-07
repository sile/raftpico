use std::{collections::BTreeMap, io::ErrorKind, net::SocketAddr, time::Duration};

use jsonlrpc::JsonlStream;
use mio::{
    net::{TcpListener, TcpStream},
    Events, Interest, Poll, Token,
};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionPhase {
    Connecting,
    Connected,
}

#[derive(Debug)]
pub struct Connection {
    stream: JsonlStream<TcpStream>,
    phase: ConnectionPhase,
}

impl Connection {
    pub fn new(stream: TcpStream, phase: ConnectionPhase) -> Self {
        Self {
            stream: JsonlStream::new(stream),
            phase,
        }
    }
}

#[derive(Debug)]
pub struct Node<M> {
    inner: raftbare::Node,
    machine: M,
    listener: TcpListener,
    local_addr: SocketAddr,
    poller: Poll,
    events: Option<Events>,
    connections: BTreeMap<Token, Connection>,
    addr_to_token: BTreeMap<SocketAddr, Token>,
}

impl<M: Machine> Node<M> {
    pub const UNINIT_NODE_ID: NodeId = NodeId::new(u64::MAX);
    pub const JOINING_NODE_ID: NodeId = NodeId::new(u64::MAX - 1);

    const SERVER_TOKEN: Token = Token(0);

    // TODO: io result ?
    pub fn new(addr: SocketAddr, machine: M) -> serde_json::Result<Self> {
        let mut listener = TcpListener::bind(addr).map_err(serde_json::Error::io)?;
        let local_addr = listener.local_addr().map_err(serde_json::Error::io)?;

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
            local_addr,
            poller,
            events: Some(events),
            connections: BTreeMap::new(),
            addr_to_token: BTreeMap::new(),
        })
    }

    pub fn id(&self) -> NodeId {
        self.inner.id()
    }

    pub fn addr(&self) -> SocketAddr {
        self.local_addr
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

    pub fn join(&mut self, contact_node_addr: SocketAddr) -> std::io::Result<()> {
        if self.id() == Self::JOINING_NODE_ID {
            return Err(std::io::Error::new(ErrorKind::InvalidInput, "Joining"));
        } else if self.id() != Self::UNINIT_NODE_ID {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                "Already initialized node",
            ));
        }

        self.inner = raftbare::Node::start(Self::JOINING_NODE_ID);

        self.send_message(contact_node_addr, &"join")?; // TODO
        Ok(())
    }

    fn send_message<T>(&mut self, dest: SocketAddr, message: &T) -> std::io::Result<()>
    where
        T: Serialize,
    {
        if !self.addr_to_token.contains_key(&dest) {
            self.connect(dest)?;
        }

        let Some(token) = self.addr_to_token.get(&dest).copied() else {
            unreachable!();
        };
        let Some(connection) = self.connections.get_mut(&token) else {
            unreachable!();
        };

        // TODO: max buffer size check
        // TODO: error handling
        let _ = connection.stream.write_object(message);

        Ok(())
    }

    fn connect(&mut self, addr: SocketAddr) -> std::io::Result<()> {
        let mut stream = TcpStream::connect(addr)?;
        let token = self.next_token();
        self.poller
            .registry()
            .register(&mut stream, token, Interest::WRITABLE)?;
        self.connections
            .insert(token, Connection::new(stream, ConnectionPhase::Connecting));
        self.addr_to_token.insert(addr, token);
        Ok(())
    }

    pub fn poll_one(&mut self, timeout: Option<Duration>) -> std::io::Result<bool> {
        let Some(mut events) = self.events.take() else {
            todo!();
        };
        self.poller.poll(&mut events, timeout)?;
        let mut did_something = false;

        for event in events.iter() {
            did_something = true;
            if event.token() == Self::SERVER_TOKEN {
                self.handle_listener()?;
            } else if let Some(stream) = self.connections.remove(&event.token()) {
                //TODO
                //   stream.handle_event(event, &mut self.inner);
                todo!();
            } else {
                todo!();
            }
        }

        self.events = Some(events);
        Ok(did_something)
    }

    fn next_token(&self) -> Token {
        let last = self
            .connections
            .last_key_value()
            .map(|(k, _)| *k)
            .unwrap_or(Self::SERVER_TOKEN);
        Token(last.0 + 1)
    }

    fn handle_listener(&mut self) -> std::io::Result<()> {
        loop {
            let Some((mut stream, _addr)) = would_block(self.listener.accept())? else {
                return Ok(());
            };
            let token = self.next_token();
            self.poller
                .registry()
                .register(&mut stream, token, Interest::READABLE)?;

            // TODO: handle initial read

            self.connections
                .insert(token, Connection::new(stream, ConnectionPhase::Connected));
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

    const POLL_TIMEOUT: Option<Duration> = Some(Duration::from_millis(5));

    #[test]
    fn create_cluster() -> orfail::Result<()> {
        let mut node = Node::new(auto_addr(), ()).or_fail()?;
        assert_eq!(node.id(), Node::<()>::UNINIT_NODE_ID);

        node.create_cluster().or_fail()?;
        assert_eq!(node.id(), NodeId::new(0));

        Ok(())
    }

    #[test]
    fn join() -> orfail::Result<()> {
        let mut node0 = Node::new(auto_addr(), ()).or_fail()?;
        node0.create_cluster().or_fail()?;

        let mut node1 = Node::new(auto_addr(), ()).or_fail()?;
        node1.join(node0.addr()).or_fail()?;
        assert_eq!(node1.id(), Node::<()>::JOINING_NODE_ID);

        while node1.id() == Node::<()>::JOINING_NODE_ID {
            node0.poll_one(POLL_TIMEOUT).or_fail()?;
            node1.poll_one(POLL_TIMEOUT).or_fail()?;
        }

        Ok(())
    }

    fn auto_addr() -> SocketAddr {
        addr("127.0.0.1:0")
    }

    fn addr(s: &str) -> SocketAddr {
        s.parse().expect("parse addr")
    }
}
