use std::{
    collections::{BTreeMap, VecDeque},
    io::ErrorKind,
    net::SocketAddr,
    time::Duration,
};

use jsonlrpc::{JsonRpcVersion, JsonlStream};
use mio::{
    event::Event,
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
    pub is_client: bool,
    pub ongoing_requests: VecDeque<TaggedMessage>,
}

impl Connection {
    pub fn new(stream: TcpStream, phase: ConnectionPhase) -> Self {
        Self {
            stream: JsonlStream::new(stream),
            phase,
            is_client: phase == ConnectionPhase::Connecting,
            ongoing_requests: VecDeque::new(),
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
    next_request_id: u64,
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
            next_request_id: 0,
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

    // TODO: priv
    pub fn next_request_id(&mut self) -> u64 {
        let id = self.next_request_id;
        self.next_request_id += 1;
        id
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

        let request = TaggedMessage::join(self.next_request_id(), self.addr());
        self.send_message(contact_node_addr, request)?; // TODO

        Ok(())
    }

    fn send_message(&mut self, dest: SocketAddr, message: TaggedMessage) -> std::io::Result<()> {
        if !self.addr_to_token.contains_key(&dest) {
            self.connect(dest)?;
        }

        let Some(token) = self.addr_to_token.get(&dest).copied() else {
            unreachable!();
        };
        let Some(conn) = self.connections.get_mut(&token) else {
            unreachable!();
        };

        // TODO: max buffer size check
        // TODO: error handling
        let _ = conn.stream.write_object(&message);
        if !conn.stream.write_buf().is_empty() {
            // TODO
            self.poller.registry().reregister(
                conn.stream.inner_mut(),
                token,
                Interest::WRITABLE,
            )?;
        }

        if !message.is_notification() {
            conn.ongoing_requests.push_back(message);
        }

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
            } else if let Some(connection) = self.connections.remove(&event.token()) {
                self.handle_connection_event(connection, event)?;
            } else {
                todo!();
            }
        }

        self.events = Some(events);
        Ok(did_something)
    }

    fn handle_connection_event(
        &mut self,
        mut conn: Connection,
        event: &Event,
    ) -> std::io::Result<()> {
        match conn.phase {
            ConnectionPhase::Connecting => {
                if let Some(e) = conn.stream.inner().take_error().unwrap_or_else(|e| Some(e)) {
                    // TODO: notify error
                    eprintln!("Deregister. Error: {:?}", e);
                    self.poller.registry().deregister(conn.stream.inner_mut())?;
                    return Ok(());
                }

                match conn.stream.inner().peer_addr() {
                    Ok(_) => {
                        eprintln!("Connected to {:?}", conn.stream.inner().peer_addr()); // TODO
                        conn.phase = ConnectionPhase::Connected;
                    }
                    Err(e) if e.kind() == ErrorKind::NotConnected => {
                        self.connections.insert(event.token(), conn);
                        return Ok(());
                    }
                    Err(e) => {
                        eprintln!("Deregister2. Error: {:?}", e);
                        self.poller.registry().deregister(conn.stream.inner_mut())?;
                        return Ok(());
                    }
                }
            }
            ConnectionPhase::Connected => {}
        }

        // Write.
        if !conn.stream.write_buf().is_empty() {
            would_block(conn.stream.flush().map_err(|e| e.into()))
                .unwrap_or_else(|e| todo!("{:?}", e));
            if conn.stream.write_buf().is_empty() {
                // TODO: or deregister?
                self.poller.registry().reregister(
                    conn.stream.inner_mut(),
                    event.token(),
                    Interest::READABLE,
                )?;
            }
        }

        // Read.
        let message = match conn.ongoing_requests.front() {
            None => would_block(
                conn.stream
                    .read_object::<TaggedMessage>()
                    .map(Message::Tagged)
                    .map_err(|e| e.into()),
            ),
            Some(TaggedMessage::Join { .. }) => {
                // TODO: check id
                would_block(
                    conn.stream
                        .read_object::<Response<JoinResult>>()
                        .map(Message::JoinResponse)
                        .map_err(|e| e.into()),
                )
            }
        };

        if let Ok(Some(message)) = message {
            todo!("MESSAGE {:?}", message);
        }

        self.connections.insert(event.token(), conn);
        Ok(())
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

// TODO: remove
#[derive(Debug, Clone)]
pub enum Message {
    Tagged(TaggedMessage),
    JoinResponse(Response<JoinResult>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response<T> {
    pub jsonrpc: JsonRpcVersion,
    pub id: u64,
    pub result: T,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinResult {
    pub promise: CommitPromiseObject,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitPromiseObject {
    pub term: u64,
    pub index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "method")]
pub enum TaggedMessage {
    Join {
        jsonrpc: JsonRpcVersion,
        id: u64,
        params: JoinParams,
    },
}

impl TaggedMessage {
    pub fn is_notification(&self) -> bool {
        false
    }

    pub fn join(id: u64, new_node_addr: SocketAddr) -> Self {
        Self::Join {
            jsonrpc: JsonRpcVersion::V2,
            id,
            params: JoinParams { new_node_addr },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinParams {
    pub new_node_addr: SocketAddr,
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
