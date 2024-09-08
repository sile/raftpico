use std::{
    collections::{BTreeMap, VecDeque},
    io::ErrorKind,
    net::SocketAddr,
    time::{Duration, Instant},
};

use jsonlrpc::{JsonRpcVersion, JsonlStream, RequestId, RpcClient};
use mio::{
    event::Event,
    net::{TcpListener, TcpStream},
    Events, Interest, Poll, Token,
};
use raftbare::{Action, CommitPromise, LogIndex, Role};
use rand::Rng;
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
    pub ongoing_proposes: VecDeque<(RequestId, ProposeParams)>,
}

impl Connection {
    pub fn new(stream: TcpStream, phase: ConnectionPhase) -> Self {
        Self {
            stream: JsonlStream::new(stream),
            phase,
            is_client: phase == ConnectionPhase::Connecting,
            ongoing_proposes: VecDeque::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(from = "u64", into = "u64")]
pub struct NodeId(raftbare::NodeId);

impl NodeId {
    pub const fn new(id: u64) -> Self {
        Self(raftbare::NodeId::new(id))
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl From<u64> for NodeId {
    fn from(id: u64) -> Self {
        Self::new(id)
    }
}

impl From<NodeId> for u64 {
    fn from(id: NodeId) -> Self {
        id.get()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Command {
    Join {
        id: Option<NodeId>,
        addr: SocketAddr,
    },
    UserCommand(serde_json::Value),
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SystemMachine {
    pub members: BTreeMap<SocketAddr, NodeId>,
}

// TODO: RaftClient
#[derive(Debug, Clone)]
pub struct NodeHandle {
    addr: SocketAddr,
    // TODO: _command: PhantomData<M::Command>,
    // TODO: _query: PhantomData<M::Query>,
}

impl NodeHandle {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    // TODO: return error object instead of false?
    pub fn create_cluster(&self) -> std::io::Result<bool> {
        let stream = std::net::TcpStream::connect(self.addr)?;
        let mut client = RpcClient::new(stream); // TODO: reuse client

        let response: Response<bool> = client.call(&Message::CreateCluster {
            jsonrpc: JsonRpcVersion::V2,
            id: RequestId::Number(0),
        })?;
        Ok(response.result)
    }

    pub fn join(&self, contact_addr: SocketAddr) -> std::io::Result<bool> {
        let stream = std::net::TcpStream::connect(self.addr)?;

        // TODO:
        stream.set_read_timeout(Some(Duration::from_secs(1)))?;

        let mut client = RpcClient::new(stream); // TODO: reuse client

        let response: Response<bool> = client.call(&Message::Join {
            jsonrpc: JsonRpcVersion::V2,
            id: RequestId::Number(0),
            params: JoinParams { contact_addr },
        })?;
        Ok(response.result)
    }
}

// TODO: RaftServer
#[derive(Debug)]
pub struct Node<M> {
    inner: raftbare::Node,
    instance_id: u64,
    machine: M,
    listener: TcpListener,
    local_addr: SocketAddr,
    poller: Poll,
    events: Option<Events>,
    connections: BTreeMap<Token, Connection>,
    addr_to_token: BTreeMap<SocketAddr, Token>,
    next_request_id: u64,
    command_log: BTreeMap<LogIndex, Command>,
    last_applied: LogIndex,
    pub system_machine: SystemMachine, // TODO
    election_timeout_time: Option<Instant>,
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
            inner: raftbare::Node::start(Self::UNINIT_NODE_ID.0),
            instance_id: rand::random(),
            machine,
            listener,
            local_addr,
            poller,
            events: Some(events),
            connections: BTreeMap::new(),
            addr_to_token: BTreeMap::new(),
            next_request_id: 0,
            command_log: BTreeMap::new(),
            last_applied: LogIndex::ZERO,
            system_machine: SystemMachine::default(),
            election_timeout_time: None,
        })
    }

    pub fn id(&self) -> NodeId {
        NodeId::new(self.inner.id().get())
    }

    pub fn addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn handle(&self) -> NodeHandle {
        NodeHandle::new(self.addr())
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
    pub fn next_request_id(&mut self) -> RequestId {
        let id = self.next_request_id;
        self.next_request_id += 1;
        RequestId::Number(id as i64) // TODO
    }

    fn propose_command(&mut self, command: Command) -> CommitPromise {
        let promise = self.inner.propose_command();
        if !promise.is_rejected() {
            self.command_log
                .insert(promise.log_position().index, command);
        }
        promise
    }

    fn send_message(&mut self, dest: SocketAddr, message: Message) -> std::io::Result<()> {
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

        // TODO
        if let Message::Propose { id, params, .. } = message {
            conn.ongoing_proposes.push_back((id, params));
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

    fn handle_action(&mut self, action: Action) -> std::io::Result<()> {
        match action {
            Action::SetElectionTimeout => self.handle_set_election_timeout(),
            Action::SaveCurrentTerm | Action::SaveVotedFor | Action::AppendLogEntries(_) => {
                // Do nothing as this crate uses in-memory storage.
            }
            Action::BroadcastMessage(_) => todo!(),
            Action::SendMessage(_, _) => todo!(),
            Action::InstallSnapshot(_) => todo!(),
        }
        Ok(())
    }

    fn handle_set_election_timeout(&mut self) {
        // TODO: configurable election timeout
        let min = Duration::from_millis(100);
        let max = Duration::from_millis(1000);
        let timeout = match self.role() {
            Role::Follower => max,
            Role::Candidate => rand::thread_rng().gen_range(min..max),
            Role::Leader => min,
        };
        self.election_timeout_time = Some(Instant::now() + timeout);
    }

    fn apply_log_entry(&mut self, index: LogIndex) -> std::io::Result<()> {
        let Some(command) = self.command_log.get(&index) else {
            return Ok(());
        };
        match command {
            Command::Join { id, addr } => {
                // TODO: addr duplicate check
                let id = id.clone().unwrap_or(NodeId::new(index.get()));
                self.system_machine.members.insert(*addr, id);
            }
            Command::UserCommand(_) => todo!(),
        }
        Ok(())
    }

    pub fn poll_one(&mut self, timeout: Option<Duration>) -> std::io::Result<bool> {
        // TODO: ajust timeout based on election_timeout_time

        let now = Instant::now();
        if self
            .election_timeout_time
            .take_if(|time| *time <= now)
            .is_some()
        {
            self.inner.handle_election_timeout();
        }

        while let Some(action) = self.inner.actions_mut().next() {
            self.handle_action(action)?;
        }
        while self.last_applied < self.inner.commit_index() {
            self.apply_log_entry(self.last_applied)?;
            self.last_applied = LogIndex::new(self.last_applied.get() + 1);
        }

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
        loop {
            let result = match conn.ongoing_proposes.front() {
                None => would_block(conn.stream.read_object::<Message>().map_err(|e| e.into()))
                    .and_then(|m| {
                        if let Some(m) = m {
                            self.handle_incoming_message(&mut conn, m)?;
                            Ok(true)
                        } else {
                            Ok(false)
                        }
                    }),
                Some((id, params)) => {
                    dbg!(id, params);
                    todo!();
                }
            };

            match result {
                Err(e) => {
                    eprintln!("Deregister3. Error: {:?}", e); // TODO: maybe result error response object
                    self.poller.registry().deregister(conn.stream.inner_mut())?;
                }
                Ok(false) => break,
                Ok(true) => {}
            }
        }
        self.connections.insert(event.token(), conn);
        Ok(())
    }

    fn handle_incoming_message(
        &mut self,
        conn: &mut Connection,
        msg: Message,
    ) -> std::io::Result<()> {
        match msg {
            Message::CreateCluster { id, .. } => self.handle_create_cluster(conn, id),
            Message::Join { id, params, .. } => self.handle_join(conn, id, params),
            Message::Propose { id, params, .. } => self.handle_propose(conn, id, params),
        }
    }

    fn handle_propose(
        &mut self,
        conn: &mut Connection,
        id: RequestId,
        params: ProposeParams,
    ) -> std::io::Result<()> {
        todo!()
    }

    fn handle_create_cluster(
        &mut self,
        conn: &mut Connection,
        id: RequestId,
    ) -> std::io::Result<()> {
        if self.id() != Self::UNINIT_NODE_ID {
            // TODO: response false
            todo!();
        }

        let node_id = NodeId::new(0);
        self.inner = raftbare::Node::start(node_id.0);

        let mut promise = self.inner.create_cluster(&[node_id.0]);
        promise.poll(&mut self.inner);
        assert!(promise.is_accepted());

        let mut promise = self.propose_command(Command::Join {
            id: Some(node_id),
            addr: self.addr(),
        });
        promise.poll(&mut self.inner);
        assert!(promise.is_accepted());

        let response = Response::new(id, true);
        conn.stream.write_object(&response)?;

        Ok(())
    }

    fn handle_join(
        &mut self,
        _conn: &mut Connection,
        _request_id: RequestId,
        JoinParams { contact_addr }: JoinParams,
    ) -> std::io::Result<()> {
        if self.id() == Self::JOINING_NODE_ID {
            todo!()
        }
        if self.id() != Self::UNINIT_NODE_ID {
            todo!()
        }

        self.inner = raftbare::Node::start(Self::JOINING_NODE_ID.0);

        let msg = Message::Propose {
            jsonrpc: JsonRpcVersion::V2,
            id: self.next_request_id(),
            params: ProposeParams {
                command: Command::Join {
                    id: None,
                    addr: self.addr(),
                },
            },
        };
        self.send_message(contact_addr, msg)?;

        // TODO: handle reply

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
        Err(e) if e.kind() == ErrorKind::WouldBlock => Ok(None),
        Err(e) => Err(e),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response<T> {
    pub jsonrpc: JsonRpcVersion,
    pub id: RequestId,
    pub result: T,
}

impl<T> Response<T> {
    pub fn new(id: RequestId, result: T) -> Self {
        Self {
            jsonrpc: JsonRpcVersion::V2,
            id,
            result,
        }
    }
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
#[serde(tag = "method", rename_all = "snake_case")]
pub enum Message {
    CreateCluster {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        // TODO: add options (e.g., election timeout)
    },
    Join {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        params: JoinParams,
    },
    Propose {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        params: ProposeParams,
    },
}

impl Message {
    pub fn is_notification(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposeParams {
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinParams {
    pub contact_addr: SocketAddr,
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Machine for () {
        type Command = ();

        fn apply(&mut self, _ctx: &MachineContext, _command: Self::Command) {}
    }

    const POLL_TIMEOUT: Option<Duration> = Some(Duration::from_millis(5));

    #[test]
    fn create_cluster() {
        let mut node = Node::new(auto_addr(), ()).expect("Node::new() failed");
        assert_eq!(node.id(), Node::<()>::UNINIT_NODE_ID);

        let handle = node.handle();

        std::thread::scope(|s| {
            s.spawn(|| {
                let created = handle.create_cluster().expect("create_cluster() failed");
                assert_eq!(created, true);
            });
            s.spawn(|| {
                while node.id() == Node::<()>::UNINIT_NODE_ID {
                    node.poll_one(POLL_TIMEOUT).expect("poll_one() failed");
                }
            });
        });

        assert_eq!(node.id(), NodeId::new(0));
    }

    #[test]
    fn join() {
        let mut seed_node = Node::new(auto_addr(), ()).expect("Node::new() failed");
        let seed_node_addr = seed_node.addr();
        let seed_thread = std::thread::spawn(move || loop {
            seed_node.poll_one(POLL_TIMEOUT).expect("poll_one() failed");
        });

        let mut node = Node::new(auto_addr(), ()).expect("Node::new() failed");
        let handle = node.handle();

        std::thread::scope(|s| {
            s.spawn(|| {
                let joined = handle.join(seed_node_addr).expect("join() failed");
                assert_eq!(joined, true);
            });
            s.spawn(|| {
                while (node.id() == Node::<()>::UNINIT_NODE_ID
                    || node.id() == Node::<()>::JOINING_NODE_ID)
                    && !seed_thread.is_finished()
                {
                    node.poll_one(POLL_TIMEOUT).expect("poll_one() failed");
                }
            });
        });

        assert_ne!(node.id(), NodeId::new(0));
    }

    fn auto_addr() -> SocketAddr {
        addr("127.0.0.1:0")
    }

    fn addr(s: &str) -> SocketAddr {
        s.parse().expect("parse addr")
    }
}
