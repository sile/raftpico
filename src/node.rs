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
    addr: SocketAddr,
    token: Token,
    pub is_client: bool, // TODO: remove

    // TODO: btreemap to support concurrent proposes
    pub ongoing_proposes: VecDeque<(RequestId, ProposeParams)>,
}

impl Connection {
    pub fn new(stream: TcpStream, phase: ConnectionPhase, addr: SocketAddr, token: Token) -> Self {
        Self {
            stream: JsonlStream::new(stream),
            phase,
            addr,
            token,
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
        // TODO: Add instance_id(?)
        addr: SocketAddr,
    },
    UserCommand(serde_json::Value),
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SystemMachine {
    pub members: BTreeMap<SocketAddr, NodeId>,
    pub address_table: BTreeMap<NodeId, SocketAddr>,
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

        // TODO:
        // stream.set_write_timeout(Some(Duration::from_secs(1)))?;
        // stream.set_read_timeout(Some(Duration::from_secs(1)))?;

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
        stream.set_write_timeout(Some(Duration::from_secs(1)))?;
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
    max_token_id: Token,
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
            max_token_id: Token(1),
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
        eprintln!(
            "[{}] Proposed: {:?}, {:?} ",
            self.instance_id,
            promise.log_position(),
            command
        );

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
        eprintln!("[{}] connect to '{}': {:?}", self.instance_id, addr, token);

        self.poller
            .registry()
            .register(&mut stream, token, Interest::WRITABLE)?;
        self.connections.insert(
            token,
            Connection::new(stream, ConnectionPhase::Connecting, addr, token),
        );
        self.addr_to_token.insert(addr, token);
        Ok(())
    }

    fn handle_action(&mut self, action: Action) -> std::io::Result<()> {
        match action {
            Action::SetElectionTimeout => self.handle_set_election_timeout(),
            Action::SaveCurrentTerm | Action::SaveVotedFor | Action::AppendLogEntries(_) => {
                // Do nothing as this crate uses in-memory storage.
            }
            Action::BroadcastMessage(m) => self.handle_broadcast_message(m)?,
            Action::SendMessage(_, _) => todo!(),
            Action::InstallSnapshot(_) => todo!(),
        }
        Ok(())
    }

    fn handle_broadcast_message(&mut self, msg: raftbare::Message) -> std::io::Result<()> {
        let msg = match msg {
            raftbare::Message::RequestVoteCall {
                header,
                last_position,
            } => todo!(),
            raftbare::Message::RequestVoteReply {
                header,
                vote_granted,
            } => todo!(),
            raftbare::Message::AppendEntriesCall {
                header,
                commit_index,
                entries,
            } => Message::new_append_entries_call(header, commit_index, entries, self),
            raftbare::Message::AppendEntriesReply {
                header,
                last_position,
            } => todo!(),
        };

        // TODO: remove allocation
        for peer in self.inner.peers().collect::<Vec<_>>() {
            let peer = NodeId::new(peer.get());
            let Some(addr) = self.system_machine.address_table.get(&peer).copied() else {
                todo!();
            };
            self.send_message(addr, msg.clone())?;
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
        eprintln!("[{}] apply log entry: {:?}", self.instance_id, index);
        let Some(command) = self.command_log.get(&index) else {
            return Ok(());
        };
        match command {
            Command::Join { id, addr } => {
                eprintln!(
                    "[{}] apply Join: id={:?}, addr={:?}",
                    self.instance_id, id, addr
                );
                let is_initial = id.is_some();

                // TODO: addr duplicate check
                let id = id.clone().unwrap_or(NodeId::new(index.get()));
                self.system_machine.members.insert(*addr, id);
                self.system_machine.address_table.insert(id, *addr);
                // if let Some((_addr, token, request_id)) = self.pending_commands.remove(&index) {
                //     let res = Response::new(request_id, true);
                //     self.reply(token, &res)?;
                // }

                if !is_initial && self.role().is_leader() {
                    let new_config = self.inner.config().to_joint_consensus(&[id.0], &[]);
                    let promise = self.inner.propose_config(new_config);
                    assert!(!promise.is_rejected()); // TODO: maybe fail here

                    // TODO: re propose if leader is changed before committed
                }
            }
            Command::UserCommand(_) => todo!(),
        }
        Ok(())
    }

    fn reply<T: Serialize>(&mut self, token: Token, res: &T) -> std::io::Result<()> {
        let Some(conn) = self.connections.get_mut(&token) else {
            // Already disconnected
            return Ok(());
        };
        let _ = conn.stream.write_object(res);
        // TODO: remove connection if error
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

        let old_last_applied = self.last_applied;
        while self.last_applied < self.inner.commit_index() {
            let index = LogIndex::new(self.last_applied.get() + 1);
            self.apply_log_entry(index)?;
            self.last_applied = index;
        }
        if old_last_applied != self.last_applied && self.role().is_leader() {
            // TODO
            // Quickly sync to followers the latest commit index
            self.inner.heartbeat();
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
                        eprintln!(
                            "[{}] Connected to {:?}",
                            self.instance_id,
                            conn.stream.inner().peer_addr()
                        ); // TODO
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
                        println!("[{}:{:?}] recv={:?}", self.instance_id, self.id(), m);
                        if let Some(m) = m {
                            self.handle_incoming_message(&mut conn, m)?;
                            Ok(true)
                        } else {
                            Ok(false)
                        }
                    }),
                Some((expected_id, _params)) => would_block(
                    conn.stream
                        .read_object::<Response<bool>>()
                        .map_err(|e| e.into()),
                )
                .and_then(|m| {
                    if let Some(m) = m {
                        println!("[{}:{:?}] recv={:?}", self.instance_id, self.id(), m);
                        assert_eq!(m.id, *expected_id);
                        //self.handle_join_result(&mut conn, m)?;

                        todo!();
                    } else {
                        Ok(false)
                    }
                }),
            };

            match result {
                Err(e) => {
                    eprintln!(
                        "[{}:{:?}] Deregister3. Error: {:?}",
                        self.instance_id,
                        self.id(),
                        e
                    ); // TODO: maybe result error response object
                    self.poller.registry().deregister(conn.stream.inner_mut())?;
                    return Ok(());
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
            Message::Raft { params, .. } => self.handle_raft_message(conn, params),
        }
    }

    fn handle_raft_message(
        &mut self,
        conn: &mut Connection,
        params: RaftMessageParams,
    ) -> std::io::Result<()> {
        match params {
            RaftMessageParams::AppendEntriesCall {
                from,
                term,
                seqno,
                commit_index,
                prev_term,
                prev_index,
                entries,
            } => {
                let mut raftbare_entries = raftbare::LogEntries::new(raftbare::LogPosition {
                    term: raftbare::Term::new(prev_term),
                    index: raftbare::LogIndex::new(prev_index),
                });

                for (i, entry) in entries.into_iter().enumerate() {
                    let raftbare_entry = match entry {
                        LogEntry::Term(t) => raftbare::LogEntry::Term(raftbare::Term::new(t)),
                        LogEntry::Config { voters, new_voters } => {
                            let voters = voters
                                .into_iter()
                                .map(|x| raftbare::NodeId::new(x.get()))
                                .collect();
                            let new_voters = new_voters
                                .into_iter()
                                .map(|x| raftbare::NodeId::new(x.get()))
                                .collect();
                            raftbare::LogEntry::ClusterConfig(raftbare::ClusterConfig {
                                voters,
                                new_voters,
                                ..Default::default()
                            })
                        }
                        LogEntry::Command(command) => {
                            let index = LogIndex::new(prev_index + i as u64 + 1);
                            self.command_log.insert(index, command);
                            raftbare::LogEntry::Command
                        }
                    };
                    raftbare_entries.push(raftbare_entry);
                }

                let header = raftbare::MessageHeader {
                    from: raftbare::NodeId::new(from.get()),
                    term: raftbare::Term::new(term),
                    seqno: raftbare::MessageSeqNo::new(seqno),
                };
                let msg = raftbare::Message::AppendEntriesCall {
                    header,
                    commit_index: LogIndex::new(commit_index),
                    entries: raftbare_entries,
                };
                self.inner.handle_message(msg);
            }
        }
        Ok(())
    }

    fn handle_propose(
        &mut self,
        conn: &mut Connection,
        id: RequestId,
        ProposeParams { command }: ProposeParams,
    ) -> std::io::Result<()> {
        if !self.role().is_leader() {
            // TODO: response to redirect to leader
            todo!();
        }

        let promise = self.propose_command(command);

        // TODO: disconnect handling
        let _ = conn
            .stream
            .write_object(&Response::new_commit_promise(id, promise));

        Ok(())
    }

    fn handle_create_cluster(
        &mut self,
        conn: &mut Connection,
        id: RequestId,
    ) -> std::io::Result<()> {
        if self.id() != Self::UNINIT_NODE_ID {
            let response = Response::new(id, false);
            conn.stream.write_object(&response)?;
            return Ok(());
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

    fn next_token(&mut self) -> Token {
        // TODO: recycle tokens
        self.max_token_id.0 += 1;
        self.max_token_id
    }

    fn handle_listener(&mut self) -> std::io::Result<()> {
        loop {
            let Some((mut stream, addr)) = would_block(self.listener.accept())? else {
                return Ok(());
            };
            let token = self.next_token();
            eprintln!("[{}] accept '{}': {:?}", self.instance_id, addr, token);
            self.poller
                .registry()
                .register(&mut stream, token, Interest::READABLE)?;

            // TODO: handle initial read

            self.connections.insert(
                token,
                Connection::new(stream, ConnectionPhase::Connected, addr, token),
            );
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

impl Response<CommitPromiseObject> {
    pub fn new_commit_promise(id: RequestId, promise: CommitPromise) -> Self {
        Self {
            jsonrpc: JsonRpcVersion::V2,
            id,
            result: CommitPromiseObject {
                term: promise.log_position().term.get(),
                index: promise.log_position().index.get(),
            },
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
    Raft {
        jsonrpc: JsonRpcVersion,
        params: RaftMessageParams,
    },
}

impl Message {
    pub fn is_notification(&self) -> bool {
        matches!(self, Self::Raft { .. })
    }

    pub fn new_append_entries_call<M>(
        header: raftbare::MessageHeader,
        commit_index: LogIndex,
        raft_entries: raftbare::LogEntries,
        node: &Node<M>,
    ) -> Self {
        let prev_position = raft_entries.prev_position();
        let mut entries = Vec::with_capacity(raft_entries.len());

        for (pos, entry) in raft_entries.iter_with_positions() {
            match entry {
                raftbare::LogEntry::Term(t) => {
                    entries.push(LogEntry::Term(t.get()));
                }
                raftbare::LogEntry::ClusterConfig(c) => {
                    entries.push(LogEntry::Config {
                        voters: c.voters.iter().map(|x| NodeId::new(x.get())).collect(),
                        new_voters: c.new_voters.iter().map(|x| NodeId::new(x.get())).collect(),
                    });
                }
                raftbare::LogEntry::Command => {
                    let command = node
                        .command_log
                        .get(&pos.index)
                        .expect("TODO: handle this case (bug or snapshot needed)");
                    entries.push(LogEntry::Command(command.clone()));
                }
            }
        }

        Self::Raft {
            jsonrpc: JsonRpcVersion::V2,
            params: RaftMessageParams::AppendEntriesCall {
                from: NodeId::new(header.from.get()),
                term: header.term.get(),
                seqno: header.seqno.get(),
                commit_index: commit_index.get(),
                prev_term: prev_position.term.get(),
                prev_index: prev_position.index.get(),
                entries,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RaftMessageParams {
    AppendEntriesCall {
        from: NodeId,
        term: u64,
        seqno: u64,
        commit_index: u64,
        prev_term: u64,
        prev_index: u64,
        entries: Vec<LogEntry>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogEntry {
    Term(u64),
    Config {
        voters: Vec<NodeId>,
        new_voters: Vec<NodeId>,
    },
    Command(Command),
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

    const POLL_TIMEOUT: Option<Duration> = Some(Duration::from_millis(10));

    #[test]
    fn create_cluster() {
        let mut node = Node::new(auto_addr(), ()).expect("Node::new() failed");
        assert_eq!(node.id(), Node::<()>::UNINIT_NODE_ID);

        let handle = node.handle();

        std::thread::scope(|s| {
            s.spawn(|| {
                let created = handle.create_cluster().expect("create_cluster() failed");
                assert_eq!(created, true);

                let created = handle.create_cluster().expect("create_cluster() failed");
                assert_eq!(created, false);
            });
            s.spawn(|| {
                for _ in 0..50 {
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
        dbg!(seed_node_addr);
        let handle = seed_node.handle();
        std::thread::spawn(move || {
            for _ in 0..100 {
                seed_node.poll_one(POLL_TIMEOUT).expect("poll_one() failed");
            }
            dbg!("seed_node done");
        });
        let created = handle.create_cluster().expect("create_cluster() failed");
        assert_eq!(created, true);

        let mut node = Node::new(auto_addr(), ()).expect("Node::new() failed");
        let handle = node.handle();

        std::thread::scope(|s| {
            s.spawn(|| {
                let joined = handle.join(seed_node_addr).expect("join() failed");
                assert_eq!(joined, true);
            });
            s.spawn(|| {
                for _ in 0..100 {
                    node.poll_one(POLL_TIMEOUT).expect("poll_one() failed");
                }
                assert_ne!(node.id(), Node::<()>::UNINIT_NODE_ID);
                assert_ne!(node.id(), Node::<()>::JOINING_NODE_ID);
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
