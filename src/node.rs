use std::{
    cmp::{Ordering, Reverse},
    collections::{BTreeMap, BinaryHeap},
    io::ErrorKind,
    marker::PhantomData,
    net::SocketAddr,
    time::{Duration, Instant},
};

use jsonlrpc::{JsonRpcVersion, JsonlStream, RequestId, RpcClient};
use mio::{
    event::Event,
    net::{TcpListener, TcpStream},
    Events, Interest, Poll, Token,
};
use raftbare::{Action, CommitPromise, LogIndex, LogPosition, NodeId, Role};
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::{remote_types::NodeIdJson, request::JoinParams};

#[derive(Debug)]
pub struct MachineContext {
    has_caller: bool,
    result: Option<serde_json::Value>,
}

impl MachineContext {
    pub fn new(has_caller: bool) -> Self {
        Self {
            has_caller,
            result: None,
        }
    }

    pub fn reply<T>(&mut self, reply: &T)
    where
        T: Serialize,
    {
        if !self.has_caller {
            return;
        }

        self.result = Some(serde_json::to_value(reply).expect("TODO"));
    }
}

pub trait Machine: Serialize + for<'a> Deserialize<'a> {
    type Command: Serialize + for<'a> Deserialize<'a>;

    fn apply(&mut self, ctx: &mut MachineContext, command: Self::Command);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionPhase {
    Connecting,
    Connected,
}

#[derive(Debug)]
pub struct Connection<M> {
    stream: JsonlStream<TcpStream>,
    phase: ConnectionPhase,
    addr: SocketAddr,
    token: Token,
    next_request_id: i64,
    ongoing_requests: BTreeMap<i64, (OnSuccess<M>, OnFailure)>,
    _machine: PhantomData<M>, // TODO: remove if possible
}

impl<M> Connection<M> {
    pub fn new(stream: TcpStream, phase: ConnectionPhase, addr: SocketAddr, token: Token) -> Self {
        Self {
            stream: JsonlStream::new(stream),
            phase,
            addr,
            token,
            next_request_id: 0,
            ongoing_requests: BTreeMap::new(),
            _machine: PhantomData,
        }
    }

    pub fn async_rpc<T>(
        &mut self,
        poll: &mut Poll,
        method: &str,
        params: &T,
        on_success: OnSuccess<M>,
        on_failure: OnFailure,
    ) -> std::io::Result<()>
    where
        T: Serialize,
    {
        #[derive(Serialize)]
        struct Req<'b, U> {
            jsonrpc: JsonRpcVersion,
            id: i64,
            method: &'b str,
            params: &'b U,
        }

        let req = Req {
            jsonrpc: JsonRpcVersion::V2,
            id: self.next_request_id,
            method,
            params,
        };
        self.next_request_id += 1;

        let _ = self.stream.write_object(&req); // TODO: error handling
        if self.stream.write_buf().is_empty() {
            // TODO
            poll.registry()
                .reregister(self.stream.inner_mut(), self.token, Interest::WRITABLE)?;
        }

        self.ongoing_requests
            .insert(req.id, (on_success, on_failure));
        Ok(())
    }
}

pub type OnSuccess<M> =
    fn(&mut Node<M>, &mut Connection<M>, result: serde_json::Value) -> std::io::Result<()>;
pub type OnFailure = fn() -> ();

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Command {
    Join {
        id: NodeIdJson,
        // TODO: Add instance_id(?)
        addr: SocketAddr,
        caller: Option<Caller>,
    },
    UserCommand {
        command: serde_json::Value,
        caller: Caller,
    },
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(into = "SystemMachineMin", from = "SystemMachineMin")]
pub struct SystemMachine {
    pub members: BTreeMap<SocketAddr, NodeId>,
    pub address_table: BTreeMap<NodeId, SocketAddr>,
}

impl From<SystemMachineMin> for SystemMachine {
    fn from(min: SystemMachineMin) -> Self {
        let mut members = BTreeMap::new();
        let mut address_table = BTreeMap::new();
        for (addr, id) in min.members {
            let id = NodeId::new(id);
            members.insert(addr, id);
            address_table.insert(id, addr);
        }

        Self {
            members,
            address_table,
        }
    }
}

impl From<SystemMachine> for SystemMachineMin {
    fn from(sys: SystemMachine) -> Self {
        let members = sys
            .members
            .into_iter()
            .map(|(addr, id)| (addr, id.get()))
            .collect();
        Self { members }
    }
}

// TODO: remove
#[derive(Debug, Serialize, Deserialize)]
pub struct SystemMachineMin {
    pub members: Vec<(SocketAddr, u64)>,
}

// TODO: RaftClient
#[derive(Debug, Clone)]
pub struct NodeHandle<M> {
    addr: SocketAddr,
    _machine: PhantomData<M>,
}

impl<M: Machine> NodeHandle<M> {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            _machine: PhantomData,
        }
    }

    // TODO: return error object instead of false?
    pub fn create_cluster(&self) -> std::io::Result<bool> {
        let stream = std::net::TcpStream::connect(self.addr)?;

        // TODO:
        // stream.set_write_timeout(Some(Duration::from_secs(1)))?;
        // stream.set_read_timeout(Some(Duration::from_secs(1)))?;

        let mut client = RpcClient::new(stream); // TODO: reuse client

        let response: Response<bool> = client.call(&Request::CreateCluster {
            jsonrpc: JsonRpcVersion::V2,
            id: RequestId::Number(0),
        })?;
        Ok(response.result)
    }

    pub fn join(&self, contact_addr: SocketAddr) -> std::io::Result<bool> {
        let stream = std::net::TcpStream::connect(self.addr)?;

        // TODO:
        stream.set_write_timeout(Some(Duration::from_secs(2)))?;
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;

        let mut client = RpcClient::new(stream); // TODO: reuse client

        let response: Response<bool> = client.call(&Request::Join {
            jsonrpc: JsonRpcVersion::V2,
            id: RequestId::Number(0),
            params: JoinParams { contact_addr },
        })?;
        Ok(response.result)
    }

    pub fn propose_command<T>(&self, command: M::Command) -> std::io::Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let stream = std::net::TcpStream::connect(self.addr)?;

        // TODO:
        stream.set_write_timeout(Some(Duration::from_secs(2)))?;
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;

        let mut client = RpcClient::new(stream); // TODO: reuse client

        // TODO: use another message
        let response: Response<T> = client.call(&Request::ProposeUserCommand {
            jsonrpc: JsonRpcVersion::V2,
            id: RequestId::Number(0),
            params: (serde_json::to_value(command)?,),
        })?;
        Ok(response.result)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromiseQueueItem {
    pub promise: CommitPromise,
    pub token: Token,
    pub request_id: RequestId,
}

impl PromiseQueueItem {
    fn key(&self) -> Reverse<(u64, u64)> {
        let pos = self.promise.log_position();
        Reverse((pos.term.get(), pos.index.get()))
    }
}

impl PartialOrd for PromiseQueueItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PromiseQueueItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key().cmp(&other.key())
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
    connections: BTreeMap<Token, Connection<M>>,
    max_token_id: Token,
    addr_to_token: BTreeMap<SocketAddr, Token>,
    next_request_id: u64, // TODO: remove
    command_log: BTreeMap<LogIndex, Command>,
    last_applied: LogIndex,
    pub system_machine: SystemMachine, // TODO
    election_timeout_time: Option<Instant>,
    pending_promise_queue: BinaryHeap<PromiseQueueItem>, // TODO: remove
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
            instance_id: rand::random(),
            machine,
            listener,
            local_addr,
            poller,
            events: Some(events),
            connections: BTreeMap::new(),
            max_token_id: Token(1),
            addr_to_token: BTreeMap::new(),
            next_request_id: 100, // TODO
            command_log: BTreeMap::new(),
            last_applied: LogIndex::ZERO,
            system_machine: SystemMachine::default(),
            election_timeout_time: None,
            pending_promise_queue: BinaryHeap::new(),
        })
    }

    pub fn id(&self) -> NodeId {
        NodeId::new(self.inner.id().get())
    }

    pub fn addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn handle(&self) -> NodeHandle<M> {
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

    // TODO:rename
    fn send_message_by_token<T: Serialize>(
        &mut self,
        token: Token,
        message: &T,
    ) -> std::io::Result<()> {
        let Some(conn) = self.connections.get_mut(&token) else {
            dbg!(token);
            unreachable!(); // or TODO
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

        Ok(())
    }

    fn send_message(&mut self, dest: SocketAddr, message: Request) -> std::io::Result<()> {
        //eprintln!("[{}] Send: {:?} -> {:?} ", self.instance_id, message, dest);
        if !self.addr_to_token.contains_key(&dest) {
            self.connect(dest)?;
        }

        let Some(token) = self.addr_to_token.get(&dest).copied() else {
            unreachable!();
        };

        self.send_message_by_token(token, &message)?;

        Ok(())
    }

    fn connect(&mut self, addr: SocketAddr) -> std::io::Result<Token> {
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
        Ok(token)
    }

    fn handle_action(&mut self, action: Action) -> std::io::Result<()> {
        match action {
            Action::SetElectionTimeout => self.handle_set_election_timeout(),
            Action::SaveCurrentTerm | Action::SaveVotedFor | Action::AppendLogEntries(_) => {
                // Do nothing as this crate uses in-memory storage.

                // TODO: reject commit log handling (to response to client)
            }
            Action::BroadcastMessage(m) => self.handle_broadcast_message(m)?,
            Action::SendMessage(dest, m) => self.handle_send_message(dest, m)?,
            Action::InstallSnapshot(_) => todo!(),
        }
        Ok(())
    }

    fn from_raft_message(&self, msg: raftbare::Message) -> Request {
        match msg {
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
            } => Request::new_append_entries_call(header, commit_index, entries, self),
            raftbare::Message::AppendEntriesReply {
                header,
                last_position,
            } => Request::new_append_entries_reply(header, last_position),
        }
    }

    fn handle_send_message(
        &mut self,
        peer: raftbare::NodeId,
        msg: raftbare::Message,
    ) -> std::io::Result<()> {
        let msg = self.from_raft_message(msg);
        let peer = NodeId::new(peer.get());
        let Some(addr) = self.system_machine.address_table.get(&peer).copied() else {
            todo!();
        };
        // TODO: drop if write buf is full
        self.send_message(addr, msg.clone())?;
        Ok(())
    }

    fn handle_broadcast_message(&mut self, msg: raftbare::Message) -> std::io::Result<()> {
        let msg = self.from_raft_message(msg);

        // TODO: remove allocation
        for peer in self.inner.peers().collect::<Vec<_>>() {
            let peer = NodeId::new(peer.get());
            let Some(addr) = self.system_machine.address_table.get(&peer).copied() else {
                todo!();
            };

            // TODO: drop if write buf is full
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
            Command::Join { id, addr, caller } => {
                let id = id.0;
                eprintln!(
                    "[{}] apply Join: id={:?}, addr={:?}",
                    self.instance_id, id, addr
                );
                let is_initial = id == NodeId::new(0);

                // TODO: addr duplicate check
                if id == Self::UNINIT_NODE_ID {
                    todo!("bug");
                }
                self.system_machine.members.insert(*addr, id);
                self.system_machine.address_table.insert(id, *addr);
                // if let Some((_addr, token, request_id)) = self.pending_commands.remove(&index) {
                //     let res = Response::new(request_id, true);
                //     self.reply(token, &res)?;
                // }

                if !is_initial && self.role().is_leader() {
                    let new_config = self.inner.config().to_joint_consensus(&[id], &[]);
                    let promise = self.inner.propose_config(new_config);
                    assert!(!promise.is_rejected()); // TODO: maybe fail here

                    // TODO: re propose if leader is changed before committed
                }

                if let Some(caller) = caller {
                    if id == self.id() {
                        let reply = Response::new(caller.request_id.clone(), true);
                        self.send_message_by_token(caller.token(), &reply)?;
                    }
                };
            }
            Command::UserCommand { command, caller } => {
                // TODO
                // let caller = if self
                //     .pending_promise_queue
                //     .peek()
                //     .map_or(false, |item| item.promise.log_position().index == index)
                // {
                //     let item = self.pending_promise_queue.pop().expect("unreachable");
                //     Some((item.token, item.request_id))
                // } else {
                //     None
                // };
                let command: M::Command = serde_json::from_value(command.clone()).expect("TODO");
                let mut ctx = MachineContext::new(caller.node_id.0 == self.id());
                self.machine.apply(&mut ctx, command);

                if let Some(result) = ctx.result {
                    let reply = Response::new(caller.request_id.clone(), result);
                    self.send_message_by_token(caller.token(), &reply)
                        .expect("TODO");
                }
            }
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

        loop {
            if let Some(mut item) = self.pending_promise_queue.peek_mut() {
                if !item.promise.poll(&self.inner).is_rejected() {
                    break;
                }
            } else {
                break;
            }
            self.pending_promise_queue.pop();
        }

        // TODO: skip until node id is fixed
        let old_last_applied = self.last_applied;
        while self.last_applied < self.inner.commit_index() {
            let index = LogIndex::new(self.last_applied.get() + 1);
            self.apply_log_entry(index)?;
            self.last_applied = index;
        }
        if old_last_applied != self.last_applied && self.role().is_leader() {
            // TODO
            // Quickly sync to followers the latest commit index
            //
            // TODO: remove?
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
        mut conn: Connection<M>,
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
            // TODO: batch RPC support
            let result = would_block(
                conn.stream
                    .read_object::<serde_json::Value>()
                    .map_err(|e| e.into()),
            );
            match result {
                Err(e) => {
                    eprintln!(
                        "[{}:{:?}] Deregister3. Error: {:?}",
                        self.instance_id,
                        self.id(),
                        e
                    ); // TODO: maybe result error response object
                    eprintln!(
                        "[{}] Deregister3. {:?}",
                        self.instance_id,
                        std::str::from_utf8(conn.stream.read_buf())
                    );

                    self.poller.registry().deregister(conn.stream.inner_mut())?;
                    return Ok(());
                }
                Ok(None) => break,
                Ok(Some(value)) => {
                    let obj = value.as_object().expect("TODO");
                    if obj.contains_key("method") {
                        let msg: Request = serde_json::from_value(value).expect("TODO");
                        self.handle_incoming_message(&mut conn, msg).expect("TODO");
                    } else {
                        let reques_id = obj.get("id").expect("TODO").as_i64().expect("TODO");
                        let result = obj.get("result").expect("TODO").clone(); // TODO: remove clone
                        if let Some((on_success, _)) = conn.ongoing_requests.remove(&reques_id) {
                            on_success(self, &mut conn, result).expect("TODO");
                        }
                    }
                }
            }
        }
        self.connections.insert(event.token(), conn);
        Ok(())
    }

    // TODO: rename
    fn handle_commit_promise(&mut self, promise: CommitPromise) -> std::io::Result<()> {
        // TODO: note
        if self.id() == Self::JOINING_NODE_ID && !promise.is_rejected() {
            let node_id = raftbare::NodeId::new(promise.log_position().index.get());
            eprintln!("[{}] Reset node id to {:?}", self.instance_id, node_id);
            self.inner = raftbare::Node::start(node_id);
        }

        // TODO: create promise to check the commit abort

        Ok(())
    }

    fn handle_incoming_message(
        &mut self,
        conn: &mut Connection<M>,
        msg: Request,
    ) -> std::io::Result<()> {
        match msg {
            Request::CreateCluster { id, .. } => self.handle_create_cluster(conn, id),
            Request::Join { id, params, .. } => self.handle_join(conn, id, params),
            Request::ProposeUserCommand { id, params, .. } => {
                self.handle_propose_user_command(conn, id, params.0)
            }
            Request::Propose { id, params, .. } => self.handle_remote_propose(conn, id, params),
            Request::Raft { params, .. } => self.handle_raft_message(conn, params),
        }
    }

    fn handle_propose_user_command(
        &mut self,
        conn: &mut Connection<M>,
        id: RequestId,
        command: serde_json::Value,
    ) -> std::io::Result<()> {
        let command = Command::UserCommand {
            command,
            caller: Caller::new(self.id(), conn.token, id),
        };

        if !self.role().is_leader() {
            let Some(maybe_leader) = self.inner.voted_for() else {
                todo!("reply error");
            };

            let addr = self
                .system_machine
                .address_table
                .get(&maybe_leader)
                .expect("TODO");
            let token = self.addr_to_token.get(&addr).expect("TODO");
            let conn = self.connections.get_mut(token).expect("TODO");

            conn.async_rpc(
                &mut self.poller,
                "propose",
                &ProposeParams { command },
                |_node, _conn, result| {
                    let params: CommitPromiseObject = serde_json::from_value(result)?;
                    let promise = params.to_promise();
                    if promise.is_rejected() {
                        todo!();
                    }
                    Ok(())
                },
                || (),
            )?;

            return Ok(());
        }

        let promise = self.propose_command(command);
        assert!(!promise.is_rejected());

        // let item = PromiseQueueItem {
        //     promise,
        //     token: conn.token,
        //     request_id: id,
        // };
        // self.pending_promise_queue.push(item);

        Ok(())
    }

    fn handle_raft_message(
        &mut self,
        conn: &mut Connection<M>,
        params: RaftMessageParams,
    ) -> std::io::Result<()> {
        let msg = match params {
            RaftMessageParams::AppendEntriesCall {
                from,
                from_addr,
                term,
                seqno,
                commit_index,
                prev_term,
                prev_index,
                entries,
            } => {
                // TODO:
                self.system_machine.address_table.insert(from.0, from_addr);

                let mut raftbare_entries = raftbare::LogEntries::new(raftbare::LogPosition {
                    term: raftbare::Term::new(prev_term),
                    index: raftbare::LogIndex::new(prev_index),
                });

                for (i, entry) in entries.into_iter().enumerate() {
                    let raftbare_entry = match entry {
                        LogEntry::Term(t) => raftbare::LogEntry::Term(raftbare::Term::new(t)),
                        LogEntry::Config { voters, new_voters } => {
                            let voters = voters.into_iter().map(|x| x.0).collect();
                            let new_voters = new_voters.into_iter().map(|x| x.0).collect();
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
                    from: from.0,
                    term: raftbare::Term::new(term),
                    seqno: raftbare::MessageSeqNo::new(seqno),
                };
                raftbare::Message::AppendEntriesCall {
                    header,
                    commit_index: LogIndex::new(commit_index),
                    entries: raftbare_entries,
                }
            }
            RaftMessageParams::AppendEntriesReply {
                from,
                term,
                seqno,
                last_term,
                last_index,
            } => {
                let header = raftbare::MessageHeader {
                    from: from.0,
                    term: raftbare::Term::new(term),
                    seqno: raftbare::MessageSeqNo::new(seqno),
                };
                raftbare::Message::AppendEntriesReply {
                    header,
                    last_position: raftbare::LogPosition {
                        term: raftbare::Term::new(last_term),
                        index: raftbare::LogIndex::new(last_index),
                    },
                }
            }
        };
        self.inner.handle_message(msg);
        Ok(())
    }

    fn handle_remote_propose(
        &mut self,
        conn: &mut Connection<M>,
        id: RequestId,
        ProposeParams { mut command }: ProposeParams,
    ) -> std::io::Result<()> {
        if !self.role().is_leader() {
            // TODO: response to redirect to leader
            todo!();
        }

        if let Command::Join { id, .. } = &mut command {
            if id.0 == Self::UNINIT_NODE_ID {
                id.0 = NodeId::new(self.inner.log().last_position().index.get() + 1);
            }
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
        conn: &mut Connection<M>,
        id: RequestId,
    ) -> std::io::Result<()> {
        if self.id() != Self::UNINIT_NODE_ID {
            let response = Response::new(id, false);
            conn.stream.write_object(&response)?;
            return Ok(());
        }

        let node_id = NodeId::new(0);
        self.inner = raftbare::Node::start(node_id);

        let mut promise = self.inner.create_cluster(&[node_id]);
        promise.poll(&mut self.inner);
        assert!(promise.is_accepted());

        let mut promise = self.propose_command(Command::Join {
            id: NodeIdJson(node_id),
            addr: self.addr(),
            caller: None,
        });
        promise.poll(&mut self.inner);
        assert!(promise.is_accepted());

        let response = Response::new(id, true);
        conn.stream.write_object(&response)?;

        Ok(())
    }

    fn handle_join(
        &mut self,
        conn: &mut Connection<M>,
        todo_request_id: RequestId,
        JoinParams { contact_addr }: JoinParams,
    ) -> std::io::Result<()> {
        if self.id() == Self::JOINING_NODE_ID {
            todo!()
        }
        if self.id() != Self::UNINIT_NODE_ID {
            todo!()
        }

        self.inner = raftbare::Node::start(Self::JOINING_NODE_ID);

        let command = Command::Join {
            id: NodeIdJson(Self::UNINIT_NODE_ID),
            addr: self.addr(),

            // TODO: note about id
            caller: Some(Caller::new(self.id(), conn.token, todo_request_id.clone())),
        };

        // TODO: error handling
        let token = self.connect(contact_addr)?;
        let conn = self.connections.get_mut(&token).expect("unreachable");
        conn.async_rpc(
            &mut self.poller,
            "propose",
            &ProposeParams { command },
            |node, _conn, result| {
                let params: CommitPromiseObject = serde_json::from_value(result)?;
                node.handle_commit_promise(params.to_promise())?;
                Ok(())
            },
            || (),
        )?;

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
    // TODO: add state?
}

impl CommitPromiseObject {
    pub fn to_promise(&self) -> CommitPromise {
        let position = raftbare::LogPosition {
            term: raftbare::Term::new(self.term),
            index: LogIndex::new(self.index),
        };
        if position == LogPosition::INVALID {
            CommitPromise::Rejected(position)
        } else {
            CommitPromise::Pending(position)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum Request {
    // API
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
    ProposeUserCommand {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        params: (serde_json::Value,),
    },

    // Internals
    Propose {
        jsonrpc: JsonRpcVersion,
        id: RequestId,
        params: ProposeParams,
    },
    Raft {
        jsonrpc: JsonRpcVersion,
        params: RaftMessageParams,
    },
    // TODO: LogStrip { smallest_last_log_index: u64 },
}

impl Request {
    pub fn is_notification(&self) -> bool {
        matches!(self, Self::Raft { .. })
    }

    pub fn new_append_entries_reply(
        header: raftbare::MessageHeader,
        position: raftbare::LogPosition,
    ) -> Self {
        Self::Raft {
            jsonrpc: JsonRpcVersion::V2,
            params: RaftMessageParams::AppendEntriesReply {
                from: NodeIdJson(header.from),
                term: header.term.get(),
                seqno: header.seqno.get(),
                last_term: position.term.get(),
                last_index: position.index.get(),
            },
        }
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
                        voters: c.voters.iter().copied().map(NodeIdJson).collect(),
                        new_voters: c.new_voters.iter().copied().map(NodeIdJson).collect(),
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
                from: NodeIdJson(header.from),
                from_addr: node.local_addr,
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
        from: NodeIdJson,
        from_addr: SocketAddr,
        term: u64,
        seqno: u64,
        commit_index: u64,
        prev_term: u64,
        prev_index: u64,
        entries: Vec<LogEntry>,
    },
    AppendEntriesReply {
        from: NodeIdJson,
        term: u64,
        seqno: u64,
        last_term: u64,
        last_index: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogEntry {
    Term(u64),
    Config {
        voters: Vec<NodeIdJson>,
        new_voters: Vec<NodeIdJson>,
    },
    Command(Command),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposeParams {
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Caller {
    pub node_id: NodeIdJson,
    pub token: usize,
    pub request_id: RequestId,
}

impl Caller {
    pub fn new(node_id: NodeId, token: Token, request_id: RequestId) -> Self {
        Self {
            node_id: NodeIdJson(node_id),
            token: token.0,
            request_id,
        }
    }

    pub fn token(&self) -> Token {
        Token(self.token)
    }
}

#[cfg(test)]
mod tests {
    use rand::seq::SliceRandom;

    use super::*;

    impl Machine for usize {
        type Command = usize;

        fn apply(&mut self, ctx: &mut MachineContext, command: Self::Command) {
            *self += command;
            ctx.reply(self);
        }
    }

    const POLL_TIMEOUT: Option<Duration> = Some(Duration::from_millis(10));

    #[test]
    fn create_cluster() {
        let mut node = Node::new(auto_addr(), 0).expect("Node::new() failed");
        assert_eq!(node.id(), Node::<usize>::UNINIT_NODE_ID);

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
        let mut seed_node = Node::new(auto_addr(), 0).expect("Node::new() failed");
        let seed_node_addr = seed_node.addr();
        dbg!(seed_node_addr);
        let handle = seed_node.handle();
        std::thread::spawn(move || {
            for _ in 0..200 {
                seed_node.poll_one(POLL_TIMEOUT).expect("poll_one() failed");
            }
            dbg!("seed_node done");
        });
        let created = handle.create_cluster().expect("create_cluster() failed");
        assert_eq!(created, true);

        let mut node = Node::new(auto_addr(), 0).expect("Node::new() failed");
        let handle = node.handle();

        std::thread::scope(|s| {
            s.spawn(|| {
                let joined = handle.join(seed_node_addr).expect("join() failed");
                assert_eq!(joined, true);
            });
            s.spawn(|| {
                for _ in 0..200 {
                    node.poll_one(POLL_TIMEOUT).expect("poll_one() failed");
                }
                assert_ne!(node.id(), Node::<usize>::UNINIT_NODE_ID);
                assert_ne!(node.id(), Node::<usize>::JOINING_NODE_ID);
            });
        });

        assert_ne!(node.id(), NodeId::new(0));
    }

    #[test]
    fn propose_command() {
        let mut clients = Vec::new();

        let mut node0 = Node::new(auto_addr(), 0).expect("Node::new() failed");
        let node0_addr = node0.addr();
        clients.push(node0.handle());

        std::thread::spawn(move || {
            for _ in 0..200 {
                node0.poll_one(POLL_TIMEOUT).expect("poll_one() failed");
            }
        });
        let created = clients[0]
            .create_cluster()
            .expect("create_cluster() failed");
        assert_eq!(created, true);

        for _ in 0..2 {
            let mut node = Node::new(auto_addr(), 0).expect("Node::new() failed");
            clients.push(node.handle());

            std::thread::spawn(move || {
                for _ in 0..200 {
                    node.poll_one(POLL_TIMEOUT).expect("poll_one() failed");
                }
            });

            clients
                .last()
                .unwrap()
                .join(node0_addr)
                .expect("join() failed");
        }

        let result: usize = clients
            .choose(&mut rand::thread_rng())
            .unwrap()
            .propose_command(3)
            .expect("propose_command() failed");

        assert_eq!(result, 3);
    }

    fn auto_addr() -> SocketAddr {
        addr("127.0.0.1:0")
    }

    fn addr(s: &str) -> SocketAddr {
        s.parse().expect("parse addr")
    }
}
