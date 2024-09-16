use std::{
    collections::{BTreeMap, BinaryHeap, HashMap},
    net::SocketAddr,
    time::{Duration, Instant},
};

use jsonlrpc::{JsonRpcVersion, RequestId};
use mio::{net::TcpListener, Events, Interest, Poll, Token};
use raftbare::{
    Action, CommitPromise, LogEntry, LogIndex, LogPosition, Message, Node, NodeId, Role,
};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use serde::Serialize;

use crate::{
    command::Command,
    connection::Connection,
    io::would_block,
    request::{
        AddServerError, AddServerParams, AddServerResult, CommonError, CreateClusterParams,
        HandshakeParams, IncomingMessage, InputParams, InternalIncomingMessage, InternalRequest,
        OutgoingMessage, OutputError, OutputResult, ProposeParams, ProposeResult, Request,
        Response,
    },
    Context, InputKind, Machine, ServerStats,
};

const UNINIT_NODE_ID: NodeId = NodeId::new(u64::MAX);
const RESERVED_NODE_ID_START: NodeId = NodeId::new(u64::MAX / 2);

const LISTENER_TOKEN: Token = Token(0);

const DEFAULT_MIN_ELECTION_TIMEOUT: Duration = Duration::from_millis(100);
const DEFAULT_MAX_ELECTION_TIMEOUT: Duration = Duration::from_millis(1000);

#[derive(Debug)]
pub struct PendingResponse {
    pub token: Token,
    pub request_id: RequestId,
    pub commit_promise: CommitPromise,
}

impl PendingResponse {
    pub fn log_position(&self) -> LogPosition {
        self.commit_promise.log_position()
    }
}

impl PartialOrd for PendingResponse {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PendingResponse {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // [NOTE] Reversed order for BinaryHeap
        let p0 = other.log_position();
        let p1 = self.log_position();
        (p0.term.get(), p0.index.get()).cmp(&(p1.term.get(), p1.index.get()))
    }
}

impl PartialEq for PendingResponse {
    fn eq(&self, other: &Self) -> bool {
        self.log_position() == other.log_position()
    }
}

impl Eq for PendingResponse {}

#[derive(Debug, Clone)]
pub struct RaftServerOptions {
    pub mio_events_capacity: usize,
    pub rng_seed: u64,

    // TODO: rename
    pub max_write_buf_size: usize,
}

impl Default for RaftServerOptions {
    fn default() -> Self {
        Self {
            mio_events_capacity: 1024,
            rng_seed: rand::random(),
            max_write_buf_size: 1024 * 1024,
        }
    }
}

#[derive(Debug)]
pub struct Member {
    pub node_id: NodeId,
    pub server_addr: SocketAddr,
    pub inviting: bool,

    // skip serialization
    pub token: Option<Token>,
}

pub type Commands = BTreeMap<LogIndex, Command>;

#[derive(Debug)]
pub struct RaftServer<M> {
    listener: TcpListener,
    addr: SocketAddr,
    next_token: Token,
    next_request_id: i64,
    connections: HashMap<Token, Connection>,
    poller: Poll,
    events: Option<Events>, // TODO: Remove Option wrapper if possible
    rng: ChaChaRng,
    node: Node,
    election_timeout: Option<Instant>,
    last_applied_index: LogIndex,
    stats: ServerStats,
    commands: Commands,
    pending_responses: BinaryHeap<PendingResponse>,
    max_write_buf_size: usize,

    ongoing_proposes: HashMap<RequestId, (Token, RequestId)>,

    machine: M,

    // TODO: Add a struct for following fields
    min_election_timeout: Duration,
    max_election_timeout: Duration,
    max_log_entries_hint: usize,
    members: BTreeMap<NodeId, Member>,
    next_node_id: NodeId,
}

impl<M: Machine> RaftServer<M> {
    pub fn start(listen_addr: SocketAddr, machine: M) -> std::io::Result<Self> {
        Self::with_options(listen_addr, machine, RaftServerOptions::default())
    }

    pub fn with_options(
        listen_addr: SocketAddr,
        machine: M,
        options: RaftServerOptions,
    ) -> std::io::Result<Self> {
        let mut listener = TcpListener::bind(listen_addr)?;
        let addr = listener.local_addr()?;

        let poller = Poll::new()?;
        let events = Events::with_capacity(options.mio_events_capacity);
        poller
            .registry()
            .register(&mut listener, LISTENER_TOKEN, Interest::READABLE)?;

        let rng = ChaChaRng::seed_from_u64(options.rng_seed);

        Ok(Self {
            listener,
            addr,
            next_token: Token(LISTENER_TOKEN.0 + 1), // TODO: randomize?
            next_request_id: 0,
            connections: HashMap::new(),
            poller,
            events: Some(events),
            rng,
            max_write_buf_size: options.max_write_buf_size,
            ongoing_proposes: HashMap::new(),
            node: Node::start(UNINIT_NODE_ID),
            election_timeout: None,
            last_applied_index: LogIndex::ZERO,
            stats: ServerStats::default(),
            commands: BTreeMap::new(),
            pending_responses: BinaryHeap::new(),

            // Replicated state
            machine,
            min_election_timeout: DEFAULT_MIN_ELECTION_TIMEOUT,
            max_election_timeout: DEFAULT_MAX_ELECTION_TIMEOUT,
            max_log_entries_hint: 0,
            members: BTreeMap::new(),
            next_node_id: NodeId::new(0),
        })
    }

    fn next_request_id(&mut self) -> RequestId {
        let id = RequestId::Number(self.next_request_id);
        self.next_request_id += 1;
        id
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn node(&self) -> Option<&Node> {
        (!matches!(self.node.id(), UNINIT_NODE_ID)).then(|| &self.node)
    }

    pub fn machine(&self) -> &M {
        &self.machine
    }

    // TODO: Return Stats
    pub fn stats(&self) -> &ServerStats {
        &self.stats
    }

    pub fn poll(&mut self, timeout: Option<Duration>) -> std::io::Result<()> {
        self.stats.poll_count += 1;

        let Some(mut events) = self.events.take() else {
            unreachable!();
        };
        let result = self.poll_with_events(&mut events, timeout);
        self.events = Some(events);
        result
    }

    fn poll_with_events(
        &mut self,
        events: &mut Events,
        timeout: Option<Duration>,
    ) -> std::io::Result<()> {
        // Timeout and I/O events handling.
        let now = Instant::now();
        let election_timeout = self
            .election_timeout
            .map(|time| time.saturating_duration_since(now));
        if let Some(election_timeout) = election_timeout.filter(|&t| t <= timeout.unwrap_or(t)) {
            self.poller.poll(events, Some(election_timeout))?;
            if events.is_empty() {
                self.stats.election_timeout_expired_count += 1;
                self.node.handle_election_timeout();
            }
        } else {
            self.poller.poll(events, timeout)?;
        }

        for event in events.iter() {
            if event.token() == LISTENER_TOKEN {
                self.handle_listener_event()?;
            } else if let Some(connection) = self.connections.remove(&event.token()) {
                // TODO: Dont remove if possible
                self.handle_connection_event_or_deregister(connection)?;
            } else {
                unreachable!();
            }
        }

        // Committed log entries handling.
        for index in self.last_applied_index.get() + 1..=self.node.commit_index().get() {
            let index = LogIndex::new(index);
            self.apply(index)?;
        }
        if self.last_applied_index != self.node.commit_index() && self.node.role().is_leader() {
            // Quickly notify followers about the latest commit index.
            self.node.heartbeat();
        }
        self.last_applied_index = self.node.commit_index();

        // Node actions handling.
        //
        // [NOTE]
        // To consolidate Raft actions as much as possible,
        // the following code is positioned at the end of this method.
        while let Some(action) = self.node.actions_mut().next() {
            self.handle_action(action)?;
        }

        Ok(())
    }

    fn handle_listener_event(&mut self) -> std::io::Result<()> {
        loop {
            let Some((mut stream, addr)) = would_block(self.listener.accept())? else {
                return Ok(());
            };
            self.stats.accept_count += 1;

            let _ = stream.set_nodelay(true);
            let token = self.next_token();

            self.poller
                .registry()
                .register(&mut stream, token, Interest::READABLE)?;

            let connection = Connection::new_connected(addr, token, stream);
            self.handle_connection_event_or_deregister(connection)?;
        }
    }

    fn handle_connection_event_or_deregister(
        &mut self,
        mut conn: Connection,
    ) -> std::io::Result<()> {
        if let Err(_e) = self.handle_connection_event(&mut conn) {
            // TODO: count stats depending on the error kind
            self.poller.registry().deregister(conn.stream_mut())?;

            // TODO: factor out
            if let Some(member) = self
                .members
                .values_mut()
                .find(|m| m.token == Some(conn.token))
            {
                member.token = None;
                for _ in conn.ongoing_requests {
                    // TODO: response error
                    todo!();
                }
            }
        } else {
            let token = conn.token;
            if let Some(interest) = conn.interest.take() {
                self.poller
                    .registry()
                    .reregister(conn.stream_mut(), token, interest)?;
            }
            self.connections.insert(token, conn);
        }
        Ok(())
    }

    fn handle_connection_event(&mut self, conn: &mut Connection) -> std::io::Result<()> {
        if !conn.poll_connect()? {
            return Ok(());
        }

        while let Some(msg) = conn.poll_recv()? {
            match msg {
                IncomingMessage::ExternalRequest(req) => self.handle_external_request(conn, req)?,
                IncomingMessage::Internal(msg) => match msg {
                    InternalIncomingMessage::Request(req) => {
                        self.handle_internal_request(conn, req)?
                    }
                    InternalIncomingMessage::Response(response) => {
                        if let Some(id) = response.id() {
                            conn.ongoing_requests.remove(id);
                        }
                        self.handle_propose_response(response);
                    }
                },
            }
        }

        conn.poll_send()?;
        Ok(())
    }

    fn handle_propose_response(&mut self, response: Response<ProposeResult>) {
        let id = response.id().expect("TODO").clone();
        let commit_promise = response.into_std_result().expect("TODO").to_promise();
        let (token, request_id) = self.ongoing_proposes.remove(&id).expect("TODO");

        // TODO: add note doc about this method is always called before the the time that the promise is accepted
        let response = PendingResponse {
            token,
            request_id,
            commit_promise,
        };
        self.pending_responses.push(response);
        dbg!(self.node.id().get());
        dbg!(commit_promise);
        dbg!(self.pending_responses.len());
    }

    fn handle_internal_request(
        &mut self,
        conn: &mut Connection,
        req: InternalRequest,
    ) -> std::io::Result<()> {
        match req {
            InternalRequest::Handshake { params, .. } => self.handle_handshake(conn, params)?,
            InternalRequest::Propose { id, params, .. } => self.handle_propose(conn, id, params)?,
            InternalRequest::AppendEntriesCall { params, .. } => {
                let msg = params.to_raft_message(&mut self.commands);
                self.node.handle_message(msg);
            }
            InternalRequest::AppendEntriesReply { params, .. } => {
                let msg = params.to_raft_message();
                self.node.handle_message(msg);
            }
        }
        Ok(())
    }

    fn handle_propose(
        &mut self,
        conn: &mut Connection,
        request_id: RequestId,
        params: ProposeParams,
    ) -> std::io::Result<()> {
        let promise = self.propose_command(params.command);
        let response = Response::propose_result(request_id, promise);

        // TODO: factor out with self.send_to(); (but need to consider self.connections)
        conn.send(&response).expect("TODO");

        Ok(())
    }

    fn handle_handshake(
        &mut self,
        conn: &mut Connection,
        params: HandshakeParams,
    ) -> std::io::Result<()> {
        if !params.inviting && self.node().is_none() {
            // TODO: handle restarted case
            todo!();
        }
        if self.node().is_none() {
            // TOOD: note
            self.node = Node::start(params.dst_node_id());
        }
        if params.dst_node_id() != self.node.id() {
            todo!();
        }

        if !self.members.contains_key(&params.src_node_id()) {
            // TODO: add note doc
            let member = Member {
                node_id: params.src_node_id(),
                server_addr: conn.addr, // TODO: params.src_addr()
                inviting: false,
                token: Some(conn.token),
            };
            self.members.insert(params.src_node_id(), member);
        }

        Ok(())
    }

    fn handle_external_request(
        &mut self,
        conn: &mut Connection,
        req: Request,
    ) -> std::io::Result<()> {
        match req {
            Request::CreateCluster { id, params, .. } => {
                let result = self.handle_create_cluster(params);
                let response = Response::create_cluster(id, result);
                conn.send(&response)?;
            }
            Request::AddServer { id, params, .. } => {
                if let Err(e) = self.handle_add_server(conn.token, &id, params) {
                    let response = Response::add_server(id, Err(e));
                    conn.send(&response)?;
                }
            }
            Request::Command { id, params, .. } => {
                if let Err(e) = self.handle_command(conn.token, &id, params) {
                    let response = Response::output(id, Err(e));
                    conn.send(&response)?;
                }
            }
        }

        Ok(())
    }

    fn handle_command(
        &mut self,
        token: Token,
        request_id: &RequestId,
        InputParams { input }: InputParams,
    ) -> Result<(), OutputError> {
        if self.node.id() >= RESERVED_NODE_ID_START {
            return Err(OutputError::ServerNotReady);
        }

        let command = Command::Command(input);

        // TODO: factor out with handle_add_server()
        if !self.node.role().is_leader() {
            if self.node.role().is_candidate() {
                return Err(OutputError::LeaderNotKnown);
            }

            let Some(leader) = self.node.voted_for() else {
                return Err(OutputError::LeaderNotKnown);
            };

            let id = self.next_request_id();
            let request = InternalRequest::Propose {
                jsonrpc: JsonRpcVersion::V2,
                id: id.clone(),
                params: ProposeParams { command },
            };
            self.internal_send_to(leader, &request, Some(id.clone()))
                .expect("TODO");
            self.ongoing_proposes
                .insert(id.clone(), (token, request_id.clone()));
            return Ok(());
        }

        let commit_promise = self.propose_command(command);

        let response = PendingResponse {
            token,
            request_id: request_id.clone(),
            commit_promise,
        };
        self.pending_responses.push(response);
        Ok(())
    }

    fn handle_add_server(
        &mut self,
        token: Token,
        request_id: &RequestId,
        AddServerParams { server_addr }: AddServerParams,
    ) -> Result<(), AddServerError> {
        if self.node.id() >= RESERVED_NODE_ID_START {
            return Err(AddServerError::ServerNotReady);
        }

        if !self.node.role().is_leader() {
            // TOOD: remote propos
            todo!();
        }

        let command = Command::InviteServer { server_addr };
        let commit_promise = self.propose_command(command);

        let response = PendingResponse {
            token,
            request_id: request_id.clone(),
            commit_promise,
        };
        self.pending_responses.push(response);
        Ok(())
    }

    fn handle_create_cluster(&mut self, params: CreateClusterParams) -> bool {
        if self.node.id() != UNINIT_NODE_ID {
            return false;
        }

        self.min_election_timeout = Duration::from_millis(params.min_election_timeout_ms as u64);
        self.max_election_timeout = Duration::from_millis(params.max_election_timeout_ms as u64);
        self.max_log_entries_hint = params.max_log_entries_hint;

        let node_id = NodeId::new(0);
        self.node = Node::start(node_id);

        let mut promise = self.node.create_cluster(&[node_id]);
        promise.poll(&mut self.node);
        assert!(promise.is_accepted());

        let command = Command::InitCluster {
            server_addr: self.addr,
            min_election_timeout: self.min_election_timeout,
            max_election_timeout: self.max_election_timeout,
            max_log_entries_hint: self.max_log_entries_hint,
        };
        let mut promise = self.propose_command(command);
        promise.poll(&mut self.node);
        assert!(promise.is_accepted());

        true
    }

    fn propose_command(&mut self, command: Command) -> CommitPromise {
        //debug_assert!(self.node.role().is_leader());

        // TODO: if self.pending_query.is_some() { merge() }

        let promise = self.node.propose_command();
        //debug_assert!(!promise.is_rejected());
        if !promise.is_rejected() {
            self.commands.insert(promise.log_position().index, command);
            if self.commands.len() > self.max_log_entries_hint {
                todo!();
            }
        }
        promise
    }

    fn next_token(&mut self) -> Token {
        let token = self.next_token;
        self.next_token = Token(self.next_token.0.wrapping_add(1));
        while self.next_token == LISTENER_TOKEN || self.connections.contains_key(&self.next_token) {
            self.next_token = Token(self.next_token.0.wrapping_add(1));
        }
        token
    }

    fn send_pending_error_response(&mut self, pending: PendingResponse) -> std::io::Result<()> {
        #[derive(Default, Serialize)]
        struct Error {
            success: bool,
            error: CommonError,
        }
        self.send_to(
            pending.token,
            &Response::ok(pending.request_id, Error::default()),
        )?;
        Ok(())
    }

    fn send_to<T: OutgoingMessage>(&mut self, token: Token, msg: &T) -> std::io::Result<()> {
        let Some(conn) = self.connections.get_mut(&token) else {
            // Already disconnected.
            return Ok(());
        };

        if conn.pending_write_size() > self.max_write_buf_size && !msg.is_mandatory() {
            // TOD: stats
            return Ok(());
        }

        if let Err(_e) = conn.send(msg) {
            // TODO: count stats depending on the error kind
            self.poller.registry().deregister(conn.stream_mut())?;
            let conn = self.connections.remove(&token).expect("unreachable");

            // TODO:
            if let Some(member) = self.members.values_mut().find(|m| m.token == Some(token)) {
                member.token = None;
                for _ in conn.ongoing_requests {
                    // TODO: response error
                    todo!();
                }
            }

            return Ok(());
        }

        if let Some(interest) = conn.interest.take() {
            self.poller
                .registry()
                .reregister(conn.stream_mut(), token, interest)?;
        }

        Ok(())
    }

    fn apply(&mut self, index: LogIndex) -> std::io::Result<()> {
        let mut pending_response = None;
        while let Some(pending) = self.pending_responses.peek() {
            match pending.commit_promise.clone().poll(&self.node) {
                CommitPromise::Rejected(_) => {
                    let pending = self.pending_responses.pop().expect("unreachable");
                    self.send_pending_error_response(pending)?;
                }
                CommitPromise::Accepted(position) if index == position.index => {
                    pending_response = self.pending_responses.pop();
                    break;
                }
                _ => break,
            }
        }

        match self.node.log().entries().get_entry(index) {
            Some(LogEntry::Term(_) | LogEntry::ClusterConfig(_)) => {
                self.maybe_update_cluster_config();
            }
            Some(LogEntry::Command) => {
                self.apply_command(index, pending_response)?;
            }
            None => {
                return Err(std::io::Error::new(
                    // TODO: unreachable?
                    std::io::ErrorKind::Other,
                    format!(
                        "There is no log entry associated with commit index {}",
                        index.get()
                    ),
                ));
            }
        }
        Ok(())
    }

    fn apply_command(
        &mut self,
        index: LogIndex,
        pending: Option<PendingResponse>,
    ) -> std::io::Result<()> {
        let Some(command) = self.commands.get(&index) else {
            unreachable!("bug");
        };

        match command {
            Command::InitCluster {
                server_addr,
                min_election_timeout,
                max_election_timeout,
                max_log_entries_hint,
            } => {
                debug_assert!(pending.is_none());

                self.min_election_timeout = *min_election_timeout;
                self.max_election_timeout = *max_election_timeout;
                self.max_log_entries_hint = *max_log_entries_hint;

                let node_id = self.next_node_id;
                let member = Member {
                    node_id,
                    server_addr: *server_addr,
                    inviting: false,
                    token: None,
                };
                self.members.insert(node_id, member);
                self.next_node_id = NodeId::new(node_id.get() + 1);
            }
            Command::InviteServer { server_addr } => {
                let result = if self
                    .members
                    .values()
                    .find(|m| m.server_addr == *server_addr)
                    .is_some()
                {
                    AddServerResult::err(AddServerError::AlreadyInCluster)
                } else {
                    let node_id = self.next_node_id;
                    let member = Member {
                        node_id,
                        server_addr: *server_addr,
                        inviting: true,
                        token: None,
                    };
                    self.members.insert(node_id, member);
                    self.next_node_id = NodeId::new(node_id.get() + 1);
                    self.maybe_update_cluster_config();
                    AddServerResult::ok()
                };
                self.try_response_to(pending, result)?;
            }
            Command::Command(input_json) => {
                dbg!(self.node.id().get());
                dbg!(index.get());
                dbg!(pending.is_some());
                dbg!(&input_json);
                let Ok(input) = serde_json::from_value::<M::Input>(input_json.clone()) else {
                    todo!("response error");
                };
                let mut ctx = Context {
                    kind: InputKind::Command,
                    node: &self.node,
                    machine_version: LogIndex::new(index.get() - 1),
                    output: None,
                    ignore_output: pending.is_none(),
                };
                self.machine.handle_input(&mut ctx, input);
                if pending.is_some() {
                    let result = match ctx.output {
                        None => OutputResult::ok(serde_json::Value::Null),
                        Some(Ok(value)) => OutputResult::ok(value),
                        Some(Err(_e)) => OutputResult::err(OutputError::InvalidOutput),
                    };
                    self.try_response_to(pending, result)?;
                }
            }
        }

        Ok(())
    }

    fn try_response_to<T: Serialize>(
        &mut self,
        pending: Option<PendingResponse>,
        result: T,
    ) -> std::io::Result<()> {
        let Some(pending) = pending else {
            return Ok(());
        };

        let response = Response::ok(pending.request_id, result);
        let token = pending.token;
        self.send_to(token, &response)?;
        Ok(())
    }

    fn maybe_update_cluster_config(&mut self) {
        if !self.node.role().is_leader() {
            return;
        }
        if self.node.config().is_joint_consensus() {
            return;
        }

        let mut adding = Vec::new();
        let mut removing = Vec::new();
        for id in self.members.keys() {
            if !self.node.config().voters.contains(id) {
                adding.push(*id);
            }
        }
        for id in &self.node.config().voters {
            if !self.members.contains_key(id) {
                removing.push(*id);
            }
        }

        let new_config = self.node.config().to_joint_consensus(&adding, &removing);
        let promise = self.node.propose_config(new_config);
        assert!(!promise.is_rejected());
    }

    fn handle_action(&mut self, action: Action) -> std::io::Result<()> {
        match action {
            Action::SetElectionTimeout => self.handle_set_election_timeout(),
            Action::SaveCurrentTerm | Action::SaveVotedFor | Action::AppendLogEntries(_) => {
                // Do nothing as this crate uses in-memory storage.
            }
            Action::BroadcastMessage(m) => self.handle_broadcast_message(m)?,
            Action::SendMessage(peer, m) => self.handle_send_message(peer, m)?,
            Action::InstallSnapshot(_) => todo!(),
        }
        Ok(())
    }

    fn handle_send_message(&mut self, peer: NodeId, msg: Message) -> std::io::Result<()> {
        let member = self.members.get(&peer).expect("unreachable");
        let token = if let Some(token) = member.token {
            token
        } else {
            self.internal_connect(peer)?
        };

        let request = InternalRequest::from_raft_message(msg, &self.commands);
        self.send_to(token, &request)?;
        Ok(())
    }

    fn handle_broadcast_message(&mut self, msg: Message) -> std::io::Result<()> {
        let request = InternalRequest::from_raft_message(msg, &self.commands);
        let mut unconnected = Vec::new();
        let peers = self.node.peers().collect::<Vec<_>>(); // TODO:remove
        for peer in peers {
            let Some(member) = self.members.get(&peer) else {
                unreachable!();
            };
            let Some(token) = member.token else {
                unconnected.push(peer);
                continue;
            };
            // if member.inviting {
            //     // TODO: stats
            //     continue;
            // }
            self.send_to(token, &request)?;
        }

        for peer in unconnected {
            let token = self.internal_connect(peer)?;
            self.send_to(token, &request)?;
        }

        Ok(())
    }

    fn internal_send_to<T: OutgoingMessage>(
        &mut self,
        dst: NodeId,
        msg: &T,
        request_id: Option<RequestId>,
    ) -> std::io::Result<()> {
        let member = self.members.get(&dst).expect("unreachable");
        let token = if let Some(token) = member.token {
            token
        } else {
            self.internal_connect(dst)?
        };
        self.send_to(token, msg)?;
        if let Some(id) = request_id {
            if let Some(conn) = self.connections.get_mut(&token) {
                conn.ongoing_requests.insert(id);
            }
        };
        Ok(())
    }

    fn internal_connect(&mut self, peer: NodeId) -> std::io::Result<Token> {
        let token = self.next_token();
        let member = self.members.get_mut(&peer).expect("unreachable");
        member.token = Some(token);

        let mut conn = Connection::connect(member.server_addr, token)?;

        self.poller
            .registry()
            .register(conn.stream_mut(), token, Interest::WRITABLE)?;
        conn.poll_connect()?; // TODO: deregister if failed
        self.connections.insert(token, conn);

        let request = InternalRequest::Handshake {
            jsonrpc: JsonRpcVersion::V2,
            params: HandshakeParams {
                src_node_id: self.node.id().get(),
                dst_node_id: peer.get(),
                inviting: member.inviting,
            },
        };
        self.send_to(token, &request)?;
        Ok(token)
    }

    fn handle_set_election_timeout(&mut self) {
        self.stats.election_timeout_set_count += 1;

        let min = self.min_election_timeout;
        let max = self.max_election_timeout;
        let timeout = match self.node.role() {
            Role::Follower => max,
            Role::Candidate => self.rng.gen_range(min..=max),
            Role::Leader => min,
        };
        self.election_timeout = Some(Instant::now() + timeout);
    }
}
