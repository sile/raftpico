use std::{
    collections::{BTreeMap, BinaryHeap, HashMap},
    net::{Shutdown, SocketAddr},
    time::{Duration, Instant},
};

use jsonlrpc::{JsonRpcVersion, RequestId};
use mio::{net::TcpListener, Events, Interest, Poll, Token};
use raftbare::{
    Action, ClusterConfig, CommitPromise, LogEntry, LogIndex, LogPosition, Message, Node, NodeId,
    Role,
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
        MemberJson, OutgoingMessage, OutputError, OutputResult, ProposeParams, ProposeResult,
        RemoveServerError, RemoveServerParams, RemoveServerResult, Request, Response,
        SnapshotParams,
    },
    Context, InputKind, Machine, Result, ServerStats,
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
    pub query: Option<serde_json::Value>,
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
        (p0.term.get(), p0.index.get(), other.query.is_some()).cmp(&(
            p1.term.get(),
            p1.index.get(),
            self.query.is_some(),
        ))
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
    pub inviting: bool, // TODO: delete?
    pub evicting: bool,

    // skip serialization => TODO: remove this field from this struct
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

    ongoing_proposes: HashMap<RequestId, (Token, RequestId, Option<serde_json::Value>)>,

    machine: M,

    // TODO: Add a struct for following fields
    min_election_timeout: Duration,
    max_election_timeout: Duration,
    max_log_entries_hint: usize,
    members: BTreeMap<NodeId, Member>,
    next_node_id: NodeId,
}

impl<M: Machine> RaftServer<M> {
    pub fn start(listen_addr: SocketAddr, machine: M) -> Result<Self> {
        Self::with_options(listen_addr, machine, RaftServerOptions::default())
    }

    pub fn with_options(
        listen_addr: SocketAddr,
        machine: M,
        options: RaftServerOptions,
    ) -> Result<Self> {
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

    pub fn poll(&mut self, timeout: Option<Duration>) -> Result<()> {
        self.stats.poll_count += 1;

        let Some(mut events) = self.events.take() else {
            unreachable!();
        };
        let result = self.poll_with_events(&mut events, timeout);
        self.events = Some(events);
        result
    }

    fn poll_with_events(&mut self, events: &mut Events, timeout: Option<Duration>) -> Result<()> {
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
                // TODO: sometimes reaches to here
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
        if self.last_applied_index < self.node.commit_index() {
            self.last_applied_index = self.node.commit_index();

            if self.commands.len() > self.max_log_entries_hint {
                self.install_snapshot();
            }
        }

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

    fn handle_listener_event(&mut self) -> Result<()> {
        loop {
            let Some((mut stream, addr)) = would_block(self.listener.accept().map_err(From::from))?
            else {
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

    fn handle_connection_event_or_deregister(&mut self, mut conn: Connection) -> Result<()> {
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

                // TODO: member.snapshot_required handling (reset sender from this node)

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

    fn handle_connection_event(&mut self, conn: &mut Connection) -> Result<()> {
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
        let (token, request_id, query) = self.ongoing_proposes.remove(&id).expect("TODO");

        // TODO: add note doc about this method is always called before the the time that the promise is accepted
        let response = PendingResponse {
            token,
            request_id,
            commit_promise,
            query,
        };
        self.pending_responses.push(response);
    }

    fn handle_internal_request(
        &mut self,
        conn: &mut Connection,
        req: InternalRequest,
    ) -> Result<()> {
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
            InternalRequest::RequestVoteCall { params, .. } => {
                let msg = params.to_raft_message();
                self.node.handle_message(msg);
            }
            InternalRequest::RequestVoteReply { params, .. } => {
                let msg = params.to_raft_message();
                self.node.handle_message(msg);
            }
            InternalRequest::Snapshot { params, .. } => {
                self.handle_snapshot(params)?;
            }
        }
        Ok(())
    }

    fn handle_snapshot(&mut self, params: SnapshotParams) -> Result<()> {
        self.min_election_timeout = params.min_election_timeout;
        self.max_election_timeout = params.max_election_timeout;
        self.max_log_entries_hint = params.max_log_entries_hint;
        self.next_node_id = params.next_node_id.into();
        self.machine = serde_json::from_value(params.machine)?;

        let mut members = BTreeMap::new();
        for m in params.members {
            let node_id = NodeId::new(m.node_id);
            members.insert(
                node_id,
                Member {
                    node_id: m.node_id.into(),
                    server_addr: m.server_addr,
                    inviting: m.inviting,
                    evicting: m.evicting,
                    token: self.members.get(&node_id).and_then(|m| m.token),
                },
            );
        }
        self.members = members;

        let last_included = LogPosition {
            term: params.last_included_term.into(),
            index: params.last_included_index.into(),
        };
        let config = ClusterConfig {
            voters: params.voters.into_iter().map(NodeId::new).collect(),
            new_voters: params.new_voters.into_iter().map(NodeId::new).collect(),
            ..Default::default()
        };
        self.last_applied_index = last_included.index;

        let ok = self.node.handle_snapshot_installed(last_included, config);
        assert!(ok); // TODO: error handling

        Ok(())
    }

    fn handle_propose(
        &mut self,
        conn: &mut Connection,
        request_id: RequestId,
        params: ProposeParams,
    ) -> Result<()> {
        let promise = self.propose_command(params.command);
        let response = Response::propose_result(request_id, promise);

        // TODO: factor out with self.send_to(); (but need to consider self.connections)
        conn.send(&response).expect("TODO");

        Ok(())
    }

    fn handle_handshake(&mut self, conn: &mut Connection, params: HandshakeParams) -> Result<()> {
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
                evicting: false,
                token: Some(conn.token),
            };
            self.members.insert(params.src_node_id(), member);
        }

        Ok(())
    }

    fn handle_external_request(&mut self, conn: &mut Connection, req: Request) -> Result<()> {
        match req {
            Request::CreateCluster { id, params, .. } => {
                let result = self.handle_create_cluster(params);
                let response = Response::create_cluster(id, result);
                conn.send(&response)?; // TODO: handle error
            }
            Request::AddServer { id, params, .. } => {
                if let Err(e) = self.handle_add_server(conn.token, &id, params) {
                    let response = Response::add_server(id, Err(e));
                    conn.send(&response)?;
                }
            }
            Request::RemoveServer { id, params, .. } => {
                if let Err(e) = self.handle_remove_server(conn.token, &id, params) {
                    let response = Response::remove_server(id, Err(e));
                    conn.send(&response)?;
                }
            }
            Request::Command { id, params, .. } => {
                if let Err(e) = self.handle_command(conn.token, &id, params) {
                    let response = Response::output(id, Err(e));
                    conn.send(&response)?;
                }
            }
            Request::Query { id, params, .. } => {
                if let Err(e) = self.handle_query(conn.token, &id, params) {
                    let response = Response::output(id, Err(e));
                    conn.send(&response)?;
                }
            }
            Request::LocalQuery { id, params, .. } => {
                self.handle_local_query(conn, id, params)?;
            }
        }

        Ok(())
    }

    fn handle_local_query(
        &mut self,
        conn: &mut Connection,
        request_id: RequestId,
        InputParams { input }: InputParams,
    ) -> Result<()> {
        let Ok(input) = serde_json::from_value::<M::Input>(input.clone()) else {
            todo!("response error");
        };
        let mut ctx = Context {
            kind: InputKind::LocalQuery,
            node: &self.node,
            machine_version: self.last_applied_index,
            output: None,
            ignore_output: false,
        };
        self.machine.handle_input(&mut ctx, input);
        let result = match ctx.output {
            None => OutputResult::ok(serde_json::Value::Null),
            Some(Ok(value)) => OutputResult::ok(value),
            Some(Err(_e)) => OutputResult::err(OutputError::InvalidOutput),
        };
        let response = Response::ok(request_id, result);
        conn.send(&response)?; // TODO: handle error

        Ok(())
    }

    fn handle_command(
        &mut self,
        token: Token,
        request_id: &RequestId,
        InputParams { input }: InputParams,
    ) -> std::result::Result<(), OutputError> {
        let command = Command::Command(input);
        self.handle_command_common(token, request_id, command, None)?;
        Ok(())
    }

    fn handle_query(
        &mut self,
        token: Token,
        request_id: &RequestId,
        InputParams { input }: InputParams,
    ) -> std::result::Result<(), OutputError> {
        let command = Command::Query;
        self.handle_command_common(token, request_id, command, Some(input))?;
        Ok(())
    }

    // TODO: rename
    fn handle_command_common(
        &mut self,
        token: Token,
        request_id: &RequestId,
        command: Command,
        query: Option<serde_json::Value>,
    ) -> std::result::Result<(), CommonError> {
        if self.node.id() >= RESERVED_NODE_ID_START {
            return Err(CommonError::ServerNotReady);
        }

        if !self.node.role().is_leader() {
            if self.node.role().is_candidate() {
                return Err(CommonError::LeaderNotKnown);
            }

            let Some(leader) = self.node.voted_for() else {
                return Err(CommonError::LeaderNotKnown);
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
                .insert(id.clone(), (token, request_id.clone(), query));
            return Ok(());
        }

        let commit_promise = self.propose_command(command);

        let response = PendingResponse {
            token,
            request_id: request_id.clone(),
            commit_promise,
            query,
        };
        self.pending_responses.push(response);
        Ok(())
    }

    fn handle_add_server(
        &mut self,
        token: Token,
        request_id: &RequestId,
        AddServerParams { server_addr }: AddServerParams,
    ) -> std::result::Result<(), AddServerError> {
        let command = Command::InviteServer { server_addr };
        self.handle_command_common(token, request_id, command, None)?;
        Ok(())
    }

    fn handle_remove_server(
        &mut self,
        token: Token,
        request_id: &RequestId,
        RemoveServerParams { server_addr }: RemoveServerParams,
    ) -> std::result::Result<(), RemoveServerError> {
        let command = Command::EvictServer { server_addr };
        self.handle_command_common(token, request_id, command, None)?;
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
        }
        promise
    }

    fn install_snapshot(&mut self) {
        let index = self.node.commit_index();
        let (position, config) = self
            .node
            .log()
            .get_position_and_config(index)
            .expect("unreachable");
        let success = self
            .node
            .handle_snapshot_installed(position, config.clone());
        assert!(success);
        self.commands = self.commands.split_off(&(index + LogIndex::new(1)));
    }

    fn next_token(&mut self) -> Token {
        let token = self.next_token;
        self.next_token = Token(self.next_token.0.wrapping_add(1));
        while self.next_token == LISTENER_TOKEN || self.connections.contains_key(&self.next_token) {
            self.next_token = Token(self.next_token.0.wrapping_add(1));
        }
        token
    }

    fn send_pending_error_response(&mut self, pending: PendingResponse) -> Result<()> {
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

    fn send_to<T: OutgoingMessage>(&mut self, token: Token, msg: &T) -> Result<()> {
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

    fn handle_evicted(&mut self, node_id: NodeId) -> Result<()> {
        let member = self.members.remove(&node_id).expect("unreachable");
        if self.node.id() == node_id {
            self.node = Node::start(UNINIT_NODE_ID);
            self.commands.clear();
            // TODO: reset other state
        }
        if let Some(token) = member.token {
            if let Some(mut conn) = self.connections.remove(&token) {
                let _ = conn.stream_mut().shutdown(Shutdown::Both);
                self.poller.registry().deregister(conn.stream_mut())?;
            }
        }
        Ok(())
    }

    fn apply(&mut self, index: LogIndex) -> Result<()> {
        let mut pending_response = None;
        while let Some(pending) = self.pending_responses.peek() {
            match pending.commit_promise.clone().poll(&self.node) {
                CommitPromise::Rejected(_) => {
                    let pending = self.pending_responses.pop().expect("unreachable");
                    self.send_pending_error_response(pending)?;
                }
                CommitPromise::Accepted(position)
                    if index == position.index && pending.query.is_some() =>
                {
                    // TODO: refactor
                    let mut pending = self.pending_responses.pop().expect("unreachable");
                    let input = pending.query.take().expect("unreachable");
                    let mut ctx = Context {
                        kind: InputKind::Query,
                        node: &self.node,
                        machine_version: LogIndex::new(index.get() - 1),
                        output: None,
                        ignore_output: false,
                    };
                    let Ok(input) = serde_json::from_value::<M::Input>(input) else {
                        todo!("response error");
                    };
                    self.machine.handle_input(&mut ctx, input);
                    let result = match ctx.output {
                        None => OutputResult::ok(serde_json::Value::Null),
                        Some(Ok(value)) => OutputResult::ok(value),
                        Some(Err(_e)) => OutputResult::err(OutputError::InvalidOutput),
                    };
                    self.try_response_to(Some(pending), result)?;
                }
                CommitPromise::Accepted(position) if index == position.index => {
                    pending_response = self.pending_responses.pop();
                    break;
                }
                _ => break,
            }
        }

        match self.node.log().entries().get_entry(index) {
            Some(LogEntry::Term(_)) => {
                self.maybe_update_cluster_config();
            }
            Some(LogEntry::ClusterConfig(c)) => {
                if c.is_joint_consensus() {
                    let mut evicted = Vec::new();
                    for m in self.members.values() {
                        if m.evicting && !c.new_voters.contains(&m.node_id) {
                            evicted.push(m.node_id);
                        }
                    }
                    for id in evicted {
                        self.handle_evicted(id)?;
                    }
                }
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
                )
                .into());
            }
        }
        Ok(())
    }

    fn apply_command(&mut self, index: LogIndex, pending: Option<PendingResponse>) -> Result<()> {
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
                    evicting: false,
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
                        evicting: false,
                        token: None,
                    };
                    self.members.insert(node_id, member);
                    self.next_node_id = NodeId::new(node_id.get() + 1);
                    self.maybe_update_cluster_config();
                    AddServerResult::ok()
                };
                self.try_response_to(pending, result)?;
            }
            Command::EvictServer { server_addr } => {
                let result = if let Some(node_id) = self
                    .members
                    .values()
                    .find(|m| m.server_addr == *server_addr)
                    .map(|m| m.node_id)
                {
                    let member = self.members.get_mut(&node_id).expect("unreachable");
                    member.evicting = true;
                    self.maybe_update_cluster_config();
                    RemoveServerResult::ok()
                } else {
                    RemoveServerResult::err(RemoveServerError::NotInCluster)
                };
                self.try_response_to(pending, result)?;
            }
            Command::Command(input_json) => {
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
            Command::Query => {}
        }

        Ok(())
    }

    fn try_response_to<T: Serialize>(
        &mut self,
        pending: Option<PendingResponse>,
        result: T,
    ) -> Result<()> {
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

        // TODO: optimize
        let mut adding = Vec::new();
        let mut removing = Vec::new();
        for (id, m) in self.members.iter() {
            if !m.evicting && !self.node.config().voters.contains(id) {
                adding.push(*id);
            }
        }
        for id in &self.node.config().voters {
            if self.members.get(id).map_or(true, |m| m.evicting) {
                removing.push(*id);
            }
        }
        if adding.is_empty() && removing.is_empty() {
            return;
        }

        let new_config = self.node.config().to_joint_consensus(&adding, &removing);
        let promise = self.node.propose_config(new_config);
        assert!(!promise.is_rejected());
    }

    fn handle_action(&mut self, action: Action) -> Result<()> {
        match action {
            Action::SetElectionTimeout => self.handle_set_election_timeout(),
            Action::SaveCurrentTerm | Action::SaveVotedFor | Action::AppendLogEntries(_) => {
                // Do nothing as this crate uses in-memory storage.
            }
            Action::BroadcastMessage(m) => self.handle_broadcast_message(m)?,
            Action::SendMessage(peer, m) => self.handle_send_message(peer, m)?,
            Action::InstallSnapshot(peer) => self.handle_install_snapshot(peer)?,
        }
        Ok(())
    }

    fn handle_install_snapshot(&mut self, dst: NodeId) -> Result<()> {
        let (last_included, config) = self
            .node
            .log()
            .get_position_and_config(self.node.commit_index())
            .expect("unreachable");
        let members = self
            .members
            .values()
            .map(|m| MemberJson {
                node_id: m.node_id.get(),
                server_addr: m.server_addr,
                inviting: m.inviting,
                evicting: m.evicting,
            })
            .collect();
        let request = InternalRequest::Snapshot {
            jsonrpc: JsonRpcVersion::V2,
            params: SnapshotParams {
                last_included_term: last_included.term.get(),
                last_included_index: last_included.index.get(),
                voters: config.voters.iter().map(|n| n.get()).collect(),
                new_voters: config.new_voters.iter().map(|n| n.get()).collect(),
                min_election_timeout: self.min_election_timeout,
                max_election_timeout: self.max_election_timeout,
                max_log_entries_hint: self.max_log_entries_hint,
                next_node_id: self.next_node_id.get(),
                members,
                machine: serde_json::to_value(&self.machine)?,
            },
        };
        self.internal_send_to(dst, &request, None)?;
        Ok(())
    }

    fn handle_send_message(&mut self, peer: NodeId, msg: Message) -> Result<()> {
        let member = self.members.get(&peer).expect("unreachable");
        let token = if let Some(token) = member.token {
            token
        } else if let Some(token) = self.internal_connect(peer)? {
            token
        } else {
            return Ok(());
        };

        let request = InternalRequest::from_raft_message(msg, &self.commands);
        self.send_to(token, &request)?;
        Ok(())
    }

    fn handle_broadcast_message(&mut self, msg: Message) -> Result<()> {
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
            if let Some(token) = self.internal_connect(peer)? {
                self.send_to(token, &request)?;
            }
        }

        Ok(())
    }

    fn internal_send_to<T: OutgoingMessage>(
        &mut self,
        dst: NodeId,
        msg: &T,
        request_id: Option<RequestId>,
    ) -> Result<()> {
        let member = self.members.get(&dst).expect("unreachable");
        let token = if let Some(token) = member.token {
            token
        } else if let Some(token) = self.internal_connect(dst)? {
            token
        } else {
            return Ok(());
        };
        self.send_to(token, msg)?;
        if let Some(id) = request_id {
            if let Some(conn) = self.connections.get_mut(&token) {
                conn.ongoing_requests.insert(id);
            }
        };
        Ok(())
    }

    fn internal_connect(&mut self, peer: NodeId) -> Result<Option<Token>> {
        let token = self.next_token();
        let member = self.members.get_mut(&peer).expect("unreachable");
        member.token = Some(token);

        let mut conn = Connection::connect(member.server_addr, token)?;

        self.poller
            .registry()
            .register(conn.stream_mut(), token, Interest::WRITABLE)?;
        if let Err(_e) = conn.poll_connect() {
            // TODO: stats
            self.poller.registry().deregister(conn.stream_mut())?;
            return Ok(None);
        }
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
        Ok(Some(token))
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
