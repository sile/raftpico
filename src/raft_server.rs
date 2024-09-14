use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    time::{Duration, Instant},
};

use mio::{net::TcpListener, Events, Interest, Poll, Token};
use raftbare::{Action, CommitPromise, LogEntry, LogIndex, Node, NodeId, Role};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;

use crate::{
    command::Command,
    connection::Connection,
    io::would_block,
    request::{CreateClusterParams, IncomingMessage, JoinError, JoinParams, Request, Response},
    Machine, ServerStats,
};

const UNINIT_NODE_ID: NodeId = NodeId::new(u64::MAX);

const LISTENER_TOKEN: Token = Token(0);

const DEFAULT_MIN_ELECTION_TIMEOUT: Duration = Duration::from_millis(100);
const DEFAULT_MAX_ELECTION_TIMEOUT: Duration = Duration::from_millis(1000);

#[derive(Debug, Clone)]
pub struct RaftServerOptions {
    pub mio_events_capacity: usize,
    pub rng_seed: u64,
    // TODO: max_write_buf_size
}

impl Default for RaftServerOptions {
    fn default() -> Self {
        Self {
            mio_events_capacity: 1024,
            rng_seed: rand::random(),
        }
    }
}

#[derive(Debug)]
pub struct RaftServer<M> {
    listener: TcpListener,
    addr: SocketAddr,
    next_token: Token,
    connections: HashMap<Token, Connection>,
    poller: Poll,
    events: Option<Events>, // TODO: Remove Option wrapper if possible
    rng: ChaChaRng,
    node: Node,
    election_timeout: Option<Instant>,
    last_applied_index: LogIndex,
    stats: ServerStats,
    commands: BTreeMap<LogIndex, Command>,
    machine: M,

    // TODO: Add a struct for following fields
    min_election_timeout: Duration,
    max_election_timeout: Duration,
    max_log_entries_hint: usize,
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
            next_token: Token(LISTENER_TOKEN.0 + 1),
            connections: HashMap::new(),
            poller,
            events: Some(events),
            rng,
            node: Node::start(UNINIT_NODE_ID),
            machine,
            min_election_timeout: DEFAULT_MIN_ELECTION_TIMEOUT,
            max_election_timeout: DEFAULT_MAX_ELECTION_TIMEOUT,
            max_log_entries_hint: 0,
            election_timeout: None,
            last_applied_index: LogIndex::ZERO,
            stats: ServerStats::default(),
            commands: BTreeMap::new(),
        })
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
            self.handle_action(action);
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
                IncomingMessage::External(req) => self.handle_external_request(conn, req)?,
            }
        }

        conn.poll_send()?;
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
            Request::Join { id, params, .. } => {
                let result = self.handle_join(params);
                let response = Response::join(id, result);
                conn.send(&response)?;
            }
        }

        Ok(())
    }

    fn handle_join(&mut self, params: JoinParams) -> Result<(), JoinError> {
        todo!();
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
        // TODO: if self.pending_query.is_some() {}

        let promise = self.node.propose_command();
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

    fn apply(&mut self, index: LogIndex) -> std::io::Result<()> {
        match self.node.log().entries().get_entry(index) {
            Some(LogEntry::Term(_) | LogEntry::ClusterConfig(_)) => {
                // Do nothing.
                Ok(())
            }
            Some(LogEntry::Command) => {
                todo!()
            }
            None => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "There is no log entry associated with commit index {}",
                    index.get()
                ),
            )),
        }
    }

    fn handle_action(&mut self, action: Action) {
        match action {
            Action::SetElectionTimeout => self.handle_set_election_timeout(),
            Action::SaveCurrentTerm | Action::SaveVotedFor | Action::AppendLogEntries(_) => {
                // Do nothing as this crate uses in-memory storage.
            }
            Action::BroadcastMessage(_) => todo!(),
            Action::SendMessage(_, _) => todo!(),
            Action::InstallSnapshot(_) => todo!(),
        }
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
