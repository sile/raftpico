use std::{
    collections::HashMap,
    net::SocketAddr,
    time::{Duration, Instant},
};

use mio::{net::TcpListener, Events, Interest, Poll, Token};
use raftbare::{Action, LogIndex, Node, NodeId, Role};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;

use crate::{
    connection::Connection, io::would_block, request::IncomingMessage, Machine, ServerStats,
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
    machine: M,
    min_election_timeout: Duration,
    max_election_timeout: Duration,
    election_timeout: Option<Instant>,
    last_applied_index: LogIndex,
    stats: ServerStats,
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
            election_timeout: None,
            last_applied_index: LogIndex::ZERO,
            stats: ServerStats::default(),
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
            } else {
                todo!();
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
            self.connections.insert(conn.token, conn);
        }
        Ok(())
    }

    fn handle_connection_event(&mut self, conn: &mut Connection) -> std::io::Result<()> {
        // Connect.
        if !conn.connected {
            // See: https://docs.rs/mio/1.0.2/mio/net/struct.TcpStream.html#method.connect
            conn.stream().take_error()?;
            match conn.stream().peer_addr() {
                Err(e) if e.kind() == std::io::ErrorKind::NotConnected => return Ok(()),
                result => {
                    result?;
                }
            }
            conn.connected = true;
        }

        // Read.
        while let Some(msg) = conn.recv_message()? {
            todo!();
        }

        // Write.
        if conn.is_writing() {
            // would_block(conn.stream.flush().map_err(|e| e.into()))
            //     .unwrap_or_else(|e| todo!("{:?}", e));
            // if conn.stream.write_buf().is_empty() {
            //     // TODO: or deregister?
            //     self.poller.registry().reregister(
            //         conn.stream.inner_mut(),
            //         event.token(),
            //         Interest::READABLE,
            //     )?;
            // }
            todo!();
        }

        Ok(())
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
        todo!("{:?}", index)
    }

    fn handle_action(&mut self, action: Action) {
        match action {
            Action::SetElectionTimeout => self.handle_set_election_timeout(),
            Action::SaveCurrentTerm | Action::SaveVotedFor => {
                // Do nothing as this crate uses in-memory storage.
            }
            Action::AppendLogEntries(_) => todo!(),
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
