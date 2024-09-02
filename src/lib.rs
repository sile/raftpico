use message::{Message, SystemCommand};
use raftbare::{
    Action, CommitPromise, LogEntries, LogIndex, LogPosition, Node as BareNode, NodeId, Role,
};
use rand::Rng;
use std::{
    collections::BTreeMap,
    io::{Error, ErrorKind},
    net::{SocketAddr, UdpSocket},
    time::{Duration, Instant},
};

pub mod message;

pub trait Machine: Sized + Default {
    type Command: Command;

    fn apply(&mut self, command: Self::Command);

    fn encode(&self, buf: &mut Vec<u8>);
    fn decode(buf: &[u8]) -> Self;
}

pub trait Command: Sized {
    fn encode(&self, buf: &mut Vec<u8>);
    fn decode(buf: &[u8]) -> Self;
}

// TODO: remove old entries
pub type CommandLog = BTreeMap<LogIndex, SystemCommand>;

#[derive(Debug)]
pub struct RaftNode<M: Machine> {
    machine: M,
    bare_node: BareNode,
    socket: UdpSocket,
    local_addr: SocketAddr,
    seqno: u32,
    command_log: CommandLog,
    election_timeout: Option<Instant>,

    join_promise: Option<CommitPromise>, // TODO

    // raft state machine (system)
    next_node_id: NodeId,
    peer_addrs: BTreeMap<NodeId, SocketAddr>,
}

impl<M: Machine> RaftNode<M> {
    pub const UNINIT_NODE_ID: NodeId = NodeId::new(u64::MAX);

    pub fn new(node_addr: SocketAddr) -> std::io::Result<Self> {
        let socket = UdpSocket::bind(node_addr)?;
        socket.set_nonblocking(true)?;
        let local_addr = socket.local_addr()?;

        Ok(Self {
            machine: M::default(),
            bare_node: BareNode::start(Self::UNINIT_NODE_ID),
            socket,
            local_addr,
            seqno: 0,
            command_log: CommandLog::new(),
            election_timeout: None,

            join_promise: None,

            next_node_id: NodeId::new(1),
            peer_addrs: BTreeMap::new(),
        })
    }

    pub fn create_cluster(&mut self) -> std::io::Result<()> {
        if self.node_id() != Self::UNINIT_NODE_ID {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Already initialized node",
            ));
        }

        let seed_node_id = NodeId::new(0);
        self.bare_node = BareNode::start(seed_node_id);

        let mut promise = self.bare_node.create_cluster(&[seed_node_id]);
        assert!(!promise.is_rejected());

        while promise.poll(&self.bare_node).is_pending() {
            while let Some(action) = self.bare_node.actions_mut().next() {
                self.handle_action(action);
            }
        }
        assert!(promise.is_accepted());

        Ok(())
    }

    fn next_seqno(&mut self) -> u32 {
        let seqno = self.seqno;
        self.seqno += 1;
        seqno
    }

    pub fn join(
        &mut self,
        contact_node_addr: SocketAddr,
        timeout: Option<Duration>,
    ) -> std::io::Result<()> {
        if self.node_id() != Self::UNINIT_NODE_ID {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Already initialized node",
            ));
        }

        let send_msg = Message::JoinCall {
            seqno: self.next_seqno(),
            from: self.local_addr,
        };

        let mut send_buf = Vec::new();
        send_msg.encode(&mut send_buf)?;

        let start_time = Instant::now();
        while timeout.map_or(true, |t| start_time.elapsed() <= t) {
            if self
                .join_promise
                .take_if(|promise| promise.poll(&self.bare_node).is_accepted())
                .is_some()
            {
                // Committed
                return Ok(());
            }

            self.socket.send_to(&send_buf, contact_node_addr)?;
            std::thread::sleep(Duration::from_millis(100)); // TODO

            // TODO: while
            let mut recv_buf = [0; 2048];
            let Some((size, _addr)) = maybe_would_block(self.socket.recv_from(&mut recv_buf))?
            else {
                continue;
            };
            assert!(size >= 5); // seqno (4) + tag (1)
            assert!(size <= 1205); // TODO
            let recv_msg = Message::decode(&recv_buf[..size])?;
            self.handle_message(recv_msg)?;
        }

        Err(Error::new(ErrorKind::TimedOut, "join timed out"))
    }

    pub fn run_while<F>(&mut self, condition: F) -> std::io::Result<()>
    where
        F: Fn() -> bool,
    {
        while condition() {
            self.run_one()?;
            std::thread::sleep(Duration::from_millis(10)); // TODO
        }
        Ok(())
    }

    fn run_one(&mut self) -> std::io::Result<()> {
        // TODO: propose follower heartbeat command periodically

        let mut recv_buf = [0; 2048];
        if let Some((size, _addr)) = maybe_would_block(self.socket.recv_from(&mut recv_buf))? {
            assert!(size >= 5); // seqno (4) + tag (1)
            assert!(size <= 1205); // TODO

            let recv_msg = Message::decode(&recv_buf[..size])?;
            self.handle_message(recv_msg)?;
        }

        while let Some(action) = self.bare_node.actions_mut().next() {
            self.handle_action(action);
        }

        Ok(())
    }

    pub fn node_id(&self) -> NodeId {
        self.bare_node.id()
    }

    pub fn role(&self) -> Role {
        self.bare_node.role()
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn machine(&self) -> &M {
        &self.machine
    }

    fn handle_message(&mut self, msg: Message) -> std::io::Result<()> {
        match msg {
            Message::JoinCall { seqno, from } => {
                self.handle_join_call(seqno, from)?;
            }
            Message::JoinReply {
                seqno,
                node_id,
                promise,
            } => {
                assert_eq!(seqno, 0);

                if self.bare_node.id() != Self::UNINIT_NODE_ID {
                    return Ok(());
                }
                self.join_promise = Some(promise);
                self.bare_node = BareNode::start(node_id);
            }
            Message::RaftMessageCast { msg, .. } => {
                self.bare_node.handle_message(msg);
            }
        }
        Ok(())
    }

    fn handle_join_call(&mut self, seqno: u32, peer_addr: SocketAddr) -> std::io::Result<()> {
        let (node_id, promise) = if let Some((&node_id, _)) =
            self.peer_addrs.iter().find(|(_, &addr)| addr == peer_addr)
        {
            (node_id, CommitPromise::Pending(LogPosition::ZERO)) // TODO: use appropriate promise
        } else {
            let node_id = self.next_node_id;
            self.next_node_id = NodeId::new(node_id.get() + 1);
            self.peer_addrs.insert(node_id, peer_addr);

            // TODO: propose system command
            let command = SystemCommand::AddNode {
                node_id,
                addr: peer_addr,
            };
            let promise = self.bare_node.propose_command(); // TODO: handle redirect
            assert!(!promise.is_rejected()); // TODO: handle this case (redirect or retry later)
            self.command_log
                .insert(promise.log_position().index, command);

            let new_config = self.bare_node.config().to_joint_consensus(&[node_id], &[]);
            let promise = self.bare_node.propose_config(new_config);
            assert!(!promise.is_rejected()); // TODO: handle this case (redirect or retry later)

            (node_id, promise)
        };

        let msg = Message::JoinReply {
            seqno,
            node_id,
            promise,
        };
        let mut buf = Vec::new(); // TODO: reuse
        msg.encode(&mut buf)?;
        self.socket.send_to(&buf, peer_addr)?;
        Ok(())
    }

    fn handle_action(&mut self, action: Action) {
        match action {
            Action::SetElectionTimeout => {
                self.set_election_timeout();
            }
            Action::SaveCurrentTerm | Action::SaveVotedFor => {
                // noop
            }
            Action::AppendLogEntries(entries) => {
                self.append_log_entries(entries);
            }
            Action::BroadcastMessage(msg) => {
                self.broadcast_message(msg);
            }
            _ => todo!("action: {:?}", action),
        }
    }

    fn broadcast_message(&mut self, msg: raftbare::Message) {
        let msg = Message::RaftMessageCast {
            seqno: self.next_seqno(),
            msg,
        };
        let mut buf = Vec::new(); // TODO: reuse
        msg.encode(&mut buf).expect("TODO");

        for id in self.bare_node.peers() {
            let peer_addr = self.peer_addrs.get(&id).copied().expect("peer addr");
            self.socket.send_to(&buf, peer_addr).expect("TODO");
        }
    }

    fn append_log_entries(&mut self, entries: LogEntries) {
        // todo
        eprintln!("{:?}", entries);
    }

    fn set_election_timeout(&mut self) {
        // TODO: configurable
        let min_timeout = Duration::from_millis(150);
        let max_timeout = Duration::from_millis(1000);

        let duration = match self.role() {
            Role::Follower => max_timeout,
            Role::Candidate => rand::thread_rng().gen_range(min_timeout..max_timeout),
            Role::Leader => min_timeout,
        };
        self.election_timeout = Some(Instant::now() + duration);
    }
}

// TODO
// join / leave
// propose_command()
// local_query()
// consistent_query()

fn maybe_would_block<T>(result: std::io::Result<T>) -> std::io::Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(err) if err.kind() == ErrorKind::WouldBlock => Ok(None),
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orfail::OrFail;

    impl Machine for () {
        type Command = ();

        fn apply(&mut self, _command: Self::Command) {}

        fn encode(&self, _buf: &mut Vec<u8>) {}

        fn decode(_buf: &[u8]) -> Self {
            ()
        }
    }

    impl Command for () {
        fn encode(&self, _buf: &mut Vec<u8>) {}

        fn decode(_buf: &[u8]) -> Self {
            ()
        }
    }

    type TestRaftNode = RaftNode<()>;

    #[test]
    fn create_cluster() -> orfail::Result<()> {
        let mut node = TestRaftNode::new(auto_addr()).or_fail()?;
        assert_eq!(node.node_id(), TestRaftNode::UNINIT_NODE_ID);

        node.create_cluster().or_fail()?;
        assert_eq!(node.node_id(), NodeId::new(0));

        Ok(())
    }

    #[test]
    fn join() -> orfail::Result<()> {
        let mut node0 = TestRaftNode::new(auto_addr()).or_fail()?;
        node0.create_cluster().or_fail()?;
        let node0_addr = node0.local_addr();
        std::thread::spawn(move || {
            node0.run_while(|| true).expect("node0 aborted");
        });

        // Join a new node to the created cluster.
        let mut node1 = TestRaftNode::new(auto_addr()).or_fail()?;
        node1
            .join(node0_addr, Some(Duration::from_secs(1)))
            .or_fail()?;

        Ok(())
    }

    fn addr(s: &str) -> SocketAddr {
        s.parse().expect("parse addr")
    }

    fn auto_addr() -> SocketAddr {
        addr("127.0.0.1:0")
    }
}
