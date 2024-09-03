use message::{Message, SystemCommand};
use raftbare::{
    Action, CommitPromise, LogEntries, LogEntry, LogIndex, LogPosition, Node as BareNode, NodeId,
    Role,
};
use rand::Rng;
use std::{
    collections::BTreeMap,
    io::{Error, ErrorKind, Read, Write},
    net::{SocketAddr, UdpSocket},
    time::{Duration, Instant},
};

pub mod message;

pub trait Machine: Sized + Default {
    type Command: Command;

    fn apply(&mut self, command: &Self::Command);

    fn encode(&self, buf: &mut Vec<u8>);
    fn decode(buf: &[u8]) -> Self;
}

pub trait Command: Sized {
    fn encode<W: Write>(&self, writer: W) -> std::io::Result<()>;
    fn decode<R: Read>(reader: R) -> std::io::Result<Self>;
}

impl Command for Vec<u8> {
    fn encode<W: Write>(&self, mut writer: W) -> std::io::Result<()> {
        let n = self.len() as u16;
        writer.write_all(&n.to_be_bytes())?;
        writer.write_all(self)
    }

    fn decode<R: Read>(mut reader: R) -> std::io::Result<Self> {
        let mut n_buf = [0; 2];
        reader.read_exact(&mut n_buf)?;
        let n = u16::from_be_bytes(n_buf) as usize;

        let mut buf = vec![0; n];
        reader.read_exact(&mut buf)?;
        Ok(buf)
    }
}

// TODO: remove old entries
pub type CommandLog<C> = BTreeMap<LogIndex, SystemCommand<C>>;

#[derive(Debug)]
pub struct RaftNode<M: Machine> {
    machine: M,
    bare_node: BareNode,
    socket: UdpSocket,
    local_addr: SocketAddr,
    seqno: u32,
    command_log: CommandLog<M::Command>,
    election_timeout: Option<Instant>,

    join_promise: Option<CommitPromise>, // TODO

    last_applied: LogIndex,

    // TODO: max_log_entries, retain_log_entries

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

            last_applied: LogIndex::ZERO,

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

    // TODO: pub fn leave()

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
        send_msg.encode(&mut send_buf, &self.command_log)?;

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
            let Some((size, addr)) = maybe_would_block(self.socket.recv_from(&mut recv_buf))?
            else {
                continue;
            };
            assert!(size >= 5); // seqno (4) + tag (1)
            assert!(size <= 1205); // TODO
            let recv_msg = Message::decode(&recv_buf[..size], &mut self.command_log)?;
            self.handle_message(addr, recv_msg)?;
            self.run_one()?; // TODO
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
        for i in self.last_applied.get() + 1..=self.bare_node.commit_index().get() {
            let i = LogIndex::new(i);
            let entry = self.bare_node.log().entries().get_entry(i).expect("bug");
            if matches!(entry, LogEntry::Command) {
                let command = self.command_log.get(&i).expect("bug");
                match command {
                    SystemCommand::AddNode { node_id, addr } => {
                        // TODO:
                        self.peer_addrs.insert(*node_id, *addr);
                    }
                    SystemCommand::User(c) => {
                        self.machine.apply(c);
                        if self.role().is_leader() {
                            // TODO: hearbeat() to sync followers quickly
                        }
                    }
                }
            }
            self.last_applied = i;
        }

        let mut recv_buf = [0; 2048];
        while let Some((size, addr)) = maybe_would_block(self.socket.recv_from(&mut recv_buf))? {
            assert!(size >= 5); // seqno (4) + tag (1)
            assert!(size <= 1205); // TODO

            let recv_msg = Message::decode(&recv_buf[..size], &mut self.command_log)?;
            self.handle_message(addr, recv_msg)?;
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

    pub fn bare_node(&self) -> &BareNode {
        &self.bare_node
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn machine(&self) -> &M {
        &self.machine
    }

    fn handle_message(&mut self, from: SocketAddr, msg: Message) -> std::io::Result<()> {
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
                if !self.peer_addrs.contains_key(&msg.from()) {
                    // TODO:
                    self.peer_addrs.insert(msg.from(), from);
                }
                self.bare_node.handle_message(msg);
            }
            Message::ProposeCommandCall {
                seqno,
                from,
                command,
            } => todo!(),
            Message::ProposeCommandReply { seqno, promise } => todo!(),
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
        msg.encode(&mut buf, &self.command_log)?;
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
            Action::SendMessage(peer, msg) => {
                self.send_message(peer, msg);
            }
            _ => todo!("action: {:?}", action),
        }
    }

    fn send_message(&mut self, peer: NodeId, msg: raftbare::Message) {
        let msg = Message::RaftMessageCast {
            seqno: self.next_seqno(),
            msg,
        };
        let mut buf = Vec::new(); // TODO: reuse
        msg.encode(&mut buf, &self.command_log).expect("TODO");

        let peer_addr = self.peer_addrs.get(&peer).copied().expect("peer addr");
        self.socket.send_to(&buf, peer_addr).expect("TODO");
    }

    fn broadcast_message(&mut self, msg: raftbare::Message) {
        let msg = Message::RaftMessageCast {
            seqno: self.next_seqno(),
            msg,
        };
        let mut buf = Vec::new(); // TODO: reuse
        msg.encode(&mut buf, &self.command_log).expect("TODO");

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

    // TODO: return promise(?)
    pub fn propose_command(
        &mut self,
        command: M::Command,
        timeout: Option<Duration>,
    ) -> std::io::Result<bool> {
        let timeout = timeout.map(|t| Instant::now() + t);

        let mut promise = if self.role().is_leader() {
            let promise = self.bare_node.propose_command();
            assert!(!promise.is_rejected());

            self.command_log
                .insert(promise.log_position().index, SystemCommand::User(command));
            promise
        } else {
            self.remote_propose_command(command, timeout)?
        };

        while promise.poll(&mut self.bare_node).is_pending() {
            if timeout.map_or(false, |timeout| timeout < Instant::now()) {
                return Err(ErrorKind::TimedOut.into());
            }

            std::thread::sleep(Duration::from_millis(5)); // TODO
            self.run_one()?;
        }
        self.run_one()?; // TODO: note (or only processing apply commands)

        Ok(promise.is_accepted())
    }

    // TODO: timeout
    fn remote_propose_command(
        &mut self,
        command: M::Command,
        timeout: Option<Instant>,
    ) -> std::io::Result<CommitPromise> {
        todo!()
    }

    pub fn sync(&mut self, _timeout: Option<Duration>) -> std::io::Result<bool> {
        // heartbeat
        todo!()
    }
}

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

    #[derive(Debug, Default)]
    struct Calc {
        value: i32,
    }

    impl Machine for Calc {
        type Command = CalcCommand;

        fn apply(&mut self, command: &Self::Command) {
            match command {
                CalcCommand::Add(value) => self.value += value,
                CalcCommand::Sub(value) => self.value -= value,
            }
        }

        fn encode(&self, buf: &mut Vec<u8>) {
            buf.extend_from_slice(&self.value.to_be_bytes());
        }

        fn decode(buf: &[u8]) -> Self {
            let value = i32::from_be_bytes(buf.try_into().expect("i32"));
            Self { value }
        }
    }

    enum CalcCommand {
        Add(i32),
        Sub(i32),
    }

    impl Command for CalcCommand {
        fn encode<W: Write>(&self, mut writer: W) -> std::io::Result<()> {
            match self {
                Self::Add(value) => {
                    writer.write_all(&[0])?;
                    writer.write_all(&value.to_be_bytes())
                }
                Self::Sub(value) => {
                    writer.write_all(&[1])?;
                    writer.write_all(&value.to_be_bytes())
                }
            }
        }

        fn decode<R: Read>(mut reader: R) -> std::io::Result<Self> {
            let mut buf = [0; 5];
            reader.read_exact(&mut buf[..])?;
            let value = i32::from_be_bytes(buf[1..].try_into().expect("i32"));
            match buf[0] {
                0 => Ok(Self::Add(value)),
                1 => Ok(Self::Sub(value)),
                _ => Err(std::io::ErrorKind::InvalidData.into()),
            }
        }
    }

    type TestRaftNode = RaftNode<Calc>;

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
        assert_eq!(node1.node_id(), NodeId::new(1));

        Ok(())
    }

    #[test]
    fn propose_command() -> orfail::Result<()> {
        let mut node0 = TestRaftNode::new(auto_addr()).or_fail()?;
        node0.create_cluster().or_fail()?;
        let node0_addr = node0.local_addr();

        std::thread::spawn(move || {
            let mut node1 = TestRaftNode::new(auto_addr()).expect("node1");
            node1
                .join(node0_addr, Some(Duration::from_secs(1)))
                .expect("join node1 to node0");
            node1.run_while(|| true).expect("node1 aborted");
        });

        while node0.bare_node.config().voters.len() == 1
            || node0.bare_node.config().new_voters.len() != 0
        {
            node0.run_one().expect("node0 aborted");
            std::thread::sleep(Duration::from_millis(10));
        }

        // Propose a command to the leader node.
        assert!(node0.role().is_leader());
        let succeeded = node0
            .propose_command(CalcCommand::Add(42), Some(Duration::from_secs(1)))
            .expect("propose command");
        assert!(succeeded);
        assert_eq!(node0.machine().value, 42);

        std::thread::spawn(move || {
            node0.run_while(|| true).expect("node0 aborted");
        });

        // Propose a command to a follower node (node2)
        let mut node2 = TestRaftNode::new(auto_addr()).expect("node2");
        node2
            .join(node0_addr, Some(Duration::from_secs(1)))
            .expect("join node1 to node0");
        node2
            .propose_command(CalcCommand::Sub(3), Some(Duration::from_secs(1)))
            .expect("propose command");

        Ok(())
    }

    fn addr(s: &str) -> SocketAddr {
        s.parse().expect("parse addr")
    }

    fn auto_addr() -> SocketAddr {
        addr("127.0.0.1:0")
    }
}
