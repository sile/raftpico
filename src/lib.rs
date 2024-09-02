use message::Message;
use raftbare::{Action, LogEntries, LogIndex, Node as BareNode, NodeId, Role};
use rand::Rng;
use std::{
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

#[derive(Debug)]
pub struct RaftNode<M: Machine> {
    machine: M,
    bare_node: BareNode,
    socket: UdpSocket,
    local_addr: SocketAddr,
    seqno: u32,
    command_log: Vec<(LogIndex, M::Command)>,
    election_timeout: Option<Instant>,
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
            command_log: Vec::new(),
            election_timeout: None,
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
        send_msg.encode(&mut send_buf);

        let start_time = Instant::now();
        while timeout.map_or(true, |t| start_time.elapsed() <= t) {
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
            let Message::JoinReply { seqno, node_id } = recv_msg else {
                // TODO: buffer pending messages
                continue;
            };
            if seqno != send_msg.seqno() {
                continue;
            }

            self.bare_node = BareNode::start(node_id);
            return Ok(());
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
            self.handle_message(recv_msg);
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

    fn handle_message(&mut self, msg: Message) {
        todo!("{:?}", msg);
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
            _ => todo!("action: {:?}", action),
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
