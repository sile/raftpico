use message::Message;
use raftbare::{Action, LogIndex, Node as BareNode, NodeId};
use std::{
    io::{Error, ErrorKind},
    net::{SocketAddr, UdpSocket},
    time::Duration,
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
}

impl<M: Machine> RaftNode<M> {
    pub fn create_cluster(node_addr: SocketAddr) -> std::io::Result<Self> {
        let socket = UdpSocket::bind(node_addr)?;
        socket.set_nonblocking(true)?;
        let local_addr = socket.local_addr()?;

        let node_id = NodeId::new(0);
        let bare_node = BareNode::start(node_id);
        let mut this = Self {
            machine: M::default(),
            bare_node,
            socket,
            local_addr,
            seqno: 0,
            command_log: Vec::new(),
        };

        let mut promise = this.bare_node.create_cluster(&[node_id]);
        assert!(!promise.is_rejected());

        while promise.poll(&this.bare_node).is_pending() {
            while let Some(action) = this.bare_node.actions_mut().next() {
                this.handle_action(action);
            }
        }
        assert!(promise.is_accepted());

        Ok(this)
    }

    pub fn join_cluster(
        node_addr: SocketAddr,
        contact_node_addr: SocketAddr,
    ) -> std::io::Result<Self> {
        let socket = UdpSocket::bind(node_addr)?;
        let local_addr = socket.local_addr()?;
        socket.set_nonblocking(true)?;

        let seqno = 0;
        let send_msg = Message::JoinClusterCall {
            seqno,
            from: local_addr,
        };

        let mut send_buf = Vec::new();
        send_msg.encode(&mut send_buf);

        for _ in 0..10 {
            socket.send_to(&send_buf, contact_node_addr)?;
            std::thread::sleep(Duration::from_millis(100));

            // TODO: while
            let mut recv_buf = [0; 2048];
            let Some((size, _addr)) = maybe_would_block(socket.recv_from(&mut recv_buf))? else {
                continue;
            };
            assert!(size >= 5); // seqno (4) + tag (1)
            assert!(size <= 1205); // TODO

            let recv_msg = Message::decode(&recv_buf[..size])?;
            let Message::JoinClusterReply { seqno, node_id } = recv_msg else {
                // TODO: buffer pending messages
                continue;
            };

            return Ok(Self {
                machine: M::default(),
                bare_node: BareNode::start(node_id),
                socket,
                local_addr,
                seqno: seqno + 1,
                command_log: Vec::new(),
            });
        }

        Err(Error::new(ErrorKind::TimedOut, "join_cluster timed out"))
    }

    pub fn node_id(&self) -> NodeId {
        self.bare_node.id()
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn machine(&self) -> &M {
        &self.machine
    }

    fn handle_action(&mut self, action: Action) {
        todo!();
    }
}

// TODO
// join / leave
// propose_command()
// local_query()
// consistent_query()
// run_one() or poll()

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

    #[test]
    fn create_cluster() {
        let node = RaftNode::<()>::create_cluster(auto_addr()).expect("create_cluster");
        assert_eq!(node.node_id().get(), 0);
    }

    #[test]
    fn join_cluster() {
        let node0 = RaftNode::<()>::create_cluster(auto_addr()).expect("create_cluster");

        // Join a new node to the created cluster.
        let node1 = RaftNode::<()>::join_cluster(auto_addr(), node0.local_addr());
    }

    fn addr(s: &str) -> SocketAddr {
        s.parse().expect("parse addr")
    }

    fn auto_addr() -> SocketAddr {
        addr("127.0.0.1:0")
    }
}
