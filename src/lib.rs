// pub mod command;
// pub mod node;
// pub mod remote_types; // TODO: rename
// pub mod request;

pub mod client;
mod machine;
mod raft_server;
pub mod stats; // TODO

pub use machine::{Context, From, InputKind, Machine};
pub use raft_server::{RaftServer, RaftServerOptions};
pub use stats::ServerStats;

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, time::Duration};

    use super::*;

    impl Machine for usize {
        type Input = usize;

        fn handle_input(&mut self, _ctx: &Context, from: From, input: &Self::Input) {
            *self += input;
            from.reply_output(self);
        }
    }

    const POLL_TIMEOUT: Option<Duration> = Some(Duration::from_millis(10));

    #[test]
    fn create_cluster() {
        let mut server = RaftServer::start(auto_addr(), 0).expect("start() failed");
        assert!(server.node().is_none());

        server.poll(POLL_TIMEOUT).expect("poll() failed");

        // let handle = node.handle();

        // std::thread::scope(|s| {
        //     s.spawn(|| {
        //         let created = handle.create_cluster().expect("create_cluster() failed");
        //         assert_eq!(created, true);

        //         let created = handle.create_cluster().expect("create_cluster() failed");
        //         assert_eq!(created, false);
        //     });
        //     s.spawn(|| {
        //         for _ in 0..50 {
        //             node.poll_one(POLL_TIMEOUT).expect("poll_one() failed");
        //         }
        //     });
        // });

        // assert_eq!(node.id(), NodeId::new(0));
    }

    fn auto_addr() -> SocketAddr {
        "127.0.0.1:0".parse().expect("unreachable")
    }
}
