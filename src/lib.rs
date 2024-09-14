// pub mod command;
// pub mod node;
// pub mod remote_types; // TODO: rename
// pub mod request;

pub mod connection;
pub mod io;
mod machine;
mod raft_server;
pub mod request; // TODO: message?
pub mod stats; // TODO

pub use machine::{Context, From, InputKind, Machine};
pub use raft_server::{RaftServer, RaftServerOptions};
pub use stats::ServerStats;

#[cfg(test)]
mod tests {
    use std::{
        net::{SocketAddr, TcpStream},
        time::{Duration, Instant},
    };

    use jsonlrpc::{RequestId, RpcClient};
    use request::{CreateClusterResult, Request, Response};

    use super::*;

    impl Machine for usize {
        type Input = usize;

        fn handle_input(&mut self, _ctx: &Context, from: From, input: &Self::Input) {
            *self += input;
            from.reply_output(self);
        }
    }

    const TEST_TIMEOUT: Duration = Duration::from_secs(2);
    const POLL_TIMEOUT: Option<Duration> = Some(Duration::from_millis(100));

    #[test]
    fn create_cluster() {
        let mut server = RaftServer::start(auto_addr(), 0).expect("start() failed");
        assert!(server.node().is_none());

        let server_addr = server.addr();
        std::thread::scope(|s| {
            s.spawn(|| {
                let mut client = RpcClient::new(connect(server_addr));

                // First call: OK
                let request = Request::create_cluster(request_id(0), None);
                let response: Response<CreateClusterResult> =
                    client.call(&request).expect("call() failed");
                let result = response.into_std_result().expect("error response");
                assert_eq!(result.success, true);

                // Second call: NG
                let request = Request::create_cluster(request_id(1), None);
                let response: Response<CreateClusterResult> =
                    client.call(&request).expect("call() failed");
                let result = response.into_std_result().expect("error response");
                assert_eq!(result.success, false);
            });
            s.spawn(|| {
                let start_time = Instant::now();
                while server.node().is_none() && start_time.elapsed() < TEST_TIMEOUT {
                    server.poll(POLL_TIMEOUT).expect("poll() failed");
                }
            });
        });

        assert!(server.node().is_some());
    }

    fn connect(addr: SocketAddr) -> TcpStream {
        let stream = TcpStream::connect(addr).expect("connect() failed");
        stream
            .set_read_timeout(Some(TEST_TIMEOUT))
            .expect("set_read_timeout() failed");
        stream
            .set_write_timeout(Some(TEST_TIMEOUT))
            .expect("set_write_timeout() failed");
        stream
    }

    fn auto_addr() -> SocketAddr {
        "127.0.0.1:0".parse().expect("unreachable")
    }

    fn request_id(id: i64) -> RequestId {
        RequestId::Number(id)
    }
}
