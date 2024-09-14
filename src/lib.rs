// pub mod command;
// pub mod node;
// pub mod remote_types; // TODO: rename
// pub mod request;

pub mod command;
pub mod connection;
pub mod io;
mod machine;
mod raft_server;
pub mod remote_types;
pub mod request; // TODO: message?
pub mod stats; // TODO

pub use machine::{Context, From, InputKind, Machine};
pub use raft_server::{RaftServer, RaftServerOptions};
pub use stats::ServerStats;

#[cfg(test)]
mod tests {
    use std::{
        net::{SocketAddr, TcpStream},
        time::Duration,
    };

    use jsonlrpc::{RequestId, RpcClient};
    use request::{AddServerResult, CreateClusterResult, Request, Response};
    use serde::{Deserialize, Serialize};

    use super::*;

    impl Machine for usize {
        type Input = usize;

        fn handle_input(&mut self, _ctx: &Context, from: From, input: &Self::Input) {
            *self += input;
            from.reply_output(self);
        }
    }

    const TEST_TIMEOUT: Duration = Duration::from_secs(2);
    const POLL_TIMEOUT: Option<Duration> = Some(Duration::from_millis(10));

    #[test]
    fn create_cluster() {
        let mut server = RaftServer::start(auto_addr(), 0).expect("start() failed");
        assert!(server.node().is_none());

        let server_addr = server.addr();
        let handle = std::thread::spawn(move || {
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

        while !handle.is_finished() {
            server.poll(POLL_TIMEOUT).expect("poll() failed");
        }
        assert!(server.node().is_some());
    }

    #[test]
    fn add_server() {
        let mut server0 = RaftServer::start(auto_addr(), 0).expect("start() failed");
        assert!(server0.node().is_none());

        // Create a cluster.
        let server_addr0 = server0.addr();
        let handle = std::thread::spawn(move || {
            rpc::<CreateClusterResult>(server_addr0, Request::create_cluster(request_id(0), None))
        });
        while !handle.is_finished() {
            server0.poll(POLL_TIMEOUT).expect("poll() failed");
        }
        assert!(server0.node().is_some());

        // Add a server to the cluster.
        let mut server1 = RaftServer::start(auto_addr(), 0).expect("start() failed");
        let server_addr1 = server1.addr();
        let handle = std::thread::spawn(move || {
            let result: AddServerResult = rpc(
                server_addr1,
                Request::add_server(request_id(0), server_addr0),
            );
            assert!(result.success);
        });
        while !handle.is_finished() {
            server0.poll(POLL_TIMEOUT).expect("poll() failed");
            server1.poll(POLL_TIMEOUT).expect("poll() failed");
        }
        assert!(server1.node().is_some());
    }

    fn rpc<T>(server_addr: SocketAddr, request: impl Serialize) -> T
    where
        T: for<'de> Deserialize<'de>,
    {
        let mut client = RpcClient::new(connect(server_addr));
        let response: Response<T> = client.call(&request).expect("call() failed");
        response.into_std_result().expect("error response")
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
