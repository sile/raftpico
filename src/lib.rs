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

pub use machine::{Context, InputKind, Machine};
pub use raft_server::{RaftServer, RaftServerOptions};
pub use stats::ServerStats;

#[cfg(test)]
mod tests {
    use std::{
        net::{SocketAddr, TcpStream},
        time::Duration,
    };

    use jsonlrpc::{RequestId, RpcClient};
    use request::{AddServerResult, CreateClusterResult, RemoveServerResult, Request, Response};
    use serde::{Deserialize, Serialize};

    use super::*;

    impl Machine for usize {
        type Input = usize;

        fn handle_input(&mut self, ctx: &mut Context, input: Self::Input) {
            *self += input;
            ctx.output(self);
        }
    }

    const TEST_TIMEOUT: Duration = Duration::from_secs(3);
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
    fn add_and_remove_server() {
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
                server_addr0,
                Request::add_server(request_id(0), server_addr1),
            );
            assert_eq!(result.error, None);

            // TODO: call machine.on_event(Event::ServerJoined)
            std::thread::sleep(Duration::from_millis(500));
        });
        while !handle.is_finished() {
            server0.poll(POLL_TIMEOUT).expect("poll() failed");
            server1.poll(POLL_TIMEOUT).expect("poll() failed");
        }
        assert!(server1.node().is_some());

        // Remove server0 from the cluster.
        let handle = std::thread::spawn(move || {
            let result: RemoveServerResult = rpc(
                server_addr0,
                Request::remove_server(request_id(0), server_addr0),
            );
            assert_eq!(result.error, None);

            // TODO: call machine.on_event(Event::ServerJoined)
            std::thread::sleep(Duration::from_millis(500));
        });
        while !handle.is_finished() {
            server0.poll(POLL_TIMEOUT).expect("poll() failed");
            server1.poll(POLL_TIMEOUT).expect("poll() failed");
        }
        assert!(server0.node().is_none());
    }

    #[test]
    fn command() {
        let mut servers = Vec::new();
        let mut server0 = RaftServer::start(auto_addr(), 0).expect("start() failed");

        // Create a cluster.
        let server_addr0 = server0.addr();
        let handle = std::thread::spawn(move || {
            rpc::<CreateClusterResult>(server_addr0, Request::create_cluster(request_id(0), None))
        });
        while !handle.is_finished() {
            server0.poll(POLL_TIMEOUT).expect("poll() failed");
        }
        servers.push(server0);

        // Add two servers to the cluster.
        let server1 = RaftServer::start(auto_addr(), 0).expect("start() failed");
        let server2 = RaftServer::start(auto_addr(), 0).expect("start() failed");
        let server_addr1 = server1.addr();
        let server_addr2 = server2.addr();
        let handle = std::thread::spawn(move || {
            let mut contact_addr = server_addr0;
            for addr in [server_addr1, server_addr2] {
                let result: AddServerResult =
                    rpc(contact_addr, Request::add_server(request_id(0), addr));
                assert_eq!(result.error, None);
                contact_addr = addr;
            }
            // TODO: call machine.on_event(Event::ServerJoined)
            std::thread::sleep(Duration::from_millis(500));
        });
        servers.push(server1);
        servers.push(server2);

        while !handle.is_finished() {
            for server in &mut servers {
                server.poll(POLL_TIMEOUT).expect("poll() failed");
            }
        }
        for server in &servers {
            assert!(server.node().is_some());
        }

        // Propose commands.
        let addrs = servers.iter().map(|s| s.addr()).collect::<Vec<_>>();
        let handle = std::thread::spawn(move || {
            for (i, addr) in addrs.into_iter().enumerate() {
                let _v: serde_json::Value = rpc(
                    addr,
                    Request::command(request_id(0), &i).expect("unreachable"),
                );
            }
        });

        while !handle.is_finished() {
            for server in &mut servers {
                server.poll(POLL_TIMEOUT).expect("poll() failed");
            }
        }
        for server in &servers {
            assert_eq!(*server.machine(), 0 + 1 + 2);
        }
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
