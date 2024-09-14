// pub mod command;
// pub mod node;
// pub mod remote_types; // TODO: rename
// pub mod request;

mod machine;
mod raft_server;
pub mod request;
pub mod stats; // TODO

pub use machine::{Context, From, InputKind, Machine};
pub use raft_server::{RaftServer, RaftServerOptions};
pub use stats::ServerStats;

#[cfg(test)]
mod tests {
    use std::net::{SocketAddr, TcpStream};

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

    #[test]
    fn create_cluster() {
        let mut server = RaftServer::start(auto_addr(), 0).expect("start() failed");
        assert!(server.node().is_none());

        let server_addr = server.addr();
        std::thread::scope(|s| {
            s.spawn(|| {
                let stream = TcpStream::connect(server_addr).expect("connect() failed");
                let mut client = RpcClient::new(stream);

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
                while server.node().is_none() {
                    server.poll(None).expect("poll() failed");
                }
            });
        });

        assert!(server.node().is_some());
    }

    fn auto_addr() -> SocketAddr {
        "127.0.0.1:0".parse().expect("unreachable")
    }

    fn request_id(id: i64) -> RequestId {
        RequestId::Number(id)
    }
}
