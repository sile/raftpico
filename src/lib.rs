pub mod command;
pub mod connection;
pub mod io;
mod machine;
pub mod message;
pub mod request; // TODO: message?
mod server;
pub mod server2;
pub mod stats;
pub mod storage; // TODO

pub use machine::{Context, InputKind, Machine};
pub use server::{Server, ServerOptions};
pub use stats::ServerStats;

pub type Result<T> = std::result::Result<T, Error>;

// TODO: remove
pub struct Error {
    pub io: std::io::Error,
    pub trace: std::backtrace::Backtrace,
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self {
            io: value,
            trace: std::backtrace::Backtrace::capture(),
        }
    }
}

impl From<Error> for std::io::Error {
    fn from(value: Error) -> Self {
        value.io
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self {
            io: value.into(),
            trace: std::backtrace::Backtrace::capture(),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.io)
    }
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.trace.status() == std::backtrace::BacktraceStatus::Captured {
            write!(f, "{}\n\nBacktrace:\n{}", self.io, self.trace)
        } else {
            write!(f, "{}", self.io)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{SocketAddr, TcpStream},
        time::Duration,
    };

    use jsonlrpc::{RequestId, ResponseObject, RpcClient};
    use machine::{Context2, Machine2};
    use request::{
        AddServerResult, CreateClusterResult, OutputResult, RemoveServerResult, Request, Response,
    };
    use serde::{Deserialize, Serialize};
    use server2::{ErrorKind, RaftServer};

    use super::*;

    impl Machine for usize {
        type Input = usize;

        fn handle_input(&mut self, ctx: &mut Context, input: Self::Input) {
            if ctx.kind().is_command() {
                *self += input;
            }
            ctx.output(self);
        }
    }

    impl Machine2 for usize {
        type Input = usize;

        fn apply(&mut self, ctx: &mut Context2, input: &Self::Input) {
            if ctx.kind.is_command() {
                *self += *input;
            }
            ctx.output(self);
        }
    }

    const TEST_TIMEOUT: Duration = Duration::from_secs(3);
    const POLL_TIMEOUT: Option<Duration> = Some(Duration::from_millis(10));

    #[test]
    fn create_cluster() {
        let mut server = RaftServer::start(auto_addr(), 0).expect("start() failed");
        assert!(server.node().is_none());

        let server_addr = server.listen_addr();
        let handle = std::thread::spawn(move || {
            let mut client = RpcClient::new(connect(server_addr));

            // First call: OK
            let request = Request::create_cluster(request_id(0), None);
            let response: ResponseObject = client.call(&request).expect("call() failed");
            let members = response.into_std_result().expect("error response");
            assert_eq!(members.as_array().map(|x| x.len()), Some(1));

            // Second call: NG
            let request = Request::create_cluster(request_id(1), None);
            let response: ResponseObject = client.call(&request).expect("call() failed");
            let error = response.into_std_result().expect_err("ok response");
            assert_eq!(error.code, ErrorKind::ClusterAlreadyCreated.code());
        });

        while !handle.is_finished() {
            server.poll(POLL_TIMEOUT).expect("poll() failed");
        }
        assert!(handle.join().is_ok());
        assert!(server.node().is_some());
    }

    #[test]
    fn add_and_remove_server() {
        let mut server0 = Server::start(auto_addr(), 0).expect("start() failed");
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
        let mut server1 = Server::start(auto_addr(), 0).expect("start() failed");
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

    // #[test]
    // fn command() {
    //     let mut servers = Vec::new();
    //     let mut server0 = Server::start(auto_addr(), 0).expect("start() failed");

    //     // Create a cluster.
    //     let server_addr0 = server0.addr();
    //     let handle = std::thread::spawn(move || {
    //         rpc::<CreateClusterResult>(server_addr0, Request::create_cluster(request_id(0), None))
    //     });
    //     while !handle.is_finished() {
    //         server0.poll(POLL_TIMEOUT).expect("poll() failed");
    //     }
    //     servers.push(server0);

    //     // Add two servers to the cluster.
    //     let server1 = Server::start(auto_addr(), 0).expect("start() failed");
    //     let server2 = Server::start(auto_addr(), 0).expect("start() failed");
    //     let server_addr1 = server1.addr();
    //     let server_addr2 = server2.addr();
    //     let handle = std::thread::spawn(move || {
    //         let mut contact_addr = server_addr0;
    //         for addr in [server_addr1, server_addr2] {
    //             let result: AddServerResult =
    //                 rpc(contact_addr, Request::add_server(request_id(0), addr));
    //             assert_eq!(result.error, None);
    //             contact_addr = addr;

    //             // TODO:
    //             std::thread::sleep(Duration::from_millis(100));
    //         }
    //     });
    //     servers.push(server1);
    //     servers.push(server2);

    //     while !handle.is_finished() {
    //         for server in &mut servers {
    //             server.poll(POLL_TIMEOUT).expect("poll() failed");
    //         }
    //     }
    //     for server in &servers {
    //         assert!(server.node().is_some());
    //     }

    //     // Propose commands.
    //     let addrs = servers.iter().map(|s| s.addr()).collect::<Vec<_>>();
    //     let handle = std::thread::spawn(move || {
    //         for (i, addr) in addrs.into_iter().enumerate() {
    //             let _v: serde_json::Value = rpc(
    //                 addr,
    //                 Request::command(request_id(0), &i).expect("unreachable"),
    //             );
    //         }
    //     });

    //     while !handle.is_finished() {
    //         for server in &mut servers {
    //             server.poll(POLL_TIMEOUT).expect("poll() failed");
    //         }
    //     }
    //     for server in &servers {
    //         assert_eq!(*server.machine(), 0 + 1 + 2);
    //     }
    // }

    #[test]
    fn re_election() {
        let mut servers = Vec::new();
        let mut server0 = Server::start(auto_addr(), 0).expect("start() failed");

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
        let server1 = Server::start(auto_addr(), 0).expect("start() failed");
        let server2 = Server::start(auto_addr(), 0).expect("start() failed");
        let server_addr1 = server1.addr();
        let server_addr2 = server2.addr();
        let handle = std::thread::spawn(move || {
            let mut contact_addr = server_addr0;
            for addr in [server_addr1, server_addr2] {
                let result: AddServerResult =
                    rpc(contact_addr, Request::add_server(request_id(0), addr));
                assert_eq!(result.error, None);
                contact_addr = addr;

                // TODO:
                std::thread::sleep(Duration::from_millis(100));
            }
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
        assert!(servers[0].node().expect("unreachable").role().is_leader());

        // Run until the leader changes.
        while !servers
            .iter()
            .skip(1)
            .any(|s| s.node().expect("unreachable").role().is_leader())
        {
            for server in servers.iter_mut().skip(1) {
                server.poll(POLL_TIMEOUT).expect("poll() failed");
            }
        }
        for _ in 0..100 {
            for server in &mut servers {
                server.poll(POLL_TIMEOUT).expect("poll() failed");
            }
            if servers[0].node().expect("unreachable").role().is_follower() {
                break;
            }
        }
        assert!(servers[0].node().expect("unreachable").role().is_follower());
    }

    // #[test]
    // fn snapshot() {
    //     let mut servers = Vec::new();
    //     let mut server0 = Server::start(auto_addr(), 0).expect("start() failed");

    //     // Create a cluster with a small max log size.
    //     let server_addr0 = server0.addr();
    //     let handle = std::thread::spawn(move || {
    //         let options = CreateClusterParams {
    //             max_log_entries_hint: 1,
    //             ..Default::default()
    //         };
    //         rpc::<CreateClusterResult>(
    //             server_addr0,
    //             Request::create_cluster(request_id(0), Some(options)),
    //         )
    //     });
    //     while !handle.is_finished() {
    //         server0.poll(POLL_TIMEOUT).expect("poll() failed");
    //     }
    //     servers.push(server0);

    //     // Add two servers to the cluster.
    //     let server1 = Server::start(auto_addr(), 0).expect("start() failed");
    //     let server2 = Server::start(auto_addr(), 0).expect("start() failed");
    //     let server_addr1 = server1.addr();
    //     let server_addr2 = server2.addr();
    //     let handle = std::thread::spawn(move || {
    //         let mut contact_addr = server_addr0;
    //         for addr in [server_addr1, server_addr2] {
    //             let result: AddServerResult =
    //                 rpc(contact_addr, Request::add_server(request_id(0), addr));
    //             assert_eq!(result.error, None);
    //             contact_addr = addr;

    //             // TODO:
    //             std::thread::sleep(Duration::from_millis(100));
    //         }
    //     });
    //     servers.push(server1);
    //     servers.push(server2);

    //     while !handle.is_finished() {
    //         for server in &mut servers {
    //             server.poll(POLL_TIMEOUT).expect("poll() failed");
    //         }
    //     }
    //     for server in &servers {
    //         assert!(server.node().is_some());
    //     }

    //     // Propose commands.
    //     let addrs = servers.iter().map(|s| s.addr()).collect::<Vec<_>>();
    //     let handle = std::thread::spawn(move || {
    //         for (i, addr) in addrs.into_iter().cycle().enumerate().take(10) {
    //             let _v: serde_json::Value = rpc(
    //                 addr,
    //                 Request::command(request_id(0), &i).expect("unreachable"),
    //             );
    //         }
    //     });

    //     while !handle.is_finished() {
    //         for server in &mut servers {
    //             server.poll(POLL_TIMEOUT).expect("poll() failed");
    //         }
    //     }
    //     for server in &servers {
    //         assert_eq!(*server.machine(), 0 + 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9);
    //     }
    // }

    // #[test]
    // fn query() {
    //     let mut servers = Vec::new();
    //     let mut server0 = Server::start(auto_addr(), 0).expect("start() failed");

    //     // Create a cluster.
    //     let server_addr0 = server0.addr();
    //     let handle = std::thread::spawn(move || {
    //         rpc::<CreateClusterResult>(server_addr0, Request::create_cluster(request_id(0), None))
    //     });
    //     while !handle.is_finished() {
    //         server0.poll(POLL_TIMEOUT).expect("poll() failed");
    //     }
    //     servers.push(server0);

    //     // Add two servers to the cluster.
    //     let server1 = Server::start(auto_addr(), 0).expect("start() failed");
    //     let server2 = Server::start(auto_addr(), 0).expect("start() failed");
    //     let server_addr1 = server1.addr();
    //     let server_addr2 = server2.addr();
    //     let handle = std::thread::spawn(move || {
    //         let mut contact_addr = server_addr0;
    //         for addr in [server_addr1, server_addr2] {
    //             let result: AddServerResult =
    //                 rpc(contact_addr, Request::add_server(request_id(0), addr));
    //             assert_eq!(result.error, None);
    //             contact_addr = addr;

    //             // TODO:
    //             std::thread::sleep(Duration::from_millis(100));
    //         }
    //     });
    //     servers.push(server1);
    //     servers.push(server2);

    //     while !handle.is_finished() {
    //         for server in &mut servers {
    //             server.poll(POLL_TIMEOUT).expect("poll() failed");
    //         }
    //     }
    //     for server in &servers {
    //         assert!(server.node().is_some());
    //     }

    //     // Commands & queries
    //     let addrs = servers.iter().map(|s| s.addr()).collect::<Vec<_>>();
    //     let handle = std::thread::spawn(move || {
    //         for (i, addr) in addrs.into_iter().enumerate() {
    //             let v0: serde_json::Value = rpc(
    //                 addr,
    //                 Request::command(request_id(0), &i).expect("unreachable"),
    //             );
    //             let v1: serde_json::Value = rpc(
    //                 addr,
    //                 Request::query(request_id(0), &i).expect("unreachable"),
    //             );
    //             assert_eq!(v0, v1);
    //         }
    //     });

    //     while !handle.is_finished() {
    //         for server in &mut servers {
    //             server.poll(POLL_TIMEOUT).expect("poll() failed");
    //         }
    //     }
    //     for server in &servers {
    //         assert_eq!(*server.machine(), 0 + 1 + 2);
    //     }
    // }

    #[test]
    fn local_query() {
        let mut servers = Vec::new();
        let mut server0 = Server::start(auto_addr(), 0).expect("start() failed");

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
        let server1 = Server::start(auto_addr(), 1).expect("start() failed");
        let server2 = Server::start(auto_addr(), 2).expect("start() failed");
        let server_addr1 = server1.addr();
        let server_addr2 = server2.addr();
        let handle = std::thread::spawn(move || {
            let mut contact_addr = server_addr0;
            for addr in [server_addr1, server_addr2] {
                let result: AddServerResult =
                    rpc(contact_addr, Request::add_server(request_id(0), addr));
                assert_eq!(result.error, None);
                contact_addr = addr;

                // TODO:
                std::thread::sleep(Duration::from_millis(100));
            }
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

        // Local query
        let addrs = servers.iter().map(|s| s.addr()).collect::<Vec<_>>();
        let handle = std::thread::spawn(move || {
            for (i, addr) in addrs.into_iter().enumerate() {
                let v: OutputResult = rpc(
                    addr,
                    Request::local_query(request_id(0), &0).expect("unreachable"),
                );
                assert_eq!(v.output, Some(serde_json::Value::Number(i.into())));
            }
        });

        while !handle.is_finished() {
            for server in &mut servers {
                server.poll(POLL_TIMEOUT).expect("poll() failed");
            }
        }
        handle.join().expect("join() failed");
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
