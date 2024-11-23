pub mod command;
mod machine;
pub mod message;
pub mod server2;
pub mod stats;
pub mod storage; // TODO

pub use machine::{Context2, InputKind, Machine2};
pub use message::Request;
pub use server2::Server;
pub use stats::ServerStats;
pub use storage::FileStorage;

#[cfg(test)]
mod tests {
    // #[test]
    // fn snapshot() {
    //     let mut servers = Vec::new();
    //     let mut server0 = RaftServer::start(auto_addr(), 0, None).expect("start() failed");

    //     // Create a cluster with a small max log size.
    //     let server_addr0 = server0.listen_addr();
    //     let handle = std::thread::spawn(move || {
    //         rpc::<CreateClusterOutput>(server_addr0, Request::create_cluster(request_id(0), None))
    //     });
    //     while !handle.is_finished() {
    //         server0.poll(POLL_TIMEOUT).expect("poll() failed");
    //     }
    //     servers.push(server0);

    //     // Propose commands.
    //     let handle = std::thread::spawn(move || {
    //         for i in 0..10 {
    //             let _v: serde_json::Value = rpc(server_addr0, apply_command_request(i));
    //         }

    //         let _: TakeSnapshotOutput = rpc(
    //             server_addr0,
    //             crate::message::Request::TakeSnapshot {
    //                 jsonrpc: jsonlrpc::JsonRpcVersion::V2,
    //                 id: RequestId::Number(0),
    //             },
    //         );
    //         // TODO:
    //         std::thread::sleep(Duration::from_millis(500));
    //     });
    //     while !handle.is_finished() {
    //         servers[0].poll(POLL_TIMEOUT).expect("poll() failed");
    //     }
    //     assert_eq!(*servers[0].machine(), 0 + 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9);

    //     // Add two servers to the cluster.
    //     let server1 = RaftServer::start(auto_addr(), 0, None).expect("start() failed");
    //     let server2 = RaftServer::start(auto_addr(), 0, None).expect("start() failed");
    //     let server_addr1 = server1.listen_addr();
    //     let server_addr2 = server2.listen_addr();
    //     let handle = std::thread::spawn(move || {
    //         let mut contact_addr = server_addr0;
    //         for addr in [server_addr1, server_addr2] {
    //             let output: AddServerOutput =
    //                 rpc(contact_addr, Request::add_server(request_id(0), addr));
    //             assert!(output.members.len() > 1);
    //             contact_addr = addr;

    //             // TODO:
    //             std::thread::sleep(Duration::from_millis(500));
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
    //     let addrs = servers.iter().map(|s| s.listen_addr()).collect::<Vec<_>>();
    //     let handle = std::thread::spawn(move || {
    //         for (i, addr) in addrs.into_iter().cycle().enumerate().take(10) {
    //             let _v: serde_json::Value = rpc(addr, apply_command_request(i));
    //         }

    //         // TODO:
    //         std::thread::sleep(Duration::from_millis(500));
    //     });

    //     while !handle.is_finished() {
    //         for server in &mut servers {
    //             server.poll(POLL_TIMEOUT).expect("poll() failed");
    //         }
    //     }
    //     for server in &servers {
    //         assert_eq!(
    //             *server.machine(),
    //             (0 + 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9) * 2
    //         );
    //     }
    // }
}
