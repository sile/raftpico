use std::{
    net::{SocketAddr, TcpStream},
    time::Duration,
};

use jsonlrpc::{ErrorCode, ResponseObject, RpcClient};
use raftpico::{
    message::{
        AddServerOutput, AddServerParams, CreateClusterOutput, RemoveServerOutput,
        RemoveServerParams,
    },
    server2::ErrorKind,
    Context2, Machine2, Server,
};
use serde::{Deserialize, Serialize};

const TEST_TIMEOUT: Duration = Duration::from_secs(3);
const POLL_TIMEOUT: Option<Duration> = Some(Duration::from_millis(10));

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Counter(usize);

impl Machine2 for Counter {
    type Input = usize;

    fn apply(&mut self, ctx: &mut Context2, input: &Self::Input) {
        if ctx.kind.is_command() {
            self.0 += *input;
        }
        ctx.output(&self.0);
    }
}

#[test]
fn create_cluster() {
    let mut server = Server::start(auto_addr(), Counter(0), None).expect("start() failed");
    assert!(server.node().is_none());

    let server_addr = server.listen_addr();
    let handle = std::thread::spawn(move || {
        // First call: OK
        let output: CreateClusterOutput = rpc(server_addr, req_without_params("CreateCluster"));
        assert_eq!(output.members.len(), 1);

        // Second call: NG
        let error = rpc_err(server_addr, req_without_params("CreateCluster"));
        assert_eq!(error, ErrorKind::ClusterAlreadyCreated.code());
    });

    while !handle.is_finished() {
        server.poll(POLL_TIMEOUT).expect("poll() failed");
    }
    assert!(handle.join().is_ok());
    assert!(server.node().is_some());
}

#[test]
fn add_and_remove_server() {
    let mut server0 = Server::start(auto_addr(), Counter(0), None).expect("start() failed");
    assert!(server0.node().is_none());

    // Create a cluster.
    let server_addr0 = server0.listen_addr();
    let handle = std::thread::spawn(move || {
        rpc::<CreateClusterOutput>(server_addr0, req_without_params("CreateCluster"))
    });
    while !handle.is_finished() {
        server0.poll(POLL_TIMEOUT).expect("poll() failed");
    }
    assert!(server0.node().is_some());

    // Add a server to the cluster.
    let mut server1 = Server::start(auto_addr(), Counter(0), None).expect("start() failed");
    let server_addr1 = server1.listen_addr();
    let handle = std::thread::spawn(move || {
        let output: AddServerOutput = rpc(server_addr0, add_server_req(server_addr1));
        assert_eq!(output.members.len(), 2);
        std::thread::sleep(Duration::from_millis(200));
    });
    while !handle.is_finished() {
        server0.poll(POLL_TIMEOUT).expect("poll() failed");
        server1.poll(POLL_TIMEOUT).expect("poll() failed");
    }
    assert!(server1.node().is_some());

    // Remove server0 from the cluster.
    let handle = std::thread::spawn(move || {
        let output: RemoveServerOutput = rpc(server_addr0, remove_server_req(server_addr0));
        assert_eq!(output.members.len(), 1);
        std::thread::sleep(Duration::from_millis(200));
    });
    while !handle.is_finished() {
        server0.poll(POLL_TIMEOUT).expect("poll() failed");
        server1.poll(POLL_TIMEOUT).expect("poll() failed");
    }
    assert!(server0.node().is_some()); // TODO: clear server0.node() if possible (but it seems difficult)
    assert!(server1.node().is_some());
}

#[test]
fn re_election() {
    let mut servers = Vec::new();
    let mut server0 = Server::start(auto_addr(), Counter(0), None).expect("start() failed");

    // Create a cluster.
    let server_addr0 = server0.listen_addr();
    let handle = std::thread::spawn(move || {
        // TODO: specify election timeout
        rpc::<CreateClusterOutput>(server_addr0, req_without_params("CreateCluster"))
    });
    while !handle.is_finished() {
        server0.poll(POLL_TIMEOUT).expect("poll() failed");
    }
    servers.push(server0);

    // Add two servers to the cluster.
    let server1 = Server::start(auto_addr(), Counter(0), None).expect("start() failed");
    let server2 = Server::start(auto_addr(), Counter(0), None).expect("start() failed");
    let server_addr1 = server1.listen_addr();
    let server_addr2 = server2.listen_addr();
    let handle = std::thread::spawn(move || {
        let mut contact_addr = server_addr0;
        for addr in [server_addr1, server_addr2] {
            let _: AddServerOutput = rpc(contact_addr, add_server_req(addr));
            contact_addr = addr;
            std::thread::sleep(Duration::from_millis(300));
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
    for _ in 0..200 {
        for server in servers.iter_mut().skip(1) {
            server.poll(POLL_TIMEOUT).expect("poll() failed");
        }
    }
    assert!(servers[1].is_leader() || servers[2].is_leader());

    for _ in 0..100 {
        for server in &mut servers {
            server.poll(POLL_TIMEOUT).expect("poll() failed");
        }
    }
    assert!(!servers[0].is_leader());
}

fn auto_addr() -> SocketAddr {
    "127.0.0.1:0".parse().expect("unreachable")
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

fn req_without_params(method: &str) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": jsonlrpc::JsonRpcVersion::V2,
        "method": method,
        "id": 0
    })
}

fn req<T: Serialize>(method: &str, params: T) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": jsonlrpc::JsonRpcVersion::V2,
        "method": method,
        "params": params,
        "id": 0
    })
}

fn add_server_req(server_addr: SocketAddr) -> serde_json::Value {
    req("AddServer", AddServerParams { server_addr })
}

fn remove_server_req(server_addr: SocketAddr) -> serde_json::Value {
    req("RemoveServer", RemoveServerParams { server_addr })
}

fn rpc<T>(server_addr: SocketAddr, request: impl Serialize) -> T
where
    T: for<'de> Deserialize<'de>,
{
    let mut client = RpcClient::new(connect(server_addr));
    let response: ResponseObject = client.call(&request).expect("call() failed");
    let result = response.into_std_result().expect("error response");
    serde_json::from_value(result).expect("malformed result")
}

fn rpc_err(server_addr: SocketAddr, request: impl Serialize) -> ErrorCode {
    let mut client = RpcClient::new(connect(server_addr));
    let response: ResponseObject = client.call(&request).expect("call() failed");
    response
        .into_std_result()
        .expect_err("not error response")
        .code
}
