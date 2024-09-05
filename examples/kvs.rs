use std::{
    collections::HashMap,
    io::{
        prelude::{Read, Write},
        BufRead, Error, ErrorKind,
    },
    net::{SocketAddr, TcpStream},
    sync::mpsc::RecvTimeoutError,
    time::Duration,
};

use clap::Parser;
use orfail::OrFail;
use raftpico::{Command, Machine, RaftNode};
use serde::{Deserialize, Serialize};

const TIMEOUT: Duration = Duration::from_secs(3);

// TODO: struct
#[derive(Parser)]
enum Args {
    Run {
        addr: SocketAddr,

        #[clap(long, conflicts_with("join"))]
        init_cluster: bool,

        #[clap(
            long,
            value_name = "CONTACT_ADDR",
            required_unless_present("init_cluster")
        )]
        join: Option<SocketAddr>,
    },
}

fn main() -> orfail::Result<()> {
    let args = Args::parse();
    match args {
        Args::Run {
            addr,
            init_cluster,
            join,
        } => {
            run(addr, init_cluster, join).or_fail()?;
        }
    }
    Ok(())
}

fn run(addr: SocketAddr, init_cluster: bool, join: Option<SocketAddr>) -> orfail::Result<()> {
    let mut node = RaftNode::<KvsMachine>::new(addr).or_fail()?;
    if init_cluster {
        node.create_cluster().or_fail()?;
    } else if let Some(contact_addr) = join {
        node.join(contact_addr, Some(TIMEOUT)).or_fail()?;
    }

    let (tx, rx) = std::sync::mpsc::channel();
    let listener = std::net::TcpListener::bind(addr).or_fail()?;
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let stream = stream.or_fail().unwrap_or_else(|e| panic!("{e}"));
            let tx = tx.clone();
            std::thread::spawn(move || {
                // TODO: write error response if failed
                handle_client(stream, tx)
                    .or_fail()
                    .unwrap_or_else(|e| panic!("{e}"))
            });
        }
    });

    loop {
        node.run_one().or_fail()?;
        match rx.recv_timeout(Duration::from_millis(10)) {
            Ok(command) => {
                node.propose_command(command, None).or_fail()?;
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }

    Ok(())
}

fn handle_client(
    mut stream: TcpStream,
    tx: std::sync::mpsc::Sender<KvsCommand>,
) -> orfail::Result<()> {
    let reader = std::io::BufReader::new(stream.try_clone().or_fail()?);
    for line in reader.lines() {
        let line = line.or_fail()?;
        let request: JsonRpcRequest = serde_json::from_str(&line).or_fail()?;
        dbg!(&request);
        let (id, result_rx, command) = request.into_command();
        tx.send(command).or_fail()?;

        let result = result_rx.recv().or_fail()?;
        let response = JsonRpcOkResponse {
            jsonrpc: JsonRpcVersion::V2,
            result: OldValue { old_value: result },
            id,
        };
        let mut response = serde_json::to_string(&response).or_fail()?;
        response.push('\n');
        stream.write_all(response.as_bytes()).or_fail()?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JsonRpcVersion {
    #[serde(rename = "2.0")]
    V2,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestId {
    Number(i64),
    String(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcOkResponse {
    jsonrpc: JsonRpcVersion,
    result: OldValue,
    id: RequestId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OldValue {
    old_value: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum JsonRpcRequest {
    Put {
        jsonrpc: JsonRpcVersion,
        params: PutParams,
        id: RequestId,
    },
    Delete {
        jsonrpc: JsonRpcVersion,
        params: DeleteParams,
        id: RequestId,
    },
}

impl JsonRpcRequest {
    pub fn into_command(
        self,
    ) -> (
        RequestId,
        std::sync::mpsc::Receiver<Option<serde_json::Value>>,
        KvsCommand,
    ) {
        let (tx, rx) = std::sync::mpsc::channel();
        match self {
            JsonRpcRequest::Put { params, id, .. } => (
                id,
                rx,
                KvsCommand::Put {
                    key: params.key,
                    value: params.value,
                    result_tx: Some(tx),
                },
            ),
            JsonRpcRequest::Delete { params, id, .. } => (
                id,
                rx,
                KvsCommand::Delete {
                    key: params.key,
                    result_tx: Some(tx),
                },
            ),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutParams {
    pub key: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteParams {
    pub key: String,
}

#[derive(Debug, Default)]
struct KvsMachine {
    kvs: HashMap<String, serde_json::Value>,
}

impl Machine for KvsMachine {
    type Command = KvsCommand;

    fn apply(&mut self, command: &Self::Command) {
        match command {
            KvsCommand::Put {
                key,
                value,
                result_tx,
            } => {
                let result = self.kvs.insert(key.clone(), value.clone());
                if let Some(tx) = result_tx {
                    let _ = tx.send(result);
                }
            }
            KvsCommand::Delete { key, result_tx } => {
                let result = self.kvs.remove(key);
                if let Some(tx) = result_tx {
                    let _ = tx.send(result);
                }
            }
        }
    }

    fn encode<W: Write>(&self, _writer: W) -> std::io::Result<()> {
        todo!()
    }

    fn decode<R: Read>(_reader: R) -> std::io::Result<Self> {
        todo!()
    }
}

type ApplyResult = std::sync::mpsc::Sender<Option<serde_json::Value>>;

#[derive(Debug, Serialize, Deserialize)]
pub enum KvsCommand {
    Put {
        key: String,
        value: serde_json::Value,

        #[serde(default, skip)]
        result_tx: Option<ApplyResult>,
    },
    Delete {
        key: String,

        #[serde(default, skip)]
        result_tx: Option<ApplyResult>,
    },
}

impl Command for KvsCommand {
    fn encode<W: Write>(&self, writer: W) -> std::io::Result<()> {
        serde_json::to_writer(writer, self).map_err(|e| Error::new(ErrorKind::InvalidData, e))
    }

    fn decode<R: Read>(reader: R) -> std::io::Result<Self> {
        serde_json::from_reader(reader).map_err(|e| Error::new(ErrorKind::InvalidData, e))
    }
}
