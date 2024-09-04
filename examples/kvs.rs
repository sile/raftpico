use std::{
    collections::HashMap,
    io::{
        prelude::{Read, Write},
        Error, ErrorKind,
    },
    net::SocketAddr,
    time::Duration,
};

use clap::Parser;
use orfail::OrFail;
use raftpico::{Command, Machine, RaftNode};
use serde::{Deserialize, Serialize};

const TIMEOUT: Duration = Duration::from_secs(3);

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
    node.run_while(|| true).or_fail()?;
    Ok(())
}

#[derive(Debug, Default)]
struct KvsMachine {
    kvs: HashMap<String, serde_json::Value>,
}

impl Machine for KvsMachine {
    type Command = KvsCommand;

    fn apply(&mut self, command: &Self::Command) {
        todo!()
    }

    fn encode(&self, buf: &mut Vec<u8>) {
        todo!()
    }

    fn decode(buf: &[u8]) -> Self {
        todo!()
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum KvsCommand {
    Put {
        key: String,
        value: serde_json::Value,
    },
    Delete {
        key: String,
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
