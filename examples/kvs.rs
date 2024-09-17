use std::{collections::HashMap, net::SocketAddr};

use clap::Parser;
use raftpico::{Context, Machine, RaftServer, Result};
use serde::{Deserialize, Serialize};

#[derive(Parser)]
struct Args {
    listen_addr: SocketAddr,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut node = RaftServer::start(args.listen_addr, KvsMachine::default())?;
    loop {
        node.poll(None)?;
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct KvsMachine {
    entries: HashMap<String, serde_json::Value>,
}

impl Machine for KvsMachine {
    type Input = KvsInput;

    fn handle_input(&mut self, ctx: &mut Context, input: Self::Input) {
        match input {
            KvsInput::Put { key, value } => {
                let value = self.entries.insert(key, value);
                ctx.output(&value);
            }
            KvsInput::Get { key } => {
                let value = self.entries.get(&key);
                ctx.output(&value);
            }
            KvsInput::Delete { key } => {
                let value = self.entries.remove(&key);
                ctx.output(&value);
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum KvsInput {
    Put {
        key: String,
        value: serde_json::Value,
    },
    Get {
        key: String,
    },
    Delete {
        key: String,
    },
}
