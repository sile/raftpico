use std::{collections::HashMap, net::SocketAddr, path::PathBuf};

use clap::Parser;
use raftpico::{Context2, FileStorage, Machine2, Result, Server};
use serde::{Deserialize, Serialize};

#[derive(Parser)]
struct Args {
    listen_addr: SocketAddr,

    #[clap(long)]
    storage_file: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let storage = args
        .storage_file
        .map(|path| FileStorage::new(path))
        .transpose()?;
    let mut server = Server::start(args.listen_addr, KvsMachine::default(), storage)?;
    loop {
        server.poll(None)?;
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct KvsMachine {
    entries: HashMap<String, serde_json::Value>,
}

impl Machine2 for KvsMachine {
    type Input = KvsInput;

    fn apply(&mut self, ctx: &mut Context2, input: &Self::Input) {
        let input = input.clone(); //TODO
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
