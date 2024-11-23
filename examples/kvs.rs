use std::{collections::HashMap, net::SocketAddr, path::PathBuf};

use clap::Parser;
use raftpico::{Context, FileStorage, Machine, Server};
use serde::{Deserialize, Serialize};

#[derive(Parser)]
struct Args {
    listen_addr: SocketAddr,

    #[clap(long)]
    storage_file: Option<PathBuf>,
}

fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let storage = args
        .storage_file
        .map(|path| FileStorage::new(path))
        .transpose()?;
    let mut server = Server::<KvsMachine>::start(args.listen_addr, storage)?;
    loop {
        server.poll(None)?;
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct KvsMachine {
    entries: HashMap<String, serde_json::Value>,
}

impl Machine for KvsMachine {
    type Input = KvsInput;

    fn apply(&mut self, ctx: &mut Context, input: &Self::Input) {
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
