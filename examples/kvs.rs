use std::{collections::HashMap, net::SocketAddr, path::PathBuf};

use clap::Parser;
use raftpico::{Context, Machine, Result, Server, ServerOptions};
use serde::{Deserialize, Serialize};

#[derive(Parser)]
struct Args {
    listen_addr: SocketAddr,

    #[clap(long)]
    storage_file: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let options = ServerOptions {
        file_path: args.storage_file,
        ..Default::default()
    };
    let mut server = Server::with_options(args.listen_addr, KvsMachine::default(), options)?;
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
