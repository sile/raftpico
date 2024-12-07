use std::{collections::HashMap, net::SocketAddr, path::PathBuf};

use clap::Parser;
use raftpico::{ApplyContext, FileStorage, Machine, Server};
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
    eprintln!("Raft server has started: {}", server.addr());

    let mut prev_term_role = None;
    loop {
        server.poll(None)?;

        let term = server.node().current_term().get();
        let role = server.node().role();
        if server.is_initialized() && prev_term_role != Some((term, role)) {
            eprintln!("Raft term or role has changed: term={term}, role={role:?}");
            prev_term_role = Some((term, role));
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct KvsMachine {
    entries: HashMap<String, serde_json::Value>,
}

impl Machine for KvsMachine {
    type Input = KvsInput;

    fn apply(&mut self, ctx: &mut ApplyContext, input: Self::Input) {
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
            KvsInput::List => {
                ctx.output(&self.entries.keys().collect::<Vec<_>>());
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    List,
}
