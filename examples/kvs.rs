use std::{collections::HashMap, net::SocketAddr};

use clap::Parser;
use orfail::OrFail;
use raftpico::node::{Machine, MachineContext};
use serde::{Deserialize, Serialize};

#[derive(Parser)]
struct Args {
    listen_addr: SocketAddr,
}

fn main() -> orfail::Result<()> {
    let args = Args::parse();
    let mut node = raftpico::node::Node::new(args.listen_addr, KvsMachine::default()).or_fail()?;
    loop {
        node.poll_one(None).or_fail()?;
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct KvsMachine {
    entries: HashMap<String, serde_json::Value>,
}

impl Machine for KvsMachine {
    type Command = KvsCommand;

    fn apply(&mut self, ctx: &mut MachineContext, command: Self::Command) {
        match command {
            KvsCommand::Put { key, value } => {
                let value = self.entries.insert(key, value);
                ctx.reply(&serde_json::json!({
                   "old_value": value,
                }));
            }
            KvsCommand::Get { key } => {
                let value = self.entries.get(&key);
                ctx.reply(&serde_json::json!({
                   "value": value,
                }));
            }
            KvsCommand::Delete { key } => {
                let value = self.entries.remove(&key);
                ctx.reply(&serde_json::json!({
                   "old_value": value,
                }));
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum KvsCommand {
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
