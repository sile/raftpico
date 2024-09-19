use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    net::SocketAddr,
    time::{Duration, SystemTime},
};

use clap::Parser;
use raftpico::{Context, Machine, Result, Server};
use serde::{Deserialize, Serialize};

const TICK_RESOLUTION: Duration = Duration::from_millis(10);

#[derive(Parser)]
struct Args {
    listen_addr: SocketAddr,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut server = Server::start(args.listen_addr, KvsMachine::default())?;
    let mut last_tick = Duration::default();
    let mut new_leader = true;
    let mut expire_queue = BinaryHeap::new();
    loop {
        server.poll(Some(TICK_RESOLUTION))?;

        if server.node().map_or(false, |n| n.role().is_leader()) {
            if let Ok(now) = SystemTime::UNIX_EPOCH.elapsed() {
                if last_tick + TICK_RESOLUTION <= now {
                    let input = KvsInput::Tick { now_unix_time: now };
                    let _promise = server.propose_user_command(&input);
                    last_tick = now;
                }
            }

            if new_leader {
                expire_queue.extend(
                    server
                        .machine()
                        .entries
                        .iter()
                        .map(|(key, entry)| Reverse((entry.expire_time, key.clone()))),
                );
            }
            expire_queue.extend(server.machine_mut().new_entries.drain(..).map(Reverse));

            while expire_queue.peek().map_or(false, |v| v.0 .0 <= last_tick) {
                let (expire_time, key) = expire_queue.pop().expect("unreachable").0;
                if server
                    .machine()
                    .entries
                    .get(&key)
                    .map_or(false, |x| x.expire_time == expire_time)
                {
                    let input = KvsInput::Expire { key };
                    let _promise = server.propose_user_command(&input);
                }
            }

            new_leader = false;
        } else {
            new_leader = true;
            expire_queue.clear();
        }
    }
}

// TODO: rename (ValueWithTtl?)
#[derive(Debug, Serialize, Deserialize)]
struct Entry {
    value: serde_json::Value,
    expire_time: Duration,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct KvsMachine {
    entries: HashMap<String, Entry>,
    now_unix_time: Duration,

    #[serde(skip)]
    new_entries: Vec<(Duration, String)>,
}

impl Machine for KvsMachine {
    type Input = KvsInput;

    fn handle_input(&mut self, ctx: &mut Context, input: Self::Input) {
        match input {
            KvsInput::Put { key, value, ttl_ms } => {
                let expire_time = self.now_unix_time + Duration::from_millis(ttl_ms as u64);
                let entry = Entry { value, expire_time };
                let old = self.entries.insert(key.clone(), entry);

                if ctx.node().role().is_leader() {
                    self.new_entries.push((expire_time, key));
                }

                ctx.output(&old);
            }
            KvsInput::Get { key } => {
                let entry = self.entries.get(&key);
                ctx.output(&entry);
            }
            KvsInput::Tick { now_unix_time } => {
                self.now_unix_time = now_unix_time;
            }
            KvsInput::Expire { key } => {
                self.entries.remove(&key);
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
        ttl_ms: u16,
    },
    Get {
        key: String,
    },

    // Internal
    Tick {
        now_unix_time: Duration,
    },
    Expire {
        key: String,
    },
}
