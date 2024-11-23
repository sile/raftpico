use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    net::SocketAddr,
    time::{Duration, SystemTime},
};

use clap::Parser;
use raftpico::{Context, Machine, Server};
use serde::{Deserialize, Serialize};

const TICK_RESOLUTION: Duration = Duration::from_millis(10);

#[derive(Parser)]
struct Args {
    listen_addr: SocketAddr,
}

fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let mut server = Server::<KvsMachine>::start(args.listen_addr, None)?;
    let mut last_tick = Duration::default();
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
    expire_queue: BinaryHeap<Reverse<(Duration, String)>>,
    now_unix_time: Duration,
}

impl Machine for KvsMachine {
    type Input = KvsInput;

    fn apply(&mut self, ctx: &mut Context, input: &Self::Input) {
        let input = input.clone(); //TODO
        match input {
            KvsInput::Put { key, value, ttl_ms } => {
                let expire_time = self.now_unix_time + Duration::from_millis(ttl_ms as u64);
                let entry = Entry { value, expire_time };
                let old = self.entries.insert(key.clone(), entry);
                self.expire_queue.push(Reverse((expire_time, key)));
                ctx.output(&old);
            }
            KvsInput::Get { key } => {
                let entry = self.entries.get(&key);
                ctx.output(&entry);
            }
            KvsInput::Tick { now_unix_time } => {
                self.now_unix_time = now_unix_time;

                while self
                    .expire_queue
                    .peek()
                    .map_or(false, |v| v.0 .0 <= now_unix_time)
                {
                    let (expire_time, key) = self.expire_queue.pop().expect("unreachable").0;
                    if self
                        .entries
                        .get(&key)
                        .map_or(false, |x| x.expire_time == expire_time)
                    {
                        self.entries.remove(&key);
                    }
                }
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
        ttl_ms: u16,
    },
    Get {
        key: String,
    },

    // Internal
    Tick {
        now_unix_time: Duration,
    },
}
