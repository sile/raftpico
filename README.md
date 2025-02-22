raftpico
========

[![raftpico](https://img.shields.io/crates/v/raftpico.svg)](https://crates.io/crates/raftpico)
[![Documentation](https://docs.rs/raftpico/badge.svg)](https://docs.rs/raftpico)
[![Actions Status](https://github.com/sile/raftpico/workflows/CI/badge.svg)](https://github.com/sile/raftpico/actions)
![License](https://img.shields.io/crates/l/raftpico)

A simple [Raft] framework for Rust built on top of the [raftbare](https://github.com/sile/raftbare) crate.

[Raft]: https://raft.github.io/

Features
--------

- **JSON-RPC API**: The framework exposes a [JSON-RPC] API for various operations including:
  - [`CreateCluster`]: Initialize a new Raft cluster.
  - [`AddServer`]: Add a new server to the cluster.
  - [`RemoveServer`]: Remove a server from the cluster.
  - [`Apply`]: Submit a command, perform a consistent query, or execute a local query on the state machine.
  - [`TakeSnapshot`]: Trigger a snapshot of the current state.
  - [`GetServerState`]: Retrieve the current state of an individual server.
- **Custom State Machines**: Provides a [`Machine`] trait that users can implement to define their own state machines that will be replicated across the Raft cluster.
- **Simple Codebase**: Designed to be easily understandable and modifiable, enabling users to add their own features with ease.
- **JSON Serialization**: Utilizes [JSON Lines] as the serialization format for persistent storage and communication between servers, offering simplicity and human-readability.

[JSON-RPC]: https://www.jsonrpc.org/specification
[JSON Lines]: https://jsonlines.org/
[`CreateCluster`]: https://docs.rs/raftpico/latest/raftpico/messages/enum.Request.html#variant.CreateCluster
[`AddServer`]: https://docs.rs/raftpico/latest/raftpico/messages/enum.Request.html#variant.AddServer
[`RemoveServer`]: https://docs.rs/raftpico/latest/raftpico/messages/enum.Request.html#variant.RemoveServer
[`Apply`]: https://docs.rs/raftpico/latest/raftpico/messages/enum.Request.html#variant.Apply
[`TakeSnapshot`]: https://docs.rs/raftpico/latest/raftpico/messages/enum.Request.html#variant.TakeSnapshot
[`GetServerState`]: https://docs.rs/raftpico/latest/raftpico/messages/enum.Request.html#variant.GetServerState
[`Machine`]: https://docs.rs/raftpico/latest/raftpico/trait.Machine.html

Example: Key-Value Store
------------------------

The code snippets provided below are from [examples/kvs.rs](examples/kvs.rs) and
demonstrate the implementation of a key-value store (KVS) using `raftpico`:

```rust
#[derive(Debug, Default, Serialize, Deserialize)]
struct KvsMachine {
    entries: HashMap<String, serde_json::Value>,
}

impl Machine for KvsMachine {
    type Input = KvsInput;

    fn apply(&mut self, ctx: &mut ApplyContext, input: Self::Input) {
        match input {
            KvsInput::Put(key, value) => {
                let value = self.entries.insert(key, value);
                ctx.output(&value);
            }
            KvsInput::Get(key) => {
                let value = self.entries.get(&key);
                ctx.output(&value);
            }
            KvsInput::Delete(key) => {
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
    Put(String, serde_json::Value),
    Get(String),
    Delete(String),
    List,
}
```

First, launch two KVS servers to form the cluster:

```console
/// Start a KVS server in one terminal.
$ cargo run --release --example kvs 127.0.0.1:4000

/// Start another KVS server in a different terminal.
$ cargo run --release --example kvs 127.0.0.1:4001
```

To set up a Raft cluster, run the following commands:
```console
$ cargo install jlot

// Initialize a Raft cluster with a server.
$ jlot req CreateCluster | jlot call :4000
{"jsonrpc":"2.0","id":0,"result":{"members":["127.0.0.1:4000"]}}

// Add an additional server to the cluster.
$ jlot req AddServer '{"addr":"127.0.0.1:4001"}' | jlot call :4000
{"jsonrpc":"2.0","id":0,"result":{"members":["127.0.0.1:4000","127.0.0.1:4001"]}}
```

To perform operations on the replicated state machine, use an `Apply` request as demonstrated below:
```console
// Insert an entry into the KVS.
$ jlot req Apply '{"input":{"Put":["foo", 123]}, "kind":"Command"}' | jlot call :4000
{"jsonrpc":"2.0","id":0,"result":null}

// Retrieve the value of the previously inserted entry from another server.
$ jlot req Apply '{"input":{"Get":"foo"}, "kind":"Query"}' | jlot call :4001
{"jsonrpc":"2.0","id":0,"result":123}
```

Limitations
-----------

- **JSON Serialization**:
  - JSON is evidently not the most efficient format for storage and internal communication.
    - This is particularly true if the replicated state machine (or its input/output) includes BLOBs.
  - Despite this drawback, `raftpico` intentionally uses JSON due to its emphasis on simplicity and readability.
    - Besides, the performance of `raftpico` appears adequate for many use cases (see the [Benchmark](#Benchmark) section).
- **JSON-RPC (over TCP) API**:
  - When the Raft server runs in the same process as the client, this can introduce unnecessary overhead.
  - [NOTE] Adding methods such as `Server::apply()` would be straightforward.
- **Single-Threaded Server**:
  - Currently, [`Server`] does not utilize multi-threading.
  - Consequently, intensive tasks like applying heavy commands to the state machine and handling large snapshots can block the server's running thread.
    - To make matters worse, `raftpico` processes an entire snapshot all at once instead of breaking it into smaller chunks.
- **In-Memory Log Entries**:
  - To maintain simplicity and minimize storage reads, `raftpico` also keeps log entries in memory.
  - This means memory usage increases with each proposed command until the next snapshot is taken.

In summary, `raftpico` is not suitable for the following purposes:
- Processing large BLOBs that are inefficient to serialize using JSON
- Managing large state machines where taking snapshots is prohibitively costly

[`Server`]: https://docs.rs/raftpico/latest/raftpico/struct.Server.html

Benchmark
---------

This is a casual benchmark of the above example KVS server.

Environment:
```console
$ lsb_release -a
No LSB modules are available.
Distributor ID: Ubuntu
Description:    Ubuntu 20.04.6 LTS
Release:        20.04
Codename:       focal

$ cat /proc/cpuinfo | grep 'model name' | head -1
model name      : AMD EPYC 7763 64-Core Processor

$ cat /proc/cpuinfo | grep 'model name' | wc -l
8
```

Create a KVS cluster with three nodes:
```console
$ parallel ::: 'cargo run --release --example kvs 127.0.0.1:4000 kvs-4000.jsonl' \
               'cargo run --release --example kvs 127.0.0.1:4001 kvs-4001.jsonl' \
               'cargo run --release --example kvs 127.0.0.1:4002 kvs-4002.jsonl'

$ echo $(jlot req CreateCluster) \
       $(jlot req AddServer '{"addr":"127.0.0.1:4001"}') \
       $(jlot req AddServer '{"addr":"127.0.0.1:4002"}') | jlot call :4000
```

`Put` command benchmark:
```console
// Generate 100,000 random requests.
$ cargo install rjg
$ rjg --count 100000 \
      --var key='{"$str":["$alpha", "$alpha", "$alpha"]}' \
      --var params='{"kind":"Command", "input":{"Put":["$key", "$u32"]}}' \
      '{"jsonrpc":"2.0", "id":"$i", "method":"Apply", "params":"$params"}' > commands.jsonl

// Execute the requests with a concurrency of 1000.
$ cat commands.jsonl | jlot call :4000 :4001 :4002 -a -c 1000 | jlot stats | jq .
{
  "rpc_calls": 100000,
  "duration": 1.874101709,
  "max_concurrency": 1000,
  "rps": 53358.896969022506,
  "latency": {
    "min": 0.003427635,
    "p25": 0.015225686,
    "p50": 0.018969559,
    "p75": 0.02225708,
    "max": 0.037078306,
    "avg": 0.018675618
  }
}
```

`Get` query (consistent query) benchmark:
```console
// Generate 100,000 random requests.
$ rjg --count 100000 \
      --var key='{"$str":["$alpha", "$alpha", "$alpha"]}' \
      --var params='{"kind":"Query", "input":{"Get":"$key"}}' \
      '{"jsonrpc":"2.0", "id":"$i", "method":"Apply", "params":"$params"}' > queries.jsonl

// Execute the requests with a concurrency of 1000.
$ cat queries.jsonl | jlot call :4000 :4001 :4002 -a -c 1000 | jlot stats | jq .
{
  "rpc_calls": 100000,
  "duration": 1.362893895,
  "max_concurrency": 1000,
  "rps": 73373.28339855833,
  "latency": {
    "min": 0.002436916,
    "p25": 0.01140849,
    "p50": 0.014045573,
    "p75": 0.01576976,
    "max": 0.024304805,
    "avg": 0.013583492
  }
}
```

`Get` local query benchmark:
```console
// Generate 100,000 random requests.
$ rjg --count 100000 \
      --var key='{"$str":["$alpha", "$alpha", "$alpha"]}' \
      --var params='{"kind":"LocalQuery", "input":{"Get":"$key"}}' \
      '{"jsonrpc":"2.0", "id":"$i", "method":"Apply", "params":"$params"}' > local-queries.jsonl

// Execute the requests with a concurrency of 1000.
$ cat local-queries.jsonl | jlot call :4000 :4001 :4002 -a -c 1000 | jlot stats | jq .
{
  "rpc_calls": 100000,
  "duration": 0.410423888,
  "max_concurrency": 993,
  "rps": 243650.53527293712,
  "latency": {
    "min": 0.000639122,
    "p25": 0.003991307,
    "p50": 0.004037483,
    "p75": 0.00421849,
    "max": 0.004560348,
    "avg": 0.004078114
  }
}
```
