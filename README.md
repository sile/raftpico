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

Example
-------

**WIP**

```console
$ cargo run --release --example kvs 127.0.0.1:4000
$ cargo run --release --example kvs 127.0.0.1:4001
$ cargo run --release --example kvs 127.0.0.1:4002
```

```console
$ jlot req CreateCluster | jlot call :4000
{"jsonrpc":"2.0","id":0,"result":{"members":["127.0.0.1:4000"]}}

$ jlot req AddServer '{"addr":"127.0.0.1:4001"}' | jlot call :4000
{"jsonrpc":"2.0","id":0,"result":{"members":["127.0.0.1:4000","127.0.0.1:4001"]}}

$ jlot req AddServer '{"addr":"127.0.0.1:4002"}' | jlot call :4000
{"jsonrpc":"2.0","id":0,"result":{"members":["127.0.0.1:4000","127.0.0.1:4001","127.0.0.1:4002"]}}

$ jlot req Apply '{"input":{"Put":{"key":"foo","value":1}}, "kind":"Command"}' | jlot call :4000
{"jsonrpc":"2.0","id":0,"result":null}

$ jlot req Apply '{"input":{"Get":{"key":"foo","value":1}}, "kind":"Query"}' | jlot call :4000
{"jsonrpc":"2.0","id":0,"result":1}
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
  - Consequently, intensive tasks like handling large snapshots can block the server's running thread.
- **In-Memory Log Entries**:
  - To maintain simplicity and minimize storage reads, `raftpico` also keeps log entries in memory.
  - This means memory usage increases with each proposed command until the next snapshot is taken.

In summary, `raftpico` is not suitable for the following purposes:
- Processing large BLOBs that are inefficient to serialize using JSON
- Managing large state machines where taking snapshots is prohibitively costly

[`Server`]: https://docs.rs/raftpico/latest/raftpico/struct.Server.html

Benchmark
---------

```console
$ parallel -u ::: 'cargo run --release --example kvs 127.0.0.1:4000' 'cargo run --release --example kvs 127.0.0.1:4001' 'cargo run --release --example kvs 127.0.0.1:4001'

$ echo $(jlot req CreateCluster) $(jlot req AddServer '{"addr":"127.0.0.1:4001"}') $(jlot req AddServer '{"addr":"127.0.0.1:4002"}') | jlot call :4000

$ rjg --count 100000 --var key='{"$str": ["$alpha", "$alpha", "$alpha"]}' --var put='{"Put": {"key":"$key", "value":"$u32"}}' --var get='{"Get": {"key": "$key"}}' -v delete='{"Delete":{"key":"$key"}}' '{"jsonrpc":"2.0", "id":"$i", "method":"Apply", "params": {"kind":"Command", "input":{"$oneof": ["$get", "$put", "$delete"]}}}' > requests.jsonl
$ cat requests.jsonl | jlot call :4000 :4001 :4002 -a -c 1000 | jlot stats | jq .
```
