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
  - Obviously, JSON is not the best efficient format for storage and internal communications.
    - Especially, if the replicated state machine (or input / output of that machine) contains BLOBs.
  - Although such demerit exists, as the primary focus of `raftpico` is simplicity and readability, it inrendedly adopts JSON.
    - However, the performance of `raftpico` seems sufficient for many usecases (see the [Benchmark](#Benchmark) section).
- **JSON-RPC (over TCP) API**:
  - This incurrs unnecessary overhead if the Raft server runs on the same process with the client.
  - [NOTE] To add methods such as `Server::apply()` would be easy though.
- **Single Threaded Server**:
  - Currently, a `Server` does not facilitate multi-thread.
  - It's not sure but some heavy workload such as snapshot handling can be offloaded to a non main thread.
- **On Memory Log Entries**:
  - Thus, ...
  
In other words, `raftpico` is not suited for the following purposes:
- Large BLOBs that inefficient to seriarize using JSON 
- Large state machine that is too costly to take the snapshot

Benchmark
---------

```console
$ parallel -u ::: 'cargo run --release --example kvs 127.0.0.1:4000' 'cargo run --release --example kvs 127.0.0.1:4001' 'cargo run --release --example kvs 127.0.0.1:4001'

$ echo $(jlot req CreateCluster) $(jlot req AddServer '{"addr":"127.0.0.1:4001"}') $(jlot req AddServer '{"addr":"127.0.0.1:4002"}') | jlot call :4000

$ rjg --count 100000 --var key='{"$str": ["$alpha", "$alpha", "$alpha"]}' --var put='{"Put": {"key":"$key", "value":"$u32"}}' --var get='{"Get": {"key": "$key"}}' -v delete='{"Delete":{"key":"$key"}}' '{"jsonrpc":"2.0", "id":"$i", "method":"Apply", "params": {"kind":"Command", "input":{"$oneof": ["$get", "$put", "$delete"]}}}' > requests.jsonl
$ cat requests.jsonl | jlot call :4000 :4001 :4002 -a -c 1000 | jlot stats | jq .
```
