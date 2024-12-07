raftpico
========

[![raftpico](https://img.shields.io/crates/v/raftpico.svg)](https://crates.io/crates/raftpico)
[![Documentation](https://docs.rs/raftpico/badge.svg)](https://docs.rs/raftpico)
[![Actions Status](https://github.com/sile/raftpico/workflows/CI/badge.svg)](https://github.com/sile/raftpico/actions)
![License](https://img.shields.io/crates/l/raftpico)

A simple Raft framework for Rust built on top of the [raftbare](https://github.com/sile/raftbare) crate.

Features
--------

**WIP**

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

API
---

**WIP**

Limitations
-----------

**WIP**

- Single thread
  - Thus, ...
- JSON-RPC
  - Thus, ...
- Copy of the log entries in the memory
  - Thus, ...

Benchmark
---------

```console
$ parallel -u ::: 'cargo run --release --example kvs 127.0.0.1:4000' 'cargo run --release --example kvs 127.0.0.1:4001' 'cargo run --release --example kvs 127.0.0.1:4001'

$ echo $(jlot req CreateCluster) $(jlot req AddServer '{"addr":"127.0.0.1:4001"}') $(jlot req AddServer '{"addr":"127.0.0.1:4002"}') | jlot call :4000

$ rjg --count 100000 --var key='{"$str": ["$alpha", "$alpha", "$alpha"]}' --var put='{"Put": {"key":"$key", "value":"$u32"}}' --var get='{"Get": {"key": "$key"}}' -v delete='{"Delete":{"key":"$key"}}' '{"jsonrpc":"2.0", "id":"$i", "method":"Apply", "params": {"kind":"Command", "input":{"$oneof": ["$get", "$put", "$delete"]}}}' > requests.jsonl
$ cat requests.jsonl | jlot call :4000 :4001 :4002 -a -c 1000 | jlot stats | jq .
```
