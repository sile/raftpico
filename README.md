raftpico
========

[![raftpico](https://img.shields.io/crates/v/raftpico.svg)](https://crates.io/crates/raftpico)
[![Documentation](https://docs.rs/raftpico/badge.svg)](https://docs.rs/raftpico)
[![Actions Status](https://github.com/sile/raftpico/workflows/CI/badge.svg)](https://github.com/sile/raftpico/actions)
![License](https://img.shields.io/crates/l/raftpico)

A simple Raft framework for Rust built on top of the [raftbare](https://github.com/sile/raftbare) crate.

Example
-------

```console
$ cargo run --example kvs 127.0.0.1:4000
$ cargo run --example kvs 127.0.0.1:4001
$ cargo run --example kvs 127.0.0.1:4002
```

```console
$ echo '{"jsonrpc":"2.0", "id":0, "method":"CreateCluster"}' | nc -w 1 localhost 4000
{"jsonrpc":"2.0","id":0,"result":{"members":["127.0.0.1:4000"]}}

$ echo '{"jsonrpc":"2.0", "id":0, "method":"AddServer", "params":{"addr":"127.0.0.1:4001"}}' | nc -w 1 localhost 4000
{"jsonrpc":"2.0","id":0,"result":{"members":["127.0.0.1:4000","127.0.0.1:4001"]}}

$ echo '{"jsonrpc":"2.0", "id":0, "method":"AddServer", "params":{"addr":"127.0.0.1:4002"}}' | nc -w 1 localhost 4000
{"jsonrpc":"2.0","id":0,"result":{"members":["127.0.0.1:4000","127.0.0.1:4001","127.0.0.1:4002"]}}
```
