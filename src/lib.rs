//! A simple Raft framework for Rust built on top of the [raftbare] crate.
//!
//! [raftbare]: https://github.com/sile/raftbare
#![warn(missing_docs)]

mod broker;
pub mod command;
mod machine;
mod machines;
pub mod messages;
mod server;
mod storage;
pub mod types;

pub use machine::{ApplyContext, ApplyKind, Machine};
pub use messages::Request;
pub use server::Server;
pub use storage::FileStorage;
