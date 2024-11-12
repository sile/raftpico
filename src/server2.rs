use std::net::SocketAddr;

use jsonlrpc_mio::RpcServer;
use mio::{Events, Poll, Token};

const SERVER_TOKEN_MIN: Token = Token(usize::MAX / 2);
const SERVER_TOKEN_MAX: Token = Token(usize::MAX);

const EVENTS_CAPACITY: usize = 1024;

#[derive(Debug)]
pub struct RaftServer<M> {
    poller: Poll,
    events: Events,
    rpc_server: RpcServer,
    machine: M,
}

impl<M> RaftServer<M> {
    pub fn start(listen_addr: SocketAddr, machine: M) -> std::io::Result<Self> {
        let mut poller = Poll::new()?;
        let events = Events::with_capacity(EVENTS_CAPACITY);
        let rpc_server =
            RpcServer::start(&mut poller, listen_addr, SERVER_TOKEN_MIN, SERVER_TOKEN_MAX)?;
        Ok(Self {
            poller,
            events,
            rpc_server,
            machine,
        })
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.rpc_server.listen_addr()
    }

    pub fn machine(&self) -> &M {
        &self.machine
    }

    pub fn machine_mut(&mut self) -> &mut M {
        &mut self.machine
    }
}
