// TODO: rename module
use std::{
    collections::{HashMap, VecDeque},
    io::ErrorKind,
    net::SocketAddr,
    time::Duration,
};

use jsonlrpc::JsonlStream;
use mio::{
    event::Event,
    net::{TcpListener, TcpStream},
    Events, Interest, Poll, Token,
};
use serde::Serialize;

use crate::{
    request::{IncomingMessage, OutgoingMessage},
    Result, ServerOptions,
};

const LISTENER_TOKEN: Token = Token(0);

#[derive(Debug)]
pub struct MessageBroker {
    poller: Poll,
    events: Events,
    inner: MessageBrokerInner,
}

impl MessageBroker {
    pub fn new(listen_addr: SocketAddr, options: &ServerOptions) -> Result<Self> {
        let mut listener = TcpListener::bind(listen_addr)?;
        log::info!(
            "Raft server was started: listen_addr={}",
            listener.local_addr()?
        );

        let poller = Poll::new()?;
        let events = Events::with_capacity(options.mio_events_capacity);
        poller
            .registry()
            .register(&mut listener, LISTENER_TOKEN, Interest::READABLE)?;

        Ok(Self {
            poller,
            events,
            inner: MessageBrokerInner {
                max_write_buf_size: options.max_write_buf_size,
                next_token: Token(LISTENER_TOKEN.0 + 1),
                listener,
                connections: HashMap::new(),
                incoming_queue: VecDeque::new(),
            },
        })
    }

    pub fn poll(&mut self, timeout: Duration) -> Result<()> {
        self.poller.poll(&mut self.events, Some(timeout))?;
        for event in self.events.iter() {
            self.inner.handle_event(&mut self.poller, event)?;
        }
        Ok(())
    }

    pub fn send<T: OutgoingMessage>(&mut self, peer: Token, msg: &T) -> Result<()> {
        self.inner.send(&mut self.poller, peer, msg)
    }

    pub fn try_recv(&mut self) -> Option<(Token, IncomingMessage)> {
        self.inner.incoming_queue.pop_front()
    }
}

#[derive(Debug)]
struct MessageBrokerInner {
    max_write_buf_size: usize,
    next_token: Token,
    listener: TcpListener,
    connections: HashMap<Token, Connection>,
    incoming_queue: VecDeque<(Token, IncomingMessage)>,
}

impl MessageBrokerInner {
    fn send<T: OutgoingMessage>(&mut self, poller: &mut Poll, peer: Token, msg: &T) -> Result<()> {
        let Some(conn) = self.connections.get_mut(&peer) else {
            // TODO: stats
            log::debug!(
                "Message was discarded: peer_token={}, reason=disconnected",
                peer.0
            );
            return Ok(());
        };

        if !msg.is_mandatory() && conn.stream.write_buf().len() > self.max_write_buf_size {
            // TOD: stats
            log::debug!(
                "Message was discarded: peer={}, token={}, reason=over_capacity, write_buf_size={}",
                conn.addr,
                peer.0,
                conn.stream.write_buf().len()
            );
            return Ok(());
        }

        if !conn.send(poller, msg)? {
            self.handle_disconnected(poller, peer)?;
        }
        Ok(())
    }

    fn handle_disconnected(&mut self, poller: &mut Poll, token: Token) -> Result<()> {
        // TODO: stats
        let mut conn = self.connections.remove(&token).expect("unreachable");
        poller.registry().deregister(conn.stream.inner_mut())?;
        Ok(())
    }

    fn handle_event(&mut self, poller: &mut Poll, event: &Event) -> Result<()> {
        let token = event.token();
        if token == LISTENER_TOKEN {
            self.handle_listener_event(poller)?;
        } else if let Some(connection) = self.connections.get_mut(&token) {
            if !connection.handle_event(poller, &mut self.incoming_queue)? {
                self.handle_disconnected(poller, token)?;
            }
        } else {
            unreachable!("Unknown mio token event: {event:?}");
        }
        Ok(())
    }

    fn handle_listener_event(&mut self, poller: &mut Poll) -> Result<()> {
        while let Some((mut stream, addr)) =
            would_block(self.listener.accept().map_err(From::from))?
        {
            let _ = stream.set_nodelay(true);
            let token = self.next_token();

            log::debug!(
                "New TCP connection was accepted: token={}, addr={addr:?}",
                token.0
            );
            // TODO: self.stats.accept_count += 1;

            poller
                .registry()
                .register(&mut stream, token, Interest::READABLE)?;
            let mut connection = Connection {
                token,
                addr,
                stream: JsonlStream::new(stream),
            };
            if connection.handle_event(poller, &mut self.incoming_queue)? {
                self.connections.insert(token, connection);
            }
        }
        Ok(())
    }

    fn next_token(&mut self) -> Token {
        loop {
            let token = self.next_token;
            self.next_token.0 = self.next_token.0.wrapping_add(1);
            if token == LISTENER_TOKEN || self.connections.contains_key(&token) {
                continue;
            }
            return token;
        }
    }
}

#[derive(Debug)]
struct Connection {
    token: Token,
    addr: SocketAddr,
    stream: JsonlStream<TcpStream>,
}

impl Connection {
    fn send<T: Serialize>(&mut self, poller: &mut Poll, msg: &T) -> Result<bool> {
        // TODO: stats
        let start_writing = !self.is_writing();
        match self.stream.write_object(msg) {
            // TODO
            // Err(_e) if !self.connected => {
            //     // TODO: self.stream.write_to_buf()
            //     Ok(())
            // }
            Err(e) if e.io_error_kind() == Some(std::io::ErrorKind::WouldBlock) => {
                if start_writing {
                    poller.registry().reregister(
                        self.stream.inner_mut(),
                        self.token,
                        Interest::READABLE | Interest::WRITABLE,
                    )?;
                }
                Ok(true)
            }
            Err(e) => {
                log::debug!(
                    "TCP write error: peer={}, token={}, reason={e}",
                    self.addr,
                    self.token.0
                );
                Ok(false)
            }
            Ok(_) => Ok(true),
        }
    }

    fn handle_event(
        &mut self,
        poller: &mut Poll,
        incoming_queue: &mut VecDeque<(Token, IncomingMessage)>,
    ) -> Result<bool> {
        if !self.poll_send(poller)? {
            return Ok(false);
        }
        if !self.poll_recv(poller, incoming_queue)? {
            return Ok(false);
        }
        Ok(true)
    }

    fn poll_send(&mut self, poller: &mut Poll) -> Result<bool> {
        if !self.is_writing() {
            return Ok(true);
        }

        // TODO: stats
        match self.stream.flush() {
            Err(e) if e.io_error_kind() == Some(ErrorKind::WouldBlock) => {}
            Err(e) => {
                log::debug!(
                    "TCP write error: peer={}, token={}, reason={e}",
                    self.addr,
                    self.token.0
                );
                return Ok(false);
            }
            Ok(()) => {
                // Remove Interest::WRITABLE
                poller.registry().reregister(
                    self.stream.inner_mut(),
                    self.token,
                    Interest::READABLE,
                )?;
            }
        }
        Ok(true)
    }

    fn poll_recv(
        &mut self,
        poller: &mut Poll,
        incoming_queue: &mut VecDeque<(Token, IncomingMessage)>,
    ) -> Result<bool> {
        // TODO
        Ok(true)
    }

    fn is_writing(&self) -> bool {
        !self.stream.write_buf().is_empty()
    }
}

fn would_block<T>(result: Result<T>) -> Result<Option<T>> {
    match result {
        Ok(v) => Ok(Some(v)),
        Err(e) if e.io.kind() == ErrorKind::WouldBlock => Ok(None),
        Err(e) => Err(e),
    }
}
