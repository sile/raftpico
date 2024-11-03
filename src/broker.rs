use std::{collections::HashMap, net::SocketAddr, time::Duration};

use jsonlrpc::JsonlStream;
use mio::{
    event::Event,
    net::{TcpListener, TcpStream},
    Events, Interest, Poll, Token,
};

use crate::{Result, ServerOptions};

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
                next_token: Token(LISTENER_TOKEN.0 + 1),
                listener,
                connections: HashMap::new(),
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

    // pub fn broadcast();
    // pub fn send_to();
    // pub fn try_recv();
}

#[derive(Debug)]
struct MessageBrokerInner {
    next_token: Token,
    listener: TcpListener,
    connections: HashMap<Token, Connection>,
}

impl MessageBrokerInner {
    fn handle_event(&mut self, poller: &mut Poll, event: &Event) -> Result<()> {
        if event.token() == LISTENER_TOKEN {
            self.handle_listener_event(poller)?;
        } else if let Some(connection) = self.connections.get_mut(&event.token()) {
            if !connection.handle_event(poller)? {
                self.connections.remove(&event.token());
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

            log::debug!("New TCP connection was accepted: addr={addr:?}, token={token:?}");
            // TODO: self.stats.accept_count += 1;

            poller
                .registry()
                .register(&mut stream, token, Interest::READABLE)?;
            let mut connection = Connection {
                token,
                addr,
                stream: JsonlStream::new(stream),
            };
            if connection.handle_event(poller)? {
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
    pub token: Token,
    pub addr: SocketAddr,
    pub stream: JsonlStream<TcpStream>,
}

impl Connection {
    fn handle_event(&mut self, poller: &mut Poll) -> Result<bool> {
        Ok(true)
    }
}

fn would_block<T>(result: Result<T>) -> Result<Option<T>> {
    match result {
        Ok(v) => Ok(Some(v)),
        Err(e) if e.io.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
        Err(e) => Err(e),
    }
}
