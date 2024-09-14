use std::net::SocketAddr;

use mio::{net::TcpStream, Token};

#[derive(Debug)]
pub struct Connection {
    pub addr: SocketAddr,
    pub token: Token,
    pub stream: TcpStream,
    pub connected: bool,
}

impl Connection {
    pub fn new_connected(addr: SocketAddr, token: Token, stream: TcpStream) -> Connection {
        Self {
            addr,
            token,
            stream,
            connected: true,
        }
    }
}
