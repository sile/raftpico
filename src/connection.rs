use std::{collections::HashSet, net::SocketAddr};

use jsonlrpc::{
    ErrorCode, ErrorObject, JsonRpcVersion, JsonlStream, RequestId, RequestObject, ResponseObject,
};
use mio::{net::TcpStream, Interest, Token};

use crate::{
    io::would_block,
    request::{
        is_known_external_method, IncomingMessage, InternalIncomingMessage, OutgoingMessage,
        Request,
    },
    Result,
};

#[derive(Debug)]
pub struct Connection {
    pub addr: SocketAddr,
    pub token: Token,
    pub stream: JsonlStream<TcpStream>,
    pub connected: bool,
    pub kind: ConnectionKind,
    pub next_request_id: i64, // TODO: remove
    pub ongoing_requests: HashSet<RequestId>,

    // TODO: current_interest / next_interest
    pub interest: Option<Interest>,
}

impl Connection {
    pub fn new_connected(addr: SocketAddr, token: Token, stream: TcpStream) -> Self {
        Self {
            addr,
            token,
            stream: JsonlStream::new(stream),
            connected: true,
            kind: ConnectionKind::Undefined,
            next_request_id: 0,
            ongoing_requests: HashSet::new(),
            interest: None,
        }
    }

    pub fn connect(addr: SocketAddr, token: Token) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        Ok(Self {
            addr,
            token,
            stream: JsonlStream::new(stream),
            connected: false,
            kind: ConnectionKind::Internal,
            next_request_id: 0,
            ongoing_requests: HashSet::new(),
            interest: None,
        })
    }

    pub fn next_request_id(&mut self) -> RequestId {
        let id = self.next_request_id;
        self.next_request_id += 1;
        RequestId::Number(id)
    }

    pub fn stream(&self) -> &TcpStream {
        self.stream.inner()
    }

    pub fn stream_mut(&mut self) -> &mut TcpStream {
        self.stream.inner_mut()
    }

    pub fn is_writing(&self) -> bool {
        !self.stream.write_buf().is_empty()
    }

    pub fn pending_write_size(&self) -> usize {
        self.stream.write_buf().len()
    }

    pub fn poll_recv(&mut self) -> Result<Option<IncomingMessage>> {
        match self.kind {
            ConnectionKind::Undefined => would_block(self.recv_undefined_message()),
            ConnectionKind::External => would_block(
                self.recv_external_message()
                    .map(IncomingMessage::ExternalRequest),
            ),
            ConnectionKind::Internal => {
                would_block(self.recv_internal_message().map(IncomingMessage::Internal))
            }
        }
    }

    fn recv_internal_message(&mut self) -> Result<InternalIncomingMessage> {
        // TODO: note about difference with the external message handling
        let msg = self.stream.read_object()?;
        Ok(msg)
    }

    fn recv_external_message(&mut self) -> Result<Request> {
        // TODO: refactor code
        // TODO: consider batch request
        match self.stream.read_object::<Request>() {
            Err(e) if e.is_io() => {
                return Err(e.into());
            }
            Err(_) => {
                let value = match self.stream.read_object::<serde_json::Value>() {
                    Err(e) => {
                        self.send_error_response_from_err("Invalid JSON value", &e)?;
                        return self.recv_external_message();
                    }
                    Ok(value) => value,
                };
                match serde_json::from_value::<RequestObject>(value) {
                    Err(e) => {
                        self.send_error_response_from_err("Invalid JSON-RPC 2.0 request", &e)?;
                    }
                    Ok(req) if req.id.is_some() => {
                        self.send_error_response_from_request(req)?;
                    }
                    Ok(_) => {
                        // Do nothing as it is a notification.
                    }
                }
                return self.recv_external_message();
            }
            Ok(req) => {
                if let Some(e) = req.validate() {
                    self.send_error_response(&req, e)?;
                    return self.recv_external_message();
                }
                Ok(req)
            }
        }
    }

    fn recv_undefined_message(&mut self) -> Result<IncomingMessage> {
        // TODO: refactor code
        match self.stream.read_object::<IncomingMessage>() {
            Err(e) if e.is_io() => {
                return Err(e.into());
            }
            Err(_) => {
                let value = match self.stream.read_object::<serde_json::Value>() {
                    Err(e) => {
                        self.send_error_response_from_err("Invalid JSON value", &e)?;
                        return self.recv_undefined_message();
                    }
                    Ok(value) => value,
                };
                match serde_json::from_value::<RequestObject>(value) {
                    Err(e) => {
                        self.send_error_response_from_err("Invalid JSON-RPC 2.0 request", &e)?;
                    }
                    Ok(req) if req.id.is_some() => {
                        self.send_error_response_from_request(req)?;
                    }
                    Ok(_) => {
                        // Do nothing as it is a notification.
                    }
                }
                return self.recv_undefined_message();
            }
            Ok(m) => {
                match &m {
                    IncomingMessage::ExternalRequest(req) => {
                        self.kind = ConnectionKind::External;
                        if let Some(e) = req.validate() {
                            self.send_error_response(req, e)?;
                            return self.recv_undefined_message();
                        }
                    }
                    IncomingMessage::Internal(_) => {
                        self.kind = ConnectionKind::Internal;
                    }
                }
                Ok(m)
            }
        }
    }

    fn send_error_response(&mut self, req: &Request, error: ErrorObject) -> Result<()> {
        let response = ResponseObject::Err {
            jsonrpc: JsonRpcVersion::V2,
            error,
            id: Some(req.id().clone()),
        };
        self.send(&response)
    }

    fn send_error_response_from_request(&mut self, req: RequestObject) -> Result<()> {
        let (code, msg) = if is_known_external_method(&req.method) {
            (ErrorCode::METHOD_NOT_FOUND, "Method not found")
        } else {
            (ErrorCode::INVALID_PARAMS, "Invalid parameters")
        };

        let response = ResponseObject::Err {
            jsonrpc: JsonRpcVersion::V2,
            error: ErrorObject {
                code,
                message: msg.to_owned(),
                data: None,
            },
            id: req.id,
        };
        self.send(&response)
    }

    fn send_error_response_from_err(&mut self, msg: &str, e: &serde_json::Error) -> Result<()> {
        let response = ResponseObject::Err {
            jsonrpc: JsonRpcVersion::V2,
            error: ErrorObject {
                code: ErrorCode::guess(e),
                message: msg.to_owned(),
                data: Some(serde_json::Value::String(e.to_string())),
            },
            id: None,
        };
        self.send(&response)
    }

    pub fn send<T: OutgoingMessage>(&mut self, msg: &T) -> Result<()> {
        let start_writing = !self.is_writing();
        match self.stream.write_object(msg) {
            Err(_e) if !self.connected => {
                // TODO: self.stream.write_to_buf()
                Ok(())
            }
            Err(e) if e.io_error_kind() == Some(std::io::ErrorKind::WouldBlock) => {
                if start_writing {
                    self.interest = Some(Interest::READABLE | Interest::WRITABLE);
                }
                Ok(())
            }
            Err(e) => Err(e.into()),
            Ok(_) => Ok(()),
        }
    }

    // TODO: try_send()

    pub fn poll_connect(&mut self) -> Result<bool> {
        if self.connected {
            return Ok(true);
        }

        // See: https://docs.rs/mio/1.0.2/mio/net/struct.TcpStream.html#method.connect
        self.stream().take_error()?;
        match self.stream().peer_addr() {
            Err(e) if e.kind() == std::io::ErrorKind::NotConnected => Ok(false),
            Err(e) => Err(e.into()),
            Ok(_) => {
                self.connected = true;
                Ok(true)
            }
        }
    }

    pub fn poll_send(&mut self) -> Result<()> {
        if !self.connected || !self.is_writing() {
            return Ok(());
        }

        match self.stream.flush() {
            Err(e) if e.io_error_kind() == Some(std::io::ErrorKind::WouldBlock) => {}
            Err(e) => {
                return Err(e.into());
            }
            Ok(_) => {
                self.interest = Some(Interest::READABLE);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectionKind {
    #[default]
    Undefined,
    External,
    Internal,
}
