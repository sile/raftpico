use std::net::SocketAddr;

use jsonlrpc::{
    ErrorCode, ErrorObject, JsonRpcVersion, JsonlStream, RequestObject, ResponseObject,
};
use mio::{net::TcpStream, Token};

use crate::request::{is_known_external_method, IncomingMessage};

#[derive(Debug)]
pub struct Connection {
    pub addr: SocketAddr,
    pub token: Token,
    pub stream: JsonlStream<TcpStream>,
    pub connected: bool,
    pub kind: ConnectionKind,
}

impl Connection {
    pub fn new_connected(addr: SocketAddr, token: Token, stream: TcpStream) -> Connection {
        Self {
            addr,
            token,
            stream: JsonlStream::new(stream),
            connected: true,
            kind: ConnectionKind::Undefined,
        }
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

    pub fn recv_message(&mut self) -> std::io::Result<Option<IncomingMessage>> {
        match self.kind {
            ConnectionKind::Undefined => self.recv_undefined_message(),
            ConnectionKind::External => todo!(),
            ConnectionKind::Internal => todo!(),
        }
    }

    fn recv_undefined_message(&mut self) -> std::io::Result<Option<IncomingMessage>> {
        match self.stream.read_object::<IncomingMessage>() {
            Err(e) if e.is_io() => {
                return Err(e.into());
            }
            Err(_) => {
                let value = match self.stream.read_object::<serde_json::Value>() {
                    Err(e) => {
                        self.send_error_response_from_err("Not valid JSON value", &e);
                        return Ok(None);
                    }
                    Ok(value) => value,
                };
                match serde_json::from_value::<RequestObject>(value) {
                    Err(e) => {
                        self.send_error_response_from_err("Not valid JSON-RPC 2.0 request", &e);
                    }
                    Ok(req) if req.id.is_some() => {
                        self.send_error_response_from_request(req);
                    }
                    Ok(_) => {
                        // Do nothing as it is a notification.
                    }
                }
                return Ok(None);
            }
            Ok(_) => todo!(),
        }
    }

    fn send_error_response_from_request(&mut self, req: RequestObject) {
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
        let _ = self.stream.write_object(&response); // TODO: add doc
    }

    fn send_error_response_from_err(&mut self, msg: &str, e: &serde_json::Error) {
        let response = ResponseObject::Err {
            jsonrpc: JsonRpcVersion::V2,
            error: ErrorObject {
                code: ErrorCode::guess(e),
                message: msg.to_owned(),
                data: Some(serde_json::Value::String(e.to_string())),
            },
            id: None,
        };
        let _ = self.stream.write_object(&response); // TODO: add doc
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectionKind {
    #[default]
    Undefined,
    External,
    Internal,
}
