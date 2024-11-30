use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::SocketAddr,
    time::Duration,
};

use jsonlrpc::{ErrorObject, JsonRpcVersion, RequestId, ResponseObject};
use jsonlrpc_mio::{ClientId, RpcClient, RpcServer};
use mio::{Events, Poll};
use serde::Serialize;

use crate::{
    messages::{Caller, ErrorKind},
    types::{NodeId, Token},
    Request,
};

const EVENTS_CAPACITY: usize = 1024;
const SEND_QUEUE_LIMIT: usize = 10 * 1024 * 1024; // 10 MB

#[derive(Debug)]
pub(crate) struct MessageBroker {
    poller: Poll,
    events: Events,
    rpc_server: RpcServer<Request>,
    rpc_clients: HashMap<Token, RpcClient>,
    id_to_token: HashMap<NodeId, Token>,
    responses: VecDeque<ResponseObject>,
}

impl MessageBroker {
    pub fn start(listen_addr: SocketAddr) -> std::io::Result<Self> {
        let mut poller = Poll::new()?;
        let events = Events::with_capacity(EVENTS_CAPACITY);
        let rpc_server = RpcServer::start(
            &mut poller,
            listen_addr,
            Token::SERVER_MIN.into(),
            Token::SERVER_MAX.into(),
        )?;
        Ok(Self {
            poller,
            events,
            rpc_server,
            rpc_clients: HashMap::new(),
            id_to_token: HashMap::new(),
            responses: VecDeque::new(),
        })
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.rpc_server.listen_addr()
    }

    pub fn update_peers<I>(&mut self, peers: I)
    where
        I: IntoIterator<Item = (NodeId, Token, SocketAddr)>,
    {
        self.id_to_token.clear();

        let mut tokens = HashSet::new();
        for (node_id, token, addr) in peers {
            self.id_to_token.insert(node_id, token);
            tokens.insert(token);
            self.rpc_clients
                .entry(token)
                .or_insert_with(|| RpcClient::new(token.into(), addr));
        }

        self.rpc_clients.retain(|token, _| tokens.contains(token));
    }

    pub fn send_to<T: Serialize>(&mut self, node_id: NodeId, message: &T) -> std::io::Result<()> {
        if let Some(client) = self
            .id_to_token
            .get(&node_id)
            .and_then(|token| self.rpc_clients.get_mut(token))
        {
            let _ = client.send(&mut self.poller, message);
        }
        Ok(())
    }

    pub fn reply_output(
        &mut self,
        caller: Caller,
        output: Option<Result<serde_json::Value, ErrorObject>>,
    ) -> std::io::Result<()> {
        let output = output.unwrap_or_else(|| Err(ErrorKind::NoMachineOutput.object()));
        match output {
            Err(e) => self.reply_error(caller, e),
            Ok(value) => self.reply_ok(caller, value),
        }
    }

    pub fn reply_ok<T: Serialize>(&mut self, caller: Caller, value: T) -> std::io::Result<()> {
        #[derive(Serialize)]
        struct Response<T> {
            jsonrpc: JsonRpcVersion,
            id: RequestId,
            result: T,
        }
        let response = Response {
            jsonrpc: JsonRpcVersion::V2,
            id: caller.request_id,
            result: value,
        };
        self.rpc_server
            .reply(&mut self.poller, caller.client_id, &response)?;
        Ok(())
    }

    pub fn reply_error(&mut self, caller: Caller, error: ErrorObject) -> std::io::Result<()> {
        let response = ResponseObject::Err {
            jsonrpc: JsonRpcVersion::V2,
            error,
            id: Some(caller.request_id),
        };
        self.rpc_server
            .reply(&mut self.poller, caller.client_id, &response)?;
        Ok(())
    }

    pub fn broadcast<T: Serialize>(&mut self, message: &T) -> std::io::Result<()> {
        let raw = serde_json::value::to_raw_value(message)?;
        for client in self.rpc_clients.values_mut() {
            if client.queued_bytes_len() > SEND_QUEUE_LIMIT {
                continue;
            }
            let _ = client.send(&mut self.poller, &raw);
        }
        Ok(())
    }

    pub fn poll(&mut self, timeout: Duration) -> std::io::Result<bool> {
        self.poller.poll(&mut self.events, Some(timeout))?;
        if self.events.is_empty() {
            return Ok(false);
        }

        for event in self.events.iter() {
            if let Some(client) = self.rpc_clients.get_mut(&event.token().into()) {
                let _ = client.handle_event(&mut self.poller, event);
                while let Some(response) = client.try_recv() {
                    self.responses.push_back(response);
                }
            } else {
                self.rpc_server.handle_event(&mut self.poller, event)?;
            }
        }

        Ok(true)
    }

    pub fn try_recv_request(&mut self) -> Option<(ClientId, Request)> {
        self.rpc_server.try_recv()
    }

    pub fn try_recv_response(&mut self) -> Option<ResponseObject> {
        self.responses.pop_front()
    }
}
