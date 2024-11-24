use std::{collections::BTreeMap, net::SocketAddr};

use raftbare::NodeId;
use serde::{Deserialize, Serialize};

use crate::{
    command::Command,
    constants::{CLIENT_TOKEN_MAX, CLIENT_TOKEN_MIN, SEED_NODE_ID},
    message::CreateClusterOutput,
    server::ClusterSettings,
    Context, ErrorKind, Machine,
};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Machines<M> {
    pub system: SystemMachine,
    pub user: M,
}

impl<M: Machine> Machine for Machines<M> {
    type Input = Command;

    fn apply(&mut self, ctx: &mut Context, input: &Self::Input) {
        match input {
            Command::CreateCluster { .. }
            | Command::AddServer { .. }
            | Command::RemoveServer { .. } => {
                self.system.apply(ctx, input);
            }
            Command::ApplyCommand { input, .. } => {
                let input = serde_json::from_value(input.clone()).expect("TODO: error response");
                self.user.apply(ctx, &input)
            }
            Command::TakeSnapshot { .. } | Command::ApplyQuery => {
                // TODO: unreachable!();
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Member {
    pub addr: SocketAddr,
    pub token: usize, // TODO: Token
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SystemMachine {
    pub(crate) settings: ClusterSettings,
    pub(crate) members: BTreeMap<u64, Member>, // TODO: key type
    next_token: usize,                         // TOOD: token
}

impl Default for SystemMachine {
    fn default() -> Self {
        Self {
            settings: ClusterSettings::default(),
            members: BTreeMap::new(),
            next_token: CLIENT_TOKEN_MIN.0,
        }
    }
}

impl SystemMachine {
    fn apply_create_cluster_command(
        &mut self,
        ctx: &mut Context,
        seed_server_addr: SocketAddr,
        settings: &ClusterSettings,
    ) {
        self.settings = settings.clone();
        self.members.insert(
            SEED_NODE_ID.get(),
            Member {
                addr: seed_server_addr,
                token: self.next_token,
            },
        );
        self.next_token += 1; // TODO: max handling
        ctx.output(&CreateClusterOutput {
            members: self.members.values().cloned().collect(),
        });
    }

    fn apply_add_server_command(&mut self, ctx: &mut Context, server_addr: SocketAddr) {
        if self.members.values().any(|m| m.addr == server_addr) {
            ctx.error(ErrorKind::ServerAlreadyAdded.object());
            return;
        }

        let node_id = NodeId::new(ctx.commit_index.get());
        let token = self.next_token;
        self.next_token += 1; // TODO: max handling
        assert!(self.next_token <= CLIENT_TOKEN_MAX.0); // TODO

        self.members.insert(
            node_id.get(),
            Member {
                addr: server_addr,
                token,
            },
        );
        ctx.output(&CreateClusterOutput {
            members: self.members.values().cloned().collect(),
        });
    }

    fn apply_remove_server_command(&mut self, ctx: &mut Context, server_addr: SocketAddr) {
        let Some((&node_id, _member)) = self.members.iter().find(|(_, m)| m.addr == server_addr)
        else {
            ctx.error(ErrorKind::NotClusterMember.object());
            return;
        };

        let node_id = NodeId::new(node_id);
        self.members.remove(&node_id.get());
        ctx.output(&CreateClusterOutput {
            members: self.members.values().cloned().collect(),
        });

        // TODO: reset self.node for removed server
    }
}

impl Machine for SystemMachine {
    type Input = Command;

    fn apply(&mut self, ctx: &mut Context, input: &Self::Input) {
        match input {
            Command::CreateCluster {
                seed_server_addr,
                settings,
                ..
            } => self.apply_create_cluster_command(ctx, *seed_server_addr, settings),
            Command::AddServer { server_addr, .. } => {
                self.apply_add_server_command(ctx, *server_addr)
            }
            Command::RemoveServer { server_addr, .. } => {
                self.apply_remove_server_command(ctx, *server_addr)
            }
            Command::ApplyCommand { .. } => {
                unreachable!();
            }
            Command::TakeSnapshot { .. } => {
                unreachable!();
            }
            Command::ApplyQuery => {
                unreachable!();
            }
        }
    }
}
