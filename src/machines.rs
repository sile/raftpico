//! Predefined replicated state machines.
use std::{collections::BTreeMap, net::SocketAddr, time::Duration};

use raftbare::Role;
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::{
    command::Command,
    messages::{AddServerResult, CreateClusterResult, ErrorReason, RemoveServerResult},
    types::{NodeId, Token},
    ApplyContext, Machine,
};

/// System and user state machines.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Machines<M> {
    /// System state machine.
    pub system: SystemMachine,

    /// User state machine.
    pub user: M,
}

impl<M: Machine> Machine for Machines<M> {
    type Input = Command;

    fn apply(&mut self, ctx: &mut ApplyContext, input: Self::Input) {
        match input {
            Command::CreateCluster { .. }
            | Command::AddServer { .. }
            | Command::RemoveServer { .. } => {
                self.system.apply(ctx, input);
            }
            Command::Apply { input } => match serde_json::from_value(input) {
                Ok(input) => {
                    self.user.apply(ctx, input);
                }
                Err(e) => {
                    ctx.error(ErrorReason::InvalidMachineInput { reason: e });
                }
            },
            Command::Query
            | Command::TakeSnapshot { .. }
            | Command::StartTerm { .. }
            | Command::UpdateClusterConfig { .. } => {
                unreachable!();
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Member {
    pub addr: SocketAddr,
    pub token: Token,
}

/// Replicated state machine responsible for handling system commands.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SystemMachine {
    pub(crate) min_election_timeout: Duration,
    pub(crate) max_election_timeout: Duration,
    pub(crate) members: BTreeMap<NodeId, Member>,
    next_token: Token,
}

impl SystemMachine {
    fn apply_create_cluster_command(
        &mut self,
        ctx: &mut ApplyContext,
        seed_addr: SocketAddr,
        min_election_timeout: Duration,
        max_election_timeout: Duration,
    ) {
        self.min_election_timeout = min_election_timeout;
        self.max_election_timeout = max_election_timeout;
        self.members.insert(
            NodeId::SEED,
            Member {
                addr: seed_addr,
                token: self.next_token.next_client_token(),
            },
        );
        ctx.output(&CreateClusterResult {
            members: self.members.values().map(|m| m.addr).collect(),
        });
    }

    fn apply_add_server_command(&mut self, ctx: &mut ApplyContext, addr: SocketAddr) {
        if self.members.values().any(|m| m.addr == addr) {
            ctx.error(ErrorReason::ServerAlreadyAdded);
            return;
        }

        let node_id = NodeId::from(u64::from(ctx.commit_index));
        let token = self.next_token.next_client_token();
        self.members.insert(node_id, Member { addr, token });
        ctx.output(&AddServerResult {
            members: self.members.values().map(|m| m.addr).collect(),
        });
    }

    fn apply_remove_server_command(&mut self, ctx: &mut ApplyContext, addr: SocketAddr) {
        let Some((&node_id, _member)) = self.members.iter().find(|(_, m)| m.addr == addr) else {
            ctx.error(ErrorReason::NotClusterMember);
            return;
        };

        self.members.remove(&node_id);
        ctx.output(&RemoveServerResult {
            members: self.members.values().map(|m| m.addr).collect(),
        });

        // TODO: reset self.node for removed server
    }

    pub(crate) fn is_known_node(&self, node_id: NodeId) -> bool {
        self.members.contains_key(&node_id)
    }

    pub(crate) fn get_node_id_by_addr(&self, addr: SocketAddr) -> Option<NodeId> {
        self.members
            .iter()
            .find(|(_, m)| m.addr == addr)
            .map(|(&id, _)| id)
    }

    pub(crate) fn gen_election_timeout(&self, role: Role) -> Duration {
        let min = self.min_election_timeout;
        let max = self.max_election_timeout.max(min);
        match role {
            Role::Follower => max,
            Role::Candidate => rand::thread_rng().gen_range(min..=max),
            Role::Leader => min,
        }
    }
}

impl Machine for SystemMachine {
    type Input = Command;

    fn apply(&mut self, ctx: &mut ApplyContext, input: Self::Input) {
        match input {
            Command::CreateCluster {
                seed_addr,
                min_election_timeout,
                max_election_timeout,
                ..
            } => self.apply_create_cluster_command(
                ctx,
                seed_addr,
                min_election_timeout,
                max_election_timeout,
            ),
            Command::AddServer { addr, .. } => self.apply_add_server_command(ctx, addr),
            Command::RemoveServer { addr, .. } => self.apply_remove_server_command(ctx, addr),
            Command::Apply { .. }
            | Command::TakeSnapshot { .. }
            | Command::Query
            | Command::StartTerm { .. }
            | Command::UpdateClusterConfig { .. } => {
                unreachable!();
            }
        }
    }
}
