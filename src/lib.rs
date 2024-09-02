use raftbare::{Action, LogIndex, Node as BareNode, NodeId};

pub trait Machine: Sized {
    type Command: Command;

    fn apply(&mut self, command: Self::Command);

    fn encode(&self, buf: &mut Vec<u8>);
    fn decode(buf: &[u8]) -> Self;
}

pub trait Command: Sized {
    fn encode(&self, buf: &mut Vec<u8>);
    fn decode(buf: &[u8]) -> Self;
}

#[derive(Debug)]
pub struct RaftNode<M: Machine> {
    machine: M,
    bare_node: BareNode,
    command_log: Vec<(LogIndex, M::Command)>,
}

impl<M: Machine> RaftNode<M> {
    pub fn create_cluster(machine: M) -> Self {
        let node_id = NodeId::new(0);
        let bare_node = BareNode::start(node_id);
        let mut this = Self {
            machine,
            bare_node,
            command_log: Vec::new(),
        };

        let mut promise = this.bare_node.create_cluster(&[node_id]);
        assert!(!promise.is_rejected());

        // while promise.poll(&self.bare_node).is_pending() {
        //     while let Some(action) = self.bare_node.actions_mut().next() {
        //         self.handle_action(action);
        //     }
        // }

        todo!()
    }

    pub fn node_id(&self) -> NodeId {
        self.bare_node.id()
    }

    pub fn machine(&self) -> &M {
        &self.machine
    }

    pub fn handle_action(&mut self, action: Action) {
        todo!();
    }
}

// join / leave
// propose_command()
// local_query()
// consistent_query()
// run_one() or poll()

#[cfg(test)]
mod tests {
    use super::*;

    impl Machine for () {
        type Command = ();

        fn apply(&mut self, _command: Self::Command) {}

        fn encode(&self, _buf: &mut Vec<u8>) {}

        fn decode(_buf: &[u8]) -> Self {
            ()
        }
    }

    impl Command for () {
        fn encode(&self, _buf: &mut Vec<u8>) {}

        fn decode(_buf: &[u8]) -> Self {
            ()
        }
    }

    #[test]
    fn create_cluster() {
        let node = RaftNode::create_cluster(());
        assert_eq!(node.node_id().get(), 0);
    }
}
