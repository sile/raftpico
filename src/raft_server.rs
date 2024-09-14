use crate::Machine;

#[derive(Debug)]
pub struct RaftServer<M> {
    machine: M,
}

impl<M: Machine> RaftServer<M> {
    pub fn machine(&self) -> &M {
        &self.machine
    }
}
