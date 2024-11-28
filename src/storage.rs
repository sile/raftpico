use std::{fs::File, path::Path};

use jsonlrpc::JsonlStream;
use serde::{Deserialize, Serialize};

use crate::{
    command::Command,
    rpc::InstallSnapshotParams,
    types::{LogPosition, NodeId, Term},
};

#[derive(Debug)]
pub struct FileStorage {
    file: JsonlStream<File>,
    force_fsync: bool,
}

impl FileStorage {
    // TODO: with_options
    pub fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        Ok(Self {
            file: JsonlStream::new(file),
            force_fsync: true,
        })
    }

    pub fn install_snapshot(&mut self, snapshot: InstallSnapshotParams) -> std::io::Result<()> {
        // TODO: temorary file and move (and writing the temporary file on a worker thread)
        self.file.inner().set_len(0)?;
        self.file.write_value(&Record::Snapshot(snapshot))?;
        self.maybe_fsync()?;
        Ok(())
    }

    fn maybe_fsync(&self) -> std::io::Result<()> {
        if self.force_fsync {
            self.file.inner().sync_data()?;
        }
        Ok(())
    }

    pub fn append_entries(
        &mut self,
        raft_log_entries: &raftbare::LogEntries,
        commands: &crate::server::Commands,
    ) -> std::io::Result<()> {
        let entries = Record::LogEntries(LogEntries::from_raftbare(raft_log_entries, commands)?);
        self.file.write_value(&entries)?;
        self.maybe_fsync()?;
        Ok(())
    }

    pub fn save_node_id(&mut self, node_id: NodeId) -> std::io::Result<()> {
        self.file.write_value(&Record::NodeId(node_id.into()))?;
        self.maybe_fsync()?;
        Ok(())
    }

    pub fn save_current_term(&mut self, term: Term) -> std::io::Result<()> {
        self.file.write_value(&Record::Term(term.into()))?;
        self.maybe_fsync()?;
        Ok(())
    }

    pub fn save_voted_for(&mut self, voted_for: Option<NodeId>) -> std::io::Result<()> {
        self.file
            .write_value(&Record::VotedFor(voted_for.map(|n| n.into())))?;
        self.maybe_fsync()?;
        Ok(())
    }

    // TODO:
    // pub fn load_record<M: Machine>(
    //     &mut self,
    //     commands: &mut Commands,
    // ) -> Result<Option<Record<raftbare::LogEntries, M>>> {
    //     if self.file.inner().metadata()?.len() == self.file.inner().stream_position()? {
    //         return Ok(None);
    //     }

    //     let record: Record<LogEntries, _> = self.file.read_value()?;
    //     match record {
    //         Record::NodeId(v) => Ok(Some(Record::NodeId(v))),
    //         Record::Term(v) => Ok(Some(Record::Term(v))),
    //         Record::VotedFor(v) => Ok(Some(Record::VotedFor(v))),
    //         Record::LogEntries(v) => Ok(Some(Record::LogEntries(v.to_raftbare(commands)))),
    //         Record::Snapshot(v) => Ok(Some(Record::Snapshot(v))),
    //     }
    // }
}

// TODO: remove parameter
#[derive(Debug, Serialize, Deserialize)]
enum Record {
    NodeId(u64),
    Term(u64),
    VotedFor(Option<u64>),
    LogEntries(LogEntries),
    Snapshot(InstallSnapshotParams),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LogEntries {
    prev_position: LogPosition,
    entries: Vec<Command>,
}

impl LogEntries {
    fn from_raftbare(
        entries: &raftbare::LogEntries,
        commands: &crate::server::Commands,
    ) -> std::io::Result<Self> {
        Ok(Self {
            prev_position: entries.prev_position().into(),
            entries: entries
                .iter_with_positions()
                .map(|(position, entry)| match entry {
                    raftbare::LogEntry::Term(t) => Ok(Command::StartTerm { term: t.into() }),
                    raftbare::LogEntry::ClusterConfig(c) => Ok(Command::UpdateClusterConfig {
                        voters: c.voters.iter().copied().map(NodeId::from).collect(),
                        new_voters: c.new_voters.iter().copied().map(NodeId::from).collect(),
                    }),
                    raftbare::LogEntry::Command => commands
                        .get(&position.index.into())
                        .cloned()
                        .ok_or(std::io::ErrorKind::InvalidInput),
                })
                .collect::<Result<_, _>>()?,
        })
    }
}
