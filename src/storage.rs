use std::{fs::File, io::Seek, path::Path};

use jsonlrpc::JsonlStream;
use raftbare::{ClusterConfig, LogIndex, LogPosition, NodeId, Term};
use serde::{Deserialize, Serialize};

use crate::{
    request::{LogEntry, SnapshotParams},
    server::Commands,
    Machine, Result,
};

#[derive(Debug)]
pub struct FileStorage {
    file: JsonlStream<File>,
}

impl FileStorage {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        Ok(Self {
            file: JsonlStream::new(file),
        })
    }

    pub fn install_snapshot<M: Serialize>(&mut self, snapshot: SnapshotParams<M>) -> Result<()> {
        // TODO: temorary file and move
        self.file.inner().set_len(0)?;
        self.file
            .write_object(&Record::<LogEntries, _>::Snapshot(snapshot))?;
        Ok(())
    }

    pub fn append_entries(
        &mut self,
        raft_log_entries: &raftbare::LogEntries,
        commands: &Commands,
    ) -> Result<()> {
        let entries = Record::<_, SnapshotParams>::LogEntries(LogEntries::from_raftbare(
            raft_log_entries,
            commands,
        )?);
        self.file.write_object(&entries)?;
        Ok(())
    }

    pub fn save_node_id(&mut self, node_id: NodeId) -> Result<()> {
        self.file
            .write_object(&Record::<LogEntries, SnapshotParams>::NodeId(node_id.get()))?;
        Ok(())
    }

    pub fn save_current_term(&mut self, term: Term) -> Result<()> {
        self.file
            .write_object(&Record::<LogEntries, SnapshotParams>::Term(term.get()))?;
        Ok(())
    }

    pub fn save_voted_for(&mut self, voted_for: Option<NodeId>) -> Result<()> {
        self.file
            .write_object(&Record::<LogEntries, SnapshotParams>::VotedFor(
                voted_for.map(|n| n.get()),
            ))?;
        Ok(())
    }

    pub fn load_record<M: Machine>(
        &mut self,
        commands: &mut Commands,
    ) -> Result<Option<Record<raftbare::LogEntries, M>>> {
        if self.file.inner().metadata()?.len() == self.file.inner().stream_position()? {
            return Ok(None);
        }

        let record: Record<LogEntries, _> = self.file.read_object()?;
        match record {
            Record::NodeId(v) => Ok(Some(Record::NodeId(v))),
            Record::Term(v) => Ok(Some(Record::Term(v))),
            Record::VotedFor(v) => Ok(Some(Record::VotedFor(v))),
            Record::LogEntries(v) => Ok(Some(Record::LogEntries(v.to_raftbare(commands)))),
            Record::Snapshot(v) => Ok(Some(Record::Snapshot(v))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Record<T, M> {
    NodeId(u64),
    Term(u64),
    VotedFor(Option<u64>),
    LogEntries(T),
    Snapshot(SnapshotParams<M>),
}

// TODO:
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntries {
    pub prev_log_term: u64,
    pub prev_log_index: u64,
    pub entries: Vec<LogEntry>,
}

impl LogEntries {
    pub fn from_raftbare(entries: &raftbare::LogEntries, commands: &Commands) -> Result<Self> {
        Ok(Self {
            prev_log_term: entries.prev_position().term.get(),
            prev_log_index: entries.prev_position().index.get(),
            entries: entries
                .iter_with_positions()
                .map(|(position, entry)| match entry {
                    raftbare::LogEntry::Term(t) => LogEntry::Term(t.get()),
                    raftbare::LogEntry::ClusterConfig(c) => LogEntry::Config {
                        voters: c.voters.iter().map(|v| v.get()).collect(),
                        new_voters: c.new_voters.iter().map(|v| v.get()).collect(),
                    },
                    raftbare::LogEntry::Command => {
                        let command = commands.get(&position.index).expect("TODO: bug");
                        LogEntry::Command(command.clone())
                    }
                })
                .collect(),
        })
    }

    pub fn to_raftbare(self, commands: &mut Commands) -> raftbare::LogEntries {
        let term = Term::new(self.prev_log_term);
        let mut index = LogIndex::new(self.prev_log_index);
        let mut entries = raftbare::LogEntries::new(LogPosition { term, index });
        for entry in self.entries {
            index = LogIndex::new(index.get() + 1);
            match entry {
                LogEntry::Term(t) => entries.push(raftbare::LogEntry::Term(Term::new(t))),
                LogEntry::Config { voters, new_voters } => {
                    entries.push(raftbare::LogEntry::ClusterConfig(ClusterConfig {
                        voters: voters.into_iter().map(NodeId::new).collect(),
                        new_voters: new_voters.into_iter().map(NodeId::new).collect(),
                        ..Default::default()
                    }))
                }
                LogEntry::Command(command) => {
                    entries.push(raftbare::LogEntry::Command);
                    commands.insert(index, command);
                }
            }
        }
        entries
    }
}
