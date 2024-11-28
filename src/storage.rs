use std::{fs::File, path::Path};

use jsonlrpc::JsonlStream;
use serde::{Deserialize, Serialize};

use crate::{
    rpc::{InstallSnapshotParams, LogEntries},
    types::{NodeId, Term},
};

/// Storage that stores the state of a server and local log entries into one .jsonl file.
#[derive(Debug)]
pub struct FileStorage {
    file: JsonlStream<File>,
    sync_data: bool,
}

impl FileStorage {
    /// Makes a new [`FileStorage`] instance with the specified .jsonl file path.
    ///
    /// If the file is not empty, its contents are loaded when the server starts.
    pub fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        Ok(Self {
            file: JsonlStream::new(file),
            sync_data: true,
        })
    }

    /// Disables the call to [`std::fs::File::sync_data()`] that is made after each file operation by default.
    ///
    /// Utilizing this method can enhance server performance, albeit at the risk of compromising consistency guarantees.
    pub fn disable_sync_data(&mut self) {
        self.sync_data = false;
    }

    pub(crate) fn save_current_term(&mut self, term: Term) -> std::io::Result<()> {
        self.file.write_value(&Record::Term(term))?;
        self.maybe_sync_data()?;
        Ok(())
    }

    pub(crate) fn save_voted_for(&mut self, voted_for: Option<NodeId>) -> std::io::Result<()> {
        self.file.write_value(&Record::VotedFor(voted_for))?;
        self.maybe_sync_data()?;
        Ok(())
    }

    pub(crate) fn append_entries(
        &mut self,
        raft_log_entries: &raftbare::LogEntries,
        commands: &crate::server::Commands,
    ) -> std::io::Result<()> {
        let entries = Record::LogEntries(
            LogEntries::from_raftbare(raft_log_entries, commands)
                .ok_or(std::io::ErrorKind::InvalidInput)?,
        );
        self.file.write_value(&entries)?;
        self.maybe_sync_data()?;
        Ok(())
    }

    pub(crate) fn install_snapshot(
        &mut self,
        snapshot: InstallSnapshotParams,
    ) -> std::io::Result<()> {
        // TODO: temorary file and move (and writing the temporary file on a worker thread)
        self.file.inner().set_len(0)?;
        self.file.write_value(&Record::Snapshot(snapshot))?;
        self.maybe_sync_data()?;
        Ok(())
    }

    fn maybe_sync_data(&self) -> std::io::Result<()> {
        if self.sync_data {
            self.file.inner().sync_data()?;
        }
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

#[derive(Debug, Serialize, Deserialize)]
enum Record {
    Term(Term),
    VotedFor(Option<NodeId>),
    LogEntries(LogEntries),
    Snapshot(InstallSnapshotParams),
}
