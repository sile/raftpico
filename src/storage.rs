use std::{
    fs::File,
    io::{Seek, SeekFrom},
    path::Path,
};

use jsonlrpc::JsonlStream;
use raftbare::Node;
use serde::{Deserialize, Serialize};

use crate::{
    machines::Machines,
    rpc::{InstallSnapshotParams, LogEntries},
    server::Commands,
    types::{NodeId, Term},
    Machine,
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
        entries: &raftbare::LogEntries,
        commands: &crate::server::Commands,
    ) -> std::io::Result<()> {
        let record = Record::LogEntries(
            LogEntries::from_raftbare(entries, commands).ok_or(std::io::ErrorKind::InvalidInput)?,
        );
        self.file.write_value(&record)?;
        self.maybe_sync_data()?;
        Ok(())
    }

    pub(crate) fn save_snapshot(&mut self, snapshot: InstallSnapshotParams) -> std::io::Result<()> {
        self.file.inner().set_len(0)?;
        self.file.inner().seek(SeekFrom::Start(0))?;

        // [NOTE]
        // There is a possibility that the storage data could be lost if the server process aborts
        // between the above `set_len()` call and the subsequent `write_value()` call.
        // In practice, I believe the likelihood of this happening is very low.
        // However, if it becomes an issue, a safer approach would be to write the snapshot to a
        // temporary file and then rename this temporary file to the storage file path.

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

    pub(crate) fn load<M: Machine>(
        &mut self,
        commands: &mut Commands,
    ) -> std::io::Result<Option<(Node, Machines<M>)>> {
        let mut machine = None;
        let mut node_id = raftbare::NodeId::from(NodeId::UNINIT);
        let mut term = raftbare::Term::new(0);
        let mut voted_for = None;
        let mut config = raftbare::ClusterConfig::default();
        let mut entries = raftbare::LogEntries::new(raftbare::LogPosition::ZERO);
        while let Some(record) = self.load_record()? {
            match record {
                Record::Term(v) => term = raftbare::Term::from(v),
                Record::VotedFor(v) => voted_for = v.map(raftbare::NodeId::from),
                Record::LogEntries(v) => {
                    let v = v.into_raftbare(commands);
                    let n = v.prev_position().index - entries.prev_position().index;
                    entries.truncate(n.get() as usize);
                    for e in v.iter() {
                        entries.push(e);
                    }
                }
                Record::Snapshot(snapshot) => {
                    node_id = snapshot.node_id.into();
                    entries = raftbare::LogEntries::new(snapshot.last_included_position.into());
                    config.voters = snapshot.voters.into_iter().map(From::from).collect();
                    config.new_voters = snapshot.new_voters.into_iter().map(From::from).collect();
                    machine = Some(serde_json::from_value(snapshot.machine)?);
                }
            }
        }
        Ok(machine.map(|m| {
            let log = raftbare::Log::new(config, entries);
            let node = Node::restart(node_id, term, voted_for, log);
            (node, m)
        }))
    }

    fn load_record(&mut self) -> std::io::Result<Option<Record>> {
        match self.file.read_value() {
            Ok(record) => Ok(Some(record)),
            Err(e)
                if e.io_error_kind() == Some(std::io::ErrorKind::UnexpectedEof)
                    && self.file.read_buf().is_empty() =>
            {
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Record {
    Term(Term),
    VotedFor(Option<NodeId>),
    LogEntries(LogEntries),
    Snapshot(InstallSnapshotParams),
}
