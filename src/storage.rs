use std::{fs::File, path::Path};

use jsonlrpc::JsonlStream;

use crate::{request::SnapshotParams, Machine, Result};

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

    pub fn load_snapshot<M: Machine>(&mut self) -> Result<Option<SnapshotParams<M>>> {
        let file_size = self.file.inner().metadata()?.len();
        if file_size == 0 {
            return Ok(None);
        }
        Ok(Some(self.file.read_object()?))
    }

    // TODO: load append entries
    // TODO: append entries

    pub fn install_snapshot<M: Machine>(&mut self, snapshot: SnapshotParams<M>) -> Result<()> {
        // TODO: temorary file and move
        self.file.inner().set_len(0)?;
        self.file.write_object(&snapshot)?;
        Ok(())
    }
}

// #[derive(Debug, Serialize, Deserialize)]
// pub struct Record {
//     pub index: u64,
//     pub command: Command,
// }

// fn maybe_eof(result: serde_json::Result<Record>) -> serde_json::Result<Option<Record>> {
//     match result {
//         Ok(v) => Ok(Some(v)),
//         Err(e) if e.io_error_kind() == Some(ErrorKind::UnexpectedEof) => Ok(None),
//         Err(e) => Err(e),
//     }
// }
