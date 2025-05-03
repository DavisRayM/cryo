//! Write-Ahead Logging (WAL) subsystem.
//!
//! This module implements Cryo's write-ahead logging (WAL) mechanism, which provides durability
//! and crash recovery guarantees for all mutating operations (e.g., inserts, deletes).
//!
//! The WAL ensures that all changes are persisted to disk before they are applied to the in-memory
//! state or storage engine. In the event of a crash, the log can be replayed to restore the system
//! to a consistent state.
//!
//! # Overview
//!
//! The WAL is append-only and organized into sequential log entries. Each entry records a single
//! logical operation (e.g., row insertion, row update) in a binary format. These entries are flushed
//! to disk before the corresponding operations are acknowledged to the client.
//!
//! During startup or recovery, the log is read and replayed to reconstruct the most recent consistent
//! database state.
//!
//! # Key Components
//!
//! - [`LogEntry`]: Enum representing a single WAL record (e.g., `Insert`, `Delete`, `Checkpoint`).
//!
//! # See Also
//!
//! - [`storage`](crate::storage): Applies changes described in WAL entries.
//! - [`protocol`](crate::protocol): Client commands that trigger WAL writes.

use std::{
    collections::VecDeque,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Seek, Write},
    path::PathBuf,
};

use bincode::{
    Decode, Encode,
    config::{BigEndian, Configuration, Fixint},
    decode_from_reader, encode_into_std_write,
};
use log::{info, trace};

use super::{LoggerError, Row, StorageError, btree::BTree, pager::Pager};

#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
pub enum LogEntry {
    Insert(Vec<u8>),
    Delete(Vec<u8>),
    Update(Vec<u8>),
    GlobalCheckpoint,
}

pub struct Logger {
    config: Configuration<BigEndian, Fixint>,
    entries: VecDeque<LogEntry>,
    pager: Pager,
    writer: BufWriter<File>,
}

pub const COMPACTION_THRESHOLD: usize = 100;

impl Logger {
    pub fn open(path: PathBuf, pager: Pager) -> Result<Self, StorageError> {
        let log = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)
            .map_err(|e| StorageError::Logger {
                cause: LoggerError::Io(e),
            })?;
        let config = bincode::config::standard()
            .with_big_endian()
            .with_fixed_int_encoding();

        let mut reader = BufReader::new(log);

        let mut entries = VecDeque::new();
        loop {
            let entry: Result<LogEntry, _> = decode_from_reader(&mut reader, config);
            match entry {
                Ok(LogEntry::GlobalCheckpoint) => entries.clear(),
                Ok(log) => entries.push_back(log),
                Err(_) => break,
            }
        }
        let pos = reader.stream_position().map_err(|e| StorageError::Logger {
            cause: LoggerError::Io(e),
        })?;
        let mut inner = reader.into_inner();
        inner
            .seek(std::io::SeekFrom::Start(pos))
            .map_err(|e| StorageError::Logger {
                cause: LoggerError::Io(e),
            })?;

        Ok(Self {
            config,
            entries,
            pager,
            writer: BufWriter::new(inner),
        })
    }

    pub fn pager(&mut self) -> &mut Pager {
        &mut self.pager
    }

    pub fn log(&mut self, entry: LogEntry) -> Result<(), StorageError> {
        info!("entry logged: {entry:?}");
        encode_into_std_write(entry.clone(), &mut self.writer, self.config).map_err(|e| {
            StorageError::Logger {
                cause: LoggerError::Serialize(e),
            }
        })?;

        self.writer.flush().map_err(|e| StorageError::Logger {
            cause: LoggerError::Io(e),
        })?;

        if entry == LogEntry::GlobalCheckpoint {
            self.compact(true)?;
        } else {
            self.entries.push_back(entry);
            if self.entries.len() >= COMPACTION_THRESHOLD {
                return self.log(LogEntry::GlobalCheckpoint);
            }
        }
        Ok(())
    }

    fn compact(&mut self, clear: bool) -> Result<(), StorageError> {
        info!("flushing {} entries", self.entries.len());
        trace!("entries: {:?}", self.entries);

        let mut entries = if clear {
            let entries = self.entries.clone();
            self.entries.clear();
            entries
        } else {
            self.entries.clone()
        };

        while let Some(entry) = entries.pop_front() {
            let mut btree = BTree::new(&mut self.pager);

            match entry {
                LogEntry::Insert(items) => {
                    let row: Row = items.as_slice().try_into()?;
                    btree.insert(row)?;
                }
                LogEntry::Delete(items) => {
                    let row: Row = items.as_slice().try_into()?;
                    btree.delete(row)?;
                }
                LogEntry::Update(items) => {
                    let row: Row = items.as_slice().try_into()?;
                    btree.update(row)?;
                }
                LogEntry::GlobalCheckpoint => {}
            }
        }

        self.pager.flush();
        self.writer.flush().map_err(|e| StorageError::Logger {
            cause: LoggerError::Io(e),
        })?;
        let f = self.writer.get_mut();
        f.set_len(0).map_err(|e| StorageError::Logger {
            cause: LoggerError::Io(e),
        })?;
        f.seek(std::io::SeekFrom::Start(0))
            .map_err(|e| StorageError::Logger {
                cause: LoggerError::Io(e),
            })?;
        info!("entries written to disk");

        Ok(())
    }

    #[allow(dead_code)]
    fn list_entries(&mut self) -> Result<Vec<LogEntry>, StorageError> {
        Ok(self.entries.iter().cloned().collect::<Vec<LogEntry>>())
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use crate::storage::row::RowType;

    use super::*;

    #[test]
    fn log_writes_entries() {
        let temp = TempDir::new("log").unwrap();
        let pager = Pager::open(temp.path().join("cryo.db")).unwrap();
        let mut logger = Logger::open(temp.path().join("wal.log"), pager).unwrap();

        logger
            .log(LogEntry::Insert(
                Row::new(0, RowType::Leaf).as_bytes().to_vec(),
            ))
            .unwrap();
        logger
            .log(LogEntry::Insert(
                Row::new(1, RowType::Leaf).as_bytes().to_vec(),
            ))
            .unwrap();
        logger
            .log(LogEntry::Update(
                Row::new(1, RowType::Leaf).as_bytes().to_vec(),
            ))
            .unwrap();
        assert_eq!(
            logger.list_entries().unwrap(),
            vec![
                LogEntry::Insert(Row::new(0, RowType::Leaf).as_bytes().to_vec()),
                LogEntry::Insert(Row::new(1, RowType::Leaf).as_bytes().to_vec()),
                LogEntry::Update(Row::new(1, RowType::Leaf).as_bytes().to_vec())
            ]
        );
    }

    #[test]
    fn only_returns_unflushed_entries() {
        let temp = TempDir::new("log").unwrap();
        let pager = Pager::open(temp.path().join("cryo.db")).unwrap();
        let mut logger = Logger::open(temp.path().join("wal.log"), pager).unwrap();

        logger
            .log(LogEntry::Insert(
                Row::new(0, RowType::Leaf).as_bytes().to_vec(),
            ))
            .unwrap();
        logger
            .log(LogEntry::Update(
                Row::new(0, RowType::Leaf).as_bytes().to_vec(),
            ))
            .unwrap();
        logger.log(LogEntry::GlobalCheckpoint).unwrap();
        logger
            .log(LogEntry::Insert(
                Row::new(1, RowType::Leaf).as_bytes().to_vec(),
            ))
            .unwrap();

        assert_eq!(
            logger.list_entries().unwrap(),
            vec![LogEntry::Insert(
                Row::new(1, RowType::Leaf).as_bytes().to_vec()
            )]
        );
    }

    #[test]
    fn logger_compacts() {
        let temp = TempDir::new("log").unwrap();
        let pager = Pager::open(temp.path().join("cryo.db")).unwrap();
        let mut logger = Logger::open(temp.path().join("wal.log"), pager).unwrap();

        logger
            .log(LogEntry::Insert(
                Row::new(0, RowType::Leaf).as_bytes().to_vec(),
            ))
            .unwrap();
        logger
            .log(LogEntry::Update(
                Row::new(0, RowType::Leaf).as_bytes().to_vec(),
            ))
            .unwrap();
        logger
            .log(LogEntry::Insert(
                Row::new(1, RowType::Leaf).as_bytes().to_vec(),
            ))
            .unwrap();
        logger
            .log(LogEntry::Update(
                Row::new(1, RowType::Leaf).as_bytes().to_vec(),
            ))
            .unwrap();
        drop(logger);

        let f = File::open(temp.path().join("wal.log")).unwrap();
        let len = f.metadata().unwrap().len();

        let pager = Pager::open(temp.path().join("cryo.db")).unwrap();
        let mut logger = Logger::open(temp.path().join("wal.log"), pager).unwrap();
        logger.log(LogEntry::GlobalCheckpoint).unwrap();
        drop(logger);

        let f = File::open(temp.path().join("wal.log")).unwrap();
        let new_len = f.metadata().unwrap().len();
        assert!(new_len < len);
    }
}
