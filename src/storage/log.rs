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
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Seek, SeekFrom, Write},
    path::PathBuf,
};

use bincode::{
    Decode, Encode,
    config::{BigEndian, Configuration, Fixint},
    decode_from_reader, encode_into_std_write,
};
use log::{debug, info, trace};

use super::{LoggerError, Row, StorageError, btree::BTree, pager::Pager};

/// Entry recorded in time for easy recovery; Any state changes
/// is tracked here and stored before the change is fully
/// written to the on-disk structure.
#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
pub enum LogEntry {
    Insert(Vec<u8>),
    Delete(Vec<u8>),
    Update(Vec<u8>),
    GlobalCheckpoint,
}

/// Tracks state changes before batch-processing permanent
/// on-disk writes.
pub struct Logger {
    config: Configuration<BigEndian, Fixint>,
    entries: Vec<(u64, usize)>,
    pager: Pager,
    writer: BufWriter<File>,
    path: PathBuf,
}

impl Logger {
    pub fn open(path: PathBuf, mut pager: Pager) -> Result<Self, StorageError> {
        let log = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| StorageError::Logger {
                cause: LoggerError::Io(e),
            })?;
        let config = bincode::config::standard()
            .with_big_endian()
            .with_fixed_int_encoding();
        let mut reader = BufReader::new(log);
        pager.commit(false);

        // Apply previous state.
        let mut btree = BTree::new(&mut pager);
        let mut entries = Vec::new();
        loop {
            let offset = reader.stream_position().map_err(|e| StorageError::Logger {
                cause: LoggerError::Io(e),
            })?;
            let entry: Result<LogEntry, _> = decode_from_reader(&mut reader, config);
            match entry {
                Ok(log) => {
                    let row: Row = match log {
                        LogEntry::Update(ref bytes)
                        | LogEntry::Insert(ref bytes)
                        | LogEntry::Delete(ref bytes) => bytes.as_slice().try_into()?,
                        LogEntry::GlobalCheckpoint => continue,
                    };
                    entries.push((offset, row.id()));

                    match log {
                        LogEntry::Update(_) => {
                            btree.update(row)?;
                        }
                        LogEntry::Insert(_) => {
                            btree.insert(row)?;
                        }
                        LogEntry::Delete(_) => {
                            btree.delete(row)?;
                        }
                        _ => unreachable!("only row tasks should be handled here"),
                    };
                }
                Err(_) => break,
            }
        }

        let inner = reader.into_inner();
        Ok(Self {
            config,
            entries,
            pager,
            path,
            writer: BufWriter::new(inner),
        })
    }

    pub fn pager(&mut self) -> &mut Pager {
        &mut self.pager
    }

    /// Writes an entry to the Write-Ahead log. If a [LogEntry::GlobalCheckpoint] is
    /// logged all previous entries are applied permanently and cleared.
    pub fn log(&mut self, entry: LogEntry) -> Result<(), StorageError> {
        let row: Row = match entry {
            LogEntry::GlobalCheckpoint => return self.compact(),
            LogEntry::Update(ref bytes)
            | LogEntry::Insert(ref bytes)
            | LogEntry::Delete(ref bytes) => bytes.as_slice().try_into()?,
        };
        let id = row.id();

        debug!("attempting to apply entry - {entry:?}");
        let mut btree = BTree::new(&mut self.pager);
        match entry {
            LogEntry::Update(_) => {
                btree.update(row)?;
            }
            LogEntry::Insert(_) => {
                btree.insert(row)?;
            }
            LogEntry::Delete(_) => {
                btree.delete(row)?;
            }
            _ => unreachable!("only row tasks should be handled here"),
        };

        info!("entry logged: {entry:?}");
        let offset = self
            .writer
            .stream_position()
            .map_err(|e| StorageError::Logger {
                cause: LoggerError::Io(e),
            })?;
        encode_into_std_write(entry, &mut self.writer, self.config).map_err(|e| {
            StorageError::Logger {
                cause: LoggerError::Serialize(e),
            }
        })?;
        self.writer.flush().map_err(|e| StorageError::Logger {
            cause: LoggerError::Io(e),
        })?;

        self.entries.push((offset, id));

        Ok(())
    }

    fn compact(&mut self) -> Result<(), StorageError> {
        info!("flushing {} entries", self.entries.len());
        trace!("entries: {:?}", self.entries);

        // Apply in-memory state
        self.pager.commit(true);
        self.pager.flush();
        self.pager.commit(false);

        // Clear applied entries
        // NOTE: This assumes the in-memory entries
        // have already been applied to the pager.
        self.entries.clear();
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
    fn list_entries(&self) -> Result<Vec<LogEntry>, StorageError> {
        let log = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&self.path)
            .map_err(|e| StorageError::Logger {
                cause: LoggerError::Io(e),
            })?;
        let mut reader = BufReader::new(log);
        let mut out = Vec::new();

        for (offset, _) in self.entries.iter() {
            reader
                .seek(SeekFrom::Start(*offset))
                .map_err(|e| StorageError::Logger {
                    cause: LoggerError::Io(e),
                })?;
            let entry: LogEntry =
                decode_from_reader(&mut reader, self.config).map_err(|e| StorageError::Logger {
                    cause: LoggerError::Deserialize(e),
                })?;
            out.push(entry);
        }
        Ok(out)
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

    #[test]
    #[should_panic(expected = "Duplicate")]
    fn logger_pager_state() {
        let temp = TempDir::new("log").unwrap();
        let pager = Pager::open(temp.path().join("cryo.db")).unwrap();
        let mut logger = Logger::open(temp.path().join("wal.log"), pager).unwrap();

        let row = Row::new(10, RowType::Leaf);
        logger
            .log(LogEntry::Insert(row.as_bytes().to_vec()))
            .unwrap();
        drop(logger);

        let pager = Pager::open(temp.path().join("cryo.db")).unwrap();
        let mut logger = Logger::open(temp.path().join("wal.log"), pager).unwrap();
        logger
            .log(LogEntry::Insert(row.as_bytes().to_vec()))
            .unwrap();
    }
}
