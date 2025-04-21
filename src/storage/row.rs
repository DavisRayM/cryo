//! Representation of a record within B-Tree pages.
//!
//! This module defines the [`Row`] struct, which encapsulates a record as stored
//! within a page on disk and as manipulated in memory. Rows are the atomic unit of storage
//! in Cryo.
//!
//! A `Row` is designed for efficient serialization to and from a binary format suitable for
//! compact storage inside fixed-size pages.
//!
//! # Structure
//!
//! All row types currently consist of:
//!
//! - An **id**: usize
//! - A **type**: u8
//!
//! While internal rows consists of:
//! - A **left** offset: usize
//! - A **right** offset: usize
//!
//! And, leaf rows consists of:
//! - A **username**: [char; USERNAME_MAX_LENGTH]
//! - An **email**: [char; EMAIL_MAX_LENGTH]
//!
//! On disk, internal rows are written as:
//!
//! ```text
//! [id: u64][left: u64][right: u64]
//! ```
//!
//! While leaf nodes are written as:
//! ```text
//! [id: u64][username_bytes][email_bytes]
//! ```
//!
//! # See Also
//! - [`StorageEngine`](crate::storage::StorageEngine): Uses rows to implement low-level get/insert/delete operations.

use std::fmt;

use crate::utilities::{EMAIL_MAX_LENGTH, USERNAME_MAX_LENGTH};

use super::StorageError;

pub const ROW_ID_SIZE: usize = size_of::<usize>();
pub const ROW_TYPE_SIZE: usize = size_of::<u8>();
pub const ROW_OFFSET_SIZE: usize = size_of::<usize>();
pub const ROW_USERNAME_SIZE: usize = size_of::<char>() * USERNAME_MAX_LENGTH;
pub const ROW_EMAIL_SIZE: usize = size_of::<char>() * EMAIL_MAX_LENGTH;
pub const ROW_VALUE_LEN_SIZE: usize = size_of::<usize>();
pub const ROW_HEADER_SIZE: usize = ROW_ID_SIZE + ROW_TYPE_SIZE;
pub const INTERNAL_ROW_SIZE: usize = ROW_HEADER_SIZE + ROW_OFFSET_SIZE + ROW_OFFSET_SIZE;
pub const BASE_LEAF_ROW_SIZE: usize = ROW_HEADER_SIZE + ROW_VALUE_LEN_SIZE;
// Common Offsets
pub const ROW_ID: usize = 0;
pub const ROW_TYPE: usize = ROW_ID + ROW_ID_SIZE;
// Internal Row Offsets
pub const ROW_LEFT: usize = ROW_TYPE + ROW_TYPE_SIZE;
pub const ROW_RIGHT: usize = ROW_LEFT + ROW_OFFSET_SIZE;
// Leaf Row Offsets
pub const ROW_VALUE_LEN: usize = ROW_TYPE + ROW_TYPE_SIZE;
pub const ROW_VALUE: usize = ROW_VALUE_LEN + ROW_VALUE_LEN_SIZE;

/// Supported [`Row`] types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowType {
    /// Located within Leaf pages
    Leaf,
    /// Located within Internal pages
    Internal,
}

/// In-memory representation of a page record.
#[derive(Clone)]
pub struct Row {
    inner: Vec<u8>,
    pub _type: RowType,
    value_len: Option<usize>,
}

impl Row {
    /// Create a new row
    pub fn new(id: usize, _type: RowType) -> Self {
        let mut inner = Vec::new();
        let value_len;
        match _type {
            RowType::Internal => {
                inner.resize(INTERNAL_ROW_SIZE, 0);
                inner[ROW_ID..ROW_ID + ROW_ID_SIZE].clone_from_slice(id.to_ne_bytes().as_ref());
                inner[ROW_TYPE] = _type.into();
                value_len = None;
            }
            RowType::Leaf => {
                inner.resize(BASE_LEAF_ROW_SIZE, 0);
                inner[ROW_ID..ROW_ID + ROW_ID_SIZE].clone_from_slice(id.to_ne_bytes().as_ref());
                inner[ROW_TYPE] = _type.into();
                value_len = Some(0);
                inner[ROW_VALUE_LEN..ROW_VALUE_LEN + ROW_VALUE_LEN_SIZE]
                    .clone_from_slice(0_usize.to_ne_bytes().as_ref());
            }
        }
        Self {
            inner,
            _type,
            value_len,
        }
    }

    /// Retrieve row ID
    pub fn id(&self) -> usize {
        self.read_usize(ROW_ID)
    }

    /// Retrieve row value
    ///
    /// # Panics
    ///
    /// This function panics if called by a non-leaf row
    pub fn value(&self) -> Vec<u8> {
        if self._type != RowType::Leaf {
            panic!("username() called on a non-leaf row")
        }
        self.read_bytes(ROW_VALUE, self.value_len.unwrap_or(0))
    }

    /// Retrieve row left offset
    ///
    /// # Panics
    ///
    /// This function panics if called by a non-internal row
    pub fn left_offset(&self) -> usize {
        if self._type != RowType::Internal {
            panic!("left_offset() called on a non-internal row")
        }
        self.read_usize(ROW_LEFT)
    }

    /// Retrieve row right offset
    ///
    /// # Panics
    ///
    /// This function panics if called by a non-internal row
    pub fn right_offset(&self) -> usize {
        if self._type != RowType::Internal {
            panic!("right_offset() called on a non-internal row")
        }
        self.read_usize(ROW_RIGHT)
    }

    /// Set row ID value
    pub fn set_id(&mut self, id: usize) {
        self.write_usize(ROW_ID, id);
    }

    /// Set row value
    ///
    /// # Panics
    ///
    /// This function panics if called by a non-leaf row
    pub fn set_value(&mut self, value: &[u8]) {
        if self._type != RowType::Leaf {
            panic!("set_email() called on a non-leaf row")
        }
        let len = value.len();
        self.inner.resize(BASE_LEAF_ROW_SIZE + len, 0);
        self.value_len = Some(len);
        self.write_usize(ROW_VALUE_LEN, len);
        self.write_bytes(value, ROW_VALUE, value.len());
    }

    /// Set row left offset value
    ///
    /// # Panics
    ///
    /// This function panics if called by a non-internal row
    pub fn set_left_offset(&mut self, offset: usize) {
        if self._type != RowType::Internal {
            panic!("set_left_offset() called on a non-internal row")
        }
        self.write_usize(ROW_LEFT, offset);
    }

    /// Set row right offset value
    ///
    /// # Panics
    ///
    /// This function panics if called by a non-internal row
    pub fn set_right_offset(&mut self, offset: usize) {
        if self._type != RowType::Internal {
            panic!("set_right_offset() called on a non-internal row")
        }
        self.write_usize(ROW_RIGHT, offset);
    }

    /// Returns byte representation of the row
    pub fn as_bytes(&self) -> Vec<u8> {
        self.inner.clone()
    }

    fn write_usize(&mut self, pos: usize, value: usize) {
        self.inner[pos..pos + size_of::<usize>()].clone_from_slice(value.to_ne_bytes().as_ref());
    }

    fn write_bytes(&mut self, bytes: &[u8], pos: usize, len: usize) {
        self.inner[pos..pos + len].clone_from_slice(bytes);
    }

    fn read_usize(&self, pos: usize) -> usize {
        usize::from_ne_bytes(
            self.inner[pos..pos + size_of::<usize>()]
                .try_into()
                .expect("bytes convertable to [u8; 8] due to definition"),
        )
    }

    fn read_bytes(&self, pos: usize, len: usize) -> Vec<u8> {
        if len == 0 {
            return vec![];
        }
        self.inner[pos..pos + len].to_vec()
    }
}

impl fmt::Debug for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(ID: {}, Type: {:?})", self.id(), self._type)
    }
}

impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        self._type == other._type && self.id().eq(&other.id())
    }
}

impl Eq for Row {}

impl PartialOrd for Row {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Row {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id().cmp(&other.id())
    }
}

impl From<u8> for RowType {
    fn from(value: u8) -> Self {
        match value {
            0x0 => RowType::Internal,
            0x1 => RowType::Leaf,
            _ => panic!("unexpected row type"),
        }
    }
}

impl From<RowType> for u8 {
    fn from(value: RowType) -> Self {
        match value {
            RowType::Leaf => 0x1,
            RowType::Internal => 0x0,
        }
    }
}

impl TryFrom<&[u8]> for Row {
    type Error = StorageError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let id = usize::from_ne_bytes(
            value[ROW_ID..ROW_ID + ROW_ID_SIZE]
                .try_into()
                .expect("should be expected size"),
        );
        let _type: RowType = value[ROW_TYPE].into();
        let mut row = Row::new(id, _type);

        match _type {
            RowType::Internal => {
                let left = usize::from_ne_bytes(
                    value[ROW_LEFT..ROW_LEFT + ROW_OFFSET_SIZE]
                        .try_into()
                        .expect("should be expected size"),
                );
                let right = usize::from_ne_bytes(
                    value[ROW_RIGHT..ROW_RIGHT + ROW_OFFSET_SIZE]
                        .try_into()
                        .expect("should be expected size"),
                );
                row.set_left_offset(left);
                row.set_right_offset(right);
            }
            RowType::Leaf => {
                let len = usize::from_ne_bytes(
                    value[ROW_VALUE_LEN..ROW_VALUE_LEN + ROW_VALUE_LEN_SIZE]
                        .try_into()
                        .expect("should be expected size"),
                );
                row.set_value(&value[ROW_VALUE..ROW_VALUE + len]);
            }
        }

        Ok(row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_row_as_bytes() {
        let row = Row::new(0, RowType::Internal);
        let bytes = row.as_bytes();

        assert_eq!(bytes.len(), INTERNAL_ROW_SIZE);
    }

    #[test]
    fn leaf_row_as_bytes() {
        let row = Row::new(0, RowType::Leaf);
        let bytes = row.as_bytes();

        assert_eq!(bytes.len(), BASE_LEAF_ROW_SIZE);
    }

    #[test]
    fn set_id() {
        let mut row = Row::new(0, RowType::Leaf);
        row.set_id(10);
        assert_eq!(row.id(), 10);
    }

    #[test]
    fn set_left_offset() {
        let mut row = Row::new(0, RowType::Internal);
        row.set_left_offset(10);
        assert_eq!(row.left_offset(), 10);
    }

    #[test]
    fn set_right_offset() {
        let mut row = Row::new(0, RowType::Internal);
        row.set_right_offset(10);
        assert_eq!(row.right_offset(), 10);
    }

    #[test]
    fn set_value() {
        let mut row = Row::new(0, RowType::Leaf);
        let expected: Vec<u8> = vec![0, 1, 1, 0, 0, 1, 2, 9];
        row.set_value(expected.as_slice());
        assert_eq!(row.value(), expected)
    }
}
