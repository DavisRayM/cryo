use std::fmt;

use super::{error::StorageError, header::row::*};

#[derive(Debug, Clone)]
pub(crate) struct Row(pub Vec<u8>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RowType {
    Internal,
    Leaf,
}

impl TryInto<RowType> for u8 {
    type Error = StorageError;
    fn try_into(self) -> Result<RowType, Self::Error> {
        match self {
            0x0 => Ok(RowType::Internal),
            0x1 => Ok(RowType::Leaf),
            _ => Err(StorageError::Row {
                action: "read row type".into(),
                error: "unknown type".into(),
            }),
        }
    }
}

impl From<RowType> for u8 {
    fn from(value: RowType) -> Self {
        match value {
            RowType::Internal => 0x0,
            RowType::Leaf => 0x1,
        }
    }
}

pub fn byte_to_char(bytes: &[u8], mut cause: Option<String>) -> Result<Vec<char>, StorageError> {
    let mut out = Vec::new();

    for chunk in bytes.chunks(4) {
        let ch = std::str::from_utf8(chunk)
            .map_err(|_| StorageError::Utility {
                name: "byte_to_char - read character".into(),
                cause: cause.take(),
            })?
            .chars()
            .next()
            .ok_or(StorageError::Utility {
                name: "byte_to_char - retrieve character".into(),
                cause: cause.take(),
            })?;
        out.push(ch);
    }

    Ok(out)
}

pub fn char_to_byte(chars: &[char]) -> Vec<u8> {
    let mut out = Vec::new();

    for ch in chars {
        let mut buf = [0; 4];
        ch.encode_utf8(&mut buf);
        out.extend_from_slice(&buf[..]);
    }

    out
}

impl fmt::Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.id().unwrap())
    }
}

impl Row {
    pub fn new(t: RowType) -> Self {
        let size = if t == RowType::Internal {
            ROW_RIGHT_OFFSET + ROW_OFFSET_SIZE
        } else {
            ROW_BODY_SIZE
        };
        let mut inner = vec![0; size];
        inner[ROW_TYPE] = t.into();
        Self(vec![0; size])
    }

    pub fn left(&self) -> Result<usize, StorageError> {
        Ok(usize::from_ne_bytes(
            self.0[ROW_LEFT_OFFSET..ROW_LEFT_OFFSET + ROW_OFFSET_SIZE]
                .try_into()
                .map_err(|_| StorageError::Row {
                    action: "retrieve id".into(),
                    error: "failed to get id bytes".into(),
                })?,
        ))
    }

    pub fn set_left(&mut self, offset: usize) {
        let end = ROW_LEFT_OFFSET + ROW_OFFSET_SIZE;
        self.0[ROW_LEFT_OFFSET..end].clone_from_slice(offset.to_ne_bytes().as_ref());
    }

    pub fn right(&self) -> Result<usize, StorageError> {
        Ok(usize::from_ne_bytes(
            self.0[ROW_RIGHT_OFFSET..ROW_RIGHT_OFFSET + ROW_OFFSET_SIZE]
                .try_into()
                .map_err(|_| StorageError::Row {
                    action: "retrieve id".into(),
                    error: "failed to get id bytes".into(),
                })?,
        ))
    }

    pub fn set_right(&mut self, offset: usize) {
        let end = ROW_RIGHT_OFFSET + ROW_OFFSET_SIZE;
        self.0[ROW_RIGHT_OFFSET..end].clone_from_slice(offset.to_ne_bytes().as_ref());
    }

    pub fn offset(&self) -> Result<usize, StorageError> {
        Ok(usize::from_ne_bytes(
            self.0[ROW_OFFSET..ROW_OFFSET + ROW_OFFSET_SIZE]
                .try_into()
                .map_err(|_| StorageError::Row {
                    action: "retrieve id".into(),
                    error: "failed to get id bytes".into(),
                })?,
        ))
    }

    pub fn id(&self) -> Result<usize, StorageError> {
        Ok(usize::from_ne_bytes(
            self.0[ROW_ID..ROW_TYPE]
                .try_into()
                .map_err(|_| StorageError::Row {
                    action: "retrieve id".into(),
                    error: "failed to get id bytes".into(),
                })?,
        ))
    }

    pub fn set_id(&mut self, id: usize) {
        self.0[ROW_ID..ROW_TYPE].clone_from_slice(id.to_ne_bytes().as_ref());
    }

    pub fn size(&self) -> Result<usize, StorageError> {
        Ok(match self.row_type()? {
            RowType::Internal => ROW_RIGHT_OFFSET + ROW_OFFSET_SIZE,
            RowType::Leaf => {
                let size =
                    usize::from_ne_bytes(self.0[ROW_VALUE..ROW_BODY_SIZE].try_into().map_err(
                        |_| StorageError::Row {
                            action: "retrieve id".into(),
                            error: "failed to get row size bytes".into(),
                        },
                    )?);
                ROW_BODY_SIZE + size
            }
        })
    }

    pub fn row_type(&self) -> Result<RowType, StorageError> {
        self.0[ROW_TYPE].try_into()
    }

    pub fn set_type(&mut self, t: RowType) {
        self.0[ROW_TYPE] = t.into();
    }

    pub fn value(&self) -> Result<&[u8], StorageError> {
        let size =
            usize::from_ne_bytes(self.0[ROW_VALUE..ROW_BODY_SIZE].try_into().map_err(|_| {
                StorageError::Row {
                    action: "retrieve id".into(),
                    error: "failed to get row size bytes".into(),
                }
            })?);
        Ok(&self.0[ROW_BODY_SIZE..ROW_BODY_SIZE + size])
    }

    pub fn set_value(&mut self, value: &[u8]) {
        let end = ROW_BODY_SIZE + value.len();
        if self.0.len() < end {
            self.0.resize(end, 0);
        }
        self.0[ROW_VALUE..ROW_BODY_SIZE].clone_from_slice(value.len().to_ne_bytes().as_ref());
        self.0[ROW_BODY_SIZE..end].clone_from_slice(value);
    }
}

impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        self.id()
            .expect("row id eq")
            .eq(&other.id().expect("row id eq"))
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
        self.id()
            .expect("row id ord")
            .cmp(&other.id().expect("row id ord"))
    }
}

impl TryFrom<&[u8]> for Row {
    type Error = StorageError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() < ROW_BODY_SIZE {
            return Err(StorageError::Row {
                action: "read bytes".into(),
                error: format!("unexpected size '{}'", value.len()),
            });
        }
        let mut row = Row(value.to_vec());
        row.set_type(row.row_type()?);
        Ok(row)
    }
}

impl From<&Row> for Vec<u8> {
    fn from(value: &Row) -> Self {
        value.0.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::statement::{EMAIL_MAX_LENGTH, convert_to_char_array};

    use super::*;

    #[test]
    fn char_to_byte_convertable() {
        let chars = vec!['a', 'b'];
        let bytes = char_to_byte(&chars);
        assert_eq!(chars, byte_to_char(&bytes, None).unwrap())
    }

    #[test]
    fn internal_cell() {
        let mut row = Row::new(RowType::Internal);
        row.set_id(90);

        let bytes: Vec<u8> = (&row).into();
        let row: Row = (&bytes[..]).try_into().unwrap();
        assert_eq!(row.id().unwrap(), 90);
    }

    #[test]
    fn leaf_cell() {
        let mut row = Row::new(RowType::Leaf);
        let email = convert_to_char_array::<EMAIL_MAX_LENGTH>(vec!['a', 'b'], '\0').unwrap();
        let email = char_to_byte(&email);
        row.set_value(&email);

        let bytes: Vec<u8> = (&row).into();
        let row: Row = (&bytes[..]).try_into().unwrap();
        assert_eq!(row.value().unwrap(), email);
    }

    #[test]
    fn row_offset() {
        let mut row = Row::new(RowType::Internal);
        row.set_left(10);
        assert_eq!(row.offset().unwrap(), 10);
    }

    #[test]
    fn row_id() {
        let mut row = Row::new(RowType::Internal);
        row.set_id(10);
        assert_eq!(row.id().unwrap(), 10);
    }
}
