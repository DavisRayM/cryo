use super::{error::StorageError, header::row::*};

pub(crate) const ROW_ALLOCATED_SPACE: usize = if INTERNAL_ROW_SIZE > LEAF_ROW_SIZE {
    INTERNAL_ROW_SIZE
} else {
    LEAF_ROW_SIZE
};

#[derive(Debug, Clone)]
pub(crate) struct Row(pub [u8; ROW_ALLOCATED_SPACE]);

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

impl Row {
    pub fn new() -> Self {
        Self([0; ROW_ALLOCATED_SPACE])
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

    pub fn set_offset(&mut self, offset: usize) {
        self.0[ROW_OFFSET..ROW_OFFSET + ROW_OFFSET_SIZE]
            .clone_from_slice(offset.to_ne_bytes().as_ref());
    }

    pub fn id(&self) -> Result<usize, StorageError> {
        Ok(usize::from_ne_bytes(
            self.0[ROW_ID..ROW_USERNAME]
                .try_into()
                .map_err(|_| StorageError::Row {
                    action: "retrieve id".into(),
                    error: "failed to get id bytes".into(),
                })?,
        ))
    }

    pub fn set_id(&mut self, id: usize) {
        self.0[ROW_ID..ROW_USERNAME].clone_from_slice(id.to_ne_bytes().as_ref());
    }

    pub fn username(&self) -> Result<String, StorageError> {
        let bytes = &self.0[ROW_USERNAME..ROW_EMAIL];
        let chars = byte_to_char(bytes, Some(format!("row({}) username", self.id()?)))?;
        Ok(chars.iter().collect())
    }

    pub fn set_username(&mut self, username: &[char]) {
        self.0[ROW_USERNAME..ROW_EMAIL].clone_from_slice(&char_to_byte(username));
    }

    pub fn email(&self) -> Result<String, StorageError> {
        let bytes = &self.0[ROW_EMAIL..self.0.len()];
        let chars = byte_to_char(bytes, Some(format!("row({}) email", self.id()?)))?;
        Ok(chars.iter().collect())
    }

    pub fn set_email(&mut self, email: &[char]) {
        let end = self.0.len();
        self.0[ROW_EMAIL..end].clone_from_slice(&char_to_byte(email));
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
        let mut buf = [0; ROW_ALLOCATED_SPACE];
        match value.len() {
            size if size == LEAF_ROW_SIZE || size == INTERNAL_ROW_SIZE => {
                buf[..size].clone_from_slice(value);
                Ok(Self(buf))
            }
            l => Err(StorageError::Row {
                action: "read bytes".into(),
                error: format!("unexpected size '{}'", l),
            }),
        }
    }
}

impl From<&Row> for [u8; INTERNAL_ROW_SIZE] {
    fn from(val: &Row) -> [u8; INTERNAL_ROW_SIZE] {
        let mut buf = [0; INTERNAL_ROW_SIZE];
        buf[..].clone_from_slice(&val.0[..INTERNAL_ROW_SIZE]);
        buf
    }
}

impl From<&Row> for [u8; LEAF_ROW_SIZE] {
    fn from(val: &Row) -> [u8; LEAF_ROW_SIZE] {
        let mut buf = [0; LEAF_ROW_SIZE];
        buf[..].clone_from_slice(&val.0[..LEAF_ROW_SIZE]);
        buf
    }
}

#[cfg(test)]
mod tests {
    use crate::statement::{EMAIL_MAX_LENGTH, USERNAME_MAX_LENGTH, convert_to_char_array};

    use super::*;

    #[test]
    fn char_to_byte_convertable() {
        let chars = vec!['a', 'b'];
        let bytes = char_to_byte(&chars);
        assert_eq!(chars, byte_to_char(&bytes, None).unwrap())
    }

    #[test]
    fn internal_cell() {
        let mut row = Row::new();
        row.set_id(90);

        let bytes: [u8; INTERNAL_ROW_SIZE] = (&row).into();
        let row: Row = (&bytes[..]).try_into().unwrap();
        assert_eq!(row.id().unwrap(), 90);
    }

    #[test]
    fn leaf_cell() {
        let mut row = Row::new();
        let email = convert_to_char_array::<EMAIL_MAX_LENGTH>(vec!['a', 'b'], '\0').unwrap();
        row.set_email(&email);

        let bytes: [u8; LEAF_ROW_SIZE] = (&row).into();
        let row: Row = (&bytes[..]).try_into().unwrap();
        assert_eq!(row.email().unwrap(), email.iter().collect::<String>());
    }

    #[test]
    fn row_offset() {
        let mut row = Row::new();
        row.set_offset(10);
        assert_eq!(row.offset().unwrap(), 10);
    }

    #[test]
    fn row_id() {
        let mut row = Row::new();
        row.set_id(10);
        assert_eq!(row.id().unwrap(), 10);
    }

    #[test]
    fn row_username() {
        let mut row = Row::new();
        let username = convert_to_char_array::<USERNAME_MAX_LENGTH>(vec!['a', 'b'], '\0').unwrap();
        row.set_username(&username);
        assert_eq!(row.username().unwrap(), username.iter().collect::<String>());
    }

    #[test]
    fn row_email() {
        let mut row = Row::new();
        let email = convert_to_char_array::<EMAIL_MAX_LENGTH>(vec!['a', 'b'], '\0').unwrap();
        row.set_email(&email);
        assert_eq!(row.email().unwrap(), email.iter().collect::<String>());
    }
}
