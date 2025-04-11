use crate::{EMAIL_MAX_LENGTH, Statement, USERNAME_MAX_LENGTH};

pub(crate) const ROW_ID_SIZE: usize = size_of::<usize>();
// NOTE: Characters in rust are Unicode scalar values which are maximum 4 bytes; Hence the *4
pub(crate) const ROW_USERNAME_SIZE: usize = USERNAME_MAX_LENGTH * 4;
pub(crate) const ROW_EMAIL_SIZE: usize = EMAIL_MAX_LENGTH * 4;

pub(crate) const ROW_SIZE: usize = ROW_ID_SIZE + ROW_USERNAME_SIZE + ROW_EMAIL_SIZE;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Row([u8; ROW_SIZE]);

impl TryFrom<Statement> for Row {
    type Error = String;

    fn try_from(value: Statement) -> Result<Self, Self::Error> {
        match value {
            Statement::Insert {
                id,
                username,
                email,
            } => {
                let mut bytes = Vec::new();

                bytes.extend_from_slice(id.to_ne_bytes().as_ref());
                bytes.extend_from_slice(&char_array_to_byte_array(&username));
                bytes.extend_from_slice(&char_array_to_byte_array(email.as_ref()));

                Ok(Self(bytes.try_into().expect("should be expected length")))
            }
            _ => Err("can not convert statement to row".to_string()),
        }
    }
}

pub(crate) fn char_array_to_byte_array(val: &[char]) -> Vec<u8> {
    let mut res = Vec::new();

    for c in val {
        let mut buf = [0; 4];
        c.encode_utf8(&mut buf);
        res.extend_from_slice(&buf);
    }

    res
}

pub(crate) fn byte_array_to_char_array(val: &[u8]) -> Vec<char> {
    let mut res = Vec::new();

    for chunk in val.chunks(4) {
        // TODO: At some point I may want to handle these errors...
        let str = std::str::from_utf8(chunk).expect("should be valid UTF-8 character");
        res.push(str.chars().next().expect("should be atleast one char"));
    }

    res
}

#[cfg(test)]
mod test {
    use crate::convert_to_char_array;

    use super::*;

    #[test]
    fn char_to_byte_array_conversion() {
        let chars = convert_to_char_array::<10>(vec!['a', 'b', 'c'], '_').unwrap();
        let bytes = char_array_to_byte_array(&chars);

        assert_eq!(byte_array_to_char_array(&bytes), chars.to_vec());
    }

    #[test]
    fn row_from_statement() {
        let id: usize = 1;
        let username = convert_to_char_array(vec!['d', 'a', 'v', 'i', 's'], '\0').unwrap();
        let email =
            Box::new(convert_to_char_array("git@davisraym.com".chars().collect(), '\0').unwrap());

        let mut expected = Vec::new();
        expected.extend_from_slice(id.to_ne_bytes().as_ref());
        expected.extend_from_slice(&char_array_to_byte_array(&username));
        expected.extend_from_slice(&char_array_to_byte_array(email.as_ref()));

        assert_eq!(
            Row(expected[..].try_into().unwrap()),
            Statement::Insert {
                id,
                username,
                email,
            }
            .try_into()
            .unwrap()
        );
    }
}
