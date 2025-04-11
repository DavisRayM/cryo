use crate::{EMAIL_MAX_LENGTH, Statement, USERNAME_MAX_LENGTH};

// NOTE: Characters in rust are Unicode scalar values which are 4 bytes; Hence the *4
pub(crate) const ROW_SIZE: usize =
    size_of::<usize>() + (USERNAME_MAX_LENGTH * 4) + (EMAIL_MAX_LENGTH * 4);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Row([u8; ROW_SIZE]);

impl TryFrom<Statement> for Row {
    type Error = String;

    fn try_from(value: Statement) -> Result<Self, Self::Error> {
        unimplemented!()
    }
}

pub(crate) fn char_array_to_byte_array(val: &[char]) -> Vec<u8> {
    todo!()
}

pub(crate) fn byte_array_to_char_array(val: &[u8]) -> Vec<char> {
    todo!()
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
