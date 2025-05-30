pub mod command;
pub mod protocol;
pub mod statement;
pub mod storage;

pub use command::Command;
pub use statement::Statement;

pub(crate) mod utilities {
    use std::error::Error;

    pub const USERNAME_MAX_LENGTH: usize = 32;
    pub const EMAIL_MAX_LENGTH: usize = 255;

    /// Converts a byte array into a UTF-8 character array.
    ///
    pub fn byte_to_char(bytes: &[u8]) -> Result<Vec<char>, Box<dyn Error>> {
        let mut out = Vec::new();

        for chunk in bytes.chunks(4) {
            let ch = std::str::from_utf8(chunk)
                .map_err(|_| "failed to convert byte into character")?
                .chars()
                .next()
                .ok_or("failed to retrieve converted byte character")?;
            out.push(ch);
        }

        Ok(out)
    }

    /// Converts a character array into a byte array.
    ///
    pub fn char_to_byte(chars: &[char]) -> Vec<u8> {
        let mut out = Vec::new();

        for ch in chars {
            let mut buf = [0; 4];
            ch.encode_utf8(&mut buf);
            out.extend_from_slice(&buf[..]);
        }

        out
    }
}
