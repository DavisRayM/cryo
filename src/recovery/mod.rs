pub mod logger;
pub mod record;

pub use record::{Lsn, Record, RecordEntry, RecordFlags};
use std::io;

/// Attempts to read `buf` bytes or an eof
///
/// ## Errors
///
/// If the function partially fills `buf` an [`io::ErrorKind::UnexpectedEof`]
/// will be returned.
pub(crate) fn read_exact_or_eof(
    reader: &mut impl io::Read,
    buf: &mut [u8],
) -> io::Result<bool> {
    let mut read = 0;

    while read < buf.len() {
        match reader.read(&mut buf[read..])? {
            0 if read == 0 => return Ok(false),
            0 => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "partial WAL frame",
                ));
            }
            n => read += n,
        }
    }

    Ok(true)
}
