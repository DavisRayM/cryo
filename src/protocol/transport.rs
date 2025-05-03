use std::io::{self, Read, Write};

use bincode::{
    config::{BigEndian, Configuration, Fixint},
    decode_from_std_read, encode_into_std_write,
};
use thiserror::Error;

use crate::Command;

use super::{Request, Response};

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("failed to encode message: {0}")]
    Serialize(#[from] bincode::error::EncodeError),
    #[error("failed to decode message: {0}")]
    Deserialize(#[from] bincode::error::DecodeError),
    #[error("Transport IO Error: {0}")]
    Io(#[from] io::Error),
}

pub struct ProtocolTransport<T: Read + Write> {
    stream: T,
    config: Configuration<BigEndian, Fixint>,
}

impl<T: Read + Write> ProtocolTransport<T> {
    pub fn new(stream: T) -> Self {
        let config = bincode::config::standard()
            .with_big_endian()
            .with_fixed_int_encoding();
        Self { stream, config }
    }

    pub fn write_command(&mut self, command: Command) -> Result<(), TransportError> {
        let req: Request = command.into();
        encode_into_std_write(req, &mut self.stream, self.config)?;
        Ok(())
    }

    pub fn write_response(&mut self, resp: Response) -> Result<(), TransportError> {
        encode_into_std_write(resp, &mut self.stream, self.config)?;
        Ok(())
    }

    pub fn read_response(&mut self) -> Result<Response, TransportError> {
        let resp: Response = decode_from_std_read(&mut self.stream, self.config)?;
        Ok(resp)
    }

    pub fn read_request(&mut self) -> Result<Request, TransportError> {
        let req: Request = decode_from_std_read(&mut self.stream, self.config)?;
        Ok(req)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Seek};

    use super::*;

    #[test]
    fn read_write_request() {
        let stream = Cursor::new(Vec::new());
        let mut transport = ProtocolTransport::new(stream);

        transport.write_command(Command::Exit).unwrap();
        transport.stream.seek(std::io::SeekFrom::Start(0)).unwrap();
        let req = transport.read_request().unwrap();
        assert_eq!(req, Request::CloseConnection);
    }

    #[test]
    fn read_write_response() {
        let stream = Cursor::new(Vec::new());
        let mut transport = ProtocolTransport::new(stream);

        transport.write_response(Response::Pong).unwrap();
        transport.stream.seek(std::io::SeekFrom::Start(0)).unwrap();
        let resp = transport.read_response().unwrap();
        assert_eq!(resp, Response::Pong);
    }
}
