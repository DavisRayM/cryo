use bincode::{Decode, Encode};

#[derive(Debug, Encode, Decode, PartialEq, Eq)]
pub enum Response {
    Ok,
    Pong,
    StateChanged,
    Query {
        rows: Vec<u8>,
    },
    Structure {
        out: String,
    },
    Err {
        code: ResponseError,
        description: String,
    },
    ConnectionClosed,
}

#[derive(Debug, Encode, Decode, PartialEq, Eq)]
pub enum ResponseError {
    Query,
    Read,
    Command,
}
