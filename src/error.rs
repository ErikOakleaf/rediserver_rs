use std::io;

pub const MAX_MESSAGE_SIZE: usize = 4096;

#[derive(Debug)]
pub enum RedisError {
    Io(io::Error),
    ProtocolError(ProtocolError),
    ConnectionClosed,
}

impl From<io::Error> for RedisError {
    fn from(err: io::Error) -> Self {
        RedisError::Io(err)
    }
}

impl From<ProtocolError> for RedisError {
    fn from(err: ProtocolError) -> Self {
        RedisError::ProtocolError(err)
    }
}

#[derive(Debug)]
pub enum ProtocolError {
    MessageTooLong(usize),
    InvalidRequest,
}
