use std::io;

pub const MAX_MESSAGE_SIZE: usize = 4096;

#[derive(Debug)]
pub enum RedisError {
    Io(io::Error),
    ProtocolError(ProtocolError),
    ConnectionClosed,
    ConnectionError(ConnectionError),
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

impl From<ConnectionError> for RedisError {
    fn from(err: ConnectionError) -> Self {
        RedisError::ConnectionError(err)
    }
}

#[derive(Debug)]
pub enum RedisCommandError {
    KeyNotFound,
    UnknownCommand(Box<[u8]>),
    WrongArity(String),
}

#[derive(Debug, PartialEq)]
pub enum ProtocolError {
    ExpectedByte { expected: u8, got: u8 },
    UnexpectedByte(u8),
    Incomplete,
    Other(String),
}

#[derive(Debug)]
pub enum ConnectionError {
    WriteBufferOverflow,
}
