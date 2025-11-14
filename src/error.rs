use std::{io};

use crate::connection::WriteBuffer;

pub const MAX_MESSAGE_SIZE: usize = 4096;

#[derive(Debug)]
pub enum RedisError {
    Io(io::Error),
    ProtocolError(ProtocolError),
    ConnectionClosed,
    ConnectionError(ConnectionError),
    Other(String),
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
    UnknownCommand(Vec<u8>),
    WrongNumberOfArguments { cmd: Vec<u8> }
}

#[derive(Debug)]
pub enum ConnectionError {
    WriteBufferOverflow,
}

// pub fn handle_redis_error(error: &RedisError, write_buffer: &mut WriteBuffer) {
//
// }

pub fn handle_protocol_error(error: &ProtocolError, write_buffer: &mut WriteBuffer) {
    let mut error_reply = b"-ERR Protocol error: ".to_vec();

    // add error string bytes depending on error
    match error {
        ProtocolError::ExpectedByte { expected, got } => {
            error_reply.extend_from_slice(b"expected: '");
            error_reply.push(*expected);
            error_reply.extend_from_slice(b"', got: '");
            error_reply.push(*got);
            error_reply.push(b'\'');
        }
        ProtocolError::UnexpectedByte(byte) => {
            error_reply.extend_from_slice(b"unexpected byte: '");
            error_reply.push(*byte);
            error_reply.push(b'\'');
        }
        ProtocolError::UnknownCommand(cmd) => {
            error_reply.extend_from_slice(b"unexpected command: '");
            error_reply.extend_from_slice(&cmd);
            error_reply.push(b'\'');
        }
        ProtocolError::WrongNumberOfArguments { cmd } => {
            error_reply.extend_from_slice(b"wrong number of arguments for '");
            error_reply.extend_from_slice(&cmd);
            error_reply.extend_from_slice(b"' command");
        }
        ProtocolError::Incomplete => unreachable!("INCOMPLETE SHOULD BE HANDLED ELSEWHERE NOT HERE"),
    }

    error_reply.extend_from_slice(b"\r\n");

    write_buffer.append_bytes(&error_reply);
}
