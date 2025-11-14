use std::io;

use crate::connection::WriteBuffer;

pub const MAX_MESSAGE_SIZE: usize = 4096;

#[derive(Debug)]
pub enum RedisError {
    Io(io::Error),
    ProtocolError(ProtocolError),
    CommandError(CommandError),
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

impl From<CommandError> for RedisError {
    fn from(err: CommandError) -> Self {
        RedisError::CommandError(err)
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
}

#[derive(Debug)]
pub enum CommandError {
    UnknownCommand { cmd: Vec<u8> },
    WrongNumberOfArguments { cmd: Vec<u8> },
}

#[derive(Debug)]
pub enum ConnectionError {
    WriteBufferOverflow,
}

// pub fn handle_redis_error(error: &RedisError, write_buffer: &mut WriteBuffer) {
//
// }

pub fn handle_protocol_error(error: &ProtocolError, write_buf: &mut WriteBuffer) {
    write_buf.append_bytes(b"-ERR Protocol error: ");

    // add error string bytes depending on error
    match error {
        ProtocolError::ExpectedByte { expected, got } => {
            write_buf.append_bytes(b"expected: '");
            write_buf.append_byte(*expected);
            write_buf.append_bytes(b"', got: '");
            write_buf.append_byte(*got);
            write_buf.append_byte(b'\'');
        }
        ProtocolError::UnexpectedByte(byte) => {
            write_buf.append_bytes(b"unexpected byte: '");
            write_buf.append_byte(*byte);
            write_buf.append_byte(b'\'');
        }
        ProtocolError::Incomplete => {
            unreachable!("INCOMPLETE SHOULD BE HANDLED ELSEWHERE NOT HERE")
        }
    }

    write_buf.append_bytes(b"\r\n");
}

pub fn handle_command_error(error: &CommandError, write_buf: &mut WriteBuffer) {
    write_buf.append_bytes(b"-ERR ");

    // add error string bytes depending on error
    match error {
        CommandError::UnknownCommand { cmd } => {
            write_buf.append_bytes(b"unknown command '");
            write_buf.append_bytes(&cmd);
            write_buf.append_byte(b'\'');
        }
        CommandError::WrongNumberOfArguments { cmd } => {
            write_buf.append_bytes(b"wrong number of arguments for '");
            write_buf.append_bytes(&cmd);
            write_buf.append_bytes(b"' command");
        }
    }

    write_buf.append_bytes(b"\r\n");
}
