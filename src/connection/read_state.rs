use crate::{commands::RedisCommand, connection::INIT_BUFFER_SIZE, error::ProtocolError};

pub struct ReadBuffer {
    pub buffer: Vec<u8>,
    pub parse_position: usize,
    pub command_start: usize,
}

impl ReadBuffer {
    pub fn new() -> Self {
        ReadBuffer {
            buffer: Vec::<u8>::with_capacity(INIT_BUFFER_SIZE),
            parse_position: 0,
            command_start: 0,
        }
    }
}
