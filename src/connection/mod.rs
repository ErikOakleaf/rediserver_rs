mod read_buffer;
mod write_buffer;

pub use read_buffer::ReadBuffer;
pub use write_buffer::WriteBuffer;

use crate::{error::RedisError, net::Socket, protocol::parser::CommandParseState};

const INIT_BUFFER_SIZE: usize = 4096;

pub struct Connection {
    pub soc: Socket,
    pub command_parse_state: CommandParseState,
    pub read_buffer: ReadBuffer,
    pub write_buffer: WriteBuffer,
}

impl Connection {
    pub fn new(soc: Socket) -> Self {
        Connection {
            soc: soc,
            command_parse_state: CommandParseState::new(),
            read_buffer: ReadBuffer::new(),
            write_buffer: WriteBuffer::new(),
        }
    }

    pub fn fill_read_buffer(&mut self) -> Result<(), RedisError> {
        let read_result = self.soc.read(&mut self.read_buffer.buf)?;

        println!("bytes read: {}", read_result);

        if read_result == 0 {
            return Ok(()); // TODO - this should maybe be an error or something
        }

        Ok(())
    }

    pub fn flush_write_buffer(&mut self) -> Result<(), RedisError> {
        let result = self.soc.write(self.write_buffer.buf.as_slice())?;
        self.write_buffer.pos += result;

        Ok(())
    }
}
