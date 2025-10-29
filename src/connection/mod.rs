mod read_state;
mod write_state;

pub use read_state::ReadState;
pub use write_state::WriteState;

use crate::{
    error::{MAX_MESSAGE_SIZE, RedisError},
    net::Socket,
};

const HEADER_SIZE: usize = 4;
const BUFFER_SIZE: usize = HEADER_SIZE + MAX_MESSAGE_SIZE;

pub enum ConnectionError {
    WriteBufferOverflow,
}

pub struct Connection {
    pub socket: Socket,
    pub read_state: ReadState,
    pub write_state: WriteState,
}

impl Connection {
    pub fn new(socket: Socket) -> Self {
        Connection {
            socket: socket,
            read_state: ReadState::new(),
            write_state: WriteState::new(),
        }
    }

    pub fn fill_read_buffer(&mut self) -> Result<(), RedisError> {
        let read_result = self
            .socket
            .read(&mut self.read_state.buffer[self.read_state.bytes_filled..])?;

        if read_result == 0 {
            return Ok(()); // TODO - this should maybe be an error or something
        }

        self.read_state.bytes_filled += read_result;
        Ok(())
    }

    // handling of messages

    fn prepare_response(&mut self, response: &[u8]) {
        self.write_state.buffer[..HEADER_SIZE]
            .copy_from_slice(&(response.len() as u32).to_be_bytes());
        self.write_state.buffer[HEADER_SIZE..HEADER_SIZE + response.len()]
            .copy_from_slice(response);
        self.write_state.size = response.len() + HEADER_SIZE;
    }

    fn flush_write_buffer(&mut self) -> Result<bool, RedisError> {
        let ws = &mut self.write_state;
        ws.bytes_written += self.socket.write(&ws.buffer[ws.bytes_written..ws.size])?;

        if ws.bytes_written == ws.size {
            self.write_state.size = 0;
            self.write_state.bytes_written = 0;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
