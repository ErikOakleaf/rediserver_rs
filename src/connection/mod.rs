mod read_state;
mod write_state;

use read_state::ReadState;
use write_state::WriteState;

use crate::{
    error::{MAX_MESSAGE_SIZE, RedisError},
    net::Socket,
};

const HEADER_SIZE: usize = 4;
const BUFFER_SIZE: usize = HEADER_SIZE + MAX_MESSAGE_SIZE;

pub enum ConnectionAction {
    None,
    WantRead,
    WantWrite,
    End,
}

#[derive(Debug, PartialEq)]
enum StringExtractionResult {
    Complete((usize, usize), usize),
    Partial(usize),
    None,
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

    pub fn handle_readable(&mut self) -> Result<ConnectionAction, RedisError> {
        Ok(ConnectionAction::None)
        // self.fill_read_buffer()?;
        // loop {
        //     let message_extraction_result = self.try_extract_message();
        //
        //     if message_extraction_result == false {
        //         return Ok(ConnectionAction::None);
        //     }
        //
        //     match self.handle_message() {
        //         ConnectionAction::WantWrite => return Ok(ConnectionAction::WantWrite),
        //         _ => {}
        //     };
        //
        //     if self.read_state.position == self.read_state.bytes_filled {
        //         break;
        //     }
        // }
        //
        // Ok(ConnectionAction::None);
    }

    fn fill_read_buffer(&mut self) -> Result<(), RedisError> {
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
    fn handle_message(&mut self) {}

    pub fn handle_writeable(&mut self) -> Result<ConnectionAction, RedisError> {
        self.try_write_after_writable()
    }

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

    fn try_write_after_read(&mut self) -> Result<ConnectionAction, RedisError> {
        if self.flush_write_buffer()? {
            Ok(ConnectionAction::None)
        } else {
            Ok(ConnectionAction::WantWrite)
        }
    }

    fn try_write_after_writable(&mut self) -> Result<ConnectionAction, RedisError> {
        if self.flush_write_buffer()? {
            Ok(ConnectionAction::WantRead)
        } else {
            Ok(ConnectionAction::None)
        }
    }
}
