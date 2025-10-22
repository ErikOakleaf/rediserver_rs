use crate::{
    error::{MAX_MESSAGE_SIZE, ProtocolError, RedisError},
    net::Socket,
};

const HEADER_SIZE: usize = 4;
const BUFFER_SIZE: usize = HEADER_SIZE + MAX_MESSAGE_SIZE;

pub struct Connection {
    pub socket: Socket,
    pub state: ConnectionState,
    pub read_state: ReadState,
    pub write_state: WriteState,
}

impl Connection {
    pub fn new(socket: Socket) -> Self {
        Connection {
            socket: socket,
            state: ConnectionState::Read,
            read_state: ReadState::new(),
            write_state: WriteState::new(),
        }
    }

    pub fn handle_readable(&mut self) -> Result<(), RedisError> {
        self.fill_read_buffer()?;
        Ok(())
    }

    pub fn handle_writeable(&mut self) -> Result<(), RedisError> {
        Ok(())
    }

    fn fill_read_buffer(&mut self) -> Result<(), RedisError> {
        let read_result = self
            .socket
            .read(&mut self.read_state.buffer[self.read_state.bytes_filled..])?;

        if read_result == 0 {
            return Err(RedisError::ConnectionClosed);
        }

        self.read_state.bytes_filled += read_result;
        Ok(())
    }

    fn try_extract_message(&mut self) -> Option<(usize, usize)> {
        if let Some(message) = self.try_extract_partial_message() {
            return Some(message);
        }

        if self.avaliable_bytes() < 4 {
            self.reset_read_buffer_if_needed();
            return None;
        }

        self.try_extract_new_message()
    }

    fn try_extract_new_message(&mut self) -> Option<(usize, usize)> {
        let mut start = self.read_state.position;
        let length_slice = &self.read_state.buffer[start..start + 4];
        let length = Self::get_message_length(length_slice).unwrap();

        let leftover = self.avaliable_bytes();

        if leftover < length {
            self.shift_read_buffer_and_remember_partial(length, leftover);
            return None;
        }

        start += 4;
        let end = start + length;

        Some((start, end))
    }

    fn try_extract_partial_message(&mut self) -> Option<(usize, usize)> {
        let length = match self.read_state.wanted_length {
            Some(length) => length,
            None => return None,
        };

        if self.avaliable_bytes() < length {
            return None;
        }

        let start = self.read_state.position;
        let end = start + length;

        self.read_state.position = end;
        self.read_state.wanted_length = None;

        Some((start, end))
    }

    // Helpers

    fn flush_write_buffer(&mut self) {
        self.write_state.size = 0;
        self.write_state.bytes_written = 0;
        self.state = ConnectionState::Read;
    }

    #[inline(always)]
    fn reset_read_buffer_if_needed(&mut self) {
        if self.read_state.position > 0 && self.read_state.position == self.read_state.bytes_filled
        {
            self.read_state.bytes_filled = 0;
            self.read_state.position = 0;
        }
    }

    #[inline]
    fn shift_read_buffer_and_remember_partial(&mut self, length: usize, leftover: usize) {
        self.read_state.buffer.copy_within(
            self.read_state.position + 4..self.read_state.bytes_filled,
            0,
        );
        self.read_state.wanted_length = Some(length);
        self.read_state.bytes_filled = leftover - 4;
        self.read_state.position = 0;
    }

    #[inline(always)]
    fn avaliable_bytes(&self) -> usize {
        self.read_state.bytes_filled - self.read_state.position
    }

    #[inline]
    fn get_message_length(buffer: &[u8]) -> Result<usize, ProtocolError> {
        let length = Self::u32_from_be_bytes(&buffer[..4]) as usize;

        if length > MAX_MESSAGE_SIZE {
            return Err(ProtocolError::MessageTooLong(length));
        }

        Ok(length)
    }

    #[inline]
    fn u32_from_be_bytes(slice: &[u8]) -> u32 {
        debug_assert!(
            slice.len() >= 4,
            "SLICE DOES NOT HAVE CORRECT LENGTH IN u32 from be bytes function",
        );

        let length = ((slice[0] as u32) << 24)
            | ((slice[1] as u32) << 16)
            | ((slice[2] as u32) << 8)
            | (slice[3] as u32);
        length
    }
}

pub struct ReadState {
    pub buffer: [u8; BUFFER_SIZE],
    pub bytes_filled: usize,
    pub position: usize,
    pub wanted_length: Option<usize>,
}

impl ReadState {
    pub fn new() -> Self {
        ReadState {
            buffer: [0u8; BUFFER_SIZE],
            bytes_filled: 0,
            position: 0,
            wanted_length: None,
        }
    }
}

pub struct WriteState {
    pub buffer: [u8; BUFFER_SIZE],
    pub size: usize,
    pub bytes_written: usize,
}

impl WriteState {
    pub fn new() -> Self {
        WriteState {
            buffer: [0u8; BUFFER_SIZE],
            size: 0,
            bytes_written: 0,
        }
    }
}

#[derive(PartialEq)]
pub enum ConnectionState {
    Read,
    Write,
    End,
}
