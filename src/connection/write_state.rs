use crate::connection::BUFFER_SIZE;

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
