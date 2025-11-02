use crate::connection::INIT_BUFFER_SIZE;

pub struct WriteState {
    pub buffer: Vec<u8>,
    pub write_position: usize,
}

impl WriteState {
    pub fn new() -> Self {
        WriteState {
            buffer: Vec::<u8>::with_capacity(INIT_BUFFER_SIZE),
            write_position: 0,
        }
    }
}
