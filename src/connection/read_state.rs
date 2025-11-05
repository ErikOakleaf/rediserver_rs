use crate::connection::INIT_BUFFER_SIZE;

pub struct ReadBuffer {
    pub buf: Vec<u8>,
    pub pos: usize,
}

impl ReadBuffer {
    pub fn new() -> Self {
        ReadBuffer {
            buf: Vec::<u8>::with_capacity(INIT_BUFFER_SIZE),
            pos: 0,
        }
    }
}
