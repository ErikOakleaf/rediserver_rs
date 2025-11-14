use crate::connection::INIT_BUFFER_SIZE;

pub struct WriteBuffer {
    pub buf: Vec<u8>,
    pub pos: usize,
}

impl WriteBuffer {
    pub fn new() -> Self {
        WriteBuffer {
            buf: Vec::<u8>::with_capacity(INIT_BUFFER_SIZE),
            pos: 0,
        }
    }

    pub fn append_bytes(&mut self, slice: &[u8]) {
        self.buf.extend_from_slice(slice);
    }

    pub fn append_byte(&mut self, byte: u8) {
        self.buf.push(byte);
    }

    pub fn clear(&mut self) {
        self.buf.clear();
        self.pos = 0;
    }
}
