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

    pub fn clear(&mut self) {
        self.buf.clear();
        self.pos = 0;
    }

    pub fn skip_to_next_command(&mut self) {
        if let Some(pos) = self.buf[self.pos..].iter().position(|&b| b == b'*') {
            self.pos += pos;
        } else {
            self.clear();
        }
    }
}
