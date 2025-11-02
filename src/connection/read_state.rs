use crate::{connection::INIT_BUFFER_SIZE, error::ProtocolError};

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

pub struct Parser<'a> {
    buffer: &'a [u8],
    position: usize,
}

const MAX_NUMBER_DIGITS: usize = 20;

impl<'a> Parser<'a> {
    pub fn new(buffer: &'a [u8]) -> Self {
        Parser {
            buffer: buffer,
            position: 0,
        }
    }

    pub fn consumed(&self) -> usize {
        self.position
    }

    fn peek(&self) -> u8 {
        self.buffer[self.position]
    }

    fn consume(&mut self) -> u8 {
        let byte = self.peek();
        self.position += 1;

        // TODO this later here for error handling if we go past the buffer it should be a pratial
        // but i don't know how we should message that up yet
        // if self.position > self.buffer.len() {
        //
        // }

        byte
    }

    fn parse_number_to_usize(&mut self) -> Result<usize, ProtocolError> {
        let mut num = 0;

        loop {
            let byte = self.consume();

            if self.check_crlf(byte)? {
                return Ok(num);
            }

            if !byte.is_ascii_digit() {
                return Err(ProtocolError::UnexpectedByte(byte));
            }

            num = (num * 10) + ((byte - b'0') as usize);
        }
    }

    // helpers

    fn check_crlf(&mut self, byte: u8) -> Result<bool, ProtocolError> {
        if byte == b'\r' {
            let second_byte = self.consume();
            if second_byte == b'\n' {
                return Ok(true);
            }

            return Err(ProtocolError::ExpectedByte {
                expected: b'\n',
                got: second_byte,
            });
        }

        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_number_to_usize() {
        struct TestData {
            buffer: &'static [u8],
            expected: Result<usize, ProtocolError>,
        }

        let tests = vec![
            TestData {
                buffer: b"0\r\n",
                expected: Ok(0),
            },
            TestData {
                buffer: b"1\r\n",
                expected: Ok(1),
            },
            TestData {
                buffer: b"15\r\n",
                expected: Ok(15),
            },
            TestData {
                buffer: b"123\r\n",
                expected: Ok(123),
            },
            TestData {
                buffer: b"83210\r\n",
                expected: Ok(83210),
            },
            TestData {
                buffer: b"752019\r\n",
                expected: Ok(752019),
            },
            TestData {
                buffer: b"752019\r5",
                expected: Err(ProtocolError::ExpectedByte {
                    expected: b'\n',
                    got: b'5',
                }),
            },
            TestData {
                buffer: b"75f019\r\n",
                expected: Err(ProtocolError::UnexpectedByte(b'f')),
            },
        ];

        for test in tests {
            let mut parser = Parser::new(test.buffer);
            let result = parser.parse_number_to_usize();

            match (&result, &test.expected) {
                (Ok(val), Ok(expected)) => assert_eq!(val, expected),
                (Err(e), Err(expected)) => {
                    assert_eq!(e, expected)
                }
                _ => panic!("Expected {:?}, got {:?}", test.expected, result),
            }
        }
    }
}
