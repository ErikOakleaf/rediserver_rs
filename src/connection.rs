use crate::{
    error::{MAX_MESSAGE_SIZE, ProtocolError, RedisError},
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
        //     if message_extraction_result = false {
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

    fn try_extract_message(&mut self) -> bool {
        // TODO - add partial reads here

        self.try_extract_new_message()
    }

    fn try_extract_new_message(&mut self) -> bool {
        if self.avaliable_bytes() < 4 {
            return false;
        }

        let amount_strings =
            Self::get_message_length(&self.read_state.buffer[self.read_state.position..]);

        self.read_state.current_message_length = HEADER_SIZE;
        self.read_state.position += HEADER_SIZE;

        self.try_extract_message_strings(amount_strings)
    }

    fn try_extract_message_strings(&mut self, amount_strings: usize) -> bool {
        for i in 0..amount_strings {
            let result = Self::try_extract_string(
                &self.read_state.buffer[self.read_state.position..],
                self.read_state.position,
            );

            match result {
                StringExtractionResult::Complete(indices, new_position) => {
                    let string_length = indices.1 - indices.0;

                    // should return error here if the string length is to much
                    if self.read_state.current_message_length + string_length > BUFFER_SIZE {
                        // self.clear_buffer(); clear the buffer here or something sinse this would
                        // be bigger than the max message size
                        return false;

                        // TODO - really the error under is the one that should be returned
                        // return Err(RedisError::MessageTooLarge);
                    }

                    self.read_state.current_message_length += string_length + HEADER_SIZE;
                    self.read_state.current_message.push(indices);
                    self.read_state.position = new_position;
                }
                StringExtractionResult::Partial(wanted_string_length) => {
                    self.read_state.wanted_strings_amount = Some(amount_strings - i);
                    self.read_state.wanted_string_length = Some(wanted_string_length);
                    return false;
                }
                StringExtractionResult::None => {
                    self.read_state.wanted_strings_amount = Some(amount_strings - i);
                    return false;
                }
            }
        }

        true
    }

    fn try_extract_string(buffer: &[u8], offset: usize) -> StringExtractionResult {
        if buffer.len() < HEADER_SIZE {
            return StringExtractionResult::None;
        }

        let string_length = Self::get_message_length(buffer);

        if buffer.len() - HEADER_SIZE < string_length {
            return StringExtractionResult::Partial(string_length);
        }

        let start = offset + HEADER_SIZE;
        let end = offset + HEADER_SIZE + string_length;
        StringExtractionResult::Complete((start, end), offset + string_length + HEADER_SIZE)
    }

    // pub fn handle_readable(&mut self) -> Result<ConnectionAction, RedisError> {
    //     self.fill_read_buffer()?;
    //     let (start, end) = match self.try_extract_message() {
    //         Some((start, end)) => (start, end),
    //         None => return Ok(ConnectionAction::None),
    //     };
    //
    //     self.handle_message(start, end)
    // }

    pub fn handle_writeable(&mut self) -> Result<ConnectionAction, RedisError> {
        self.try_write_after_writable()
    }

    //
    // fn try_extract_message(&mut self) -> Option<(usize, usize)> {
    //     if let Some(message) = self.try_extract_partial_message() {
    //         return Some(message);
    //     }
    //
    //     if self.avaliable_bytes() < 4 {
    //         self.reset_read_buffer_if_needed();
    //         return None;
    //     }
    //
    //     self.try_extract_new_message()
    // }
    //
    // fn try_extract_new_message(&mut self) -> Option<(usize, usize)> {
    //     let mut start = self.read_state.position;
    //     let length_slice = &self.read_state.buffer[start..start + 4];
    //     let length = Self::get_message_length(length_slice).unwrap();
    //
    //     let leftover = self.avaliable_bytes();
    //
    //     if leftover < length {
    //         self.shift_read_buffer_and_remember_partial(length, leftover);
    //         return None;
    //     }
    //
    //     start += 4;
    //     let end = start + length;
    //     self.read_state.position = end;
    //
    //     Some((start, end))
    // }
    //
    // fn try_extract_partial_message(&mut self) -> Option<(usize, usize)> {
    //     let length = match self.read_state.wanted_length {
    //         Some(length) => length,
    //         None => return None,
    //     };
    //
    //     if self.avaliable_bytes() < length {
    //         return None;
    //     }
    //
    //     let start = self.read_state.position;
    //     let end = start + length;
    //
    //     self.read_state.position = end;
    //     self.read_state.wanted_length = None;
    //
    //     Some((start, end))
    // }
    //
    // fn handle_message(&mut self, start: usize, end: usize) -> Result<ConnectionAction, RedisError> {
    //     let message = &self.read_state.buffer[start..end];
    //     let s = std::str::from_utf8(message).unwrap().to_string();
    //
    //     println!("client says {}", s);
    //
    //     let response = b"world";
    //
    //     self.prepare_response(response);
    //
    //     self.try_write_after_read()
    // }

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

    // Helpers

    // #[inline(always)]
    // fn reset_read_buffer_if_needed(&mut self) {
    //     if self.read_state.position > 0 && self.read_state.position == self.read_state.bytes_filled
    //     {
    //         self.read_state.bytes_filled = 0;
    //         self.read_state.position = 0;
    //     }
    // }
    //
    // #[inline]
    // fn shift_read_buffer_and_remember_partial(&mut self, length: usize, leftover: usize) {
    //     self.read_state.buffer.copy_within(
    //         self.read_state.position + 4..self.read_state.bytes_filled,
    //         0,
    //     );
    //     self.read_state.wanted_length = Some(length);
    //     self.read_state.bytes_filled = leftover - 4;
    //     self.read_state.position = 0;
    // }

    #[inline(always)]
    fn avaliable_bytes(&self) -> usize {
        self.read_state.bytes_filled - self.read_state.position
    }

    #[inline]
    fn get_message_length(buffer: &[u8]) -> usize {
        debug_assert!(
            buffer.len() >= 4,
            "BUFFER IS NOT LONG ENOUGH TO HAVE A LENGTH HEADER"
        );

        let length = Self::u32_from_be_bytes(&buffer[..4]) as usize;

        length
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
    pub wanted_string_length: Option<usize>,
    pub wanted_strings_amount: Option<usize>,
    pub current_message: Vec<(usize, usize)>,
    pub current_message_length: usize,
}

impl ReadState {
    pub fn new() -> Self {
        ReadState {
            buffer: [0u8; BUFFER_SIZE],
            bytes_filled: 0,
            position: 0,
            wanted_string_length: None,
            wanted_strings_amount: None,
            current_message: Vec::<(usize, usize)>::new(),
            current_message_length: 0,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_try_extract_string() {
        struct TestData {
            string: &'static [u8],
            offset: usize,
            expected: StringExtractionResult,
        }

        let tests = vec![
            TestData {
                string: b"\x00\x00\x00\x05hello",
                offset: 0,
                expected: StringExtractionResult::Complete((4, 9), 9),
            },
            TestData {
                string: b"\x00\x00\x00\x0bhello world",
                offset: 5,
                expected: StringExtractionResult::Complete((9, 20), 20),
            },
            TestData {
                string: b"\x00\x00\x00\x0bhello world\x00\x00\x00\x05hello",
                offset: 15,
                expected: StringExtractionResult::Complete((19, 30), 30),
            },
            TestData {
                string: b"\x00\x00\x00\x0bhello",
                offset: 0,
                expected: StringExtractionResult::Partial(11),
            },
            TestData {
                string: b"\x00\x00\x00\x0bworld",
                offset: 999,
                expected: StringExtractionResult::Partial(11),
            },
            TestData {
                string: b"\x00\x00\x00\x0b",
                offset: 999,
                expected: StringExtractionResult::Partial(11),
            },
            TestData {
                string: b"\x00\x00\x00",
                offset: 0,
                expected: StringExtractionResult::None,
            },
            TestData {
                string: b"\x00\x00",
                offset: 50,
                expected: StringExtractionResult::None,
            },
            TestData {
                string: b"\x00",
                offset: 100,
                expected: StringExtractionResult::None,
            },
        ];

        for test in tests {
            let result = Connection::try_extract_string(&test.string, test.offset);
            assert_eq!(
                test.expected, result,
                "for test: {:?}\nexpecetd: {:?}\ngot: {:?}",
                test.string, test.expected, result
            );
        }
    }

    #[test]
    fn test_try_extract_new_message() {
        struct TestData {
            message: &'static [u8],
            expected_string: String,
            expected_result: bool,
            expected_message_length: usize,
            expected_wanted_string_length: Option<usize>,
            expected_wanted_strings_amount: Option<usize>,
        }

        let tests = vec![
            
        ];
    }

    // #[test]
    // fn test_try_extract_message() {
    //     struct TestData {
    //         prefix_short_1: Option<u16>,
    //         prefix_short_2: Option<u16>,
    //         message: &'static [u8],
    //         expected: Option<&'static [u8]>,
    //     }
    //
    //     let tests = vec![
    //         TestData {
    //             prefix_short_1: Some(0),
    //             prefix_short_2: Some(5),
    //             message: b"hello",
    //             expected: Some(b"hello"),
    //         },
    //         TestData {
    //             prefix_short_1: Some(0),
    //             prefix_short_2: Some(5),
    //             message: b"world",
    //             expected: Some(b"world"),
    //         },
    //         TestData {
    //             prefix_short_1: Some(0),
    //             prefix_short_2: Some(11),
    //             message: b"hello",
    //             expected: None,
    //         },
    //         TestData {
    //             prefix_short_1: None,
    //             prefix_short_2: None,
    //             message: b" world",
    //             expected: Some(b"hello world"),
    //         },
    //         TestData {
    //             prefix_short_1: Some(0),
    //             prefix_short_2: None,
    //             message: &[],
    //             expected: None,
    //         },
    //         TestData {
    //             prefix_short_1: None,
    //             prefix_short_2: Some(5),
    //             message: b"hello",
    //             expected: Some(b"hello"),
    //         },
    //         TestData {
    //             prefix_short_1: Some(0),
    //             prefix_short_2: None,
    //             message: &[],
    //             expected: None,
    //         },
    //         TestData {
    //             prefix_short_1: None,
    //             prefix_short_2: Some(5),
    //             message: &[],
    //             expected: None,
    //         },
    //         TestData {
    //             prefix_short_1: None,
    //             prefix_short_2: None,
    //             message: b"hel",
    //             expected: None,
    //         },
    //         TestData {
    //             prefix_short_1: None,
    //             prefix_short_2: None,
    //             message: b"lo",
    //             expected: Some(b"hello"),
    //         },
    //         TestData {
    //             prefix_short_1: Some(0),
    //             prefix_short_2: Some(5),
    //             message: b"hello\x00\x00\x00\x05world", // Two messages back-to-back
    //             expected: Some(b"hello"),
    //         },
    //         TestData {
    //             prefix_short_1: None,
    //             prefix_short_2: None,
    //             message: &[],
    //             expected: Some(b"world"),
    //         },
    //         TestData {
    //             prefix_short_1: Some(0),
    //             prefix_short_2: Some(5),
    //             message: b"hello\x00\x00\x00\x05wor", // one and a half messages back-to-back
    //             expected: Some(b"hello"),
    //         },
    //         TestData {
    //             prefix_short_1: None,
    //             prefix_short_2: None,
    //             message: b"ld",
    //             expected: Some(b"world"),
    //         },
    //         TestData {
    //             prefix_short_1: Some(0),
    //             prefix_short_2: None,
    //             message: &[],
    //             expected: None,
    //         },
    //         TestData {
    //             prefix_short_1: None,
    //             prefix_short_2: Some(5),
    //             message: &[],
    //             expected: None,
    //         },
    //         TestData {
    //             prefix_short_1: None,
    //             prefix_short_2: None,
    //             message: b"hello",
    //             expected: Some(b"hello"),
    //         },
    //         TestData {
    //             prefix_short_1: None,
    //             prefix_short_2: None,
    //             message: &[0], // sending 1 then 3 bytes for the length
    //             expected: None,
    //         },
    //         TestData {
    //             prefix_short_1: None,
    //             prefix_short_2: None,
    //             message: &[0, 0, 5],
    //             expected: None,
    //         },
    //         TestData {
    //             prefix_short_1: None,
    //             prefix_short_2: None,
    //             message: b"hello",
    //             expected: Some(b"hello"),
    //         },
    //         TestData {
    //             prefix_short_1: None,
    //             prefix_short_2: None,
    //             message: &[0, 0, 0], // sending 3 then 1 bytes for the length
    //             expected: None,
    //         },
    //         TestData {
    //             prefix_short_1: None,
    //             prefix_short_2: None,
    //             message: &[5],
    //             expected: None,
    //         },
    //         TestData {
    //             prefix_short_1: None,
    //             prefix_short_2: None,
    //             message: b"world",
    //             expected: Some(b"world"),
    //         },
    //     ];
    //
    //     let placeholder_socket = Socket::new_tcp();
    //     let mut dummy_connection = Connection::new(placeholder_socket);
    //
    //     let mut i = 1;
    //     for test in tests {
    //         let mut combined_vec: Vec<u8> = Vec::new();
    //
    //         if let Some(short1) = test.prefix_short_1 {
    //             combined_vec.extend_from_slice(&short1.to_be_bytes());
    //         }
    //
    //         if let Some(short2) = test.prefix_short_2 {
    //             combined_vec.extend_from_slice(&short2.to_be_bytes());
    //         }
    //
    //         combined_vec.extend_from_slice(test.message);
    //
    //         for (i, &byte) in combined_vec.iter().enumerate() {
    //             dummy_connection.read_state.buffer[dummy_connection.read_state.bytes_filled + i] =
    //                 byte;
    //         }
    //         dummy_connection.read_state.bytes_filled += combined_vec.len();
    //
    //         let result: Option<(usize, usize)> = dummy_connection.try_extract_message();
    //         let result_slice =
    //             result.map(|(start, end)| &dummy_connection.read_state.buffer[start..end]);
    //
    //         let format_output = |opt: &Option<&[u8]>| -> String {
    //             match opt {
    //                 Some(bytes) => String::from_utf8_lossy(bytes).to_string(),
    //                 None => "None".to_string(),
    //             }
    //         };
    //
    //         assert_eq!(
    //             test.expected,
    //             result_slice,
    //             "in test {}\nexpected: {}\ngot: {}\n",
    //             i,
    //             format_output(&test.expected),
    //             format_output(&result_slice)
    //         );
    //
    //         i += 1;
    //     }
    // }
}
