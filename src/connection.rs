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
        if let Some(wanted_length) = self.read_state.wanted_string_length {
            let result = self.try_extract_partial_string(wanted_length);

            if result == false {
                return false;
            }

            if self.read_state.wanted_strings_amount.is_none() {
                return true;
            }
        }

        if let Some(wanted_strings_amount) = self.read_state.wanted_strings_amount {
            return self.try_extract_partial_message_strings(wanted_strings_amount);
        }

        self.try_extract_new_message()
    }

    fn try_extract_new_message(&mut self) -> bool {
        if self.avaliable_bytes() < 4 {
            return false;
        }

        self.read_state.current_message_start = self.read_state.position;
        let amount_strings =
            Self::get_message_length(&self.read_state.buffer[self.read_state.position..]);

        self.read_state.current_message_bytes_length = HEADER_SIZE;
        self.read_state.position += HEADER_SIZE;

        self.try_extract_message_strings(amount_strings)
    }

    fn try_extract_message_strings(&mut self, amount_strings: usize) -> bool {
        for i in 0..amount_strings {
            let result = Self::try_extract_string(
                &self.read_state.buffer[self.read_state.position..self.read_state.bytes_filled],
                self.read_state.position,
            );

            match result {
                StringExtractionResult::Complete(indices, new_position) => {
                    let string_length = indices.1 - indices.0;

                    // should return error here if the string length is to much
                    if self.read_state.current_message_bytes_length + string_length > BUFFER_SIZE {
                        // self.clear_buffer(); clear the buffer here or something sinse this would
                        // be bigger than the max message size
                        return false;

                        // TODO - really the error under is the one that should be returned
                        // return Err(RedisError::MessageTooLarge);
                    }

                    self.read_state.current_message_bytes_length += string_length + HEADER_SIZE;
                    self.read_state.current_message.push(indices);
                    self.read_state.position = new_position;
                }
                StringExtractionResult::Partial(wanted_string_length) => {
                    self.read_state.wanted_strings_amount = Some(amount_strings - i);
                    self.read_state.wanted_string_length = Some(wanted_string_length);
                    self.read_state.current_message_bytes_length +=
                        HEADER_SIZE + wanted_string_length;
                    self.read_state.position += HEADER_SIZE;

                    // partial read and buffer is not big enough we shift it back
                    if BUFFER_SIZE - self.read_state.position < wanted_string_length {
                        self.shift_read_buffer(self.read_state.current_message_start);
                    }

                    return false;
                }
                StringExtractionResult::None => {
                    self.read_state.wanted_strings_amount = Some(amount_strings - i);

                    // partial read and buffer is not big enough we shift it back
                    if BUFFER_SIZE - self.read_state.position < HEADER_SIZE {
                        self.shift_read_buffer(self.read_state.current_message_start);
                    }

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

        if buffer.len() < HEADER_SIZE + string_length {
            return StringExtractionResult::Partial(string_length);
        }

        let start = offset + HEADER_SIZE;
        let end = offset + HEADER_SIZE + string_length;
        StringExtractionResult::Complete((start, end), offset + string_length + HEADER_SIZE)
    }

    fn try_extract_partial_string(&mut self, wanted_length: usize) -> bool {
        if self.avaliable_bytes() < wanted_length {
            return false;
        }

        let start = self.read_state.position;
        let end = start + wanted_length;

        self.read_state.current_message.push((start, end));
        self.read_state.position = end;

        self.read_state.wanted_string_length = None;
        self.decrement_wanted_strings();

        true
    }

    fn try_extract_partial_message_strings(&mut self, wanted_strings_amount: usize) -> bool {
        let result = self.try_extract_message_strings(wanted_strings_amount);
        if result == true {
            self.read_state.wanted_strings_amount = None;
            return true;
        }
        return false;
    }

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

    // Helpers

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

    #[inline]
    fn decrement_wanted_strings(&mut self) {
        match self.read_state.wanted_strings_amount {
            Some(amount) => {
                if amount == 1 {
                    self.read_state.wanted_strings_amount = None;
                } else {
                    self.read_state.wanted_strings_amount = Some(amount);
                }
            }
            None => {}
        }
    }

    #[inline]
    fn shift_read_buffer(&mut self, keep_from: usize) {
        debug_assert!(self.read_state.position >= keep_from);

        debug_assert!(
            self.read_state
                .current_message
                .iter()
                .all(|(s, e)| *s >= keep_from && *e >= keep_from)
        );

        debug_assert!(
            keep_from < self.read_state.bytes_filled,
            "TRYING TO SHIFT MORE BYTES THEN ARE READ IN THE READ BUFFER"
        );

        let leftover = self.read_state.bytes_filled - keep_from;

        self.read_state
            .buffer
            .copy_within(keep_from..self.read_state.bytes_filled, 0);
        self.read_state.bytes_filled = leftover;
        self.read_state.position = self.read_state.position - keep_from;
        self.read_state.current_message_start = 0;

        self.read_state
            .current_message
            .iter_mut()
            .for_each(|(start, end)| {
                *start -= keep_from;
                *end -= keep_from;
            });
    }
}

pub struct ReadState {
    pub buffer: [u8; BUFFER_SIZE],
    pub bytes_filled: usize,
    pub position: usize,
    pub wanted_string_length: Option<usize>,
    pub wanted_strings_amount: Option<usize>,
    pub current_message: Vec<(usize, usize)>,
    pub current_message_start: usize,
    pub current_message_bytes_length: usize,
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
            current_message_start: 0,
            current_message_bytes_length: 0,
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
            expected_message_bytes_length: usize,
            expected_wanted_string_length: Option<usize>,
            expected_wanted_strings_amount: Option<usize>,
        }

        let tests = vec![
            TestData {
                message: b"\x00\x00\x00\x01\x00\x00\x00\x0bhello world",
                expected_string: "hello world".to_string(),
                expected_result: true,
                expected_message_bytes_length: 19,
                expected_wanted_string_length: None,
                expected_wanted_strings_amount: None,
            },
            TestData {
                message: b"\x00\x00\x00\x02\x00\x00\x00\x05hello\x00\x00\x00\x05world",
                expected_string: "hello world".to_string(),
                expected_result: true,
                expected_message_bytes_length: 22,
                expected_wanted_string_length: None,
                expected_wanted_strings_amount: None,
            },
            TestData {
                message: b"\x00\x00\x00\x01\x00\x00\x00\x0bhello",
                expected_string: "".to_string(),
                expected_result: false,
                expected_message_bytes_length: 19,
                expected_wanted_string_length: Some(11),
                expected_wanted_strings_amount: Some(1),
            },
            TestData {
                message: b"\x00\x00\x00\x01\x00\x00\x00\x0b",
                expected_string: "".to_string(),
                expected_result: false,
                expected_message_bytes_length: 19,
                expected_wanted_string_length: Some(11),
                expected_wanted_strings_amount: Some(1),
            },
            TestData {
                message: b"\x00\x00\x00\x02\x00\x00\x00\x05hello",
                expected_string: "".to_string(),
                expected_result: false,
                expected_message_bytes_length: 13,
                expected_wanted_string_length: None,
                expected_wanted_strings_amount: Some(1),
            },
            TestData {
                message: b"\x00\x00\x00\x05",
                expected_string: "".to_string(),
                expected_result: false,
                expected_message_bytes_length: 4,
                expected_wanted_string_length: None,
                expected_wanted_strings_amount: Some(5),
            },
            TestData {
                message: b"\x00\x00\x00",
                expected_string: "".to_string(),
                expected_result: false,
                expected_message_bytes_length: 0,
                expected_wanted_string_length: None,
                expected_wanted_strings_amount: None,
            },
        ];

        let dummy_socket = Socket { fd: -1 };
        let mut test_connection = Connection::new(dummy_socket);

        for test in tests {
            put_new_message_in_read_buffer(&mut test_connection, test.message);
            let result = test_connection.try_extract_new_message();
            let result_string = match result {
                true => test_connection
                    .read_state
                    .current_message
                    .iter()
                    .map(|(start, end)| {
                        std::str::from_utf8(&test_connection.read_state.buffer[*start..*end])
                            .unwrap()
                    })
                    .collect::<Vec<&str>>()
                    .join(" "),
                false => "".to_string(),
            };
            let result_message_bytes_length =
                test_connection.read_state.current_message_bytes_length;
            let result_wanted_string_length = test_connection.read_state.wanted_string_length;
            let result_wanted_strings_amount = test_connection.read_state.wanted_strings_amount;

            assert_eq!(
                test.expected_result, result,
                "in test: {:?}\nexpected result: {}\ngot: {}",
                test.message, test.expected_result, result
            );

            assert_eq!(
                test.expected_string, result_string,
                "in test: {:?}\nexpected result: {}\ngot: {}",
                test.message, test.expected_string, result_string
            );

            assert_eq!(
                test.expected_message_bytes_length, result_message_bytes_length,
                "in test: {:?}\nexpected result: {}\ngot: {}",
                test.message, test.expected_message_bytes_length, result_message_bytes_length
            );

            assert_eq!(
                test.expected_wanted_string_length, result_wanted_string_length,
                "in test: {:?}\nexpected result: {:?}\ngot: {:?}",
                test.message, test.expected_wanted_string_length, result_wanted_string_length,
            );

            assert_eq!(
                test.expected_wanted_strings_amount, result_wanted_strings_amount,
                "in test: {:?}\nexpected result: {:?}\ngot: {:?}",
                test.message, test.expected_wanted_strings_amount, result_wanted_strings_amount,
            );
        }
    }

    #[test]
    fn test_try_extract_message_with_partial_reads() {
        struct TestData {
            messages: Vec<&'static [u8]>,
            expected_strings: Vec<&'static [u8]>,
        }

        let tests = vec![
            TestData {
                messages: vec![b"\x00\x00\x00\x01\x00\x00\x00\x05hello"],
                expected_strings: vec![b"hello"],
            },
            TestData {
                messages: vec![
                    b"\x00\x00\x00\x02\x00\x00\x00\x05hello",
                    b"\x00\x00\x00\x05world",
                ],
                expected_strings: vec![b"hello", b"world"],
            },
            TestData {
                messages: vec![
                    b"\x00\x00\x00\x02\x00\x00\x00\x05hello",
                    b"\x00\x00\x00\x05wo",
                    b"rld",
                ],
                expected_strings: vec![b"hello", b"world"],
            },
            TestData {
                messages: vec![
                    b"\x00\x00\x00\x02",
                    b"\x00\x00\x00\x05",
                    b"hello",
                    b"\x00\x00\x00\x05",
                    b"world",
                ],
                expected_strings: vec![b"hello", b"world"],
            },
            TestData {
                messages: vec![
                    b"\x00",
                    b"\x00",
                    b"\x00",
                    b"\x02",
                    b"\x00",
                    b"\x00",
                    b"\x00",
                    b"\x05",
                    b"he",
                    b"ll",
                    b"o",
                    b"\x00\x00",
                    b"\x00\x05",
                    b"w",
                    b"o",
                    b"r",
                    b"l",
                    b"d",
                ],
                expected_strings: vec![b"hello", b"world"],
            },
        ];

        let dummy_socket = Socket { fd: -1 };
        let mut test_connection = Connection::new(dummy_socket);

        for test in tests {
            reset_connection_read_buffer(&mut test_connection);

            for message in test.messages {
                append_to_read_buffer(&mut test_connection, message);
                test_connection.try_extract_message();
            }

            assert_eq!(
                test.expected_strings.len(),
                test_connection.read_state.current_message.len(),
                "message length is not correct"
            );

            for (expected_string, (start, end)) in test
                .expected_strings
                .iter()
                .zip(test_connection.read_state.current_message.iter())
            {
                let actual_string = &test_connection.read_state.buffer[*start..*end];
                assert_eq!(
                    *expected_string, actual_string,
                    "expected string: {:?}\ngot: {:?}\n",
                    *expected_string, actual_string,
                );
            }
        }
    }

    #[test]
    fn test_try_extract_message_with_partial_reads_on_full_buffer() {
        macro_rules! repeat_byte {
            ($byte:literal, $count:expr) => {{
                const BYTES: &[u8; $count] = &[$byte; $count];
                BYTES as &[u8]
            }};
        }

        struct TestData {
            messages: Vec<&'static [u8]>,
            expected_strings: Vec<&'static [u8]>,
        }

        let tests = vec![
            TestData {
                messages: vec![
                    b"\x00\x00\x00\x01\x00\x00\x0F\xF8",
                    repeat_byte!(b'a', BUFFER_SIZE - 12),
                    b"\x00\x00\x00\x02",
                    b"\x00\x00\x00\x05hello\x00\x00\x00\x05world",
                ],
                expected_strings: vec![b"hello", b"world"],
            },
            TestData {
                messages: vec![
                    b"\x00\x00\x00\x01\x00\x00\x0F\xF8",
                    repeat_byte!(b'a', BUFFER_SIZE - 12),
                    b"\x00\x00\x00\x02",
                    b"\x00\x00\x00\x05hello\x00\x00\x00\x05world",
                    b"\x00\x00\x00\x01\x00\x00\x0F\x7E",
                    repeat_byte!(b'a', BUFFER_SIZE - 34),
                    b"\x00\x00\x00\x02",
                    b"\x00\x00\x00\x05hello\x00\x00\x00\x05world",
                ],
                expected_strings: vec![b"hello", b"world"],
            },
            // TestData {
            //     messages: vec![
            //         b"\x00\x00\x00\x01\x00\x00\x0F\xF4",
            //         repeat_byte!(b'a', BUFFER_SIZE - 16),
            //         b"\x00\x00\x00\x02\x00\x00\x00\x05",
            //         b"hello\x00\x00\x00\x05world",
            //     ],
            //     expected_strings: vec![b"hello", b"world"],
            // },
            // TestData {
            //     messages: vec![
            //         b"\x00\x00\x00\x01\x00\x00\x0F\xF4",
            //         repeat_byte!(b'a', BUFFER_SIZE - 16),
            //         b"\x00\x00\x00\x02\x00\x00\x00\x05",
            //         b"hello\x00\x00\x00\x05world",
            //         b"\x00\x00\x00\x01\x00\x00\x0F\xF8",
            //         repeat_byte!(b'a', BUFFER_SIZE - 12),
            //         b"\x00\x00\x00\x02",
            //         b"\x00\x00\x00\x05hello\x00\x00\x00\x05world",
            //     ],
            //     expected_strings: vec![b"hello", b"world"],
            // },
            // TestData {
            //     messages: vec![
            //         b"\x00\x00\x00\x01\x00\x00\x0F\xFA",
            //         repeat_byte!(b'a', BUFFER_SIZE - 14),
            //         b"\x00\x00\x00\x01\x00\x00\x00\x05hel", // Split "hello"
            //         b"lo",
            //     ],
            //     expected_strings: vec![b"hello"],
            // },
        ];

        let dummy_socket = Socket { fd: -1 };
        let mut test_connection = Connection::new(dummy_socket);

        for test in tests {
            reset_connection_read_buffer(&mut test_connection);

            let mut string_indices_copy: Vec<(usize, usize)> = Vec::new();

            for message in test.messages {
                append_to_read_buffer(&mut test_connection, message);

                loop {
                    let result = test_connection.try_extract_message();
                    if result == true {
                        string_indices_copy = test_connection.read_state.current_message.clone();
                        test_connection.read_state.current_message.clear();
                    } else {
                        break;
                    }
                }
            }

            for (expected_string, (start, end)) in
                test.expected_strings.iter().zip(string_indices_copy.iter())
            {
                let actual_string = &test_connection.read_state.buffer[*start..*end];
                assert_eq!(
                    *expected_string, actual_string,
                    "expected string: {:?}\ngot: {:?}\n",
                    *expected_string, actual_string,
                );
            }
        }
    }

    // Test helpers

    fn put_new_message_in_read_buffer(connection: &mut Connection, message: &[u8]) {
        assert!(
            message.len() <= BUFFER_SIZE,
            "Test message too large for buffer"
        );
        reset_connection_read_buffer(connection);
        connection.read_state.buffer[..message.len()].copy_from_slice(message);
        connection.read_state.bytes_filled = message.len();
    }

    fn append_to_read_buffer(connection: &mut Connection, message: &[u8]) {
        assert!(
            message.len() <= BUFFER_SIZE - connection.read_state.bytes_filled,
            "Test message too large for buffer. Bytes left: {} got {}",
            BUFFER_SIZE - connection.read_state.bytes_filled,
            message.len(),
        );
        connection.read_state.buffer[connection.read_state.bytes_filled
            ..connection.read_state.bytes_filled + message.len()]
            .copy_from_slice(message);
        connection.read_state.bytes_filled += message.len();
    }

    fn reset_connection_read_buffer(connection: &mut Connection) {
        connection.read_state.bytes_filled = 0;
        connection.read_state.position = 0;
        connection.read_state.current_message_bytes_length = 0;
        connection.read_state.current_message.clear();
        connection.read_state.wanted_string_length = None;
        connection.read_state.wanted_strings_amount = None;
    }
}
