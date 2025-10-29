use crate::{
    commands::RedisCommand,
    connection::{BUFFER_SIZE, HEADER_SIZE},
    error::RedisError,
};

#[derive(Debug, PartialEq)]
enum StringExtractionResult {
    Complete((usize, usize), usize),
    Partial(usize),
    None,
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

    pub fn try_extract_message(&mut self) -> bool {
        if let Some(wanted_length) = self.wanted_string_length {
            let result = self.try_extract_partial_string(wanted_length);

            if result == false {
                return false;
            }

            if self.wanted_strings_amount.is_none() {
                return true;
            }
        }

        if let Some(wanted_strings_amount) = self.wanted_strings_amount {
            return self.try_extract_partial_message_strings(wanted_strings_amount);
        }

        self.try_extract_new_message()
    }

    fn try_extract_new_message(&mut self) -> bool {
        if self.avaliable_bytes() < 4 {
            // shift here if we have a partially read amount strings header on a full buffer
            if BUFFER_SIZE - self.position < 4 {
                self.shift_read_buffer(self.position);
            }

            return false;
        }

        self.current_message_start = self.position;
        let amount_strings = Self::get_message_length(&self.buffer[self.position..]);

        self.current_message_bytes_length = HEADER_SIZE;
        self.position += HEADER_SIZE;

        self.try_extract_message_strings(amount_strings)
    }

    fn try_extract_message_strings(&mut self, amount_strings: usize) -> bool {
        for i in 0..amount_strings {
            let result = Self::try_extract_string(
                &self.buffer[self.position..self.bytes_filled],
                self.position,
            );

            match result {
                StringExtractionResult::Complete(indices, new_position) => {
                    let string_length = indices.1 - indices.0;

                    // should return error here if the string length is to much
                    if self.current_message_bytes_length + string_length > BUFFER_SIZE {
                        // self.clear_buffer(); clear the buffer here or something sinse this would
                        // be bigger than the max message size
                        return false;

                        // TODO - really the error under is the one that should be returned
                        // return Err(RedisError::MessageTooLarge);
                    }

                    self.current_message_bytes_length += string_length + HEADER_SIZE;
                    self.current_message.push(indices);
                    self.position = new_position;
                }
                StringExtractionResult::Partial(wanted_string_length) => {
                    self.wanted_strings_amount = Some(amount_strings - i);
                    self.wanted_string_length = Some(wanted_string_length);
                    self.current_message_bytes_length += HEADER_SIZE + wanted_string_length;
                    self.position += HEADER_SIZE;

                    // partial read and buffer is not big enough we shift it back
                    if BUFFER_SIZE - self.position < wanted_string_length {
                        self.shift_read_buffer(self.current_message_start);
                    }

                    return false;
                }
                StringExtractionResult::None => {
                    self.wanted_strings_amount = Some(amount_strings - i);

                    // partial read and buffer is not big enough we shift it back
                    if BUFFER_SIZE - self.position < HEADER_SIZE {
                        self.shift_read_buffer(self.current_message_start);
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

        let start = self.position;
        let end = start + wanted_length;

        self.current_message.push((start, end));
        self.position = end;

        self.wanted_string_length = None;
        self.decrement_wanted_strings();

        true
    }

    fn try_extract_partial_message_strings(&mut self, wanted_strings_amount: usize) -> bool {
        let result = self.try_extract_message_strings(wanted_strings_amount);
        if result == true {
            self.wanted_strings_amount = None;
            return true;
        }
        return false;
    }

    pub fn get_commands<'a>(&'a mut self) -> Result<Option<Vec<RedisCommand<'a>>>, RedisError> {
        let result = self.try_extract_message();
        if result {
            Ok(Some(Self::parse_message(
                &self.current_message,
                &self.buffer,
            )?))
        } else {
            Ok(None)
        }
    }

    fn parse_message<'a>(
        message: &[(usize, usize)],
        buffer: &'a [u8],
    ) -> Result<Vec<RedisCommand<'a>>, RedisError> {
        let mut commands = Vec::<RedisCommand>::new();
        let mut strings_consumed = 0;

        while strings_consumed < message.len() {
            let (command, amount_strings_consumed) =
                Self::parse_command(&message[strings_consumed..], buffer)?;
            commands.push(command);
            strings_consumed += amount_strings_consumed;
        }

        Ok(commands)
    }

    fn parse_command<'a>(
        message: &[(usize, usize)],
        buffer: &'a [u8],
    ) -> Result<(RedisCommand<'a>, usize), RedisError> {
        let slice = |(start, end): (usize, usize)| &buffer[start..end];
        let cmd = slice(message[0]);

        match cmd {
            b"GET" | b"get" | b"Get" => {
                Self::check_arity_error(2, message.len(), cmd)?;
                let key = slice(message[1]);
                Ok((RedisCommand::Get { key }, 2))
            }
            b"DEL" | b"del" | b"Del" => {
                Self::check_arity_error(2, message.len(), cmd)?;
                let key = slice(message[1]);
                Ok((RedisCommand::Del { key }, 2))
            }
            b"SET" | b"set" | b"Set" => {
                Self::check_arity_error(3, message.len(), cmd)?;
                let key = slice(message[1]);
                let value = slice(message[2]);
                Ok((RedisCommand::Set { key, value }, 3))
            }
            _ => Err(RedisError::UnknownCommand(
                String::from_utf8_lossy(cmd).to_string(),
            )),
        }
    }

    // Helpers

    #[inline(always)]
    fn avaliable_bytes(&self) -> usize {
        self.bytes_filled - self.position
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
        match self.wanted_strings_amount {
            Some(amount) => {
                if amount <= 1 {
                    self.wanted_strings_amount = None;
                } else {
                    self.wanted_strings_amount = Some(amount - 1);
                }
            }
            None => {}
        }
    }

    #[inline]
    fn shift_read_buffer(&mut self, keep_from: usize) {
        debug_assert!(self.position >= keep_from);

        debug_assert!(
            self.current_message
                .iter()
                .all(|(s, e)| *s >= keep_from && *e >= keep_from),
            "current_message contains ranges before keep_from boundary: keep_from={}, current_message={:?}",
            keep_from,
            self.current_message
        );

        debug_assert!(
            keep_from < self.bytes_filled,
            "TRYING TO SHIFT MORE BYTES THEN ARE READ IN THE READ BUFFER"
        );

        let leftover = self.bytes_filled - keep_from;

        self.buffer.copy_within(keep_from..self.bytes_filled, 0);
        self.bytes_filled = leftover;
        self.position = self.position - keep_from;
        self.current_message_start = 0;

        self.current_message.iter_mut().for_each(|(start, end)| {
            *start -= keep_from;
            *end -= keep_from;
        });
    }

    #[inline(always)]
    fn check_arity_error(
        expected_length: usize,
        message_length: usize,
        cmd: &[u8],
    ) -> Result<(), RedisError> {
        if message_length < expected_length {
            return Err(RedisError::WrongArity(
                String::from_utf8_lossy(cmd).to_string(),
            ));
        }
        Ok(())
    }
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
            let result = ReadState::try_extract_string(&test.string, test.offset);
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

        let mut read_state = ReadState::new();

        for test in tests {
            put_new_message_in_read_buffer(&mut read_state, test.message);
            let result = read_state.try_extract_new_message();
            let result_string = match result {
                true => read_state
                    .current_message
                    .iter()
                    .map(|(start, end)| {
                        std::str::from_utf8(&read_state.buffer[*start..*end]).unwrap()
                    })
                    .collect::<Vec<&str>>()
                    .join(" "),
                false => "".to_string(),
            };
            let result_message_bytes_length = read_state.current_message_bytes_length;
            let result_wanted_string_length = read_state.wanted_string_length;
            let result_wanted_strings_amount = read_state.wanted_strings_amount;

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

        let mut read_state = ReadState::new();
        for test in tests {
            reset_read_buffer(&mut read_state);

            for message in test.messages {
                append_to_read_buffer(&mut read_state, message);
                read_state.try_extract_message();
            }

            assert_eq!(
                test.expected_strings.len(),
                read_state.current_message.len(),
                "message length is not correct"
            );

            for (expected_string, (start, end)) in test
                .expected_strings
                .iter()
                .zip(read_state.current_message.iter())
            {
                let actual_string = &read_state.buffer[*start..*end];
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
            message_buffer: MessageBuffer,
            expected_strings: Vec<&'static [u8]>,
        }

        let tests = vec![
            TestData {
                message_buffer: MessageBuffer {
                    bytes: {
                        let mut bytes = Vec::new();
                        bytes.extend_from_slice(b"\x00\x00\x00\x01\x00\x00\x0F\xF8");
                        bytes.extend_from_slice(&repeat_byte!(b'a', BUFFER_SIZE - 12));
                        bytes.extend_from_slice(b"\x00\x00\x00\x02");
                        bytes.extend_from_slice(b"\x00\x00\x00\x05hello\x00\x00\x00\x05world");
                        bytes
                    },
                    position: 0,
                },
                expected_strings: vec![b"hello", b"world"],
            },
            TestData {
                message_buffer: MessageBuffer {
                    bytes: {
                        let mut bytes = Vec::new();
                        bytes.extend_from_slice(b"\x00\x00\x00\x01\x00\x00\x0F\xFA");
                        bytes.extend_from_slice(&repeat_byte!(b'a', BUFFER_SIZE - 10));
                        bytes.extend_from_slice(b"\x00\x00");
                        bytes.extend_from_slice(b"\x00\x02");
                        bytes.extend_from_slice(b"\x00\x00\x00\x05hello\x00\x00\x00\x05world");
                        bytes
                    },
                    position: 0,
                },
                expected_strings: vec![b"hello", b"world"],
            },
            TestData {
                message_buffer: MessageBuffer {
                    bytes: {
                        let mut bytes = Vec::new();
                        bytes.extend_from_slice(b"\x00\x00\x00\x01\x00\x00\x0F\xF4");
                        bytes.extend_from_slice(&repeat_byte!(b'a', BUFFER_SIZE - 16));
                        bytes.extend_from_slice(b"\x00\x00\x00\x02\x00\x00\x00\x05");
                        bytes.extend_from_slice(b"hello\x00\x00\x00\x05world");
                        bytes
                    },
                    position: 0,
                },
                expected_strings: vec![b"hello", b"world"],
            },
            TestData {
                message_buffer: MessageBuffer {
                    bytes: {
                        let mut bytes = Vec::new();
                        bytes.extend_from_slice(b"\x00\x00\x00\x01\x00\x00\x0F\xF6");
                        bytes.extend_from_slice(&repeat_byte!(b'a', BUFFER_SIZE - 14));
                        bytes.extend_from_slice(b"\x00\x00\x00\x02\x00\x00");
                        bytes.extend_from_slice(b"\x00\x05");
                        bytes.extend_from_slice(b"hello\x00\x00\x00\x05world");
                        bytes
                    },
                    position: 0,
                },
                expected_strings: vec![b"hello", b"world"],
            },
            // Same tests as above but repeated twice so two shifts would have to happen here
            TestData {
                message_buffer: MessageBuffer {
                    bytes: {
                        let mut bytes = Vec::new();
                        bytes.extend_from_slice(b"\x00\x00\x00\x01\x00\x00\x0F\xF8");
                        bytes.extend_from_slice(&repeat_byte!(b'a', BUFFER_SIZE - 12));
                        bytes.extend_from_slice(b"\x00\x00\x00\x02");
                        bytes.extend_from_slice(b"\x00\x00\x00\x05hello\x00\x00\x00\x05world");
                        bytes.extend_from_slice(b"\x00\x00\x00\x01\x00\x00\x0F\xE2");
                        bytes.extend_from_slice(&repeat_byte!(b'a', BUFFER_SIZE - 34));
                        bytes.extend_from_slice(b"\x00\x00\x00\x02");
                        bytes.extend_from_slice(b"\x00\x00\x00\x05hello\x00\x00\x00\x05world");
                        bytes
                    },
                    position: 0,
                },
                expected_strings: vec![b"hello", b"world"],
            },
            TestData {
                message_buffer: MessageBuffer {
                    bytes: {
                        let mut bytes = Vec::new();
                        bytes.extend_from_slice(b"\x00\x00\x00\x01\x00\x00\x0F\xF4");
                        bytes.extend_from_slice(&repeat_byte!(b'a', BUFFER_SIZE - 16));
                        bytes.extend_from_slice(b"\x00\x00\x00\x02\x00\x00\x00\x05");
                        bytes.extend_from_slice(b"hello\x00\x00\x00\x05world");
                        bytes.extend_from_slice(b"\x00\x00\x00\x01\x00\x00\x0F\xEE");
                        bytes.extend_from_slice(&repeat_byte!(b'a', BUFFER_SIZE - 22));
                        bytes.extend_from_slice(b"\x00\x00\x00\x02\x00\x00\x00\x05");
                        bytes.extend_from_slice(b"hello\x00\x00\x00\x05world");
                        bytes
                    },
                    position: 0,
                },
                expected_strings: vec![b"hello", b"world"],
            },
        ];

        let mut read_state = ReadState::new();
        let mut string_indices_copy = Vec::<(usize, usize)>::new();

        for mut test in tests {
            reset_read_buffer(&mut read_state);

            loop {
                append_to_read_buffer_from_message_buffer(
                    &mut read_state,
                    &mut test.message_buffer,
                );

                let result = read_state.try_extract_message();

                if result == true {
                    string_indices_copy = read_state.current_message.clone();
                    read_state.current_message.clear();
                }

                if test.message_buffer.bytes.len() == test.message_buffer.position {
                    break;
                }
            }

            for (expected_strings, (start, end)) in
                test.expected_strings.iter().zip(string_indices_copy.iter())
            {
                let actual_string = &read_state.buffer[*start..*end];
                assert_eq!(
                    *expected_strings, actual_string,
                    "expected string: {:?}\ngot: {:?}\n",
                    *expected_strings, actual_string,
                );
            }
        }
    }

    #[test]
    fn test_parse_message_to_command() -> Result<(), RedisError> {
        struct TestData<'a> {
            buffer: &'static [u8],
            message: Vec<(usize, usize)>,
            expected_commands: Vec<RedisCommand<'a>>,
        }

        let tests = vec![
            TestData {
                buffer: b"\x00\x00\x00\x03\x00\x00\x00\x03GET\x00\x00\x00\x05hello",
                message: vec![(8, 11), (15, 20)],
                expected_commands: vec![RedisCommand::Get { key: b"hello" }],
            },
            TestData {
                buffer: b"\x00\x00\x00\x03\x00\x00\x00\x03DEL\x00\x00\x00\x05world",
                message: vec![(8, 11), (15, 20)],
                expected_commands: vec![RedisCommand::Del { key: b"world" }],
            },
            TestData {
                buffer:
                    b"\x00\x00\x00\x03\x00\x00\x00\x03SET\x00\x00\x00\x05hello\x00\x00\x00\x05world",
                message: vec![(8, 11), (15, 20), (24, 29)],
                expected_commands: vec![RedisCommand::Set {
                    key: b"hello",
                    value: b"world",
                }],
            },
            TestData {
                buffer: b"\x00\x00\x00\x06\x00\x00\x00\x03GET\x00\x00\x00\x05hello\x00\x00\x00\x03DEL\x00\x00\x00\x05world\x00\x00\x00\x03SET\x00\x00\x00\x05hello\x00\x00\x00\x05world",
                message: vec![(8, 11), (15, 20), (24, 27), (31, 36), (40, 43), (47, 52), (56, 61)],
                expected_commands: vec![RedisCommand::Get { key: b"hello" }, RedisCommand::Del { key: b"world" }, RedisCommand::Set {
                    key: b"hello",
                    value: b"world",
                }],
            },
        ];

        for test in tests {
            let result = ReadState::parse_message(&test.message, test.buffer)?;
            assert_eq!(
                test.expected_commands, result,
                "expected commands: {:?}\ngot: {:?}",
                test.expected_commands, result
            );
        }

        Ok(())
    }

    // Test helpers

    struct MessageBuffer {
        bytes: Vec<u8>,
        position: usize,
    }

    fn put_new_message_in_read_buffer(read_state: &mut ReadState, message: &[u8]) {
        assert!(
            message.len() <= BUFFER_SIZE,
            "Test message too large for buffer"
        );
        reset_read_buffer(read_state);
        read_state.buffer[..message.len()].copy_from_slice(message);
        read_state.bytes_filled = message.len();
    }

    fn append_to_read_buffer(read_state: &mut ReadState, message: &[u8]) {
        assert!(
            message.len() <= BUFFER_SIZE - read_state.bytes_filled,
            "Test message too large for buffer. Bytes left: {} got {}",
            BUFFER_SIZE - read_state.bytes_filled,
            message.len(),
        );
        read_state.buffer[read_state.bytes_filled..read_state.bytes_filled + message.len()]
            .copy_from_slice(message);
        read_state.bytes_filled += message.len();
    }

    fn append_to_read_buffer_from_message_buffer(
        read_state: &mut ReadState,
        message_buffer: &mut MessageBuffer,
    ) {
        let space_available = BUFFER_SIZE - read_state.bytes_filled;
        let bytes_remaining = message_buffer.bytes.len() - message_buffer.position;
        let bytes_to_copy = space_available.min(bytes_remaining);

        read_state.buffer[read_state.bytes_filled..read_state.bytes_filled + bytes_to_copy]
            .copy_from_slice(
                &message_buffer.bytes
                    [message_buffer.position..message_buffer.position + bytes_to_copy],
            );

        // Update both positions
        read_state.bytes_filled += bytes_to_copy;
        message_buffer.position += bytes_to_copy;
    }

    fn reset_read_buffer(read_state: &mut ReadState) {
        read_state.bytes_filled = 0;
        read_state.position = 0;
        read_state.current_message_bytes_length = 0;
        read_state.current_message.clear();
        read_state.wanted_string_length = None;
        read_state.wanted_strings_amount = None;
    }
}
