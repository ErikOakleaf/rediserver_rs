use crate::error::ProtocolError;

#[derive(PartialEq, Eq, Debug)]
pub enum ParseState {
    Empty,
    Partial,
    Complete,
}

#[derive(PartialEq, Eq, Debug)]
pub struct CommandParseState {
    command_name: Option<Vec<u8>>, // change this to an enum later
    args: Vec<Vec<u8>>,
    expected_args: usize,
    current_arg: usize,
    state: ParseState,
}

impl CommandParseState {
    pub fn new() -> CommandParseState {
        CommandParseState {
            command_name: None,
            args: Vec::<Vec<u8>>::new(),
            expected_args: 0,
            current_arg: 0,
            state: ParseState::Empty,
        }
    }

    pub fn clear(command_parse_state: &mut CommandParseState) {
        command_parse_state.command_name = None;
        command_parse_state.args.clear();
        command_parse_state.expected_args = 0;
        command_parse_state.current_arg = 0;
        command_parse_state.state = ParseState::Empty;
    }
}

fn parse_command(
    buf: &[u8],
    pos: &mut usize,
    command_parse_state: &mut CommandParseState,
) -> Result<(), ProtocolError> {
    let amount_strings = parse_array_header(buf, pos)?;

    let command = parse_bulk_string(buf, pos)?;
    let amount_arguments = get_command_arguments_amount(command)?;

    if amount_strings - 1 != amount_arguments {
        return Err(ProtocolError::WrongNumberOfArguments);
    }

    command_parse_state.command_name = Some(command.to_vec());
    command_parse_state.expected_args = amount_arguments;

    command_parse_state.state = ParseState::Partial;

    for _ in 0..amount_strings - 1 {
        let argument = parse_bulk_string(buf, pos)?;
        command_parse_state.current_arg += 1;
        command_parse_state.args.push(argument.to_vec());
    }

    command_parse_state.state = ParseState::Complete;
    Ok(())
}

fn parse_partial_command(
    buf: &[u8],
    pos: &mut usize,
    command_parse_state: &mut CommandParseState,
) -> Result<(), ProtocolError> {
    let amount_arguments = command_parse_state.expected_args - command_parse_state.current_arg;

    for _ in 0..amount_arguments {
        let argument = parse_bulk_string(buf, pos)?;
        command_parse_state.current_arg += 1;
        command_parse_state.args.push(argument.to_vec());
    }

    command_parse_state.state = ParseState::Complete;
    Ok(())
}

fn parse_array_header(buf: &[u8], pos: &mut usize) -> Result<usize, ProtocolError> {
    let start = *pos;
    expect(buf, pos, b'*')?;
    let array_len = match parse_number_to_usize(buf, pos) {
        Ok(len) => len,
        Err(ProtocolError::Incomplete) => {
            *pos = start;
            return Err(ProtocolError::Incomplete);
        }
        Err(e) => return Err(e),
    };

    if *pos + array_len + 2 > buf.len() {
        *pos = start;
        return Err(ProtocolError::Incomplete); // partial read
    }

    Ok(array_len)
}

fn parse_bulk_string<'a>(buf: &'a [u8], pos: &mut usize) -> Result<&'a [u8], ProtocolError> {
    let start = *pos;
    expect(buf, pos, b'$')?;

    let string_len = match parse_number_to_usize(buf, pos) {
        Ok(len) => len,
        Err(ProtocolError::Incomplete) => {
            *pos = start;
            return Err(ProtocolError::Incomplete);
        }
        Err(e) => return Err(e),
    };

    if *pos + string_len + 2 > buf.len() {
        *pos = start;
        return Err(ProtocolError::Incomplete); // partial read
    }

    let slice = &buf[*pos..*pos + string_len];
    *pos += string_len;

    check_crlf(buf, pos)?;

    Ok(slice)
}

// helpers

#[inline(always)]
fn peek(buf: &[u8], pos: usize) -> u8 {
    buf[pos]
}

#[inline(always)]
fn consume(buf: &[u8], pos: &mut usize) -> Result<u8, ProtocolError> {
    if *pos >= buf.len() {
        return Err(ProtocolError::Incomplete);
    }

    let byte = peek(buf, *pos);
    *pos += 1;

    Ok(byte)
}

fn parse_number_to_usize(buf: &[u8], pos: &mut usize) -> Result<usize, ProtocolError> {
    let mut num = 0;

    loop {
        let byte = consume(buf, pos)?;

        if check_crlf_peek(buf, pos, byte)? {
            return Ok(num);
        }

        if !byte.is_ascii_digit() {
            return Err(ProtocolError::UnexpectedByte(byte));
        }

        num = (num * 10) + ((byte - b'0') as usize);
    }
}

fn expect(buf: &[u8], pos: &mut usize, expected_byte: u8) -> Result<(), ProtocolError> {
    let byte = consume(buf, pos)?;

    if byte != expected_byte {
        return Err(ProtocolError::ExpectedByte {
            expected: expected_byte,
            got: byte,
        });
    }

    Ok(())
}

fn check_crlf(buf: &[u8], pos: &mut usize) -> Result<(), ProtocolError> {
    if *pos + 1 >= buf.len() {
        return Err(ProtocolError::Incomplete);
    }

    expect(buf, pos, b'\r')?;
    expect(buf, pos, b'\n')?;
    Ok(())
}

fn check_crlf_peek(buf: &[u8], pos: &mut usize, byte: u8) -> Result<bool, ProtocolError> {
    if byte == b'\r' {
        if *pos >= buf.len() {
            return Err(ProtocolError::Incomplete);
        }

        expect(buf, pos, b'\n')?;
        return Ok(true);
    }

    Ok(false)
}

#[inline(always)]
fn get_command_arguments_amount(command: &[u8]) -> Result<usize, ProtocolError> {
    match command {
        b"GET" | b"get" | b"Get" => Ok(1),
        b"DEL" | b"del" | b"Del" => Ok(1),
        b"SET" | b"set" | b"Set" => Ok(2),
        _ => Err(ProtocolError::UnkownCommand(
            str::from_utf8(command).unwrap().to_string(),
        )),
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
            TestData {
                buffer: b"75019\r",
                expected: Err(ProtocolError::Incomplete),
            },
            TestData {
                buffer: b"750",
                expected: Err(ProtocolError::Incomplete),
            },
        ];

        for test in tests {
            let mut position = 0;
            let result = parse_number_to_usize(test.buffer, &mut position);

            match (&result, &test.expected) {
                (Ok(val), Ok(expected)) => assert_eq!(expected, val),
                (Err(e), Err(expected)) => {
                    assert_eq!(expected, e)
                }
                _ => panic!("Expected {:?}, got {:?}", test.expected, result),
            }
        }
    }

    #[test]
    fn test_parse_bulk_string() {
        struct TestData {
            buffer: &'static [u8],
            expected: Result<&'static [u8], ProtocolError>,
        }

        let tests = vec![
            TestData {
                buffer: b"$3\r\nSET\r\n",
                expected: Ok(b"SET"),
            },
            TestData {
                buffer: b"$3\r\nGET\r\n",
                expected: Ok(b"GET"),
            },
            TestData {
                buffer: b"$11\r\nhello world\r\n",
                expected: Ok(b"hello world"),
            },
            TestData {
                buffer: b"11\r\nhello world\r\n",
                expected: Err(ProtocolError::ExpectedByte {
                    expected: b'$',
                    got: b'1',
                }),
            },
            TestData {
                buffer: b"$11\nhello world\r\n",
                expected: Err(ProtocolError::UnexpectedByte(b'\n')),
            },
            TestData {
                buffer: b"$11\r",
                expected: Err(ProtocolError::Incomplete),
            },
            TestData {
                buffer: b"$11\r\nhello world\ro",
                expected: Err(ProtocolError::ExpectedByte {
                    expected: b'\n',
                    got: b'o',
                }),
            },
            TestData {
                buffer: b"$11\r\nhello world\r",
                expected: Err(ProtocolError::Incomplete),
            },
            TestData {
                buffer: b"$11\r\nhello",
                expected: Err(ProtocolError::Incomplete),
            },
        ];

        let mut i = 1;
        for test in tests {
            let mut position = 0;
            let result = parse_bulk_string(test.buffer, &mut position);

            match (&result, &test.expected) {
                (Ok(val), Ok(expected)) => assert_eq!(
                    expected, val,
                    "In test {}\nExpected {:?}\n got {:?}",
                    i, expected, val
                ),
                (Err(e), Err(expected)) => {
                    assert_eq!(
                        expected, e,
                        "In test {}\nExpected {:?}\n got {:?}",
                        i, expected, e
                    )
                }
                _ => panic!("Expected {:?}, got {:?}", test.expected, result),
            }

            i += 1;
        }
    }

    #[test]
    fn test_parse_command() {
        struct TestData {
            buffer: &'static [u8],
            expected_position: usize,
            expected_state: CommandParseState,
        }

        let tests = vec![
            TestData {
                buffer: b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n",
                expected_position: 24,
                expected_state: CommandParseState {
                    command_name: Some(b"GET".to_vec()),
                    args: vec![b"hello".to_vec()],
                    expected_args: 1,
                    current_arg: 1,
                    state: ParseState::Complete,
                },
            },
            TestData {
                buffer: b"*2\r\n$3\r\nDEL\r\n$5\r\nhello\r\n",
                expected_position: 24,
                expected_state: CommandParseState {
                    command_name: Some(b"DEL".to_vec()),
                    args: vec![b"hello".to_vec()],
                    expected_args: 1,
                    current_arg: 1,
                    state: ParseState::Complete,
                },
            },
            TestData {
                buffer: b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
                expected_position: 35,
                expected_state: CommandParseState {
                    command_name: Some(b"SET".to_vec()),
                    args: vec![b"hello".to_vec(), b"world".to_vec()],
                    expected_args: 2,
                    current_arg: 2,
                    state: ParseState::Complete,
                },
            },
            TestData {
                buffer: b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld",
                expected_position: 24,
                expected_state: CommandParseState {
                    command_name: Some(b"SET".to_vec()),
                    args: vec![b"hello".to_vec()],
                    expected_args: 2,
                    current_arg: 1,
                    state: ParseState::Partial,
                },
            },
        ];

        for test in tests {
            let mut parse_state = CommandParseState::new();
            let mut position = 0;

            let _ = parse_command(test.buffer, &mut position, &mut parse_state);

            assert_eq!(test.expected_position, position);
            assert_eq!(test.expected_state, parse_state);
        }
    }

    #[test]
    fn test_parse_partial_command() {
        struct TestData {
            buffer: Vec<&'static [u8]>,
            expected_position: usize,
            expected_state: CommandParseState,
        }

        let tests = vec![
            TestData {
                buffer: vec![b"*2\r\n$3\r\nGET\r\n", b"$5\r\nhello\r\n"],
                expected_position: 24,
                expected_state: CommandParseState {
                    command_name: Some(b"GET".to_vec()),
                    args: vec![b"hello".to_vec()],
                    expected_args: 1,
                    current_arg: 1,
                    state: ParseState::Complete,
                },
            },
            TestData {
                buffer: vec![b"*2\r\n$3\r\nDEL\r\n", b"$5\r\n", b"hello\r\n"],
                expected_position: 24,
                expected_state: CommandParseState {
                    command_name: Some(b"DEL".to_vec()),
                    args: vec![b"hello".to_vec()],
                    expected_args: 1,
                    current_arg: 1,
                    state: ParseState::Complete,
                },
            },
            TestData {
                buffer: vec![b"*2\r\n$3\r\nDEL\r\n", b"$5", b"\r", b"\n", b"hello\r\n"],
                expected_position: 24,
                expected_state: CommandParseState {
                    command_name: Some(b"DEL".to_vec()),
                    args: vec![b"hello".to_vec()],
                    expected_args: 1,
                    current_arg: 1,
                    state: ParseState::Complete,
                },
            },
        ];

        for test in tests {
            let mut parse_state = CommandParseState::new();
            let mut position = 0;

            let mut test_buffer = Vec::<u8>::new();
            test_buffer.extend_from_slice(test.buffer[0]);
            let _ = parse_command(&test_buffer, &mut position, &mut parse_state);

            for i in 1..test.buffer.len() {
                test_buffer.extend_from_slice(test.buffer[i]);

                let _ = parse_partial_command(&test_buffer, &mut position, &mut parse_state);
            }

            assert_eq!(test.expected_position, position);
            assert_eq!(test.expected_state, parse_state);
        }
    }
}
