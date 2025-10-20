use redis::{
    error::{MAX_MESSAGE_SIZE, ProtocolError, RedisError},
    net::{Socket, make_ipv4_address},
};

const BUFFER_SIZE: usize = 4 + MAX_MESSAGE_SIZE;

fn main() {
    let soc = Socket::new_tcp_();
    soc.set_reuseaddr().unwrap();

    let address = make_ipv4_address(0, 1234);

    soc.bind(&address).unwrap();

    soc.listen().unwrap();

    loop {
        let (connection_socket, _) = match soc.accept() {
            Ok(value) => value,
            Err(_) => continue,
        };

        process_requests(&connection_socket).unwrap();
    }
}

struct ReadState {
    buffer: [u8; BUFFER_SIZE],
    bytes_filled: usize,
    position: usize,
    wanted_length: Option<usize>,
}

impl ReadState {
    fn new() -> Self {
        ReadState {
            buffer: [0u8; BUFFER_SIZE],
            bytes_filled: 0,
            position: 0,
            wanted_length: None,
        }
    }
}

fn process_requests(soc: &Socket) -> Result<(), RedisError> {
    let mut read_state = ReadState::new();

    loop {
        let read_result = fill_buffer(soc, &mut read_state);
        if let Err(_) = read_result {
            break;
        }

        try_process_messages(soc, &mut read_state);
    }

    Ok(())
}

#[inline(always)]
fn fill_buffer(soc: &Socket, state: &mut ReadState) -> Result<(), RedisError> {
    let read_result = soc.read(&mut state.buffer[state.bytes_filled..])?;

    if read_result == 0 {
        return Err(RedisError::ConnectionClosed);
    }

    state.bytes_filled += read_result;
    Ok(())
}

#[inline]
fn try_process_messages(soc: &Socket, state: &mut ReadState) {
    loop {
        let message = try_extract_message(state);

        match message {
            None => return,
            Some(message) => handle_message(message, soc),
        }
    }
}

fn try_extract_message(state: &mut ReadState) -> Option<&[u8]> {
    if let Some(length) = state.wanted_length {
        if state.bytes_filled - state.position < length {
            return None;
        }

        let message = &state.buffer[state.position..state.position + length];
        state.position += length;
        state.wanted_length = None;
        return Some(message);
    }

    if state.bytes_filled - state.position < 4 {
        shift_buffer_if_needed(state);
        return None;
    }

    let length = get_message_length(&state.buffer[state.position..state.position + 4]).unwrap();

    let leftover = state.bytes_filled - state.position;

    if leftover < length {
        state
            .buffer
            .copy_within(state.position + 4..state.bytes_filled, 0);
        state.wanted_length = Some(length);
        state.bytes_filled = leftover - 4;
        state.position = 0;
        return None;
    }

    let result = &state.buffer[state.position + 4..state.position + 4 + length];
    state.position += length + 4;

    Some(result)
}

#[inline(always)]
fn shift_buffer_if_needed(state: &mut ReadState) {
    if state.position > 0 && state.position == state.bytes_filled {
        state.bytes_filled = 0;
        state.position = 0;
    }
}

fn handle_message(buffer: &[u8], soc: &Socket) {
    let s = std::str::from_utf8(buffer).unwrap().to_string();

    println!("client says {}", s);

    let response = b"world";
    let response_length = response.len();

    let mut write_buffer = [0u8; 9];

    write_buffer[..4].copy_from_slice(&(response_length as u32).to_be_bytes());
    write_buffer[4..4 + response.len()].copy_from_slice(response);

    soc.write_full(&write_buffer).unwrap();
}

// Helpers

#[inline]
fn get_message_length(buffer: &[u8]) -> Result<usize, ProtocolError> {
    let length = u32_from_be_bytes(&buffer[..4]) as usize;

    if length > MAX_MESSAGE_SIZE {
        return Err(ProtocolError::MessageTooLong(length));
    }

    Ok(length)
}

fn u32_from_be_bytes(slice: &[u8]) -> u32 {
    debug_assert_eq!(
        slice.len(),
        4,
        "SLICE DOES NOT HAVE CORRECT LENGTH IN u32 from be bytes function",
    );

    let length = ((slice[0] as u32) << 24)
        | ((slice[1] as u32) << 16)
        | ((slice[2] as u32) << 8)
        | (slice[3] as u32);
    length
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_try_extract_message() {
        struct TestData {
            prefix_short_1: Option<u16>,
            prefix_short_2: Option<u16>,
            message: &'static [u8],
            expected: Option<&'static [u8]>,
        }

        let tests = vec![
            TestData {
                prefix_short_1: Some(0),
                prefix_short_2: Some(5),
                message: b"hello",
                expected: Some(b"hello"),
            },
            TestData {
                prefix_short_1: Some(0),
                prefix_short_2: Some(5),
                message: b"world",
                expected: Some(b"world"),
            },
            TestData {
                prefix_short_1: Some(0),
                prefix_short_2: Some(11),
                message: b"hello",
                expected: None,
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: None,
                message: b" world",
                expected: Some(b"hello world"),
            },
            TestData {
                prefix_short_1: Some(0),
                prefix_short_2: None,
                message: &[],
                expected: None,
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: Some(5),
                message: b"hello",
                expected: Some(b"hello"),
            },
            TestData {
                prefix_short_1: Some(0),
                prefix_short_2: None,
                message: &[],
                expected: None,
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: Some(5),
                message: &[],
                expected: None,
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: None,
                message: b"hel",
                expected: None,
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: None,
                message: b"lo",
                expected: Some(b"hello"),
            },
            TestData {
                prefix_short_1: Some(0),
                prefix_short_2: Some(5),
                message: b"hello\x00\x00\x00\x05world", // Two messages back-to-back
                expected: Some(b"hello"),
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: None,
                message: &[],
                expected: Some(b"world"),
            },
            TestData {
                prefix_short_1: Some(0),
                prefix_short_2: Some(5),
                message: b"hello\x00\x00\x00\x05wor", // one and a half messages back-to-back
                expected: Some(b"hello"),
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: None,
                message: b"ld",
                expected: Some(b"world"),
            },
            TestData {
                prefix_short_1: Some(0),
                prefix_short_2: None,
                message: &[],
                expected: None,
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: Some(5),
                message: &[],
                expected: None,
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: None,
                message: b"hello",
                expected: Some(b"hello"),
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: None,
                message: &[0], // sending 1 then 3 bytes for the length
                expected: None,
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: None,
                message: &[0, 0, 5],
                expected: None,
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: None,
                message: b"hello",
                expected: Some(b"hello"),
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: None,
                message: &[0, 0, 0], // sending 3 then 1 bytes for the length
                expected: None,
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: None,
                message: &[5],
                expected: None,
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: None,
                message: b"world",
                expected: Some(b"world"),
            },
        ];

        let mut read_state = ReadState::new();

        let mut i = 1;
        for test in tests {
            let mut combined_vec: Vec<u8> = Vec::new();

            if let Some(short1) = test.prefix_short_1 {
                combined_vec.extend_from_slice(&short1.to_be_bytes());
            }

            if let Some(short2) = test.prefix_short_2 {
                combined_vec.extend_from_slice(&short2.to_be_bytes());
            }

            combined_vec.extend_from_slice(test.message);

            for (i, &byte) in combined_vec.iter().enumerate() {
                read_state.buffer[read_state.bytes_filled + i] = byte;
            }
            read_state.bytes_filled += combined_vec.len();

            let result = try_extract_message(&mut read_state);

            let format_output = |opt: &Option<&[u8]>| -> String {
                match opt {
                    Some(bytes) => String::from_utf8_lossy(bytes).to_string(),
                    None => "None".to_string(),
                }
            };

            assert_eq!(
                test.expected,
                result.as_deref(),
                "in test {}\nexpected: {}\ngot: {}\n",
                i,
                format_output(&test.expected),
                format_output(&result.as_deref())
            );

            i += 1;
        }
    }
}
