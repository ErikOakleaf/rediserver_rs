use crate::error::ProtocolError;

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

fn parse_bulk_string<'a>(buf: &'a [u8], pos: &mut usize) -> Result<&'a [u8], ProtocolError> {
    expect(buf, pos, b'$')?;

    let string_len = parse_number_to_usize(buf, pos)?;

    if *pos + string_len + 2 > buf.len() {
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
}
