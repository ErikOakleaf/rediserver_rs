use crate::redis::ziplist::ZipList;

#[derive(Clone, Debug, PartialEq)]
pub enum RedisObject {
    String(Box<[u8]>),
    Int(i64),
    List(ZipList), // lists are just ziplists for now quicklists down the line
}

impl RedisObject {
    pub fn new_from_bytes(bytes: &[u8]) -> RedisObject {
        Self::get_redis_object(bytes)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            RedisObject::String(s) => s.to_vec(),
            RedisObject::Int(i) => i.to_string().as_bytes().to_vec(),
            _ => todo!("todo ziplist to bytes"),
        }
    }

    // TODO - this method should be optimized with how it handles bytes but for now this will just
    // be doing a bunch of allocations and conversions to strings and stuff
    pub fn to_resp(&self) -> Vec<u8> {
        let data = self.to_bytes();
        let data_len = data.len();
        let header = format!("${}\r\n", data_len).as_bytes().to_vec();

        let mut result = Vec::<u8>::new();
        result.extend_from_slice(&header);
        result.extend_from_slice(&data);
        result.extend_from_slice(b"\r\n");

        result
    }

    // Helpers

    fn get_redis_object(bytes: &[u8]) -> RedisObject {
        match try_parse_int(bytes) {
            Some(num) => RedisObject::Int(num),
            None => RedisObject::String(box_bytes_from_slice(bytes)),
        }
    }
}

pub fn try_parse_int(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }

    let mut num: i64 = 0;
    let mut i = 0;
    let is_negative = match bytes[0] {
        b'-' => {
            i += 1;
            true
        }
        _ => false,
    };

    if bytes[i] == b'0' && bytes.len() > i + 1 {
        return None;
    }

    while i < bytes.len() {
        if !bytes[i].is_ascii_digit() {
            return None;
        }
        let digit = (bytes[i] - b'0') as i64;

        if is_negative {
            num = num.checked_mul(10)?.checked_sub(digit)?;
        } else {
            num = num.checked_mul(10)?.checked_add(digit)?;
        }
        i += 1;
    }

    Some(num)
}

fn box_bytes_from_slice(src: &[u8]) -> Box<[u8]> {
    let layout = std::alloc::Layout::array::<u8>(src.len()).unwrap();

    unsafe {
        let ptr = std::alloc::alloc(layout);

        std::ptr::copy_nonoverlapping(src.as_ptr(), ptr, src.len());

        // Convert raw pointer to Box<[u8]>
        Box::from_raw(std::slice::from_raw_parts_mut(ptr, src.len()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_redis_object() {
        struct TestData {
            bytes: &'static [u8],
            expected: RedisObject,
        }

        let tests = vec![
            TestData {
                bytes: b"hello",
                expected: RedisObject::String(b"hello".to_vec().into_boxed_slice()),
            },
            TestData {
                bytes: b"",
                expected: RedisObject::String(b"".to_vec().into_boxed_slice()),
            },
            TestData {
                bytes: b"123",
                expected: RedisObject::Int(123),
            },
            TestData {
                bytes: b"-123",
                expected: RedisObject::Int(-123),
            },
            TestData {
                bytes: b"0",
                expected: RedisObject::Int(0),
            },
            TestData {
                bytes: b"-0",
                expected: RedisObject::Int(0),
            },
            TestData {
                bytes: b"01",
                expected: RedisObject::String(b"01".to_vec().into_boxed_slice()),
            },
            TestData {
                bytes: b"32767",
                expected: RedisObject::Int(32767),
            },
            TestData {
                bytes: b"52767",
                expected: RedisObject::Int(52767),
            },
            TestData {
                bytes: b"-52767",
                expected: RedisObject::Int(-52767),
            },
            TestData {
                bytes: b"-2147483648",
                expected: RedisObject::Int(-2147483648),
            },
            TestData {
                bytes: b"2147483647",
                expected: RedisObject::Int(2147483647),
            },
            TestData {
                bytes: b"2147483648",
                expected: RedisObject::Int(2147483648),
            },
            TestData {
                bytes: b"3147483647",
                expected: RedisObject::Int(3147483647),
            },
            TestData {
                bytes: b"-3147483647",
                expected: RedisObject::Int(-3147483647),
            },
            TestData {
                bytes: b"9223372036854775807",
                expected: RedisObject::Int(9223372036854775807),
            },
            TestData {
                bytes: b"-9223372036854775808",
                expected: RedisObject::Int(-9223372036854775808),
            },
            TestData {
                bytes: b"28399223372036854775808",
                expected: RedisObject::String(
                    b"28399223372036854775808".to_vec().into_boxed_slice(),
                ),
            },
        ];

        for test in tests {
            let result = RedisObject::get_redis_object(test.bytes);
            assert_eq!(
                test.expected,
                result,
                "for bytes: {}\nexpected type: {:?}\ngot: {:?}",
                str::from_utf8(test.bytes).unwrap(),
                test.expected,
                result
            );
        }
    }
}
