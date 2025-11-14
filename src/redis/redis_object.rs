use std::alloc::{self, Layout};

#[derive(Clone, Debug, PartialEq)]
pub enum RedisObject {
    String(Box<[u8]>),
    Int(i64),
}

impl RedisObject {
    pub fn new_from_bytes(bytes: &[u8]) -> RedisObject {
        Self::get_redis_object(bytes)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            RedisObject::String(s) => s.to_vec(),
            RedisObject::Int(i) => i.to_string().as_bytes().to_vec(),
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
        if bytes.is_empty() {
            return RedisObject::String(Box::new([]));
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

        // check if the number starts with zero and has more digits after which would make it a
        // string and not int
        if bytes[i] == b'0' && bytes.len() > i + 1 {
            return RedisObject::String(box_bytes_from_slice(bytes));
        }

        while i < bytes.len() {
            if !bytes[i].is_ascii_digit() {
                return RedisObject::String(box_bytes_from_slice(bytes));
            }

            let digit = (bytes[i] - b'0') as i64;

            if is_negative {
                match num.checked_mul(10).and_then(|n| n.checked_sub(digit)) {
                    Some(n) => num = n,
                    None => return RedisObject::String(box_bytes_from_slice(bytes)),
                }
            } else {
                match num.checked_mul(10).and_then(|n| n.checked_add(digit)) {
                    Some(n) => num = n,
                    None => return RedisObject::String(box_bytes_from_slice(bytes)),
                }
            }

            i += 1;
        }

        RedisObject::Int(num)
    }
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
                expected: RedisObject::String(b"28399223372036854775808".to_vec().into_boxed_slice()),

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
