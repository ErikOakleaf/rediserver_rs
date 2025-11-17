use std::mem;

use crate::redis::redis_object::RedisObject;

const ZL_HEADERS_SIZE: usize = mem::size_of::<u32>() * 2 + mem::size_of::<u16>();
const ZL_END_SIZE: usize = mem::size_of::<u8>();
const ZL_END: u8 = 0xFF;

pub struct ZipList {
    data: Vec<u8>,
}

impl ZipList {
    pub fn new() -> ZipList {
        const ZL_BYTES: u32 = (ZL_HEADERS_SIZE + ZL_END_SIZE) as u32;
        const ZL_TAIL: u32 = ZL_HEADERS_SIZE as u32;
        const ZL_LENGTH: u16 = 0;

        let mut data = Vec::<u8>::with_capacity(ZL_BYTES as usize);
        data.extend_from_slice(&ZL_BYTES.to_le_bytes());
        data.extend_from_slice(&ZL_TAIL.to_le_bytes());
        data.extend_from_slice(&ZL_LENGTH.to_le_bytes());
        data.push(ZL_END);

        ZipList { data: data }
    }
}

#[derive(Debug, PartialEq)]
enum ZipEncoding {
    Int4BitsImmediate(u8),
    Int8(i8),
    Int16(i16),
    Int24(i32),
    Int32(i32),
    Int64(i64),

    // String encodings
    Str6BitsLength(Box<[u8]>),  // 6-bit immediate
    Str14BitsLength(Box<[u8]>), // 14-bit big-endian
    Str32BitsLength(Box<[u8]>), // 32-bit big-endian
}

impl ZipEncoding {
    pub fn from_redis_object(obj: RedisObject) -> ZipEncoding {
        const INT8_MIN: i64 = i8::MIN as i64;
        const INT8_MAX: i64 = i8::MAX as i64;
        const INT16_MIN: i64 = i16::MIN as i64;
        const INT16_MAX: i64 = i16::MAX as i64;
        const INT24_MIN: i64 = -8388608;
        const INT24_MAX: i64 = 8388607;
        const INT32_MIN: i64 = i32::MIN as i64;
        const INT32_MAX: i64 = i32::MAX as i64;

        const U32_MAX: usize = u32::MAX as usize;

        match obj {
            RedisObject::Int(i) => match i {
                0..=12 => ZipEncoding::Int4BitsImmediate(i as u8),
                INT8_MIN..=INT8_MAX => ZipEncoding::Int8(i as i8),
                INT16_MIN..=INT16_MAX => ZipEncoding::Int16(i as i16),
                INT24_MIN..=INT24_MAX => ZipEncoding::Int24(i as i32),
                INT32_MIN..=INT32_MAX => ZipEncoding::Int32(i as i32),
                _ => ZipEncoding::Int64(i),
            },
            RedisObject::String(s) => match s.len() {
                0..=63 => ZipEncoding::Str6BitsLength(s),
                64..=16383 => ZipEncoding::Str14BitsLength(s),
                16384..=U32_MAX => ZipEncoding::Str32BitsLength(s),
                _ => panic!("string to long for ziplist"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zip_encoding_from_redis_object() {
        struct TestData {
            obj: RedisObject,
            expected: ZipEncoding,
        }

        let tests = vec![
            TestData {
                obj: RedisObject::new_from_bytes(b"5"),
                expected: ZipEncoding::Int4BitsImmediate(5),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"12"),
                expected: ZipEncoding::Int4BitsImmediate(12),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"0"),
                expected: ZipEncoding::Int4BitsImmediate(0),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"100"),
                expected: ZipEncoding::Int8(100),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"-100"),
                expected: ZipEncoding::Int8(-100),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"127"),
                expected: ZipEncoding::Int8(127),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"-128"),
                expected: ZipEncoding::Int8(-128),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"1000"),
                expected: ZipEncoding::Int16(1000),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"-1000"),
                expected: ZipEncoding::Int16(-1000),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"32767"),
                expected: ZipEncoding::Int16(32767),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"-32768"),
                expected: ZipEncoding::Int16(-32768),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"100000"),
                expected: ZipEncoding::Int24(100000),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"-100000"),
                expected: ZipEncoding::Int24(-100000),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"8388607"),
                expected: ZipEncoding::Int24(8388607),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"-8388608"),
                expected: ZipEncoding::Int24(-8388608),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"10000000"),
                expected: ZipEncoding::Int32(10000000),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"-10000000"),
                expected: ZipEncoding::Int32(-10000000),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"2147483647"),
                expected: ZipEncoding::Int32(2147483647),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"-2147483648"),
                expected: ZipEncoding::Int32(-2147483648),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"5000000000"),
                expected: ZipEncoding::Int64(5000000000),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"-5000000000"),
                expected: ZipEncoding::Int64(-5000000000),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"9223372036854775807"),
                expected: ZipEncoding::Int64(9223372036854775807),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"-9223372036854775808"),
                expected: ZipEncoding::Int64(-9223372036854775808),
            },
            // strings
            TestData {
                obj: RedisObject::String(b"hello".to_vec().into_boxed_slice()),
                expected: ZipEncoding::Str6BitsLength(b"hello".to_vec().into_boxed_slice()),
            },
            TestData {
                obj: RedisObject::String(vec![b'a'; 63].into_boxed_slice()),
                expected: ZipEncoding::Str6BitsLength(vec![b'a'; 63].into_boxed_slice()),
            },
            TestData {
                obj: RedisObject::String(b"".to_vec().into_boxed_slice()),
                expected: ZipEncoding::Str6BitsLength(b"".to_vec().into_boxed_slice()),
            },
            TestData {
                obj: RedisObject::String(vec![b'b'; 1000].into_boxed_slice()),
                expected: ZipEncoding::Str14BitsLength(vec![b'b'; 1000].into_boxed_slice()),
            },
            TestData {
                obj: RedisObject::String(vec![b'c'; 16383].into_boxed_slice()),
                expected: ZipEncoding::Str14BitsLength(vec![b'c'; 16383].into_boxed_slice()),
            },
            TestData {
                obj: RedisObject::String(vec![b'd'; 64].into_boxed_slice()),
                expected: ZipEncoding::Str14BitsLength(vec![b'd'; 64].into_boxed_slice()),
            },
            TestData {
                obj: RedisObject::String(vec![b'e'; 100000].into_boxed_slice()),
                expected: ZipEncoding::Str32BitsLength(vec![b'e'; 100000].into_boxed_slice()),
            },
            // this test would be about 4gb of memory so i skip it because it takes so long as well
            // but it has passed
            // TestData {
            //     obj: RedisObject::String(vec![b'f'; 4294967295].into_boxed_slice()),
            //     expected: ZipEncoding::Str32BitsLength(vec![b'f'; 4294967295].into_boxed_slice()),
            // },
            TestData {
                obj: RedisObject::String(vec![b'g'; 16384].into_boxed_slice()),
                expected: ZipEncoding::Str32BitsLength(vec![b'g'; 16384].into_boxed_slice()),
            },
        ];

        for test in tests {
            let result = ZipEncoding::from_redis_object(test.obj);
            assert_eq!(test.expected, result);
        }
    }
}
