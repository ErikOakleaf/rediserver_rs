use std::mem::{self};

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
        const ZL_LEN: u16 = 0;

        let mut data = Vec::<u8>::with_capacity(ZL_BYTES as usize);
        data.extend_from_slice(&ZL_BYTES.to_le_bytes());
        data.extend_from_slice(&ZL_TAIL.to_le_bytes());
        data.extend_from_slice(&ZL_LEN.to_le_bytes());
        data.push(ZL_END);

        ZipList { data: data }
    }

    fn push(&mut self, value: ZipEntry) {
        self.get_tail_prevlen();

        // Remove 0xFF
        self.data.pop();

        match value {
            ZipEntry::Int8()
        }
    }

    // Helpers

    #[inline(always)]
    fn get_zl_bytes(&self) -> u32 {
        let bytes: [u8; 4] = self.data[0..4]
            .try_into()
            .expect("Slice length mismatch for u32 conversion");

        u32::from_le_bytes(bytes)
    }

    #[inline(always)]
    fn get_zl_tail(&self) -> u32 {
        let bytes: [u8; 4] = self.data[4..8]
            .try_into()
            .expect("Slice length mismatch for u32 conversion");

        u32::from_le_bytes(bytes)
    }

    #[inline(always)]
    fn get_zl_len(&self) -> u16 {
        let bytes: [u8; 2] = self.data[8..10]
            .try_into()
            .expect("Slice length mismatch for u16 conversion");

        u16::from_le_bytes(bytes)
    }

    #[inline(always)]
    fn get_tail_prevlen(&self) -> u32 {
        // subtract 1 for the 0xFF
        self.get_zl_bytes() - self.get_zl_tail() - 1
    }

    fn extract_6bit_length(byte: u8) -> u8 {
        byte & 0b0011_1111
    }

    fn extract_14bit_length(byte1: u8, byte2: u8) -> u16 {
        let high_bits = (byte1 & 0b0011_1111) as u16;
        let low_bits = byte2 as u16;

        (high_bits << 8) | low_bits
    }
}

#[derive(Debug, PartialEq)]
enum ZipEntry {
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

impl ZipEntry {
    pub fn from_redis_object(obj: RedisObject) -> ZipEntry {
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
                0..=12 => ZipEntry::Int4BitsImmediate(i as u8),
                INT8_MIN..=INT8_MAX => ZipEntry::Int8(i as i8),
                INT16_MIN..=INT16_MAX => ZipEntry::Int16(i as i16),
                INT24_MIN..=INT24_MAX => ZipEntry::Int24(i as i32),
                INT32_MIN..=INT32_MAX => ZipEntry::Int32(i as i32),
                _ => ZipEntry::Int64(i),
            },
            RedisObject::String(s) => match s.len() {
                0..=63 => ZipEntry::Str6BitsLength(s),
                64..=16383 => ZipEntry::Str14BitsLength(s),
                16384..=U32_MAX => ZipEntry::Str32BitsLength(s),
                _ => panic!("string to long for ziplist"),
            },
        }
    }
}

#[derive(Debug, PartialEq)]
enum EncodingType {
    Int4BitsImmediate,
    Int8,
    Int16,
    Int24,
    Int32,
    Int64,
    Str6BitsLength,
    Str14BitsLength,
    Str32BitsLength,
}

impl EncodingType {
    fn from_header(header: u8) -> EncodingType {
        const INT8_TAG: u8 = 0b1111_1110;
        const INT16_TAG: u8 = 0b1100_0000;
        const INT24_TAG: u8 = 0b1111_0000;
        const INT32_TAG: u8 = 0b1101_0000;
        const INT64_TAG: u8 = 0b1110_0000;
        const STR6_TAG: u8 = 0b0000_0000;
        const STR14_TAG: u8 = 0b0100_0000;
        const STR32_TAG: u8 = 0b1000_0000;

        const STR6_MASK: u8 = 0b1100_0000;
        const STR14_MASK: u8 = 0b1100_0000;

        match header {
            INT8_TAG => EncodingType::Int8,
            INT16_TAG => EncodingType::Int16,
            INT24_TAG => EncodingType::Int24,
            INT32_TAG => EncodingType::Int32,
            INT64_TAG => EncodingType::Int64,
            STR32_TAG => EncodingType::Str32BitsLength,

            // Int4: 1111xxxx where xxxx is 0001-1101 (0xF1-0xFD)
            0xF1..=0xFD => EncodingType::Int4BitsImmediate,

            _ if (header & STR14_MASK) == STR14_TAG => EncodingType::Str14BitsLength,
            _ if (header & STR6_MASK) == STR6_TAG => EncodingType::Str6BitsLength,

            _ => panic!("invalid header"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zip_entry_from_redis_object() {
        struct TestData {
            obj: RedisObject,
            expected: ZipEntry,
        }

        let tests = vec![
            TestData {
                obj: RedisObject::new_from_bytes(b"5"),
                expected: ZipEntry::Int4BitsImmediate(5),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"12"),
                expected: ZipEntry::Int4BitsImmediate(12),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"0"),
                expected: ZipEntry::Int4BitsImmediate(0),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"100"),
                expected: ZipEntry::Int8(100),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"-100"),
                expected: ZipEntry::Int8(-100),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"127"),
                expected: ZipEntry::Int8(127),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"-128"),
                expected: ZipEntry::Int8(-128),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"1000"),
                expected: ZipEntry::Int16(1000),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"-1000"),
                expected: ZipEntry::Int16(-1000),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"32767"),
                expected: ZipEntry::Int16(32767),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"-32768"),
                expected: ZipEntry::Int16(-32768),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"100000"),
                expected: ZipEntry::Int24(100000),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"-100000"),
                expected: ZipEntry::Int24(-100000),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"8388607"),
                expected: ZipEntry::Int24(8388607),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"-8388608"),
                expected: ZipEntry::Int24(-8388608),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"10000000"),
                expected: ZipEntry::Int32(10000000),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"-10000000"),
                expected: ZipEntry::Int32(-10000000),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"2147483647"),
                expected: ZipEntry::Int32(2147483647),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"-2147483648"),
                expected: ZipEntry::Int32(-2147483648),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"5000000000"),
                expected: ZipEntry::Int64(5000000000),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"-5000000000"),
                expected: ZipEntry::Int64(-5000000000),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"9223372036854775807"),
                expected: ZipEntry::Int64(9223372036854775807),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"-9223372036854775808"),
                expected: ZipEntry::Int64(-9223372036854775808),
            },
            // strings
            TestData {
                obj: RedisObject::String(b"hello".to_vec().into_boxed_slice()),
                expected: ZipEntry::Str6BitsLength(b"hello".to_vec().into_boxed_slice()),
            },
            TestData {
                obj: RedisObject::String(vec![b'a'; 63].into_boxed_slice()),
                expected: ZipEntry::Str6BitsLength(vec![b'a'; 63].into_boxed_slice()),
            },
            TestData {
                obj: RedisObject::String(b"".to_vec().into_boxed_slice()),
                expected: ZipEntry::Str6BitsLength(b"".to_vec().into_boxed_slice()),
            },
            TestData {
                obj: RedisObject::String(vec![b'b'; 1000].into_boxed_slice()),
                expected: ZipEntry::Str14BitsLength(vec![b'b'; 1000].into_boxed_slice()),
            },
            TestData {
                obj: RedisObject::String(vec![b'c'; 16383].into_boxed_slice()),
                expected: ZipEntry::Str14BitsLength(vec![b'c'; 16383].into_boxed_slice()),
            },
            TestData {
                obj: RedisObject::String(vec![b'd'; 64].into_boxed_slice()),
                expected: ZipEntry::Str14BitsLength(vec![b'd'; 64].into_boxed_slice()),
            },
            TestData {
                obj: RedisObject::String(vec![b'e'; 100000].into_boxed_slice()),
                expected: ZipEntry::Str32BitsLength(vec![b'e'; 100000].into_boxed_slice()),
            },
            // this test would be about 4gb of memory so i skip it because it takes so long as well
            // but it has passed
            // TestData {
            //     obj: RedisObject::String(vec![b'f'; 4294967295].into_boxed_slice()),
            //     expected: ZipEntry::Str32BitsLength(vec![b'f'; 4294967295].into_boxed_slice()),
            // },
            TestData {
                obj: RedisObject::String(vec![b'g'; 16384].into_boxed_slice()),
                expected: ZipEntry::Str32BitsLength(vec![b'g'; 16384].into_boxed_slice()),
            },
        ];

        for test in tests {
            let result = ZipEntry::from_redis_object(test.obj);
            assert_eq!(test.expected, result);
        }
    }

    #[test]
    fn test_get_encoding_from_header() {
        struct TestData {
            header: u8,
            expected: EncodingType,
        }

        let tests = vec![
            TestData {
                header: 0b0011_1011,
                expected: EncodingType::Str6BitsLength,
            },
            TestData {
                header: 0b0110_1111,
                expected: EncodingType::Str14BitsLength,
            },
            TestData {
                header: 0b1000_0000,
                expected: EncodingType::Str32BitsLength,
            },
            TestData {
                header: 0b1111_0001,
                expected: EncodingType::Int4BitsImmediate,
            },
            TestData {
                header: 0b1111_0010,
                expected: EncodingType::Int4BitsImmediate,
            },
            TestData {
                header: 0b1111_1101,
                expected: EncodingType::Int4BitsImmediate,
            },
            TestData {
                header: 0b1111_1110,
                expected: EncodingType::Int8,
            },
            TestData {
                header: 0b1111_1110,
                expected: EncodingType::Int8,
            },
            TestData {
                header: 0b1100_0000,
                expected: EncodingType::Int16,
            },
            TestData {
                header: 0b1111_0000,
                expected: EncodingType::Int24,
            },
            TestData {
                header: 0b1101_0000,
                expected: EncodingType::Int32,
            },
            TestData {
                header: 0b1110_0000,
                expected: EncodingType::Int64,
            },
        ];

        for test in tests {
            let result = EncodingType::from_header(test.header);
            assert_eq!(test.expected, result);
        }
    }
}
