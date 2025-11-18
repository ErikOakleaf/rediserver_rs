use std::mem::{self};

use crate::redis::redis_object::RedisObject;

const ZL_HEADERS_SIZE: usize = mem::size_of::<u32>() * 2 + mem::size_of::<u16>();
const ZL_END_SIZE: usize = mem::size_of::<u8>();
const ZL_END: u8 = 0xFF;

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
        let prevlen = self.get_tail_prevlen();

        // Remove 0xFF
        self.data.pop();

        // Set tail to point to the new value
        self.set_lz_tail(self.data.len() as u32);
        self.increment_zl_len(1);

        // add prevlen
        if prevlen < 254 {
            self.data.push(prevlen as u8);
            self.increment_zl_bytes(1);
        } else {
            self.data.push(0xFE);
            self.data.extend_from_slice(&prevlen.to_le_bytes());
            self.increment_zl_bytes(5);
        }

        match value {
            ZipEntry::Int4BitsImmediate(i) => {
                let num = (i + 1) | 0b1111_0000;
                self.data.push(num);
                self.increment_zl_bytes(1);
            }
            ZipEntry::Int8(i) => {
                self.data.push(INT8_TAG);
                self.data.push(i as u8);
                self.increment_zl_bytes(2);
            }
            ZipEntry::Int16(i) => {
                self.data.push(INT16_TAG);
                self.data.extend_from_slice(&i.to_le_bytes());
                self.increment_zl_bytes(3);
            }
            ZipEntry::Int24(i) => {
                self.data.push(INT24_TAG);
                self.data.extend_from_slice(&Self::i24_to_le_bytes(i));
                self.increment_zl_bytes(4);
            }
            ZipEntry::Int32(i) => {
                self.data.push(INT32_TAG);
                self.data.extend_from_slice(&i.to_le_bytes());
                self.increment_zl_bytes(5);
            }
            ZipEntry::Int64(i) => {
                self.data.push(INT64_TAG);
                self.data.extend_from_slice(&i.to_le_bytes());
                self.increment_zl_bytes(9);
            }
            ZipEntry::Str6BitsLength(s) => {
                // length and tag 00 encoded here
                let str_tag = (s.len() as u8) & 0b00111111;
                self.data.push(str_tag);
                self.data.extend_from_slice(&s);

                let str_total_len = (s.len() + 1) as u32;
                self.increment_zl_bytes(str_total_len);
            }
            ZipEntry::Str14BitsLength(s) => {
                // this is ugly right now
                let str_len = s.len() as u32;
                // the first mask is technically unesicary because the length should never be
                // touching the upper two bytes
                let str_tag_1 = (((str_len >> 8) as u8) & 0b0011_1111) | 0b0100_0000;
                let str_tag_2 = str_len as u8;

                self.data.push(str_tag_1);
                self.data.push(str_tag_2);

                self.data.extend_from_slice(&s);

                let str_total_len = str_len + 2;
                self.increment_zl_bytes(str_total_len);
            }
            ZipEntry::Str32BitsLength(s) => {
                let str_len = s.len() as u32;

                self.data.push(STR32_TAG);
                self.data.extend_from_slice(&str_len.to_be_bytes());
                self.data.extend_from_slice(&s);

                let str_total_len = str_len + 5;
                self.increment_zl_bytes(str_total_len);
            }
        }

        // add back 0xFF
        self.data.push(0xFF);
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
    fn set_lz_bytes(&mut self, new_value: u32) {
        let bytes = new_value.to_le_bytes();
        self.data[0] = bytes[0];
        self.data[1] = bytes[1];
        self.data[2] = bytes[2];
        self.data[3] = bytes[3];
    }

    #[inline(always)]
    fn set_lz_tail(&mut self, new_value: u32) {
        let bytes = new_value.to_le_bytes();
        self.data[4] = bytes[0];
        self.data[5] = bytes[1];
        self.data[6] = bytes[2];
        self.data[7] = bytes[3];
    }

    #[inline(always)]
    fn set_lz_len(&mut self, new_value: u16) {
        let bytes = new_value.to_le_bytes();
        self.data[8] = bytes[0];
        self.data[9] = bytes[1];
    }

    #[inline(always)]
    fn increment_zl_bytes(&mut self, n: u32) {
        let mut num = self.get_zl_bytes();
        num += n;
        self.set_lz_bytes(num);
    }

    #[inline(always)]
    fn increment_zl_len(&mut self, n: u16) {
        let mut num = self.get_zl_len();
        num += n;
        self.set_lz_len(num);
    }

    #[inline(always)]
    fn get_tail_prevlen(&self) -> u32 {
        // subtract 1 for the 0xFF
        self.get_zl_bytes() - self.get_zl_tail() - 1
    }

    #[inline(always)]
    fn extract_6bit_length(byte: u8) -> u8 {
        byte & 0b0011_1111
    }

    #[inline(always)]
    fn extract_14bit_length(byte1: u8, byte2: u8) -> u16 {
        let high_bits = (byte1 & 0b0011_1111) as u16;
        let low_bits = byte2 as u16;

        (high_bits << 8) | low_bits
    }

    #[inline(always)]
    fn i24_to_le_bytes(num: i32) -> [u8; 3] {
        [num as u8, (num >> 8) as u8, (num >> 16) as u8]
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
                0..=12 => {
                    let num = i as u8 + 1;
                    let tag = 0b1111_0000;
                    let val = num | tag;
                    ZipEntry::Int4BitsImmediate(val)
                }
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
    fn test_zip_list_push() {
        struct TestData {
            entries: Vec<ZipEntry>,
            expected: &'static [u8],
        }

        #[rustfmt::skip]
        let tests = vec![
            TestData {
                entries: vec![ZipEntry::Int4BitsImmediate(5)],
                expected: &[
                    /*zl bytes*/ 13, 0, 0, 0, /*zl tail*/ 10, 0, 0, 0, /*zl len*/ 1, 0,
                    /*prevlen*/ 0, /*data + tag*/ 0b1111_0110,
                    /*zl end*/ 0xFF,
                ],
            },
            TestData {
                entries: vec![ZipEntry::Int8(100)],
                expected: &[
                    /*zl bytes*/ 14, 0, 0, 0, /*zl tail*/ 10, 0, 0, 0, /*zl len*/ 1, 0,
                    /*prevlen*/ 0, /*data*/ INT8_TAG, 100,
                    /*zl end*/ 0xFF,
                ],
            },
            TestData {
                entries: vec![ZipEntry::Int16(1000)],
                expected: &[
                    /*zl bytes*/ 15, 0, 0, 0, /*zl tail*/ 10, 0, 0, 0, /*zl len*/ 1, 0,
                    /*prevlen*/ 0, /*data*/ INT16_TAG, 0xE8, 0x03, 
                    /*zl end*/ 0xFF,
                ],
            },
            TestData {
                entries: vec![ZipEntry::Int24(8388607)],
                expected: &[
                    /*zl bytes*/ 16, 0, 0, 0, /*zl tail*/ 10, 0, 0, 0, /*zl len*/ 1, 0,
                    /*prevlen*/ 0, /*data*/ INT24_TAG, 0xFF, 0xFF, 0x7F,
                    /*zl end*/ 0xFF,
                ],
            },
            TestData {
                entries: vec![ZipEntry::Int32(2147483647)],
                expected: &[
                    /*zl bytes*/ 17, 0, 0, 0, /*zl tail*/ 10, 0, 0, 0, /*zl len*/ 1, 0,
                    /*prevlen*/ 0, /*data*/ INT32_TAG, 0xFF, 0xFF, 0xFF, 0x7F, 
                    /*zl end*/ 0xFF,
                ],
            },
            TestData {
                entries: vec![ZipEntry::Int64(5000000000)],
                expected: &[
                    /*zl bytes*/ 21, 0, 0, 0, /*zl tail*/ 10, 0, 0, 0, /*zl len*/ 1, 0,
                    /*prevlen*/ 0, /*data*/ INT64_TAG, 0x00, 0xF2, 0x05, 0x2A, 0x01, 0x00, 0x00,
                    0x00,
                    /*zl end*/ 0xFF,
                ],
            },
            // all int tests together
            TestData {
                entries: vec![
                    ZipEntry::Int4BitsImmediate(5),
                    ZipEntry::Int8(100),
                    ZipEntry::Int16(1000),
                    ZipEntry::Int24(8388607),
                    ZipEntry::Int32(2147483647),
                    ZipEntry::Int64(5000000000),
                ],
                expected: &[
                    /*zl bytes*/ 41, 0, 0, 0, /*zl tail*/ 30, 0, 0, 0, /*zl len*/ 6, 0,
                    /*prevlen*/ 0, /*data + tag*/ 0b1111_0110,

                    /*prevlen*/ 2, /*data*/ INT8_TAG, 100,

                    /*prevlen*/ 3, /*data*/ INT16_TAG, 0xE8, 0x03,

                    /*prevlen*/ 4, /*data*/ INT24_TAG, 0xFF, 0xFF, 0x7F,

                    /*prevlen*/ 5, /*data*/ INT32_TAG, 0xFF, 0xFF, 0xFF, 0x7F,

                    /*prevlen*/ 6, /*data*/ INT64_TAG, 0x00, 0xF2, 0x05, 0x2A, 0x01, 0x00, 0x00, 0x00,

                    /*zl end*/ 0xFF,
                ],
            },
        ];

        for test in tests {
            let mut zl = ZipList::new();
            for entry in test.entries {
                zl.push(entry);
            }

            assert_eq!(test.expected, &zl.data);
        }
    }

    #[test]
    fn test_zip_entry_from_redis_object() {
        struct TestData {
            obj: RedisObject,
            expected: ZipEntry,
        }

        let tests = vec![
            TestData {
                obj: RedisObject::new_from_bytes(b"5"),
                expected: ZipEntry::Int4BitsImmediate(0b1111_0110),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"12"),
                expected: ZipEntry::Int4BitsImmediate(0b1111_1101),
            },
            TestData {
                obj: RedisObject::new_from_bytes(b"0"),
                expected: ZipEntry::Int4BitsImmediate(0b1111_0001),
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
