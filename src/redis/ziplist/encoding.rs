use crate::redis::ziplist::{
    INT8_TAG, INT16_TAG, INT24_TAG, INT32_TAG, INT64_TAG, STR6_MASK, STR6_TAG, STR14_MASK,
    STR14_TAG, STR32_TAG,
};

#[derive(Debug, PartialEq)]
pub enum EncodingType {
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
    pub fn from_header(header: u8) -> EncodingType {
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
