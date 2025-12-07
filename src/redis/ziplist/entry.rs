use crate::redis::redis_object::try_parse_int;

#[derive(Debug, PartialEq)]
pub enum ZipEntry<'a> {
    Int4BitsImmediate(u8),
    Int8(i8),
    Int16(i16),
    Int24(i32),
    Int32(i32),
    Int64(i64),

    // String encodings
    Str6BitsLength(&'a [u8]),  // 6-bit immediate
    Str14BitsLength(&'a [u8]), // 14-bit big-endian
    Str32BitsLength(&'a [u8]), // 32-bit big-endian
}

impl<'a> ZipEntry<'a> {
    // this should probably be it's own thing and not from redis object
    pub fn from_bytes(bytes: &'a [u8]) -> ZipEntry<'a> {
        const INT8_MIN: i64 = i8::MIN as i64;
        const INT8_MAX: i64 = i8::MAX as i64;
        const INT16_MIN: i64 = i16::MIN as i64;
        const INT16_MAX: i64 = i16::MAX as i64;
        const INT24_MIN: i64 = -8388608;
        const INT24_MAX: i64 = 8388607;
        const INT32_MIN: i64 = i32::MIN as i64;
        const INT32_MAX: i64 = i32::MAX as i64;

        const U32_MAX: usize = u32::MAX as usize;

        match try_parse_int(bytes) {
            Some(i) => match i {
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
            // TODO this might be able to be a reference instead with no extra overhead
            None => match bytes.len() {
                0..=63 => ZipEntry::Str6BitsLength(bytes),
                64..=16383 => ZipEntry::Str14BitsLength(bytes),
                16384..=U32_MAX => ZipEntry::Str32BitsLength(bytes),
                _ => panic!("string to long for ziplist"),
            },
        }
    }

    // gives the length of the zip entry in how many bytes header + payload
    pub fn amount_bytes(&self) -> usize {
        match self {
            ZipEntry::Int4BitsImmediate(_) => 1,
            ZipEntry::Int8(_) => 2,
            ZipEntry::Int16(_) => 3,
            ZipEntry::Int24(_) => 4,
            ZipEntry::Int32(_) => 5,
            ZipEntry::Int64(_) => 9,
            ZipEntry::Str6BitsLength(s) => s.len() + 1,
            ZipEntry::Str14BitsLength(s) => s.len() + 2,
            ZipEntry::Str32BitsLength(s) => s.len() + 5,
        }
    }
}
