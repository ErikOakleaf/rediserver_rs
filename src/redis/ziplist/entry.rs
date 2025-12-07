use crate::redis::redis_object::RedisObject;

#[derive(Debug, PartialEq)]
pub enum ZipEntry {
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
    // this should probably be it's own thing and not from redis object
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
            _ => unreachable!("Can't insert ziplist in ziplist"),
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
