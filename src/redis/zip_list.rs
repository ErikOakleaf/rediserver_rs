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
    pub fn from_redis_object(obj: &RedisObject) -> ZipEncoding {
        match obj {
            RedisObject::Int(i) => {
                match i {
                    1..13 => ZipEncoding::Int4BitsImmediate(*i as u8),
                    -128..127 => ZipEncoding::Int8(*i as i8),
                    -32768..32767 => ZipEncoding::Int16(*i as i16),
                    -8388608..8388607 => ZipEncoding::Int24(*i as i32),
                    -2147483648..2147483647 => ZipEncoding::Int32(*i as i32),
                    _ => ZipEncoding::Int64(*i),
                }
            }
            RedisObject::String(s) => {
                match s.len() {
                    0..=63 => ZipEncoding::Str6BitsLength(s), 
                    64..=16384 => ZipEncoding::Str6BitsLength(s), 
                    16384..=u32::MAX => ZipEncoding::Str6BitsLength(s), 
                }
            }
        }
    }
}
