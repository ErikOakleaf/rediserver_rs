use std::{
    alloc::{self, Layout},
    intrinsics::unreachable,
};

#[derive(Clone, Debug, PartialEq)]
pub enum RedisType {
    String,
    Int,
    Int16,
    Int32,
    Int64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RedisObject {
    type_: RedisType,
    value: Box<[u8]>,
}

impl RedisObject {
    pub fn new_from_bytes(bytes: &[u8]) -> RedisObject {
        RedisObject {
            type_: RedisType::String,
            value: box_bytes_from_slice(bytes),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self.type_ {
            RedisType::String => self.value.to_vec(),
            RedisType::Int => {
                let bytes = [
                    self.value[0],
                    self.value[1],
                    self.value[2],
                    self.value[3],
                    self.value[4],
                    self.value[5],
                    self.value[6],
                    self.value[7],
                ];
                let num = i64::from_be_bytes(bytes);

                num.to_string().into_bytes()
            }
            _ => panic!("just something"),
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

    fn get_redis_type(bytes: &[u8]) -> RedisType {
        let mut num: i64 = 0;
        let mut i = 0;

        let is_negative = match bytes[0] {
            b'-' => {
                i += 1;
                true
            }
            _ => false,
        };

        while i < bytes.len() {
            if !bytes[i].is_ascii_digit() {
                return RedisType::String;
            }

            num = num * 10 + (bytes[i] - b'0') as i64;
        }

        if is_negative {
            num *= -1;
        }

        const I16_MIN: i64 = i16::MIN as i64;
        const I16_MAX: i64 = i16::MAX as i64;
        const I32_MIN: i64 = i32::MIN as i64;
        const I32_MAX: i64 = i32::MAX as i64;

        match num {
            I16_MIN..=I16_MAX => RedisType::Int16,
            I32_MIN..=I32_MAX => RedisType::Int32,
            _ => RedisType::Int64,
        }
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
