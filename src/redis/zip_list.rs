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

#[derive(Clone, Debug, PartialEq)]
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

    pub fn push(&mut self, value: ZipEntry) {
        let prevlen = self.get_tail_prevlen();

        // Remove 0xFF
        self.data.pop();

        // Set tail to point to the new value
        self.set_zl_tail(self.data.len() as u32);
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

    pub fn insert(&mut self, index: usize, value: ZipEntry) {
        // if the index is at the end just use the push logic
        if index == self.get_zl_len() as usize {
            self.push(value);
            return;
        } else if index > self.get_zl_len() as usize {
            panic!("inserted in a index that does not exist")
        }

        let mut offset = if index == 0 {
            ZL_HEADERS_SIZE
        } else {
            self.get_index_offset(index)
        };

        // skip prevlen
        let current_prevlen;
        if self.data[offset] < 0xFE {
            offset += 1;
            current_prevlen = 1;
            println!("skipping prevlen 1 byte")
        } else if self.data[offset] == 0xFE {
            offset += 5;
            current_prevlen = 2;
        } else {
            panic!("incorrect encoding")
        }

        let entry_length = Self::get_entry_length(&value);
        let prevlen_length = {
            if entry_length + current_prevlen >= 254 {
                5
            } else {
                1
            }
        };
        let prevlen_value = entry_length + current_prevlen;
        let total_len = entry_length + prevlen_length;

        // shift to make space in the array
        self.shift_bytes(offset, total_len);
        println!("shifting bytes at {} by {}", offset, total_len);

        // insert the thing
        match value {
            ZipEntry::Int4BitsImmediate(i) => {
                let num = (i + 1) | 0b1111_0000;
                self.data[offset] = num;

                offset += 1;
            }
            ZipEntry::Int8(i) => {
                self.data[offset] = INT8_TAG;
                self.data[offset + 1] = i as u8;

                offset += 2;
            }
            ZipEntry::Int16(i) => {
                self.data[offset] = INT16_TAG;
                offset += 1;

                self.data[offset..offset + 2].copy_from_slice(&i.to_le_bytes());

                offset += 2
            }
            ZipEntry::Int24(i) => {
                self.data[offset] = INT24_TAG;
                offset += 1;

                self.data[offset..offset + 3].copy_from_slice(&Self::i24_to_le_bytes(i));

                offset += 3
            }
            ZipEntry::Int32(i) => {
                self.data[offset] = INT32_TAG;
                offset += 1;

                self.data[offset..offset + 4].copy_from_slice(&i.to_le_bytes());

                offset += 4
            }
            ZipEntry::Int64(i) => {
                self.data[offset] = INT64_TAG;
                offset += 1;

                self.data[offset..offset + 8].copy_from_slice(&i.to_le_bytes());

                offset += 8
            }
            ZipEntry::Str6BitsLength(s) => {
                // length and tag 00 encoded here
                let str_tag = (s.len() as u8) & 0b00111111;
                self.data[offset] = str_tag;
                offset += 1;

                self.data[offset..offset + s.len()].copy_from_slice(&s);

                offset += s.len();
            }
            ZipEntry::Str14BitsLength(s) => {
                // this is ugly right now
                let str_len = s.len() as u32;
                // the first mask is technically unesicary because the length should never be
                // touching the upper two bytes
                let str_tag_1 = (((str_len >> 8) as u8) & 0b0011_1111) | 0b0100_0000;
                let str_tag_2 = str_len as u8;

                self.data[offset] = str_tag_1;
                self.data[offset + 1] = str_tag_2;

                offset += 2;

                self.data[offset..offset + s.len()].copy_from_slice(&s);

                offset += s.len();
            }
            ZipEntry::Str32BitsLength(s) => {
                let str_len = s.len() as u32;

                self.data[offset] = STR32_TAG;
                offset += 1;

                self.data[offset..offset + 4].copy_from_slice(&str_len.to_be_bytes());
                offset += 4;

                self.data[offset..offset + s.len()].copy_from_slice(&s);

                offset += s.len();
            }
        }

        // add prevlen
        // total len should not be used here but the entry len plus the previous prevlen len
        if prevlen_value < 254 {
            self.data[offset] = prevlen_value as u8;
        } else {
            self.data[offset] = 0xFE;
            offset += 1;
            self.data[offset..offset + 4].copy_from_slice(&(prevlen_value as u32).to_le_bytes());
        }

        // change headers
        self.increment_zl_bytes(total_len as u32);

        // the ammount the tail moves is depenedent if this is the final index that is not push
        if index == (self.get_zl_len() - 1) as usize {
            self.increment_zl_tail((entry_length + current_prevlen) as u32);
        } else {
            self.increment_zl_tail(total_len as u32);
        }

        self.increment_zl_len(1);
    }

    fn delete(&mut self, index: usize) {
        if index == (self.get_zl_len() - 1) as usize {
            self.delete_tail();
            println!("tail deletion");
            return;
        }

        let mut offset = self.get_index_offset(index);

        // figure out if the value before the tail has a long or short prevlen this will come in
        // handy later
        let before_tail_prevlen = self.data[self.get_index_offset(index) - 1];
        let long_tail_prevlen = before_tail_prevlen == 0xFE;

        // skip the prevlen we don't want to touch this unless it is the tail i supose then it
        // would basically be a pop

        if self.data[offset] == 0xFE {
            offset += 5;
        } else if self.data[offset] == 0xFF {
            panic!("no hold up");
        } else {
            offset += 1;
        }

        let init_pos = offset;

        let data_header = EncodingType::from_header(self.data[offset]);

        match data_header {
            EncodingType::Int4BitsImmediate => offset += 1,
            EncodingType::Int8 => offset += 2,
            EncodingType::Int16 => offset += 3,
            EncodingType::Int24 => offset += 4,
            EncodingType::Int32 => offset += 5,
            EncodingType::Int64 => offset += 9,
            EncodingType::Str6BitsLength => {
                let str_len = self.data[offset] & 0b00_111111;
                offset += 1;
                offset += str_len as usize;
            }
            EncodingType::Str14BitsLength => {
                let str_len_1 = self.data[offset] & 0b00_111111;
                let str_len_2 = self.data[offset + 1];

                let str_len: usize = ((str_len_1 as usize) << 8) | (str_len_2 as usize);

                offset += 2;
                offset += str_len;
            }
            EncodingType::Str32BitsLength => {
                offset += 1;
                let str_len_1 = self.data[offset];
                let str_len_2 = self.data[offset + 1];
                let str_len_3 = self.data[offset + 2];
                let str_len_4 = self.data[offset + 3];
                let str_len =
                    u32::from_be_bytes([str_len_1, str_len_2, str_len_3, str_len_4]) as usize;
                offset += 4;
                offset += str_len;
                println!("strlen is {}", str_len);
            }
        }

        // delete the prevlen

        let long_after_prevlen;
        if self.data[offset] == 0xFE {
            offset += 5;
            long_after_prevlen = true;
        } else if self.data[offset] == 0xFF {
            panic!("no hold up");
        } else {
            offset += 1;
            long_after_prevlen = false;
        }

        self.data.drain(init_pos..offset);

        let bytes_deleted = offset - init_pos;
        self.decrement_zl_bytes(bytes_deleted as u32);
        // depending on if the prevlen of the value behind it is larger or less than the current
        // one we would have to shift the tail to point to the correct thing

        let new_tail = {
            if long_tail_prevlen && !long_after_prevlen {
                println!("shorten prevlen");
                self.get_zl_tail() - bytes_deleted as u32 - 4
            } else if !long_tail_prevlen && long_after_prevlen {
                println!("elongate prevlen");
                self.get_zl_tail() - bytes_deleted as u32 + 4
            } else {
                self.get_zl_tail() - bytes_deleted as u32
            }
        };

        self.set_zl_tail(new_tail);
        self.decrement_zl_len(1);

        // if there is a value after the deletion we have to modify it's prevlen
    }

    fn delete_tail(&mut self) {
        let current_tail = self.get_zl_tail();
        println!("current tail = {}", current_tail);
        println!(
            "current prevlen = {}",
            Self::get_prevlen(&self.data[current_tail as usize..])
        );
        let new_tail = current_tail - Self::get_prevlen(&self.data[current_tail as usize..]) as u32;
        let bytes_deleted = self.get_zl_bytes() - self.get_zl_tail() - 1;

        self.data.drain(current_tail as usize..self.data.len() - 1);
        self.decrement_zl_bytes(bytes_deleted);
        self.set_zl_tail(new_tail);
        self.decrement_zl_len(1);
    }

    fn get(&mut self, index: usize) -> RedisObject {
        let mut offset = self.get_index_offset(index);

        // skip prevlen
        if self.data[offset] == 0xFE {
            offset += 5;
        } else if self.data[offset] == 0xFF {
            panic!("no hold up");
        } else {
            offset += 1;
        }

        let encoding_type = EncodingType::from_header(self.data[offset]);

        match encoding_type {
            EncodingType::Int4BitsImmediate => {
                let num = (self.data[offset] & 0b0000_1111) - 1;
                RedisObject::Int(num as i64)
            }
            EncodingType::Int8 => {
                let num = self.data[offset + 1] as i8;
                RedisObject::Int(num as i64)
            }
            EncodingType::Int16 => {
                let b1 = self.data[offset + 1];
                let b2 = self.data[offset + 2];
                let num = i16::from_le_bytes([b1, b2]);
                RedisObject::Int(num as i64)
            }
            EncodingType::Int24 => {
                let b1 = self.data[offset + 1];
                let b2 = self.data[offset + 2];
                let b3 = self.data[offset + 3];
                let num = Self::i24_from_le_bytes([b1, b2, b3]);
                RedisObject::Int(num as i64)
            }
            EncodingType::Int32 => {
                let b1 = self.data[offset + 1];
                let b2 = self.data[offset + 2];
                let b3 = self.data[offset + 3];
                let b4 = self.data[offset + 4];
                let num = i32::from_le_bytes([b1, b2, b3, b4]);
                RedisObject::Int(num as i64)
            }
            EncodingType::Int64 => {
                let b1 = self.data[offset + 1];
                let b2 = self.data[offset + 2];
                let b3 = self.data[offset + 3];
                let b4 = self.data[offset + 4];
                let b5 = self.data[offset + 5];
                let b6 = self.data[offset + 6];
                let b7 = self.data[offset + 7];
                let b8 = self.data[offset + 8];
                let num = i64::from_le_bytes([b1, b2, b3, b4, b5, b6, b7, b8]);
                RedisObject::Int(num as i64)
            }
            // TODO you could create a method here to turn things instantly to a box like is
            // elsewhere
            EncodingType::Str6BitsLength => {
                let str_len = (self.data[offset] & 0b00_111111) as usize;
                RedisObject::String(
                    self.data[offset + 1..offset + 1 + str_len]
                        .to_vec()
                        .into_boxed_slice(),
                )
            }
            EncodingType::Str14BitsLength => {
                let b1 = self.data[offset] & 0b00_111111;
                let b2 = self.data[offset + 1];

                let str_len = (((b1 as u16) << 8) | b2 as u16) as usize;

                RedisObject::String(
                    self.data[offset + 2..offset + 2 + str_len]
                        .to_vec()
                        .into_boxed_slice(),
                )
            }
            EncodingType::Str32BitsLength => {
                let b1 = self.data[offset + 1];
                let b2 = self.data[offset + 2];
                let b3 = self.data[offset + 3];
                let b4 = self.data[offset + 4];

                let str_len = u32::from_be_bytes([b1, b2, b3, b4]) as usize;

                RedisObject::String(
                    self.data[offset + 5..offset + 5 + str_len]
                        .to_vec()
                        .into_boxed_slice(),
                )
            }
        }
    }

    pub fn pop_tail(&mut self) -> RedisObject {
        let object = self.get((self.get_zl_len() - 1) as usize);
        self.delete_tail();

        object
    }

    pub fn pop_head(&mut self) -> RedisObject {
        let object = self.get(0);
        self.delete(0);

        object
    }

    // Helpers

    fn get_index_offset(&self, index: usize) -> usize {
        // TODO make this be able to walk forward and backwards maybe and handle the index 0 case

        let len = self.get_zl_len() as usize;
        let indices_to_step_back = len - index - 1;
        let mut current_index = self.get_zl_tail() as usize;

        for _ in 0..indices_to_step_back {
            current_index -= Self::get_prevlen(&self.data[current_index..]);
        }

        return current_index;
    }

    fn get_prevlen(prevlen: &[u8]) -> usize {
        if prevlen[0] < 0xFE {
            return prevlen[0] as usize;
        } else if prevlen[0] == 0xFE {
            let bytes = [prevlen[1], prevlen[2], prevlen[3], prevlen[4]];
            return u32::from_le_bytes(bytes) as usize;
        } else {
            panic!("incorrect encoding")
        }
    }

    fn get_entry_length(entry: &ZipEntry) -> usize {
        match entry {
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

    fn shift_bytes(&mut self, index: usize, n: usize) {
        let original_len = self.data.len();

        debug_assert!(index <= original_len, "index out of bounds");

        unsafe {
            self.data.reserve(n);
            let ptr = self.data.as_mut_ptr();

            std::ptr::copy(ptr.add(index), ptr.add(index + n), original_len - index);

            self.data.set_len(original_len + n);
        }
    }

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
    fn set_zl_bytes(&mut self, new_value: u32) {
        let bytes = new_value.to_le_bytes();
        self.data[0] = bytes[0];
        self.data[1] = bytes[1];
        self.data[2] = bytes[2];
        self.data[3] = bytes[3];
    }

    #[inline(always)]
    fn set_zl_tail(&mut self, new_value: u32) {
        let bytes = new_value.to_le_bytes();
        self.data[4] = bytes[0];
        self.data[5] = bytes[1];
        self.data[6] = bytes[2];
        self.data[7] = bytes[3];
    }

    #[inline(always)]
    fn set_zl_len(&mut self, new_value: u16) {
        let bytes = new_value.to_le_bytes();
        self.data[8] = bytes[0];
        self.data[9] = bytes[1];
    }

    #[inline(always)]
    fn increment_zl_bytes(&mut self, n: u32) {
        let mut num = self.get_zl_bytes();
        num += n;
        self.set_zl_bytes(num);
    }

    #[inline(always)]
    fn increment_zl_tail(&mut self, n: u32) {
        let mut num = self.get_zl_tail();
        num += n;
        self.set_zl_tail(num);
    }

    #[inline(always)]
    fn increment_zl_len(&mut self, n: u16) {
        let mut num = self.get_zl_len();
        num += n;
        self.set_zl_len(num);
    }

    #[inline(always)]
    fn decrement_zl_bytes(&mut self, n: u32) {
        let mut num = self.get_zl_bytes();
        num -= n;
        self.set_zl_bytes(num);
    }

    #[inline(always)]
    fn decrement_zl_tail(&mut self, n: u32) {
        let mut num = self.get_zl_tail();
        num -= n;
        self.set_zl_tail(num);
    }

    #[inline(always)]
    fn decrement_zl_len(&mut self, n: u16) {
        let mut num = self.get_zl_len();
        num -= n;
        self.set_zl_len(num);
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

    #[inline(always)]
    fn i24_from_le_bytes(num: [u8; 3]) -> i32 {
        let value = (num[0] as i32) | ((num[1] as i32) << 8) | ((num[2] as i32) << 16);
        (value << 8) >> 8
    }
}

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

    #[test]
    fn test_zip_list_push() {
        struct TestData {
            entries: Vec<ZipEntry>,
            expected: Vec<u8>,
        }

        let tests = vec![
            TestData {
                entries: vec![ZipEntry::Int4BitsImmediate(5)],
                #[rustfmt::skip]
                expected: vec![
                    /*zl bytes*/ 13, 0, 0, 0, /*zl tail*/ 10, 0, 0, 0, /*zl len*/ 1, 0,
                    /*prevlen*/ 0, /*data + tag*/ 0b1111_0110,
                    /*zl end*/ 0xFF,
                ],
            },
            TestData {
                entries: vec![ZipEntry::Int8(100)],
                #[rustfmt::skip]
                expected: vec![
                    /*zl bytes*/ 14, 0, 0, 0, /*zl tail*/ 10, 0, 0, 0, /*zl len*/ 1, 0,
                    /*prevlen*/ 0, /*data*/ INT8_TAG, 100,
                    /*zl end*/ 0xFF,
                ],
            },
            TestData {
                entries: vec![ZipEntry::Int8(-100)],
                #[rustfmt::skip]
                expected: vec![
                    /*zl bytes*/ 14, 0, 0, 0, /*zl tail*/ 10, 0, 0, 0, /*zl len*/ 1, 0,
                    /*prevlen*/ 0, /*data*/ INT8_TAG, 0b10011100 /* -100 */,
                    /*zl end*/ 0xFF,
                ],
            },
            TestData {
                entries: vec![ZipEntry::Int16(1000)],
                #[rustfmt::skip]
                expected: vec![
                    /*zl bytes*/ 15, 0, 0, 0, /*zl tail*/ 10, 0, 0, 0, /*zl len*/ 1, 0,
                    /*prevlen*/ 0, /*data*/ INT16_TAG, 0xE8, 0x03,
                    /*zl end*/ 0xFF,
                ],
            },
            TestData {
                entries: vec![ZipEntry::Int24(8388607)],
                #[rustfmt::skip]
                expected: vec![
                    /*zl bytes*/ 16, 0, 0, 0, /*zl tail*/ 10, 0, 0, 0, /*zl len*/ 1, 0,
                    /*prevlen*/ 0, /*data*/ INT24_TAG, 0xFF, 0xFF, 0x7F,
                    /*zl end*/ 0xFF,
                ],
            },
            TestData {
                entries: vec![ZipEntry::Int32(2147483647)],
                #[rustfmt::skip]
                expected: vec![
                    /*zl bytes*/ 17, 0, 0, 0, /*zl tail*/ 10, 0, 0, 0, /*zl len*/ 1, 0,
                    /*prevlen*/ 0, /*data*/ INT32_TAG, 0xFF, 0xFF, 0xFF, 0x7F,
                    /*zl end*/ 0xFF,
                ],
            },
            TestData {
                entries: vec![ZipEntry::Int64(5000000000)],
                #[rustfmt::skip]
                expected: vec![
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
                #[rustfmt::skip]
                expected: vec![
                    /*zl bytes*/ 41, 0, 0, 0, /*zl tail*/ 30, 0, 0, 0, /*zl len*/ 6, 0,
                    /*prevlen*/ 0, /*data + tag*/ 0b1111_0110,

                    /*prevlen*/ 2, /*data + tag*/ INT8_TAG, 100,

                    /*prevlen*/ 3, /*data + tag*/ INT16_TAG, 0xE8, 0x03,

                    /*prevlen*/ 4, /*data + tag*/ INT24_TAG, 0xFF, 0xFF, 0x7F,

                    /*prevlen*/ 5, /*data + tag*/ INT32_TAG, 0xFF, 0xFF, 0xFF, 0x7F,

                    /*prevlen*/ 6, /*data + tag*/ INT64_TAG, 0x00, 0xF2, 0x05, 0x2A, 0x01, 0x00, 0x00, 0x00,

                    /*zl end*/ 0xFF,
                ],
            },
            TestData {
                entries: vec![
                    ZipEntry::Int8(-100),
                    ZipEntry::Int16(-1000),
                    ZipEntry::Int24(-8388607),
                    ZipEntry::Int32(-2147483647),
                    ZipEntry::Int64(-5000000000),
                ],
                #[rustfmt::skip]
                expected: vec![
                    /*zl bytes*/ 39, 0, 0, 0, /*zl tail*/ 28, 0, 0, 0, /*zl len*/ 5, 0,
                    /*prevlen*/ 0, /*data + tag*/ INT8_TAG, 156,

                    /*prevlen*/ 3, /*data + tag*/ INT16_TAG, 0x18, 0xFC,

                    /*prevlen*/ 4, /*data + tag*/ INT24_TAG, 0x01, 0x00, 0x80,

                    /*prevlen*/ 5, /*data + tag*/ INT32_TAG, 0x01, 0x00, 0x00, 0x80,

                    /*prevlen*/ 6, /*data + tag*/ INT64_TAG, 0x00, 0x0E, 0xFA, 0xD5, 0xFE, 0xFF, 0xFF, 0xFF,

                    /*zl end*/ 0xFF,
                ],
            },
            // string tests
            TestData {
                entries: vec![
                    ZipEntry::Str6BitsLength(b"Hello World".to_vec().into_boxed_slice()),
                    ZipEntry::Str14BitsLength(Box::new([b'a'; 70])),
                ],
                #[rustfmt::skip]
                expected:{
                    let mut e = vec![
                        /*zl bytes*/ 97, 0x00, 0x00, 0x00, /*zl tail*/ 23, 0x00, 0x00, 0x00, /*zl len*/ 2, 0,
                        /*prevlen*/ 0,
                        /*tag*/ 0b00_001011,
                        /*data*/ 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64,
                        /*prevlen*/ 13,
                        /*tag*/ 0b01_000000, 0b0_1000110,
                    ];
                    e.extend_from_slice(&[b'a'; 70]); // add 70 bytes string
                    e.push(0xFF);
                    e
                },
            },
            // long string test
            TestData {
                entries: vec![
                    ZipEntry::Str6BitsLength(b"Hello World".to_vec().into_boxed_slice()),
                    ZipEntry::Str14BitsLength(Box::new([b'a'; 70])),
                    ZipEntry::Str32BitsLength(Box::new([b'b'; 70_000])),
                    ZipEntry::Str32BitsLength(Box::new([b'c'; 70_000])),
                ],
                #[rustfmt::skip]
                expected:{
                    let mut e = vec![
                    /*zl bytes*/ 0x51, 0x23, 0x02, 0x00, /*zl tail*/ 0xD6, 0x11, 0x01, 0x00, /*zl len*/ 4, 0,
                    /*prevlen*/ 0,
                    /*tag*/ 0b00_001011,
                    /*data*/ 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64,
                    /*prevlen*/ 13,
                    /*tag*/ 0b01_000000, 0b0_1000110,
                    ];
                    e.extend_from_slice(&[b'a'; 70]); // add 70 bytes string
                    e.extend_from_slice(&[            // next entry
                    /*prevlen*/ 73,
                    /*tag*/ STR32_TAG, 0x00, 0x01, 0x11, 0x70
                    ]);
                    e.extend_from_slice(&[b'b'; 70_000]);   // add 70 000 bytes string
                    e.extend_from_slice(&[                  // next entry
                    /*prevlen*/ 0xFE, 0x76, 0x11, 0x01, 0x00,
                    /*tag*/ STR32_TAG, 0x00, 0x01, 0x11, 0x70
                    ]);
                    e.extend_from_slice(&[b'c'; 70_000]);   // add 70 000 bytes string
                    e.push(0xFF);
                    e
                },
            },
        ];

        for test in tests {
            let mut zl = ZipList::new();
            for entry in test.entries {
                zl.push(entry);
            }

            assert_eq!(&test.expected, &zl.data);

            println!(
                "expected headers: {:?}\ngot headers: {:?}",
                &test.expected[..10],
                &zl.data[..10]
            );
        }
    }

    #[test]
    fn test_zip_list_insert() {
        struct InsertEntry {
            entry: ZipEntry,
            index: usize,
        }

        struct TestData {
            entries: Vec<InsertEntry>,
            expected: Vec<u8>,
        }

        let tests = vec![
            TestData {
                entries: vec![InsertEntry {
                    entry: ZipEntry::Int4BitsImmediate(5),
                    index: 0,
                }],
                #[rustfmt::skip]
                expected: vec![
                    /*zl bytes*/ 13, 0, 0, 0, /*zl tail*/ 10, 0, 0, 0, /*zl len*/ 1, 0,
                    /*prevlen*/ 0, /*data + tag*/ 0b1111_0110,
                    /*zl end*/ 0xFF,
                ],
            },
            TestData {
                entries: vec![
                    InsertEntry {
                        entry: ZipEntry::Int4BitsImmediate(5),
                        index: 0,
                    },
                    InsertEntry {
                        entry: ZipEntry::Int4BitsImmediate(4),
                        index: 0,
                    },
                    InsertEntry {
                        entry: ZipEntry::Int4BitsImmediate(3),
                        index: 1,
                    },
                ],
                #[rustfmt::skip]
                expected: vec![
                    /*zl bytes*/ 17, 0, 0, 0, /*zl tail*/ 14, 0, 0, 0, /*zl len*/ 3, 0,
                    /*prevlen*/ 0, /*data + tag*/ 0b1111_0101,
                    /*prevlen*/ 2, /*data + tag*/ 0b1111_0100,
                    /*prevlen*/ 2, /*data + tag*/ 0b1111_0110,
                    /*zl end*/ 0xFF,
                ],
            },
            TestData {
                entries: vec![
                    InsertEntry {
                        entry: ZipEntry::Int8(50),
                        index: 0,
                    },
                    InsertEntry {
                        entry: ZipEntry::Int16(300),
                        index: 0,
                    },
                    InsertEntry {
                        entry: ZipEntry::Int24(8388607),
                        index: 1,
                    },
                    InsertEntry {
                        entry: ZipEntry::Int32(2147483647),
                        index: 2,
                    },
                    InsertEntry {
                        entry: ZipEntry::Int64(5000000000),
                        index: 1,
                    },
                    InsertEntry {
                        entry: ZipEntry::Int4BitsImmediate(4),
                        index: 3,
                    },
                ],
                #[rustfmt::skip]
                expected: vec![
                    /*zl bytes*/ 41, 0, 0, 0, /*zl tail*/ 37, 0, 0, 0, /*zl len*/ 6, 0,

                    /*prevlen*/ 0, /*data + tag*/ INT16_TAG, 0x2C, 0x01,

                    /*prevlen*/ 4, /*data + tag*/ INT64_TAG, 0x00, 0xF2, 0x05, 0x2A, 0x01, 0x00, 0x00, 0x00,

                    /*prevlen*/ 10, /*data + tag*/ INT24_TAG, 0xFF, 0xFF, 0x7F,

                    /*prevlen*/ 5, /*data + tag*/ 0b1111_0101,

                    /*prevlen*/ 2, /*data + tag*/ INT32_TAG, 0xFF, 0xFF, 0xFF, 0x7F,

                    /*prevlen*/ 6, /*data + tag*/ INT8_TAG, 50,

                    /*zl end*/ 0xFF,
                ],
            },
            TestData {
                entries: vec![
                    InsertEntry {
                        entry: ZipEntry::Int24(-8388607),
                        index: 0,
                    },
                    InsertEntry {
                        entry: ZipEntry::Int8(-100),
                        index: 0,
                    },
                    InsertEntry {
                        entry: ZipEntry::Int64(-5000000000),
                        index: 2,
                    },
                    InsertEntry {
                        entry: ZipEntry::Int16(-1000),
                        index: 1,
                    },
                    InsertEntry {
                        entry: ZipEntry::Int32(-2147483647),
                        index: 3,
                    },
                ],
                #[rustfmt::skip]
                expected: vec![
                    /*zl bytes*/ 39, 0, 0, 0, /*zl tail*/ 28, 0, 0, 0, /*zl len*/ 5, 0,
                    /*prevlen*/ 0, /*data + tag*/ INT8_TAG, 156,

                    /*prevlen*/ 3, /*data + tag*/ INT16_TAG, 0x18, 0xFC,

                    /*prevlen*/ 4, /*data + tag*/ INT24_TAG, 0x01, 0x00, 0x80,

                    /*prevlen*/ 5, /*data + tag*/ INT32_TAG, 0x01, 0x00, 0x00, 0x80,

                    /*prevlen*/ 6, /*data + tag*/ INT64_TAG, 0x00, 0x0E, 0xFA, 0xD5, 0xFE, 0xFF, 0xFF, 0xFF,

                    /*zl end*/ 0xFF,
                ],
            },
            // string tests
            TestData {
                entries: vec![
                    InsertEntry {
                        entry: ZipEntry::Str32BitsLength(Box::new([b'b'; 70_000])),
                        index: 0,
                    },
                    InsertEntry {
                        entry: ZipEntry::Str14BitsLength(Box::new([b'a'; 70])),

                        index: 0,
                    },
                    InsertEntry {
                        entry: ZipEntry::Str6BitsLength(b"Hello World".to_vec().into_boxed_slice()),
                        index: 0,
                    },
                ],
                #[rustfmt::skip]
                expected:{
                    let mut e = vec![
                    /*zl bytes*/ 0xD7, 0x11, 0x01, 0x00, /*zl tail*/ 0x60, 0x00, 0x00, 0x00, /*zl len*/ 3, 0,
                    /*prevlen*/ 0,
                    /*tag*/ 0b00_001011,
                    /*data*/ 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64,
                    /*prevlen*/ 13,
                    /*tag*/ 0b01_000000, 0b0_1000110,
                    ];
                    e.extend_from_slice(&[b'a'; 70]); // add 70 bytes string
                    e.extend_from_slice(&[            // next entry
                    /*prevlen*/ 73,
                    /*tag*/ STR32_TAG, 0x00, 0x01, 0x11, 0x70
                    ]);
                    e.extend_from_slice(&[b'b'; 70_000]);   // add 70 000 bytes string
                    e.push(0xFF);
                    e
                },
            },
            TestData {
                entries: vec![
                    InsertEntry {
                        entry: ZipEntry::Str6BitsLength(b"Hello World".to_vec().into_boxed_slice()),
                        index: 0,
                    },
                    InsertEntry {
                        entry: ZipEntry::Str14BitsLength(Box::new([b'a'; 70])),
                        index: 1,
                    },
                    InsertEntry {
                        entry: ZipEntry::Str32BitsLength(Box::new([b'b'; 70_000])),
                        index: 2,
                    },
                    InsertEntry {
                        entry: ZipEntry::Str32BitsLength(Box::new([b'c'; 70_000])),
                        index: 3,
                    },
                ],
                #[rustfmt::skip]
                expected:{
                    let mut e = vec![
                    /*zl bytes*/ 0x51, 0x23, 0x02, 0x00, /*zl tail*/ 0xD6, 0x11, 0x01, 0x00, /*zl len*/ 4, 0,
                    /*prevlen*/ 0,
                    /*tag*/ 0b00_001011,
                    /*data*/ 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64,
                    /*prevlen*/ 13,
                    /*tag*/ 0b01_000000, 0b0_1000110,
                    ];
                    e.extend_from_slice(&[b'a'; 70]); // add 70 bytes string
                    e.extend_from_slice(&[            // next entry
                    /*prevlen*/ 73,
                    /*tag*/ STR32_TAG, 0x00, 0x01, 0x11, 0x70
                    ]);
                    e.extend_from_slice(&[b'b'; 70_000]);   // add 70 000 bytes string
                    e.extend_from_slice(&[                  // next entry
                    /*prevlen*/ 0xFE, 0x76, 0x11, 0x01, 0x00,
                    /*tag*/ STR32_TAG, 0x00, 0x01, 0x11, 0x70
                    ]);
                    e.extend_from_slice(&[b'c'; 70_000]);   // add 70 000 bytes string
                    e.push(0xFF);
                    e
                },
            },
        ];

        for test in tests {
            let mut zl = ZipList::new();
            for entry in test.entries {
                zl.insert(entry.index, entry.entry);
            }

            assert_eq!(&test.expected, &zl.data);
        }
    }

    #[test]
    fn test_zip_list_delete() {
        struct TestData {
            init_state: Vec<u8>,
            deletions: Vec<usize>,
            expected: Vec<u8>,
        }

        let tests = vec![
            // int tests
            TestData {
                #[rustfmt::skip]
                init_state: vec![
                    /*zl bytes*/ 13, 0, 0, 0, /*zl tail*/ 10, 0, 0, 0, /*zl len*/ 1, 0,
                    /*prevlen*/ 0, /*data + tag*/ 0b1111_0110,
                    /*zl end*/ 0xFF,
                ],
                deletions: vec![0],
                #[rustfmt::skip]
                expected: vec![
                    /*zl bytes*/ 11, 0, 0, 0, /*zl tail*/ 10, 0, 0, 0, /*zl len*/ 0, 0,
                    /*zl end*/ 0xFF,

                ],
            },
            TestData {
                #[rustfmt::skip]
                init_state: vec![
                    /*zl bytes*/ 41, 0, 0, 0, /*zl tail*/ 30, 0, 0, 0, /*zl len*/ 6, 0,
                    /*prevlen*/ 0, /*data + tag*/ 0b1111_0110,

                    /*prevlen*/ 2, /*data + tag*/ INT8_TAG, 100,

                    /*prevlen*/ 3, /*data + tag*/ INT16_TAG, 0xE8, 0x03,

                    /*prevlen*/ 4, /*data + tag*/ INT24_TAG, 0xFF, 0xFF, 0x7F,

                    /*prevlen*/ 5, /*data + tag*/ INT32_TAG, 0xFF, 0xFF, 0xFF, 0x7F,

                    /*prevlen*/ 6, /*data + tag*/ INT64_TAG, 0x00, 0xF2, 0x05, 0x2A, 0x01, 0x00, 0x00, 0x00,

                    /*zl end*/ 0xFF,
                ],
                deletions: vec![1, 4],
                #[rustfmt::skip]
                expected: vec![
                    /*zl bytes*/ 28, 0, 0, 0, /*zl tail*/ 21, 0, 0, 0, /*zl len*/ 4, 0,
                    /*prevlen*/ 0, /*data + tag*/ 0b1111_0110,

                    /*prevlen*/ 2, /*data + tag*/ INT16_TAG, 0xE8, 0x03,

                    /*prevlen*/ 4, /*data + tag*/ INT24_TAG, 0xFF, 0xFF, 0x7F,

                    /*prevlen*/ 5, /*data + tag*/ INT32_TAG, 0xFF, 0xFF, 0xFF, 0x7F,

                    /*zl end*/ 0xFF,

                ],
            },
            TestData {
                #[rustfmt::skip]
                init_state: vec![
                    /*zl bytes*/ 41, 0, 0, 0, /*zl tail*/ 30, 0, 0, 0, /*zl len*/ 6, 0,
                    /*prevlen*/ 0, /*data + tag*/ 0b1111_0110,

                    /*prevlen*/ 2, /*data + tag*/ INT8_TAG, 100,

                    /*prevlen*/ 3, /*data + tag*/ INT16_TAG, 0xE8, 0x03,

                    /*prevlen*/ 4, /*data + tag*/ INT24_TAG, 0xFF, 0xFF, 0x7F,

                    /*prevlen*/ 5, /*data + tag*/ INT32_TAG, 0xFF, 0xFF, 0xFF, 0x7F,

                    /*prevlen*/ 6, /*data + tag*/ INT64_TAG, 0x00, 0xF2, 0x05, 0x2A, 0x01, 0x00, 0x00, 0x00,

                    /*zl end*/ 0xFF,
                ],
                deletions: vec![3, 4, 0, 1, 0, 0],
                #[rustfmt::skip]
                expected: vec![
                    /*zl bytes*/ 11, 0, 0, 0, /*zl tail*/ 10, 0, 0, 0, /*zl len*/ 0, 0,
                    /*zl end*/ 0xFF,
                ],
            },
            // string tests
            TestData {
                #[rustfmt::skip]
                init_state:{
                    let mut e = vec![
                    /*zl bytes*/ 100, 0x00, 0x00, 0x00, /*zl tail*/ 96, 0x00, 0x00, 0x00, /*zl len*/ 3, 0,
                    /*prevlen*/ 0,
                    /*tag*/ 0b00_001011,
                    /*data*/ 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64,
                    /*prevlen*/ 13,
                    /*tag*/ 0b01_000000, 0b0_1000110,
                    ];
                    e.extend_from_slice(&[b'a'; 70]); // add 70 bytes string
                    e.extend_from_slice(&[            // next entry
                    /*prevlen*/ 73,
                    /*tag + data*/ INT8_TAG, 5,
                    ]);
                    e.push(0xFF);
                    e
                },
                deletions: vec![1, 1],
                #[rustfmt::skip]
                expected: {
                    let mut e = vec![
                    /*zl bytes*/ 24, 0x00, 0x00, 0x00, /*zl tail*/ 10, 0x00, 0x00, 0x00, /*zl len*/ 1, 0,
                    /*prevlen*/ 0,
                    /*tag*/ 0b00_001011,
                    /*data*/ 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64,
                    ];
                    e.push(0xFF);
                    e
                },
            },
            TestData {
                #[rustfmt::skip]
                init_state: vec![
                    /*zl bytes*/ 50, 0x00, 0x00, 0x00, /*zl tail*/ 36, 0x00, 0x00, 0x00, /*zl len*/ 3, 0,
                    /*prevlen*/ 0,
                    /*tag*/ 0b00_001011,
                    /*data*/ 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64,
                    /*prevlen*/ 13,
                    /*tag*/ 0b00_001011,
                    /*data*/ 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64,
                    /*prevlen*/ 13,
                    /*tag*/ 0b00_001011,
                    /*data*/ 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64,
                    /*end*/ 0xFF
                ],
                deletions: vec![1, 1],
                #[rustfmt::skip]
                expected:vec![
                    /*zl bytes*/ 24, 0x00, 0x00, 0x00, /*zl tail*/ 10, 0x00, 0x00, 0x00, /*zl len*/ 1, 0,
                    /*prevlen*/ 0,
                    /*tag*/ 0b00_001011,
                    /*data*/ 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64,
                    /*end*/ 0xFF
                ],
            },
            TestData {
                #[rustfmt::skip]
                init_state:{
                    let mut e = vec![
                    /*zl bytes*/ 0x51, 0x23, 0x02, 0x00, /*zl tail*/ 0xD6, 0x11, 0x01, 0x00, /*zl len*/ 4, 0,
                    /*prevlen*/ 0,
                    /*tag*/ 0b00_001011,
                    /*data*/ 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64,
                    /*prevlen*/ 13,
                    /*tag*/ 0b01_000000, 0b0_1000110,
                    ];
                    e.extend_from_slice(&[b'a'; 70]); // add 70 bytes string
                    e.extend_from_slice(&[            // next entry
                    /*prevlen*/ 73,
                    /*tag*/ STR32_TAG, 0x00, 0x01, 0x11, 0x70
                    ]);
                    e.extend_from_slice(&[b'b'; 70_000]);   // add 70 000 bytes string
                    e.extend_from_slice(&[                  // next entry
                    /*prevlen*/ 0xFE, 0x76, 0x11, 0x01, 0x00,
                    /*tag*/ STR32_TAG, 0x00, 0x01, 0x11, 0x70
                    ]);
                    e.extend_from_slice(&[b'c'; 70_000]);   // add 70 000 bytes string
                    e.push(0xFF);
                    e
                },
                deletions: vec![2, 2],
                #[rustfmt::skip]
                expected: {
                    let mut e = vec![
                    /*zl bytes*/ 97, 0x00, 0x00, 0x00, /*zl tail*/ 23, 0x00, 0x00, 0x00, /*zl len*/ 2, 0,
                    /*prevlen*/ 0,
                    /*tag*/ 0b00_001011,
                    /*data*/ 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64,
                    /*prevlen*/ 13,
                    /*tag*/ 0b01_000000, 0b0_1000110,
                    ];
                    e.extend_from_slice(&[b'a'; 70]); // add 70 bytes string
                    e.push(0xFF);
                    e
                },
            },
        ];

        for test in tests {
            let mut zl = ZipList::new();
            zl.data = test.init_state;
            for index in test.deletions {
                zl.delete(index);
            }

            assert_eq!(&test.expected, &zl.data);
        }
    }

    #[test]
    fn test_zip_list_get() {
        struct TestData {
            init_state: Vec<u8>,
            get: Vec<usize>,
            expected: Vec<RedisObject>,
        }

        let tests = vec![
            TestData {
                #[rustfmt::skip]
                    init_state: vec![
                        /*zl bytes*/ 41, 0, 0, 0, /*zl tail*/ 30, 0, 0, 0, /*zl len*/ 6, 0,
                        /*prevlen*/ 0, /*data + tag*/ 0b1111_0110,

                        /*prevlen*/ 2, /*data + tag*/ INT8_TAG, 100,

                        /*prevlen*/ 3, /*data + tag*/ INT16_TAG, 0xE8, 0x03,

                        /*prevlen*/ 4, /*data + tag*/ INT24_TAG, 0xFF, 0xFF, 0x7F,

                        /*prevlen*/ 5, /*data + tag*/ INT32_TAG, 0xFF, 0xFF, 0xFF, 0x7F,

                        /*prevlen*/ 6, /*data + tag*/ INT64_TAG, 0x00, 0xF2, 0x05, 0x2A, 0x01, 0x00, 0x00, 0x00,

                        /*zl end*/ 0xFF,
                    ],
                get: vec![0, 1, 2, 3, 4, 5],
                expected: vec![
                    RedisObject::Int(5),
                    RedisObject::Int(100),
                    RedisObject::Int(1000),
                    RedisObject::Int(8388607),
                    RedisObject::Int(2147483647),
                    RedisObject::Int(5000000000),
                ],
            },
            TestData {
                #[rustfmt::skip]
                    init_state: vec![
                        /*zl bytes*/ 39, 0, 0, 0, /*zl tail*/ 28, 0, 0, 0, /*zl len*/ 5, 0,
                        /*prevlen*/ 0, /*data + tag*/ INT8_TAG, 156,

                        /*prevlen*/ 3, /*data + tag*/ INT16_TAG, 0x18, 0xFC,

                        /*prevlen*/ 4, /*data + tag*/ INT24_TAG, 0x01, 0x00, 0x80,

                        /*prevlen*/ 5, /*data + tag*/ INT32_TAG, 0x01, 0x00, 0x00, 0x80,

                        /*prevlen*/ 6, /*data + tag*/ INT64_TAG, 0x00, 0x0E, 0xFA, 0xD5, 0xFE, 0xFF, 0xFF, 0xFF,

                        /*zl end*/ 0xFF,
                    ],
                get: vec![0, 1, 2, 3, 4],
                expected: vec![
                    RedisObject::Int(-100),
                    RedisObject::Int(-1000),
                    RedisObject::Int(-8388607),
                    RedisObject::Int(-2147483647),
                    RedisObject::Int(-5000000000),
                ],
            },
            // get strings
            TestData {
                #[rustfmt::skip]
                    init_state:{
                        let mut e = vec![
                        /*zl bytes*/ 0x51, 0x23, 0x02, 0x00, /*zl tail*/ 0xD6, 0x11, 0x01, 0x00, /*zl len*/ 4, 0,
                        /*prevlen*/ 0,
                        /*tag*/ 0b00_001011,
                        /*data*/ 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64,
                        /*prevlen*/ 13,
                        /*tag*/ 0b01_000000, 0b0_1000110,
                        ];
                        e.extend_from_slice(&[b'a'; 70]); // add 70 bytes string
                        e.extend_from_slice(&[            // next entry
                        /*prevlen*/ 73,
                        /*tag*/ STR32_TAG, 0x00, 0x01, 0x11, 0x70
                        ]);
                        e.extend_from_slice(&[b'b'; 70_000]);   // add 70 000 bytes string
                        e.extend_from_slice(&[                  // next entry
                        /*prevlen*/ 0xFE, 0x76, 0x11, 0x01, 0x00,
                        /*tag*/ STR32_TAG, 0x00, 0x01, 0x11, 0x70
                        ]);
                        e.extend_from_slice(&[b'c'; 70_000]);   // add 70 000 bytes string
                        e.push(0xFF);
                        e
                    },
                get: vec![2, 3, 1, 0],
                expected: vec![
                    RedisObject::String([b'b'; 70_000].to_vec().into_boxed_slice()),
                    RedisObject::String([b'c'; 70_000].to_vec().into_boxed_slice()),
                    RedisObject::String([b'a'; 70].to_vec().into_boxed_slice()),
                    RedisObject::String(b"Hello World".to_vec().into_boxed_slice()),
                ],
            },
        ];

        for test in tests {
            let mut zl = ZipList::new();
            zl.data = test.init_state;

            for (index, expected) in test.get.iter().zip(test.expected) {
                let result = zl.get(*index);
                assert_eq!(expected, result);
            }
        }
    }
}
