use crate::redis::ziplist::{EncodingType, ZL_END, get_prevlen, get_prevlen_size};

pub struct ZipListIter<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> Iterator for ZipListIter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.data.len() - 1 || self.data[self.offset] == ZL_END {
            return None;
        }

        let current_offset = self.offset;

        // skip prevlen
        self.offset += get_prevlen_size(self.data[self.offset]);

        let encoding_type = EncodingType::from_header(self.data[self.offset]);

        match encoding_type {
            EncodingType::Int4BitsImmediate => self.offset += 1,
            EncodingType::Int8 => self.offset += 2,
            EncodingType::Int16 => self.offset += 3,
            EncodingType::Int24 => self.offset += 4,
            EncodingType::Int32 => self.offset += 5,
            EncodingType::Int64 => self.offset += 9,
            EncodingType::Str6BitsLength => {
                let str_len = (self.data[self.offset] & 0b00_111111) as usize;
                self.offset += 1;
                self.offset += str_len;
            }
            EncodingType::Str14BitsLength => {
                let b1 = self.data[self.offset] & 0b00_111111;
                let b2 = self.data[self.offset + 1];
                let str_len = u16::from_be_bytes([b1, b2]) as usize;
                self.offset += 2;
                self.offset += str_len;
            }
            EncodingType::Str32BitsLength => {
                let b1 = self.data[self.offset + 1];
                let b2 = self.data[self.offset + 2];
                let b3 = self.data[self.offset + 3];
                let b4 = self.data[self.offset + 4];
                let str_len = u32::from_be_bytes([b1, b2, b3, b4]) as usize;
                self.offset += 5;
                self.offset += str_len;
            }
        };

        Some(current_offset)
    }
}

pub struct ZipListIterRev<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> Iterator for ZipListIterRev<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset <= 10 {
            return None;
        }

        let current_offset = self.offset;

        self.offset -= get_prevlen(&self.data[self.offset..]);

        Some(current_offset)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
}
