use std::collections::HashMap;

use crate::{commands::RedisCommand, error::RedisError};

pub enum RedisResult {
    SimpleString(&'static [u8]),
    BulkString(Vec<u8>),
    Error(RedisError),
}

pub struct Redis {
    store: HashMap<Vec<u8>, Vec<u8>>,
}

impl Redis {
    pub fn new() -> Self {
        Redis {
            store: HashMap::<Vec<u8>, Vec<u8>>::new(),
        }
    }

    pub fn execute_command(&mut self, command: &RedisCommand) -> RedisResult {
        match command {
            RedisCommand::Set { key, value } => RedisResult::SimpleString(b"+OK\r\n"),
            RedisCommand::Get { key } => RedisResult::BulkString(b"$-1\r\n".to_vec()),
            RedisCommand::Del { key } => RedisResult::BulkString(b"$-1\r\n".to_vec()),
        }
    }
}

#[derive(Clone)]
pub struct HashNode {
    key: Box<[u8]>,
    value: RedisObject,
    next: Option<Box<HashNode>>,
    hash: u64,
}

pub struct HashTable {
    table: Vec<Option<Box<HashNode>>>,
    used: usize,
    mask: usize,
}

impl HashTable {
    pub fn new(size: usize) -> Self {
        assert!(size > 0 && ((size - 1) & size) == 0);
        let table: Vec<Option<Box<HashNode>>> = vec![None; size];

        HashTable {
            table: table,
            used: 0,
            mask: size - 1,
        }
    }

    pub fn insert(&mut self, mut node: Box<HashNode>) {
        let pos = (node.hash as usize) & self.mask;

        node.next = self.table[pos].take();

        self.table[pos] = Some(node);

        self.used += 1;
    }

    pub fn lookup(&mut self, key: &[u8]) -> Option<&HashNode> {
        let hash = hash_bytes(key) as usize;
        let pos = hash & self.mask;

        let mut current = self.table[pos].as_deref()?;

        loop {
            if current.key.as_ref() == key {
                return Some(current);
            }
            current = current.next.as_deref()?;
        }
    }
}

#[derive(Clone)]
enum ValueType {
    String,
    Int,
}

#[derive(Clone)]
enum RedisValue {
    String(Vec<u8>),
    Int(i64),
}

#[derive(Clone)]
struct RedisObject {
    value_type: ValueType,
    value: RedisValue,
}

// helper

fn slice_to_box(slice: &[u8]) -> Box<[u8]> {
    let len = slice.len();
    let layout = std::alloc::Layout::array::<u8>(len).unwrap();

    unsafe {
        let ptr = std::alloc::alloc(layout);
        std::ptr::copy_nonoverlapping(slice.as_ptr(), ptr, len);
        Box::from_raw(std::slice::from_raw_parts_mut(ptr, len))
    }
}

// FNV HASH FROM "Writing your own redis in C"
fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut h: u32 = 0x811C9DC5;

    for &byte in bytes {
        h = h.wrapping_add(byte as u32).wrapping_mul(0x01000193);
    }

    h as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slice_to_box() {
        struct TestData {
            slice: &'static [u8],
            expected: Box<[u8]>,
        }

        let tests = vec![
            TestData {
                slice: &[1, 2, 3, 4, 5],
                expected: Box::new([1, 2, 3, 4, 5]),
            },
            TestData {
                slice: &[10, 99, 123, 72, 11],
                expected: Box::new([10, 99, 123, 72, 11]),
            },
            TestData {
                slice: &[],
                expected: Box::new([]),
            },
        ];

        for test in tests {
            let result = slice_to_box(test.slice);
            assert_eq!(test.expected, result);
        }
    }
}
