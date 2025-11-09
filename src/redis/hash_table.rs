use crate::redis::redis_object::RedisObject;

pub enum ResizeState {
    NotResizing,
    Resizing {
        new_ht: HashTable,
        resizing_pos: usize,
    },
}

pub struct HashDict {
    main_ht: HashTable,
    state: ResizeState,
}

impl HashDict {
    fn help_resizing(&mut self, nwork: usize) {
        // let (new_ht, resizing_pos) = self.get_resize_state();

        match &mut self.state {
            ResizeState::Resizing {
                new_ht,
                resizing_pos,
            } => {
                for i in *resizing_pos..*resizing_pos + nwork {
                    let current_entry = match &self.main_ht.table[i] {
                        Some(hash_node) => hash_node,
                        None => continue,
                    };

                    let new_node = current_entry.clone();
                    new_ht.insert(new_node);
                }

                *resizing_pos += nwork;
            }
            ResizeState::NotResizing => {
                unreachable!("SHOULD HAVE CHECKED THAT IT IS RESIZING BEFORE THIS POINT")
            }
        }
    }

    #[inline(always)]
    fn is_resizing(&self) -> bool {
        match self.state {
            ResizeState::NotResizing => false,
            ResizeState::Resizing { .. } => true,
        }
    }
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

        let mut current = self.table[pos].as_mut();
        while let Some(existing_node) = current {
            if existing_node.key == node.key {
                existing_node.value = node.value;
                return;
            }

            current = existing_node.next.as_mut();
        }

        node.next = self.table[pos].take();

        self.table[pos] = Some(node);

        self.used += 1;
    }

    pub fn lookup(&mut self, key: &[u8]) -> Option<&HashNode> {
        let hash = hash_bytes(key);
        let pos = hash as usize & self.mask;

        let mut current = self.table[pos].as_deref()?;

        loop {
            if current.hash == hash && current.key.as_ref() == key {
                return Some(current);
            }
            current = current.next.as_deref()?;
        }
    }

    pub fn delete(&mut self, key: &[u8]) -> bool {
        let hash = hash_bytes(key);
        let pos = hash as usize & self.mask;

        let head = match self.table[pos].as_mut() {
            Some(hash_node) => hash_node,
            None => return false,
        };

        if head.hash == hash && head.key.as_ref() == key {
            self.table[pos] = head.next.take();
            self.used -= 1;
            return true;
        }

        let mut current = head;

        loop {
            let next_matches = match &current.next {
                Some(next) => next.hash == hash && next.key.as_ref() == key,
                None => return false,
            };

            if next_matches {
                let removed_node = current.next.as_mut().unwrap();
                current.next = removed_node.next.take();
                self.used -= 1;
                return true;
            }

            current = match current.next.as_mut() {
                Some(node) => node,
                None => return false,
            };
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct HashNode {
    key: Box<[u8]>,
    value: RedisObject,
    next: Option<Box<HashNode>>,
    hash: u64,
}

impl HashNode {
    pub fn new(key: &[u8], value: &[u8]) -> HashNode {
        let node_key = slice_to_box(key);
        let node_value = RedisObject::new_from_bytes(value);
        let node_hash = hash_bytes(key);

        HashNode {
            key: node_key,
            value: node_value,
            next: None,
            hash: node_hash,
        }
    }
}

// Helpers

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

    #[test]
    fn test_insert_lookup_and_delete() {
        struct TestData {
            key: &'static [u8],
            value: &'static [u8],
            expected: HashNode,
        }

        let tests = vec![
            TestData {
                key: b"key1",
                value: b"value1",
                expected: HashNode::new(b"key1", b"value1"),
            },
            TestData {
                key: b"key2",
                value: b"value2",
                expected: HashNode::new(b"key2", b"value2"),
            },
            TestData {
                key: b"key3",
                value: b"value3",
                expected: HashNode::new(b"key3", b"value3"),
            },
        ];

        let mut ht = HashTable::new(128);

        // insert nodes into hash table
        for test in &tests {
            ht.insert(Box::new(HashNode::new(test.key, test.value)));
        }

        assert_eq!(ht.used, 3);

        // extract nodes
        for test in &tests {
            let result = ht.lookup(test.key).unwrap();
            assert_eq!(&test.expected, result);
        }

        // delete nodes
        for test in &tests {
            let result = ht.delete(test.key);
            assert_eq!(true, result);

            let lookup = ht.lookup(test.key);
            assert!(lookup.is_none());
        }
    }

    #[test]
    fn test_overlapping_inserts() {
        struct TestData {
            key: &'static [u8],
            value: &'static [u8],
            expected: HashNode,
        }

        let tests = vec![
            TestData {
                key: b"key1",
                value: b"value1",
                expected: HashNode::new(b"key1", b"value1"),
            },
            TestData {
                key: b"key1",
                value: b"value2",
                expected: HashNode::new(b"key1", b"value2"),
            },
            TestData {
                key: b"key1",
                value: b"value3",
                expected: HashNode::new(b"key1", b"value3"),
            },
        ];

        let mut ht = HashTable::new(128);

        for test in tests {
            ht.insert(Box::new(HashNode::new(test.key, test.value)));
            let result = ht.lookup(test.key).unwrap();
            assert_eq!(&test.expected, result);
            assert_eq!(ht.used, 1);
        }
    }
}
