use crate::redis::redis_object::RedisObject;

const REHASHING_SPEED: usize = 1;
const MAX_LOAD_FACTOR: usize = 1;
const INIT_HT_SIZE: usize = 4;

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
    pub fn new() -> Self {
        HashDict {
            main_ht: HashTable::new(INIT_HT_SIZE),
            state: ResizeState::NotResizing,
        }
    }

    pub fn insert(&mut self, node: Box<HashNode>) {
        self.try_finish_resizing();

        match &mut self.state {
            ResizeState::NotResizing => {
                self.main_ht.insert(node);

                // check if resize is needed
                let load_factor = self.main_ht.used / (self.main_ht.mask + 1);
                if load_factor >= MAX_LOAD_FACTOR {
                    self.start_resizing();
                }
            }
            ResizeState::Resizing {
                new_ht,
                resizing_pos,
            } => {
                Self::help_resizing(&mut self.main_ht, new_ht, resizing_pos, REHASHING_SPEED);
                new_ht.insert(node);
            }
        }
    }

    pub fn lookup(&mut self, key: &[u8]) -> Option<&RedisObject> {
        self.try_finish_resizing();

        match &mut self.state {
            ResizeState::NotResizing => self.main_ht.lookup(key),
            ResizeState::Resizing {
                new_ht,
                resizing_pos,
            } => {
                Self::help_resizing(&mut self.main_ht, new_ht, resizing_pos, REHASHING_SPEED);
                new_ht.lookup(key).or_else(|| self.main_ht.lookup(key))
            }
        }
    }

    pub fn lookup_mut(&mut self, key: &[u8]) -> Option<&mut RedisObject> {
        self.try_finish_resizing();

        match &mut self.state {
            ResizeState::NotResizing => self.main_ht.lookup_mut(key),
            ResizeState::Resizing {
                new_ht,
                resizing_pos,
            } => {
                Self::help_resizing(&mut self.main_ht, new_ht, resizing_pos, REHASHING_SPEED);
                new_ht.lookup_mut(key).or_else(|| self.main_ht.lookup_mut(key))
            }
        }
    }

    pub fn delete(&mut self, key: &[u8]) -> bool {
        self.try_finish_resizing();

        match &mut self.state {
            ResizeState::NotResizing => self.main_ht.delete(key),
            ResizeState::Resizing {
                new_ht,
                resizing_pos,
            } => {
                Self::help_resizing(&mut self.main_ht, new_ht, resizing_pos, REHASHING_SPEED);
                let main_result = self.main_ht.delete(key);
                let new_result = new_ht.delete(key);
                main_result || new_result
            }
        }
    }

    fn help_resizing(
        main_ht: &mut HashTable,
        new_ht: &mut HashTable,
        resizing_pos: &mut usize,
        nwork: usize,
    ) {
        let mut amount_non_empty_buckets = 0;
        let ceiling = *resizing_pos + nwork * 10;

        while *resizing_pos < ceiling {
            if *resizing_pos >= main_ht.table.len() {
                break;
            }

            let mut current_entry = match main_ht.table[*resizing_pos].take() {
                Some(hash_node) => {
                    *resizing_pos += 1;
                    amount_non_empty_buckets += 1;
                    hash_node
                }
                None => {
                    *resizing_pos += 1;
                    continue;
                }
            };

            // insert all entries from the linked list in the new bucket
            loop {
                let next = current_entry.next.take();
                new_ht.insert(current_entry);

                match next {
                    Some(hash_node) => current_entry = hash_node,
                    None => break,
                }
            }

            if amount_non_empty_buckets >= nwork {
                break;
            }
        }
    }

    #[inline(always)]
    fn start_resizing(&mut self) {
        self.state = ResizeState::Resizing {
            new_ht: HashTable::new(self.main_ht.table.capacity() * 2),
            resizing_pos: 0,
        };
    }

    #[inline]
    fn try_finish_resizing(&mut self) {
        match &mut self.state {
            ResizeState::Resizing {
                new_ht,
                resizing_pos,
            } => {
                if *resizing_pos >= self.main_ht.table.capacity() {
                    std::mem::swap(&mut self.main_ht, new_ht);
                    self.state = ResizeState::NotResizing;
                }
            }
            ResizeState::NotResizing => {}
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

    fn insert(&mut self, mut node: Box<HashNode>) {
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

    fn lookup(&mut self, key: &[u8]) -> Option<&RedisObject> {
        let hash = hash_bytes(key);
        let pos = hash as usize & self.mask;

        let mut current = self.table[pos].as_deref()?;

        loop {
            if current.hash == hash && current.key.as_ref() == key {
                return Some(&current.value);
            }
            current = current.next.as_deref()?;
        }
    }

    fn lookup_mut(&mut self, key: &[u8]) -> Option<&mut RedisObject> {
        let hash = hash_bytes(key);
        let pos = hash as usize & self.mask;

        let mut current = self.table[pos].as_deref_mut()?;

        loop {
            if current.hash == hash && current.key.as_ref() == key {
                return Some(&mut current.value);
            }
            current = current.next.as_deref_mut()?;
        }
    }

    fn delete(&mut self, key: &[u8]) -> bool {
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
    pub value: RedisObject,
    next: Option<Box<HashNode>>,
    hash: u64,
}

impl HashNode {
    pub fn new_from_object(key: &[u8], value: RedisObject) -> HashNode {
        let node_key = slice_to_box(key);
        let node_hash = hash_bytes(key);

        HashNode {
            key: node_key,
            value: value,
            next: None,
            hash: node_hash,
        }
    }

    pub fn new_from_bytes(key: &[u8], value: &[u8]) -> HashNode {
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
                expected: HashNode::new_from_bytes(b"key1", b"value1"),
            },
            TestData {
                key: b"key2",
                value: b"value2",
                expected: HashNode::new_from_bytes(b"key2", b"value2"),
            },
            TestData {
                key: b"key3",
                value: b"value3",
                expected: HashNode::new_from_bytes(b"key3", b"value3"),
            },
        ];

        let mut ht = HashTable::new(128);

        // insert nodes into hash table
        for test in &tests {
            ht.insert(Box::new(HashNode::new_from_bytes(test.key, test.value)));
        }

        assert_eq!(ht.used, 3);

        // extract nodes
        for test in &tests {
            let result = ht.lookup(test.key).unwrap();
            assert_eq!(&test.expected.value, result);
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
                expected: HashNode::new_from_bytes(b"key1", b"value1"),
            },
            TestData {
                key: b"key1",
                value: b"value2",
                expected: HashNode::new_from_bytes(b"key1", b"value2"),
            },
            TestData {
                key: b"key1",
                value: b"value3",
                expected: HashNode::new_from_bytes(b"key1", b"value3"),
            },
        ];

        let mut ht = HashTable::new(128);

        for test in tests {
            ht.insert(Box::new(HashNode::new_from_bytes(test.key, test.value)));
            let result = ht.lookup(test.key).unwrap();
            assert_eq!(&test.expected.value, result);
            assert_eq!(ht.used, 1);
        }
    }

    #[test]
    fn test_resizing_hash_dict() {
        let mut hash_dict = HashDict::new();
        let mut inserted = 0;

        // insert until resizing is triggerd
        loop {
            let key_str = format!("key{}", inserted);
            let value_str = format!("value{}", inserted);
            let node = Box::new(HashNode::new_from_bytes(
                key_str.as_bytes(),
                value_str.as_bytes(),
            ));
            hash_dict.insert(node);
            inserted += 1;

            match hash_dict.state {
                ResizeState::Resizing { .. } => {
                    break;
                }
                ResizeState::NotResizing => {}
            }
        }

        // misc lookups until resizing is done
        loop {
            let key_str = format!("key{}", inserted);
            let _ = hash_dict.lookup(key_str.as_bytes());

            match hash_dict.state {
                ResizeState::Resizing { .. } => {}
                ResizeState::NotResizing => {
                    break;
                }
            }
        }

        let expected_capacity = 2 * INIT_HT_SIZE;
        assert_eq!(expected_capacity, hash_dict.main_ht.table.capacity());

        for i in 0..inserted {
            let key_str = format!("key{}", i);

            let value_str = format!("value{}", i);
            let expected = RedisObject::new_from_bytes(value_str.as_bytes());

            let redis_object = hash_dict
                .lookup(key_str.as_bytes())
                .expect(&format!("Missing key {}", key_str));

            assert_eq!(expected, redis_object.clone());
        }
    }
}
