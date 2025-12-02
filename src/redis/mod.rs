mod hash_table;
mod redis_object;
mod zip_list;

use crate::{
    commands::RedisCommand,
    error::RedisError,
    redis::{
        hash_table::{HashDict, HashNode},
        redis_object::RedisObject,
        zip_list::{ZipEntry, ZipList},
    },
};

pub enum RedisResult {
    SimpleString(&'static [u8]),
    BulkString(Vec<u8>),
    Int(i64),
    Error(RedisError),
}

pub struct Redis {
    dict: HashDict,
}

impl Redis {
    pub fn new() -> Self {
        Redis {
            dict: HashDict::new(),
        }
    }

    pub fn execute_command(&mut self, command: &RedisCommand) -> RedisResult {
        match command {
            RedisCommand::Set { key, value } => {
                let node = Box::new(HashNode::new_from_bytes(key, value));
                self.dict.insert(node);
                RedisResult::SimpleString(b"+OK\r\n")
            }
            RedisCommand::Get { key } => {
                let lookup_node = self.dict.lookup(key);
                match lookup_node {
                    Some(value) => {
                        let response = value.to_resp();
                        RedisResult::BulkString(response)
                    }
                    None => RedisResult::BulkString(b"$-1\r\n".to_vec()),
                }
            }
            RedisCommand::Del { keys } => {
                let mut amount_deletions: i64 = 0;
                for key in keys {
                    let result = self.dict.delete(key);

                    if result == true {
                        amount_deletions += 1;
                    }
                }

                RedisResult::Int(amount_deletions)
            }
            // code duplication for these two but i think it is the most optimal way could be
            // solved with macros or a function that takes a bool or something although this could
            // create extra unecisary branching
            RedisCommand::LPush { key, value } => {
                let value_object = RedisObject::new_from_bytes(value);
                let zip_entry = ZipEntry::from_redis_object(value_object.clone());
                let possible_node = self.dict.lookup_mut(key);
                match possible_node {
                    // insert into list
                    Some(node) => match node {
                        RedisObject::List(list) => {
                            list.insert(0, zip_entry);
                            RedisResult::SimpleString(b"+OK\r\n")
                        }
                        _ => return RedisResult::BulkString(b"$-1\r\n".to_vec()),
                    },
                    // create the list
                    None => {
                        let mut new_zip_list = ZipList::new();
                        let zip_entry = ZipEntry::from_redis_object(value_object);
                        new_zip_list.push(zip_entry);
                        let value_object = RedisObject::List(new_zip_list);

                        let new_node = HashNode::new_from_object(key, value_object);

                        self.dict.insert(Box::new(new_node));

                        RedisResult::SimpleString(b"+OK\r\n")
                    }
                }
            }
            RedisCommand::RPush { key, value } => {
                let value_object = RedisObject::new_from_bytes(value);
                let zip_entry = ZipEntry::from_redis_object(value_object.clone());
                let possible_node = self.dict.lookup_mut(key);
                match possible_node {
                    // insert into list
                    Some(node) => match node {
                        RedisObject::List(list) => {
                            list.push(zip_entry);
                            RedisResult::SimpleString(b"+OK\r\n")
                        }
                        _ => return RedisResult::BulkString(b"$-1\r\n".to_vec()),
                    },
                    // create the list
                    None => {
                        let mut new_zip_list = ZipList::new();
                        let zip_entry = ZipEntry::from_redis_object(value_object);
                        new_zip_list.push(zip_entry);
                        let value_object = RedisObject::List(new_zip_list);

                        let new_node = HashNode::new_from_object(key, value_object);

                        self.dict.insert(Box::new(new_node));

                        RedisResult::SimpleString(b"+OK\r\n")
                    }
                }
            }
            RedisCommand::LPop { key } => {
                let possible_node = self.dict.lookup_mut(key);
                match possible_node {
                    Some(redis_object) => match redis_object {
                        RedisObject::List(list) => {
                            let value = list.pop_head();
                            let response = value.to_resp();
                            RedisResult::BulkString(response)
                        }
                        _ => todo!("implement error stuff"),
                    },
                    None => {
                        todo!("implement error stuff here");
                    }
                }
            }
            RedisCommand::RPop { key } => {
                let possible_node = self.dict.lookup_mut(key);
                match possible_node {
                    Some(redis_object) => match redis_object {
                        RedisObject::List(list) => {
                            let value = list.pop_tail();
                            let response = value.to_resp();
                            RedisResult::BulkString(response)
                        }
                        // panic is not here
                        _ => todo!("implement error stuff"),
                    },
                    None => {
                        // panic is not here
                        todo!("implement error stuff here");
                    }
                }
            }
        }
    }
}
