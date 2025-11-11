mod hash_table;
mod redis_object;

use crate::{
    commands::RedisCommand,
    error::RedisError,
    redis::hash_table::{HashDict, HashNode},
};

pub enum RedisResult {
    SimpleString(&'static [u8]),
    BulkString(Vec<u8>),
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
                let node = Box::new(HashNode::new(key, value));
                self.dict.insert(node);
                RedisResult::SimpleString(b"+OK\r\n")
            }
            RedisCommand::Get { key } => {
                let node = self.dict.lookup(key);
                RedisResult::BulkString(b"$-1\r\n".to_vec())},
            RedisCommand::Del { key } => RedisResult::BulkString(b"$-1\r\n".to_vec()),
        }
    }
}
