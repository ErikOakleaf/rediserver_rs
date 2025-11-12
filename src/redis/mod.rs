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
                let node = Box::new(HashNode::new(key, value));
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
        }
    }
}
