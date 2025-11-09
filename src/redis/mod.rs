mod hash_table;
mod redis_object;

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
