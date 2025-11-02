use crate::{
    commands::RedisCommand,
    connection::{BUFFER_SIZE, HEADER_SIZE},
    error::{RedisCommandError, RedisError},
};

#[derive(Debug, PartialEq)]
enum StringExtractionResult {
    Complete((usize, usize), usize),
    Partial(usize),
    None,
}

pub struct ReadState {
    pub buffer: [u8; BUFFER_SIZE],
    pub bytes_filled: usize,
    pub position: usize,
    pub wanted_string_length: Option<usize>,
    pub wanted_strings_amount: Option<usize>,
    pub current_message: Vec<(usize, usize)>,
    pub current_message_start: usize,
    pub current_message_bytes_length: usize,
}

impl ReadState {
    pub fn new() -> Self {
        ReadState {
            buffer: [0u8; BUFFER_SIZE],
            bytes_filled: 0,
            position: 0,
            wanted_string_length: None,
            wanted_strings_amount: None,
            current_message: Vec::<(usize, usize)>::new(),
            current_message_start: 0,
            current_message_bytes_length: 0,
        }
    }
}
