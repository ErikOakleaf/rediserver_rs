#[derive(Debug, PartialEq)]
pub enum RedisCommand<'a> {
    Get { key: &'a [u8] },
    Set { key: &'a [u8], value: &'a [u8] },
    Del { keys: Vec<&'a [u8]> },
    // list commands
    LPush { key: &'a [u8], value: &'a [u8] },
    RPush { key: &'a [u8], value: &'a [u8] },
    LPop { key: &'a [u8] },
    RPop { key: &'a [u8] },
}
