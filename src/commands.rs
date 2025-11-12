#[derive(Debug, PartialEq)]
pub enum RedisCommand<'a> {
    Get { key: &'a [u8] },
    Set { key: &'a [u8], value: &'a [u8] },
    Del { keys: Vec<&'a [u8]> },
}
