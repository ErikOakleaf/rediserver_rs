#[derive(Clone, PartialEq, Debug)]
pub enum ValueType {
    String,
    Int,
}

#[derive(Clone, PartialEq, Debug)]
pub enum RedisValue {
    String(Vec<u8>),
    Int(i64),
}

#[derive(Clone, PartialEq, Debug)]
pub struct RedisObject {
    value_type: ValueType,
    value: RedisValue,
}

impl RedisObject {
    pub fn new_from_bytes(bytes: &[u8]) -> RedisObject {
        let value_type = ValueType::String;
        let value = RedisValue::String(bytes.to_vec());

        RedisObject { value_type, value }
    }
}
