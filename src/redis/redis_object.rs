#[derive(Clone, PartialEq, Debug)]
pub enum RedisValue {
    String(Vec<u8>),
    Int(i64),
}

impl RedisValue {
    pub fn new_from_bytes(bytes: &[u8]) -> RedisValue {
        RedisValue::String(bytes.to_vec())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            RedisValue::String(s) => s.clone(),
            RedisValue::Int(i) => i.to_string().into_bytes(),
        }
    }

    // TODO - this method should be optimized with how it handles bytes but for now this will just
    // be doing a bunch of allocations and conversions to strings and stuff
    pub fn to_resp(&self) -> Vec<u8> {
        let data = self.to_bytes();
        let data_len = data.len();
        let header = format!("${}\r\n", data_len).as_bytes().to_vec();

        let mut result = Vec::<u8>::new();
        result.extend_from_slice(&header);
        result.extend_from_slice(&data);
        result.extend_from_slice(b"\r\n");

        result
    }
}
