use libc::INADDR_LOOPBACK;
use redis::{
    error::{MAX_MESSAGE_SIZE, ProtocolError, RedisError},
    net::{Socket, make_ipv4_address},
};

fn main() -> Result<(), RedisError> {
    let soc = Socket::new_tcp();

    let address = make_ipv4_address(INADDR_LOOPBACK, 1234);
    soc.connect(&address).unwrap();

    query(&soc, "hello1")?;
    query(&soc, "hello2")?;
    query(&soc, "hello3")?;

    Ok(())
}

fn query(soc: &Socket, message: &str) -> Result<(), RedisError> {
    let bytes = message.as_bytes();
    let bytes_length = bytes.len();
    if bytes_length > MAX_MESSAGE_SIZE {
        return Err(RedisError::ProtocolError(ProtocolError::MessageTooLong(
            bytes_length,
        )));
    }

    let mut write_buffer = [0u8; 4 + MAX_MESSAGE_SIZE];
    write_buffer[..4].copy_from_slice(&(bytes_length as u32).to_be_bytes());
    write_buffer[4..4 + bytes_length].copy_from_slice(bytes);
    soc.write_full(&write_buffer[..4 + bytes_length])?;

    // read reply
    let mut read_buffer: [u8; 4 + MAX_MESSAGE_SIZE + 1] = [0; 4 + MAX_MESSAGE_SIZE + 1];

    // read length
    soc.read_full(&mut read_buffer[..4])?;
    let length = get_message_length(&read_buffer)?;

    // read body
    soc.read_full(&mut read_buffer[4..length + 4])?;

    // just print for now
    let s = std::str::from_utf8(&read_buffer[4..length + 4])
        .unwrap()
        .to_string();

    println!("server says: {}", s);

    Ok(())
}

#[inline]
fn get_message_length(buffer: &[u8]) -> Result<usize, ProtocolError> {
    let length = u32_from_be_bytes(&buffer[..4]) as usize;

    if length > MAX_MESSAGE_SIZE {
        return Err(ProtocolError::MessageTooLong(length));
    }

    Ok(length)
}

fn u32_from_be_bytes(slice: &[u8]) -> u32 {
    debug_assert_eq!(
        slice.len(),
        4,
        "SLICE DOES NOT HAVE CORRECT LENGTH IN u32 from be bytes function",
    );

    let length = ((slice[0] as u32) << 24)
        | ((slice[1] as u32) << 16)
        | ((slice[2] as u32) << 8)
        | (slice[3] as u32);
    length
}
