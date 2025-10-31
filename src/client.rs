use std::io;

use libc::INADDR_LOOPBACK;
use redis::{
    error::{MAX_MESSAGE_SIZE, ProtocolError, RedisError},
    net::{Socket, make_ipv4_address},
};

fn main() -> Result<(), RedisError> {
    let soc = Socket::new_tcp();

    let address = make_ipv4_address(INADDR_LOOPBACK, 1234);
    soc.connect(&address).unwrap();

    println!("CLIENT");

    query(&soc, vec![b"GET", b"hello"])?;
    println!("FIRST QUERY COMPLETE");
    query(&soc, vec![b"DEL", b"hello"])?;
    query(&soc, vec![b"SET", b"hello", b"world"])?;

    Ok(())
}

fn query(soc: &Socket, message: Vec<&[u8]>) -> Result<(), RedisError> {

    println!("QUERY START");
    let mut write_buffer = [0u8; 4 + MAX_MESSAGE_SIZE];

    let amount_strings = (message.len() as u32).to_be_bytes();
    write_buffer[0..4].copy_from_slice(&amount_strings);

    let mut position = 4;

    for string in message {
        let string_length = string.len();
        let string_length_bytes = (string_length as u32).to_be_bytes();
        write_buffer[position..position + 4].copy_from_slice(&string_length_bytes);
        position += 4;

        write_buffer[position..position + string_length].copy_from_slice(string);
        position += string_length;
    }

    soc.write_full(&write_buffer[..position])?;

    read_reply(soc)?;
    println!("REPLY READ");

    Ok(())
}

fn read_reply(soc: &Socket) -> Result<(), RedisError> {
    let mut read_buffer: [u8; 4 + MAX_MESSAGE_SIZE + 1] = [0; 4 + MAX_MESSAGE_SIZE + 1];

    let mut position = 0;

    loop {
        // read string length
        soc.read_full(&mut read_buffer[position..position + 4])?;
        let string_length = u32_from_be_bytes(&read_buffer[position..position + 4]);
        if string_length == 0 {
            break;
        }

        position += 4;

        // read the rest of the string

        soc.read_full(&mut read_buffer[position..position + string_length as usize])?;

        let status = u32_from_be_bytes(&read_buffer[position..position + 4]);

        if string_length == 4 {
            println!("server says: [{}]", status);
        } else {
            let data =
                std::str::from_utf8(&read_buffer[position + 4..position + string_length as usize])
                    .unwrap()
                    .to_string();

            println!("server says: [{}] {}", status, data);
        }

        println!("REPLY READ IN FUNCTION");

        position += string_length as usize;
    }

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
