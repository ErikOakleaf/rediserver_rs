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

    // read_reply(soc)?;
    println!("REPLY READ");

    Ok(())
}
