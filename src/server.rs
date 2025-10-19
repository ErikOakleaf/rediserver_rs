use redis::{
    error::{ProtocolError, RedisError, MAX_MESSAGE_SIZE},
    net::{make_ipv4_address, Socket},
};


fn main() {
    let soc = Socket::new_tcp_();
    soc.set_reuseaddr().unwrap();

    let address = make_ipv4_address(0, 1234);

    soc.bind(&address).unwrap();

    soc.listen().unwrap();

    loop {
        let (connection_socket, _) = match soc.accept() {
            Ok(value) => value,
            Err(_) => continue,
        };

        loop {
            let result = one_request(&connection_socket);
            if let Err(_) = result {
                break;
            }
        }
    }
}

fn one_request(soc: &Socket) -> Result<(), RedisError> {
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

    println!("client says {}", s);

    let response = b"world";
    let response_length = response.len();

    let mut write_buffer = [0u8; 9];

    write_buffer[..4].copy_from_slice(&(response_length as u32).to_be_bytes());
    write_buffer[4..4 + response.len()].copy_from_slice(response);

    soc.write_full(&write_buffer).unwrap();

    Ok(())
}

// Helpers

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
