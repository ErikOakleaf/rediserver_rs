use libc::INADDR_LOOPBACK;
use redis::{
    error::{ProtocolError, RedisError},
    net::{Socket, make_ipv4_address},
    protocol::parser::parse_reply,
};

fn main() -> Result<(), RedisError> {
    let soc = Socket::new_tcp();

    let address = make_ipv4_address(INADDR_LOOPBACK, 1234);
    soc.connect(&address).unwrap();

    println!("CLIENT");

    query(&soc, b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n".to_vec())?;
    query(&soc, b"*2\r\n$3\r\nDEL\r\n$5\r\nhello\r\n".to_vec())?;
    query(
        &soc,
        b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n".to_vec(),
    )?;

    Ok(())
}

fn query(soc: &Socket, message: Vec<u8>) -> Result<(), RedisError> {
    soc.write_full(message.as_slice())?;
    println!(
        "QUERY WRITTEN: {}",
        str::from_utf8(message.as_slice()).unwrap()
    );

    read_reply(soc)?;

    Ok(())
}

fn read_reply(soc: &Socket) -> Result<(), RedisError> {
    let mut read_buffer = Vec::<u8>::with_capacity(4100);
    let mut reply = Vec::<u8>::new();

    loop {
        let bytes_read = soc.read(&mut read_buffer)?;
        let result = parse_reply(read_buffer.as_slice());
        match result {
            Ok(result) => {
                reply = result;
                break;
            }
            Err(ProtocolError::Incomplete) => {
                continue;
            }
            Err(e) => return Err(RedisError::ProtocolError(e)),
        }
    }

    let s = str::from_utf8(reply.as_slice()).unwrap();

    println!("SERVER SAYS: {}", s);

    Ok(())
}
