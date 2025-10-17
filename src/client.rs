use libc::INADDR_LOOPBACK;
use redis::net::{Socket, make_ipv4_address};
use std::mem;

fn main() {
    println!("CLIENT");
    unsafe {
        let soc = Socket::new_tcp_();

        let address = make_ipv4_address(INADDR_LOOPBACK, 1234);
        soc.connect(&address).unwrap();

        let message: [u8; 5] = *b"hello";
        soc.write(&message).unwrap();

        let mut read_buffer: [u8; 64] = mem::zeroed();
        soc.read(&mut read_buffer).unwrap();

        let s = std::str::from_utf8(&read_buffer).unwrap();

        println!("server says: {}", s);
    }
}
