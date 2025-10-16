use libc::{
    AF_INET, INADDR_LOOPBACK, SO_REUSEADDR, SOCK_STREAM, SOL_SOCKET, SOMAXCONN, accept, bind,
    c_int, close, connect, in_addr, listen, ntohl, ntohs, read, setsockopt, sockaddr, sockaddr_in,
    socket, socklen_t, write,
};
use std::{ffi::c_void, mem};

fn main() {
    println!("CLIENT");
    unsafe {
        let fd = socket(AF_INET, SOCK_STREAM, 0);
        if fd == -1 {
            panic!("socket() failed");
        }

        let address = sockaddr_in {
            sin_family: AF_INET as u16,
            sin_port: ntohs(1234),
            sin_addr: in_addr {
                s_addr: ntohl(INADDR_LOOPBACK),
            },
            sin_zero: [0; 8],
        };

        if connect(
            fd,
            &address as *const sockaddr_in as *const sockaddr,
            mem::size_of::<sockaddr_in>() as socklen_t,
        ) == -1
        {
            panic!("bind error");
        }

        let message: [u8; 5] = *b"hello";
        write(fd, message.as_ptr() as *const c_void, message.len());

        let mut read_buffer: [u8; 64] = mem::zeroed();
        let n = read(
            fd,
            read_buffer.as_mut_ptr() as *mut c_void,
            read_buffer.len(),
        );

        if n < 0 {
            println!("read error");
        }

        let s = std::str::from_utf8(&read_buffer).unwrap();

        println!("server says: {}", s);
        close(fd);
    }
}
