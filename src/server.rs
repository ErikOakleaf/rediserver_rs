use libc::{
    AF_INET, SO_REUSEADDR, SOCK_STREAM, SOL_SOCKET, SOMAXCONN, accept, bind, c_int, close, in_addr,
    listen, ntohl, ntohs, read, setsockopt, sockaddr, sockaddr_in, socket, socklen_t, write,
};
use std::{ffi::c_void, mem};

fn main() {
    println!("SERVER");
    unsafe {
        let fd = socket(AF_INET, SOCK_STREAM, 0);

        let value = 1;
        if setsockopt(
            fd,
            SOL_SOCKET,
            SO_REUSEADDR,
            &value as *const i32 as *const c_void,
            mem::size_of::<i32>() as socklen_t,
        ) == -1
        {
            panic!("could not set socket options")
        };

        let address = sockaddr_in {
            sin_family: AF_INET as u16,
            sin_port: ntohs(1234),
            sin_addr: in_addr { s_addr: ntohl(0) },
            sin_zero: [0; 8],
        };

        if bind(
            fd,
            &address as *const sockaddr_in as *const sockaddr,
            mem::size_of::<sockaddr_in>() as socklen_t,
        ) == -1
        {
            panic!("bind error");
        }

        if listen(fd, SOMAXCONN) == -1 {
            panic!("listen error");
        }

        loop {
            let mut client_address: sockaddr_in = mem::zeroed();
            let mut socklen = mem::size_of::<sockaddr_in>() as socklen_t;

            let connection_fd = accept(
                fd,
                &mut client_address as *mut sockaddr_in as *mut sockaddr,
                &mut socklen,
            );

            if connection_fd == -1 {
                continue;
            }


            let mut read_buffer: [u8; 64] = mem::zeroed();
            let n = read(connection_fd, read_buffer.as_mut_ptr() as *mut c_void, read_buffer.len());

            if n < 0 {
                println!("read error");
            }

            let s = std::str::from_utf8(&read_buffer).unwrap();

            println!("client says: {}", s);

            let write_buffer: [u8; 5] = *b"world";
            write(connection_fd, write_buffer.as_ptr() as *const c_void, write_buffer.len());
            close(connection_fd);
        }
    }
}
