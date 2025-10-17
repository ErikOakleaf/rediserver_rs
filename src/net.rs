use libc::{
    AF_INET, SO_REUSEADDR, SOCK_STREAM, SOL_SOCKET, SOMAXCONN, accept, bind, close, connect, htonl,
    htons, in_addr, listen, read, setsockopt, sockaddr, sockaddr_in, socklen_t, write,
};
use libc::{c_int, socket};
use std::ffi::c_void;
use std::{io, mem};

pub struct Socket {
    fd: c_int,
}

impl Socket {
    pub fn new_tcp_() -> Self {
        let fd = unsafe { socket(AF_INET, SOCK_STREAM, 0) };
        Socket { fd: fd }
    }

    pub fn set_reuseaddr(&self) -> io::Result<()> {
        let val: i32 = 1;
        set_socket_options(self.fd, SOL_SOCKET, SO_REUSEADDR, &val)
    }

    pub fn bind(&self, address: &sockaddr_in) -> io::Result<()> {
        let return_value = unsafe {
            bind(
                self.fd,
                address as *const sockaddr_in as *const sockaddr,
                mem::size_of::<sockaddr_in>() as u32,
            )
        };
        if return_value == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub fn listen(&self) -> io::Result<()> {
        let return_value = unsafe { listen(self.fd, SOMAXCONN) };

        if return_value == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub fn accept(&self) -> io::Result<(Socket, sockaddr_in)> {
        unsafe {
            let mut client_address: sockaddr_in = mem::zeroed();
            let mut socklen = mem::size_of::<sockaddr_in>() as socklen_t;

            let connection_fd = accept(
                self.fd,
                &mut client_address as *mut sockaddr_in as *mut sockaddr,
                &mut socklen,
            );

            if connection_fd == -1 {
                Err(io::Error::last_os_error())
            } else {
                let connection_socket = Socket { fd: connection_fd };
                Ok((connection_socket, client_address))
            }
        }
    }

    pub fn connect(&self, address: &sockaddr_in) -> io::Result<()> {
        let return_value = unsafe {
            connect(
                self.fd,
                address as *const sockaddr_in as *const sockaddr,
                mem::size_of::<sockaddr_in>() as socklen_t,
            )
        };

        if return_value == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub fn read(&self, buffer: &mut [u8]) -> io::Result<usize> {
        read_socket(self.fd, buffer)
    }

    pub fn read_full(&self, buffer: &mut [u8]) -> io::Result<()> {
        read_full_socket(self.fd, buffer)
    }

    pub fn write(&self, buffer: &[u8]) -> io::Result<usize> {
        write_socket(self.fd, buffer)
    }

    pub fn write_full(&self, buffer: &[u8]) -> io::Result<()> {
        write_full_socket(self.fd, buffer)
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        if self.fd != -1 {
            let return_value = unsafe { close(self.fd) };
            if return_value == -1 {
                eprintln!(
                    "Warning: close(fd={}) failed: {:?}",
                    self.fd,
                    io::Error::last_os_error()
                );
            }
            self.fd = -1;
        }
    }
}

pub fn read_socket(fd: c_int, buffer: &mut [u8]) -> io::Result<usize> {
    let n = unsafe { read(fd, buffer.as_mut_ptr() as *mut c_void, buffer.len()) };
    if n < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

pub fn read_full_socket(fd: c_int, buffer: &mut [u8]) -> io::Result<()> {
    let buffer_length = buffer.len();
    let mut total = 0;
    while total < buffer_length {
        let n = unsafe {
            read(
                fd,
                buffer[total..].as_mut_ptr() as *mut c_void,
                buffer_length - total,
            )
        };

        if n < 0 {
            return Err(io::Error::last_os_error());
        } else if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed",
            ));
        }

        total += n as usize;
    }

    Ok(())
}

pub fn write_socket(fd: c_int, buffer: &[u8]) -> io::Result<usize> {
    let n = unsafe { write(fd, buffer.as_ptr() as *const c_void, buffer.len()) };
    if n < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

pub fn write_full_socket(fd: c_int, buffer: &[u8]) -> io::Result<()> {
    let buffer_length = buffer.len();
    let mut total = 0;
    while total < buffer_length {
        let n = unsafe { write(fd, buffer[total..].as_ptr() as *const c_void, buffer_length - total) };

        if n < 0 {
            return Err(io::Error::last_os_error());
        }

        total += n as usize;
    }

    Ok(())
}

fn set_socket_options<T>(fd: c_int, level: i32, optname: i32, value: &T) -> io::Result<()> {
    let return_value = unsafe {
        setsockopt(
            fd,
            level,
            optname,
            value as *const _ as *const c_void,
            mem::size_of::<T>() as socklen_t,
        )
    };
    if return_value == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub fn make_ipv4_address(ip: u32, port: u16) -> sockaddr_in {
    sockaddr_in {
        sin_family: AF_INET as u16,
        sin_port: htons(port),
        sin_addr: in_addr { s_addr: htonl(ip) },
        sin_zero: [0; 8],
    }
}
