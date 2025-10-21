use libc::{
    AF_INET, EPOLL_CTL_ADD, F_GETFL, F_SETFL, O_NONBLOCK, SO_REUSEADDR, SOCK_STREAM, SOL_SOCKET,
    SOMAXCONN, accept, bind, close, connect, epoll_create1, epoll_ctl, epoll_event, epoll_wait,
    fcntl, htonl, htons, in_addr, listen, read, setsockopt, sockaddr, sockaddr_in, socklen_t,
    write,
};
use libc::{c_int, socket};
use std::ffi::c_void;
use std::{io, mem};

pub struct Socket {
    pub fd: c_int,
}

impl Socket {
    pub fn new_tcp() -> Self {
        let fd = unsafe { socket(AF_INET, SOCK_STREAM, 0) };
        Socket { fd: fd }
    }

    pub fn set_reuseaddr(&self) -> io::Result<()> {
        let val: i32 = 1;
        set_socket_options(self.fd, SOL_SOCKET, SO_REUSEADDR, &val)
    }

    pub fn set_non_blocking(&self) -> io::Result<()> {
        let mut flags = unsafe { fcntl(self.fd, F_GETFL, 0) };
        if flags == -1 {
            return Err(io::Error::last_os_error());
        }

        flags |= O_NONBLOCK;

        if unsafe { fcntl(self.fd, F_SETFL, flags) } == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
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

fn read_socket(fd: c_int, buffer: &mut [u8]) -> io::Result<usize> {
    let n = unsafe { read(fd, buffer.as_mut_ptr() as *mut c_void, buffer.len()) };
    if n < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

fn read_full_socket(fd: c_int, buffer: &mut [u8]) -> io::Result<()> {
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

fn write_socket(fd: c_int, buffer: &[u8]) -> io::Result<usize> {
    let n = unsafe { write(fd, buffer.as_ptr() as *const c_void, buffer.len()) };
    if n < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

fn write_full_socket(fd: c_int, buffer: &[u8]) -> io::Result<()> {
    let buffer_length = buffer.len();
    let mut total = 0;
    while total < buffer_length {
        let n = unsafe {
            write(
                fd,
                buffer[total..].as_ptr() as *const c_void,
                buffer_length - total,
            )
        };

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

pub struct Epoll {
    fd: c_int,
}

impl Epoll {
    pub fn new() -> Self {
        let fd = unsafe { epoll_create1(0) };
        Epoll { fd: fd }
    }

    pub fn add(&self, fd: c_int, events: u32) -> io::Result<()> {
        let mut event = epoll_event {
            events: events,
            u64: fd as u64,
        };

        let return_value = unsafe { epoll_ctl(self.fd, EPOLL_CTL_ADD, fd, &mut event) };

        if return_value == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub fn wait(&self, events: &mut [epoll_event], timeout_ms: i32) -> io::Result<usize> {
        let amount_ready = unsafe {
            epoll_wait(
                self.fd,
                events.as_mut_ptr(),
                events.len() as i32,
                timeout_ms,
            )
        };

        if amount_ready == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(amount_ready as usize)
        }
    }
}

// helpers

pub fn make_ipv4_address(ip: u32, port: u16) -> sockaddr_in {
    sockaddr_in {
        sin_family: AF_INET as u16,
        sin_port: htons(port),
        sin_addr: in_addr { s_addr: htonl(ip) },
        sin_zero: [0; 8],
    }
}
