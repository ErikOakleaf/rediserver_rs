use std::io;

use libc::{EPOLLERR, EPOLLHUP, EPOLLIN, EPOLLOUT, c_int, epoll_event};
use redis::{
    connection::{Connection, ConnectionState, ReadState, WriteState},
    error::{MAX_MESSAGE_SIZE, ProtocolError, RedisError},
    net::{Epoll, Socket, make_ipv4_address},
};

const HEADER_SIZE: usize = 4;
const BUFFER_SIZE: usize = HEADER_SIZE + MAX_MESSAGE_SIZE;
const MAX_CONNECTIONS: usize = 1000;

struct Server {
    epoll: Epoll,
    listener: Socket,
    connections: Vec<Option<Connection>>,
    events: Vec<epoll_event>,
}

impl Server {
    fn new(ip: u32, port: u16) -> Result<Self, RedisError> {
        let mut connections: Vec<Option<Connection>> = Vec::with_capacity(MAX_CONNECTIONS);
        connections.resize_with(MAX_CONNECTIONS, || None);

        let mut events: Vec<epoll_event> = Vec::with_capacity(MAX_CONNECTIONS);
        events.resize_with(MAX_CONNECTIONS, || epoll_event { events: 0, u64: 0 });

        // create listening socket
        let listen_socket = Socket::new_tcp();
        listen_socket.set_reuseaddr()?;
        listen_socket.set_non_blocking()?;

        let address = make_ipv4_address(ip, port);

        listen_socket.bind(&address)?;

        listen_socket.listen()?;

        // create epoll and add listening socket
        let epoll = Epoll::new();
        epoll.add(listen_socket.fd, (EPOLLIN | EPOLLERR | EPOLLHUP) as u32)?;

        Ok(Server {
            epoll: epoll,
            listener: listen_socket,
            connections: connections,
            events: events,
        })
    }

    fn accept_new_connections(&mut self) -> Result<(), RedisError> {
        loop {
            match self.listener.accept() {
                Ok((client_socket, _address)) => {
                    let client_fd = client_socket.fd;
                    client_socket.set_non_blocking()?;
                    self.epoll
                        .add(client_fd, (EPOLLIN | EPOLLERR | EPOLLHUP) as u32)?;
                    let connection = Connection::new(client_socket);
                    self.connections[client_fd as usize] = Some(connection);
                }

                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    fn handle_event(&mut self, event: &epoll_event) -> Result<(), RedisError> {
        let fd = event.u64 as c_int;
        let flags = event.events;

        // listen socket
        if fd == self.listener.fd && (flags & EPOLLIN as u32) != 0 {
            self.accept_new_connections()?;
        }

        let connection = match &mut self.connections[fd as usize] {
            Some(connection) => connection,
            None => return Ok(()), // TODO - this should probably return some sort of error since
                                   // there is not a connection to a socket that is till there
        };

        if (flags & EPOLLOUT as u32) != 0 && connection.state == ConnectionState::Write {
            connection.handle_readable()?;
        }

        if (flags & EPOLLOUT as u32) != 0 && connection.state == ConnectionState::Write {
            connection.handle_writeable()?;
        }

        Ok(())
    }
}

fn main() -> Result<(), RedisError> {
    let server = Server::new(0, 1234)?;

    Ok(())
    // let mut connections: Vec<Option<Connection>> = Vec::with_capacity(MAX_CONNECTIONS);
    // connections.resize_with(MAX_CONNECTIONS, || None);
    //
    // let mut events: Vec<epoll_event> = Vec::with_capacity(MAX_CONNECTIONS);
    // events.resize_with(MAX_CONNECTIONS, || epoll_event { events: 0, u64: 0 });
    //
    // // create listening socket
    // let listen_socket = Socket::new_tcp();
    // listen_socket.set_reuseaddr()?;
    // listen_socket.set_non_blocking()?;
    //
    // let address = make_ipv4_address(0, 1234);
    //
    // listen_socket.bind(&address)?;
    //
    // listen_socket.listen()?;
    //
    // // create epoll and add listening socket
    // let epoll = Epoll::new();
    // epoll.add(listen_socket.fd, (EPOLLIN | EPOLLERR | EPOLLHUP) as u32)?;
    //
    // loop {
    //     let amount_events = epoll.wait(&mut events, -1)?;
    //
    //     for i in 0..amount_events {
    //         let event = events[i];
    //         let fd = event.u64 as c_int;
    //         let flags = event.events;
    //
    //         // listen socket
    //         if fd == listen_socket.fd && (flags & EPOLLIN as u32) != 0 {
    //             accept_new_connections(&listen_socket, &epoll, &mut connections)?;
    //         }
    //
    //         let connection = match &mut connections[fd as usize] {
    //             Some(connection) => connection,
    //             None => continue,
    //         };
    //
    //         // read from sockets
    //         if (flags & EPOLLIN as u32) != 0 && connection.state == ConnectionState::Read {
    //             read_ready(connection, &epoll)?;
    //         }
    //
    //         // write to sockets
    //         if (flags & EPOLLOUT as u32) != 0 && connection.state == ConnectionState::Write {
    //             let write_state = &mut connection.write_state;
    //             let buffer = &write_state.buffer;
    //             write_state.bytes_written += connection
    //                 .socket
    //                 .write(&buffer[write_state.bytes_written..write_state.size])?;
    //
    //             if write_state.size == write_state.bytes_written {
    //                 write_state.size = 0;
    //                 write_state.bytes_written = 0;
    //                 connection.state = ConnectionState::Read;
    //             }
    //         }
    //     }
    // }
}

fn accept_new_connections(
    listen_socket: &Socket,
    epoll: &Epoll,
    connections: &mut Vec<Option<Connection>>,
) -> Result<(), RedisError> {
    loop {
        match listen_socket.accept() {
            Ok((client_socket, _address)) => {
                let client_fd = client_socket.fd;
                client_socket.set_non_blocking()?;
                epoll.add(client_fd, (EPOLLIN | EPOLLERR | EPOLLHUP) as u32)?;
                let connection = Connection::new(client_socket);
                connections[client_fd as usize] = Some(connection);
            }

            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                break;
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

fn read_ready(connection: &mut Connection, epoll: &Epoll) -> Result<(), RedisError> {
    match fill_buffer(&connection.socket, &mut connection.read_state) {
        Ok(_) => loop {
            let maybe_message = try_extract_message(&mut connection.read_state);

            match maybe_message {
                Some(message) => handle_message(
                    message,
                    &connection.socket,
                    &mut connection.write_state,
                    &mut connection.state,
                    epoll,
                )?,
                None => break,
            }
        },
        Err(_) => {}
    }

    Ok(())
}

#[inline(always)]
fn fill_buffer(socket: &Socket, state: &mut ReadState) -> Result<(), RedisError> {
    let read_result = socket.read(&mut state.buffer[state.bytes_filled..])?;

    if read_result == 0 {
        return Err(RedisError::ConnectionClosed);
    }

    state.bytes_filled += read_result;
    Ok(())
}

fn try_extract_message(state: &mut ReadState) -> Option<&[u8]> {
    if let Some(length) = state.wanted_length {
        if state.bytes_filled - state.position < length {
            return None;
        }

        let message = &state.buffer[state.position..state.position + length];
        state.position += length;
        state.wanted_length = None;
        return Some(message);
    }

    if state.bytes_filled - state.position < 4 {
        reset_buffer_if_needed(state);
        return None;
    }

    let length = get_message_length(&state.buffer[state.position..state.position + 4]).unwrap();

    let leftover = state.bytes_filled - state.position;

    if leftover < length {
        state
            .buffer
            .copy_within(state.position + 4..state.bytes_filled, 0);
        state.wanted_length = Some(length);
        state.bytes_filled = leftover - 4;
        state.position = 0;
        return None;
    }

    let result = &state.buffer[state.position + 4..state.position + 4 + length];
    state.position += length + 4;

    Some(result)
}

#[inline(always)]
fn reset_buffer_if_needed(state: &mut ReadState) {
    if state.position > 0 && state.position == state.bytes_filled {
        state.bytes_filled = 0;
        state.position = 0;
    }
}

fn handle_message(
    buffer: &[u8],
    socket: &Socket,
    write_state: &mut WriteState,
    connection_state: &mut ConnectionState,
    epoll: &Epoll,
) -> Result<(), RedisError> {
    let s = std::str::from_utf8(buffer).unwrap().to_string();

    println!("client says {}", s);

    let response = b"world";
    let response_length = response.len();

    write_state.buffer[..4].copy_from_slice(&(response_length as u32).to_be_bytes());
    write_state.buffer[4..4 + response.len()].copy_from_slice(response);
    write_state.size = response_length + 4;

    write_state.bytes_written += socket
        .write(&write_state.buffer[write_state.bytes_written..write_state.size])
        .unwrap();

    if write_state.bytes_written != write_state.size {
        *connection_state = ConnectionState::Write;
        epoll.modify(socket.fd, (EPOLLOUT | EPOLLERR | EPOLLHUP) as u32)?;
    } else {
        if *connection_state == ConnectionState::Write {
            epoll.modify(socket.fd, (EPOLLIN | EPOLLERR | EPOLLHUP) as u32)?;
        }
        write_state.size = 0;
        write_state.bytes_written = 0;
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_try_extract_message() {
        struct TestData {
            prefix_short_1: Option<u16>,
            prefix_short_2: Option<u16>,
            message: &'static [u8],
            expected: Option<&'static [u8]>,
        }

        let tests = vec![
            TestData {
                prefix_short_1: Some(0),
                prefix_short_2: Some(5),
                message: b"hello",
                expected: Some(b"hello"),
            },
            TestData {
                prefix_short_1: Some(0),
                prefix_short_2: Some(5),
                message: b"world",
                expected: Some(b"world"),
            },
            TestData {
                prefix_short_1: Some(0),
                prefix_short_2: Some(11),
                message: b"hello",
                expected: None,
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: None,
                message: b" world",
                expected: Some(b"hello world"),
            },
            TestData {
                prefix_short_1: Some(0),
                prefix_short_2: None,
                message: &[],
                expected: None,
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: Some(5),
                message: b"hello",
                expected: Some(b"hello"),
            },
            TestData {
                prefix_short_1: Some(0),
                prefix_short_2: None,
                message: &[],
                expected: None,
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: Some(5),
                message: &[],
                expected: None,
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: None,
                message: b"hel",
                expected: None,
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: None,
                message: b"lo",
                expected: Some(b"hello"),
            },
            TestData {
                prefix_short_1: Some(0),
                prefix_short_2: Some(5),
                message: b"hello\x00\x00\x00\x05world", // Two messages back-to-back
                expected: Some(b"hello"),
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: None,
                message: &[],
                expected: Some(b"world"),
            },
            TestData {
                prefix_short_1: Some(0),
                prefix_short_2: Some(5),
                message: b"hello\x00\x00\x00\x05wor", // one and a half messages back-to-back
                expected: Some(b"hello"),
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: None,
                message: b"ld",
                expected: Some(b"world"),
            },
            TestData {
                prefix_short_1: Some(0),
                prefix_short_2: None,
                message: &[],
                expected: None,
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: Some(5),
                message: &[],
                expected: None,
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: None,
                message: b"hello",
                expected: Some(b"hello"),
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: None,
                message: &[0], // sending 1 then 3 bytes for the length
                expected: None,
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: None,
                message: &[0, 0, 5],
                expected: None,
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: None,
                message: b"hello",
                expected: Some(b"hello"),
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: None,
                message: &[0, 0, 0], // sending 3 then 1 bytes for the length
                expected: None,
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: None,
                message: &[5],
                expected: None,
            },
            TestData {
                prefix_short_1: None,
                prefix_short_2: None,
                message: b"world",
                expected: Some(b"world"),
            },
        ];

        let mut read_state = ReadState::new();

        let mut i = 1;
        for test in tests {
            let mut combined_vec: Vec<u8> = Vec::new();

            if let Some(short1) = test.prefix_short_1 {
                combined_vec.extend_from_slice(&short1.to_be_bytes());
            }

            if let Some(short2) = test.prefix_short_2 {
                combined_vec.extend_from_slice(&short2.to_be_bytes());
            }

            combined_vec.extend_from_slice(test.message);

            for (i, &byte) in combined_vec.iter().enumerate() {
                read_state.buffer[read_state.bytes_filled + i] = byte;
            }
            read_state.bytes_filled += combined_vec.len();

            let result = try_extract_message(&mut read_state);

            let format_output = |opt: &Option<&[u8]>| -> String {
                match opt {
                    Some(bytes) => String::from_utf8_lossy(bytes).to_string(),
                    None => "None".to_string(),
                }
            };

            assert_eq!(
                test.expected,
                result.as_deref(),
                "in test {}\nexpected: {}\ngot: {}\n",
                i,
                format_output(&test.expected),
                format_output(&result.as_deref())
            );

            i += 1;
        }
    }
}
