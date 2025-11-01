use std::io;

use libc::{EPOLLERR, EPOLLHUP, EPOLLIN, EPOLLOUT, c_int, epoll_event};
use redis::{
    commands::RedisCommand,
    connection::{Connection, ReadState, WriteState},
    error::{RedisCommandError, RedisError},
    net::{Epoll, Socket, make_ipv4_address},
};

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

    fn handle_events(&mut self) -> Result<(), RedisError> {
        loop {
            let amount_events = self.get_events()?;

            for i in 0..amount_events {
                self.handle_event(i)?
            }
        }
    }

    fn handle_event(&mut self, event_index: usize) -> Result<(), RedisError> {
        let event = &self.events[event_index];
        let fd = event.u64 as c_int;
        let flags = event.events;

        // listen socket
        if fd == self.listener.fd && (flags & EPOLLIN as u32) != 0 {
            self.accept_new_connections()?;
        }

        let mut connection = match self.connections[fd as usize].take() {
            Some(connection) => connection,
            None => return Ok(()), // TODO - this should probably return some sort of error since
                                   // there is not a connection to a socket that is till there
        };

        if (flags & EPOLLIN as u32) != 0 {
            connection.fill_read_buffer()?;
            self.handle_readable(&mut connection)?;
        }

        if (flags & EPOLLOUT as u32) != 0 {
            self.handle_writeable(&mut connection)?;
        }

        // put the connection back in where it was taken from
        self.connections[fd as usize] = Some(connection);

        Ok(())
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

    // TODO - modularize this function later split it into more functions
    fn handle_readable(&mut self, connection: &mut Connection) -> Result<(), RedisError> {
        self.process_incoming(&mut connection.read_state, &mut connection.write_state)?;
        connection.read_state.reset_if_empty();

        let write_result = connection.flush_write_buffer()?;

        if write_result == false {
            Self::poll_socket_out(&self.epoll, connection.socket.fd)?;
        }

        Ok(())
    }

    fn process_incoming(
        &mut self,
        read_state: &mut ReadState,
        write_state: &mut WriteState,
    ) -> Result<(), RedisError> {
        loop {
            match read_state.get_commands() {
                Ok(Some(commands)) => {
                    self.handle_commands(&commands, write_state)?;
                }
                Ok(None) => break,
                Err(err) => {
                    write_state.append_amount_responses_header(1)?;

                    match err {
                        RedisCommandError::UnknownCommand(cmd) => {
                            write_state.append_bytes(1, &cmd)?;
                        }
                        RedisCommandError::WrongArity(_) => {
                            write_state.append_bytes(1, b"wrong arity")?; // TODO add
                            // the command that has the wrong arity here as well
                        }
                        _ => {
                            unreachable!(
                                "this should be unreachable other errors should be detected elsewhere"
                            );
                        }
                    };

                    break;
                }
            }
        }

        Ok(())
    }

    fn handle_commands(
        &mut self,
        commands: &[RedisCommand],
        write_state: &mut WriteState,
    ) -> Result<(), RedisError> {
        write_state.append_amount_responses_header(commands.len() as u32)?;

        for command in commands {
            let result = self.handle_command(command);

            match result {
                Ok(_) => {
                    write_state.append_bytes(0, &[])?;
                }
                Err(err) => {
                    let status_code = match err {
                        RedisCommandError::KeyNotFound => 2,
                        _ => 1, // There should possibly be more errors here in the
                                // future right now this does not make that much sense
                    };
                    write_state.append_bytes(status_code, &[])?;
                }
            }
        }

        Ok(())
    }

    fn handle_command(&mut self, command: &RedisCommand) -> Result<(), RedisCommandError> {
        match command {
            RedisCommand::Get { key } => self.get(key),
            RedisCommand::Del { key } => self.del(key),
            RedisCommand::Set { key, value } => self.set(key, value),
        }
    }

    // for now these return just whatever since there is no actual things to mutate right now
    fn get(&mut self, key: &[u8]) -> Result<(), RedisCommandError> {
        Err(RedisCommandError::KeyNotFound)
    }

    fn del(&mut self, key: &[u8]) -> Result<(), RedisCommandError> {
        Err(RedisCommandError::KeyNotFound)
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), RedisCommandError> {
        Ok(())
    }

    fn handle_writeable(&mut self, connection: &mut Connection) -> Result<(), RedisError> {
        let write_result = connection.flush_write_buffer()?;

        if write_result == true {
            Self::poll_socket_in(&self.epoll, connection.socket.fd)?;
        }

        Ok(())
    }

    // Helpers

    fn get_events(&mut self) -> Result<usize, RedisError> {
        let amount_events = self.epoll.wait(&mut self.events, -1)?;
        Ok(amount_events)
    }

    fn poll_socket_in(epoll: &Epoll, connection_fd: c_int) -> io::Result<()> {
        epoll.modify(connection_fd, (EPOLLIN | EPOLLERR | EPOLLHUP) as u32)
    }

    fn poll_socket_out(epoll: &Epoll, connection_fd: c_int) -> io::Result<()> {
        epoll.modify(connection_fd, (EPOLLOUT | EPOLLERR | EPOLLHUP) as u32)
    }
}

fn main() -> Result<(), RedisError> {
    let mut server = Server::new(0, 1234)?;

    server.handle_events()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_incoming_output() -> Result<(), RedisError> {
        struct TestData {
            read_buffer: &'static [u8],
            message: &'static [(usize, usize)],
            expected_write_buffer: &'static [u8],
        }

        let tests = vec![
            TestData {
                read_buffer: b"\x00\x00\x00\x02\x00\x00\x00\x03GET\x00\x00\x00\x05hello",
                message: &[(8, 11), (15, 20)],
                expected_write_buffer: b"\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x02",
            },
            TestData {
                read_buffer: b"\x00\x00\x00\x02\x00\x00\x00\x03DEL\x00\x00\x00\x05hello",
                message: &[(8, 11), (15, 20)],
                expected_write_buffer: b"\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x02",
            },
            TestData {
                read_buffer: b"\x00\x00\x00\x02\x00\x00\x00\x03BEL\x00\x00\x00\x05hello",
                message: &[(8, 11), (15, 20)],
                expected_write_buffer: b"\x00\x00\x00\x01\x00\x00\x00\x07\x00\x00\x00\x01BEL",
            },
            TestData {
                read_buffer:
                    b"\x00\x00\x00\x03\x00\x00\x00\x03SET\x00\x00\x00\x05hello\x00\x00\x00\x05world",
                message: &[(8, 11), (15, 20)],
                expected_write_buffer: b"\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x00",
            },
        ];

        // TODO this test could probably in the future be modified to not take self but just the
        // individual components but for now this is a good sanity check

        for test in tests {
            let mut dummy_server = Server::new(0000, 00)?;
            let mut dummy_read_state = ReadState::new();
            let mut dummy_write_state = WriteState::new();

            dummy_read_state.buffer[..test.read_buffer.len()].copy_from_slice(test.read_buffer);
            dummy_read_state.bytes_filled += test.read_buffer.len();

            dummy_server.process_incoming(&mut dummy_read_state, &mut dummy_write_state)?;

            assert_eq!(
                test.expected_write_buffer,
                &dummy_write_state.buffer[..dummy_write_state.size],
                "\nexpected buffer {:?}\ngot {:?}",
                test.expected_write_buffer,
                &dummy_write_state.buffer[..dummy_write_state.size]
            );
        }

        Ok(())
    }
}
