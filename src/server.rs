use std::io;

use crate::{
    connection::{Connection, ReadBuffer, WriteBuffer},
    error::{ProtocolError, RedisError, handle_command_error, handle_protocol_error},
    net::{Epoll, Socket, make_ipv4_address},
    protocol::parser::{
        CommandParseState, ParseState, convert_command_parse_state_to_redis_command, parse_command,
        parse_partial_command,
    },
    redis::{Redis, RedisResult},
};
use libc::{EPOLLERR, EPOLLHUP, EPOLLIN, EPOLLOUT, c_int, epoll_event};

const MAX_CONNECTIONS: usize = 1000;

pub struct Server {
    redis: Redis,
    epoll: Epoll,
    listener: Socket,
    connections: Vec<Option<Connection>>,
    events: Vec<epoll_event>,
}

impl Server {
    pub fn new(ip: u32, port: u16) -> Result<Self, RedisError> {
        let redis = Redis::new();

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
            redis: redis,
            epoll: epoll,
            listener: listen_socket,
            connections: connections,
            events: events,
        })
    }

    pub fn run(&mut self) -> Result<(), RedisError> {
        loop {
            let amount_events = self.get_events()?;

            for i in 0..amount_events {
                self.handle_event(i)?
            }
        }
    }

    // TODO this function is in desperate need of refactoring

    fn handle_event(&mut self, event_index: usize) -> Result<(), RedisError> {
        let event = &self.events[event_index];
        let fd = event.u64 as c_int;
        let flags = event.events;

        if (flags & (EPOLLHUP | EPOLLERR) as u32) != 0 {
            self.connections[fd as usize] = None;
            return Ok(());
        }

        // listen socket
        if fd == self.listener.fd && (flags & EPOLLIN as u32) != 0 {
            self.accept_new_connections()?;
        }

        let mut connection = match &mut self.connections[fd as usize] {
            Some(connection) => connection,
            None => return Ok(()), // TODO - this should probably return some sort of error since
                                   // there is not a connection to a socket that is till there
        };

        if Self::is_readable(flags) {
            Self::handle_readable_connection(&mut self.redis, &mut self.epoll, connection)?;
        }

        if Self::is_writeable(flags) {
            Self::flush_write_buffer_on_write(&self.epoll, &mut connection)?;
        }

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

    fn get_events(&mut self) -> Result<usize, RedisError> {
        let amount_events = self.epoll.wait(&mut self.events, -1)?;
        Ok(amount_events)
    }

    fn handle_readable_connection(
        redis: &mut Redis,
        epoll: &Epoll,
        connection: &mut Connection,
    ) -> Result<(), RedisError> {
        connection.fill_read_buffer()?;
        loop {
            let return_value = match connection.command_parse_state.state {
                ParseState::Empty => parse_command(
                    &connection.read_buffer.buf,
                    &mut connection.read_buffer.pos,
                    &mut connection.command_parse_state,
                ),
                ParseState::Partial => parse_partial_command(
                    &connection.read_buffer.buf,
                    &mut connection.read_buffer.pos,
                    &mut connection.command_parse_state,
                ),
                _ => unreachable!("COMPLETE COMMAND SHOULD NOT BE ABLE TO REACH HERE"),
            };

            match return_value {
                Ok(_) => {
                    let command = match convert_command_parse_state_to_redis_command(
                        &connection.command_parse_state,
                    ) {
                        Ok(command) => command,
                        Err(e) => {
                            Self::handle_parse_failure(
                                &mut connection.write_buffer,
                                &mut connection.read_buffer,
                                &mut connection.command_parse_state,
                                |wb| handle_command_error(&e, wb),
                            );
                            continue;
                        }
                    };
                    let result = redis.execute_command(&command);
                    Self::handle_redis_result(&result, &mut connection.write_buffer);
                }
                Err(ProtocolError::Incomplete) => {
                    break;
                    // break;
                }
                Err(e) => {
                    Self::handle_parse_failure(
                        &mut connection.write_buffer,
                        &mut connection.read_buffer,
                        &mut connection.command_parse_state,
                        |wb| handle_protocol_error(&e, wb),
                    );
                    continue;
                }
            }

            connection.command_parse_state.clear();

            if connection.read_buffer.pos >= connection.read_buffer.buf.len() {
                connection.read_buffer.clear();
                break;
            }
        }

        Self::flush_write_buffer_after_read(epoll, connection)?;

        Ok(())
    }

    fn handle_redis_result(result: &RedisResult, write_buffer: &mut WriteBuffer) {
        match result {
            RedisResult::SimpleString(simple_string) => {
                write_buffer.append_bytes(simple_string);
            }
            RedisResult::BulkString(bulk_string) => {
                write_buffer.append_bytes(bulk_string.as_slice());
            }
            RedisResult::Int(num) => {
                let response = format!(":{}\r\n", num);
                write_buffer.append_bytes(response.as_bytes());
            }
            _ => unreachable!("FOR NOW YOU SHOULD NOT BE ABLE TO GET HERE"),
        }
    }

    fn flush_write_buffer_after_read(
        epoll: &Epoll,
        connection: &mut Connection,
    ) -> Result<(), RedisError> {
        connection.flush_write_buffer()?;

        if connection.write_buffer.pos != connection.write_buffer.buf.len() {
            Self::poll_socket_out(epoll, connection.soc.fd)?;
        } else {
            connection.write_buffer.clear();
        }

        Ok(())
    }

    fn flush_write_buffer_on_write(
        epoll: &Epoll,
        connection: &mut Connection,
    ) -> Result<(), RedisError> {
        connection.flush_write_buffer()?;

        if connection.write_buffer.pos == connection.write_buffer.buf.len() {
            connection.write_buffer.clear();
            Self::poll_socket_in(epoll, connection.soc.fd)?;
        }

        Ok(())
    }

    fn poll_socket_in(epoll: &Epoll, connection_fd: c_int) -> io::Result<()> {
        epoll.modify(connection_fd, (EPOLLIN | EPOLLERR | EPOLLHUP) as u32)
    }

    fn poll_socket_out(epoll: &Epoll, connection_fd: c_int) -> io::Result<()> {
        epoll.modify(connection_fd, (EPOLLOUT | EPOLLERR | EPOLLHUP) as u32)
    }

    // Helepers

    fn handle_parse_failure<F>(
        write_buf: &mut WriteBuffer,
        read_buf: &mut ReadBuffer,
        parse_state: &mut CommandParseState,
        f: F,
    ) where
        F: FnOnce(&mut WriteBuffer),
    {
        // write the error reply
        f(write_buf);

        read_buf.skip_to_next_command();

        parse_state.clear();
    }

    #[inline(always)]
    fn is_readable(flags: u32) -> bool {
        flags & EPOLLIN as u32 != 0
    }

    #[inline(always)]
    fn is_writeable(flags: u32) -> bool {
        flags & EPOLLOUT as u32 != 0
    }
}
