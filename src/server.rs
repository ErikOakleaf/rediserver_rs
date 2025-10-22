use std::io;

use libc::{EPOLLERR, EPOLLHUP, EPOLLIN, EPOLLOUT, c_int, epoll_event};
use redis::{
    connection::{Connection, ConnectionAction},
    error::RedisError,
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

        let connection = match &mut self.connections[fd as usize] {
            Some(connection) => connection,
            None => return Ok(()), // TODO - this should probably return some sort of error since
                                   // there is not a connection to a socket that is till there
        };

        if (flags & EPOLLIN as u32) != 0 {
            Self::handle_connection_action(
                &mut self.epoll,
                fd as c_int,
                connection.handle_readable()?,
            )?;
        }

        if (flags & EPOLLOUT as u32) != 0 {
            Self::handle_connection_action(
                &mut self.epoll,
                fd as c_int,
                connection.handle_writeable()?,
            )?;
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

    fn handle_connection_action(
        epoll: &mut Epoll,
        connection_fd: c_int,
        connection_action: ConnectionAction,
    ) -> Result<(), RedisError> {
        match connection_action {
            ConnectionAction::WantRead => {
                epoll.modify(connection_fd, (EPOLLIN | EPOLLERR | EPOLLHUP) as u32)?;
            }
            ConnectionAction::WantWrite => {
                epoll.modify(connection_fd, (EPOLLOUT | EPOLLERR | EPOLLHUP) as u32)?;
            }
            ConnectionAction::End => {}
            ConnectionAction::None => {}
        };

        Ok(())
    }

    // Helpers

    fn get_events(&mut self) -> Result<usize, RedisError> {
        let amount_events = self.epoll.wait(&mut self.events, -1)?;
        Ok(amount_events)
    }
}

fn main() -> Result<(), RedisError> {
    let mut server = Server::new(0, 1234)?;

    server.handle_events()?;

    Ok(())
}
