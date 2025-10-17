use redis::net::{Socket, make_ipv4_address};

fn main() {
    println!("SERVER");
    let soc = Socket::new_tcp_();
    soc.set_reuseaddr().unwrap();

    let address = make_ipv4_address(0, 1234);

    soc.bind(&address).unwrap();

    soc.listen().unwrap();

    loop {
        let (connection_socket, _) = match soc.accept() {
            Ok(value) => value,
            Err(_) => continue,
        };

        let mut read_buffer: [u8; 64] = [0; 64];
        connection_socket.read(&mut read_buffer).unwrap();

        let s = std::str::from_utf8(&read_buffer).unwrap();
        println!("client says: {}", s);

        let write_buffer: [u8; 5] = *b"world";
        connection_socket.write(&write_buffer).unwrap();
    }
}
