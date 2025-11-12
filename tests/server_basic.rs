use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

fn main() -> std::io::Result<()> {
    // spawn server in another thread
    thread::spawn(|| {
        redis::server::main().unwrap();
    });

    // give server a moment to start
    thread::sleep(Duration::from_millis(200));

    let mut stream = TcpStream::connect("127.0.0.1:1234")?;

    // send a command
    let cmd = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    stream.write_all(cmd)?; // write all bytes

    // let's say we expect a reply of exactly 5 bytes: "+OK\r\n"
    let mut buf = [0u8; 5];
    stream.read_exact(&mut buf)?; // blocks until all 5 bytes are received

    assert_eq!(&buf, b"+OK\r\n");
    Ok(())
}
