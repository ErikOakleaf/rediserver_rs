use serial_test::serial;
use std::fmt::format;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

// #[test]
// #[serial]
fn test_many_writes_and_reads_from_one_socket() -> std::io::Result<()> {
    // spawn server in another thread

    thread::spawn(|| {
        let mut server = redis::server::Server::new(0, 1234).unwrap();
        server.run().unwrap();
    });

    // give server a moment to start
    thread::sleep(Duration::from_millis(200));

    // connect socket to server
    let mut stream = TcpStream::connect("127.0.0.1:1234")?;
    let amount_writes_and_reads = 10000;

    // do writes
    for i in 0..amount_writes_and_reads {
        let key = format!("key{}", i);
        let value = format!("value{}", i);

        let command = format!(
            "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            key.len(),
            key,
            value.len(),
            value
        );

        stream.write_all(command.as_bytes())?;

        let mut buf = vec![0u8; 5];
        stream.read_exact(&mut buf)?;

        assert_eq!(
            buf.as_slice(),
            b"+OK\r\n",
            "expected {:?}\ngot: {:?}",
            buf.as_slice(),
            b"+OK\r\n"
        );
    }

    // do reads
    for i in 0..amount_writes_and_reads {
        let key = format!("key{}", i);
        let value = format!("value{}", i);

        let command = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key,);

        stream.write_all(command.as_bytes())?;

        let expected = format!("${}\r\n{}\r\n", value.len(), value);
        let mut buf = vec![0u8; expected.len()];
        stream.read_exact(&mut buf)?;

        assert_eq!(
            buf.as_slice(),
            expected.as_bytes(),
            "expected {:?}\ngot: {:?}",
            buf.as_slice(),
            expected.as_bytes(),
        );
    }
    Ok(())
}
