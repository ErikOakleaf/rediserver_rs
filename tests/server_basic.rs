use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

#[test]
fn test_basic_server_commands() -> std::io::Result<()> {
    // spawn server in another thread

    thread::spawn(|| {
        let mut server = redis::server::Server::new(0, 1234).unwrap();
        server.run().unwrap();
    });

    // give server a moment to start
    thread::sleep(Duration::from_millis(200));

    // connect socket to server
    let mut stream = TcpStream::connect("127.0.0.1:1234")?;

    struct TestData {
        command: &'static [u8],
        expected: &'static [u8],
    }

    let tests = vec![
        //basic functionality
        TestData {
            command: b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
            expected: b"+OK\r\n",
        },
        TestData {
            command: b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
            expected: b"$3\r\nbar\r\n",
        },
        TestData {
            command: b"*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n",
            expected: b":1\r\n",
        },
        TestData {
            command: b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
            expected: b"$-1\r\n",
        },
        // test overwriting sets
        TestData {
            command: b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
            expected: b"+OK\r\n",
        },
        TestData {
            command: b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbaz\r\n",
            expected: b"+OK\r\n",
        },
        TestData {
            command: b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
            expected: b"$3\r\nbaz\r\n",
        },
        // multiple deletes
        TestData {
            command: b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
            expected: b"+OK\r\n",
        },
        TestData {
            command: b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n",
            expected: b"$5\r\nworld\r\n",
        },
        TestData {
            command: b"*3\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n$5\r\nhello\r\n",
            expected: b":2\r\n",
        },
        TestData {
            command: b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
            expected: b"$-1\r\n",
        },
        TestData {
            command: b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n",
            expected: b"$-1\r\n",
        },
    ];

    for test in tests {
        stream.write_all(test.command)?;

        let mut buf = vec![0u8; test.expected.len()];
        stream.read_exact(&mut buf)?;

        assert_eq!(
            buf.as_slice(),
            test.expected,
            "expected {:?}\ngot: {:?}",
            buf.as_slice(),
            test.expected
        );
    }

    Ok(())
}
