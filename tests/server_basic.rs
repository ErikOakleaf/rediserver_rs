use serial_test::serial;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

#[test]
#[serial]
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

#[test]
#[serial]
fn test_multiple_server_commands_in_one_message() -> std::io::Result<()> {
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
        commands: &'static [u8],
        expected: &'static [u8],
    }

    let tests = vec![
        // same tests as previous but now combined into one message to se if the server can handle
        // multiple commands in one message
        TestData {
            commands: b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
            expected: b"+OK\r\n$3\r\nbar\r\n",
        },
        TestData {
            commands: b"*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
            expected: b":1\r\n$-1\r\n+OK\r\n",
        },
        TestData {
            commands: b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbaz\r\n*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n",
            expected: b"+OK\r\n$3\r\nbaz\r\n+OK\r\n$5\r\nworld\r\n",
        },
        TestData {
            commands: b"*3\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n$5\r\nhello\r\n*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n",
            expected: b":2\r\n$-1\r\n$-1\r\n",
        },
    ];

    for test in tests {
        stream.write_all(test.commands)?;

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

#[test]
#[serial]
fn test_multiple_server_commands_partial_in_one_message() -> std::io::Result<()> {
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
        commands: Vec<&'static [u8]>,
        expected: Vec<&'static [u8]>,
    }

    let tests = vec![
        TestData {
            commands: vec![
                b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*2\r\n$3\r",
                b"\nGET\r\n$3\r\nfoo\r\n",
            ],
            expected: vec![b"+OK\r\n", b"$3\r\nbar\r\n"],
        },
        TestData {
            commands: vec![
                b"*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n*2\r\n$3\r\nGET\r\n$3\r\nf",
                b"oo\r\n",
            ],
            expected: vec![b":1\r\n", b"$-1\r\n"],
        },
    ];

    for test in tests {
        for (command, expected) in test.commands.into_iter().zip(test.expected) {
            stream.write_all(command)?;

            let mut buf = vec![0u8; expected.len()];
            stream.read_exact(&mut buf)?;

            assert_eq!(
                buf.as_slice(),
                expected,
                "expected {:?}\ngot: {:?}",
                buf.as_slice(),
                expected
            );
        }
    }

    Ok(())
}

#[test]
#[serial]
fn test_server_partial_reads() -> std::io::Result<()> {
    thread::spawn(|| {
        let mut server = redis::server::Server::new(0, 1234).unwrap();
        server.run().unwrap();
    });

    // give server a moment to start
    thread::sleep(Duration::from_millis(200));

    // connect socket to server
    let mut stream = TcpStream::connect("127.0.0.1:1234")?;

    struct TestData {
        command: Vec<&'static [u8]>,
        expected: &'static [u8],
    }

    let tests = vec![
        //basic functionality
        TestData {
            command: vec![b"*3\r\n$3\r\nSET\r\n$3", b"\r\nfoo\r\n$3\r\nbar\r\n"],
            expected: b"+OK\r\n",
        },
        TestData {
            command: vec![
                b"*3\r",
                b"\n$3\r\nSET",
                b"\r\n$",
                b"3\r\nfoo\r",
                b"\n$3\r\nbar\r\n",
            ],
            expected: b"+OK\r\n",
        },
        TestData {
            command: vec![
                b"*2\r",
                b"\n$3\r\nG",
                b"E",
                b"T\r\n",
                b"$3\r",
                b"\nfo",
                b"o\r\n",
            ],
            expected: b"$3\r\nbar\r\n",
        },
        TestData {
            command: vec![
                b"*", b"2", b"\r", b"\n", b"$", b"3", b"\r", b"\n", b"D", b"E", b"L", b"\r", b"\n",
                b"$", b"3", b"\r", b"\n", b"f", b"o", b"o", b"\r", b"\n",
            ],
            expected: b":1\r\n",
        },
    ];

    for test in tests {
        for bytes in test.command {
            stream.write_all(bytes)?;
        }

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

#[test]
#[serial]
fn test_server_partial_writes() -> std::io::Result<()> {
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
        read_amount_bytes: &'static [usize],
        expected: &'static [u8],
    }

    let tests = vec![
        //same as from the first tests but now taking in partial writes
        TestData {
            command: b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
            read_amount_bytes: &[3, 2],
            expected: b"+OK\r\n",
        },
        TestData {
            command: b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
            read_amount_bytes: &[1, 2, 6],
            expected: b"$3\r\nbar\r\n",
        },
        TestData {
            command: b"*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n",
            read_amount_bytes: &[1, 1, 1, 1],
            expected: b":1\r\n",
        },
        TestData {
            command: b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
            read_amount_bytes: &[1, 2, 2],
            expected: b"$-1\r\n",
        },
        TestData {
            command: b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
            read_amount_bytes: &[3, 2],
            expected: b"+OK\r\n",
        },
        TestData {
            command: b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbaz\r\n",
            read_amount_bytes: &[2, 3],
            expected: b"+OK\r\n",
        },
        TestData {
            command: b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
            read_amount_bytes: &[2, 1, 1, 2, 2, 1],
            expected: b"$3\r\nbaz\r\n",
        },
        TestData {
            command: b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
            read_amount_bytes: &[2, 2, 1],
            expected: b"+OK\r\n",
        },
        TestData {
            command: b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n",
            read_amount_bytes: &[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            expected: b"$5\r\nworld\r\n",
        },
        TestData {
            command: b"*3\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n$5\r\nhello\r\n",
            read_amount_bytes: &[1, 3],
            expected: b":2\r\n",
        },
        TestData {
            command: b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
            read_amount_bytes: &[3, 2],
            expected: b"$-1\r\n",
        },
        TestData {
            command: b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n",
            read_amount_bytes: &[2, 1, 1, 1],
            expected: b"$-1\r\n",
        },
    ];

    for test in tests {
        stream.write_all(test.command)?;

        let mut result = Vec::<u8>::new();

        for amount_bytes in test.read_amount_bytes {
            let mut buf = vec![0u8; *amount_bytes];
            stream.read_exact(&mut buf)?;

            result.extend_from_slice(buf.as_slice());
        }

        assert_eq!(
            result.as_slice(),
            test.expected,
            "expected {:?}\ngot: {:?}",
            result.as_slice(),
            test.expected
        );
    }

    Ok(())
}
