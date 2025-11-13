# redisserver_rs

**redisserver_rs** is a Redis server implementation with all components written entirely in Rust

## Features

Currently, `redisserver_rs` supports:

- **GET** – Retrieve the value of a key.
- **SET** – Set the value of a key.
- **DEL** – Delete keys.
- RESP (Redis Serialization Protocol) compliant for the commands above.

## Roadmap

Future plans include:

- Support for more Redis data types: arrays, hashes, sets, etc.
- Efficient internal representation of integers.
- Full RESP protocol compliance.
- Improved performance and connection handling.
