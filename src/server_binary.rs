use redis::error::RedisError;
use redis::server::Server;



fn main() -> Result<(), RedisError> {
    let mut server = Server::new(0, 1234)?;

    server.run()?;

    Ok(())
}
