mod server;
extern crate redis;

use crate::server::connection;
use tokio::net::TcpListener;
use tokio::sync::broadcast::channel;

const ADDRESS: &str = "0.0.0.0:8080";
const REDIS_CLIENT_URL: &str = "redis://127.0.0.1:6379";

async fn start() -> Result<(), std::io::Error> {
    let redis_client = redis::Client::open(REDIS_CLIENT_URL).expect("ERROR: Redis url check fail");

    let listener = TcpListener::bind(ADDRESS).await?;

    let (connection_tx, _) = channel(16);

    let mut token_buf = [0u8; 16];
    if let Err(err) = getrandom::fill(&mut token_buf) {
        eprintln!("ERROR: Failed to generate random token, {err}")
    }

    let token_hex = hex::encode(token_buf);
    println!("Secret access token: {token_hex}");

    loop {
        let (stream, addr) = listener.accept().await?;
        let connection_tx = connection_tx.clone();
        let connection_rx = connection_tx.subscribe();
        let redis_client = redis_client.clone();
        let token_hex = token_hex.clone();
        tokio::spawn(connection(
            stream,
            connection_tx,
            connection_rx,
            addr,
            redis_client,
            token_hex,
        ));
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match start().await {
        Ok(_) => {}
        Err(e) => eprintln!("{:?}", e),
    };
    Ok(())
}
