mod client;
mod common;

use crate::client::client;
use tokio::net::TcpListener;
use tokio::sync::broadcast::channel;

const ADDRESS: &str = "0.0.0.0:8080";

async fn start() -> Result<(), std::io::Error> {
    let listener = TcpListener::bind(ADDRESS).await?;

    let (client_tx, _) = channel(16);

    loop {
        let (stream, addr) = listener.accept().await?;
        let client_tx = client_tx.clone();
        let client_rx = client_tx.subscribe();
        tokio::spawn(client(stream, client_tx, client_rx, addr));
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
