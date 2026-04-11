mod client;
mod common;
mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match server::start().await {
        Ok(_) => {}
        Err(e) => eprintln!("{:?}", e),
    };
    Ok(())
}
