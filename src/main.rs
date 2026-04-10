mod client;
mod common;
mod server;

fn main() {
    match server::start() {
        Ok(_) => {}
        Err(e) => println!("{:?}", e),
    };
}
