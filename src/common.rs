use std::net::SocketAddr;

#[derive(Clone)]
pub enum Message {
    ClientConnected {
        author_addr: SocketAddr,
    },
    NewMessage {
        author_addr: SocketAddr,
        bytes: Vec<u8>,
    },
}
