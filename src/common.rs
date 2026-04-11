use std::net::SocketAddr;
use tokio::net::tcp::OwnedWriteHalf;

pub enum Message {
    ClientConnected {
        author: OwnedWriteHalf,
    },
    ClientDisconnected {
        author_addr: SocketAddr,
    },
    NewMessage {
        author_addr: SocketAddr,
        bytes: Vec<u8>,
    },
}
