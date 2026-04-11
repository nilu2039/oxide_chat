use tokio::net::TcpStream;

use crate::common::Message;
use tokio::net::tcp::OwnedWriteHalf;

use tokio::io::{AsyncBufReadExt, BufReader};

use tokio::sync::mpsc::Sender;

pub struct Client {
    pub conn: OwnedWriteHalf,
}

pub async fn client(stream: TcpStream, messages: Sender<Message>) {
    let (read_stream, write_stream) = stream.into_split();
    let _ = messages
        .send(Message::ClientConnected {
            author: write_stream,
        })
        .await;

    let author_addr = match read_stream.as_ref().peer_addr() {
        Ok(addr) => addr,
        Err(err) => {
            eprintln!("ERROR: Failed to get client peer address, {err}");
            return;
        }
    };

    let mut reader = BufReader::new(read_stream);

    loop {
        let mut line = String::new();
        let n = match reader.read_line(&mut line).await {
            Ok(n) => n,
            Err(err) => {
                eprintln!("ERROR: Failed to read from client stream, {err}");
                let _ = messages
                    .send(Message::ClientDisconnected { author_addr })
                    .await;
                break;
            }
        };

        if n == 0 {
            let _ = messages
                .send(Message::ClientDisconnected { author_addr })
                .await;
            break;
        }

        let _ = messages
            .send(Message::NewMessage {
                author_addr,
                bytes: line.into(),
            })
            .await;
    }
}
