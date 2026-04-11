use std::collections::HashMap;

use tokio::{io::AsyncWriteExt, net::TcpListener};

use crate::client::{Client, client};
use crate::common::Message;
use tokio::sync::mpsc::{Receiver, channel};

const ADDRESS: &str = "0.0.0.0:8080";

async fn server(mut messages: Receiver<Message>) {
    let mut clients = HashMap::new();

    while let Some(msg) = messages.recv().await {
        match msg {
            Message::ClientConnected { author } => {
                let addr = match author.peer_addr() {
                    Ok(addr) => addr,
                    Err(err) => {
                        eprintln!("ERROR: failed to get client address: {err}");
                        continue;
                    }
                };
                clients.insert(addr, Client { conn: author });
            }

            Message::ClientDisconnected { author_addr } => {
                clients.remove_entry(&author_addr);
            }

            Message::NewMessage { author_addr, bytes } => {
                if std::str::from_utf8(&bytes).is_ok() {
                    for (client_addr, client) in clients.iter_mut() {
                        if author_addr != *client_addr {
                            let _ = client.conn.write_all(&bytes).await;
                        }
                    }
                }
            }
        }
    }
}

pub async fn start() -> Result<(), std::io::Error> {
    let listener = TcpListener::bind(ADDRESS).await?;

    let (message_sender, message_receiver) = channel(100);
    tokio::spawn(server(message_receiver));

    loop {
        let (stream, _) = listener.accept().await?;
        let message_sender = message_sender.clone();
        tokio::spawn(client(stream, message_sender));
    }
}
