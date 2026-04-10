use std::{
    collections::HashMap,
    io::Write,
    net::TcpListener,
    sync::mpsc::{Receiver, channel},
    thread,
};

use crate::client::{Client, client};
use crate::common::Message;

const ADDRESS: &str = "0.0.0.0:8080";

fn server(messages: Receiver<Message>) {
    let mut clients = HashMap::new();

    while let Ok(msg) = messages.recv() {
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
                    for (client_addr, client) in clients.iter() {
                        if author_addr != *client_addr {
                            let _ = client.conn.as_ref().write_all(&bytes);
                        }
                    }
                }
            }
        }
    }
}

pub fn start() -> Result<(), std::io::Error> {
    let listener = TcpListener::bind(ADDRESS)?;

    let (message_sender, message_receiver) = channel();
    thread::spawn(|| server(message_receiver));

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                let message_sender = message_sender.clone();
                thread::spawn(|| client(s, message_sender));
            }
            Err(err) => {
                eprintln!("ERROR: Failed to receive client tcp stream, {err}");
            }
        }
    }

    Ok(())
}
