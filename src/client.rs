use crate::common::Message;
use std::net::SocketAddr;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::broadcast::{Receiver, Sender};

pub async fn client(
    stream: TcpStream,
    client_tx: Sender<Message>,
    mut client_rx: Receiver<Message>,
    client_addr: SocketAddr,
) {
    let (read_stream, mut write_stream) = stream.into_split();

    let _ = client_tx.send(Message::ClientConnected {
        author_addr: client_addr,
    });

    let mut reader = BufReader::new(read_stream);
    let mut line = String::new();

    loop {
        line.clear();

        select! {
            result = reader.read_line(&mut line) => {
                match result {
                    Ok(n) => {
                        if n == 0 {
                        println!("INFO: A client disconnected with address: {client_addr:?}");
                        break;
                        }

                        let _ = client_tx.send(Message::NewMessage {
                                author_addr: client_addr,
                                bytes: line.as_bytes().to_vec()
                        });
                    },
                    Err(err) => {
                        eprintln!("ERROR: Failed to read from client stream, {err}");
                        println!("INFO: A client disconnected with address: {client_addr:?}");
                        break;
                    }
                }
            }

            result = client_rx.recv() => {
                match result {
                    Ok(msg) => {
                        match msg {
                            Message::ClientConnected{author_addr} => {
                                if client_addr == author_addr {
                                    println!("INFO: A client connected with address: {author_addr:?}");
                                }
                            }

                            Message::NewMessage{author_addr, bytes} => {
                                if client_addr != author_addr {
                                    match write_stream.write_all(&bytes).await {
                                        Ok(()) => {},
                                        Err(err) => {
                                            eprintln!("ERROR: {err}");
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(err) => {
                        eprintln!("ERROR: {err}");
                        break
                    }
                }
            }
        }
    }
}
