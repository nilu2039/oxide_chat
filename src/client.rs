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
                            let _ = client_tx
                                .send(Message::ClientDisconnected { author_addr: client_addr });
                        }

                        let _ = client_tx.send(Message::NewMessage {
                                author_addr: client_addr,
                                bytes: line.clone().into(),
                        });
                    },
                    Err(err) => {
                        eprintln!("ERROR: Failed to read from client stream, {err}");
                        let _ = client_tx
                            .send(Message::ClientDisconnected {  author_addr: client_addr });
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

                            Message::ClientDisconnected{author_addr} => {
                                if client_addr == author_addr {
                                    println!("INFO: A client disconnected with address: {author_addr:?}");
                                    break;
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

    // let _ = messages
    //     .send(Message::ClientConnected {
    //         author: write_stream,
    //     })
    //     .await;
    //
    // let author_addr = match read_stream.as_ref().peer_addr() {
    //     Ok(addr) => addr,
    //     Err(err) => {
    //         eprintln!("ERROR: Failed to get client peer address, {err}");
    //         return;
    //     }
    // };
    //
    //
    // loop {
    //     let n = match reader.read_line(&mut line).await {
    //         Ok(n) => n,
    //         Err(err) => {
    //             eprintln!("ERROR: Failed to read from client stream, {err}");
    //             let _ = messages
    //                 .send(Message::ClientDisconnected { author_addr })
    //                 .await;
    //             break;
    //         }
    //     };
    //
    //     if n == 0 {
    //         let _ = messages
    //             .send(Message::ClientDisconnected { author_addr })
    //             .await;
    //         break;
    //     }
    //
    //     let _ = messages
    //         .send(Message::NewMessage {
    //             author_addr,
    //             bytes: line.into(),
    //         })
    //         .await;
    // }
}
