use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc,
        mpsc::{Receiver, Sender, channel},
    },
    thread,
};

struct Client {
    conn: Arc<TcpStream>,
}

enum Message {
    ClientConnected {
        author: Arc<TcpStream>,
    },
    ClientDisconnected {
        author_addr: SocketAddr,
    },
    NewMessage {
        author_addr: SocketAddr,
        message: Vec<u8>,
    },
}

fn server(messages: Receiver<Message>) {
    let mut clients = HashMap::new();

    loop {
        let msg = messages.recv().unwrap();

        match msg {
            Message::ClientConnected { author } => {
                let addr = author.peer_addr().expect("Unable to get client address");
                clients.insert(
                    addr,
                    Client {
                        conn: author.clone(),
                    },
                );
            }

            Message::ClientDisconnected { author_addr } => {
                clients.remove_entry(&author_addr);
            }

            Message::NewMessage {
                author_addr,
                message,
            } => {
                for client in clients.values() {
                    let client_address = client
                        .conn
                        .peer_addr()
                        .expect("Unable to get client address");

                    if author_addr != client_address {
                        let _ = client.conn.as_ref().write_all(&message);
                    }
                }
            }
        }
    }
}

fn client(stream: TcpStream, messages: Sender<Message>) -> Result<(), std::io::Error> {
    let stream = Arc::new(stream);
    let mut reader = BufReader::new(stream.as_ref());
    let _ = messages.send(Message::ClientConnected {
        author: stream.clone(),
    });
    let author_addr = stream.peer_addr().expect("Unable to get author address");

    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line)?;

        if n == 0 {
            let _ = messages.send(Message::ClientDisconnected { author_addr });
            break;
        }

        let _ = messages.send(Message::NewMessage {
            author_addr,
            message: line.into(),
        });
    }
    Ok(())
}

pub fn start() -> Result<(), std::io::Error> {
    let listener = TcpListener::bind("0.0.0.0:8080")?;

    let (message_sender, message_receiver) = channel();
    thread::spawn(|| server(message_receiver));

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                // let stream = Arc::new(s);
                let message_sender = message_sender.clone();
                thread::spawn(|| client(s, message_sender));
            }
            Err(e) => {
                panic!("{:?}", e);
            }
        }
    }

    Ok(())
}
