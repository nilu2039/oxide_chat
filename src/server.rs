use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
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
        author: Arc<TcpStream>,
    },
    NewMessage {
        author: Arc<TcpStream>,
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

            Message::ClientDisconnected { author } => {
                let addr = author.peer_addr().expect("Unable to get client address");
                clients.remove_entry(&addr);
            }

            Message::NewMessage { author, message } => {
                let author_address = author.peer_addr().expect("Unable to get author address");

                for client in clients.values() {
                    let client_address = client
                        .conn
                        .peer_addr()
                        .expect("Unable to get client address");

                    if author_address != client_address {
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

    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line)?;

        if n == 0 {
            let _ = messages.send(Message::ClientDisconnected {
                author: stream.clone(),
            });
            break;
        }

        let _ = messages.send(Message::NewMessage {
            author: stream.clone(),
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
