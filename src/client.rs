use crate::common::Message;
use std::{
    io::{BufRead, BufReader},
    net::TcpStream,
    sync::{Arc, mpsc::Sender},
};

pub struct Client {
    pub conn: Arc<TcpStream>,
}

pub fn client(stream: TcpStream, messages: Sender<Message>) -> Result<(), std::io::Error> {
    let stream = Arc::new(stream);
    let mut reader = BufReader::new(stream.as_ref());
    let _ = messages.send(Message::ClientConnected {
        author: stream.clone(),
    });

    let author_addr = stream.peer_addr()?;

    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line)?;

        if n == 0 {
            let _ = messages.send(Message::ClientDisconnected { author_addr });
            break;
        }

        let _ = messages.send(Message::NewMessage {
            author_addr,
            bytes: line.into(),
        });
    }

    Ok(())
}
