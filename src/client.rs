use redis::Commands;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::broadcast::{Receiver, Sender};

const MESSAGE_RATE: Duration = Duration::from_secs(1);
const MAX_STRIKE_COUNT: usize = 6;

#[derive(Clone)]
pub enum Message {
    NewMessage {
        author_addr: SocketAddr,
        bytes: Vec<u8>,
    },
}

struct Client {
    last_message: Option<Instant>,
    strike_count: usize,
}

async fn write_ban_msg_to_stream(mut write_stream: OwnedWriteHalf, msg: Option<&str>) {
    println!("INFO: User is banned");

    let msg = match msg {
        Some(msg) => msg,
        None => "You are banned\n",
    };

    if let Err(e) = write_stream.write_all(msg.as_bytes()).await {
        eprintln!("Write error: {e}");
        return;
    }
    let _ = write_stream.shutdown().await;
}

fn is_banned(redis_conn: &mut redis::Connection, client_addr: &SocketAddr) -> bool {
    let is_client_banned = match redis_conn.get(format!("ban_user_{ip}", ip = client_addr.ip())) {
        Ok(b) => b,
        Err(err) => {
            eprintln!("ERROR: Unable to get banned user, {err}");
            false
        }
    };

    return is_client_banned;
}

fn ban_user(redis_conn: &mut redis::Connection, client_addr: &SocketAddr) {
    let _ = redis_conn
        .set::<std::string::String, bool, ()>(format!("ban_user_{ip}", ip = client_addr.ip()), true)
        .map_err(|err| {
            eprintln!("ERROR: Unable to set redis ban key, {err}");
        });
}

pub async fn client(
    stream: TcpStream,
    client_tx: Sender<Message>,
    mut client_rx: Receiver<Message>,
    client_addr: SocketAddr,
    redis_client: redis::Client,
) {
    let mut redis_conn = redis_client
        .get_connection()
        .expect("ERROR: Redis connection error");

    let is_client_banned = is_banned(&mut redis_conn, &client_addr);

    let mut client = Client {
        last_message: None,
        strike_count: 0,
    };

    let (read_stream, mut write_stream) = stream.into_split();

    if is_client_banned {
        write_ban_msg_to_stream(write_stream, None).await;
        return;
    }

    println!("INFO: A client connected with address: {client_addr:?}");

    let mut reader = BufReader::new(read_stream);
    let mut buf = Vec::new();

    loop {
        buf.clear();

        tokio::select! {
            result = reader.read_until(b'\n', &mut buf) => {
                match result {
                    Ok(n) => {

                        if n == 0 {
                            println!("INFO: A client disconnected with address: {client_addr:?}");
                            break;
                        }

                        if !std::str::from_utf8(&buf[..n]).is_ok() {
                            eprintln!("ERROR: Stream did not contain valid UTF-8");
                            ban_user(&mut redis_conn, &client_addr);
                            write_ban_msg_to_stream(write_stream, Option::from("Invalid UTF-8, you are banned\n")).await;
                            println!("INFO: Client disconnected with address: {client_addr:?}");
                            break;
                        }


                        let now = Instant::now();
                        let diff = match client.last_message {
                            Some(last) => now.duration_since(last),
                            None => MESSAGE_RATE * 2,
                        };

                        if diff > MESSAGE_RATE {
                            client.strike_count = 0;
                            let _ = client_tx.send(Message::NewMessage {
                                    author_addr: client_addr,
                                    bytes: buf.clone()
                            });
                            client.last_message = Option::from(now);
                        } else {
                            client.strike_count += 1;
                            if client.strike_count >= MAX_STRIKE_COUNT {
                                ban_user(&mut redis_conn, &client_addr);
                                write_ban_msg_to_stream(write_stream, None).await;
                                break;
                            }
                            let bytes = std::format!("You are sending messages too fast, please slow down, {secs} secs left.\n", secs = (MESSAGE_RATE - diff).as_secs_f32()).into_bytes();
                            let _ = write_stream.write_all(&bytes).await;
                        }

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
