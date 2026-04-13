use redis::Commands;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::broadcast::{Receiver, Sender};

const MESSAGE_RATE: Duration = Duration::from_secs(1);
const MAX_STRIKE_COUNT: usize = 6;
const BAN_LIMIT_IN_SECS: u64 = 60;

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

async fn write_ban_msg_to_stream(write_stream: &mut OwnedWriteHalf, msg: Option<&str>) {
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
        .set_ex::<std::string::String, bool, ()>(
            format!("ban_user_{ip}", ip = client_addr.ip()),
            true,
            BAN_LIMIT_IN_SECS,
        )
        .map_err(|err| {
            eprintln!("ERROR: Unable to set redis ban key, {err}");
        });
}

async fn handle_rate_limit(
    client: &mut Client,
    redis_conn: &mut redis::Connection,
    client_addr: &SocketAddr,
    write_stream: &mut OwnedWriteHalf,
) -> (bool, bool) {
    let now = Instant::now();
    let diff = match client.last_message {
        Some(last) => now.duration_since(last),
        None => MESSAGE_RATE * 2,
    };

    let secs_left = if MESSAGE_RATE > diff {
        MESSAGE_RATE - diff
    } else {
        Duration::from_secs(0)
    };

    if diff > MESSAGE_RATE {
        client.strike_count = 0;
        client.last_message = Option::from(now);
        (false, false)
    } else {
        client.strike_count += 1;
        if client.strike_count >= MAX_STRIKE_COUNT {
            ban_user(redis_conn, &client_addr);
            write_ban_msg_to_stream(write_stream, None).await;
            return (true, true);
        }
        let bytes = format!(
            "You are sending messages too fast, please slow down, {secs} secs left.\n",
            secs = secs_left.as_secs_f32()
        )
        .into_bytes();
        let _ = write_stream.write_all(&bytes).await;
        (true, false)
    }
}

pub async fn client(
    stream: TcpStream,
    client_tx: Sender<Message>,
    mut client_rx: Receiver<Message>,
    client_addr: SocketAddr,
    redis_client: redis::Client,
    valid_token_hex: String,
) {
    let mut client = Client {
        last_message: None,
        strike_count: 0,
    };

    let (mut read_stream, mut write_stream) = stream.into_split();

    let mut redis_conn = redis_client
        .get_connection()
        .expect("ERROR: Redis connection error");

    let is_client_banned = is_banned(&mut redis_conn, &client_addr);

    if is_client_banned {
        write_ban_msg_to_stream(&mut write_stream, None).await;
        return;
    }

    let mut reader = BufReader::new(&mut read_stream);
    let mut buf = Vec::new();

    let _ = write_stream.write_all(b"Enter the security token\n").await;

    let valid_token_bytes = match hex::decode(valid_token_hex) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("ERROR: Hex token decode error, {e}");
            return;
        }
    };

    loop {
        buf.clear();

        match reader.read_until(b'\n', &mut buf).await {
            Ok(n) => {
                if n == 0 {
                    println!("INFO: A client disconnected with address: {client_addr:?}");
                    return;
                }

                let (is_rate_limited, strike_count_exceed) = handle_rate_limit(
                    &mut client,
                    &mut redis_conn,
                    &client_addr,
                    &mut write_stream,
                )
                .await;

                if strike_count_exceed {
                    return;
                }

                if is_rate_limited {
                    continue;
                }

                buf = buf
                    .iter()
                    .copied()
                    .filter(|b| *b >= 32 && *b != 127)
                    .collect();

                let n = buf.len();

                let token_hex = &buf[..n];

                let token_bytes = match hex::decode(token_hex) {
                    Ok(t) => t,
                    Err(e) => {
                        eprintln!("ERROR: Hex token decode error, {e}");
                        let _ = write_stream.write_all(b"Invalid token\n").await;
                        continue;
                    }
                };

                if token_bytes == valid_token_bytes {
                    let _ = write_stream.write_all(b"Welcome!\n").await;
                    break;
                }

                let _ = write_stream.write_all(b"Invalid token\n").await;
            }
            Err(err) => {
                eprintln!("ERROR: Unable to read security token, {err}");
                return;
            }
        };
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

                        buf = buf
                            .iter()
                            .copied()
                            .filter(|b| *b >= 32 && *b != 127)
                            .collect();
                        buf.push(b'\n');
                        let n = buf.len();

                        if !std::str::from_utf8(&buf[..n]).is_ok() {
                            eprintln!("ERROR: Stream did not contain valid UTF-8");
                            ban_user(&mut redis_conn, &client_addr);
                            write_ban_msg_to_stream(&mut write_stream, Option::from("Invalid UTF-8, you are banned\n")).await;
                            println!("INFO: Client disconnected with address: {client_addr:?}");
                            break;
                        }

                        let (is_rate_limited, strike_count_exceed) = handle_rate_limit(
                            &mut client,
                            &mut redis_conn,
                            &client_addr,
                            &mut write_stream,
                        )
                        .await;

                        if !is_rate_limited {
                            let _ = client_tx.send(Message::NewMessage {
                                    author_addr: client_addr,
                                    bytes: buf.clone()
                            });
                        } else {
                            if strike_count_exceed {
                                break;
                            }
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
