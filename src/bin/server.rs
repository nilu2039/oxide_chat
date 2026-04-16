use redis::Commands;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::broadcast::{Receiver, Sender};

extern crate redis;

use tokio::net::TcpListener;
use tokio::sync::broadcast::channel;

const MESSAGE_RATE: Duration = Duration::from_secs(1);
const MAX_STRIKE_COUNT: usize = 6;
const BAN_LIMIT_IN_SECS: u64 = 60;

const ADDRESS: &str = "0.0.0.0:8080";
const REDIS_CLIENT_URL: &str = "redis://127.0.0.1:6379";

#[derive(Clone)]
pub enum Message {
    NewMessage {
        author_addr: SocketAddr,
        bytes: Vec<u8>,
    },
}

struct Connection {
    last_message: Option<Instant>,
    strike_count: usize,
}

async fn start() -> Result<(), std::io::Error> {
    let redis_client = redis::Client::open(REDIS_CLIENT_URL).expect("ERROR: Redis url check fail");

    let listener = TcpListener::bind(ADDRESS).await?;

    let (connection_tx, _) = channel(16);

    let mut token_buf = [0u8; 16];
    if let Err(err) = getrandom::fill(&mut token_buf) {
        eprintln!("ERROR: Failed to generate random token, {err}")
    }

    let token_hex = hex::encode(token_buf);
    println!("Secret access token: {token_hex}");

    loop {
        let (stream, addr) = listener.accept().await?;
        let connection_tx = connection_tx.clone();
        let connection_rx = connection_tx.subscribe();
        let redis_client = redis_client.clone();
        let token_hex = token_hex.clone();
        tokio::spawn(connection(
            stream,
            connection_tx,
            connection_rx,
            addr,
            redis_client,
            token_hex,
        ));
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match start().await {
        Ok(_) => {}
        Err(e) => eprintln!("{:?}", e),
    };
    Ok(())
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
    if let Err(e) = write_stream.shutdown().await {
        eprintln!("ERROR: shutdown error, {e}");
    }
}

fn is_ip_banned(redis_conn: &mut redis::Connection, connection_addr: &SocketAddr) -> bool {
    let is_ip_banned = match redis_conn.get(format!("ban_user_{ip}", ip = connection_addr.ip())) {
        Ok(b) => b,
        Err(err) => {
            eprintln!("ERROR: Unable to get banned user, {err}");
            false
        }
    };

    return is_ip_banned;
}

async fn ban_user(
    redis_conn: &mut redis::Connection,
    write_stream: &mut OwnedWriteHalf,
    connection_addr: &SocketAddr,
    ban_msg: Option<&str>,
) {
    if let Err(e) = redis_conn.set_ex::<std::string::String, bool, ()>(
        format!("ban_user_{ip}", ip = connection_addr.ip()),
        true,
        BAN_LIMIT_IN_SECS,
    ) {
        eprintln!("ERROR: Unable to set redis ban key, {e}");
        if let Err(e) = write_stream.write_all(b"Something went wrong\n").await {
            eprintln!("ERROR: Tcp write fail, {e}");
        }
    } else {
        write_ban_msg_to_stream(write_stream, ban_msg).await;
    }
}

async fn handle_rate_limit(
    connection: &mut Connection,
    redis_conn: &mut redis::Connection,
    connection_addr: &SocketAddr,
    write_stream: &mut OwnedWriteHalf,
) -> (bool, bool) {
    let now = Instant::now();
    let diff = match connection.last_message {
        Some(last) => now.duration_since(last),
        None => MESSAGE_RATE * 2,
    };

    let secs_left = if MESSAGE_RATE > diff {
        MESSAGE_RATE - diff
    } else {
        Duration::from_secs(0)
    };

    if diff > MESSAGE_RATE {
        connection.strike_count = 0;
        connection.last_message = Option::from(now);
        (false, false)
    } else {
        connection.strike_count += 1;
        if connection.strike_count >= MAX_STRIKE_COUNT {
            ban_user(redis_conn, write_stream, &connection_addr, None).await;
            return (true, true);
        }
        let bytes = format!(
            "You are sending messages too fast, please slow down, {secs} secs left.\n",
            secs = secs_left.as_secs_f32()
        )
        .into_bytes();
        if let Err(e) = write_stream.write_all(&bytes).await {
            eprintln!("ERROR: Tcp write fail, {e}");
        }
        (true, false)
    }
}

pub async fn connection(
    stream: TcpStream,
    connection_tx: Sender<Message>,
    mut connection_rx: Receiver<Message>,
    connection_addr: SocketAddr,
    redis_client: redis::Client,
    valid_token_hex: String,
) {
    let mut redis_conn = match redis_client.get_connection() {
        Ok(d) => d,
        Err(err) => {
            eprintln!("ERROR: Redis connection error, {err}");
            return;
        }
    };

    let mut connection = Connection {
        last_message: None,
        strike_count: 0,
    };

    let (mut read_stream, mut write_stream) = stream.into_split();

    let is_ip_banned = is_ip_banned(&mut redis_conn, &connection_addr);

    if is_ip_banned {
        write_ban_msg_to_stream(&mut write_stream, None).await;
        return;
    }

    let mut reader = BufReader::new(&mut read_stream);
    let mut buf = Vec::new();

    if let Err(e) = write_stream.write_all(b"Enter the security token\n").await {
        eprintln!("ERROR: Tcp write fail, {e}");
    }

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
                    println!("INFO: A client disconnected with address: {connection_addr:?}");
                    return;
                }

                let (is_rate_limited, strike_count_exceed) = handle_rate_limit(
                    &mut connection,
                    &mut redis_conn,
                    &connection_addr,
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
                        if let Err(e) = write_stream.write_all(b"Invalid token\n").await {
                            eprintln!("ERROR: Tcp write fail, {e}");
                        }
                        continue;
                    }
                };

                if token_bytes == valid_token_bytes {
                    if let Err(e) = write_stream.write_all(b"Welcome!\n").await {
                        eprintln!("ERROR: Tcp write fail, {e}");
                    }
                    break;
                }

                if let Err(e) = write_stream.write_all(b"Invalid token\n").await {
                    eprintln!("ERROR: Tcp write fail, {e}");
                }
            }
            Err(err) => {
                eprintln!("ERROR: Unable to read security token, {err}");
                return;
            }
        };
    }

    println!("INFO: A client connected with address: {connection_addr:?}");

    let mut reader = BufReader::new(read_stream);
    let mut buf = Vec::new();

    loop {
        buf.clear();

        tokio::select! {
            result = reader.read_until(b'\n', &mut buf) => {
                match result {
                    Ok(n) => {
                        if n == 0 {
                            println!("INFO: A client disconnected with address: {connection_addr:?}");
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
                            ban_user(&mut redis_conn, &mut write_stream,&connection_addr, Option::from("Invalid UTF-8, you are banned\n")).await;
                            println!("INFO: Client disconnected with address: {connection_addr:?}");
                            break;
                        }

                        let (is_rate_limited, strike_count_exceed) = handle_rate_limit(
                            &mut connection,
                            &mut redis_conn,
                            &connection_addr,
                            &mut write_stream,
                        )
                        .await;

                        if !is_rate_limited {
                            if let Err(e) = connection_tx.send(Message::NewMessage {
                                    author_addr: connection_addr,
                                    bytes: buf.clone()
                            }) {
                                eprintln!("ERROR: NewMessage send error, {e}");
                            };
                        } else {
                            if strike_count_exceed {
                                break;
                            }
                        }

                    },
                    Err(err) => {
                        eprintln!("ERROR: Failed to read from client stream, {err}");
                        println!("INFO: A client disconnected with address: {connection_addr:?}");
                        break;
                    }
                }
            }

            result = connection_rx.recv() => {
                match result {
                    Ok(msg) => {
                        match msg {
                            Message::NewMessage{author_addr, bytes} => {
                                if connection_addr != author_addr {
                                    if let Err(e) = write_stream.write_all(&bytes).await {
                                        eprintln!("ERROR: Tcp write fail, {e}");
                                        break
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
