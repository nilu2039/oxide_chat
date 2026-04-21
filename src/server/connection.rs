use crate::Message;

use redis::Commands;
use serde::Serialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::broadcast::{Receiver, Sender};

extern crate redis;

const MESSAGE_RATE: Duration = Duration::from_secs(1);
const MAX_STRIKE_COUNT: usize = 6;
const BAN_LIMIT_IN_SECS: u64 = 60;

#[derive(Clone)]
pub enum AuthorMsg {
    SendMessage(Message),
    SendInfo(String),
    SendError(String),
}

#[derive(Serialize)]
pub struct ResponseMsg {
    data: Option<Message>,
    info_msg: Option<String>,
    err_msg: Option<String>,
}

struct Connection {
    last_message: Option<Instant>,
    strike_count: usize,
}

async fn send_response(
    write_stream: &mut OwnedWriteHalf,
    author_msg: AuthorMsg,
) -> Result<(), Box<dyn std::error::Error>> {
    match author_msg {
        AuthorMsg::SendMessage(author_msg) => {
            let res_msg = ResponseMsg {
                data: Option::from(Message {
                    username: author_msg.username.clone(),
                    text: author_msg.text.clone(),
                }),
                err_msg: None,
                info_msg: None,
            };
            if let Ok(mut json_str) = serde_json::to_string(&res_msg) {
                json_str.push('\n');
                write_stream.write_all(json_str.as_bytes()).await?;
            }
            Ok(())
        }
        AuthorMsg::SendInfo(msg) => {
            let res_msg = ResponseMsg {
                data: None,
                info_msg: Option::from(msg),
                err_msg: None,
            };
            if let Ok(mut json_str) = serde_json::to_string(&res_msg) {
                json_str.push('\n');
                write_stream.write_all(json_str.as_bytes()).await?
            }
            Ok(())
        }
        AuthorMsg::SendError(msg) => {
            let res_msg = ResponseMsg {
                data: None,
                info_msg: None,
                err_msg: Option::from(msg),
            };
            if let Ok(mut json_str) = serde_json::to_string(&res_msg) {
                json_str.push('\n');
                write_stream.write_all(json_str.as_bytes()).await?
            }
            Ok(())
        }
    }
}

async fn write_ban_msg_to_stream(write_stream: &mut OwnedWriteHalf, msg: Option<&str>) {
    println!("INFO: User is banned");

    let msg = match msg {
        Some(msg) => msg,
        None => "You are banned\n",
    };

    if let Err(e) = send_response(write_stream, AuthorMsg::SendInfo(msg.to_string())).await {
        eprintln!("ERROR: {e}");
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
        if let Err(e) = send_response(
            write_stream,
            AuthorMsg::SendError("Something went wrong\n".to_string()),
        )
        .await
        {
            eprintln!("ERROR: {e}");
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
        let msg = format!(
            "You are sending messages too fast, please slow down, {secs} secs left.\n",
            secs = secs_left.as_secs_f32()
        )
        .to_string();

        if let Err(e) = send_response(write_stream, AuthorMsg::SendInfo(msg)).await {
            eprintln!("ERROR: {e}");
        }

        (true, false)
    }
}

pub async fn handle_client_disconnect(
    connection_addr: &SocketAddr,
    active_connections: &mut HashMap<SocketAddr, String>,
) {
    println!("INFO: A client disconnected with address {connection_addr:?}");
    active_connections.remove(connection_addr);
}

pub async fn connection(
    stream: TcpStream,
    connection_tx: Sender<AuthorMsg>,
    mut connection_rx: Receiver<AuthorMsg>,
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

    let mut active_connections = HashMap::new();

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

    if let Err(e) = send_response(
        &mut write_stream,
        AuthorMsg::SendInfo("Enter the security token\n".to_string()),
    )
    .await
    {
        eprintln!("ERROR: {e}");
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
                    println!("INFO: connection connection broken: {connection_addr:?}");
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
                        if let Err(e) = send_response(
                            &mut write_stream,
                            AuthorMsg::SendInfo("Invalid token\n".to_string()),
                        )
                        .await
                        {
                            eprintln!("ERROR: {e}");
                        }
                        continue;
                    }
                };

                if token_bytes == valid_token_bytes {
                    if let Err(e) = send_response(
                        &mut write_stream,
                        AuthorMsg::SendInfo("Welcome, please enter an username!\n".to_string()),
                    )
                    .await
                    {
                        eprintln!("ERROR: {e}");
                    }
                    break;
                }

                if let Err(e) = send_response(
                    &mut write_stream,
                    AuthorMsg::SendInfo("Invalid token\n".to_string()),
                )
                .await
                {
                    eprintln!("ERROR: {e}");
                }
            }
            Err(err) => {
                eprintln!("ERROR: Unable to read security token, {err}");
                return;
            }
        };
    }

    loop {
        buf.clear();

        match reader.read_until(b'\n', &mut buf).await {
            Ok(n) => {
                if n == 0 {
                    println!("INFO: client connection broken: {connection_addr:?}");
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

                if let Ok(username) = std::str::from_utf8(&buf[..n]) {
                    active_connections.insert(connection_addr, username.to_string());
                    if let Err(e) = send_response(
                        &mut write_stream,
                        AuthorMsg::SendInfo(format!("Welcome {username}!\n").to_string()),
                    )
                    .await
                    {
                        eprintln!("ERROR: {e}");
                    }
                    break;
                } else {
                    eprintln!("ERROR: Invalid UTF-8 username");
                    if let Err(e) = send_response(
                        &mut write_stream,
                        AuthorMsg::SendError("Invalid username format\n".to_string()),
                    )
                    .await
                    {
                        eprintln!("ERROR: {e}");
                    }
                }
            }
            Err(err) => {
                eprintln!("ERROR: Unable to read security token, {err}");
                return;
            }
        }
    }

    println!("INFO: A client connected with address: {connection_addr:?}");

    loop {
        buf.clear();

        tokio::select! {
            result = reader.read_until(b'\n', &mut buf) => {
                match result {
                    Ok(n) => {
                        if n == 0 {
                            handle_client_disconnect(
                                &connection_addr,
                                &mut active_connections
                               ).await;
                            break;
                        }

                        buf = buf
                            .iter()
                            .copied()
                            .filter(|b| *b >= 32 && *b != 127)
                            .collect();
                        let n = buf.len();

                        if let Ok(text) = std::str::from_utf8(&buf[..n]) {
                            let (is_rate_limited, strike_count_exceed) = handle_rate_limit(
                                &mut connection,
                                &mut redis_conn,
                                &connection_addr,
                                &mut write_stream,
                            )
                            .await;

                            if !is_rate_limited {
                                if let Some(username) = active_connections.get(&connection_addr) {
                                    if let Err(e) = connection_tx.send(AuthorMsg::SendMessage(Message{
                                            text : text.to_string(),
                                            username: username.clone()
                                    })) {
                                        eprintln!("ERROR: NewMessage send error, {e}");
                                    };
                                }
                            } else {
                                if strike_count_exceed {
                                    break;
                                }
                            }
                        } else {
                            eprintln!("ERROR: Stream did not contain valid UTF-8");

                            ban_user(&mut redis_conn,
                                &mut write_stream,&connection_addr,
                                Option::from("Invalid UTF-8, you are banned\n"),
                                ).await;

                            handle_client_disconnect(
                                &connection_addr,
                                &mut active_connections
                                ).await;
                            break;

                        }
                    },
                    Err(err) => {
                        eprintln!("ERROR: Failed to read from client stream, {err}");
                            handle_client_disconnect(
                                &connection_addr,
                                &mut active_connections
                                ).await;
                        break;
                    }
                }
            }

            result = connection_rx.recv() => {
                match result {
                    Ok(msg) => {
                        if let Err(e) = send_response(&mut write_stream, msg).await {
                            eprintln!("ERROR: {e}");
                            return;
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
