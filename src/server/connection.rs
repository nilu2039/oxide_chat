use crate::{Message, ResponseMsg};

use redis::Commands;
use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
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

struct Connection {
    last_message: Option<Instant>,
    strike_count: usize,
}

async fn read_body(
    read_stream: &mut OwnedReadHalf,
    body: &mut Vec<u8>,
    connection: &mut Connection,
    redis_conn: &mut redis::Connection,
    connection_addr: &SocketAddr,
    write_stream: &mut OwnedWriteHalf,
    active_connections: &mut HashMap<SocketAddr, String>,
) -> Result<usize, Box<dyn Error + Send + Sync>> {
    let mut buf = [0; 100];
    let mut raw_data = Vec::new();
    let mut final_headers = Vec::new();

    let (_, strike_count_exceed) =
        handle_rate_limit(connection, redis_conn, connection_addr, write_stream).await;

    if strike_count_exceed {
        return Err("Strike count exceeded".into());
    }

    loop {
        body.clear();

        let n = {
            let res = read_stream.read(&mut buf).await;
            match res {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("ERROR: TCP read error, {e}");
                    return Err(e.into());
                }
            }
        };

        if n == 0 {
            handle_client_disconnect(connection_addr, active_connections).await;
            return Err("Client disconnected".into());
        }

        raw_data.extend_from_slice(&buf[..n]);

        if let Some(pos) = raw_data.windows(4).position(|w| w == b"\r\n\r\n") {
            let headers = &raw_data[..pos];
            let partial_body = &raw_data[pos + 4..];
            final_headers.extend_from_slice(headers);
            body.extend_from_slice(partial_body);
            break;
        }
    }

    let headers_str = std::str::from_utf8(&final_headers)?.to_string();
    if let Some(content_length) = headers_str
        .lines()
        .find(|line| line.to_ascii_lowercase().starts_with("content-length"))
        .and_then(|line| line.split_once(":"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
    {
        println!("content_length {content_length}");
        while body.len() < content_length {
            let n = match read_stream.read(&mut buf).await {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("ERROR: TCP read error, {e}");
                    return Err(e.into());
                }
            };

            if n == 0 {
                return Err("Client disconnected".into());
            }

            body.extend_from_slice(&buf[..n]);
        }
    }

    body.retain(|b| (*b >= 32 && *b != 127) || *b == b'\n' || *b == b'\r');
    let n = body.len();

    Ok(n)
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
        match read_body(
            &mut read_stream,
            &mut buf,
            &mut connection,
            &mut redis_conn,
            &connection_addr,
            &mut write_stream,
            &mut active_connections,
        )
        .await
        {
            Ok(n) => {
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
            Err(e) => {
                eprintln!("ERROR: {e}");
                return;
            }
        };
    }

    loop {
        match read_body(
            &mut read_stream,
            &mut buf,
            &mut connection,
            &mut redis_conn,
            &connection_addr,
            &mut write_stream,
            &mut active_connections,
        )
        .await
        {
            Ok(n) => {
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
            Err(e) => {
                eprintln!("ERROR: {e}");
                return;
            }
        };
    }

    println!("INFO: A client connected with address: {connection_addr:?}");

    loop {
        tokio::select! {
            result = read_body(
                &mut read_stream,
                &mut buf,
                &mut connection,
                &mut redis_conn,
                &connection_addr,
                &mut write_stream,
                &mut active_connections,
            ) => {
                match result {
                    Ok(n) => {
                        if let Ok(text) = std::str::from_utf8(&buf[..n]) {
                            if let Some(username) = active_connections.get(&connection_addr) {
                                if let Err(e) = connection_tx.send(AuthorMsg::SendMessage(Message{
                                        text : text.to_string(),
                                        username: username.clone()
                                })) {
                                    eprintln!("ERROR: NewMessage send error, {e}");
                                };
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
