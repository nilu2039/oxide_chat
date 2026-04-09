use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
};

pub fn start() -> Result<(), std::io::Error> {
    let listener = TcpListener::bind("0.0.0.0:8080")?;

    for stream in listener.incoming() {
        match stream {
            Ok(mut s) => {
                let mut reader = BufReader::new(&s);
                let mut raw_data = String::new();

                loop {
                    let mut line = String::new();
                    let n = reader.read_line(&mut line)?;

                    if n == 0 {
                        break;
                    }

                    raw_data.push_str(&line);

                    if raw_data.contains("\r\n\r\n") {
                        break;
                    }
                }

                println!("RAW DATA: {raw_data}");

                let headers_end = raw_data.find("\r\n\r\n").unwrap();
                let headers = &raw_data[..headers_end];
                let content_length = headers
                    .lines()
                    .find(|line| line.to_lowercase().starts_with("content-length"))
                    .and_then(|line| line.split(": ").nth(1))
                    .and_then(|v| v.trim().parse::<usize>().ok())
                    .unwrap_or(0);

                let mut body = raw_data[headers_end + 4..].to_string();
                while body.len() < content_length {
                    let mut buff = [0; 100];
                    let n = reader.read(&mut buff)?;

                    if n == 0 {
                        break;
                    }

                    body.push_str(std::str::from_utf8(&buff[..n]).unwrap());
                }
                let res = format!("HTTP/1.1 200 OK\r\nContent-Length:{:?}\r\n\r\n{body}", {
                    body.len()
                });
                println!("RES: {res}");
                let _ = s.write(res.as_bytes())?;
            }
            Err(e) => {
                panic!("{:?}", e);
            }
        }
    }

    Ok(())
}
