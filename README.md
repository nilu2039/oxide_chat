# Oxide Chat

A basic chat server built in Rust. It lets multiple users connect over TCP and talk to each other.

It uses:
- **Tokio** to handle many users at the same time.
- **Redis** to keep track of users who are banned for spamming or sending bad data.

## How to run

1. Make sure Redis is installed and running locally on port `6379` (the default port).
2. Start the server:
```bash
cargo run
```
3. The server will print a `Secret access token` in your terminal. Copy this token.
4. In a new terminal window, connect to the server:
```bash
telnet 127.0.0.1 8080
# or
nc 127.0.0.1 8080
```
5. When it asks, paste the `Secret access token` you copied earlier.
6. Now you can type messages and talk to anyone else who is connected!

## Rules
- You must enter the correct token to join.
- Do not send messages too fast. You will get a warning if you do.
- If you keep sending messages too fast, you will be banned and won't be able to connect again for a while.
