# Oxide Chat

A chat application built in Rust. It features an asynchronous server and a native graphical client, allowing multiple users to connect and chat in real-time.

It uses:
- **Tokio** on the server for handling multiple concurrent TCP connections asynchronously.
- **eframe (egui)** for a native, fast, and cross-platform GUI client.
- **Redis** to keep track of user states, including rate limiting and bans for spamming.

## How to run

1. Make sure Redis is installed and running locally on port `6379` (the default port).
2. Start the server:
```bash
cargo run --bin server
```
3. The server will print a `Secret access token` in your terminal. Copy this token.
4. In a new terminal window, start the chat client:
```bash
cargo run --bin client
```
5. A GUI window will open. When prompted by the server, enter the `Secret access token` you copied earlier to authenticate.
6. Now you can type messages in the client and chat with anyone else who is connected!

## Rules
- You must provide the correct server token to join.
- Do not send messages too fast. You will receive warnings if you do.
- If you exceed the rate limit multiple times, you will be temporarily banned.
