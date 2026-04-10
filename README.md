# Rust TCP Chat Server

A simple TCP chat server built in Rust. It accepts connections and broadcasts messages from any user to everyone else connected.

## How to run

1. Start the server:
```bash
cargo run
```

2. Connect using telnet or netcat (in a different terminal window):
```bash
telnet 127.0.0.1 8080
# or
nc 127.0.0.1 8080
```

3. Open a few more terminals, connect them using step 2, and type a message. Anything you type and send will be visible to everyone else connected.
