use serde::Serialize;

pub mod server;

#[derive(Serialize, Clone)]
pub struct Message {
    username: String,
    text: String,
}
