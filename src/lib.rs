use serde::{Deserialize, Serialize};

pub mod server;

#[derive(Serialize, Deserialize, Clone)]
pub struct Message {
    username: String,
    text: String,
}

#[derive(Serialize, Deserialize)]
pub struct ResponseMsg {
    data: Option<Message>,
    info_msg: Option<String>,
    err_msg: Option<String>,
}
