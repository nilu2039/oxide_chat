use serde::{Deserialize, Serialize};

pub mod server;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Message {
    pub username: String,
    pub text: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ResponseMsg {
    pub data: Option<Message>,
    pub info_msg: Option<String>,
    pub err_msg: Option<String>,
}
