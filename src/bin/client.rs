use oxide_chat::ResponseMsg;

use eframe::egui::{self, Color32, Context, Key, KeyboardShortcut, Modifiers, RichText, TextStyle};
use rand::rng;
use rand::seq::IndexedRandom;
use std::vec;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Receiver, Sender, channel};

async fn handle_tcp_connection(
    ctx: Context,
    tx_in: Sender<ResponseMsg>,
    mut rx_out: Receiver<String>,
) {
    let stream = TcpStream::connect("127.0.0.1:8080").await.unwrap();
    let (read_stream, mut write_stream) = stream.into_split();
    let mut reader = BufReader::new(read_stream);
    let mut buf = Vec::new();

    tokio::spawn(async move {
        while let Some(msg) = rx_out.recv().await {
            let msg_bytes = format!(
                "content-length: {msg_length}\r\n\r\n{msg}",
                msg_length = &msg.len()
            );

            if let Err(e) = write_stream.write_all(msg_bytes.as_bytes()).await {
                eprintln!("ERROR: {e}")
            };
        }
    });

    loop {
        buf.clear();

        match reader.read_until(b'\n', &mut buf).await {
            Ok(n) => {
                if n == 0 {
                    return;
                }
                let line = &buf[..n];
                let json_str = std::str::from_utf8(&line).unwrap();

                let parsed_json_res: ResponseMsg = serde_json::from_str(json_str).unwrap();

                if let Err(e) = tx_in.send(parsed_json_res).await {
                    eprintln!("ERROR: {e}");
                };

                ctx.request_repaint();
            }
            Err(_err) => {
                return;
            }
        }
    }
}

#[tokio::main]
async fn main() -> eframe::Result {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([1280.0, 720.0]),
        ..Default::default()
    };

    let (tx_in, rx_in) = channel(100);
    let (tx_out, rx_out) = channel(100);

    eframe::run_native(
        "Oxide chat client",
        options,
        Box::new(|cc| {
            let ctx = cc.egui_ctx.clone();
            tokio::spawn(handle_tcp_connection(ctx, tx_in, rx_out));
            Ok(Box::new(App {
                messages: vec![],
                rx_in,
                tx_out,
                out_message: String::new(),
            }))
        }),
    )
}

struct Message {
    text: String,
    username: Option<String>,
    color: Color32,
}

struct App {
    messages: Vec<Message>,
    rx_in: Receiver<ResponseMsg>,
    tx_out: Sender<String>,
    out_message: String,
}

const COLORS: [Color32; 3] = [
    egui::Color32::RED,
    egui::Color32::GREEN,
    egui::Color32::ORANGE,
];

impl eframe::App for App {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let mut rng = rng();

        while let Ok(msg) = self.rx_in.try_recv() {
            let mut text = String::new();
            let mut username = None;
            match msg {
                ResponseMsg {
                    data: Some(msg), ..
                } => {
                    text = msg.text.trim_end_matches('\n').to_string();
                    username = Option::from(msg.username);
                }
                ResponseMsg {
                    info_msg: Some(info),
                    ..
                } => {
                    text = info.trim_end_matches('\n').to_string();
                }
                ResponseMsg {
                    err_msg: Some(err), ..
                } => {
                    text = err.trim_end_matches('\n').to_string();
                }
                _ => {}
            };
            self.messages.push(Message {
                text,
                color: *COLORS.choose(&mut rng).unwrap(),
                username,
            });
        }

        let input_id = ui.make_persistent_id("chat_input");

        egui::Panel::bottom("input_label").show_inside(ui, |ui| {
            let response = ui.add_sized(
                ui.available_size(),
                egui::TextEdit::multiline(&mut self.out_message)
                    .desired_rows(2)
                    .return_key(KeyboardShortcut {
                        modifiers: Modifiers {
                            shift: true,
                            ..Default::default()
                        },
                        logical_key: Key::Enter,
                    })
                    .font(TextStyle::Heading)
                    .id(input_id),
            );
            if response.has_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                if !self.out_message.is_empty() {
                    if let Err(e) = self.tx_out.try_send(self.out_message.clone()) {
                        eprintln!("ERROR: {e}");
                    };
                    self.out_message.clear();
                }
            }
        });

        egui::CentralPanel::default().show_inside(ui, |ui| {
            ui.heading(RichText::new("Oxide Chat").color(egui::Color32::RED));
            ui.add_space(10.0);
            egui::ScrollArea::vertical()
                .stick_to_bottom(true)
                .show(ui, |ui| {
                    ui.set_width(ui.available_width());
                    for msg in &self.messages {
                        ui.horizontal_wrapped(|ui| {
                            if let Some(username) = &msg.username {
                                ui.label(
                                    RichText::new(format!("{username}: "))
                                        .color(msg.color)
                                        .text_style(TextStyle::Heading),
                                );
                                ui.add_space(2.0);
                            };

                            ui.label(
                                RichText::new(&msg.text)
                                    .color(Color32::WHITE)
                                    .text_style(TextStyle::Monospace),
                            );
                        });
                        ui.add_space(5.0);
                    }
                });
        });
    }
}
