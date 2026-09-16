use crate::models::ProxyResponse;
use crate::proxy::state::ProxyState;
use crate::proxy::trigger_rules::evaluate_and_trigger_queue;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};
use tokio::time::{interval, Duration};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;

#[derive(Deserialize)]
struct GatewayPayload {
    op: u8,
    #[serde(default)]
    d: serde_json::Value,
    #[serde(default)]
    t: Option<String>,
}

pub type SharedGwWriter = Arc<
    Mutex<
        Option<
            futures_util::stream::SplitSink<
                tokio_tungstenite::WebSocketStream<
                    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
                >,
                Message,
            >,
        >,
    >,
>;

pub async fn run_discord_gateway(
    discord_token: String,
    state: ProxyState,
    gw_broadcast_tx: broadcast::Sender<ProxyResponse>,
    http_client: Arc<reqwest::Client>,
    gw_writer_ref: SharedGwWriter,
) {
    let gw_url = "wss://gateway.discord.gg/?v=10&encoding=json";
    loop {
        if let Ok((ws, _)) = connect_async(gw_url).await {
            let (write, mut read) = ws.split();
            *gw_writer_ref.lock().await = Some(write);

            if let Some(Ok(Message::Text(t))) = read.next().await {
                if let Ok(p) = serde_json::from_str::<GatewayPayload>(&t) {
                    if p.op == 10 {
                        let heartbeat_interval = p.d["heartbeat_interval"].as_u64().unwrap_or(41250);
                        let identify = serde_json::json!({
                            "op": 2,
                            "d": {
                                "token": discord_token,
                                "capabilities": 16381,
                                "properties": {
                                    "$os": "Windows",
                                    "$browser": "Chrome",
                                    "$device": "",
                                    "system_locale": "en-US",
                                    "browser_user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
                                    "browser_version": "128.0.0.0",
                                    "os_version": "10",
                                    "referrer": "https://discord.com/",
                                    "referring_domain": "discord.com"
                                },
                                "presence": {
                                    "status": "online",
                                    "since": 0,
                                    "activities": [],
                                    "afk": false
                                },
                                "compress": false
                            }
                        });

                        if let Some(w) = gw_writer_ref.lock().await.as_mut() {
                            let _ = w.send(Message::Text(identify.to_string())).await;
                        }

                        // Heartbeat loop
                        let writer_hb = Arc::clone(&gw_writer_ref);
                        tokio::spawn(async move {
                            let mut hb_timer = interval(Duration::from_millis(heartbeat_interval));
                            loop {
                                hb_timer.tick().await;
                                let hb_payload = serde_json::json!({ "op": 1, "d": null }).to_string();
                                let mut lock = writer_hb.lock().await;
                                if let Some(w) = lock.as_mut() {
                                    if w.send(Message::Text(hb_payload)).await.is_err() {
                                        break;
                                    }
                                } else {
                                    break;
                                }
                            }
                        });

                        while let Some(Ok(msg_text)) = read.next().await {
                            if let Message::Text(txt) = msg_text {
                                if let Ok(pay) = serde_json::from_str::<GatewayPayload>(&txt) {
                                    if pay.op == 0 {
                                        let event_type = pay.t.unwrap_or_default();
                                        let data = pay.d.clone();

                                        if event_type == "READY" {
                                            let uid = data["user"]["id"].as_str().unwrap_or("");
                                            let uname = data["user"]["username"].as_str().unwrap_or("");
                                            state.set_self_info(uid, uname).await;
                                        }

                                        // Stealth Queue Evaluation via WebSocket MESSAGE_CREATE
                                        if event_type == "MESSAGE_CREATE" {
                                            let cid = data["channel_id"].as_str().unwrap_or("");
                                            if !cid.is_empty() {
                                                state
                                                    .last_seen_message
                                                    .write()
                                                    .await
                                                    .insert(cid.to_string(), data.clone());
                                            }

                                            evaluate_and_trigger_queue(
                                                Some(&data),
                                                cid,
                                                &state,
                                                discord_token.clone(),
                                                Arc::clone(&http_client),
                                                gw_broadcast_tx.clone(),
                                                false,
                                            )
                                            .await;
                                        }

                                        let response = ProxyResponse::GatewayEvent { event_type, data };
                                        let _ = gw_broadcast_tx.send(response);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}
