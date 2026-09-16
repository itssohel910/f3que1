use crate::models::{ProxyAction, ProxyResponse};
use crate::proxy::queue::execute_queued_reaction;
use crate::proxy::state::ProxyState;
use crate::proxy::trigger_rules::evaluate_and_trigger_queue;
use futures_util::{stream::SplitSink, SinkExt};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, Mutex};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::WebSocketStream;

type WsWriter = SplitSink<WebSocketStream<TcpStream>, Message>;

pub async fn send_resp(writer: &Arc<Mutex<WsWriter>>, resp: &ProxyResponse) -> bool {
    if let Ok(json_str) = serde_json::to_string(resp) {
        let mut w = writer.lock().await;
        w.send(Message::Text(json_str)).await.is_ok()
    } else {
        false
    }
}

pub async fn handle_action(
    action: ProxyAction,
    state: &ProxyState,
    write_arc: &Arc<Mutex<WsWriter>>,
    discord_token: &str,
    http_client: &Arc<reqwest::Client>,
    subscribed_cid: &Arc<tokio::sync::RwLock<String>>,
    gw_broadcast_tx: &broadcast::Sender<ProxyResponse>,
) {
    match action {
        ProxyAction::UnlockQueue => {
            state.set_manual_lock(false);
            send_resp(write_arc, &ProxyResponse::QueueLockedStatus { locked: false }).await;
        }
        ProxyAction::SetQueueMode { enabled } => {
            state.set_queue_mode(enabled).await;
            if !enabled {
                send_resp(write_arc, &ProxyResponse::QueueSync { queue: Vec::new() }).await;
            }
        }
        ProxyAction::UpdateHardwareDelay { delay_ms } => {
            state.set_hardware_delay(delay_ms).await;
        }
        ProxyAction::UpdateReactionDelayMode { mode } => {
            state.set_reaction_delay_mode(mode).await;
        }
        ProxyAction::ClearQueue { channel_id } => {
            let cleared = state.clear_queue(&channel_id).await;
            send_resp(write_arc, &ProxyResponse::QueueSync { queue: cleared }).await;
        }
        ProxyAction::TriggerTopQueue { channel_id } => {
            state.last_sender_was_me.store(true, Ordering::SeqCst);
            if let Some((item, remaining_q)) = state.pop_next_item(&channel_id).await {
                execute_queued_reaction(
                    item,
                    channel_id,
                    discord_token.to_string(),
                    Arc::clone(http_client),
                    gw_broadcast_tx.clone(),
                    remaining_q,
                    state.clone(),
                    true,
                )
                .await;
            } else {
                state.last_sender_was_me.store(false, Ordering::SeqCst);
            }
        }
        ProxyAction::EnqueueNumber { channel_id, item } => {
            if state.is_manually_locked() {
                return;
            }

            let queue_was_empty = state.is_queue_empty(&channel_id).await;
            let updated_q = state.enqueue_item(&channel_id, item).await;
            send_resp(write_arc, &ProxyResponse::QueueSync { queue: updated_q.clone() }).await;

            if queue_was_empty {
                let cached_msg = {
                    let map = state.last_seen_message.read().await;
                    map.get(&channel_id).cloned()
                };

                if let Some(msg_data) = cached_msg {
                    let author_id = msg_data["author"]["id"].as_str().unwrap_or("");
                    let is_known_bot_id = author_id == "510016054391734273" || author_id == "639599059036012605";
                    let is_bot = is_known_bot_id
                        || msg_data["author"]["bot"].as_bool().unwrap_or(false)
                        || msg_data["webhook_id"].is_string()
                        || msg_data["type"].as_u64().map_or(false, |t| t != 0 && t != 19);

                    if is_bot {
                        state.last_sender_was_me.store(true, Ordering::SeqCst);
                    } else {
                        evaluate_and_trigger_queue(
                            Some(&msg_data),
                            &channel_id,
                            state,
                            discord_token.to_string(),
                            Arc::clone(http_client),
                            gw_broadcast_tx.clone(),
                            true,
                        )
                        .await;
                    }
                }
            }
        }
        ProxyAction::SubscribeChannel { channel_id } => {
            *subscribed_cid.write().await = channel_id;
        }
        ProxyAction::Ping => {
            send_resp(write_arc, &ProxyResponse::Pong).await;
        }
        ProxyAction::SendMessage { channel_id, content, nonce } => {
            let w_arc_ack = Arc::clone(write_arc);
            let ack_nonce = nonce.clone();
            tokio::spawn(async move {
                send_resp(&w_arc_ack, &ProxyResponse::Ack { nonce: ack_nonce }).await;
            });

            let url = format!("https://discord.com/api/v10/channels/{}/messages", channel_id);
            let payload = serde_json::json!({ "content": content, "nonce": nonce });
            let client_ref = Arc::clone(http_client);
            let token_ref = discord_token.to_string();
            let w_arc_res = Arc::clone(write_arc);

            tokio::spawn(async move {
                let res = client_ref.post(&url)
                    .header("Authorization", &token_ref)
                    .header("Content-Type", "application/json")
                    .json(&payload)
                    .send()
                    .await;

                let response = match res {
                    Ok(resp) if resp.status().is_success() => ProxyResponse::MessageResult {
                        nonce,
                        success: true,
                        error: None,
                    },
                    Ok(resp) => {
                        let err = resp.text().await.unwrap_or_else(|_| "Rejected".to_string());
                        ProxyResponse::MessageResult {
                            nonce,
                            success: false,
                            error: Some(err),
                        }
                    }
                    Err(e) => ProxyResponse::MessageResult {
                        nonce,
                        success: false,
                        error: Some(e.to_string()),
                    },
                };

                send_resp(&w_arc_res, &response).await;
            });
        }
        ProxyAction::SendTyping { channel_id } => {
            let url = format!("https://discord.com/api/v10/channels/{}/typing", channel_id);
            let client_ref = Arc::clone(http_client);
            let token_ref = discord_token.to_string();
            tokio::spawn(async move {
                let _ = client_ref.post(&url)
                    .header("Authorization", &token_ref)
                    .header("Content-Length", "0")
                    .send()
                    .await;
            });
        }
        ProxyAction::FetchHistory { channel_id, limit } => {
            let url = format!("https://discord.com/api/v10/channels/{}/messages?limit={}", channel_id, limit);
            let client_ref = Arc::clone(http_client);
            let token_ref = discord_token.to_string();
            let w_arc_hist = Arc::clone(write_arc);

            tokio::spawn(async move {
                let res = client_ref.get(&url)
                    .header("Authorization", &token_ref)
                    .send()
                    .await;

                if let Ok(resp) = res {
                    if let Ok(json_data) = resp.json::<serde_json::Value>().await {
                        let resp_struct = ProxyResponse::ChannelHistory {
                            channel_id,
                            messages: json_data,
                        };
                        send_resp(&w_arc_hist, &resp_struct).await;
                    }
                }
            });
        }
    }
}
