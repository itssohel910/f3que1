use crate::models::{ProxyResponse, ReactionDelayMode};
use crate::proxy::queue::execute_queued_reaction;
use crate::proxy::state::ProxyState;
use rand::Rng;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

pub async fn evaluate_and_trigger_queue(
    message_data: Option<&serde_json::Value>,
    channel_id: &str,
    state: &ProxyState,
    discord_token: String,
    http_client: Arc<reqwest::Client>,
    gw_broadcast_tx: broadcast::Sender<ProxyResponse>,
    force_skip_delay: bool,
) {
    if state.is_manually_locked() {
        return;
    }

    if channel_id.is_empty() {
        return;
    }

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    if message_data.is_some() {
        state.last_live_event_time.store(now_ms, Ordering::SeqCst);
        state.last_sender_was_me.store(false, Ordering::SeqCst);
    }

    // Rule 1: Queue Mode must be active
    if !state.is_queue_mode_enabled().await {
        return;
    }

    // Check queue existence, zero check, and non-empty status
    {
        let map = state.active_queue.read().await;
        if let Some(q) = map.get(channel_id) {
            if let Some(top_item) = q.first() {
                if top_item.number == 0 || top_item.content.trim() == "0" {
                    drop(map);
                    let cleared = state.clear_queue(channel_id).await;
                    let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync { queue: cleared });
                    return;
                }
            } else {
                return;
            }

            if q.is_empty() {
                return;
            }
        } else {
            return;
        }
    }

    if let Some(data) = message_data {
        let msg_id = data["id"].as_str().unwrap_or("");
        if !msg_id.is_empty() {
            if !state.try_mark_message_processed(channel_id, msg_id).await {
                return;
            }
        }

        let author_id = data["author"]["id"].as_str().unwrap_or("");
        let author_uname = data["author"]["username"].as_str().unwrap_or("");

        if state.is_self_author(author_id, author_uname).await {
            return;
        }
    }

    let delay_mode = state.get_reaction_delay_mode().await;
    if !force_skip_delay {
        let delay_ms = match delay_mode {
            ReactionDelayMode::Normal => rand::thread_rng().gen_range(200..=300),
            ReactionDelayMode::Fast => rand::thread_rng().gen_range(0..=200),
            ReactionDelayMode::Instant => 0,
        };
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
    }

    if let Some((top_item, _)) = state.pop_next_item(channel_id).await {
        execute_queued_reaction(
            top_item,
            channel_id.to_string(),
            discord_token,
            http_client,
            gw_broadcast_tx.clone(),
            Vec::new(),
            state.clone(),
            true,
        )
        .await;
    }

    let cleared = state.clear_queue(channel_id).await;
    state.set_manual_lock(true);
    let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync { queue: cleared });
    let _ = gw_broadcast_tx.send(ProxyResponse::QueueLockedStatus { locked: true });
}
