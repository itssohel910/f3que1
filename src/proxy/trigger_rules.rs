use crate::models::{ProxyResponse, ReactionDelayMode};
use crate::proxy::queue::execute_queued_reaction;
use crate::proxy::state::ProxyState;
use rand::Rng;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

/// Rules governing when a stealth queue reaction should trigger:
/// 1. Manual Lock check: Drops immediately if deadbolt `manual_lock` is active.
/// 2. Queue Mode must be active.
/// 3. Zero check: If top item number is 0 (or content "0"), clear entire queue and do not send.
/// 4. Emptiness check: Requires queue to be non-empty to process.
/// 5. Interruption handling: On external message or bot message, wait out configured reaction delay,
///    pop/execute exactly 1 item, wipe remaining queue, send QueueSync, and set manual_lock = true.
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
    } else {
        let last_event = state.last_live_event_time.load(Ordering::SeqCst);
        if now_ms.saturating_sub(last_event) < 2000 {
            return;
        }
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

    // Read cached/provided message payload
    let fetched_last_msg;
    let data = match message_data {
        Some(d) => d,
        None => {
            let map = state.last_seen_message.read().await;
            if let Some(cached) = map.get(channel_id) {
                fetched_last_msg = cached.clone();
                &fetched_last_msg
            } else {
                return;
            }
        }
    };

    let msg_id = data["id"].as_str().unwrap_or("");

    // ATOMIC DEDUPLICATION: Ensures payload never conflicts or double-triggers
    if !msg_id.is_empty() {
        if !state.try_mark_message_processed(channel_id, msg_id).await {
            return;
        }
    }

    let author_id = data["author"]["id"].as_str().unwrap_or("");
    let author_uname = data["author"]["username"].as_str().unwrap_or("");

    // Enhanced Bot Detection
    let is_known_bot_id = author_id == "510016054391734273" || author_id == "639599059036012605";
    let is_bot = is_known_bot_id
        || data["author"]["bot"].as_bool().unwrap_or(false)
        || data["webhook_id"].is_string()
        || data["type"].as_u64().map_or(false, |t| t != 0 && t != 19);

    // Cannot trigger on own message
    if state.is_self_author(author_id, author_uname).await {
        return;
    }

    // Helper closure to calculate and sleep reaction delay jitter
    let delay_mode = state.get_reaction_delay_mode().await;
    let apply_jitter_sleep = || async move {
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
    };

    // Interruption Rule: Handle out-of-order, double human messages or bot messages
    if is_bot || !is_bot {
        apply_jitter_sleep().await;

        if let Some((top_item, _)) = state.pop_next_item(channel_id).await {
            // Execute single top item without secondary internal delay since we already slept
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

        // Wipe out remaining items, set manual lock, and sync client
        let cleared = state.clear_queue(channel_id).await;
        state.set_manual_lock(true);
        let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync { queue: cleared });
        let _ = gw_broadcast_tx.send(ProxyResponse::QueueLockedStatus { locked: true });
    }
}
