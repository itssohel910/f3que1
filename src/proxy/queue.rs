use crate::models::{ProxyResponse, QueuedItem, ReactionDelayMode};
use crate::proxy::state::ProxyState;
use crate::proxy::utils::generate_snowflake_nonce;
use rand::Rng;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::time::Duration;

pub async fn execute_queued_reaction(
    item: QueuedItem,
    channel_id: String,
    discord_token: String,
    http_client: Arc<reqwest::Client>,
    gw_broadcast_tx: broadcast::Sender<ProxyResponse>,
    remaining_queue: Vec<QueuedItem>,
    state: ProxyState,
    skip_delay: bool,
) {
    // Notify connected client of the updated queue state immediately
    let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync {
        queue: remaining_queue,
    });

    let delay_mode = state.get_reaction_delay_mode().await;

    tokio::spawn(async move {
        if !skip_delay {
            let delay_ms = match delay_mode {
                ReactionDelayMode::Normal => rand::thread_rng().gen_range(200..=300),
                ReactionDelayMode::Fast => rand::thread_rng().gen_range(0..=200),
                ReactionDelayMode::Instant => 0,
            };

            if delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
        }

        let msg_url = format!("https://discord.com/api/v10/channels/{}/messages", channel_id);
        let nonce = generate_snowflake_nonce();
        let payload = serde_json::json!({
            "content": item.content,
            "nonce": nonce
        });

        let res = http_client
            .post(&msg_url)
            .header("Authorization", &discord_token)
            .header("Content-Type", "application/json")
            .header("Origin", "https://discord.com")
            .header("Referer", format!("https://discord.com/channels/@me/{}", channel_id))
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36")
            .json(&payload)
            .send()
            .await;

        let (is_success, err_msg) = match res {
            Ok(resp) if resp.status().is_success() => (true, None),
            Ok(resp) => {
                let status_code = resp.status();
                let err_text = resp.text().await.unwrap_or_else(|_| "Rate Limited / Rejected".to_string());
                (false, Some(format!("HTTP {}: {}", status_code, err_text)))
            }
            Err(e) => (false, Some(e.to_string())),
        };

        if !is_success {
            let _ = gw_broadcast_tx.send(ProxyResponse::QueuedMessageFailed {
                nonce,
                content: item.content,
                error: err_msg,
            });
        }
    });
}
