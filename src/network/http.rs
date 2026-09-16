// src/network/http.rs
use crate::models::{Channel, DiscordApiMessage, DiscordMessage, MessagePayload, MessageStatus, Server};
use chrono::Local;

pub struct DiscordHttpClient {
    pub client: reqwest::Client,
    pub token: String,
}

impl DiscordHttpClient {
    pub fn new(client: reqwest::Client, token: String) -> Self {
        Self { client, token }
    }

    /// Fetches all guilds (servers) the user belongs to
    pub async fn fetch_guilds(&self) -> Result<Vec<Server>, reqwest::Error> {
        let url = "https://discord.com/api/v10/users/@me/guilds";
        
        let res = self.client.get(url)
            .header("Authorization", &self.token) 
            .header("Content-Type", "application/json")
            .send()
            .await?;
            
        res.json::<Vec<Server>>().await
    }

    /// Fetches all channels belonging to a specific server guild ID
    pub async fn fetch_channels(&self, server_id: &str) -> Result<Vec<Channel>, reqwest::Error> {
        let url = format!("https://discord.com/api/v10/guilds/{}/channels", server_id);
        let res = self.client.get(&url)
            .header("Authorization", &self.token)
            .header("Content-Type", "application/json")
            .send()
            .await?;
            
        let mut channels = res.json::<Vec<Channel>>().await?;
        channels.retain(|c| !c.name.is_empty());
        Ok(channels)
    }

    /// Fetches past message history for a given channel ID
    pub async fn fetch_messages(&self, channel_id: &str, limit: u32) -> Result<Vec<DiscordMessage>, reqwest::Error> {
        let url = format!("https://discord.com/api/v10/channels/{}/messages?limit={}", channel_id, limit);
        let res = self.client.get(&url)
            .header("Authorization", &self.token)
            .header("Content-Type", "application/json")
            .send()
            .await?;

        if !res.status().is_success() {
            return Ok(Vec::new());
        }

        let api_msgs = res.json::<Vec<DiscordApiMessage>>().await?;
        let mut parsed_msgs = Vec::with_capacity(api_msgs.len());

        for msg in api_msgs.into_iter().rev() {
            let author = msg.author.global_name.unwrap_or(msg.author.username);
            
            let time_formatted = match chrono::DateTime::parse_from_rfc3339(&msg.timestamp) {
                Ok(dt) => dt.with_timezone(&Local).format("%H:%M:%S%.3f").to_string(),
                Err(_) => {
                    if msg.timestamp.len() >= 19 {
                        msg.timestamp[11..19].to_string()
                    } else {
                        msg.timestamp
                    }
                }
            };

            let nonce_str = match msg.nonce {
                Some(serde_json::Value::String(s)) => s,
                Some(serde_json::Value::Number(n)) => n.to_string(),
                _ => msg.id.clone(),
            };

            parsed_msgs.push(DiscordMessage {
                nonce: nonce_str,
                author,
                content: msg.content,
                timestamp: time_formatted,
                status: MessageStatus::Delivered,
            });
        }

        Ok(parsed_msgs)
    }

    /// ⚡ OPTIMIZED FOR MAX SPEED: Fires typing indicator down persistent TCP pool
    pub async fn send_typing(&self, channel_id: &str) -> Result<(), reqwest::Error> {
        let url = format!("https://discord.com/api/v10/channels/{}/typing", channel_id);
        let _ = self.client.post(&url)
            .header("Authorization", &self.token)
            .header("Content-Length", "0")
            .send()
            .await?;
        Ok(())
    }

    /// ⚡ OPTIMIZED FOR MAX SPEED: Sends message json frame cleanly with zero tree allocations
    pub async fn send_message(&self, channel_id: &str, text: &str, nonce: &str) -> Result<reqwest::Response, reqwest::Error> {
        let url = format!("https://discord.com/api/v10/channels/{}/messages", channel_id);
        let payload = MessagePayload {
            content: text.to_string(),
            nonce: nonce.to_string(),
        };

        self.client.post(&url)
            .header("Authorization", &self.token)
            .header("Content-Type", "application/json")
            .header("Origin", "https://discord.com")
            .header("Referer", format!("https://discord.com/channels/@me/{}", channel_id))
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36")
            .json(&payload)
            .send()
            .await
    }
}
