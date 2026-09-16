// src/models.rs
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum ReactionDelayMode {
    #[default]
    Normal,  // 200ms - 300ms
    Fast,    // 0ms - 200ms
    Instant, // 0ms
}

#[derive(Debug, Clone, Deserialize)]
pub struct Channel {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Server {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub channels: Vec<Channel>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageStatus { Sending, Delivered, Failed }

#[derive(Debug, Clone)]
pub struct DiscordMessage {
    pub nonce: String,
    pub author: String,
    pub content: String,
    pub timestamp: String,
    pub status: MessageStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueuedItem {
    pub content: String,
    pub number: i64,
    #[serde(default)]
    pub was_empty: bool,
}

#[derive(Debug, Clone)]
pub enum AppEvent {
    Tick,
    IncomingMessage(DiscordMessage),
    MessageSent { nonce: String, timestamp: String },
    MessageFailed { nonce: String },
    Terminal(crossterm::event::Event),
    GatewayClosed,
    SetSelfUsername(String),
    HttpTriggerTyping,
    HttpSendChat { nonce: String, text: String },
    FetchChannelHistory(String),
    LoadChannelHistory(Vec<DiscordMessage>),
    SwitchChannel(String),
    ToggleMode,
    UpdateGatewayRtt { rtt_ms: u64, offset_ms: i64 },
    UpdateClockOffset(i64),
    ToggleTimestamp,
    ToggleLatency,
    ScrollChat(i32),
    ToggleQueueMode,
    ClearQueue,
    TriggerTopQueue,
    ToggleManualLock,
    UpdateQueueState(Vec<QueuedItem>),
    EnqueueNumberItem(QueuedItem),
    UpdateHardwareDelay(u64),
    UpdateReactionDelayMode(ReactionDelayMode),
}

#[derive(Serialize)]
pub struct MessagePayload {
    pub content: String,
    pub nonce: String,
}

#[derive(Deserialize)]
pub struct GatewayPayload {
    pub op: u8,
    #[serde(default)]
    pub d: serde_json::Value,
    #[serde(default)]
    pub t: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DiscordApiMessage {
    pub id: String,
    #[serde(default)]
    pub nonce: Option<serde_json::Value>,
    #[serde(default)]
    pub content: String,
    pub author: DiscordApiAuthor,
    #[serde(default)]
    pub timestamp: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DiscordApiAuthor {
    pub id: Option<String>,
    pub username: String,
    #[serde(default)]
    pub global_name: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type")]
pub enum ProxyAction {
    SendMessage { channel_id: String, content: String, nonce: String },
    SendTyping { channel_id: String },
    FetchHistory { channel_id: String, limit: u32 },
    SubscribeChannel { channel_id: String },
    Ping,
    SetQueueMode { enabled: bool },
    ClearQueue { channel_id: String },
    TriggerTopQueue { channel_id: String },
    EnqueueNumber { channel_id: String, item: QueuedItem },
    UpdateHardwareDelay { delay_ms: u64 },
    UpdateReactionDelayMode { mode: ReactionDelayMode },
    UnlockQueue,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum ProxyResponse {
    Ack { nonce: String },
    MessageResult { nonce: String, success: bool, error: Option<String> },
    ChannelHistory { channel_id: String, messages: serde_json::Value },
    GatewayEvent { event_type: String, data: serde_json::Value },
    QueueSync { queue: Vec<QueuedItem> },
    QueuedMessageFailed { nonce: String, content: String, error: Option<String> },
    Pong,
    QueueLockedStatus { locked: bool },
}
