use crate::models::{QueuedItem, ReactionDelayMode};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct ProxyState {
    pub active_queue: Arc<RwLock<HashMap<String, Vec<QueuedItem>>>>,
    pub queue_mode_enabled: Arc<RwLock<bool>>,
    pub hardware_delay_ms: Arc<RwLock<u64>>,
    pub reaction_delay_mode: Arc<RwLock<ReactionDelayMode>>,
    pub self_user_id: Arc<RwLock<String>>,
    pub self_username: Arc<RwLock<String>>,
    pub last_processed_message_id: Arc<RwLock<HashMap<String, String>>>,
    pub last_sender_was_me: Arc<AtomicBool>,
    pub last_live_event_time: Arc<AtomicU64>,
    pub last_seen_message: Arc<RwLock<HashMap<String, serde_json::Value>>>,
    pub manual_lock: Arc<AtomicBool>,
}

impl Default for ProxyState {
    fn default() -> Self {
        Self::new()
    }
}

impl ProxyState {
    pub fn new() -> Self {
        Self {
            active_queue: Arc::new(RwLock::new(HashMap::new())),
            queue_mode_enabled: Arc::new(RwLock::new(true)),
            hardware_delay_ms: Arc::new(RwLock::new(45u64)),
            reaction_delay_mode: Arc::new(RwLock::new(ReactionDelayMode::Normal)),
            self_user_id: Arc::new(RwLock::new(String::new())),
            self_username: Arc::new(RwLock::new(String::new())),
            last_processed_message_id: Arc::new(RwLock::new(HashMap::new())),
            last_sender_was_me: Arc::new(AtomicBool::new(false)),
            last_live_event_time: Arc::new(AtomicU64::new(0)),
            last_seen_message: Arc::new(RwLock::new(HashMap::new())),
            manual_lock: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn set_manual_lock(&self, locked: bool) {
        self.manual_lock.store(locked, Ordering::SeqCst);
    }

    pub fn is_manually_locked(&self) -> bool {
        self.manual_lock.load(Ordering::SeqCst)
    }

    pub async fn is_queue_mode_enabled(&self) -> bool {
        *self.queue_mode_enabled.read().await
    }

    pub async fn set_queue_mode(&self, enabled: bool) {
        *self.queue_mode_enabled.write().await = enabled;
        if !enabled {
            self.clear_all_queues().await;
        }
    }

    pub async fn set_hardware_delay(&self, delay_ms: u64) {
        *self.hardware_delay_ms.write().await = delay_ms;
    }

    pub async fn set_reaction_delay_mode(&self, mode: ReactionDelayMode) {
        *self.reaction_delay_mode.write().await = mode;
    }

    pub async fn get_reaction_delay_mode(&self) -> ReactionDelayMode {
        *self.reaction_delay_mode.read().await
    }

    pub async fn clear_all_queues(&self) {
        let mut map = self.active_queue.write().await;
        for q in map.values_mut() {
            q.clear();
        }
    }

    pub async fn clear_queue(&self, channel_id: &str) -> Vec<QueuedItem> {
        let mut map = self.active_queue.write().await;
        if let Some(q) = map.get_mut(channel_id) {
            q.clear();
        }
        Vec::new()
    }

    pub async fn is_queue_empty(&self, cid: &str) -> bool {
        let map = self.active_queue.read().await;
        map.get(cid).map_or(true, |q| q.is_empty())
    }

    pub async fn enqueue_item(&self, channel_id: &str, mut item: QueuedItem) -> Vec<QueuedItem> {
        let mut map = self.active_queue.write().await;
        let q = map.entry(channel_id.to_string()).or_default();
        if q.is_empty() {
            item.was_empty = true;
        }
        q.push(item);
        q.clone()
    }

    pub async fn pop_next_item(&self, channel_id: &str) -> Option<(QueuedItem, Vec<QueuedItem>)> {
        let mut map = self.active_queue.write().await;
        if let Some(q) = map.get_mut(channel_id) {
            if !q.is_empty() {
                let item = q.remove(0);
                return Some((item, q.clone()));
            }
        }
        None
    }

    pub async fn set_self_info(&self, user_id: &str, username: &str) {
        if !user_id.is_empty() {
            *self.self_user_id.write().await = user_id.to_string();
        }
        if !username.is_empty() {
            *self.self_username.write().await = username.to_string();
        }
    }

    pub async fn is_self_author(&self, author_id: &str, author_uname: &str) -> bool {
        let my_id = self.self_user_id.read().await;
        let my_uname = self.self_username.read().await;
        (!my_id.is_empty() && author_id == *my_id) || (!my_uname.is_empty() && author_uname == *my_uname)
    }

    /// Atomically checks if message was already processed and marks it as processed.
    /// Returns `true` if it was NEW (successfully marked), or `false` if ALREADY processed.
    pub async fn try_mark_message_processed(&self, channel_id: &str, msg_id: &str) -> bool {
        if msg_id.is_empty() {
            return true;
        }
        let mut map = self.last_processed_message_id.write().await;
        if let Some(last_id) = map.get(channel_id) {
            if last_id == msg_id {
                return false;
            }
        }
        map.insert(channel_id.to_string(), msg_id.to_string());
        true
    }
}
