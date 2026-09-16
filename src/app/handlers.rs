// src/app/handlers.rs
use crate::app::queue_logic::process_queue_input;
use crate::app::state::ActiveModal;
use crate::models::{AppEvent, DiscordMessage, MessageStatus, ReactionDelayMode};
use chrono::Local;
use crossterm::event::{Event, KeyCode, KeyModifiers, MouseEventKind};
use std::time::Instant;
use tokio::sync::mpsc::Sender;

impl crate::app::state::AppState {
    pub async fn handle_event(
        &mut self,
        event: AppEvent,
        tx: &Sender<AppEvent>,
    ) -> bool {
        match event {
            AppEvent::Tick => {}
            AppEvent::ToggleMode => {
                let is_proxy = self.token.starts_with("ws://") || self.token.starts_with("wss://");
                if is_proxy {
                    if let Ok(tok) = std::fs::read_to_string(".token_cache") {
                        if !tok.trim().is_empty() {
                            self.token = tok.trim().to_string();
                            self.queue_mode = false;
                            self.queue.clear();
                            self.update_preview_typed_text();
                        }
                    }
                } else if let Ok(prx) = std::fs::read_to_string(".proxy_cache") {
                    if !prx.trim().is_empty() {
                        self.token = prx.trim().to_string();
                        self.queue_mode = true;
                    }
                }
            }
            AppEvent::ToggleQueueMode => {
                let is_proxy = self.token.starts_with("ws://") || self.token.starts_with("wss://");
                if is_proxy {
                    self.queue_mode = !self.queue_mode;
                    if !self.queue_mode {
                        self.queue.clear();
                        let _ = tx.send(AppEvent::ClearQueue).await;
                    }
                    self.update_preview_typed_text();
                } else {
                    self.queue_mode = false;
                    self.queue.clear();
                    self.update_preview_typed_text();
                }
            }
            AppEvent::ClearQueue => {
                self.queue.clear();
                self.update_preview_typed_text();
            }
            AppEvent::TriggerTopQueue => {
                if !self.queue.is_empty() {
                    self.queue.remove(0);
                }
                self.update_preview_typed_text();
            }
            AppEvent::ToggleManualLock => {}
            AppEvent::UpdateQueueState(q) => {
                self.queue = q;
                self.update_preview_typed_text();
            }
            AppEvent::UpdateReactionDelayMode(mode) => {
                self.reaction_delay_mode = mode;
            }
            AppEvent::ToggleTimestamp => {
                self.show_timestamp = !self.show_timestamp;
            }
            AppEvent::ToggleLatency => {
                self.show_latency = !self.show_latency;
            }
            AppEvent::ScrollChat(delta) => {
                if delta < 0 {
                    let amount = (-delta) as usize;
                    let current = self.list_state.selected().unwrap_or(0);
                    let new_idx = current.saturating_sub(amount);
                    self.list_state.select(Some(new_idx));
                } else if delta > 0 {
                    let amount = delta as usize;
                    let current = self.list_state.selected().unwrap_or(0);
                    let max_idx = self.messages.len().saturating_sub(1);
                    let new_idx = (current + amount).min(max_idx);
                    self.list_state.select(Some(new_idx));
                }
            }
            AppEvent::UpdateClockOffset(offset_ms) => {
                self.clock_offset_ms = Some(offset_ms);
            }
            AppEvent::UpdateGatewayRtt { rtt_ms, offset_ms } => {
                self.gateway_rtt_ms = Some(rtt_ms);
                if self.clock_offset_ms.is_none() {
                    self.clock_offset_ms = Some(offset_ms);
                }
            }
            AppEvent::SetSelfUsername(username) => {
                if !username.is_empty() {
                    self.self_username = username;
                }
            }
            AppEvent::LoadChannelHistory(msgs) => {
                self.messages = msgs;
                if !self.messages.is_empty() {
                    self.list_state.select(Some(self.messages.len() - 1));
                }
            }
            AppEvent::SwitchChannel(new_channel_id) => {
                if !new_channel_id.is_empty() {
                    self.set_target_channel_id(new_channel_id.clone());
                    let _ = std::fs::write(".channel_cache", &new_channel_id);
                    self.messages.clear();
                    self.queue.clear();
                    self.update_preview_typed_text();
                    let _ = tx.send(AppEvent::ClearQueue).await;
                    let _ = tx.send(AppEvent::FetchChannelHistory(new_channel_id)).await;
                }
            }
            AppEvent::IncomingMessage(m) => {
                if m.nonce.starts_with("err-") {
                    self.messages.push(m.clone());
                } else if !self
                    .messages
                    .iter()
                    .any(|x| x.nonce == m.nonce && !m.nonce.is_empty())
                {
                    self.messages.push(m.clone());
                }

                if !self.messages.is_empty() {
                    self.list_state.select(Some(self.messages.len() - 1));
                }
            }
            AppEvent::MessageSent { nonce, timestamp } => {
                let elapsed_ms = self.outbound_timers.remove(&nonce).map(|start| start.elapsed().as_millis());

                if let Some(m) = self.messages.iter_mut().find(|x| x.nonce == nonce) {
                    m.status = MessageStatus::Delivered;
                    
                    if let Some(rtt) = elapsed_ms {
                        m.timestamp = format!(
                            "{} | {}ms",
                            Local::now().format("%H:%M:%S%.3f"),
                            rtt
                        );
                    } else if !timestamp.is_empty() {
                        m.timestamp = timestamp;
                    }
                    
                    self.failed_nonces.retain(|x| x != &nonce);
                }
            }
            AppEvent::MessageFailed { nonce } => {
                self.outbound_timers.remove(&nonce);
                if let Some(m) = self.messages.iter_mut().find(|x| x.nonce == nonce) {
                    m.status = MessageStatus::Failed;
                }
                if !self.failed_nonces.contains(&nonce) {
                    self.failed_nonces.push(nonce);
                }
            }
            AppEvent::GatewayClosed => {
                self.queue.clear();
                self.update_preview_typed_text();
                let _ = tx.send(AppEvent::ClearQueue).await;

                self.messages.push(DiscordMessage {
                    nonce: "err-close".into(),
                    author: "System".into(),
                    content: "⚠️ Internet / WebSocket disconnected.".into(),
                    timestamp: Local::now().format("%H:%M:%S%.3f").to_string(),
                    status: MessageStatus::Failed,
                });
            }
            AppEvent::Terminal(Event::Mouse(m)) => match m.kind {
                MouseEventKind::ScrollUp => {
                    let current = self.list_state.selected().unwrap_or(0);
                    let new_idx = current.saturating_sub(3);
                    self.list_state.select(Some(new_idx));
                }
                MouseEventKind::ScrollDown => {
                    let current = self.list_state.selected().unwrap_or(0);
                    let max_idx = self.messages.len().saturating_sub(1);
                    let new_idx = (current + 3).min(max_idx);
                    self.list_state.select(Some(new_idx));
                }
                _ => {}
            },
            AppEvent::Terminal(Event::Key(k))
                if k.kind == crossterm::event::KeyEventKind::Press =>
            {
                if k.code == KeyCode::Char('c') && k.modifiers.contains(KeyModifiers::CONTROL) && !self.input_text.is_empty() {
                    self.input_text.clear();
                    return false;
                }

                if self.active_modal != ActiveModal::None {
                    match self.active_modal {
                        ActiveModal::LogoutPrompt => match k.code {
                            KeyCode::Char('y') | KeyCode::Char('Y') => {
                                let _ = std::fs::remove_file(".token_cache");
                                let _ = std::fs::remove_file(".channel_cache");
                                return true;
                            }
                            KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
                                self.active_modal = ActiveModal::None;
                            }
                            _ => {}
                        },
                        ActiveModal::SwitchChannelPrompt => match k.code {
                            KeyCode::Esc => {
                                self.active_modal = ActiveModal::None;
                                self.modal_input.clear();
                            }
                            KeyCode::Backspace => {
                                self.modal_input.pop();
                            }
                            KeyCode::Enter => {
                                let input = self.modal_input.trim().to_string();
                                let clean_input = input.trim_end_matches('/');
                                let target_id = clean_input.split('/').last().unwrap_or("").split('?').next().unwrap_or("").to_string();
                                if !target_id.is_empty() && target_id.chars().all(|c| c.is_numeric()) {
                                    let _ = tx.send(AppEvent::SwitchChannel(target_id)).await;
                                }
                                self.modal_input.clear();
                                self.active_modal = ActiveModal::None;
                            }
                            KeyCode::Char(c) => {
                                self.modal_input.push(c);
                            }
                            _ => {}
                        },
                        ActiveModal::None => {}
                    }
                    return false;
                }

                if (k.code == KeyCode::Char('q') || k.code == KeyCode::Char('Q')) && k.modifiers.contains(KeyModifiers::CONTROL) {
                    let _ = tx.send(AppEvent::ToggleQueueMode).await;
                    return false;
                }
                if k.code == KeyCode::Char('c') && k.modifiers.contains(KeyModifiers::CONTROL) && self.input_text.is_empty() {
                    self.queue.clear();
                    self.input_text.clear();
                    self.failed_nonces.clear();
                    self.update_preview_typed_text();
                    let _ = tx.send(AppEvent::ClearQueue).await;
                    return false;
                }
                if k.code == KeyCode::Delete {
                    self.queue.clear();
                    self.input_text.clear();
                    self.failed_nonces.clear();
                    self.update_preview_typed_text();
                    let _ = tx.send(AppEvent::ClearQueue).await;
                    return false;
                }
                if k.code == KeyCode::Char('x') && k.modifiers.contains(KeyModifiers::CONTROL) {
                    self.active_modal = ActiveModal::LogoutPrompt;
                    return false;
                }
                if k.code == KeyCode::Char('g') && k.modifiers.contains(KeyModifiers::CONTROL) {
                    self.active_modal = ActiveModal::SwitchChannelPrompt;
                    self.modal_input.clear();
                    return false;
                }

                let now = Instant::now();
                if let Some(prev) = self.last_keystroke_time {
                    let diff_ms = now.duration_since(prev).as_millis() as u64;
                    if diff_ms > 10 && diff_ms < 1500 {
                        self.hardware_delay_ms = diff_ms;
                        let _ = tx.send(AppEvent::UpdateHardwareDelay(self.hardware_delay_ms)).await;
                    }
                }
                self.last_keystroke_time = Some(now);

                match k.code {
                    KeyCode::F(1) => {
                        let _ = tx.send(AppEvent::TriggerTopQueue).await;
                        let _ = tx.send(AppEvent::ToggleManualLock).await;
                    }
                    KeyCode::F(2) => {
                        let _ = tx.send(AppEvent::ToggleQueueMode).await;
                    }
                    KeyCode::F(3) => {
                        self.queue.clear();
                        self.input_text.clear();
                        self.failed_nonces.clear();
                        self.update_preview_typed_text();
                        let _ = tx.send(AppEvent::ClearQueue).await;
                    }
                    KeyCode::F(4) => {
                        let _ = tx.send(AppEvent::ToggleManualLock).await;
                    }
                    KeyCode::F(6) => {
                        self.show_timestamp = !self.show_timestamp;
                    }
                    KeyCode::F(7) => {
                        self.show_latency = !self.show_latency;
                    }
                    KeyCode::F(10) => {
                        self.reaction_delay_mode = match self.reaction_delay_mode {
                            ReactionDelayMode::Normal => ReactionDelayMode::Fast,
                            ReactionDelayMode::Fast => ReactionDelayMode::Instant,
                            ReactionDelayMode::Instant => ReactionDelayMode::Normal,
                        };
                        let _ = tx.send(AppEvent::UpdateReactionDelayMode(self.reaction_delay_mode)).await;
                    }
                    KeyCode::BackTab | KeyCode::F(11) | KeyCode::F(12) | KeyCode::Insert => {
                        let _ = tx.send(AppEvent::ToggleMode).await;
                    }
                    KeyCode::F(5) => {
                        self.active_modal = ActiveModal::SwitchChannelPrompt;
                        self.modal_input.clear();
                    }
                    KeyCode::PageUp => {
                        let current = self.list_state.selected().unwrap_or(0);
                        let new_idx = current.saturating_sub(10);
                        self.list_state.select(Some(new_idx));
                    }
                    KeyCode::PageDown => {
                        let current = self.list_state.selected().unwrap_or(0);
                        let max_idx = self.messages.len().saturating_sub(1);
                        let new_idx = (current + 10).min(max_idx);
                        self.list_state.select(Some(new_idx));
                    }
                    KeyCode::Home => {
                        if !self.messages.is_empty() {
                            self.list_state.select(Some(0));
                        }
                    }
                    KeyCode::End => {
                        if !self.messages.is_empty() {
                            self.list_state.select(Some(self.messages.len() - 1));
                        }
                    }
                    KeyCode::Up if self.input_text.is_empty() || k.modifiers.contains(KeyModifiers::SHIFT) || k.modifiers.contains(KeyModifiers::ALT) => {
                        let current = self.list_state.selected().unwrap_or(0);
                        let new_idx = current.saturating_sub(1);
                        self.list_state.select(Some(new_idx));
                    }
                    KeyCode::Down if self.input_text.is_empty() || k.modifiers.contains(KeyModifiers::SHIFT) || k.modifiers.contains(KeyModifiers::ALT) => {
                        let current = self.list_state.selected().unwrap_or(0);
                        let max_idx = self.messages.len().saturating_sub(1);
                        let new_idx = (current + 1).min(max_idx);
                        self.list_state.select(Some(new_idx));
                    }
                    KeyCode::Tab if k.modifiers.contains(KeyModifiers::SHIFT) => {
                        let _ = tx.send(AppEvent::ToggleMode).await;
                    }
                    KeyCode::Tab => {
                        if let Some(last_failed_nonce) = self.failed_nonces.last().cloned() {
                            let (text, found) = if let Some(m) = self.messages.iter_mut().find(|x| x.nonce == last_failed_nonce) {
                                m.status = MessageStatus::Sending;
                                m.timestamp = format!("{} | ...", Local::now().format("%H:%M:%S%.3f"));
                                (m.content.clone(), true)
                            } else {
                                (String::new(), false)
                            };

                            if found {
                                self.outbound_timers.insert(last_failed_nonce.clone(), Instant::now());
                                let _ = tx.send(AppEvent::HttpSendChat {
                                    nonce: last_failed_nonce,
                                    text,
                                }).await;
                            }
                        }
                    }
                    KeyCode::Char(c) => {
                        self.input_text.push(c);
                        
                        let trigger = match self.last_typing_sent {
                            Some(last_sent) => last_sent.elapsed() >= std::time::Duration::from_secs(7),
                            None => true,
                        };

                        if trigger && !self.target_channel_id.is_empty() {
                            self.last_typing_sent = Some(std::time::Instant::now());
                            let _ = tx.send(AppEvent::HttpTriggerTyping).await;
                        }
                    }
                    KeyCode::Backspace => {
                        self.input_text.pop();
                    }
                    KeyCode::Enter if !self.input_text.is_empty() => {
                        self.last_typing_sent = None;
                        let text = std::mem::take(&mut self.input_text);
                        process_queue_input(self, text, tx).await;
                    }
                    _ => {}
                }
            }
            _ => {}
        }
        false
    }
}
