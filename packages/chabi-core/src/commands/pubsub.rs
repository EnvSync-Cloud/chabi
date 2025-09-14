use std::collections::HashMap;
use crate::resp::RespValue;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc::Sender;
use crate::commands::CommandHandler;
use crate::Result;
use async_trait::async_trait;

// Updated: store (conn_id, Sender<(channel, message)>) for each subscriber
pub type ChannelMap = HashMap<String, Vec<(usize, Sender<(String, String)>)>>;

#[derive(Clone)]
pub struct PublishCommand {
    channels: Arc<RwLock<ChannelMap>>,
}

impl PublishCommand {
    pub fn new(channels: Arc<RwLock<ChannelMap>>) -> Self {
        PublishCommand { channels }
    }
}

#[async_trait]
impl CommandHandler for PublishCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error("ERR wrong number of arguments for 'publish' command".to_string()));
        }

        let channel = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid channel name".to_string())),
        };

        let message = match &args[1] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid message".to_string())),
        };

        // First, get all senders for the channel
        let senders = {
            let channels = self.channels.read().unwrap();
            channels.get(&channel)
                .cloned()
                .unwrap_or_default()
        };

        // Send messages and track failed senders
        let mut receivers = 0;
        let mut failed_indices = Vec::new();

        for (idx, (_conn_id, sender)) in senders.iter().enumerate() {
            if sender.send((channel.clone(), message.clone())).await.is_ok() {
                receivers += 1;
            } else {
                failed_indices.push(idx);
            }
        }

        // Clean up dead senders if any were found
        if !failed_indices.is_empty() {
            let mut channels = self.channels.write().unwrap();
            if let Some(channel_senders) = channels.get_mut(&channel) {
                // Remove from highest index to lowest to maintain correct indices
                for &idx in failed_indices.iter().rev() {
                    if idx < channel_senders.len() {
                        channel_senders.swap_remove(idx);
                    }
                }
                // Remove the channel if no senders remain
                if channel_senders.is_empty() {
                    channels.remove(&channel);
                }
            }
        }

        Ok(RespValue::Integer(receivers))
    }
}

#[derive(Clone)]
pub struct SubscribeCommand;

impl SubscribeCommand {
    pub fn new() -> Self { Self }
}

#[async_trait]
impl CommandHandler for SubscribeCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::Error("ERR wrong number of arguments for 'subscribe' command".to_string()));
        }

        // Note: Actual subscribe flow (attaching connection sender and streaming messages)
        // is handled by the server connection loop. Here we only acknowledge channels
        // to keep basic client expectations, but do not register anything at this layer.
        let mut responses = Vec::new();
        for arg in args {
            let channel = match arg {
                RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(&bytes).to_string(),
                _ => continue,
            };
            let subscribe_response = RespValue::Array(Some(vec![
                RespValue::BulkString(Some("subscribe".as_bytes().to_vec())),
                RespValue::BulkString(Some(channel.as_bytes().to_vec())),
                RespValue::Integer(1), // placeholder
            ]));
            responses.push(subscribe_response);
        }
        Ok(RespValue::Array(Some(responses)))
    }
}

#[derive(Clone)]
pub struct UnsubscribeCommand;

impl UnsubscribeCommand {
    pub fn new() -> Self { Self }
}

#[async_trait]
impl CommandHandler for UnsubscribeCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        let mut responses = Vec::new();
        for arg in args {
            let channel = match arg {
                RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(&bytes).to_string(),
                _ => continue,
            };
            let unsubscribe_response = RespValue::Array(Some(vec![
                RespValue::BulkString(Some("unsubscribe".as_bytes().to_vec())),
                RespValue::BulkString(Some(channel.as_bytes().to_vec())),
                RespValue::Integer(0),
            ]));
            responses.push(unsubscribe_response);
        }
        if responses.is_empty() {
            // Unsubscribe from all (placeholder)
            responses.push(RespValue::Array(Some(vec![
                RespValue::BulkString(Some("unsubscribe".as_bytes().to_vec())),
                RespValue::BulkString(Some("".as_bytes().to_vec())),
                RespValue::Integer(0),
            ])));
        }
        Ok(RespValue::Array(Some(responses)))
    }
}

#[derive(Clone)]
pub struct PubSubCommand {
    channels: Arc<RwLock<ChannelMap>>,
}

impl PubSubCommand {
    pub fn new(channels: Arc<RwLock<ChannelMap>>) -> Self {
        PubSubCommand { channels }
    }
}

#[async_trait]
impl CommandHandler for PubSubCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::Error("ERR wrong number of arguments for 'pubsub' command".to_string()));
        }

        let subcommand = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string().to_lowercase(),
            _ => return Ok(RespValue::Error("ERR invalid subcommand".to_string())),
        };

        match subcommand.as_str() {
            "channels" => {
                let channels = self.channels.read().unwrap();
                let channel_list: Vec<RespValue> = channels
                    .keys()
                    .map(|k| RespValue::BulkString(Some(k.as_bytes().to_vec())))
                    .collect();
                Ok(RespValue::Array(Some(channel_list)))
            }
            "numsub" => {
                let channels = self.channels.read().unwrap();
                let mut response = Vec::new();

                for arg in args.iter().skip(1) {
                    let channel = match arg {
                        RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
                        _ => continue,
                    };

                    response.push(RespValue::BulkString(Some(channel.as_bytes().to_vec())));
                    response.push(RespValue::Integer(
                        channels.get(&channel).map_or(0, |subs| subs.len() as i64),
                    ));
                }

                Ok(RespValue::Array(Some(response)))
            }
            "numpat" => Ok(RespValue::Integer(0)), // Pattern subscriptions not supported
            _ => Ok(RespValue::Error("ERR Unknown PUBSUB subcommand".to_string())),
        }
    }
}