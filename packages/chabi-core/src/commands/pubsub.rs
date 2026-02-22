use crate::commands::CommandHandler;
use crate::resp::RespValue;
use crate::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc::Sender;

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
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'publish' command".to_string(),
            ));
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
            channels.get(&channel).cloned().unwrap_or_default()
        };

        // Send messages and track failed senders
        let mut receivers = 0;
        let mut failed_indices = Vec::new();

        for (idx, (_conn_id, sender)) in senders.iter().enumerate() {
            if sender
                .send((channel.clone(), message.clone()))
                .await
                .is_ok()
            {
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
pub struct SubscribeCommand {
    // Kept for compatibility; server handles subscription lifecycle
    _channels: Arc<RwLock<ChannelMap>>,
}

impl SubscribeCommand {
    pub fn new(channels: Arc<RwLock<ChannelMap>>) -> Self {
        SubscribeCommand {
            _channels: channels,
        }
    }
}

#[async_trait]
impl CommandHandler for SubscribeCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'subscribe' command".to_string(),
            ));
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
pub struct UnsubscribeCommand {
    // Kept for compatibility; server handles unsubscription lifecycle
    _channels: Arc<RwLock<ChannelMap>>,
}

impl UnsubscribeCommand {
    pub fn new(channels: Arc<RwLock<ChannelMap>>) -> Self {
        UnsubscribeCommand {
            _channels: channels,
        }
    }
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
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'pubsub' command".to_string(),
            ));
        }

        let subcommand = match &args[0] {
            RespValue::BulkString(Some(bytes)) => {
                String::from_utf8_lossy(bytes).to_string().to_lowercase()
            }
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
                        RespValue::BulkString(Some(bytes)) => {
                            String::from_utf8_lossy(bytes).to_string()
                        }
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
            _ => Ok(RespValue::Error(
                "ERR Unknown PUBSUB subcommand".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    fn channels() -> Arc<RwLock<ChannelMap>> {
        Arc::new(RwLock::new(HashMap::new()))
    }

    fn bulk(s: &str) -> RespValue {
        RespValue::BulkString(Some(s.as_bytes().to_vec()))
    }

    #[tokio::test]
    async fn test_publish_no_subscribers() {
        let ch = channels();
        let cmd = PublishCommand::new(ch);
        let r = cmd
            .execute(vec![bulk("chan1"), bulk("hello")])
            .await
            .unwrap();
        assert_eq!(r, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_publish_with_subscriber() {
        let ch = channels();
        let (tx, mut rx) = mpsc::channel(10);
        {
            let mut map = ch.write().unwrap();
            map.entry("chan1".to_string()).or_default().push((1, tx));
        }
        let cmd = PublishCommand::new(ch);
        let r = cmd
            .execute(vec![bulk("chan1"), bulk("hello")])
            .await
            .unwrap();
        assert_eq!(r, RespValue::Integer(1));
        let msg = rx.recv().await.unwrap();
        assert_eq!(msg, ("chan1".to_string(), "hello".to_string()));
    }

    #[tokio::test]
    async fn test_publish_dead_sender_cleanup() {
        let ch = channels();
        let (tx, rx) = mpsc::channel(10);
        {
            let mut map = ch.write().unwrap();
            map.entry("chan1".to_string()).or_default().push((1, tx));
        }
        // Drop receiver so sender is dead
        drop(rx);
        let cmd = PublishCommand::new(ch.clone());
        let r = cmd
            .execute(vec![bulk("chan1"), bulk("hello")])
            .await
            .unwrap();
        assert_eq!(r, RespValue::Integer(0));
        // Channel should be cleaned up
        let map = ch.read().unwrap();
        assert!(!map.contains_key("chan1"));
    }

    #[tokio::test]
    async fn test_publish_wrong_args() {
        let ch = channels();
        let cmd = PublishCommand::new(ch);
        let r = cmd.execute(vec![bulk("chan1")]).await.unwrap();
        assert!(matches!(r, RespValue::Error(_)));
        let r = cmd.execute(vec![]).await.unwrap();
        assert!(matches!(r, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_publish_invalid_channel() {
        let ch = channels();
        let cmd = PublishCommand::new(ch);
        let r = cmd
            .execute(vec![RespValue::Integer(1), bulk("msg")])
            .await
            .unwrap();
        assert!(matches!(r, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_publish_invalid_message() {
        let ch = channels();
        let cmd = PublishCommand::new(ch);
        let r = cmd
            .execute(vec![bulk("chan1"), RespValue::Integer(1)])
            .await
            .unwrap();
        assert!(matches!(r, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_subscribe_command() {
        let ch = channels();
        let cmd = SubscribeCommand::new(ch);
        let r = cmd
            .execute(vec![bulk("chan1"), bulk("chan2")])
            .await
            .unwrap();
        match r {
            RespValue::Array(Some(arr)) => {
                assert_eq!(arr.len(), 2); // Two subscribe acknowledgments
            }
            _ => panic!("Expected Array"),
        }
    }

    #[tokio::test]
    async fn test_subscribe_no_args() {
        let ch = channels();
        let cmd = SubscribeCommand::new(ch);
        let r = cmd.execute(vec![]).await.unwrap();
        assert!(matches!(r, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_unsubscribe_specific() {
        let ch = channels();
        let cmd = UnsubscribeCommand::new(ch);
        let r = cmd.execute(vec![bulk("chan1")]).await.unwrap();
        match r {
            RespValue::Array(Some(arr)) => {
                assert_eq!(arr.len(), 1);
            }
            _ => panic!("Expected Array"),
        }
    }

    #[tokio::test]
    async fn test_unsubscribe_all() {
        let ch = channels();
        let cmd = UnsubscribeCommand::new(ch);
        let r = cmd.execute(vec![]).await.unwrap();
        match r {
            RespValue::Array(Some(arr)) => {
                assert_eq!(arr.len(), 1); // placeholder unsubscribe
            }
            _ => panic!("Expected Array"),
        }
    }

    #[tokio::test]
    async fn test_pubsub_channels() {
        let ch = channels();
        let (tx, _rx) = mpsc::channel(10);
        {
            let mut map = ch.write().unwrap();
            map.entry("chan1".to_string()).or_default().push((1, tx));
        }
        let cmd = PubSubCommand::new(ch);
        let r = cmd.execute(vec![bulk("CHANNELS")]).await.unwrap();
        match r {
            RespValue::Array(Some(arr)) => {
                assert_eq!(arr.len(), 1);
            }
            _ => panic!("Expected Array"),
        }
    }

    #[tokio::test]
    async fn test_pubsub_numsub() {
        let ch = channels();
        let (tx, _rx) = mpsc::channel(10);
        {
            let mut map = ch.write().unwrap();
            map.entry("chan1".to_string()).or_default().push((1, tx));
        }
        let cmd = PubSubCommand::new(ch);
        let r = cmd
            .execute(vec![bulk("NUMSUB"), bulk("chan1"), bulk("chan2")])
            .await
            .unwrap();
        match r {
            RespValue::Array(Some(arr)) => {
                // chan1 + count + chan2 + count = 4 elements
                assert_eq!(arr.len(), 4);
                assert_eq!(arr[1], RespValue::Integer(1)); // chan1 has 1 subscriber
                assert_eq!(arr[3], RespValue::Integer(0)); // chan2 has 0
            }
            _ => panic!("Expected Array"),
        }
    }

    #[tokio::test]
    async fn test_pubsub_numpat() {
        let ch = channels();
        let cmd = PubSubCommand::new(ch);
        let r = cmd.execute(vec![bulk("NUMPAT")]).await.unwrap();
        assert_eq!(r, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_pubsub_unknown_subcommand() {
        let ch = channels();
        let cmd = PubSubCommand::new(ch);
        let r = cmd.execute(vec![bulk("FOOBAR")]).await.unwrap();
        assert!(matches!(r, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_pubsub_no_args() {
        let ch = channels();
        let cmd = PubSubCommand::new(ch);
        let r = cmd.execute(vec![]).await.unwrap();
        assert!(matches!(r, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_pubsub_invalid_subcommand_type() {
        let ch = channels();
        let cmd = PubSubCommand::new(ch);
        let r = cmd.execute(vec![RespValue::Integer(1)]).await.unwrap();
        assert!(matches!(r, RespValue::Error(_)));
    }
}
