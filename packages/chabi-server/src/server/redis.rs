use chabi_core::commands::CommandHandler;
use chabi_core::resp::RespValue;
use chabi_core::Result;
use chabi_core::RwLock;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

// Simplify complex pubsub channel type
type PubSubChannels =
    std::sync::RwLock<HashMap<String, Vec<(usize, mpsc::Sender<(String, String)>)>>>;

static NEXT_CONN_ID: AtomicUsize = AtomicUsize::new(1);

pub struct RedisServer {
    command_registry: Arc<HashMap<String, Box<dyn CommandHandler + Send + Sync>>>,
    // PubSub channels shared with Publish/PubSub commands
    pubsub_channels: Arc<PubSubChannels>,
}

impl RedisServer {
    pub fn new() -> Self {
        let mut command_registry = HashMap::new();

        // Initialize stores (async RwLock-backed)
        let string_store: Arc<RwLock<HashMap<String, String>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let list_store: Arc<RwLock<HashMap<String, Vec<String>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let set_store: Arc<RwLock<HashMap<String, HashSet<String>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let hash_store: Arc<RwLock<HashMap<String, HashMap<String, String>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let expirations: Arc<RwLock<HashMap<String, Instant>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // channel -> Vec<(conn_id, Sender<(channel, message)>)>
        let pubsub_channels: Arc<PubSubChannels> = Arc::new(std::sync::RwLock::new(HashMap::new()));

        // Connection commands
        command_registry.insert(
            "PING".to_string(),
            Box::new(chabi_core::commands::connection::PingCommand::new())
                as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "ECHO".to_string(),
            Box::new(chabi_core::commands::connection::EchoCommand::new())
                as Box<dyn CommandHandler + Send + Sync>,
        );

        // Register string commands
        command_registry.insert(
            "SET".to_string(),
            Box::new(chabi_core::commands::string::SetCommand::new(Arc::clone(
                &string_store,
            ))) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "GET".to_string(),
            Box::new(chabi_core::commands::string::GetCommand::new(Arc::clone(
                &string_store,
            ))) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "DEL".to_string(),
            Box::new(chabi_core::commands::string::DelCommand::new(Arc::clone(
                &string_store,
            ))) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "EXISTS".to_string(),
            Box::new(chabi_core::commands::string::ExistsCommand::new(
                Arc::clone(&string_store),
            )) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "APPEND".to_string(),
            Box::new(chabi_core::commands::string::AppendCommand::new(
                Arc::clone(&string_store),
            )) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "STRLEN".to_string(),
            Box::new(chabi_core::commands::string::StrlenCommand::new(
                Arc::clone(&string_store),
            )) as Box<dyn CommandHandler + Send + Sync>,
        );

        // Register list commands
        command_registry.insert(
            "LPUSH".to_string(),
            Box::new(chabi_core::commands::list::LPushCommand::new(Arc::clone(
                &list_store,
            ))) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "RPUSH".to_string(),
            Box::new(chabi_core::commands::list::RPushCommand::new(Arc::clone(
                &list_store,
            ))) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "LPOP".to_string(),
            Box::new(chabi_core::commands::list::LPopCommand::new(Arc::clone(
                &list_store,
            ))) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "RPOP".to_string(),
            Box::new(chabi_core::commands::list::RPopCommand::new(Arc::clone(
                &list_store,
            ))) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "LRANGE".to_string(),
            Box::new(chabi_core::commands::list::LRangeCommand::new(Arc::clone(
                &list_store,
            ))) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "LLEN".to_string(),
            Box::new(chabi_core::commands::list::LLenCommand::new(Arc::clone(
                &list_store,
            ))) as Box<dyn CommandHandler + Send + Sync>,
        );

        // Register set commands
        command_registry.insert(
            "SADD".to_string(),
            Box::new(chabi_core::commands::set::SAddCommand::new(Arc::clone(
                &set_store,
            ))) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "SMEMBERS".to_string(),
            Box::new(chabi_core::commands::set::SMembersCommand::new(Arc::clone(
                &set_store,
            ))) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "SISMEMBER".to_string(),
            Box::new(chabi_core::commands::set::SIsMemberCommand::new(
                Arc::clone(&set_store),
            )) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "SCARD".to_string(),
            Box::new(chabi_core::commands::set::SCardCommand::new(Arc::clone(
                &set_store,
            ))) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "SREM".to_string(),
            Box::new(chabi_core::commands::set::SRemCommand::new(Arc::clone(
                &set_store,
            ))) as Box<dyn CommandHandler + Send + Sync>,
        );

        // Register hash commands
        command_registry.insert(
            "HSET".to_string(),
            Box::new(chabi_core::commands::hash::HSetCommand::new(Arc::clone(
                &hash_store,
            ))) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "HGET".to_string(),
            Box::new(chabi_core::commands::hash::HGetCommand::new(Arc::clone(
                &hash_store,
            ))) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "HGETALL".to_string(),
            Box::new(chabi_core::commands::hash::HGetAllCommand::new(Arc::clone(
                &hash_store,
            ))) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "HEXISTS".to_string(),
            Box::new(chabi_core::commands::hash::HExistsCommand::new(Arc::clone(
                &hash_store,
            ))) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "HDEL".to_string(),
            Box::new(chabi_core::commands::hash::HDelCommand::new(Arc::clone(
                &hash_store,
            ))) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "HLEN".to_string(),
            Box::new(chabi_core::commands::hash::HLenCommand::new(Arc::clone(
                &hash_store,
            ))) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "HKEYS".to_string(),
            Box::new(chabi_core::commands::hash::HKeysCommand::new(Arc::clone(
                &hash_store,
            ))) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "HVALS".to_string(),
            Box::new(chabi_core::commands::hash::HValsCommand::new(Arc::clone(
                &hash_store,
            ))) as Box<dyn CommandHandler + Send + Sync>,
        );

        // Key commands (use async RwLock-backed store same as string)
        command_registry.insert(
            "KEYS".to_string(),
            Box::new(chabi_core::commands::key::KeysCommand::new(Arc::clone(
                &string_store,
            ))) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "TTL".to_string(),
            Box::new(chabi_core::commands::key::TTLCommand::new(
                Arc::clone(&string_store),
                Arc::clone(&expirations),
            )) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "EXPIRE".to_string(),
            Box::new(chabi_core::commands::key::ExpireCommand::new(
                Arc::clone(&string_store),
                Arc::clone(&expirations),
            )) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "RENAME".to_string(),
            Box::new(chabi_core::commands::key::RenameCommand::new(
                Arc::clone(&string_store),
                Arc::clone(&expirations),
            )) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "TYPE".to_string(),
            Box::new(chabi_core::commands::key::TypeCommand::new(Arc::clone(
                &string_store,
            ))) as Box<dyn CommandHandler + Send + Sync>,
        );

        // Server commands (INFO, SAVE) - use async RwLock-backed stores
        command_registry.insert(
            "INFO".to_string(),
            Box::new(chabi_core::commands::server::InfoCommand::new(
                Arc::clone(&string_store),
                Arc::clone(&hash_store),
                Arc::clone(&list_store),
                Arc::clone(&set_store),
            )) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "SAVE".to_string(),
            Box::new(chabi_core::commands::server::SaveCommand::new(
                Arc::clone(&string_store),
                Arc::clone(&hash_store),
                Arc::clone(&list_store),
                Arc::clone(&set_store),
            )) as Box<dyn CommandHandler + Send + Sync>,
        );

        // Documentation commands
        command_registry.insert(
            "DOCS".to_string(),
            Box::new(chabi_core::commands::docs::DocsCommand::new())
                as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "COMMAND".to_string(),
            Box::new(chabi_core::commands::docs::CommandCommand::new())
                as Box<dyn CommandHandler + Send + Sync>,
        );

        // Register pubsub commands (Publish and PubSub; Subscribe/Unsubscribe will be handled at connection level)
        command_registry.insert(
            "PUBLISH".to_string(),
            Box::new(chabi_core::commands::pubsub::PublishCommand::new(
                Arc::clone(&pubsub_channels),
            )) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "SUBSCRIBE".to_string(),
            Box::new(chabi_core::commands::pubsub::SubscribeCommand::new(
                Arc::clone(&pubsub_channels),
            )) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "UNSUBSCRIBE".to_string(),
            Box::new(chabi_core::commands::pubsub::UnsubscribeCommand::new(
                Arc::clone(&pubsub_channels),
            )) as Box<dyn CommandHandler + Send + Sync>,
        );
        command_registry.insert(
            "PUBSUB".to_string(),
            Box::new(chabi_core::commands::pubsub::PubSubCommand::new(
                Arc::clone(&pubsub_channels),
            )) as Box<dyn CommandHandler + Send + Sync>,
        );

        RedisServer {
            command_registry: Arc::new(command_registry),
            pubsub_channels: Arc::clone(&pubsub_channels),
        }
    }

    async fn handle_connection(&self, stream: TcpStream) -> Result<()> {
        let (mut reader, mut writer) = stream.into_split();
        let mut buffer = [0; 4096];
        let registry = Arc::clone(&self.command_registry);
        let conn_id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);
        let (tx, mut rx) = mpsc::channel::<(String, String)>(100);
        let mut subscriptions: HashSet<String> = HashSet::new();

        loop {
            tokio::select! {
                read_res = reader.read(&mut buffer) => {
                    let n = read_res?;
                    if n == 0 {
                        break;
                    }
                    let request = RespValue::parse(&buffer[..n])?;
                    match request {
                        RespValue::Array(Some(array)) => {
                            if array.is_empty() { continue; }
                            let command_name = match &array[0] {
                                RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string().to_uppercase(),
                                _ => continue,
                            };

                            match command_name.as_str() {
                                "SUBSCRIBE" => {
                                    // Subscribe channels and acknowledge
                                    for arg in array.iter().skip(1) {
                                        let channel = match arg {
                                            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
                                            _ => continue,
                                        };
                                        if !subscriptions.contains(&channel) {
                                            {
                                                let mut map = self.pubsub_channels.write().unwrap();
                                                let vec = map.entry(channel.clone()).or_default();
                                                vec.push((conn_id, tx.clone()));
                                            }
                                            subscriptions.insert(channel.clone());
                                        }
                                        let resp = RespValue::Array(Some(vec![
                                            RespValue::BulkString(Some("subscribe".as_bytes().to_vec())),
                                            RespValue::BulkString(Some(channel.as_bytes().to_vec())),
                                            RespValue::Integer(subscriptions.len() as i64),
                                        ]));
                                        let serialized = resp.serialize();
                                        writer.write_all(&serialized).await?;
                                    }
                                }
                                "UNSUBSCRIBE" => {
                                    // If no args, unsubscribe from all
                                    let targets: Vec<String> = if array.len() == 1 {
                                        subscriptions.iter().cloned().collect()
                                    } else {
                                        array.iter().skip(1).filter_map(|arg| {
                                            match arg { RespValue::BulkString(Some(bytes)) => Some(String::from_utf8_lossy(bytes).to_string()), _ => None }
                                        }).collect()
                                    };
                                    for channel in targets {
                                        // remove from map in a limited scope to drop the write guard before await
                                        {
                                            let mut map = self.pubsub_channels.write().unwrap();
                                            if let Some(vec) = map.get_mut(&channel) {
                                                if let Some(pos) = vec.iter().position(|(id, _)| *id == conn_id) {
                                                    vec.swap_remove(pos);
                                                }
                                                if vec.is_empty() { map.remove(&channel); }
                                            }
                                        }
                                        // update subscription set
                                        subscriptions.remove(&channel);
                                        let resp = RespValue::Array(Some(vec![
                                            RespValue::BulkString(Some("unsubscribe".as_bytes().to_vec())),
                                            RespValue::BulkString(Some(channel.as_bytes().to_vec())),
                                            RespValue::Integer(subscriptions.len() as i64),
                                        ]));
                                        writer.write_all(&resp.serialize()).await?;
                                    }
                                }
                                _ => {
                                    // Forward to command handlers
                                    let args = array[1..].to_vec();
                                    match registry.get(&command_name) {
                                        Some(handler) => {
                                            let response = handler.execute(args).await?;
                                            writer.write_all(&response.serialize()).await?;
                                        },
                                        None => {
                                            let err = RespValue::Error(format!("ERR unknown command '{}'", command_name));
                                            writer.write_all(&err.serialize()).await?;
                                        }
                                    }
                                }
                            }
                        }
                        _ => {
                            let err = RespValue::Error("ERR invalid request format".to_string());
                            writer.write_all(&err.serialize()).await?;
                        }
                    }
                }
                msg = rx.recv() => {
                    match msg {
                        Some((channel, message)) => {
                            let resp = RespValue::Array(Some(vec![
                                RespValue::BulkString(Some("message".as_bytes().to_vec())),
                                RespValue::BulkString(Some(channel.as_bytes().to_vec())),
                                RespValue::BulkString(Some(message.as_bytes().to_vec())),
                            ]));
                            writer.write_all(&resp.serialize()).await?;
                        }
                        None => {
                            // channel closed
                            break;
                        }
                    }
                }
            }
        }

        // Cleanup on disconnect: remove this connection from all channels
        {
            let mut map = self.pubsub_channels.write().unwrap();
            for (_chan, vec) in map.iter_mut() {
                if let Some(pos) = vec.iter().position(|(id, _)| *id == conn_id) {
                    vec.swap_remove(pos);
                }
            }
            // Remove any empty channels
            let empty: Vec<String> = map
                .iter()
                .filter(|(_, v)| v.is_empty())
                .map(|(k, _)| k.clone())
                .collect();
            for ch in empty {
                map.remove(&ch);
            }
        }

        Ok(())
    }

    pub async fn run_server(&self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        println!("Redis server listening on {}", addr);

        loop {
            let (socket, addr) = listener.accept().await?;
            println!("New connection from {}", addr);

            let server = Arc::new(self.clone());
            tokio::spawn(async move {
                if let Err(e) = server.handle_connection(socket).await {
                    eprintln!("Error handling connection from {}: {}", addr, e);
                }
            });
        }
    }
}

impl Clone for RedisServer {
    fn clone(&self) -> Self {
        RedisServer {
            command_registry: Arc::clone(&self.command_registry),
            pubsub_channels: Arc::clone(&self.pubsub_channels),
        }
    }
}
