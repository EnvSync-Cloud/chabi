use chabi_core::commands::CommandHandler;
use chabi_core::resp::{RespParser, RespValue};
use chabi_core::Result;
use chabi_core::RwLock;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_util::codec::Framed;

// Simplify complex pubsub channel type
type PubSubChannels =
    std::sync::RwLock<HashMap<String, Vec<(usize, mpsc::Sender<(String, String)>)>>>;

static NEXT_CONN_ID: AtomicUsize = AtomicUsize::new(1);

pub struct RedisServer {
    command_registry: Arc<HashMap<String, Box<dyn CommandHandler + Send + Sync>>>,
    // PubSub channels shared with Publish/PubSub commands
    pubsub_channels: Arc<PubSubChannels>,
    // Backing stores for snapshotting
    string_store: Arc<RwLock<HashMap<String, String>>>,
    list_store: Arc<RwLock<HashMap<String, Vec<String>>>>,
    set_store: Arc<RwLock<HashMap<String, HashSet<String>>>>,
    hash_store: Arc<RwLock<HashMap<String, HashMap<String, String>>>>,
    expirations: Arc<RwLock<HashMap<String, Instant>>>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Snapshot {
    strings: HashMap<String, String>,
    lists: HashMap<String, Vec<String>>,
    sets: HashMap<String, HashSet<String>>,
    hashes: HashMap<String, HashMap<String, String>>,
    expirations_epoch_secs: HashMap<String, u64>,
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
        // SCARD and SREM are not implemented in chabi-core yet; registrations removed.

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
            string_store,
            list_store,
            set_store,
            hash_store,
            expirations,
        }
    }

    pub fn start_snapshot_task(&self, path: String, interval: Duration) {
        let strings = Arc::clone(&self.string_store);
        let lists = Arc::clone(&self.list_store);
        let sets = Arc::clone(&self.set_store);
        let hashes = Arc::clone(&self.hash_store);
        let expirations = Arc::clone(&self.expirations);
        tokio::spawn(async move {
            loop {
                sleep(interval).await;
                let snapshot = {
                    let strings_guard = strings.read().await;
                    let lists_guard = lists.read().await;
                    let sets_guard = sets.read().await;
                    let hashes_guard = hashes.read().await;
                    let expirations_guard = expirations.read().await;
                    let now_instant = Instant::now();
                    let now_system = SystemTime::now();
                    let mut exps: HashMap<String, u64> = HashMap::new();
                    for (k, inst) in expirations_guard.iter() {
                        let delta = inst.saturating_duration_since(now_instant);
                        let ts = now_system
                            .checked_add(delta)
                            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                            .map(|d| d.as_secs())
                            .unwrap_or_else(|| {
                                UNIX_EPOCH.elapsed().map(|d| d.as_secs()).unwrap_or(0)
                            });
                        exps.insert(k.clone(), ts);
                    }
                    Snapshot {
                        strings: strings_guard.clone(),
                        lists: lists_guard.clone(),
                        sets: sets_guard.clone(),
                        hashes: hashes_guard.clone(),
                        expirations_epoch_secs: exps,
                    }
                };

                match serde_json::to_vec_pretty(&snapshot) {
                    Ok(bytes) => {
                        let tmp_path = format!("{}.tmp", path);
                        if let Err(e) = tokio::fs::write(&tmp_path, &bytes).await {
                            tracing::error!("snapshot write error: {}", e);
                        } else {
                            #[cfg(target_family = "windows")]
                            let _ = tokio::fs::remove_file(&path).await;

                            match tokio::fs::rename(&tmp_path, &path).await {
                                Ok(_) => {
                                    tracing::debug!(
                                        "snapshot saved to {} ({} bytes)",
                                        path,
                                        bytes.len()
                                    );
                                }
                                Err(e) => {
                                    // Fallback: copy then remove tmp (best-effort on non-atomic platforms)
                                    match tokio::fs::copy(&tmp_path, &path).await {
                                        Ok(_) => {
                                            let _ = tokio::fs::remove_file(&tmp_path).await;
                                            tracing::debug!(
                                                "snapshot saved to {} ({} bytes) [fallback copy]",
                                                path,
                                                bytes.len()
                                            );
                                        }
                                        Err(copy_err) => {
                                            tracing::error!(
                                                "snapshot rename/copy error: {} (rename_err: {})",
                                                copy_err,
                                                e
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => tracing::error!("snapshot serialize error: {}", e),
                }
            }
        });
    }

    // Add: build a snapshot of current in-memory data
    pub async fn build_snapshot(&self) -> Snapshot {
        let strings_guard = self.string_store.read().await;
        let lists_guard = self.list_store.read().await;
        let sets_guard = self.set_store.read().await;
        let hashes_guard = self.hash_store.read().await;
        let expirations_guard = self.expirations.read().await;
        let now_instant = Instant::now();
        let now_system = SystemTime::now();
        let mut exps: HashMap<String, u64> = HashMap::new();
        for (k, inst) in expirations_guard.iter() {
            let delta = inst.saturating_duration_since(now_instant);
            let ts = now_system
                .checked_add(delta)
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or_else(|| UNIX_EPOCH.elapsed().map(|d| d.as_secs()).unwrap_or(0));
            exps.insert(k.clone(), ts);
        }
        Snapshot {
            strings: strings_guard.clone(),
            lists: lists_guard.clone(),
            sets: sets_guard.clone(),
            hashes: hashes_guard.clone(),
            expirations_epoch_secs: exps,
        }
    }

    // Add: load snapshot from a path into in-memory data
    pub async fn load_snapshot_from_path(&self, path: &str) -> Result<()> {
        if tokio::fs::metadata(path).await.is_err() {
            tracing::info!("snapshot file not found: {}", path);
            return Ok(());
        }
        let data = tokio::fs::read(path).await?;
        let snapshot: Snapshot = match serde_json::from_slice(&data) {
            Ok(s) => s,
            Err(e) => {
                let err: Box<dyn std::error::Error + Send + Sync> = Box::new(e);
                return Err(err);
            }
        };

        let strings_len = snapshot.strings.len();
        let lists_len = snapshot.lists.len();
        let sets_len = snapshot.sets.len();
        let hashes_len = snapshot.hashes.len();
        let expirations_len = snapshot.expirations_epoch_secs.len();

        {
            let mut s = self.string_store.write().await;
            *s = snapshot.strings;
        }
        {
            let mut l = self.list_store.write().await;
            *l = snapshot.lists;
        }
        {
            let mut s = self.set_store.write().await;
            *s = snapshot.sets;
        }
        {
            let mut h = self.hash_store.write().await;
            *h = snapshot.hashes;
        }
        {
            let mut exp = self.expirations.write().await;
            exp.clear();
            let now_system = SystemTime::now();
            let now_instant = Instant::now();
            for (k, ts) in snapshot.expirations_epoch_secs.into_iter() {
                let target_time = UNIX_EPOCH + Duration::from_secs(ts);
                if let Ok(delta) = target_time.duration_since(now_system) {
                    if !delta.is_zero() {
                        exp.insert(k, now_instant + delta);
                    }
                }
            }
        }

        tracing::info!(
            "loaded snapshot from {} (strings={}, lists={}, sets={}, hashes={}, expirations={})",
            path,
            strings_len,
            lists_len,
            sets_len,
            hashes_len,
            expirations_len
        );

        Ok(())
    }

    async fn handle_connection(&self, stream: TcpStream) -> Result<()> {
        // Low-latency optimization
        let _ = stream.set_nodelay(true);

        let registry = Arc::clone(&self.command_registry);
        let conn_id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);
        let (tx, mut rx) = mpsc::channel::<(String, String)>(100);
        let mut subscriptions: HashSet<String> = HashSet::new();

        let framed = Framed::new(stream, RespParser::new());
        let (mut sink, mut stream) = framed.split();

        loop {
            tokio::select! {
                maybe_msg = stream.next() => {
                    match maybe_msg {
                        Some(Ok(request)) => {
                            match request {
                                RespValue::Array(Some(array)) => {
                                    if array.is_empty() { continue; }
                                    let command_name = match &array[0] {
                                        RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string().to_uppercase(),
                                        _ => continue,
                                    };

                                    match command_name.as_str() {
                                        "SUBSCRIBE" => {
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
                                                if let Err(e) = sink.send(resp).await { tracing::error!("send error: {}", e); break; }
                                            }
                                        }
                                        "UNSUBSCRIBE" => {
                                            let targets: Vec<String> = if array.len() == 1 {
                                                subscriptions.iter().cloned().collect()
                                            } else {
                                                array.iter().skip(1).filter_map(|arg| {
                                                    match arg { RespValue::BulkString(Some(bytes)) => Some(String::from_utf8_lossy(bytes).to_string()), _ => None }
                                                }).collect()
                                            };
                                            for channel in targets {
                                                {
                                                    let mut map = self.pubsub_channels.write().unwrap();
                                                    if let Some(vec) = map.get_mut(&channel) {
                                                        if let Some(pos) = vec.iter().position(|(id, _)| *id == conn_id) {
                                                            vec.swap_remove(pos);
                                                        }
                                                        if vec.is_empty() { map.remove(&channel); }
                                                    }
                                                }
                                                subscriptions.remove(&channel);
                                                let resp = RespValue::Array(Some(vec![
                                                    RespValue::BulkString(Some("unsubscribe".as_bytes().to_vec())),
                                                    RespValue::BulkString(Some(channel.as_bytes().to_vec())),
                                                    RespValue::Integer(subscriptions.len() as i64),
                                                ]));
                                                if let Err(e) = sink.send(resp).await { tracing::error!("send error: {}", e); break; }
                                            }
                                        }
                                        _ => {
                                            let args = array[1..].to_vec();
                                            match registry.get(&command_name) {
                                                Some(handler) => {
                                                    let response = handler.execute(args).await?;
                                                    if let Err(e) = sink.send(response).await { tracing::error!("send error: {}", e); break; }
                                                },
                                                None => {
                                                    let err = RespValue::Error(format!("ERR unknown command '{}'", command_name));
                                                    if let Err(e) = sink.send(err).await { tracing::error!("send error: {}", e); break; }
                                                }
                                            }
                                        }
                                    }
                                }
                                _ => {
                                    let err = RespValue::Error("ERR invalid request format".to_string());
                                    if let Err(e) = sink.send(err).await { tracing::error!("send error: {}", e); break; }
                                }
                            }
                        }
                        Some(Err(e)) => {
                            tracing::error!("decode error: {}", e);
                            break;
                        }
                        None => {
                            // stream closed
                            break;
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
                            if let Err(e) = sink.send(resp).await { tracing::error!("send error: {}", e); break; }
                        }
                        None => { break; }
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
        tracing::info!("Redis server listening on {}", addr);

        loop {
            let (socket, addr) = listener.accept().await?;
            tracing::debug!("New connection from {}", addr);

            let server = self.clone();
            tokio::spawn(async move {
                if let Err(e) = server.handle_connection(socket).await {
                    tracing::error!("Error handling connection from {}: {}", addr, e);
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
            string_store: Arc::clone(&self.string_store),
            list_store: Arc::clone(&self.list_store),
            set_store: Arc::clone(&self.set_store),
            hash_store: Arc::clone(&self.hash_store),
            expirations: Arc::clone(&self.expirations),
        }
    }
}
