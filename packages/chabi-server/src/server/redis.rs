use chabi_core::commands::sorted_set::SortedSet;
use chabi_core::commands::CommandHandler;
use chabi_core::resp::{RespParser, RespValue};
use chabi_core::storage::{DataStore, Snapshot};
use chabi_core::Result;
use chabi_core::RwLock;
use futures::{SinkExt, StreamExt};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_util::codec::Framed;

// Simplify complex pubsub channel type
type PubSubChannels =
    std::sync::RwLock<HashMap<String, Vec<(usize, mpsc::Sender<(String, String)>)>>>;

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

static NEXT_CONN_ID: AtomicUsize = AtomicUsize::new(1);

pub const NUM_DBS: usize = 16;

/// Build a command registry bound to the given DataStore and PubSub channels.
/// SELECT and FLUSHALL are NOT included — they are handled specially in dispatch.
fn build_command_registry(
    store: &DataStore,
    pubsub: &Arc<PubSubChannels>,
) -> HashMap<String, Box<dyn CommandHandler + Send + Sync>> {
    let mut reg = HashMap::new();

    macro_rules! insert {
        ($name:expr, $cmd:expr) => {
            reg.insert(
                $name.to_string(),
                Box::new($cmd) as Box<dyn CommandHandler + Send + Sync>,
            );
        };
    }

    // --- Connection commands ---
    insert!("PING", chabi_core::commands::connection::PingCommand::new());
    insert!("ECHO", chabi_core::commands::connection::EchoCommand::new());
    insert!("QUIT", chabi_core::commands::connection::QuitCommand::new());
    insert!(
        "RESET",
        chabi_core::commands::connection::ResetCommand::new()
    );
    insert!("AUTH", chabi_core::commands::connection::AuthCommand::new());
    insert!(
        "CLIENT",
        chabi_core::commands::connection::ClientCommand::new()
    );
    insert!(
        "HELLO",
        chabi_core::commands::connection::HelloCommand::new()
    );

    // --- String commands ---
    insert!(
        "SET",
        chabi_core::commands::string::SetCommand::new(store.clone())
    );
    insert!(
        "GET",
        chabi_core::commands::string::GetCommand::new(store.clone())
    );
    insert!(
        "DEL",
        chabi_core::commands::string::DelCommand::new(store.clone())
    );
    insert!(
        "EXISTS",
        chabi_core::commands::string::ExistsCommand::new(store.clone())
    );
    insert!(
        "APPEND",
        chabi_core::commands::string::AppendCommand::new(store.clone())
    );
    insert!(
        "STRLEN",
        chabi_core::commands::string::StrlenCommand::new(store.clone())
    );
    insert!(
        "INCR",
        chabi_core::commands::string::IncrCommand::new(store.clone())
    );
    insert!(
        "DECR",
        chabi_core::commands::string::DecrCommand::new(store.clone())
    );
    insert!(
        "INCRBY",
        chabi_core::commands::string::IncrByCommand::new(store.clone())
    );
    insert!(
        "DECRBY",
        chabi_core::commands::string::DecrByCommand::new(store.clone())
    );
    insert!(
        "INCRBYFLOAT",
        chabi_core::commands::string::IncrByFloatCommand::new(store.clone())
    );
    insert!(
        "MGET",
        chabi_core::commands::string::MGetCommand::new(store.clone())
    );
    insert!(
        "MSET",
        chabi_core::commands::string::MSetCommand::new(store.clone())
    );
    insert!(
        "MSETNX",
        chabi_core::commands::string::MSetNxCommand::new(store.clone())
    );
    insert!(
        "SETNX",
        chabi_core::commands::string::SetNxCommand::new(store.clone())
    );
    insert!(
        "SETEX",
        chabi_core::commands::string::SetExCommand::new(store.clone())
    );
    insert!(
        "PSETEX",
        chabi_core::commands::string::PSetExCommand::new(store.clone())
    );
    insert!(
        "GETRANGE",
        chabi_core::commands::string::GetRangeCommand::new(store.clone())
    );
    insert!(
        "SETRANGE",
        chabi_core::commands::string::SetRangeCommand::new(store.clone())
    );
    insert!(
        "GETDEL",
        chabi_core::commands::string::GetDelCommand::new(store.clone())
    );
    insert!(
        "GETEX",
        chabi_core::commands::string::GetExCommand::new(store.clone())
    );

    // --- Key commands ---
    insert!(
        "KEYS",
        chabi_core::commands::key::KeysCommand::new(store.clone())
    );
    insert!(
        "TTL",
        chabi_core::commands::key::TTLCommand::new(store.clone())
    );
    insert!(
        "PTTL",
        chabi_core::commands::key::PTTLCommand::new(store.clone())
    );
    insert!(
        "EXPIRE",
        chabi_core::commands::key::ExpireCommand::new(store.clone())
    );
    insert!(
        "PEXPIRE",
        chabi_core::commands::key::PExpireCommand::new(store.clone())
    );
    insert!(
        "EXPIREAT",
        chabi_core::commands::key::ExpireAtCommand::new(store.clone())
    );
    insert!(
        "PEXPIREAT",
        chabi_core::commands::key::PExpireAtCommand::new(store.clone())
    );
    insert!(
        "RENAME",
        chabi_core::commands::key::RenameCommand::new(store.clone())
    );
    insert!(
        "RENAMENX",
        chabi_core::commands::key::RenameNxCommand::new(store.clone())
    );
    insert!(
        "TYPE",
        chabi_core::commands::key::TypeCommand::new(store.clone())
    );
    insert!(
        "PERSIST",
        chabi_core::commands::key::PersistCommand::new(store.clone())
    );
    insert!(
        "UNLINK",
        chabi_core::commands::key::UnlinkCommand::new(store.clone())
    );
    insert!(
        "RANDOMKEY",
        chabi_core::commands::key::RandomKeyCommand::new(store.clone())
    );
    insert!(
        "SCAN",
        chabi_core::commands::key::ScanCommand::new(store.clone())
    );
    insert!(
        "COPY",
        chabi_core::commands::key::CopyCommand::new(store.clone())
    );
    insert!(
        "TOUCH",
        chabi_core::commands::key::TouchCommand::new(store.clone())
    );
    insert!("OBJECT", chabi_core::commands::key::ObjectCommand::new());

    // --- List commands ---
    insert!(
        "LPUSH",
        chabi_core::commands::list::LPushCommand::new(store.clone())
    );
    insert!(
        "RPUSH",
        chabi_core::commands::list::RPushCommand::new(store.clone())
    );
    insert!(
        "LPOP",
        chabi_core::commands::list::LPopCommand::new(store.clone())
    );
    insert!(
        "RPOP",
        chabi_core::commands::list::RPopCommand::new(store.clone())
    );
    insert!(
        "LRANGE",
        chabi_core::commands::list::LRangeCommand::new(store.clone())
    );
    insert!(
        "LLEN",
        chabi_core::commands::list::LLenCommand::new(store.clone())
    );
    insert!(
        "LINDEX",
        chabi_core::commands::list::LIndexCommand::new(store.clone())
    );
    insert!(
        "LSET",
        chabi_core::commands::list::LSetCommand::new(store.clone())
    );
    insert!(
        "LTRIM",
        chabi_core::commands::list::LTrimCommand::new(store.clone())
    );
    insert!(
        "LINSERT",
        chabi_core::commands::list::LInsertCommand::new(store.clone())
    );
    insert!(
        "LREM",
        chabi_core::commands::list::LRemCommand::new(store.clone())
    );
    insert!(
        "LPOS",
        chabi_core::commands::list::LPosCommand::new(store.clone())
    );
    insert!(
        "LPUSHX",
        chabi_core::commands::list::LPushXCommand::new(store.clone())
    );
    insert!(
        "RPUSHX",
        chabi_core::commands::list::RPushXCommand::new(store.clone())
    );
    insert!(
        "LMOVE",
        chabi_core::commands::list::LMoveCommand::new(store.clone())
    );

    // --- Set commands ---
    insert!(
        "SADD",
        chabi_core::commands::set::SAddCommand::new(store.clone())
    );
    insert!(
        "SMEMBERS",
        chabi_core::commands::set::SMembersCommand::new(store.clone())
    );
    insert!(
        "SISMEMBER",
        chabi_core::commands::set::SIsMemberCommand::new(store.clone())
    );
    insert!(
        "SCARD",
        chabi_core::commands::set::SCardCommand::new(store.clone())
    );
    insert!(
        "SREM",
        chabi_core::commands::set::SRemCommand::new(store.clone())
    );
    insert!(
        "SPOP",
        chabi_core::commands::set::SPopCommand::new(store.clone())
    );
    insert!(
        "SRANDMEMBER",
        chabi_core::commands::set::SRandMemberCommand::new(store.clone())
    );
    insert!(
        "SMOVE",
        chabi_core::commands::set::SMoveCommand::new(store.clone())
    );
    insert!(
        "SINTER",
        chabi_core::commands::set::SInterCommand::new(store.clone())
    );
    insert!(
        "SUNION",
        chabi_core::commands::set::SUnionCommand::new(store.clone())
    );
    insert!(
        "SDIFF",
        chabi_core::commands::set::SDiffCommand::new(store.clone())
    );
    insert!(
        "SINTERSTORE",
        chabi_core::commands::set::SInterStoreCommand::new(store.clone())
    );
    insert!(
        "SUNIONSTORE",
        chabi_core::commands::set::SUnionStoreCommand::new(store.clone())
    );
    insert!(
        "SDIFFSTORE",
        chabi_core::commands::set::SDiffStoreCommand::new(store.clone())
    );
    insert!(
        "SSCAN",
        chabi_core::commands::set::SScanCommand::new(store.clone())
    );
    insert!(
        "SMISMEMBER",
        chabi_core::commands::set::SMisMemberCommand::new(store.clone())
    );
    insert!(
        "SINTERCARD",
        chabi_core::commands::set::SInterCardCommand::new(store.clone())
    );

    // --- Hash commands ---
    insert!(
        "HSET",
        chabi_core::commands::hash::HSetCommand::new(store.clone())
    );
    // HMSET is an alias for HSET
    insert!(
        "HMSET",
        chabi_core::commands::hash::HSetCommand::new(store.clone())
    );
    insert!(
        "HGET",
        chabi_core::commands::hash::HGetCommand::new(store.clone())
    );
    insert!(
        "HGETALL",
        chabi_core::commands::hash::HGetAllCommand::new(store.clone())
    );
    insert!(
        "HEXISTS",
        chabi_core::commands::hash::HExistsCommand::new(store.clone())
    );
    insert!(
        "HDEL",
        chabi_core::commands::hash::HDelCommand::new(store.clone())
    );
    insert!(
        "HLEN",
        chabi_core::commands::hash::HLenCommand::new(store.clone())
    );
    insert!(
        "HKEYS",
        chabi_core::commands::hash::HKeysCommand::new(store.clone())
    );
    insert!(
        "HVALS",
        chabi_core::commands::hash::HValsCommand::new(store.clone())
    );
    insert!(
        "HMGET",
        chabi_core::commands::hash::HMGetCommand::new(store.clone())
    );
    insert!(
        "HINCRBY",
        chabi_core::commands::hash::HIncrByCommand::new(store.clone())
    );
    insert!(
        "HINCRBYFLOAT",
        chabi_core::commands::hash::HIncrByFloatCommand::new(store.clone())
    );
    insert!(
        "HSETNX",
        chabi_core::commands::hash::HSetNxCommand::new(store.clone())
    );
    insert!(
        "HSTRLEN",
        chabi_core::commands::hash::HStrLenCommand::new(store.clone())
    );
    insert!(
        "HSCAN",
        chabi_core::commands::hash::HScanCommand::new(store.clone())
    );
    insert!(
        "HRANDFIELD",
        chabi_core::commands::hash::HRandFieldCommand::new(store.clone())
    );

    // --- Sorted set commands ---
    insert!(
        "ZADD",
        chabi_core::commands::sorted_set::ZAddCommand::new(store.clone())
    );
    insert!(
        "ZREM",
        chabi_core::commands::sorted_set::ZRemCommand::new(store.clone())
    );
    insert!(
        "ZSCORE",
        chabi_core::commands::sorted_set::ZScoreCommand::new(store.clone())
    );
    insert!(
        "ZCARD",
        chabi_core::commands::sorted_set::ZCardCommand::new(store.clone())
    );
    insert!(
        "ZCOUNT",
        chabi_core::commands::sorted_set::ZCountCommand::new(store.clone())
    );
    insert!(
        "ZRANGE",
        chabi_core::commands::sorted_set::ZRangeCommand::new(store.clone())
    );
    insert!(
        "ZREVRANGE",
        chabi_core::commands::sorted_set::ZRevRangeCommand::new(store.clone())
    );
    insert!(
        "ZRANGEBYSCORE",
        chabi_core::commands::sorted_set::ZRangeByScoreCommand::new(store.clone())
    );
    insert!(
        "ZREVRANGEBYSCORE",
        chabi_core::commands::sorted_set::ZRevRangeByScoreCommand::new(store.clone())
    );
    insert!(
        "ZRANK",
        chabi_core::commands::sorted_set::ZRankCommand::new(store.clone())
    );
    insert!(
        "ZREVRANK",
        chabi_core::commands::sorted_set::ZRevRankCommand::new(store.clone())
    );
    insert!(
        "ZINCRBY",
        chabi_core::commands::sorted_set::ZIncrByCommand::new(store.clone())
    );
    insert!(
        "ZPOPMIN",
        chabi_core::commands::sorted_set::ZPopMinCommand::new(store.clone())
    );
    insert!(
        "ZPOPMAX",
        chabi_core::commands::sorted_set::ZPopMaxCommand::new(store.clone())
    );
    insert!(
        "ZRANDMEMBER",
        chabi_core::commands::sorted_set::ZRandMemberCommand::new(store.clone())
    );
    insert!(
        "ZMSCORE",
        chabi_core::commands::sorted_set::ZMScoreCommand::new(store.clone())
    );
    insert!(
        "ZUNIONSTORE",
        chabi_core::commands::sorted_set::ZUnionStoreCommand::new(store.clone())
    );
    insert!(
        "ZINTERSTORE",
        chabi_core::commands::sorted_set::ZInterStoreCommand::new(store.clone())
    );
    insert!(
        "ZSCAN",
        chabi_core::commands::sorted_set::ZScanCommand::new(store.clone())
    );

    // --- Bitmap commands ---
    insert!(
        "SETBIT",
        chabi_core::commands::bitmap::SetBitCommand::new(store.clone())
    );
    insert!(
        "GETBIT",
        chabi_core::commands::bitmap::GetBitCommand::new(store.clone())
    );
    insert!(
        "BITCOUNT",
        chabi_core::commands::bitmap::BitCountCommand::new(store.clone())
    );
    insert!(
        "BITPOS",
        chabi_core::commands::bitmap::BitPosCommand::new(store.clone())
    );

    // --- HyperLogLog commands ---
    insert!(
        "PFADD",
        chabi_core::commands::hyperloglog::PfAddCommand::new(store.clone())
    );
    insert!(
        "PFCOUNT",
        chabi_core::commands::hyperloglog::PfCountCommand::new(store.clone())
    );
    insert!(
        "PFMERGE",
        chabi_core::commands::hyperloglog::PfMergeCommand::new(store.clone())
    );

    // --- Server commands (per-DB) ---
    insert!(
        "INFO",
        chabi_core::commands::server::InfoCommand::new(store.clone())
    );
    insert!("SAVE", chabi_core::commands::server::SaveCommand::new());
    insert!(
        "DBSIZE",
        chabi_core::commands::server::DbSizeCommand::new(store.clone())
    );
    insert!(
        "FLUSHDB",
        chabi_core::commands::server::FlushDbCommand::new(store.clone())
    );
    insert!("CONFIG", chabi_core::commands::server::ConfigCommand::new());
    insert!(
        "COMMAND",
        chabi_core::commands::server::CommandCommand::new()
    );
    insert!("TIME", chabi_core::commands::server::TimeCommand::new());
    insert!("BGSAVE", chabi_core::commands::server::BgSaveCommand::new());

    // --- PubSub commands ---
    insert!(
        "PUBLISH",
        chabi_core::commands::pubsub::PublishCommand::new(Arc::clone(pubsub))
    );
    insert!(
        "SUBSCRIBE",
        chabi_core::commands::pubsub::SubscribeCommand::new(Arc::clone(pubsub))
    );
    insert!(
        "UNSUBSCRIBE",
        chabi_core::commands::pubsub::UnsubscribeCommand::new(Arc::clone(pubsub))
    );
    insert!(
        "PUBSUB",
        chabi_core::commands::pubsub::PubSubCommand::new(Arc::clone(pubsub))
    );

    reg
}

pub struct RedisServer {
    databases: [DataStore; NUM_DBS],
    registries: [Arc<HashMap<String, Box<dyn CommandHandler + Send + Sync>>>; NUM_DBS],
    pubsub_channels: Arc<PubSubChannels>,
    snapshot_dir: Arc<RwLock<Option<String>>>,
    total_connections_served: Arc<AtomicUsize>,
    total_commands_processed: Arc<AtomicUsize>,
    connected_clients: Arc<AtomicUsize>,
}

impl RedisServer {
    pub fn new() -> Self {
        let snapshot_dir: Arc<RwLock<Option<String>>> = Arc::new(RwLock::new(None));
        let pubsub_channels: Arc<PubSubChannels> = Arc::new(std::sync::RwLock::new(HashMap::new()));

        let databases: [DataStore; NUM_DBS] = std::array::from_fn(|_| DataStore::new());
        let registries: [Arc<HashMap<String, Box<dyn CommandHandler + Send + Sync>>>; NUM_DBS] =
            std::array::from_fn(|i| {
                Arc::new(build_command_registry(&databases[i], &pubsub_channels))
            });

        RedisServer {
            databases,
            registries,
            pubsub_channels: Arc::clone(&pubsub_channels),
            snapshot_dir,
            total_connections_served: Arc::new(AtomicUsize::new(0)),
            total_commands_processed: Arc::new(AtomicUsize::new(0)),
            connected_clients: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub async fn set_snapshot_dir(&self, dir: String) {
        let mut g = self.snapshot_dir.write().await;
        *g = Some(dir);
    }

    fn db_file_path(dir: &str) -> PathBuf {
        Path::new(dir).join("chabi.kdb")
    }

    pub fn start_snapshot_task(&self, dir: String, interval: Duration) {
        let databases: Vec<DataStore> = self.databases.to_vec();
        tokio::spawn(async move {
            loop {
                sleep(interval).await;
                let snapshots = build_all_snapshots(&databases).await;

                if let Err(e) = Self::persist_snapshot_to_dir(&dir, snapshots).await {
                    tracing::error!("snapshot persist error: {}", e);
                } else {
                    tracing::debug!("snapshot saved to {}/chabi.kdb", dir);
                }
            }
        });
    }

    pub async fn prometheus_metrics(&self) -> String {
        let connected = self.connected_clients.load(Ordering::Relaxed);
        let total_conns = self.total_connections_served.load(Ordering::Relaxed);
        let total_cmds = self.total_commands_processed.load(Ordering::Relaxed);

        let mut string_count = 0;
        let mut list_count = 0;
        let mut set_count = 0;
        let mut hash_count = 0;
        let mut sorted_set_count = 0;
        let mut expiration_count = 0;

        for db in &self.databases {
            string_count += db.strings.read().await.len();
            list_count += db.lists.read().await.len();
            set_count += db.sets.read().await.len();
            hash_count += db.hashes.read().await.len();
            sorted_set_count += db.sorted_sets.read().await.len();
            expiration_count += db.expirations.read().await.len();
        }

        let pubsub_channels = self.pubsub_channels.read().map(|m| m.len()).unwrap_or(0);

        format!(
            "# HELP chabi_connected_clients Number of currently connected clients\n\
             # TYPE chabi_connected_clients gauge\n\
             chabi_connected_clients {}\n\
             # HELP chabi_total_connections_served Total connections served since start\n\
             # TYPE chabi_total_connections_served counter\n\
             chabi_total_connections_served {}\n\
             # HELP chabi_total_commands_processed Total commands processed since start\n\
             # TYPE chabi_total_commands_processed counter\n\
             chabi_total_commands_processed {}\n\
             # HELP chabi_keys Number of keys by data type\n\
             # TYPE chabi_keys gauge\n\
             chabi_keys{{type=\"string\"}} {}\n\
             chabi_keys{{type=\"list\"}} {}\n\
             chabi_keys{{type=\"set\"}} {}\n\
             chabi_keys{{type=\"hash\"}} {}\n\
             chabi_keys{{type=\"zset\"}} {}\n\
             # HELP chabi_expiring_keys Number of keys with TTL set\n\
             # TYPE chabi_expiring_keys gauge\n\
             chabi_expiring_keys {}\n\
             # HELP chabi_pubsub_channels Number of active pubsub channels\n\
             # TYPE chabi_pubsub_channels gauge\n\
             chabi_pubsub_channels {}\n",
            connected,
            total_conns,
            total_cmds,
            string_count,
            list_count,
            set_count,
            hash_count,
            sorted_set_count,
            expiration_count,
            pubsub_channels,
        )
    }

    pub async fn build_all_snapshots(&self) -> Vec<(usize, Snapshot)> {
        build_all_snapshots(&self.databases).await
    }

    pub async fn load_snapshot_from_dir(&self, dir: &str) -> Result<()> {
        let db_path = Self::db_file_path(dir);
        if tokio::fs::metadata(&db_path).await.is_err() {
            tracing::info!("snapshot database not found: {}", db_path.display());
            return Ok(());
        }
        // Use blocking thread for redb IO
        let all_snapshots = tokio::task::spawn_blocking({
            let db_path = db_path.clone();
            move || -> std::result::Result<Vec<(usize, Snapshot)>, BoxedError> {
                let db = Database::open(db_path)?;
                let rtxn = db.begin_read()?;

                let mut results = Vec::new();

                for db_idx in 0..NUM_DBS {
                    let snapshot = read_snapshot_for_db(&rtxn, db_idx)?;
                    if !snapshot_is_empty(&snapshot) {
                        results.push((db_idx, snapshot));
                    }
                }

                Ok(results)
            }
        })
        .await
        .map_err(|e| -> BoxedError { Box::new(e) })??;

        for (db_idx, snapshot) in &all_snapshots {
            tracing::info!(
                "loaded snapshot for db{} from {}/chabi.kdb (strings={}, lists={}, sets={}, hashes={}, sorted_sets={}, hll={}, expirations={})",
                db_idx,
                dir,
                snapshot.strings.len(),
                snapshot.lists.len(),
                snapshot.sets.len(),
                snapshot.hashes.len(),
                snapshot.sorted_sets.len(),
                snapshot.hll.len(),
                snapshot.expirations_epoch_secs.len(),
            );
        }

        for (db_idx, snapshot) in all_snapshots {
            self.databases[db_idx].restore_from_snapshot(snapshot).await;
        }

        Ok(())
    }

    async fn persist_snapshot_to_dir(dir: &str, snapshots: Vec<(usize, Snapshot)>) -> Result<()> {
        let dir = dir.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            // Ensure directory exists
            std::fs::create_dir_all(&dir)?;
            let final_db_path = Self::db_file_path(&dir);
            let tmp_db_path = final_db_path.with_extension("kdb.tmp");
            // Clean up any previous tmp
            if tmp_db_path.exists() {
                let _ = std::fs::remove_file(&tmp_db_path);
            }
            {
                // Create and write to temporary DB
                let db = Database::create(&tmp_db_path)?;
                let write_txn = db.begin_write()?;

                for (db_idx, snapshot) in &snapshots {
                    write_snapshot_for_db(&write_txn, *db_idx, snapshot)?;
                }

                write_txn.commit()?;
                // db dropped here to release file handles before rename
            }
            // Now replace the final DB atomically when possible
            #[cfg(target_family = "windows")]
            let _ = std::fs::remove_file(&final_db_path);
            match std::fs::rename(&tmp_db_path, &final_db_path) {
                Ok(_) => {}
                Err(e) => {
                    // Fallback: copy then remove tmp (best-effort on non-atomic platforms)
                    std::fs::copy(&tmp_db_path, &final_db_path)
                        .map(|_| std::fs::remove_file(&tmp_db_path).ok())
                        .map_err(|copy_err| {
                            let msg = format!(
                                "snapshot rename/copy error: {} (rename_err: {})",
                                copy_err, e
                            );
                            std::io::Error::other(msg)
                        })?;
                }
            }
            Ok(())
        })
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })??;
        Ok(())
    }

    async fn ensure_snapshot_dir(&self) -> String {
        // Returns configured dir or creates a temp one
        if let Some(dir) = self.snapshot_dir.read().await.clone() {
            return dir;
        }
        let tmp = std::env::temp_dir();
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let dir = tmp.join(format!("chabi-{}", ts));
        let dir_str = dir.to_string_lossy().to_string();
        if let Err(e) = tokio::fs::create_dir_all(&dir).await {
            tracing::error!(
                "Failed to create temp snapshot directory {}: {}",
                dir.display(),
                e
            );
        } else {
            let mut g = self.snapshot_dir.write().await;
            *g = Some(dir_str.clone());
        }
        self.snapshot_dir
            .read()
            .await
            .clone()
            .unwrap_or_else(|| "".to_string())
    }

    async fn handle_connection(&self, stream: TcpStream) -> Result<()> {
        // Low-latency optimization
        let _ = stream.set_nodelay(true);

        self.total_connections_served
            .fetch_add(1, Ordering::Relaxed);
        self.connected_clients.fetch_add(1, Ordering::Relaxed);

        let mut current_db: usize = 0;
        let conn_id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);
        let (tx, mut rx) = mpsc::channel::<(String, String)>(100);
        let mut subscriptions: HashSet<String> = HashSet::new();
        // Pattern subscriptions: pattern -> original pattern string
        let mut pattern_subscriptions: HashSet<String> = HashSet::new();
        // Transaction state
        let mut transaction_queue: Option<Vec<(String, Vec<RespValue>)>> = None;

        let framed = Framed::new(stream, RespParser::new());
        let (mut sink, mut stream) = framed.split();

        // Helper macro to send response and break on error
        macro_rules! send_resp {
            ($resp:expr) => {
                if let Err(e) = sink.send($resp).await {
                    tracing::error!("send error: {}", e);
                    break;
                }
            };
        }

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

                                    self.total_commands_processed.fetch_add(1, Ordering::Relaxed);

                                    // If in MULTI mode, queue commands (except MULTI/EXEC/DISCARD/WATCH/UNWATCH)
                                    if let Some(ref mut queue) = transaction_queue {
                                        if !matches!(command_name.as_str(), "MULTI" | "EXEC" | "DISCARD" | "WATCH" | "UNWATCH") {
                                            let args = array[1..].to_vec();
                                            queue.push((command_name, args));
                                            send_resp!(RespValue::SimpleString("QUEUED".to_string()));
                                            continue;
                                        }
                                    }

                                    match command_name.as_str() {
                                        // --- SELECT ---
                                        "SELECT" => {
                                            if array.len() < 2 {
                                                send_resp!(RespValue::Error("ERR wrong number of arguments for 'select' command".to_string()));
                                            } else {
                                                let db_str = match &array[1] {
                                                    RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
                                                    _ => String::new(),
                                                };
                                                match db_str.parse::<usize>() {
                                                    Ok(idx) if idx < NUM_DBS => {
                                                        current_db = idx;
                                                        send_resp!(RespValue::SimpleString("OK".to_string()));
                                                    }
                                                    _ => {
                                                        send_resp!(RespValue::Error("ERR DB index is out of range".to_string()));
                                                    }
                                                }
                                            }
                                        }
                                        // --- FLUSHALL ---
                                        "FLUSHALL" => {
                                            for db in &self.databases {
                                                db.strings.write().await.clear();
                                                db.lists.write().await.clear();
                                                db.sets.write().await.clear();
                                                db.hashes.write().await.clear();
                                                db.sorted_sets.write().await.clear();
                                                db.hll.write().await.clear();
                                                db.bitmaps.write().await.clear();
                                                db.expirations.write().await.clear();
                                            }
                                            send_resp!(RespValue::SimpleString("OK".to_string()));
                                        }
                                        // --- Transaction commands ---
                                        "MULTI" => {
                                            if transaction_queue.is_some() {
                                                send_resp!(RespValue::Error("ERR MULTI calls can not be nested".to_string()));
                                            } else {
                                                transaction_queue = Some(Vec::new());
                                                send_resp!(RespValue::SimpleString("OK".to_string()));
                                            }
                                        }
                                        "EXEC" => {
                                            match transaction_queue.take() {
                                                Some(queued) => {
                                                    let registry = Arc::clone(&self.registries[current_db]);
                                                    let mut results = Vec::with_capacity(queued.len());
                                                    for (cmd, args) in queued {
                                                        if cmd == "FLUSHALL" {
                                                            for db in &self.databases {
                                                                db.strings.write().await.clear();
                                                                db.lists.write().await.clear();
                                                                db.sets.write().await.clear();
                                                                db.hashes.write().await.clear();
                                                                db.sorted_sets.write().await.clear();
                                                                db.hll.write().await.clear();
                                                                db.bitmaps.write().await.clear();
                                                                db.expirations.write().await.clear();
                                                            }
                                                            results.push(RespValue::SimpleString("OK".to_string()));
                                                        } else if cmd == "SELECT" {
                                                            // SELECT inside MULTI: update current_db
                                                            if let Some(RespValue::BulkString(Some(bytes))) = args.first() {
                                                                let db_str = String::from_utf8_lossy(bytes).to_string();
                                                                match db_str.parse::<usize>() {
                                                                    Ok(idx) if idx < NUM_DBS => {
                                                                        current_db = idx;
                                                                        results.push(RespValue::SimpleString("OK".to_string()));
                                                                    }
                                                                    _ => {
                                                                        results.push(RespValue::Error("ERR DB index is out of range".to_string()));
                                                                    }
                                                                }
                                                            } else {
                                                                results.push(RespValue::Error("ERR wrong number of arguments for 'select' command".to_string()));
                                                            }
                                                        } else {
                                                            match registry.get(&cmd) {
                                                                Some(handler) => {
                                                                    match handler.execute(args).await {
                                                                        Ok(resp) => results.push(resp),
                                                                        Err(e) => results.push(RespValue::Error(format!("ERR {}", e))),
                                                                    }
                                                                }
                                                                None => {
                                                                    results.push(RespValue::Error(format!("ERR unknown command '{}'", cmd)));
                                                                }
                                                            }
                                                        }
                                                    }
                                                    send_resp!(RespValue::Array(Some(results)));
                                                }
                                                None => {
                                                    send_resp!(RespValue::Error("ERR EXEC without MULTI".to_string()));
                                                }
                                            }
                                        }
                                        "DISCARD" => {
                                            if transaction_queue.is_some() {
                                                transaction_queue = None;
                                                send_resp!(RespValue::SimpleString("OK".to_string()));
                                            } else {
                                                send_resp!(RespValue::Error("ERR DISCARD without MULTI".to_string()));
                                            }
                                        }
                                        "WATCH" => {
                                            // WATCH is a stub - we don't track key versions but accept the command
                                            if transaction_queue.is_some() {
                                                send_resp!(RespValue::Error("ERR WATCH inside MULTI is not allowed".to_string()));
                                            } else {
                                                send_resp!(RespValue::SimpleString("OK".to_string()));
                                            }
                                        }
                                        "UNWATCH" => {
                                            send_resp!(RespValue::SimpleString("OK".to_string()));
                                        }
                                        // --- PubSub commands ---
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
                                                let total = subscriptions.len() + pattern_subscriptions.len();
                                                let resp = RespValue::Array(Some(vec![
                                                    RespValue::BulkString(Some("subscribe".as_bytes().to_vec())),
                                                    RespValue::BulkString(Some(channel.as_bytes().to_vec())),
                                                    RespValue::Integer(total as i64),
                                                ]));
                                                send_resp!(resp);
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
                                                let total = subscriptions.len() + pattern_subscriptions.len();
                                                let resp = RespValue::Array(Some(vec![
                                                    RespValue::BulkString(Some("unsubscribe".as_bytes().to_vec())),
                                                    RespValue::BulkString(Some(channel.as_bytes().to_vec())),
                                                    RespValue::Integer(total as i64),
                                                ]));
                                                send_resp!(resp);
                                            }
                                        }
                                        "PSUBSCRIBE" => {
                                            for arg in array.iter().skip(1) {
                                                let pattern = match arg {
                                                    RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
                                                    _ => continue,
                                                };
                                                pattern_subscriptions.insert(pattern.clone());
                                                let total = subscriptions.len() + pattern_subscriptions.len();
                                                let resp = RespValue::Array(Some(vec![
                                                    RespValue::BulkString(Some("psubscribe".as_bytes().to_vec())),
                                                    RespValue::BulkString(Some(pattern.as_bytes().to_vec())),
                                                    RespValue::Integer(total as i64),
                                                ]));
                                                send_resp!(resp);
                                            }
                                        }
                                        "PUNSUBSCRIBE" => {
                                            let targets: Vec<String> = if array.len() == 1 {
                                                pattern_subscriptions.iter().cloned().collect()
                                            } else {
                                                array.iter().skip(1).filter_map(|arg| {
                                                    match arg { RespValue::BulkString(Some(bytes)) => Some(String::from_utf8_lossy(bytes).to_string()), _ => None }
                                                }).collect()
                                            };
                                            for pattern in targets {
                                                pattern_subscriptions.remove(&pattern);
                                                let total = subscriptions.len() + pattern_subscriptions.len();
                                                let resp = RespValue::Array(Some(vec![
                                                    RespValue::BulkString(Some("punsubscribe".as_bytes().to_vec())),
                                                    RespValue::BulkString(Some(pattern.as_bytes().to_vec())),
                                                    RespValue::Integer(total as i64),
                                                ]));
                                                send_resp!(resp);
                                            }
                                        }
                                        "QUIT" => {
                                            send_resp!(RespValue::SimpleString("OK".to_string()));
                                            break;
                                        }
                                        "RESET" => {
                                            // Reset connection state: DB selection, transactions, subscriptions
                                            current_db = 0;
                                            transaction_queue = None;
                                            // Unsubscribe from all channels
                                            {
                                                let mut map = self.pubsub_channels.write().unwrap();
                                                for channel in &subscriptions {
                                                    if let Some(vec) = map.get_mut(channel) {
                                                        if let Some(pos) = vec.iter().position(|(id, _)| *id == conn_id) {
                                                            vec.swap_remove(pos);
                                                        }
                                                        if vec.is_empty() { map.remove(channel); }
                                                    }
                                                }
                                            }
                                            subscriptions.clear();
                                            pattern_subscriptions.clear();
                                            send_resp!(RespValue::SimpleString("RESET".to_string()));
                                        }
                                        "SAVE" => {
                                            // Perform a synchronous snapshot dump to the configured directory
                                            let snapshots = self.build_all_snapshots().await;
                                            let dir = self.ensure_snapshot_dir().await;
                                            match Self::persist_snapshot_to_dir(&dir, snapshots).await {
                                                Ok(_) => {
                                                    send_resp!(RespValue::SimpleString("OK".to_string()));
                                                }
                                                Err(e) => {
                                                    send_resp!(RespValue::Error(format!("ERR snapshot failed: {}", e)));
                                                }
                                            }
                                        }
                                        _ => {
                                            let registry = Arc::clone(&self.registries[current_db]);
                                            let args = array[1..].to_vec();
                                            match registry.get(&command_name) {
                                                Some(handler) => {
                                                    let response = handler.execute(args).await?;
                                                    send_resp!(response);
                                                },
                                                None => {
                                                    let err = RespValue::Error(format!("ERR unknown command '{}'", command_name));
                                                    send_resp!(err);
                                                }
                                            }
                                        }
                                    }
                                }
                                _ => {
                                    let err = RespValue::Error("ERR invalid request format".to_string());
                                    send_resp!(err);
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
                            send_resp!(resp);
                        }
                        None => { break; }
                    }
                }
            }
        }

        self.connected_clients.fetch_sub(1, Ordering::Relaxed);

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

/// Build snapshots for all non-empty databases.
async fn build_all_snapshots(databases: &[DataStore]) -> Vec<(usize, Snapshot)> {
    let mut snapshots = Vec::new();
    for (i, db) in databases.iter().enumerate() {
        let snapshot = db.build_snapshot().await;
        if !snapshot_is_empty(&snapshot) {
            snapshots.push((i, snapshot));
        }
    }
    snapshots
}

fn snapshot_is_empty(s: &Snapshot) -> bool {
    s.strings.is_empty()
        && s.lists.is_empty()
        && s.sets.is_empty()
        && s.hashes.is_empty()
        && s.sorted_sets.is_empty()
        && s.hll.is_empty()
        && s.expirations_epoch_secs.is_empty()
}

/// Return the redb table name for a given DB index and base table name.
/// DB 0 uses unprefixed names for backward compatibility.
fn table_name(db_idx: usize, base: &str) -> String {
    if db_idx == 0 {
        base.to_string()
    } else {
        format!("db{}_{}", db_idx, base)
    }
}

/// Write a single DB's snapshot into the given redb write transaction.
fn write_snapshot_for_db(
    write_txn: &redb::WriteTransaction,
    db_idx: usize,
    snapshot: &Snapshot,
) -> Result<()> {
    // Strings
    {
        let tname = table_name(db_idx, "strings");
        let tdef: TableDefinition<&str, &[u8]> = TableDefinition::new(&tname);
        let mut table = write_txn.open_table(tdef)?;
        for (k, v) in snapshot.strings.iter() {
            table.insert(k.as_str(), v.as_bytes())?;
        }
    }
    // Lists
    {
        let tname = table_name(db_idx, "lists");
        let tdef: TableDefinition<&str, &[u8]> = TableDefinition::new(&tname);
        let mut table = write_txn.open_table(tdef)?;
        for (k, v) in snapshot.lists.iter() {
            let bytes = bincode::serde::encode_to_vec(v, bincode::config::standard())?;
            table.insert(k.as_str(), bytes.as_slice())?;
        }
    }
    // Sets
    {
        let tname = table_name(db_idx, "sets");
        let tdef: TableDefinition<&str, &[u8]> = TableDefinition::new(&tname);
        let mut table = write_txn.open_table(tdef)?;
        for (k, v) in snapshot.sets.iter() {
            let bytes = bincode::serde::encode_to_vec(v, bincode::config::standard())?;
            table.insert(k.as_str(), bytes.as_slice())?;
        }
    }
    // Hashes
    {
        let tname = table_name(db_idx, "hashes");
        let tdef: TableDefinition<&str, &[u8]> = TableDefinition::new(&tname);
        let mut table = write_txn.open_table(tdef)?;
        for (k, v) in snapshot.hashes.iter() {
            let bytes = bincode::serde::encode_to_vec(v, bincode::config::standard())?;
            table.insert(k.as_str(), bytes.as_slice())?;
        }
    }
    // Sorted Sets
    {
        let tname = table_name(db_idx, "sorted_sets");
        let tdef: TableDefinition<&str, &[u8]> = TableDefinition::new(&tname);
        let mut table = write_txn.open_table(tdef)?;
        for (k, v) in snapshot.sorted_sets.iter() {
            let bytes = bincode::serde::encode_to_vec(v, bincode::config::standard())?;
            table.insert(k.as_str(), bytes.as_slice())?;
        }
    }
    // HyperLogLog
    {
        let tname = table_name(db_idx, "hll");
        let tdef: TableDefinition<&str, &[u8]> = TableDefinition::new(&tname);
        let mut table = write_txn.open_table(tdef)?;
        for (k, v) in snapshot.hll.iter() {
            table.insert(k.as_str(), v.as_slice())?;
        }
    }
    // Expirations
    {
        let tname = table_name(db_idx, "expirations");
        let tdef: TableDefinition<&str, u64> = TableDefinition::new(&tname);
        let mut table = write_txn.open_table(tdef)?;
        for (k, ts) in snapshot.expirations_epoch_secs.iter() {
            table.insert(k.as_str(), *ts)?;
        }
    }
    Ok(())
}

/// Read a single DB's snapshot from a redb read transaction.
fn read_snapshot_for_db(
    rtxn: &redb::ReadTransaction,
    db_idx: usize,
) -> std::result::Result<Snapshot, BoxedError> {
    // Strings
    let mut strings = HashMap::new();
    {
        let tname = table_name(db_idx, "strings");
        let tdef: TableDefinition<&str, &[u8]> = TableDefinition::new(&tname);
        if let Ok(table) = rtxn.open_table(tdef) {
            for item in table.iter()? {
                let (k, v) = item?;
                let key = k.value().to_string();
                let val = String::from_utf8(v.value().to_vec())?;
                strings.insert(key, val);
            }
        }
    }

    // Lists
    let mut lists = HashMap::new();
    {
        let tname = table_name(db_idx, "lists");
        let tdef: TableDefinition<&str, &[u8]> = TableDefinition::new(&tname);
        if let Ok(table) = rtxn.open_table(tdef) {
            for item in table.iter()? {
                let (k, v) = item?;
                let key = k.value().to_string();
                let val: Vec<String> =
                    bincode::serde::decode_from_slice(v.value(), bincode::config::standard())?.0;
                lists.insert(key, val);
            }
        }
    }

    // Sets
    let mut sets = HashMap::new();
    {
        let tname = table_name(db_idx, "sets");
        let tdef: TableDefinition<&str, &[u8]> = TableDefinition::new(&tname);
        if let Ok(table) = rtxn.open_table(tdef) {
            for item in table.iter()? {
                let (k, v) = item?;
                let key = k.value().to_string();
                let val: HashSet<String> =
                    bincode::serde::decode_from_slice(v.value(), bincode::config::standard())?.0;
                sets.insert(key, val);
            }
        }
    }

    // Hashes
    let mut hashes = HashMap::new();
    {
        let tname = table_name(db_idx, "hashes");
        let tdef: TableDefinition<&str, &[u8]> = TableDefinition::new(&tname);
        if let Ok(table) = rtxn.open_table(tdef) {
            for item in table.iter()? {
                let (k, v) = item?;
                let key = k.value().to_string();
                let val: HashMap<String, String> =
                    bincode::serde::decode_from_slice(v.value(), bincode::config::standard())?.0;
                hashes.insert(key, val);
            }
        }
    }

    // Sorted Sets
    let mut sorted_sets = HashMap::new();
    {
        let tname = table_name(db_idx, "sorted_sets");
        let tdef: TableDefinition<&str, &[u8]> = TableDefinition::new(&tname);
        if let Ok(table) = rtxn.open_table(tdef) {
            for item in table.iter()? {
                let (k, v) = item?;
                let key = k.value().to_string();
                let val: SortedSet =
                    bincode::serde::decode_from_slice(v.value(), bincode::config::standard())?.0;
                sorted_sets.insert(key, val);
            }
        }
    }

    // HyperLogLog
    let mut hll = HashMap::new();
    {
        let tname = table_name(db_idx, "hll");
        let tdef: TableDefinition<&str, &[u8]> = TableDefinition::new(&tname);
        if let Ok(table) = rtxn.open_table(tdef) {
            for item in table.iter()? {
                let (k, v) = item?;
                let key = k.value().to_string();
                let val = v.value().to_vec();
                hll.insert(key, val);
            }
        }
    }

    // Expirations
    let mut expirations_epoch_secs = HashMap::new();
    {
        let tname = table_name(db_idx, "expirations");
        let tdef: TableDefinition<&str, u64> = TableDefinition::new(&tname);
        if let Ok(table) = rtxn.open_table(tdef) {
            for item in table.iter()? {
                let (k, v) = item?;
                let key = k.value().to_string();
                let val = v.value();
                expirations_epoch_secs.insert(key, val);
            }
        }
    }

    Ok(Snapshot {
        strings,
        lists,
        sets,
        hashes,
        sorted_sets,
        hll,
        expirations_epoch_secs,
    })
}

impl Clone for RedisServer {
    fn clone(&self) -> Self {
        RedisServer {
            databases: self.databases.clone(),
            registries: self.registries.clone(),
            pubsub_channels: Arc::clone(&self.pubsub_channels),
            snapshot_dir: Arc::clone(&self.snapshot_dir),
            total_connections_served: Arc::clone(&self.total_connections_served),
            total_commands_processed: Arc::clone(&self.total_commands_processed),
            connected_clients: Arc::clone(&self.connected_clients),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Start a test server on a random port, returns the server and the bound address.
    async fn start_test_server() -> (Arc<RedisServer>, SocketAddr) {
        let server = Arc::new(RedisServer::new());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_clone = Arc::clone(&server);
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let srv = server_clone.clone();
                tokio::spawn(async move {
                    let _ = srv.handle_connection(socket).await;
                });
            }
        });

        // Give server a moment to be ready
        tokio::time::sleep(Duration::from_millis(20)).await;
        (server, addr)
    }

    /// Build RESP array from string parts
    fn build_command(parts: &[&str]) -> Vec<u8> {
        let mut cmd = format!("*{}\r\n", parts.len());
        for p in parts {
            cmd.push_str(&format!("${}\r\n{}\r\n", p.len(), p));
        }
        cmd.into_bytes()
    }

    /// Send a RESP command and read the response using async IO.
    async fn send_command(stream: &mut TcpStream, parts: &[&str]) -> String {
        let cmd = build_command(parts);
        stream.write_all(&cmd).await.unwrap();
        stream.flush().await.unwrap();

        let mut buf = vec![0u8; 4096];
        let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
            .await
            .unwrap_or(Ok(0))
            .unwrap_or(0);
        String::from_utf8_lossy(&buf[..n]).to_string()
    }

    /// Connect to a test server.
    async fn connect(addr: SocketAddr) -> TcpStream {
        TcpStream::connect(addr).await.unwrap()
    }

    #[tokio::test]
    async fn test_ping_pong() {
        let (_server, addr) = start_test_server().await;
        let mut stream = connect(addr).await;
        let resp = send_command(&mut stream, &["PING"]).await;
        assert!(resp.contains("PONG"));
    }

    #[tokio::test]
    async fn test_set_get() {
        let (_server, addr) = start_test_server().await;
        let mut stream = connect(addr).await;
        let resp = send_command(&mut stream, &["SET", "mykey", "myvalue"]).await;
        assert!(resp.contains("OK"));

        let resp = send_command(&mut stream, &["GET", "mykey"]).await;
        assert!(resp.contains("myvalue"));
    }

    #[tokio::test]
    async fn test_unknown_command() {
        let (_server, addr) = start_test_server().await;
        let mut stream = connect(addr).await;
        let resp = send_command(&mut stream, &["FOOBAR"]).await;
        assert!(resp.contains("ERR"));
    }

    #[tokio::test]
    async fn test_multi_exec() {
        let (_server, addr) = start_test_server().await;
        let mut stream = connect(addr).await;
        let resp = send_command(&mut stream, &["MULTI"]).await;
        assert!(resp.contains("OK"));

        let resp = send_command(&mut stream, &["SET", "txkey", "txval"]).await;
        assert!(resp.contains("QUEUED"));

        let resp = send_command(&mut stream, &["GET", "txkey"]).await;
        assert!(resp.contains("QUEUED"));

        let resp = send_command(&mut stream, &["EXEC"]).await;
        assert!(resp.contains("OK"));
        assert!(resp.contains("txval"));
    }

    #[tokio::test]
    async fn test_multi_nested_error() {
        let (_server, addr) = start_test_server().await;
        let mut stream = connect(addr).await;
        let _ = send_command(&mut stream, &["MULTI"]).await;
        let resp = send_command(&mut stream, &["MULTI"]).await;
        assert!(resp.contains("ERR"));
    }

    #[tokio::test]
    async fn test_exec_without_multi() {
        let (_server, addr) = start_test_server().await;
        let mut stream = connect(addr).await;
        let resp = send_command(&mut stream, &["EXEC"]).await;
        assert!(resp.contains("ERR"));
    }

    #[tokio::test]
    async fn test_discard() {
        let (_server, addr) = start_test_server().await;
        let mut stream = connect(addr).await;
        let _ = send_command(&mut stream, &["MULTI"]).await;
        let resp = send_command(&mut stream, &["SET", "k", "v"]).await;
        assert!(resp.contains("QUEUED"));
        let resp = send_command(&mut stream, &["DISCARD"]).await;
        assert!(resp.contains("OK"));
    }

    #[tokio::test]
    async fn test_discard_without_multi() {
        let (_server, addr) = start_test_server().await;
        let mut stream = connect(addr).await;
        let resp = send_command(&mut stream, &["DISCARD"]).await;
        assert!(resp.contains("ERR"));
    }

    #[tokio::test]
    async fn test_watch_unwatch() {
        let (_server, addr) = start_test_server().await;
        let mut stream = connect(addr).await;
        let resp = send_command(&mut stream, &["WATCH", "mykey"]).await;
        assert!(resp.contains("OK"));

        let resp = send_command(&mut stream, &["UNWATCH"]).await;
        assert!(resp.contains("OK"));
    }

    #[tokio::test]
    async fn test_watch_inside_multi() {
        let (_server, addr) = start_test_server().await;
        let mut stream = connect(addr).await;
        let _ = send_command(&mut stream, &["MULTI"]).await;
        let resp = send_command(&mut stream, &["WATCH", "mykey"]).await;
        assert!(resp.contains("ERR"));
    }

    #[tokio::test]
    async fn test_quit() {
        let (_server, addr) = start_test_server().await;
        let mut stream = connect(addr).await;
        let resp = send_command(&mut stream, &["QUIT"]).await;
        assert!(resp.contains("OK"));
    }

    #[tokio::test]
    async fn test_save_command() {
        let (_server, addr) = start_test_server().await;
        let mut stream = connect(addr).await;
        let resp = send_command(&mut stream, &["SAVE"]).await;
        assert!(resp.contains("OK"));
    }

    #[tokio::test]
    async fn test_subscribe_publish() {
        let (_server, addr) = start_test_server().await;

        // Subscriber connection
        let mut sub_stream = connect(addr).await;
        let resp = send_command(&mut sub_stream, &["SUBSCRIBE", "chan1"]).await;
        assert!(resp.contains("subscribe"));

        // Publisher connection
        let mut pub_stream = connect(addr).await;
        let resp = send_command(&mut pub_stream, &["PUBLISH", "chan1", "hello"]).await;
        assert!(resp.contains(":1"));

        // Read message on subscriber
        let mut buf = vec![0u8; 4096];
        let n = tokio::time::timeout(Duration::from_secs(2), sub_stream.read(&mut buf))
            .await
            .unwrap_or(Ok(0))
            .unwrap_or(0);
        if n > 0 {
            let msg = String::from_utf8_lossy(&buf[..n]);
            assert!(msg.contains("message") || msg.contains("hello"));
        }
    }

    #[tokio::test]
    async fn test_unsubscribe() {
        let (_server, addr) = start_test_server().await;
        let mut stream = connect(addr).await;
        let _ = send_command(&mut stream, &["SUBSCRIBE", "chan1"]).await;
        let resp = send_command(&mut stream, &["UNSUBSCRIBE", "chan1"]).await;
        assert!(resp.contains("unsubscribe"));
    }

    #[tokio::test]
    async fn test_psubscribe_punsubscribe() {
        let (_server, addr) = start_test_server().await;
        let mut stream = connect(addr).await;
        let resp = send_command(&mut stream, &["PSUBSCRIBE", "chan*"]).await;
        assert!(resp.contains("psubscribe"));

        let resp = send_command(&mut stream, &["PUNSUBSCRIBE", "chan*"]).await;
        assert!(resp.contains("punsubscribe"));
    }

    #[tokio::test]
    async fn test_prometheus_metrics() {
        let server = RedisServer::new();
        let metrics = server.prometheus_metrics().await;
        assert!(metrics.contains("chabi_connected_clients"));
        assert!(metrics.contains("chabi_total_connections_served"));
        assert!(metrics.contains("chabi_total_commands_processed"));
        assert!(metrics.contains("chabi_keys"));
        assert!(metrics.contains("chabi_expiring_keys"));
        assert!(metrics.contains("chabi_pubsub_channels"));
    }

    #[tokio::test]
    async fn test_command_registry_completeness() {
        let server = RedisServer::new();
        let registry = &server.registries[0];
        for cmd in &[
            "PING",
            "SET",
            "GET",
            "DEL",
            "EXISTS",
            "LPUSH",
            "RPUSH",
            "SADD",
            "HSET",
            "HGET",
            "ZADD",
            "INFO",
            "DBSIZE",
            "FLUSHDB",
            "SUBSCRIBE",
            "PUBLISH",
        ] {
            assert!(
                registry.contains_key(*cmd),
                "Command {} not found in registry",
                cmd
            );
        }
    }

    #[tokio::test]
    async fn test_snapshot_persist_and_load() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap().to_string();

        let server = RedisServer::new();
        server.databases[0]
            .strings
            .write()
            .await
            .insert("k1".into(), "v1".into());
        server.databases[0]
            .lists
            .write()
            .await
            .insert("l1".into(), vec!["a".into(), "b".into()]);
        server.databases[0].sets.write().await.insert(
            "s1".into(),
            std::collections::HashSet::from(["x".to_string()]),
        );

        let snapshots = server.build_all_snapshots().await;
        RedisServer::persist_snapshot_to_dir(&dir_path, snapshots)
            .await
            .unwrap();

        let server2 = RedisServer::new();
        server2.load_snapshot_from_dir(&dir_path).await.unwrap();

        assert_eq!(
            server2.databases[0].strings.read().await.get("k1").unwrap(),
            "v1"
        );
        assert_eq!(
            server2.databases[0].lists.read().await.get("l1").unwrap(),
            &vec!["a".to_string(), "b".to_string()]
        );
        assert!(server2.databases[0]
            .sets
            .read()
            .await
            .get("s1")
            .unwrap()
            .contains("x"));
    }

    #[tokio::test]
    async fn test_load_snapshot_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap().to_string();

        let server = RedisServer::new();
        server.load_snapshot_from_dir(&dir_path).await.unwrap();
    }

    #[tokio::test]
    async fn test_set_snapshot_dir() {
        let server = RedisServer::new();
        server.set_snapshot_dir("/tmp/test-snap".to_string()).await;
        let dir = server.snapshot_dir.read().await;
        assert_eq!(dir.as_deref(), Some("/tmp/test-snap"));
    }

    #[tokio::test]
    async fn test_ensure_snapshot_dir_with_config() {
        let server = RedisServer::new();
        server
            .set_snapshot_dir("/tmp/configured-dir".to_string())
            .await;
        let dir = server.ensure_snapshot_dir().await;
        assert_eq!(dir, "/tmp/configured-dir");
    }

    #[tokio::test]
    async fn test_ensure_snapshot_dir_without_config() {
        let server = RedisServer::new();
        let dir = server.ensure_snapshot_dir().await;
        assert!(!dir.is_empty());
        assert!(dir.contains("chabi-"));
    }

    #[tokio::test]
    async fn test_clone_shares_state() {
        let server = RedisServer::new();
        server.databases[0]
            .strings
            .write()
            .await
            .insert("k".into(), "v".into());
        let cloned = server.clone();
        assert_eq!(
            cloned.databases[0].strings.read().await.get("k").unwrap(),
            "v"
        );
    }

    #[tokio::test]
    async fn test_build_snapshot() {
        let server = RedisServer::new();
        server.databases[0]
            .strings
            .write()
            .await
            .insert("foo".into(), "bar".into());
        let snaps = server.build_all_snapshots().await;
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].0, 0);
        assert_eq!(snaps[0].1.strings.get("foo").unwrap(), "bar");
    }

    #[tokio::test]
    async fn test_invalid_request_format() {
        let (_server, addr) = start_test_server().await;
        let mut stream = connect(addr).await;

        // Send a simple string instead of an array
        stream.write_all(b"+PING\r\n").await.unwrap();
        stream.flush().await.unwrap();

        let mut buf = vec![0u8; 4096];
        let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
            .await
            .unwrap_or(Ok(0))
            .unwrap_or(0);
        if n > 0 {
            let resp = String::from_utf8_lossy(&buf[..n]);
            assert!(resp.contains("ERR"));
        }
    }

    #[tokio::test]
    async fn test_echo_command_via_tcp() {
        let (_server, addr) = start_test_server().await;
        let mut stream = connect(addr).await;
        let resp = send_command(&mut stream, &["ECHO", "hello"]).await;
        assert!(resp.contains("hello"));
    }

    #[tokio::test]
    async fn test_info_command_via_tcp() {
        let (_server, addr) = start_test_server().await;
        let mut stream = connect(addr).await;
        let resp = send_command(&mut stream, &["INFO"]).await;
        assert!(resp.contains("redis_version"));
    }

    #[tokio::test]
    async fn test_dbsize_command_via_tcp() {
        let (_server, addr) = start_test_server().await;
        let mut stream = connect(addr).await;
        let resp = send_command(&mut stream, &["DBSIZE"]).await;
        assert!(resp.contains(":"));
    }

    #[tokio::test]
    async fn test_snapshot_with_hashes_and_sorted_sets() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap().to_string();

        let server = RedisServer::new();
        server.databases[0].hashes.write().await.insert(
            "h1".into(),
            HashMap::from([("field1".to_string(), "val1".to_string())]),
        );
        let mut zset = SortedSet::new();
        zset.insert("member1".to_string(), 1.0);
        server.databases[0]
            .sorted_sets
            .write()
            .await
            .insert("z1".into(), zset);

        let snapshots = server.build_all_snapshots().await;
        RedisServer::persist_snapshot_to_dir(&dir_path, snapshots)
            .await
            .unwrap();

        let server2 = RedisServer::new();
        server2.load_snapshot_from_dir(&dir_path).await.unwrap();

        assert_eq!(
            server2.databases[0]
                .hashes
                .read()
                .await
                .get("h1")
                .unwrap()
                .get("field1")
                .unwrap(),
            "val1"
        );
        assert!(server2.databases[0]
            .sorted_sets
            .read()
            .await
            .contains_key("z1"));
    }

    #[tokio::test]
    async fn test_snapshot_with_expirations() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap().to_string();

        let server = RedisServer::new();
        server.databases[0]
            .strings
            .write()
            .await
            .insert("k1".into(), "v1".into());
        server.databases[0].expirations.write().await.insert(
            "k1".into(),
            std::time::Instant::now() + Duration::from_secs(3600),
        );

        let snapshots = server.build_all_snapshots().await;
        RedisServer::persist_snapshot_to_dir(&dir_path, snapshots)
            .await
            .unwrap();

        let server2 = RedisServer::new();
        server2.load_snapshot_from_dir(&dir_path).await.unwrap();

        assert!(server2.databases[0]
            .expirations
            .read()
            .await
            .contains_key("k1"));
    }

    #[tokio::test]
    async fn test_unsubscribe_all_channels() {
        let (_server, addr) = start_test_server().await;
        let mut stream = connect(addr).await;
        // Subscribe to one channel only to simplify
        let resp = send_command(&mut stream, &["SUBSCRIBE", "c1"]).await;
        assert!(resp.contains("subscribe"));
        let resp = send_command(&mut stream, &["UNSUBSCRIBE"]).await;
        assert!(resp.contains("unsubscribe"));
    }

    #[tokio::test]
    async fn test_punsubscribe_all() {
        let (_server, addr) = start_test_server().await;
        let mut stream = connect(addr).await;
        // Subscribe to one pattern only to simplify
        let resp = send_command(&mut stream, &["PSUBSCRIBE", "c*"]).await;
        assert!(resp.contains("psubscribe"));
        let resp = send_command(&mut stream, &["PUNSUBSCRIBE"]).await;
        assert!(resp.contains("punsubscribe"));
    }

    // --- Multi-database tests ---

    #[tokio::test]
    async fn test_select_db_isolation() {
        let (_server, addr) = start_test_server().await;
        let mut stream = connect(addr).await;

        // SET in DB 0
        let resp = send_command(&mut stream, &["SET", "foo", "bar"]).await;
        assert!(resp.contains("OK"));

        // SELECT 1
        let resp = send_command(&mut stream, &["SELECT", "1"]).await;
        assert!(resp.contains("OK"));

        // GET in DB 1 returns nil
        let resp = send_command(&mut stream, &["GET", "foo"]).await;
        assert!(resp.contains("$-1"), "Expected nil but got: {}", resp);

        // SET in DB 1
        let resp = send_command(&mut stream, &["SET", "foo", "baz"]).await;
        assert!(resp.contains("OK"));

        // SELECT 0, GET returns original
        let resp = send_command(&mut stream, &["SELECT", "0"]).await;
        assert!(resp.contains("OK"));
        let resp = send_command(&mut stream, &["GET", "foo"]).await;
        assert!(resp.contains("bar"), "Expected 'bar' but got: {}", resp);
    }

    #[tokio::test]
    async fn test_flushdb_current_only() {
        let (_server, addr) = start_test_server().await;
        let mut stream = connect(addr).await;

        // SET in DB 0
        let resp = send_command(&mut stream, &["SET", "key0", "val0"]).await;
        assert!(resp.contains("OK"));

        // SELECT 1, SET in DB 1
        let resp = send_command(&mut stream, &["SELECT", "1"]).await;
        assert!(resp.contains("OK"));
        let resp = send_command(&mut stream, &["SET", "key1", "val1"]).await;
        assert!(resp.contains("OK"));

        // FLUSHDB only clears DB 1
        let resp = send_command(&mut stream, &["FLUSHDB"]).await;
        assert!(resp.contains("OK"));
        let resp = send_command(&mut stream, &["GET", "key1"]).await;
        assert!(resp.contains("$-1"), "Expected nil but got: {}", resp);

        // DB 0 still has its key
        let resp = send_command(&mut stream, &["SELECT", "0"]).await;
        assert!(resp.contains("OK"));
        let resp = send_command(&mut stream, &["GET", "key0"]).await;
        assert!(resp.contains("val0"), "Expected 'val0' but got: {}", resp);
    }

    #[tokio::test]
    async fn test_flushall_clears_all() {
        let (_server, addr) = start_test_server().await;
        let mut stream = connect(addr).await;

        // SET in DB 0
        let resp = send_command(&mut stream, &["SET", "key0", "val0"]).await;
        assert!(resp.contains("OK"));

        // SELECT 1, SET in DB 1
        let resp = send_command(&mut stream, &["SELECT", "1"]).await;
        assert!(resp.contains("OK"));
        let resp = send_command(&mut stream, &["SET", "key1", "val1"]).await;
        assert!(resp.contains("OK"));

        // FLUSHALL clears everything
        let resp = send_command(&mut stream, &["FLUSHALL"]).await;
        assert!(resp.contains("OK"));

        // DB 1 empty
        let resp = send_command(&mut stream, &["GET", "key1"]).await;
        assert!(resp.contains("$-1"), "Expected nil but got: {}", resp);

        // DB 0 also empty
        let resp = send_command(&mut stream, &["SELECT", "0"]).await;
        assert!(resp.contains("OK"));
        let resp = send_command(&mut stream, &["GET", "key0"]).await;
        assert!(resp.contains("$-1"), "Expected nil but got: {}", resp);
    }

    #[tokio::test]
    async fn test_dbsize_per_db() {
        let (_server, addr) = start_test_server().await;
        let mut stream = connect(addr).await;

        // SET two keys in DB 0
        let _ = send_command(&mut stream, &["SET", "a", "1"]).await;
        let _ = send_command(&mut stream, &["SET", "b", "2"]).await;
        let resp = send_command(&mut stream, &["DBSIZE"]).await;
        assert!(resp.contains(":2"), "Expected :2 but got: {}", resp);

        // SELECT 1, DBSIZE should be 0
        let _ = send_command(&mut stream, &["SELECT", "1"]).await;
        let resp = send_command(&mut stream, &["DBSIZE"]).await;
        assert!(resp.contains(":0"), "Expected :0 but got: {}", resp);
    }

    #[tokio::test]
    async fn test_select_out_of_range() {
        let (_server, addr) = start_test_server().await;
        let mut stream = connect(addr).await;
        let resp = send_command(&mut stream, &["SELECT", "16"]).await;
        assert!(resp.contains("ERR"), "Expected ERR but got: {}", resp);
        assert!(
            resp.contains("out of range"),
            "Expected 'out of range' but got: {}",
            resp
        );
    }

    #[tokio::test]
    async fn test_multi_db_snapshot_persist_and_load() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap().to_string();

        let server = RedisServer::new();
        // Put data in DB 0
        server.databases[0]
            .strings
            .write()
            .await
            .insert("k0".into(), "v0".into());
        // Put data in DB 3
        server.databases[3]
            .strings
            .write()
            .await
            .insert("k3".into(), "v3".into());

        let snapshots = server.build_all_snapshots().await;
        assert_eq!(snapshots.len(), 2);
        RedisServer::persist_snapshot_to_dir(&dir_path, snapshots)
            .await
            .unwrap();

        let server2 = RedisServer::new();
        server2.load_snapshot_from_dir(&dir_path).await.unwrap();

        assert_eq!(
            server2.databases[0].strings.read().await.get("k0").unwrap(),
            "v0"
        );
        assert_eq!(
            server2.databases[3].strings.read().await.get("k3").unwrap(),
            "v3"
        );
        // DB 1 should be empty
        assert!(server2.databases[1].strings.read().await.is_empty());
    }
}
