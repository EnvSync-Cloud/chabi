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

// redb table definitions
const T_STRINGS: TableDefinition<&str, &[u8]> = TableDefinition::new("strings");
const T_LISTS: TableDefinition<&str, &[u8]> = TableDefinition::new("lists");
const T_SETS: TableDefinition<&str, &[u8]> = TableDefinition::new("sets");
const T_HASHES: TableDefinition<&str, &[u8]> = TableDefinition::new("hashes");
const T_SORTED_SETS: TableDefinition<&str, &[u8]> = TableDefinition::new("sorted_sets");
const T_HLL: TableDefinition<&str, &[u8]> = TableDefinition::new("hll");
const T_EXPIRATIONS: TableDefinition<&str, u64> = TableDefinition::new("expirations");

static NEXT_CONN_ID: AtomicUsize = AtomicUsize::new(1);

pub struct RedisServer {
    command_registry: Arc<HashMap<String, Box<dyn CommandHandler + Send + Sync>>>,
    pubsub_channels: Arc<PubSubChannels>,
    store: DataStore,
    snapshot_dir: Arc<RwLock<Option<String>>>,
    total_connections_served: Arc<AtomicUsize>,
    total_commands_processed: Arc<AtomicUsize>,
    connected_clients: Arc<AtomicUsize>,
}

impl RedisServer {
    pub fn new() -> Self {
        let mut command_registry = HashMap::new();

        let store = DataStore::new();
        let snapshot_dir: Arc<RwLock<Option<String>>> = Arc::new(RwLock::new(None));

        // channel -> Vec<(conn_id, Sender<(channel, message)>)>
        let pubsub_channels: Arc<PubSubChannels> = Arc::new(std::sync::RwLock::new(HashMap::new()));

        // Helper macro to reduce boilerplate
        macro_rules! reg {
            ($name:expr, $cmd:expr) => {
                command_registry.insert(
                    $name.to_string(),
                    Box::new($cmd) as Box<dyn CommandHandler + Send + Sync>,
                );
            };
        }

        // --- Connection commands ---
        reg!("PING", chabi_core::commands::connection::PingCommand::new());
        reg!("ECHO", chabi_core::commands::connection::EchoCommand::new());
        reg!(
            "SELECT",
            chabi_core::commands::connection::SelectCommand::new()
        );
        reg!("QUIT", chabi_core::commands::connection::QuitCommand::new());
        reg!(
            "RESET",
            chabi_core::commands::connection::ResetCommand::new()
        );
        reg!("AUTH", chabi_core::commands::connection::AuthCommand::new());
        reg!(
            "CLIENT",
            chabi_core::commands::connection::ClientCommand::new()
        );
        reg!(
            "HELLO",
            chabi_core::commands::connection::HelloCommand::new()
        );

        // --- String commands ---
        reg!(
            "SET",
            chabi_core::commands::string::SetCommand::new(store.clone())
        );
        reg!(
            "GET",
            chabi_core::commands::string::GetCommand::new(store.clone())
        );
        reg!(
            "DEL",
            chabi_core::commands::string::DelCommand::new(store.clone())
        );
        reg!(
            "EXISTS",
            chabi_core::commands::string::ExistsCommand::new(store.clone())
        );
        reg!(
            "APPEND",
            chabi_core::commands::string::AppendCommand::new(store.clone())
        );
        reg!(
            "STRLEN",
            chabi_core::commands::string::StrlenCommand::new(store.clone())
        );
        reg!(
            "INCR",
            chabi_core::commands::string::IncrCommand::new(store.clone())
        );
        reg!(
            "DECR",
            chabi_core::commands::string::DecrCommand::new(store.clone())
        );
        reg!(
            "INCRBY",
            chabi_core::commands::string::IncrByCommand::new(store.clone())
        );
        reg!(
            "DECRBY",
            chabi_core::commands::string::DecrByCommand::new(store.clone())
        );
        reg!(
            "INCRBYFLOAT",
            chabi_core::commands::string::IncrByFloatCommand::new(store.clone())
        );
        reg!(
            "MGET",
            chabi_core::commands::string::MGetCommand::new(store.clone())
        );
        reg!(
            "MSET",
            chabi_core::commands::string::MSetCommand::new(store.clone())
        );
        reg!(
            "MSETNX",
            chabi_core::commands::string::MSetNxCommand::new(store.clone())
        );
        reg!(
            "SETNX",
            chabi_core::commands::string::SetNxCommand::new(store.clone())
        );
        reg!(
            "SETEX",
            chabi_core::commands::string::SetExCommand::new(store.clone())
        );
        reg!(
            "PSETEX",
            chabi_core::commands::string::PSetExCommand::new(store.clone())
        );
        reg!(
            "GETRANGE",
            chabi_core::commands::string::GetRangeCommand::new(store.clone())
        );
        reg!(
            "SETRANGE",
            chabi_core::commands::string::SetRangeCommand::new(store.clone())
        );
        reg!(
            "GETDEL",
            chabi_core::commands::string::GetDelCommand::new(store.clone())
        );
        reg!(
            "GETEX",
            chabi_core::commands::string::GetExCommand::new(store.clone())
        );

        // --- Key commands ---
        reg!(
            "KEYS",
            chabi_core::commands::key::KeysCommand::new(store.clone())
        );
        reg!(
            "TTL",
            chabi_core::commands::key::TTLCommand::new(store.clone())
        );
        reg!(
            "PTTL",
            chabi_core::commands::key::PTTLCommand::new(store.clone())
        );
        reg!(
            "EXPIRE",
            chabi_core::commands::key::ExpireCommand::new(store.clone())
        );
        reg!(
            "PEXPIRE",
            chabi_core::commands::key::PExpireCommand::new(store.clone())
        );
        reg!(
            "EXPIREAT",
            chabi_core::commands::key::ExpireAtCommand::new(store.clone())
        );
        reg!(
            "PEXPIREAT",
            chabi_core::commands::key::PExpireAtCommand::new(store.clone())
        );
        reg!(
            "RENAME",
            chabi_core::commands::key::RenameCommand::new(store.clone())
        );
        reg!(
            "RENAMENX",
            chabi_core::commands::key::RenameNxCommand::new(store.clone())
        );
        reg!(
            "TYPE",
            chabi_core::commands::key::TypeCommand::new(store.clone())
        );
        reg!(
            "PERSIST",
            chabi_core::commands::key::PersistCommand::new(store.clone())
        );
        reg!(
            "UNLINK",
            chabi_core::commands::key::UnlinkCommand::new(store.clone())
        );
        reg!(
            "RANDOMKEY",
            chabi_core::commands::key::RandomKeyCommand::new(store.clone())
        );
        reg!(
            "SCAN",
            chabi_core::commands::key::ScanCommand::new(store.clone())
        );
        reg!(
            "COPY",
            chabi_core::commands::key::CopyCommand::new(store.clone())
        );
        reg!(
            "TOUCH",
            chabi_core::commands::key::TouchCommand::new(store.clone())
        );
        reg!("OBJECT", chabi_core::commands::key::ObjectCommand::new());

        // --- List commands ---
        reg!(
            "LPUSH",
            chabi_core::commands::list::LPushCommand::new(store.clone())
        );
        reg!(
            "RPUSH",
            chabi_core::commands::list::RPushCommand::new(store.clone())
        );
        reg!(
            "LPOP",
            chabi_core::commands::list::LPopCommand::new(store.clone())
        );
        reg!(
            "RPOP",
            chabi_core::commands::list::RPopCommand::new(store.clone())
        );
        reg!(
            "LRANGE",
            chabi_core::commands::list::LRangeCommand::new(store.clone())
        );
        reg!(
            "LLEN",
            chabi_core::commands::list::LLenCommand::new(store.clone())
        );
        reg!(
            "LINDEX",
            chabi_core::commands::list::LIndexCommand::new(store.clone())
        );
        reg!(
            "LSET",
            chabi_core::commands::list::LSetCommand::new(store.clone())
        );
        reg!(
            "LTRIM",
            chabi_core::commands::list::LTrimCommand::new(store.clone())
        );
        reg!(
            "LINSERT",
            chabi_core::commands::list::LInsertCommand::new(store.clone())
        );
        reg!(
            "LREM",
            chabi_core::commands::list::LRemCommand::new(store.clone())
        );
        reg!(
            "LPOS",
            chabi_core::commands::list::LPosCommand::new(store.clone())
        );
        reg!(
            "LPUSHX",
            chabi_core::commands::list::LPushXCommand::new(store.clone())
        );
        reg!(
            "RPUSHX",
            chabi_core::commands::list::RPushXCommand::new(store.clone())
        );
        reg!(
            "LMOVE",
            chabi_core::commands::list::LMoveCommand::new(store.clone())
        );

        // --- Set commands ---
        reg!(
            "SADD",
            chabi_core::commands::set::SAddCommand::new(store.clone())
        );
        reg!(
            "SMEMBERS",
            chabi_core::commands::set::SMembersCommand::new(store.clone())
        );
        reg!(
            "SISMEMBER",
            chabi_core::commands::set::SIsMemberCommand::new(store.clone())
        );
        reg!(
            "SCARD",
            chabi_core::commands::set::SCardCommand::new(store.clone())
        );
        reg!(
            "SREM",
            chabi_core::commands::set::SRemCommand::new(store.clone())
        );
        reg!(
            "SPOP",
            chabi_core::commands::set::SPopCommand::new(store.clone())
        );
        reg!(
            "SRANDMEMBER",
            chabi_core::commands::set::SRandMemberCommand::new(store.clone())
        );
        reg!(
            "SMOVE",
            chabi_core::commands::set::SMoveCommand::new(store.clone())
        );
        reg!(
            "SINTER",
            chabi_core::commands::set::SInterCommand::new(store.clone())
        );
        reg!(
            "SUNION",
            chabi_core::commands::set::SUnionCommand::new(store.clone())
        );
        reg!(
            "SDIFF",
            chabi_core::commands::set::SDiffCommand::new(store.clone())
        );
        reg!(
            "SINTERSTORE",
            chabi_core::commands::set::SInterStoreCommand::new(store.clone())
        );
        reg!(
            "SUNIONSTORE",
            chabi_core::commands::set::SUnionStoreCommand::new(store.clone())
        );
        reg!(
            "SDIFFSTORE",
            chabi_core::commands::set::SDiffStoreCommand::new(store.clone())
        );
        reg!(
            "SSCAN",
            chabi_core::commands::set::SScanCommand::new(store.clone())
        );
        reg!(
            "SMISMEMBER",
            chabi_core::commands::set::SMisMemberCommand::new(store.clone())
        );
        reg!(
            "SINTERCARD",
            chabi_core::commands::set::SInterCardCommand::new(store.clone())
        );

        // --- Hash commands ---
        reg!(
            "HSET",
            chabi_core::commands::hash::HSetCommand::new(store.clone())
        );
        // HMSET is an alias for HSET
        reg!(
            "HMSET",
            chabi_core::commands::hash::HSetCommand::new(store.clone())
        );
        reg!(
            "HGET",
            chabi_core::commands::hash::HGetCommand::new(store.clone())
        );
        reg!(
            "HGETALL",
            chabi_core::commands::hash::HGetAllCommand::new(store.clone())
        );
        reg!(
            "HEXISTS",
            chabi_core::commands::hash::HExistsCommand::new(store.clone())
        );
        reg!(
            "HDEL",
            chabi_core::commands::hash::HDelCommand::new(store.clone())
        );
        reg!(
            "HLEN",
            chabi_core::commands::hash::HLenCommand::new(store.clone())
        );
        reg!(
            "HKEYS",
            chabi_core::commands::hash::HKeysCommand::new(store.clone())
        );
        reg!(
            "HVALS",
            chabi_core::commands::hash::HValsCommand::new(store.clone())
        );
        reg!(
            "HMGET",
            chabi_core::commands::hash::HMGetCommand::new(store.clone())
        );
        reg!(
            "HINCRBY",
            chabi_core::commands::hash::HIncrByCommand::new(store.clone())
        );
        reg!(
            "HINCRBYFLOAT",
            chabi_core::commands::hash::HIncrByFloatCommand::new(store.clone())
        );
        reg!(
            "HSETNX",
            chabi_core::commands::hash::HSetNxCommand::new(store.clone())
        );
        reg!(
            "HSTRLEN",
            chabi_core::commands::hash::HStrLenCommand::new(store.clone())
        );
        reg!(
            "HSCAN",
            chabi_core::commands::hash::HScanCommand::new(store.clone())
        );
        reg!(
            "HRANDFIELD",
            chabi_core::commands::hash::HRandFieldCommand::new(store.clone())
        );

        // --- Sorted set commands ---
        reg!(
            "ZADD",
            chabi_core::commands::sorted_set::ZAddCommand::new(store.clone())
        );
        reg!(
            "ZREM",
            chabi_core::commands::sorted_set::ZRemCommand::new(store.clone())
        );
        reg!(
            "ZSCORE",
            chabi_core::commands::sorted_set::ZScoreCommand::new(store.clone())
        );
        reg!(
            "ZCARD",
            chabi_core::commands::sorted_set::ZCardCommand::new(store.clone())
        );
        reg!(
            "ZCOUNT",
            chabi_core::commands::sorted_set::ZCountCommand::new(store.clone())
        );
        reg!(
            "ZRANGE",
            chabi_core::commands::sorted_set::ZRangeCommand::new(store.clone())
        );
        reg!(
            "ZREVRANGE",
            chabi_core::commands::sorted_set::ZRevRangeCommand::new(store.clone())
        );
        reg!(
            "ZRANGEBYSCORE",
            chabi_core::commands::sorted_set::ZRangeByScoreCommand::new(store.clone())
        );
        reg!(
            "ZREVRANGEBYSCORE",
            chabi_core::commands::sorted_set::ZRevRangeByScoreCommand::new(store.clone())
        );
        reg!(
            "ZRANK",
            chabi_core::commands::sorted_set::ZRankCommand::new(store.clone())
        );
        reg!(
            "ZREVRANK",
            chabi_core::commands::sorted_set::ZRevRankCommand::new(store.clone())
        );
        reg!(
            "ZINCRBY",
            chabi_core::commands::sorted_set::ZIncrByCommand::new(store.clone())
        );
        reg!(
            "ZPOPMIN",
            chabi_core::commands::sorted_set::ZPopMinCommand::new(store.clone())
        );
        reg!(
            "ZPOPMAX",
            chabi_core::commands::sorted_set::ZPopMaxCommand::new(store.clone())
        );
        reg!(
            "ZRANDMEMBER",
            chabi_core::commands::sorted_set::ZRandMemberCommand::new(store.clone())
        );
        reg!(
            "ZMSCORE",
            chabi_core::commands::sorted_set::ZMScoreCommand::new(store.clone())
        );
        reg!(
            "ZUNIONSTORE",
            chabi_core::commands::sorted_set::ZUnionStoreCommand::new(store.clone())
        );
        reg!(
            "ZINTERSTORE",
            chabi_core::commands::sorted_set::ZInterStoreCommand::new(store.clone())
        );
        reg!(
            "ZSCAN",
            chabi_core::commands::sorted_set::ZScanCommand::new(store.clone())
        );

        // --- Bitmap commands ---
        reg!(
            "SETBIT",
            chabi_core::commands::bitmap::SetBitCommand::new(store.clone())
        );
        reg!(
            "GETBIT",
            chabi_core::commands::bitmap::GetBitCommand::new(store.clone())
        );
        reg!(
            "BITCOUNT",
            chabi_core::commands::bitmap::BitCountCommand::new(store.clone())
        );
        reg!(
            "BITPOS",
            chabi_core::commands::bitmap::BitPosCommand::new(store.clone())
        );

        // --- HyperLogLog commands ---
        reg!(
            "PFADD",
            chabi_core::commands::hyperloglog::PfAddCommand::new(store.clone())
        );
        reg!(
            "PFCOUNT",
            chabi_core::commands::hyperloglog::PfCountCommand::new(store.clone())
        );
        reg!(
            "PFMERGE",
            chabi_core::commands::hyperloglog::PfMergeCommand::new(store.clone())
        );

        // --- Server commands ---
        reg!(
            "INFO",
            chabi_core::commands::server::InfoCommand::new(store.clone())
        );
        reg!("SAVE", chabi_core::commands::server::SaveCommand::new());
        reg!(
            "DBSIZE",
            chabi_core::commands::server::DbSizeCommand::new(store.clone())
        );
        reg!(
            "FLUSHDB",
            chabi_core::commands::server::FlushDbCommand::new(store.clone())
        );
        reg!(
            "FLUSHALL",
            chabi_core::commands::server::FlushDbCommand::new(store.clone())
        );
        reg!("CONFIG", chabi_core::commands::server::ConfigCommand::new());
        reg!(
            "COMMAND",
            chabi_core::commands::server::CommandCommand::new()
        );
        reg!("TIME", chabi_core::commands::server::TimeCommand::new());
        reg!("BGSAVE", chabi_core::commands::server::BgSaveCommand::new());

        // --- PubSub commands ---
        reg!(
            "PUBLISH",
            chabi_core::commands::pubsub::PublishCommand::new(Arc::clone(&pubsub_channels))
        );
        reg!(
            "SUBSCRIBE",
            chabi_core::commands::pubsub::SubscribeCommand::new(Arc::clone(&pubsub_channels))
        );
        reg!(
            "UNSUBSCRIBE",
            chabi_core::commands::pubsub::UnsubscribeCommand::new(Arc::clone(&pubsub_channels))
        );
        reg!(
            "PUBSUB",
            chabi_core::commands::pubsub::PubSubCommand::new(Arc::clone(&pubsub_channels))
        );

        RedisServer {
            command_registry: Arc::new(command_registry),
            pubsub_channels: Arc::clone(&pubsub_channels),
            store,
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
        let store = self.store.clone();
        tokio::spawn(async move {
            loop {
                sleep(interval).await;
                let snapshot = store.build_snapshot().await;

                if let Err(e) = Self::persist_snapshot_to_dir(&dir, snapshot).await {
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

        let string_count = self.store.strings.read().await.len();
        let list_count = self.store.lists.read().await.len();
        let set_count = self.store.sets.read().await.len();
        let hash_count = self.store.hashes.read().await.len();
        let sorted_set_count = self.store.sorted_sets.read().await.len();
        let expiration_count = self.store.expirations.read().await.len();
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

    pub async fn build_snapshot(&self) -> Snapshot {
        self.store.build_snapshot().await
    }

    pub async fn load_snapshot_from_dir(&self, dir: &str) -> Result<()> {
        let db_path = Self::db_file_path(dir);
        if tokio::fs::metadata(&db_path).await.is_err() {
            tracing::info!("snapshot database not found: {}", db_path.display());
            return Ok(());
        }
        // Use blocking thread for redb IO
        let snapshot = tokio::task::spawn_blocking({
            let db_path = db_path.clone();
            move || -> std::result::Result<Snapshot, BoxedError> {
                let db = Database::open(db_path)?;
                let rtxn = db.begin_read()?;

                // Strings
                let mut strings = HashMap::new();
                if let Ok(table) = rtxn.open_table(T_STRINGS) {
                    for item in table.iter()? {
                        let (k, v) = item?;
                        let key = k.value().to_string();
                        let val = String::from_utf8(v.value().to_vec())?;
                        strings.insert(key, val);
                    }
                }

                // Lists
                let mut lists = HashMap::new();
                if let Ok(table) = rtxn.open_table(T_LISTS) {
                    for item in table.iter()? {
                        let (k, v) = item?;
                        let key = k.value().to_string();
                        let val: Vec<String> = bincode::serde::decode_from_slice(
                            v.value(),
                            bincode::config::standard(),
                        )?
                        .0;
                        lists.insert(key, val);
                    }
                }

                // Sets
                let mut sets = HashMap::new();
                if let Ok(table) = rtxn.open_table(T_SETS) {
                    for item in table.iter()? {
                        let (k, v) = item?;
                        let key = k.value().to_string();
                        let val: HashSet<String> = bincode::serde::decode_from_slice(
                            v.value(),
                            bincode::config::standard(),
                        )?
                        .0;
                        sets.insert(key, val);
                    }
                }

                // Hashes
                let mut hashes = HashMap::new();
                if let Ok(table) = rtxn.open_table(T_HASHES) {
                    for item in table.iter()? {
                        let (k, v) = item?;
                        let key = k.value().to_string();
                        let val: HashMap<String, String> = bincode::serde::decode_from_slice(
                            v.value(),
                            bincode::config::standard(),
                        )?
                        .0;
                        hashes.insert(key, val);
                    }
                }

                // Sorted Sets
                let mut sorted_sets = HashMap::new();
                if let Ok(table) = rtxn.open_table(T_SORTED_SETS) {
                    for item in table.iter()? {
                        let (k, v) = item?;
                        let key = k.value().to_string();
                        let val: SortedSet = bincode::serde::decode_from_slice(
                            v.value(),
                            bincode::config::standard(),
                        )?
                        .0;
                        sorted_sets.insert(key, val);
                    }
                }

                // HyperLogLog
                let mut hll = HashMap::new();
                if let Ok(table) = rtxn.open_table(T_HLL) {
                    for item in table.iter()? {
                        let (k, v) = item?;
                        let key = k.value().to_string();
                        let val = v.value().to_vec();
                        hll.insert(key, val);
                    }
                }

                // Expirations
                let mut expirations_epoch_secs = HashMap::new();
                if let Ok(table) = rtxn.open_table(T_EXPIRATIONS) {
                    for item in table.iter()? {
                        let (k, v) = item?;
                        let key = k.value().to_string();
                        let val = v.value();
                        expirations_epoch_secs.insert(key, val);
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
        })
        .await
        .map_err(|e| -> BoxedError { Box::new(e) })??;

        tracing::info!(
            "loaded snapshot from {}/chabi.kdb (strings={}, lists={}, sets={}, hashes={}, sorted_sets={}, hll={}, expirations={})",
            dir,
            snapshot.strings.len(),
            snapshot.lists.len(),
            snapshot.sets.len(),
            snapshot.hashes.len(),
            snapshot.sorted_sets.len(),
            snapshot.hll.len(),
            snapshot.expirations_epoch_secs.len(),
        );

        self.store.restore_from_snapshot(snapshot).await;

        Ok(())
    }

    async fn persist_snapshot_to_dir(dir: &str, snapshot: Snapshot) -> Result<()> {
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
                {
                    // Strings
                    let mut table = write_txn.open_table(T_STRINGS)?;
                    for (k, v) in snapshot.strings.iter() {
                        table.insert(k.as_str(), v.as_bytes())?;
                    }
                }
                {
                    // Lists
                    let mut table = write_txn.open_table(T_LISTS)?;
                    for (k, v) in snapshot.lists.iter() {
                        let bytes = bincode::serde::encode_to_vec(v, bincode::config::standard())?;
                        table.insert(k.as_str(), bytes.as_slice())?;
                    }
                }
                {
                    // Sets
                    let mut table = write_txn.open_table(T_SETS)?;
                    for (k, v) in snapshot.sets.iter() {
                        let bytes = bincode::serde::encode_to_vec(v, bincode::config::standard())?;
                        table.insert(k.as_str(), bytes.as_slice())?;
                    }
                }
                {
                    // Hashes
                    let mut table = write_txn.open_table(T_HASHES)?;
                    for (k, v) in snapshot.hashes.iter() {
                        let bytes = bincode::serde::encode_to_vec(v, bincode::config::standard())?;
                        table.insert(k.as_str(), bytes.as_slice())?;
                    }
                }
                {
                    // Sorted Sets
                    let mut table = write_txn.open_table(T_SORTED_SETS)?;
                    for (k, v) in snapshot.sorted_sets.iter() {
                        let bytes = bincode::serde::encode_to_vec(v, bincode::config::standard())?;
                        table.insert(k.as_str(), bytes.as_slice())?;
                    }
                }
                {
                    // HyperLogLog
                    let mut table = write_txn.open_table(T_HLL)?;
                    for (k, v) in snapshot.hll.iter() {
                        table.insert(k.as_str(), v.as_slice())?;
                    }
                }
                {
                    // Expirations
                    let mut table = write_txn.open_table(T_EXPIRATIONS)?;
                    for (k, ts) in snapshot.expirations_epoch_secs.iter() {
                        table.insert(k.as_str(), *ts)?;
                    }
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

        let registry = Arc::clone(&self.command_registry);
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
                                                    let mut results = Vec::with_capacity(queued.len());
                                                    for (cmd, args) in queued {
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
                                        "SAVE" => {
                                            // Perform a synchronous snapshot dump to the configured directory
                                            let snapshot = self.build_snapshot().await;
                                            let dir = self.ensure_snapshot_dir().await;
                                            match Self::persist_snapshot_to_dir(&dir, snapshot).await {
                                                Ok(_) => {
                                                    send_resp!(RespValue::SimpleString("OK".to_string()));
                                                }
                                                Err(e) => {
                                                    send_resp!(RespValue::Error(format!("ERR snapshot failed: {}", e)));
                                                }
                                            }
                                        }
                                        _ => {
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

impl Clone for RedisServer {
    fn clone(&self) -> Self {
        RedisServer {
            command_registry: Arc::clone(&self.command_registry),
            pubsub_channels: Arc::clone(&self.pubsub_channels),
            store: self.store.clone(),
            snapshot_dir: Arc::clone(&self.snapshot_dir),
            total_connections_served: Arc::clone(&self.total_connections_served),
            total_commands_processed: Arc::clone(&self.total_commands_processed),
            connected_clients: Arc::clone(&self.connected_clients),
        }
    }
}
