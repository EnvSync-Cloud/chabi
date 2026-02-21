use crate::commands::CommandHandler;
use crate::resp::RespValue;
use crate::storage::DataStore;
use crate::Result;
use async_trait::async_trait;

fn extract_string(val: &RespValue) -> Option<String> {
    match val {
        RespValue::BulkString(Some(bytes)) => Some(String::from_utf8_lossy(bytes).to_string()),
        _ => None,
    }
}

// --- INFO ---

#[derive(Clone)]
pub struct InfoCommand {
    store: DataStore,
}

impl InfoCommand {
    pub fn new(store: DataStore) -> Self {
        InfoCommand { store }
    }
}

#[async_trait]
impl CommandHandler for InfoCommand {
    async fn execute(&self, _args: Vec<RespValue>) -> Result<RespValue> {
        let string_count = self.store.strings.read().await.len();
        let hash_count = self.store.hashes.read().await.len();
        let list_count = self.store.lists.read().await.len();
        let set_count = self.store.sets.read().await.len();
        let total_keys = string_count + hash_count + list_count + set_count;
        let info = format!(
            "# Server\r\nredis_version:7.0.0\r\nredis_mode:standalone\r\nprocess_id:{}\r\n# Keyspace\r\ndb0:keys={},expires=0,avg_ttl=0\r\n",
            std::process::id(),
            total_keys
        );
        Ok(RespValue::BulkString(Some(info.as_bytes().to_vec())))
    }
}

// --- SAVE ---

#[derive(Clone)]
pub struct SaveCommand;

impl SaveCommand {
    pub fn new() -> Self {
        SaveCommand
    }
}

impl Default for SaveCommand {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CommandHandler for SaveCommand {
    async fn execute(&self, _args: Vec<RespValue>) -> Result<RespValue> {
        Ok(RespValue::SimpleString("OK".to_string()))
    }
}

// --- DBSIZE ---

#[derive(Clone)]
pub struct DbSizeCommand {
    store: DataStore,
}

impl DbSizeCommand {
    pub fn new(store: DataStore) -> Self {
        DbSizeCommand { store }
    }
}

#[async_trait]
impl CommandHandler for DbSizeCommand {
    async fn execute(&self, _args: Vec<RespValue>) -> Result<RespValue> {
        let total = self.store.strings.read().await.len()
            + self.store.lists.read().await.len()
            + self.store.sets.read().await.len()
            + self.store.hashes.read().await.len();
        Ok(RespValue::Integer(total as i64))
    }
}

// --- FLUSHDB / FLUSHALL ---

#[derive(Clone)]
pub struct FlushDbCommand {
    store: DataStore,
}

impl FlushDbCommand {
    pub fn new(store: DataStore) -> Self {
        FlushDbCommand { store }
    }
}

#[async_trait]
impl CommandHandler for FlushDbCommand {
    async fn execute(&self, _args: Vec<RespValue>) -> Result<RespValue> {
        self.store.strings.write().await.clear();
        self.store.lists.write().await.clear();
        self.store.sets.write().await.clear();
        self.store.hashes.write().await.clear();
        self.store.sorted_sets.write().await.clear();
        self.store.hll.write().await.clear();
        self.store.expirations.write().await.clear();
        Ok(RespValue::SimpleString("OK".to_string()))
    }
}

// --- CONFIG (stub) ---

#[derive(Clone)]
pub struct ConfigCommand;

impl ConfigCommand {
    pub fn new() -> Self {
        ConfigCommand
    }
}

impl Default for ConfigCommand {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CommandHandler for ConfigCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'config' command".to_string(),
            ));
        }
        let subcmd = match extract_string(&args[0]) {
            Some(s) => s.to_uppercase(),
            None => return Ok(RespValue::Error("ERR invalid subcommand".to_string())),
        };
        match subcmd.as_str() {
            "GET" => {
                // Return empty array for all config GETs
                Ok(RespValue::Array(Some(vec![])))
            }
            "SET" => Ok(RespValue::SimpleString("OK".to_string())),
            "RESETSTAT" => Ok(RespValue::SimpleString("OK".to_string())),
            "REWRITE" => Ok(RespValue::SimpleString("OK".to_string())),
            _ => Ok(RespValue::Error(format!(
                "ERR unknown subcommand '{}'",
                subcmd
            ))),
        }
    }
}

// --- COMMAND ---

#[derive(Clone)]
pub struct CommandCommand;

impl CommandCommand {
    pub fn new() -> Self {
        CommandCommand
    }
}

impl Default for CommandCommand {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CommandHandler for CommandCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() {
            // COMMAND with no args returns list of all commands (stub: empty array)
            return Ok(RespValue::Array(Some(vec![])));
        }
        let subcmd = match extract_string(&args[0]) {
            Some(s) => s.to_uppercase(),
            None => return Ok(RespValue::Error("ERR invalid subcommand".to_string())),
        };
        match subcmd.as_str() {
            "COUNT" => Ok(RespValue::Integer(100)),
            "DOCS" => Ok(RespValue::Array(Some(vec![]))),
            "INFO" => Ok(RespValue::Array(Some(vec![]))),
            _ => Ok(RespValue::Error(format!(
                "ERR unknown subcommand '{}'",
                subcmd
            ))),
        }
    }
}

// --- TIME ---

#[derive(Clone)]
pub struct TimeCommand;

impl TimeCommand {
    pub fn new() -> Self {
        TimeCommand
    }
}

impl Default for TimeCommand {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CommandHandler for TimeCommand {
    async fn execute(&self, _args: Vec<RespValue>) -> Result<RespValue> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let secs = now.as_secs();
        let micros = now.subsec_micros();
        Ok(RespValue::Array(Some(vec![
            RespValue::BulkString(Some(secs.to_string().into_bytes())),
            RespValue::BulkString(Some(micros.to_string().into_bytes())),
        ])))
    }
}

// --- BGSAVE ---

#[derive(Clone)]
pub struct BgSaveCommand;

impl BgSaveCommand {
    pub fn new() -> Self {
        BgSaveCommand
    }
}

impl Default for BgSaveCommand {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CommandHandler for BgSaveCommand {
    async fn execute(&self, _args: Vec<RespValue>) -> Result<RespValue> {
        Ok(RespValue::SimpleString(
            "Background saving started".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::CommandHandler;

    fn bulk(s: &str) -> RespValue {
        RespValue::BulkString(Some(s.as_bytes().to_vec()))
    }

    #[tokio::test]
    async fn test_info() {
        let store = DataStore::new();
        let cmd = InfoCommand::new(store);
        let r = cmd.execute(vec![]).await.unwrap();
        match r {
            RespValue::BulkString(Some(bytes)) => {
                let s = String::from_utf8_lossy(&bytes);
                assert!(s.contains("redis_version"));
            }
            _ => panic!("Expected BulkString"),
        }
    }

    #[tokio::test]
    async fn test_dbsize() {
        let store = DataStore::new();
        store.strings.write().await.insert("k1".into(), "v1".into());
        store
            .lists
            .write()
            .await
            .insert("l1".into(), vec!["a".into()]);
        let cmd = DbSizeCommand::new(store);
        let r = cmd.execute(vec![]).await.unwrap();
        assert_eq!(r, RespValue::Integer(2));
    }

    #[tokio::test]
    async fn test_flushdb() {
        let store = DataStore::new();
        store.strings.write().await.insert("k1".into(), "v1".into());
        store
            .lists
            .write()
            .await
            .insert("l1".into(), vec!["a".into()]);
        store
            .sets
            .write()
            .await
            .insert("s1".into(), std::collections::HashSet::from(["m".into()]));
        let cmd = FlushDbCommand::new(store.clone());
        let r = cmd.execute(vec![]).await.unwrap();
        assert_eq!(r, RespValue::SimpleString("OK".to_string()));
        assert!(store.strings.read().await.is_empty());
        assert!(store.lists.read().await.is_empty());
        assert!(store.sets.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_config_get_set() {
        let cmd = ConfigCommand::new();
        let r = cmd
            .execute(vec![bulk("GET"), bulk("maxmemory")])
            .await
            .unwrap();
        assert!(matches!(r, RespValue::Array(Some(_))));
        let r = cmd
            .execute(vec![bulk("SET"), bulk("maxmemory"), bulk("100")])
            .await
            .unwrap();
        assert_eq!(r, RespValue::SimpleString("OK".to_string()));
        let r = cmd.execute(vec![bulk("RESETSTAT")]).await.unwrap();
        assert_eq!(r, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_config_wrong_args() {
        let cmd = ConfigCommand::new();
        let r = cmd.execute(vec![]).await.unwrap();
        assert!(matches!(r, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_command_count() {
        let cmd = CommandCommand::new();
        let r = cmd.execute(vec![bulk("COUNT")]).await.unwrap();
        assert_eq!(r, RespValue::Integer(100));
    }

    #[tokio::test]
    async fn test_command_docs() {
        let cmd = CommandCommand::new();
        let r = cmd.execute(vec![bulk("DOCS")]).await.unwrap();
        assert!(matches!(r, RespValue::Array(Some(_))));
    }

    #[tokio::test]
    async fn test_command_no_args() {
        let cmd = CommandCommand::new();
        let r = cmd.execute(vec![]).await.unwrap();
        assert!(matches!(r, RespValue::Array(Some(_))));
    }

    #[tokio::test]
    async fn test_time() {
        let cmd = TimeCommand::new();
        let r = cmd.execute(vec![]).await.unwrap();
        match r {
            RespValue::Array(Some(arr)) => {
                assert_eq!(arr.len(), 2);
            }
            _ => panic!("Expected Array"),
        }
    }

    #[tokio::test]
    async fn test_save() {
        let cmd = SaveCommand::new();
        let r = cmd.execute(vec![]).await.unwrap();
        assert_eq!(r, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_bgsave() {
        let cmd = BgSaveCommand::new();
        let r = cmd.execute(vec![]).await.unwrap();
        assert_eq!(
            r,
            RespValue::SimpleString("Background saving started".to_string())
        );
    }
}
