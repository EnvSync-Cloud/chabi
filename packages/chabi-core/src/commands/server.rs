use crate::commands::CommandHandler;
use crate::resp::RespValue;
use crate::Result;
use crate::RwLock; // use async RwLock from crate
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Clone)]
pub struct InfoCommand {
    string_store: Arc<RwLock<HashMap<String, String>>>,
    hash_store: Arc<RwLock<HashMap<String, HashMap<String, String>>>>,
    list_store: Arc<RwLock<HashMap<String, Vec<String>>>>,
    set_store: Arc<RwLock<HashMap<String, HashSet<String>>>>,
}

impl InfoCommand {
    pub fn new(
        string_store: Arc<RwLock<HashMap<String, String>>>,
        hash_store: Arc<RwLock<HashMap<String, HashMap<String, String>>>>,
        list_store: Arc<RwLock<HashMap<String, Vec<String>>>>,
        set_store: Arc<RwLock<HashMap<String, HashSet<String>>>>,
    ) -> Self {
        InfoCommand {
            string_store,
            hash_store,
            list_store,
            set_store,
        }
    }
}

#[async_trait]
impl CommandHandler for InfoCommand {
    async fn execute(&self, _args: Vec<RespValue>) -> Result<RespValue> {
        let string_count = self.string_store.read().await.len();
        let hash_count = self.hash_store.read().await.len();
        let list_count = self.list_store.read().await.len();
        let set_count = self.set_store.read().await.len();
        let total_keys = string_count + hash_count + list_count + set_count;

        let info = format!(
            "# Server\r\nredis_version:1.0.0\r\nprocess_id:{}\r\n# Keyspace\r\ndb0:keys={},expires=0,avg_ttl=0\r\n",
            std::process::id(),
            total_keys
        );
        Ok(RespValue::BulkString(Some(info.as_bytes().to_vec())))
    }
}

#[derive(Clone)]
pub struct SaveCommand {
    _string_store: Arc<RwLock<HashMap<String, String>>>,
    _hash_store: Arc<RwLock<HashMap<String, HashMap<String, String>>>>,
    _list_store: Arc<RwLock<HashMap<String, Vec<String>>>>,
    _set_store: Arc<RwLock<HashMap<String, HashSet<String>>>>,
}

impl SaveCommand {
    pub fn new(
        string_store: Arc<RwLock<HashMap<String, String>>>,
        hash_store: Arc<RwLock<HashMap<String, HashMap<String, String>>>>,
        list_store: Arc<RwLock<HashMap<String, Vec<String>>>>,
        set_store: Arc<RwLock<HashMap<String, HashSet<String>>>>,
    ) -> Self {
        SaveCommand {
            _string_store: string_store,
            _hash_store: hash_store,
            _list_store: list_store,
            _set_store: set_store,
        }
    }
}

#[async_trait]
impl CommandHandler for SaveCommand {
    async fn execute(&self, _args: Vec<RespValue>) -> Result<RespValue> {
        // TODO: Implement actual persistence
        Ok(RespValue::SimpleString("OK".to_string()))
    }
}
