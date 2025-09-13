use std::collections::HashMap;
use crate::resp::RespValue;
use std::sync::Arc;
use crate::RwLock;
use crate::commands::CommandHandler;
use crate::Result;
use async_trait::async_trait;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct KeysCommand {
    store: Arc<RwLock<HashMap<String, String>>>,
}

impl KeysCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, String>>>) -> Self {
        KeysCommand { store }
    }
}

#[async_trait]
impl CommandHandler for KeysCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error("ERR wrong number of arguments for 'keys' command".to_string()));
        }

        let pattern = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid pattern".to_string())),
        };

        let store = self.store.read().await;
        let keys_iter = store.keys();

        let filtered: Vec<RespValue> = if pattern == "*" {
            keys_iter
                .map(|k| RespValue::BulkString(Some(k.as_bytes().to_vec())))
                .collect()
        } else if pattern.ends_with('*') {
            let prefix = pattern.trim_end_matches('*');
            keys_iter
                .filter(|k| k.starts_with(prefix))
                .map(|k| RespValue::BulkString(Some(k.as_bytes().to_vec())))
                .collect()
        } else {
            keys_iter
                .filter(|k| k.as_str() == pattern.as_str())
                .map(|k| RespValue::BulkString(Some(k.as_bytes().to_vec())))
                .collect()
        };

        Ok(RespValue::Array(Some(filtered)))
    }
}

#[derive(Clone)]
pub struct TTLCommand {
    store: Arc<RwLock<HashMap<String, String>>>,
    expirations: Arc<RwLock<HashMap<String, Instant>>>,
}

impl TTLCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, String>>>, expirations: Arc<RwLock<HashMap<String, Instant>>>) -> Self {
        TTLCommand { store, expirations }
    }
}

#[async_trait]
impl CommandHandler for TTLCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error("ERR wrong number of arguments for 'ttl' command".to_string()));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        // First check expiration map
        let now = Instant::now();
        {
            let expirations_read = self.expirations.read().await;
            if let Some(&deadline) = expirations_read.get(&key) {
                if deadline <= now {
                    drop(expirations_read);
                    // Expired: remove from both maps in consistent lock order (expirations then store)
                    let mut expirations_write = self.expirations.write().await;
                    expirations_write.remove(&key);
                    drop(expirations_write);

                    let mut store_write = self.store.write().await;
                    store_write.remove(&key);
                    drop(store_write);

                    return Ok(RespValue::Integer(-2));
                } else {
                    let remaining = deadline.saturating_duration_since(now).as_secs() as i64;
                    return Ok(RespValue::Integer(remaining.max(0)));
                }
            }
        }

        // No expiration set, check existence in store
        let store_read = self.store.read().await;
        if store_read.contains_key(&key) {
            Ok(RespValue::Integer(-1))
        } else {
            Ok(RespValue::Integer(-2))
        }
    }
}

#[derive(Clone)]
pub struct ExpireCommand {
    store: Arc<RwLock<HashMap<String, String>>>,
    expirations: Arc<RwLock<HashMap<String, Instant>>>,
}

impl ExpireCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, String>>>, expirations: Arc<RwLock<HashMap<String, Instant>>>) -> Self {
        ExpireCommand { store, expirations }
    }
}

#[async_trait]
impl CommandHandler for ExpireCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error("ERR wrong number of arguments for 'expire' command".to_string()));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let seconds: i64 = match &args[1] {
            RespValue::BulkString(Some(bytes)) => {
                match String::from_utf8_lossy(bytes).parse::<i64>() {
                    Ok(v) => v,
                    Err(_) => return Ok(RespValue::Error("ERR value is not an integer or out of range".to_string())),
                }
            }
            RespValue::Integer(v) => *v,
            _ => return Ok(RespValue::Error("ERR value is not an integer or out of range".to_string())),
        };

        // Check existence
        let exists = { self.store.read().await.contains_key(&key) };
        if !exists {
            return Ok(RespValue::Integer(0));
        }

        if seconds <= 0 {
            // immediate expiration: remove key and expiration
            let mut expirations_write = self.expirations.write().await;
            expirations_write.remove(&key);
            drop(expirations_write);

            let mut store_write = self.store.write().await;
            store_write.remove(&key);
            drop(store_write);

            return Ok(RespValue::Integer(1));
        }

        let deadline = Instant::now() + Duration::from_secs(seconds as u64);
        let mut expirations_write = self.expirations.write().await;
        expirations_write.insert(key, deadline);
        Ok(RespValue::Integer(1))
    }
}

#[derive(Clone)]
pub struct RenameCommand {
    store: Arc<RwLock<HashMap<String, String>>>,
    expirations: Arc<RwLock<HashMap<String, Instant>>>,
}

impl RenameCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, String>>>, expirations: Arc<RwLock<HashMap<String, Instant>>>) -> Self {
        RenameCommand { store, expirations }
    }
}

#[async_trait]
impl CommandHandler for RenameCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error("ERR wrong number of arguments for 'rename' command".to_string()));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let newkey = match &args[1] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid newkey".to_string())),
        };

        if key == newkey {
            return Ok(RespValue::Error("ERR source and destination objects are the same".to_string()));
        }

        // Move value
        let mut store = self.store.write().await;
        match store.remove(&key) {
            Some(value) => {
                store.insert(newkey.clone(), value);
                drop(store);
                // Move expiration if exists
                let mut expirations = self.expirations.write().await;
                if let Some(deadline) = expirations.remove(&key) {
                    expirations.insert(newkey.clone(), deadline);
                }
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            None => Ok(RespValue::Error("ERR no such key".to_string())),
        }
    }
}

#[derive(Clone)]
pub struct TypeCommand {
    store: Arc<RwLock<HashMap<String, String>>>,
}

impl TypeCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, String>>>) -> Self {
        TypeCommand { store }
    }
}

#[async_trait]
impl CommandHandler for TypeCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error("ERR wrong number of arguments for 'type' command".to_string()));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let store = self.store.read().await;

        match store.get(&key) {
            Some(_) => Ok(RespValue::SimpleString("string".to_string())),
            None => Ok(RespValue::SimpleString("none".to_string())),
        }
    }
}