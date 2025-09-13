use std::collections::HashMap;
use crate::resp::RespValue;
use std::sync::Arc;
use crate::RwLock;
use crate::commands::CommandHandler;
use crate::Result;
use async_trait::async_trait;

#[derive(Clone)]
pub struct HSetCommand {
    store: Arc<RwLock<HashMap<String, HashMap<String, String>>>>,
}

impl HSetCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, HashMap<String, String>>>>) -> Self {
        HSetCommand { store }
    }
}

#[async_trait]
impl CommandHandler for HSetCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 3 || args.len() % 2 != 1 {
            return Ok(RespValue::Error("ERR wrong number of arguments for 'hset' command".to_string()));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let mut store = self.store.write().await;
        let hash = store.entry(key).or_insert_with(HashMap::new);
        let mut added = 0;

        for chunk in args[1..].chunks(2) {
            let field = match &chunk[0] {
                RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
                _ => return Ok(RespValue::Error("ERR invalid field".to_string())),
            };

            let value = match &chunk[1] {
                RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
                _ => return Ok(RespValue::Error("ERR invalid value".to_string())),
            };

            if hash.insert(field, value).is_none() {
                added += 1;
            }
        }

        Ok(RespValue::Integer(added))
    }
}

#[derive(Clone)]
pub struct HGetCommand {
    store: Arc<RwLock<HashMap<String, HashMap<String, String>>>>,
}

impl HGetCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, HashMap<String, String>>>>) -> Self {
        HGetCommand { store }
    }
}

#[async_trait]
impl CommandHandler for HGetCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error("ERR wrong number of arguments for 'hget' command".to_string()));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let field = match &args[1] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid field".to_string())),
        };

        let store = self.store.read().await;

        match store.get(&key).and_then(|hash| hash.get(&field)) {
            Some(value) => Ok(RespValue::BulkString(Some(value.as_bytes().to_vec()))),
            None => Ok(RespValue::BulkString(None)),
        }
    }
}

#[derive(Clone)]
pub struct HGetAllCommand {
    store: Arc<RwLock<HashMap<String, HashMap<String, String>>>>,
}

impl HGetAllCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, HashMap<String, String>>>>) -> Self {
        HGetAllCommand { store }
    }
}

#[async_trait]
impl CommandHandler for HGetAllCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error("ERR wrong number of arguments for 'hgetall' command".to_string()));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let store = self.store.read().await;

        match store.get(&key) {
            Some(hash) => {
                let mut result = Vec::new();
                for (field, value) in hash.iter() {
                    result.push(RespValue::BulkString(Some(field.as_bytes().to_vec())));
                    result.push(RespValue::BulkString(Some(value.as_bytes().to_vec())));
                }
                Ok(RespValue::Array(Some(result)))
            }
            None => Ok(RespValue::Array(vec![].into())),
        }
    }
}

#[derive(Clone)]
pub struct HExistsCommand {
    store: Arc<RwLock<HashMap<String, HashMap<String, String>>>>,
}

impl HExistsCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, HashMap<String, String>>>>) -> Self {
        HExistsCommand { store }
    }
}

#[async_trait]
impl CommandHandler for HExistsCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error("ERR wrong number of arguments for 'hexists' command".to_string()));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let field = match &args[1] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid field".to_string())),
        };

        let store = self.store.read().await;

        Ok(RespValue::Integer(
            if store.get(&key).map_or(false, |hash| hash.contains_key(&field)) {
                1
            } else {
                0
            }
        ))
    }
}

#[derive(Clone)]
pub struct HDelCommand {
    store: Arc<RwLock<HashMap<String, HashMap<String, String>>>>,
}

impl HDelCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, HashMap<String, String>>>>) -> Self {
        HDelCommand { store }
    }
}

#[async_trait]
impl CommandHandler for HDelCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::Error("ERR wrong number of arguments for 'hdel' command".to_string()));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let mut store = self.store.write().await;

        match store.get_mut(&key) {
            Some(hash) => {
                let mut deleted = 0;
                for arg in args.iter().skip(1) {
                    match arg {
                        RespValue::BulkString(Some(bytes)) => {
                            let field = String::from_utf8_lossy(bytes).to_string();
                            if hash.remove(&field).is_some() {
                                deleted += 1;
                            }
                        }
                        _ => return Ok(RespValue::Error("ERR invalid field".to_string())),
                    }
                }
                if hash.is_empty() {
                    store.remove(&key);
                }
                Ok(RespValue::Integer(deleted))
            }
            None => Ok(RespValue::Integer(0)),
        }
    }
}

#[derive(Clone)]
pub struct HLenCommand {
    store: Arc<RwLock<HashMap<String, HashMap<String, String>>>>,
}

impl HLenCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, HashMap<String, String>>>>) -> Self {
        HLenCommand { store }
    }
}

#[async_trait]
impl CommandHandler for HLenCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error("ERR wrong number of arguments for 'hlen' command".to_string()));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let store = self.store.read().await;

        match store.get(&key) {
            Some(hash) => Ok(RespValue::Integer(hash.len() as i64)),
            None => Ok(RespValue::Integer(0)),
        }
    }
}

#[derive(Clone)]
pub struct HKeysCommand {
    store: Arc<RwLock<HashMap<String, HashMap<String, String>>>>,
}

impl HKeysCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, HashMap<String, String>>>>) -> Self {
        HKeysCommand { store }
    }
}

#[async_trait]
impl CommandHandler for HKeysCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error("ERR wrong number of arguments for 'hkeys' command".to_string()));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let store = self.store.read().await;

        match store.get(&key) {
            Some(hash) => {
                let keys: Vec<RespValue> = hash.keys()
                    .map(|k| RespValue::BulkString(Some(k.as_bytes().to_vec())))
                    .collect();
                Ok(RespValue::Array(Some(keys)))
            }
            None => Ok(RespValue::Array(vec![].into())),
        }
    }
}

#[derive(Clone)]
pub struct HValsCommand {
    store: Arc<RwLock<HashMap<String, HashMap<String, String>>>>,
}

impl HValsCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, HashMap<String, String>>>>) -> Self {
        HValsCommand { store }
    }
}

#[async_trait]
impl CommandHandler for HValsCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error("ERR wrong number of arguments for 'hvals' command".to_string()));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let store = self.store.read().await;

        match store.get(&key) {
            Some(hash) => {
                let values: Vec<RespValue> = hash.values()
                    .map(|v| RespValue::BulkString(Some(v.as_bytes().to_vec())))
                    .collect();
                Ok(RespValue::Array(Some(values)))
            }
            None => Ok(RespValue::Array(vec![].into())),
        }
    }
}