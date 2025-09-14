use crate::commands::CommandHandler;
use crate::resp::RespValue;
use crate::Result;
use crate::RwLock;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct SetCommand {
    store: Arc<RwLock<HashMap<String, String>>>,
}

impl SetCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, String>>>) -> Self {
        SetCommand { store }
    }
}

#[async_trait]
impl CommandHandler for SetCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'set' command".to_string(),
            ));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let value = match &args[1] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid value".to_string())),
        };

        let mut store = self.store.write().await;
        store.insert(key, value);

        Ok(RespValue::SimpleString("OK".to_string()))
    }
}

#[derive(Clone)]
pub struct GetCommand {
    store: Arc<RwLock<HashMap<String, String>>>,
}

impl GetCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, String>>>) -> Self {
        GetCommand { store }
    }
}

#[async_trait]
impl CommandHandler for GetCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'get' command".to_string(),
            ));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let store = self.store.read().await;

        match store.get(&key) {
            Some(value) => Ok(RespValue::BulkString(Some(value.as_bytes().to_vec()))),
            None => Ok(RespValue::BulkString(None)),
        }
    }
}

#[derive(Clone)]
pub struct DelCommand {
    store: Arc<RwLock<HashMap<String, String>>>,
}

impl DelCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, String>>>) -> Self {
        DelCommand { store }
    }
}

#[async_trait]
impl CommandHandler for DelCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'del' command".to_string(),
            ));
        }

        let mut store = self.store.write().await;
        let mut deleted = 0;

        for arg in args {
            let key = match arg {
                RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(&bytes).to_string(),
                _ => continue,
            };

            if store.remove(&key).is_some() {
                deleted += 1;
            }
        }

        Ok(RespValue::Integer(deleted))
    }
}

#[derive(Clone)]
pub struct ExistsCommand {
    store: Arc<RwLock<HashMap<String, String>>>,
}

impl ExistsCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, String>>>) -> Self {
        ExistsCommand { store }
    }
}

#[async_trait]
impl CommandHandler for ExistsCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'exists' command".to_string(),
            ));
        }

        let store = self.store.read().await;
        let mut count = 0;

        for arg in args {
            let key = match arg {
                RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(&bytes).to_string(),
                _ => continue,
            };

            if store.contains_key(&key) {
                count += 1;
            }
        }

        Ok(RespValue::Integer(count))
    }
}

#[derive(Clone)]
pub struct AppendCommand {
    store: Arc<RwLock<HashMap<String, String>>>,
}

impl AppendCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, String>>>) -> Self {
        AppendCommand { store }
    }
}

#[async_trait]
impl CommandHandler for AppendCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'append' command".to_string(),
            ));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let value = match &args[1] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid value".to_string())),
        };

        let mut store = self.store.write().await;
        let new_len = match store.get_mut(&key) {
            Some(existing) => {
                existing.push_str(&value);
                existing.len()
            }
            None => {
                store.insert(key, value.clone());
                value.len()
            }
        };

        Ok(RespValue::Integer(new_len as i64))
    }
}

#[derive(Clone)]
pub struct StrlenCommand {
    store: Arc<RwLock<HashMap<String, String>>>,
}

impl StrlenCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, String>>>) -> Self {
        StrlenCommand { store }
    }
}

#[async_trait]
impl CommandHandler for StrlenCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'strlen' command".to_string(),
            ));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let store = self.store.read().await;

        match store.get(&key) {
            Some(value) => Ok(RespValue::Integer(value.len() as i64)),
            None => Ok(RespValue::Integer(0)),
        }
    }
}
