use crate::commands::CommandHandler;
use crate::resp::RespValue;
use crate::Result;
use crate::RwLock;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct LPushCommand {
    store: Arc<RwLock<HashMap<String, Vec<String>>>>,
}

impl LPushCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, Vec<String>>>>) -> Self {
        LPushCommand { store }
    }
}

#[async_trait]
impl CommandHandler for LPushCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'lpush' command".to_string(),
            ));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let mut store = self.store.write().await;
        let list = store.entry(key).or_insert_with(Vec::new);

        for arg in args.iter().skip(1) {
            match arg {
                RespValue::BulkString(Some(bytes)) => {
                    list.insert(0, String::from_utf8_lossy(bytes).to_string());
                }
                _ => return Ok(RespValue::Error("ERR invalid value".to_string())),
            }
        }

        Ok(RespValue::Integer(list.len() as i64))
    }
}

#[derive(Clone)]
pub struct RPushCommand {
    store: Arc<RwLock<HashMap<String, Vec<String>>>>,
}

impl RPushCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, Vec<String>>>>) -> Self {
        RPushCommand { store }
    }
}

#[async_trait]
impl CommandHandler for RPushCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'rpush' command".to_string(),
            ));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let mut store = self.store.write().await;
        let list = store.entry(key).or_insert_with(Vec::new);

        for arg in args.iter().skip(1) {
            match arg {
                RespValue::BulkString(Some(bytes)) => {
                    list.push(String::from_utf8_lossy(bytes).to_string());
                }
                _ => return Ok(RespValue::Error("ERR invalid value".to_string())),
            }
        }

        Ok(RespValue::Integer(list.len() as i64))
    }
}

#[derive(Clone)]
pub struct LPopCommand {
    store: Arc<RwLock<HashMap<String, Vec<String>>>>,
}

impl LPopCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, Vec<String>>>>) -> Self {
        LPopCommand { store }
    }
}

#[async_trait]
impl CommandHandler for LPopCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'lpop' command".to_string(),
            ));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let mut store = self.store.write().await;

        match store.get_mut(&key) {
            Some(list) => {
                if list.is_empty() {
                    Ok(RespValue::BulkString(None))
                } else {
                    let value = list.remove(0);
                    Ok(RespValue::BulkString(Some(value.into_bytes())))
                }
            }
            None => Ok(RespValue::BulkString(None)),
        }
    }
}

#[derive(Clone)]
pub struct RPopCommand {
    store: Arc<RwLock<HashMap<String, Vec<String>>>>,
}

impl RPopCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, Vec<String>>>>) -> Self {
        RPopCommand { store }
    }
}

#[async_trait]
impl CommandHandler for RPopCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'rpop' command".to_string(),
            ));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let mut store = self.store.write().await;

        match store.get_mut(&key) {
            Some(list) => {
                if list.is_empty() {
                    Ok(RespValue::BulkString(None))
                } else {
                    let value = list.pop().unwrap();
                    Ok(RespValue::BulkString(Some(value.into_bytes())))
                }
            }
            None => Ok(RespValue::BulkString(None)),
        }
    }
}

#[derive(Clone)]
pub struct LRangeCommand {
    store: Arc<RwLock<HashMap<String, Vec<String>>>>,
}

impl LRangeCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, Vec<String>>>>) -> Self {
        LRangeCommand { store }
    }
}

#[async_trait]
impl CommandHandler for LRangeCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'lrange' command".to_string(),
            ));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let start = match &args[1] {
            RespValue::BulkString(Some(bytes)) => {
                match String::from_utf8_lossy(bytes).parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => {
                        return Ok(RespValue::Error("ERR value is not an integer".to_string()))
                    }
                }
            }
            _ => return Ok(RespValue::Error("ERR invalid start index".to_string())),
        };

        let stop = match &args[2] {
            RespValue::BulkString(Some(bytes)) => {
                match String::from_utf8_lossy(bytes).parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => {
                        return Ok(RespValue::Error("ERR value is not an integer".to_string()))
                    }
                }
            }
            _ => return Ok(RespValue::Error("ERR invalid stop index".to_string())),
        };

        let store = self.store.read().await;

        match store.get(&key) {
            Some(list) => {
                let len = list.len() as i64;
                let mut start_idx = if start < 0 { len + start } else { start };
                let mut stop_idx = if stop < 0 { len + stop } else { stop };

                if start_idx < 0 {
                    start_idx = 0;
                }
                if stop_idx >= len {
                    stop_idx = len - 1;
                }
                if start_idx > stop_idx || start_idx >= len {
                    return Ok(RespValue::Array(Some(vec![])));
                }

                let result: Vec<RespValue> = list[(start_idx as usize)..=(stop_idx as usize)]
                    .iter()
                    .map(|s| RespValue::BulkString(Some(s.as_bytes().to_vec())))
                    .collect();

                Ok(RespValue::Array(Some(result)))
            }
            None => Ok(RespValue::Array(Some(vec![]))),
        }
    }
}

#[derive(Clone)]
pub struct LLenCommand {
    store: Arc<RwLock<HashMap<String, Vec<String>>>>,
}

impl LLenCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, Vec<String>>>>) -> Self {
        LLenCommand { store }
    }
}

#[async_trait]
impl CommandHandler for LLenCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'llen' command".to_string(),
            ));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let store = self.store.read().await;

        match store.get(&key) {
            Some(list) => Ok(RespValue::Integer(list.len() as i64)),
            None => Ok(RespValue::Integer(0)),
        }
    }
}
