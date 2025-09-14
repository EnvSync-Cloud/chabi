use crate::commands::CommandHandler;
use crate::resp::RespValue;
use crate::Result;
use crate::RwLock;
use async_trait::async_trait;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Clone)]
pub struct SAddCommand {
    store: Arc<RwLock<HashMap<String, HashSet<String>>>>,
}

impl SAddCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, HashSet<String>>>>) -> Self {
        SAddCommand { store }
    }
}

#[async_trait]
impl CommandHandler for SAddCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'sadd' command".to_string(),
            ));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let mut store = self.store.write().await;
        let set = store.entry(key).or_insert_with(HashSet::new);
        let mut added = 0;

        for arg in args.iter().skip(1) {
            match arg {
                RespValue::BulkString(Some(bytes)) => {
                    if set.insert(String::from_utf8_lossy(bytes).to_string()) {
                        added += 1;
                    }
                }
                _ => return Ok(RespValue::Error("ERR invalid member".to_string())),
            }
        }

        Ok(RespValue::Integer(added))
    }
}

#[derive(Clone)]
pub struct SMembersCommand {
    store: Arc<RwLock<HashMap<String, HashSet<String>>>>,
}

impl SMembersCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, HashSet<String>>>>) -> Self {
        SMembersCommand { store }
    }
}

#[async_trait]
impl CommandHandler for SMembersCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'smembers' command".to_string(),
            ));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let store = self.store.read().await;

        match store.get(&key) {
            Some(set) => {
                let members: Vec<RespValue> = set
                    .iter()
                    .map(|s| RespValue::BulkString(Some(s.as_bytes().to_vec())))
                    .collect();
                Ok(RespValue::Array(Some(members)))
            }
            None => Ok(RespValue::Array(Some(vec![]))),
        }
    }
}

#[derive(Clone)]
pub struct SIsMemberCommand {
    store: Arc<RwLock<HashMap<String, HashSet<String>>>>,
}

impl SIsMemberCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, HashSet<String>>>>) -> Self {
        SIsMemberCommand { store }
    }
}

#[async_trait]
impl CommandHandler for SIsMemberCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'sismember' command".to_string(),
            ));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let member = match &args[1] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid member".to_string())),
        };

        let store = self.store.read().await;

        match store.get(&key) {
            Some(set) => Ok(RespValue::Integer(if set.contains(&member) {
                1
            } else {
                0
            })),
            None => Ok(RespValue::Integer(0)),
        }
    }
}

#[derive(Clone)]
pub struct SCardCommand {
    store: Arc<RwLock<HashMap<String, HashSet<String>>>>,
}

impl SCardCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, HashSet<String>>>>) -> Self {
        SCardCommand { store }
    }
}

#[async_trait]
impl CommandHandler for SCardCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'scard' command".to_string(),
            ));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let store = self.store.read().await;

        match store.get(&key) {
            Some(set) => Ok(RespValue::Integer(set.len() as i64)),
            None => Ok(RespValue::Integer(0)),
        }
    }
}

#[derive(Clone)]
pub struct SRemCommand {
    store: Arc<RwLock<HashMap<String, HashSet<String>>>>,
}

impl SRemCommand {
    pub fn new(store: Arc<RwLock<HashMap<String, HashSet<String>>>>) -> Self {
        SRemCommand { store }
    }
}

#[async_trait]
impl CommandHandler for SRemCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'srem' command".to_string(),
            ));
        }

        let key = match &args[0] {
            RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
            _ => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let mut store = self.store.write().await;

        match store.get_mut(&key) {
            Some(set) => {
                let mut removed = 0;
                for arg in args.iter().skip(1) {
                    match arg {
                        RespValue::BulkString(Some(bytes)) => {
                            if set.remove(&String::from_utf8_lossy(bytes).to_string()) {
                                removed += 1;
                            }
                        }
                        _ => return Ok(RespValue::Error("ERR invalid member".to_string())),
                    }
                }
                if set.is_empty() {
                    store.remove(&key);
                }
                Ok(RespValue::Integer(removed))
            }
            None => Ok(RespValue::Integer(0)),
        }
    }
}
