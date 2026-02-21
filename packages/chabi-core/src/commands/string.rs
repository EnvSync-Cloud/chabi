use crate::commands::CommandHandler;
use crate::resp::RespValue;
use crate::storage::DataStore;
use crate::Result;
use async_trait::async_trait;
use std::time::{Duration, Instant};

fn extract_string(val: &RespValue) -> Option<String> {
    match val {
        RespValue::BulkString(Some(bytes)) => Some(String::from_utf8_lossy(bytes).to_string()),
        _ => None,
    }
}

// --- SET with EX/PX/NX/XX/GET/KEEPTTL ---

#[derive(Clone)]
pub struct SetCommand {
    store: DataStore,
}

impl SetCommand {
    pub fn new(store: DataStore) -> Self {
        SetCommand { store }
    }
}

#[async_trait]
impl CommandHandler for SetCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'set' command".to_string(),
            ));
        }

        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let value = match extract_string(&args[1]) {
            Some(v) => v,
            None => return Ok(RespValue::Error("ERR invalid value".to_string())),
        };

        let mut ex: Option<u64> = None;
        let mut px: Option<u64> = None;
        let mut nx = false;
        let mut xx = false;
        let mut get = false;
        let mut keepttl = false;

        let mut i = 2;
        while i < args.len() {
            let flag = match extract_string(&args[i]) {
                Some(f) => f.to_uppercase(),
                None => return Ok(RespValue::Error("ERR syntax error".to_string())),
            };
            match flag.as_str() {
                "EX" => {
                    i += 1;
                    if i >= args.len() {
                        return Ok(RespValue::Error("ERR syntax error".to_string()));
                    }
                    match extract_string(&args[i]).and_then(|s| s.parse::<u64>().ok()) {
                        Some(v) if v > 0 => ex = Some(v),
                        _ => {
                            return Ok(RespValue::Error(
                                "ERR invalid expire time in 'set' command".to_string(),
                            ))
                        }
                    }
                }
                "PX" => {
                    i += 1;
                    if i >= args.len() {
                        return Ok(RespValue::Error("ERR syntax error".to_string()));
                    }
                    match extract_string(&args[i]).and_then(|s| s.parse::<u64>().ok()) {
                        Some(v) if v > 0 => px = Some(v),
                        _ => {
                            return Ok(RespValue::Error(
                                "ERR invalid expire time in 'set' command".to_string(),
                            ))
                        }
                    }
                }
                "NX" => nx = true,
                "XX" => xx = true,
                "GET" => get = true,
                "KEEPTTL" => keepttl = true,
                _ => return Ok(RespValue::Error("ERR syntax error".to_string())),
            }
            i += 1;
        }

        if nx && xx {
            return Ok(RespValue::Error(
                "ERR XX and NX options at the same time are not compatible".to_string(),
            ));
        }

        let mut store = self.store.strings.write().await;
        let old_value = store.get(&key).cloned();

        if nx && old_value.is_some() {
            return Ok(if get {
                match old_value {
                    Some(v) => RespValue::BulkString(Some(v.into_bytes())),
                    None => RespValue::BulkString(None),
                }
            } else {
                RespValue::BulkString(None)
            });
        }
        if xx && old_value.is_none() {
            return Ok(RespValue::BulkString(None));
        }

        store.insert(key.clone(), value);
        drop(store);

        let mut expirations = self.store.expirations.write().await;
        if let Some(secs) = ex {
            expirations.insert(key.clone(), Instant::now() + Duration::from_secs(secs));
        } else if let Some(ms) = px {
            expirations.insert(key.clone(), Instant::now() + Duration::from_millis(ms));
        } else if !keepttl {
            expirations.remove(&key);
        }

        if get {
            Ok(match old_value {
                Some(v) => RespValue::BulkString(Some(v.into_bytes())),
                None => RespValue::BulkString(None),
            })
        } else {
            Ok(RespValue::SimpleString("OK".to_string()))
        }
    }
}

// --- GET ---

#[derive(Clone)]
pub struct GetCommand {
    store: DataStore,
}

impl GetCommand {
    pub fn new(store: DataStore) -> Self {
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
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let store = self.store.strings.read().await;
        match store.get(&key) {
            Some(value) => Ok(RespValue::BulkString(Some(value.as_bytes().to_vec()))),
            None => Ok(RespValue::BulkString(None)),
        }
    }
}

// --- DEL (cross-type) ---

#[derive(Clone)]
pub struct DelCommand {
    store: DataStore,
}

impl DelCommand {
    pub fn new(store: DataStore) -> Self {
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
        let mut deleted = 0i64;
        let mut strings = self.store.strings.write().await;
        let mut lists = self.store.lists.write().await;
        let mut sets = self.store.sets.write().await;
        let mut hashes = self.store.hashes.write().await;
        let mut exps = self.store.expirations.write().await;

        for arg in &args {
            let key = match extract_string(arg) {
                Some(k) => k,
                None => continue,
            };
            let mut found = false;
            if strings.remove(&key).is_some() {
                found = true;
            }
            if lists.remove(&key).is_some() {
                found = true;
            }
            if sets.remove(&key).is_some() {
                found = true;
            }
            if hashes.remove(&key).is_some() {
                found = true;
            }
            if found {
                exps.remove(&key);
                deleted += 1;
            }
        }
        Ok(RespValue::Integer(deleted))
    }
}

// --- EXISTS (cross-type) ---

#[derive(Clone)]
pub struct ExistsCommand {
    store: DataStore,
}

impl ExistsCommand {
    pub fn new(store: DataStore) -> Self {
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
        let strings = self.store.strings.read().await;
        let lists = self.store.lists.read().await;
        let sets = self.store.sets.read().await;
        let hashes = self.store.hashes.read().await;
        let mut count = 0i64;

        for arg in &args {
            let key = match extract_string(arg) {
                Some(k) => k,
                None => continue,
            };
            if strings.contains_key(&key)
                || lists.contains_key(&key)
                || sets.contains_key(&key)
                || hashes.contains_key(&key)
            {
                count += 1;
            }
        }
        Ok(RespValue::Integer(count))
    }
}

// --- APPEND ---

#[derive(Clone)]
pub struct AppendCommand {
    store: DataStore,
}

impl AppendCommand {
    pub fn new(store: DataStore) -> Self {
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
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let value = match extract_string(&args[1]) {
            Some(v) => v,
            None => return Ok(RespValue::Error("ERR invalid value".to_string())),
        };
        let mut store = self.store.strings.write().await;
        let new_len = match store.get_mut(&key) {
            Some(existing) => {
                existing.push_str(&value);
                existing.len()
            }
            None => {
                let len = value.len();
                store.insert(key, value);
                len
            }
        };
        Ok(RespValue::Integer(new_len as i64))
    }
}

// --- STRLEN ---

#[derive(Clone)]
pub struct StrlenCommand {
    store: DataStore,
}

impl StrlenCommand {
    pub fn new(store: DataStore) -> Self {
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
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let store = self.store.strings.read().await;
        match store.get(&key) {
            Some(value) => Ok(RespValue::Integer(value.len() as i64)),
            None => Ok(RespValue::Integer(0)),
        }
    }
}

// --- INCR ---

#[derive(Clone)]
pub struct IncrCommand {
    store: DataStore,
}

impl IncrCommand {
    pub fn new(store: DataStore) -> Self {
        IncrCommand { store }
    }
}

#[async_trait]
impl CommandHandler for IncrCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'incr' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let mut store = self.store.strings.write().await;
        let current = store.get(&key).map(|v| v.as_str()).unwrap_or("0");
        match current.parse::<i64>() {
            Ok(n) => {
                let new_val = n + 1;
                store.insert(key, new_val.to_string());
                Ok(RespValue::Integer(new_val))
            }
            Err(_) => Ok(RespValue::Error(
                "ERR value is not an integer or out of range".to_string(),
            )),
        }
    }
}

// --- DECR ---

#[derive(Clone)]
pub struct DecrCommand {
    store: DataStore,
}

impl DecrCommand {
    pub fn new(store: DataStore) -> Self {
        DecrCommand { store }
    }
}

#[async_trait]
impl CommandHandler for DecrCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'decr' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let mut store = self.store.strings.write().await;
        let current = store.get(&key).map(|v| v.as_str()).unwrap_or("0");
        match current.parse::<i64>() {
            Ok(n) => {
                let new_val = n - 1;
                store.insert(key, new_val.to_string());
                Ok(RespValue::Integer(new_val))
            }
            Err(_) => Ok(RespValue::Error(
                "ERR value is not an integer or out of range".to_string(),
            )),
        }
    }
}

// --- INCRBY ---

#[derive(Clone)]
pub struct IncrByCommand {
    store: DataStore,
}

impl IncrByCommand {
    pub fn new(store: DataStore) -> Self {
        IncrByCommand { store }
    }
}

#[async_trait]
impl CommandHandler for IncrByCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'incrby' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let increment: i64 = match extract_string(&args[1]).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => {
                return Ok(RespValue::Error(
                    "ERR value is not an integer or out of range".to_string(),
                ))
            }
        };
        let mut store = self.store.strings.write().await;
        let current = store.get(&key).map(|v| v.as_str()).unwrap_or("0");
        match current.parse::<i64>() {
            Ok(n) => {
                let new_val = n + increment;
                store.insert(key, new_val.to_string());
                Ok(RespValue::Integer(new_val))
            }
            Err(_) => Ok(RespValue::Error(
                "ERR value is not an integer or out of range".to_string(),
            )),
        }
    }
}

// --- DECRBY ---

#[derive(Clone)]
pub struct DecrByCommand {
    store: DataStore,
}

impl DecrByCommand {
    pub fn new(store: DataStore) -> Self {
        DecrByCommand { store }
    }
}

#[async_trait]
impl CommandHandler for DecrByCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'decrby' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let decrement: i64 = match extract_string(&args[1]).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => {
                return Ok(RespValue::Error(
                    "ERR value is not an integer or out of range".to_string(),
                ))
            }
        };
        let mut store = self.store.strings.write().await;
        let current = store.get(&key).map(|v| v.as_str()).unwrap_or("0");
        match current.parse::<i64>() {
            Ok(n) => {
                let new_val = n - decrement;
                store.insert(key, new_val.to_string());
                Ok(RespValue::Integer(new_val))
            }
            Err(_) => Ok(RespValue::Error(
                "ERR value is not an integer or out of range".to_string(),
            )),
        }
    }
}

// --- INCRBYFLOAT ---

#[derive(Clone)]
pub struct IncrByFloatCommand {
    store: DataStore,
}

impl IncrByFloatCommand {
    pub fn new(store: DataStore) -> Self {
        IncrByFloatCommand { store }
    }
}

#[async_trait]
impl CommandHandler for IncrByFloatCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'incrbyfloat' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let increment: f64 = match extract_string(&args[1]).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => {
                return Ok(RespValue::Error(
                    "ERR value is not a valid float".to_string(),
                ))
            }
        };
        let mut store = self.store.strings.write().await;
        let current = store.get(&key).map(|v| v.as_str()).unwrap_or("0");
        match current.parse::<f64>() {
            Ok(n) => {
                let new_val = n + increment;
                let s = format_float(new_val);
                store.insert(key, s.clone());
                Ok(RespValue::BulkString(Some(s.into_bytes())))
            }
            Err(_) => Ok(RespValue::Error(
                "ERR value is not a valid float".to_string(),
            )),
        }
    }
}

fn format_float(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e17 {
        format!("{:.0}", v)
    } else {
        let s = format!("{}", v);
        s
    }
}

// --- MGET ---

#[derive(Clone)]
pub struct MGetCommand {
    store: DataStore,
}

impl MGetCommand {
    pub fn new(store: DataStore) -> Self {
        MGetCommand { store }
    }
}

#[async_trait]
impl CommandHandler for MGetCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'mget' command".to_string(),
            ));
        }
        let store = self.store.strings.read().await;
        let results: Vec<RespValue> = args
            .iter()
            .map(|arg| {
                let key = match extract_string(arg) {
                    Some(k) => k,
                    None => return RespValue::BulkString(None),
                };
                match store.get(&key) {
                    Some(v) => RespValue::BulkString(Some(v.as_bytes().to_vec())),
                    None => RespValue::BulkString(None),
                }
            })
            .collect();
        Ok(RespValue::Array(Some(results)))
    }
}

// --- MSET ---

#[derive(Clone)]
pub struct MSetCommand {
    store: DataStore,
}

impl MSetCommand {
    pub fn new(store: DataStore) -> Self {
        MSetCommand { store }
    }
}

#[async_trait]
impl CommandHandler for MSetCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() || !args.len().is_multiple_of(2) {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'mset' command".to_string(),
            ));
        }
        let mut store = self.store.strings.write().await;
        for chunk in args.chunks(2) {
            let key = match extract_string(&chunk[0]) {
                Some(k) => k,
                None => return Ok(RespValue::Error("ERR invalid key".to_string())),
            };
            let value = match extract_string(&chunk[1]) {
                Some(v) => v,
                None => return Ok(RespValue::Error("ERR invalid value".to_string())),
            };
            store.insert(key, value);
        }
        Ok(RespValue::SimpleString("OK".to_string()))
    }
}

// --- MSETNX ---

#[derive(Clone)]
pub struct MSetNxCommand {
    store: DataStore,
}

impl MSetNxCommand {
    pub fn new(store: DataStore) -> Self {
        MSetNxCommand { store }
    }
}

#[async_trait]
impl CommandHandler for MSetNxCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() || !args.len().is_multiple_of(2) {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'msetnx' command".to_string(),
            ));
        }
        let mut store = self.store.strings.write().await;
        // Check all keys first
        for chunk in args.chunks(2) {
            let key = match extract_string(&chunk[0]) {
                Some(k) => k,
                None => return Ok(RespValue::Error("ERR invalid key".to_string())),
            };
            if store.contains_key(&key) {
                return Ok(RespValue::Integer(0));
            }
        }
        for chunk in args.chunks(2) {
            let key = extract_string(&chunk[0]).unwrap();
            let value = match extract_string(&chunk[1]) {
                Some(v) => v,
                None => return Ok(RespValue::Error("ERR invalid value".to_string())),
            };
            store.insert(key, value);
        }
        Ok(RespValue::Integer(1))
    }
}

// --- SETNX ---

#[derive(Clone)]
pub struct SetNxCommand {
    store: DataStore,
}

impl SetNxCommand {
    pub fn new(store: DataStore) -> Self {
        SetNxCommand { store }
    }
}

#[async_trait]
impl CommandHandler for SetNxCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'setnx' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let value = match extract_string(&args[1]) {
            Some(v) => v,
            None => return Ok(RespValue::Error("ERR invalid value".to_string())),
        };
        let mut store = self.store.strings.write().await;
        if let std::collections::hash_map::Entry::Vacant(e) = store.entry(key) {
            e.insert(value);
            Ok(RespValue::Integer(1))
        } else {
            Ok(RespValue::Integer(0))
        }
    }
}

// --- SETEX ---

#[derive(Clone)]
pub struct SetExCommand {
    store: DataStore,
}

impl SetExCommand {
    pub fn new(store: DataStore) -> Self {
        SetExCommand { store }
    }
}

#[async_trait]
impl CommandHandler for SetExCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'setex' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let seconds: u64 = match extract_string(&args[1]).and_then(|s| s.parse().ok()) {
            Some(v) if v > 0 => v,
            _ => {
                return Ok(RespValue::Error(
                    "ERR invalid expire time in 'setex' command".to_string(),
                ))
            }
        };
        let value = match extract_string(&args[2]) {
            Some(v) => v,
            None => return Ok(RespValue::Error("ERR invalid value".to_string())),
        };
        let mut store = self.store.strings.write().await;
        store.insert(key.clone(), value);
        drop(store);
        let mut exps = self.store.expirations.write().await;
        exps.insert(key, Instant::now() + Duration::from_secs(seconds));
        Ok(RespValue::SimpleString("OK".to_string()))
    }
}

// --- PSETEX ---

#[derive(Clone)]
pub struct PSetExCommand {
    store: DataStore,
}

impl PSetExCommand {
    pub fn new(store: DataStore) -> Self {
        PSetExCommand { store }
    }
}

#[async_trait]
impl CommandHandler for PSetExCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'psetex' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let ms: u64 = match extract_string(&args[1]).and_then(|s| s.parse().ok()) {
            Some(v) if v > 0 => v,
            _ => {
                return Ok(RespValue::Error(
                    "ERR invalid expire time in 'psetex' command".to_string(),
                ))
            }
        };
        let value = match extract_string(&args[2]) {
            Some(v) => v,
            None => return Ok(RespValue::Error("ERR invalid value".to_string())),
        };
        let mut store = self.store.strings.write().await;
        store.insert(key.clone(), value);
        drop(store);
        let mut exps = self.store.expirations.write().await;
        exps.insert(key, Instant::now() + Duration::from_millis(ms));
        Ok(RespValue::SimpleString("OK".to_string()))
    }
}

// --- GETRANGE ---

#[derive(Clone)]
pub struct GetRangeCommand {
    store: DataStore,
}

impl GetRangeCommand {
    pub fn new(store: DataStore) -> Self {
        GetRangeCommand { store }
    }
}

#[async_trait]
impl CommandHandler for GetRangeCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'getrange' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let start: i64 = match extract_string(&args[1]).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => {
                return Ok(RespValue::Error(
                    "ERR value is not an integer or out of range".to_string(),
                ))
            }
        };
        let end: i64 = match extract_string(&args[2]).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => {
                return Ok(RespValue::Error(
                    "ERR value is not an integer or out of range".to_string(),
                ))
            }
        };
        let store = self.store.strings.read().await;
        let value = store.get(&key).map(|v| v.as_bytes()).unwrap_or(&[][..]);
        let len = value.len() as i64;
        if len == 0 {
            return Ok(RespValue::BulkString(Some(vec![])));
        }
        let s = if start < 0 {
            (len + start).max(0)
        } else {
            start
        };
        let e = if end < 0 {
            (len + end).max(0)
        } else {
            end.min(len - 1)
        };
        if s > e || s >= len {
            return Ok(RespValue::BulkString(Some(vec![])));
        }
        Ok(RespValue::BulkString(Some(
            value[s as usize..=e as usize].to_vec(),
        )))
    }
}

// --- SETRANGE ---

#[derive(Clone)]
pub struct SetRangeCommand {
    store: DataStore,
}

impl SetRangeCommand {
    pub fn new(store: DataStore) -> Self {
        SetRangeCommand { store }
    }
}

#[async_trait]
impl CommandHandler for SetRangeCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'setrange' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let offset: usize = match extract_string(&args[1]).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => {
                return Ok(RespValue::Error(
                    "ERR value is not an integer or out of range".to_string(),
                ))
            }
        };
        let value = match &args[2] {
            RespValue::BulkString(Some(bytes)) => bytes.clone(),
            _ => return Ok(RespValue::Error("ERR invalid value".to_string())),
        };
        let mut store = self.store.strings.write().await;
        let current = store.entry(key).or_insert_with(String::new);
        let mut bytes = current.as_bytes().to_vec();
        let needed = offset + value.len();
        if bytes.len() < needed {
            bytes.resize(needed, 0);
        }
        bytes[offset..offset + value.len()].copy_from_slice(&value);
        let new_len = bytes.len();
        *current = String::from_utf8_lossy(&bytes).to_string();
        Ok(RespValue::Integer(new_len as i64))
    }
}

// --- GETDEL ---

#[derive(Clone)]
pub struct GetDelCommand {
    store: DataStore,
}

impl GetDelCommand {
    pub fn new(store: DataStore) -> Self {
        GetDelCommand { store }
    }
}

#[async_trait]
impl CommandHandler for GetDelCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'getdel' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let mut store = self.store.strings.write().await;
        match store.remove(&key) {
            Some(v) => Ok(RespValue::BulkString(Some(v.into_bytes()))),
            None => Ok(RespValue::BulkString(None)),
        }
    }
}

// --- GETEX ---

#[derive(Clone)]
pub struct GetExCommand {
    store: DataStore,
}

impl GetExCommand {
    pub fn new(store: DataStore) -> Self {
        GetExCommand { store }
    }
}

#[async_trait]
impl CommandHandler for GetExCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'getex' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let store = self.store.strings.read().await;
        let value = match store.get(&key) {
            Some(v) => v.clone(),
            None => return Ok(RespValue::BulkString(None)),
        };
        drop(store);

        // Parse optional expiry flags
        if args.len() > 1 {
            let flag = match extract_string(&args[1]) {
                Some(f) => f.to_uppercase(),
                None => return Ok(RespValue::Error("ERR syntax error".to_string())),
            };
            let mut exps = self.store.expirations.write().await;
            match flag.as_str() {
                "EX" => {
                    if args.len() != 3 {
                        return Ok(RespValue::Error("ERR syntax error".to_string()));
                    }
                    let secs: u64 = match extract_string(&args[2]).and_then(|s| s.parse().ok()) {
                        Some(v) if v > 0 => v,
                        _ => {
                            return Ok(RespValue::Error(
                                "ERR invalid expire time in 'getex' command".to_string(),
                            ))
                        }
                    };
                    exps.insert(key, Instant::now() + Duration::from_secs(secs));
                }
                "PX" => {
                    if args.len() != 3 {
                        return Ok(RespValue::Error("ERR syntax error".to_string()));
                    }
                    let ms: u64 = match extract_string(&args[2]).and_then(|s| s.parse().ok()) {
                        Some(v) if v > 0 => v,
                        _ => {
                            return Ok(RespValue::Error(
                                "ERR invalid expire time in 'getex' command".to_string(),
                            ))
                        }
                    };
                    exps.insert(key, Instant::now() + Duration::from_millis(ms));
                }
                "EXAT" => {
                    if args.len() != 3 {
                        return Ok(RespValue::Error("ERR syntax error".to_string()));
                    }
                    let ts: u64 = match extract_string(&args[2]).and_then(|s| s.parse().ok()) {
                        Some(v) => v,
                        None => {
                            return Ok(RespValue::Error(
                                "ERR value is not an integer or out of range".to_string(),
                            ))
                        }
                    };
                    let now_sys = std::time::SystemTime::now();
                    let now_epoch = now_sys
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    if ts > now_epoch {
                        let delta = Duration::from_secs(ts - now_epoch);
                        exps.insert(key, Instant::now() + delta);
                    } else {
                        exps.insert(key, Instant::now());
                    }
                }
                "PXAT" => {
                    if args.len() != 3 {
                        return Ok(RespValue::Error("ERR syntax error".to_string()));
                    }
                    let ts_ms: u64 = match extract_string(&args[2]).and_then(|s| s.parse().ok()) {
                        Some(v) => v,
                        None => {
                            return Ok(RespValue::Error(
                                "ERR value is not an integer or out of range".to_string(),
                            ))
                        }
                    };
                    let now_sys = std::time::SystemTime::now();
                    let now_ms = now_sys
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    if ts_ms > now_ms {
                        let delta = Duration::from_millis(ts_ms - now_ms);
                        exps.insert(key, Instant::now() + delta);
                    } else {
                        exps.insert(key, Instant::now());
                    }
                }
                "PERSIST" => {
                    exps.remove(&key);
                }
                _ => return Ok(RespValue::Error("ERR syntax error".to_string())),
            }
        }

        Ok(RespValue::BulkString(Some(value.into_bytes())))
    }
}
