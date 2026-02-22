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
        let mut sorted_sets = self.store.sorted_sets.write().await;
        let mut hll = self.store.hll.write().await;
        let mut bitmaps = self.store.bitmaps.write().await;
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
            if sorted_sets.remove(&key).is_some() {
                found = true;
            }
            if hll.remove(&key).is_some() {
                found = true;
            }
            if bitmaps.remove(&key).is_some() {
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
        let sorted_sets = self.store.sorted_sets.read().await;
        let hll = self.store.hll.read().await;
        let bitmaps = self.store.bitmaps.read().await;
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
                || sorted_sets.contains_key(&key)
                || hll.contains_key(&key)
                || bitmaps.contains_key(&key)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::CommandHandler;

    fn bulk(s: &str) -> RespValue {
        RespValue::BulkString(Some(s.as_bytes().to_vec()))
    }

    // 1. test_set_get - basic SET then GET
    #[tokio::test]
    async fn test_set_get() {
        let store = DataStore::new();
        let set_cmd = SetCommand::new(store.clone());
        let get_cmd = GetCommand::new(store.clone());

        // SET mykey myvalue
        let res = set_cmd
            .execute(vec![bulk("mykey"), bulk("myvalue")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::SimpleString("OK".to_string()));

        // GET mykey
        let res = get_cmd.execute(vec![bulk("mykey")]).await.unwrap();
        assert_eq!(res, RespValue::BulkString(Some(b"myvalue".to_vec())));

        // GET nonexistent
        let res = get_cmd.execute(vec![bulk("nokey")]).await.unwrap();
        assert_eq!(res, RespValue::BulkString(None));
    }

    // 2. test_set_nx_xx - NX and XX flags
    #[tokio::test]
    async fn test_set_nx_xx() {
        let store = DataStore::new();
        let set_cmd = SetCommand::new(store.clone());
        let get_cmd = GetCommand::new(store.clone());

        // SET key val NX on nonexistent key -> should succeed
        let res = set_cmd
            .execute(vec![bulk("k"), bulk("v1"), bulk("NX")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::SimpleString("OK".to_string()));

        // SET key val NX on existing key -> should return nil
        let res = set_cmd
            .execute(vec![bulk("k"), bulk("v2"), bulk("NX")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::BulkString(None));

        // Value should still be v1
        let res = get_cmd.execute(vec![bulk("k")]).await.unwrap();
        assert_eq!(res, RespValue::BulkString(Some(b"v1".to_vec())));

        // SET key val XX on existing key -> should succeed
        let res = set_cmd
            .execute(vec![bulk("k"), bulk("v3"), bulk("XX")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::SimpleString("OK".to_string()));

        let res = get_cmd.execute(vec![bulk("k")]).await.unwrap();
        assert_eq!(res, RespValue::BulkString(Some(b"v3".to_vec())));

        // SET key val XX on nonexistent key -> should return nil
        let res = set_cmd
            .execute(vec![bulk("new"), bulk("v"), bulk("XX")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::BulkString(None));
    }

    // 3. test_set_ex - SET with EX expiration
    #[tokio::test]
    async fn test_set_ex() {
        let store = DataStore::new();
        let set_cmd = SetCommand::new(store.clone());

        // SET key val EX 100
        let res = set_cmd
            .execute(vec![bulk("k"), bulk("v"), bulk("EX"), bulk("100")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::SimpleString("OK".to_string()));

        // Verify expiration was set
        let exps = store.expirations.read().await;
        assert!(exps.contains_key("k"));
    }

    // 4. test_set_get_flag - SET with GET flag
    #[tokio::test]
    async fn test_set_get_flag() {
        let store = DataStore::new();
        let set_cmd = SetCommand::new(store.clone());

        // SET key val GET on nonexistent key -> returns nil
        let res = set_cmd
            .execute(vec![bulk("k"), bulk("v1"), bulk("GET")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::BulkString(None));

        // SET key val GET on existing key -> returns old value
        let res = set_cmd
            .execute(vec![bulk("k"), bulk("v2"), bulk("GET")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::BulkString(Some(b"v1".to_vec())));
    }

    // 5. test_del_single_multi - DEL one key and multiple keys
    #[tokio::test]
    async fn test_del_single_multi() {
        let store = DataStore::new();
        let set_cmd = SetCommand::new(store.clone());
        let del_cmd = DelCommand::new(store.clone());

        set_cmd.execute(vec![bulk("a"), bulk("1")]).await.unwrap();
        set_cmd.execute(vec![bulk("b"), bulk("2")]).await.unwrap();
        set_cmd.execute(vec![bulk("c"), bulk("3")]).await.unwrap();

        // DEL single key
        let res = del_cmd.execute(vec![bulk("a")]).await.unwrap();
        assert_eq!(res, RespValue::Integer(1));

        // DEL multiple keys (b exists, d does not)
        let res = del_cmd
            .execute(vec![bulk("b"), bulk("c"), bulk("d")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Integer(2));

        // DEL nonexistent key
        let res = del_cmd.execute(vec![bulk("a")]).await.unwrap();
        assert_eq!(res, RespValue::Integer(0));
    }

    // 6. test_exists - EXISTS single and multi-key
    #[tokio::test]
    async fn test_exists() {
        let store = DataStore::new();
        let set_cmd = SetCommand::new(store.clone());
        let exists_cmd = ExistsCommand::new(store.clone());

        set_cmd.execute(vec![bulk("a"), bulk("1")]).await.unwrap();
        set_cmd.execute(vec![bulk("b"), bulk("2")]).await.unwrap();

        let res = exists_cmd.execute(vec![bulk("a")]).await.unwrap();
        assert_eq!(res, RespValue::Integer(1));

        let res = exists_cmd.execute(vec![bulk("nonexistent")]).await.unwrap();
        assert_eq!(res, RespValue::Integer(0));

        // Multiple keys, a and b exist, c does not
        let res = exists_cmd
            .execute(vec![bulk("a"), bulk("b"), bulk("c")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Integer(2));
    }

    // 7. test_append - APPEND to existing and new key
    #[tokio::test]
    async fn test_append() {
        let store = DataStore::new();
        let append_cmd = AppendCommand::new(store.clone());
        let get_cmd = GetCommand::new(store.clone());

        // APPEND to nonexistent key creates it
        let res = append_cmd
            .execute(vec![bulk("k"), bulk("hello")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Integer(5));

        // APPEND to existing key
        let res = append_cmd
            .execute(vec![bulk("k"), bulk(" world")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Integer(11));

        let res = get_cmd.execute(vec![bulk("k")]).await.unwrap();
        assert_eq!(res, RespValue::BulkString(Some(b"hello world".to_vec())));
    }

    // 8. test_strlen - STRLEN for existing and missing keys
    #[tokio::test]
    async fn test_strlen() {
        let store = DataStore::new();
        let set_cmd = SetCommand::new(store.clone());
        let strlen_cmd = StrlenCommand::new(store.clone());

        // STRLEN of nonexistent key -> 0
        let res = strlen_cmd.execute(vec![bulk("k")]).await.unwrap();
        assert_eq!(res, RespValue::Integer(0));

        set_cmd
            .execute(vec![bulk("k"), bulk("hello")])
            .await
            .unwrap();

        let res = strlen_cmd.execute(vec![bulk("k")]).await.unwrap();
        assert_eq!(res, RespValue::Integer(5));
    }

    // 9. test_incr_decr - INCR and DECR
    #[tokio::test]
    async fn test_incr_decr() {
        let store = DataStore::new();
        let set_cmd = SetCommand::new(store.clone());
        let incr_cmd = IncrCommand::new(store.clone());
        let decr_cmd = DecrCommand::new(store.clone());

        // INCR nonexistent key starts from 0
        let res = incr_cmd.execute(vec![bulk("counter")]).await.unwrap();
        assert_eq!(res, RespValue::Integer(1));

        let res = incr_cmd.execute(vec![bulk("counter")]).await.unwrap();
        assert_eq!(res, RespValue::Integer(2));

        let res = decr_cmd.execute(vec![bulk("counter")]).await.unwrap();
        assert_eq!(res, RespValue::Integer(1));

        // INCR on non-integer value
        set_cmd
            .execute(vec![bulk("str"), bulk("abc")])
            .await
            .unwrap();
        let res = incr_cmd.execute(vec![bulk("str")]).await.unwrap();
        assert!(matches!(res, RespValue::Error(_)));
    }

    // 10. test_incrby_decrby - INCRBY and DECRBY
    #[tokio::test]
    async fn test_incrby_decrby() {
        let store = DataStore::new();
        let set_cmd = SetCommand::new(store.clone());
        let incrby_cmd = IncrByCommand::new(store.clone());
        let decrby_cmd = DecrByCommand::new(store.clone());

        set_cmd.execute(vec![bulk("k"), bulk("10")]).await.unwrap();

        let res = incrby_cmd
            .execute(vec![bulk("k"), bulk("5")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Integer(15));

        let res = decrby_cmd
            .execute(vec![bulk("k"), bulk("3")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Integer(12));

        // INCRBY on nonexistent key
        let res = incrby_cmd
            .execute(vec![bulk("new"), bulk("7")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Integer(7));
    }

    // 11. test_incrbyfloat - INCRBYFLOAT
    #[tokio::test]
    async fn test_incrbyfloat() {
        let store = DataStore::new();
        let set_cmd = SetCommand::new(store.clone());
        let incrbyfloat_cmd = IncrByFloatCommand::new(store.clone());

        set_cmd.execute(vec![bulk("k"), bulk("10")]).await.unwrap();

        let res = incrbyfloat_cmd
            .execute(vec![bulk("k"), bulk("1.5")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::BulkString(Some(b"11.5".to_vec())));

        // Float increment on nonexistent key (starts at 0)
        let res = incrbyfloat_cmd
            .execute(vec![bulk("new"), bulk("3.14")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::BulkString(Some(b"3.14".to_vec())));

        // Negative increment
        let res = incrbyfloat_cmd
            .execute(vec![bulk("new"), bulk("-1.14")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::BulkString(Some(b"2".to_vec())));
    }

    // 12. test_mget_mset - MGET and MSET
    #[tokio::test]
    async fn test_mget_mset() {
        let store = DataStore::new();
        let mset_cmd = MSetCommand::new(store.clone());
        let mget_cmd = MGetCommand::new(store.clone());

        // MSET k1 v1 k2 v2
        let res = mset_cmd
            .execute(vec![bulk("k1"), bulk("v1"), bulk("k2"), bulk("v2")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::SimpleString("OK".to_string()));

        // MGET k1 k2 k3(missing)
        let res = mget_cmd
            .execute(vec![bulk("k1"), bulk("k2"), bulk("k3")])
            .await
            .unwrap();
        assert_eq!(
            res,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"v1".to_vec())),
                RespValue::BulkString(Some(b"v2".to_vec())),
                RespValue::BulkString(None),
            ]))
        );
    }

    // 13. test_msetnx - MSETNX all-or-nothing semantics
    #[tokio::test]
    async fn test_msetnx() {
        let store = DataStore::new();
        let msetnx_cmd = MSetNxCommand::new(store.clone());
        let get_cmd = GetCommand::new(store.clone());

        // MSETNX when none exist -> 1
        let res = msetnx_cmd
            .execute(vec![bulk("a"), bulk("1"), bulk("b"), bulk("2")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Integer(1));

        // MSETNX when one key already exists -> 0, none should be set
        let res = msetnx_cmd
            .execute(vec![bulk("b"), bulk("new"), bulk("c"), bulk("3")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Integer(0));

        // c should not exist since msetnx was atomic
        let res = get_cmd.execute(vec![bulk("c")]).await.unwrap();
        assert_eq!(res, RespValue::BulkString(None));

        // b should still have old value
        let res = get_cmd.execute(vec![bulk("b")]).await.unwrap();
        assert_eq!(res, RespValue::BulkString(Some(b"2".to_vec())));
    }

    // 14. test_setnx - SETNX
    #[tokio::test]
    async fn test_setnx() {
        let store = DataStore::new();
        let setnx_cmd = SetNxCommand::new(store.clone());
        let get_cmd = GetCommand::new(store.clone());

        // SETNX on nonexistent key -> 1
        let res = setnx_cmd
            .execute(vec![bulk("k"), bulk("v1")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Integer(1));

        // SETNX on existing key -> 0
        let res = setnx_cmd
            .execute(vec![bulk("k"), bulk("v2")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Integer(0));

        // Value unchanged
        let res = get_cmd.execute(vec![bulk("k")]).await.unwrap();
        assert_eq!(res, RespValue::BulkString(Some(b"v1".to_vec())));
    }

    // 15. test_setex - SETEX
    #[tokio::test]
    async fn test_setex() {
        let store = DataStore::new();
        let setex_cmd = SetExCommand::new(store.clone());
        let get_cmd = GetCommand::new(store.clone());

        // SETEX key 60 value
        let res = setex_cmd
            .execute(vec![bulk("k"), bulk("60"), bulk("val")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::SimpleString("OK".to_string()));

        let res = get_cmd.execute(vec![bulk("k")]).await.unwrap();
        assert_eq!(res, RespValue::BulkString(Some(b"val".to_vec())));

        // Expiration should be set
        let exps = store.expirations.read().await;
        assert!(exps.contains_key("k"));
    }

    // 16. test_psetex - PSETEX
    #[tokio::test]
    async fn test_psetex() {
        let store = DataStore::new();
        let psetex_cmd = PSetExCommand::new(store.clone());
        let get_cmd = GetCommand::new(store.clone());

        // PSETEX key 60000 value
        let res = psetex_cmd
            .execute(vec![bulk("k"), bulk("60000"), bulk("val")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::SimpleString("OK".to_string()));

        let res = get_cmd.execute(vec![bulk("k")]).await.unwrap();
        assert_eq!(res, RespValue::BulkString(Some(b"val".to_vec())));

        let exps = store.expirations.read().await;
        assert!(exps.contains_key("k"));
    }

    // 17. test_getrange - GETRANGE
    #[tokio::test]
    async fn test_getrange() {
        let store = DataStore::new();
        let set_cmd = SetCommand::new(store.clone());
        let getrange_cmd = GetRangeCommand::new(store.clone());

        set_cmd
            .execute(vec![bulk("k"), bulk("Hello, World!")])
            .await
            .unwrap();

        // GETRANGE k 0 4 -> "Hello"
        let res = getrange_cmd
            .execute(vec![bulk("k"), bulk("0"), bulk("4")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::BulkString(Some(b"Hello".to_vec())));

        // Negative indices: GETRANGE k -6 -1 -> "World!"
        let res = getrange_cmd
            .execute(vec![bulk("k"), bulk("-6"), bulk("-1")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::BulkString(Some(b"World!".to_vec())));

        // Out of range -> empty
        let res = getrange_cmd
            .execute(vec![bulk("k"), bulk("50"), bulk("100")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::BulkString(Some(vec![])));

        // GETRANGE on nonexistent key -> empty
        let res = getrange_cmd
            .execute(vec![bulk("nokey"), bulk("0"), bulk("5")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::BulkString(Some(vec![])));
    }

    // 18. test_setrange - SETRANGE
    #[tokio::test]
    async fn test_setrange() {
        let store = DataStore::new();
        let set_cmd = SetCommand::new(store.clone());
        let get_cmd = GetCommand::new(store.clone());
        let setrange_cmd = SetRangeCommand::new(store.clone());

        set_cmd
            .execute(vec![bulk("k"), bulk("Hello World")])
            .await
            .unwrap();

        // SETRANGE k 6 Redis -> "Hello Redis"
        let res = setrange_cmd
            .execute(vec![bulk("k"), bulk("6"), bulk("Redis")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Integer(11));

        let res = get_cmd.execute(vec![bulk("k")]).await.unwrap();
        assert_eq!(res, RespValue::BulkString(Some(b"Hello Redis".to_vec())));

        // SETRANGE on nonexistent key with offset -> zero-padded
        let res = setrange_cmd
            .execute(vec![bulk("new"), bulk("5"), bulk("abc")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Integer(8));
    }

    // 19. test_getdel - GETDEL
    #[tokio::test]
    async fn test_getdel() {
        let store = DataStore::new();
        let set_cmd = SetCommand::new(store.clone());
        let get_cmd = GetCommand::new(store.clone());
        let getdel_cmd = GetDelCommand::new(store.clone());

        set_cmd.execute(vec![bulk("k"), bulk("val")]).await.unwrap();

        // GETDEL returns value and deletes it
        let res = getdel_cmd.execute(vec![bulk("k")]).await.unwrap();
        assert_eq!(res, RespValue::BulkString(Some(b"val".to_vec())));

        // Key should be gone
        let res = get_cmd.execute(vec![bulk("k")]).await.unwrap();
        assert_eq!(res, RespValue::BulkString(None));

        // GETDEL on nonexistent key -> nil
        let res = getdel_cmd.execute(vec![bulk("nokey")]).await.unwrap();
        assert_eq!(res, RespValue::BulkString(None));
    }

    // 20. test_getex_persist - GETEX with PERSIST
    #[tokio::test]
    async fn test_getex_persist() {
        let store = DataStore::new();
        let setex_cmd = SetExCommand::new(store.clone());
        let getex_cmd = GetExCommand::new(store.clone());

        // Set key with expiration
        setex_cmd
            .execute(vec![bulk("k"), bulk("100"), bulk("val")])
            .await
            .unwrap();

        // Verify expiration exists
        {
            let exps = store.expirations.read().await;
            assert!(exps.contains_key("k"));
        }

        // GETEX k PERSIST -> returns value and removes expiration
        let res = getex_cmd
            .execute(vec![bulk("k"), bulk("PERSIST")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::BulkString(Some(b"val".to_vec())));

        // Expiration should be removed
        let exps = store.expirations.read().await;
        assert!(!exps.contains_key("k"));
    }

    // 21. test_wrong_args - error handling for wrong argument counts
    #[tokio::test]
    async fn test_wrong_args() {
        let store = DataStore::new();

        // SET with too few args
        let res = SetCommand::new(store.clone())
            .execute(vec![])
            .await
            .unwrap();
        assert!(matches!(res, RespValue::Error(_)));

        let res = SetCommand::new(store.clone())
            .execute(vec![bulk("k")])
            .await
            .unwrap();
        assert!(matches!(res, RespValue::Error(_)));

        // GET with wrong arg count
        let res = GetCommand::new(store.clone())
            .execute(vec![])
            .await
            .unwrap();
        assert!(matches!(res, RespValue::Error(_)));

        let res = GetCommand::new(store.clone())
            .execute(vec![bulk("a"), bulk("b")])
            .await
            .unwrap();
        assert!(matches!(res, RespValue::Error(_)));

        // DEL with no args
        let res = DelCommand::new(store.clone())
            .execute(vec![])
            .await
            .unwrap();
        assert!(matches!(res, RespValue::Error(_)));

        // EXISTS with no args
        let res = ExistsCommand::new(store.clone())
            .execute(vec![])
            .await
            .unwrap();
        assert!(matches!(res, RespValue::Error(_)));

        // APPEND with wrong args
        let res = AppendCommand::new(store.clone())
            .execute(vec![bulk("k")])
            .await
            .unwrap();
        assert!(matches!(res, RespValue::Error(_)));

        // STRLEN with wrong args
        let res = StrlenCommand::new(store.clone())
            .execute(vec![])
            .await
            .unwrap();
        assert!(matches!(res, RespValue::Error(_)));

        // INCR with wrong args
        let res = IncrCommand::new(store.clone())
            .execute(vec![])
            .await
            .unwrap();
        assert!(matches!(res, RespValue::Error(_)));

        // DECR with wrong args
        let res = DecrCommand::new(store.clone())
            .execute(vec![])
            .await
            .unwrap();
        assert!(matches!(res, RespValue::Error(_)));

        // INCRBY with wrong args
        let res = IncrByCommand::new(store.clone())
            .execute(vec![bulk("k")])
            .await
            .unwrap();
        assert!(matches!(res, RespValue::Error(_)));

        // DECRBY with wrong args
        let res = DecrByCommand::new(store.clone())
            .execute(vec![bulk("k")])
            .await
            .unwrap();
        assert!(matches!(res, RespValue::Error(_)));

        // INCRBYFLOAT with wrong args
        let res = IncrByFloatCommand::new(store.clone())
            .execute(vec![])
            .await
            .unwrap();
        assert!(matches!(res, RespValue::Error(_)));

        // MGET with no args
        let res = MGetCommand::new(store.clone())
            .execute(vec![])
            .await
            .unwrap();
        assert!(matches!(res, RespValue::Error(_)));

        // MSET with odd args
        let res = MSetCommand::new(store.clone())
            .execute(vec![bulk("k")])
            .await
            .unwrap();
        assert!(matches!(res, RespValue::Error(_)));

        // MSETNX with odd args
        let res = MSetNxCommand::new(store.clone())
            .execute(vec![bulk("k")])
            .await
            .unwrap();
        assert!(matches!(res, RespValue::Error(_)));

        // SETNX with wrong args
        let res = SetNxCommand::new(store.clone())
            .execute(vec![bulk("k")])
            .await
            .unwrap();
        assert!(matches!(res, RespValue::Error(_)));

        // SETEX with wrong args
        let res = SetExCommand::new(store.clone())
            .execute(vec![bulk("k")])
            .await
            .unwrap();
        assert!(matches!(res, RespValue::Error(_)));

        // PSETEX with wrong args
        let res = PSetExCommand::new(store.clone())
            .execute(vec![bulk("k")])
            .await
            .unwrap();
        assert!(matches!(res, RespValue::Error(_)));

        // GETRANGE with wrong args
        let res = GetRangeCommand::new(store.clone())
            .execute(vec![bulk("k")])
            .await
            .unwrap();
        assert!(matches!(res, RespValue::Error(_)));

        // SETRANGE with wrong args
        let res = SetRangeCommand::new(store.clone())
            .execute(vec![bulk("k")])
            .await
            .unwrap();
        assert!(matches!(res, RespValue::Error(_)));

        // GETDEL with wrong args
        let res = GetDelCommand::new(store.clone())
            .execute(vec![])
            .await
            .unwrap();
        assert!(matches!(res, RespValue::Error(_)));

        // GETEX with no args
        let res = GetExCommand::new(store.clone())
            .execute(vec![])
            .await
            .unwrap();
        assert!(matches!(res, RespValue::Error(_)));
    }
}
