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

// --- HSET ---

#[derive(Clone)]
pub struct HSetCommand {
    store: DataStore,
}

impl HSetCommand {
    pub fn new(store: DataStore) -> Self {
        HSetCommand { store }
    }
}

#[async_trait]
impl CommandHandler for HSetCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 3 || args.len() % 2 != 1 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'hset' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let mut store = self.store.hashes.write().await;
        let hash = store.entry(key).or_default();
        let mut added = 0i64;
        for chunk in args[1..].chunks(2) {
            let field = match extract_string(&chunk[0]) {
                Some(f) => f,
                None => return Ok(RespValue::Error("ERR invalid field".to_string())),
            };
            let value = match extract_string(&chunk[1]) {
                Some(v) => v,
                None => return Ok(RespValue::Error("ERR invalid value".to_string())),
            };
            if hash.insert(field, value).is_none() {
                added += 1;
            }
        }
        Ok(RespValue::Integer(added))
    }
}

// --- HGET ---

#[derive(Clone)]
pub struct HGetCommand {
    store: DataStore,
}

impl HGetCommand {
    pub fn new(store: DataStore) -> Self {
        HGetCommand { store }
    }
}

#[async_trait]
impl CommandHandler for HGetCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'hget' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let field = match extract_string(&args[1]) {
            Some(f) => f,
            None => return Ok(RespValue::Error("ERR invalid field".to_string())),
        };
        let store = self.store.hashes.read().await;
        match store.get(&key).and_then(|h| h.get(&field)) {
            Some(v) => Ok(RespValue::BulkString(Some(v.as_bytes().to_vec()))),
            None => Ok(RespValue::BulkString(None)),
        }
    }
}

// --- HGETALL ---

#[derive(Clone)]
pub struct HGetAllCommand {
    store: DataStore,
}

impl HGetAllCommand {
    pub fn new(store: DataStore) -> Self {
        HGetAllCommand { store }
    }
}

#[async_trait]
impl CommandHandler for HGetAllCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'hgetall' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let store = self.store.hashes.read().await;
        match store.get(&key) {
            Some(hash) => {
                let mut result = Vec::new();
                for (field, value) in hash.iter() {
                    result.push(RespValue::BulkString(Some(field.as_bytes().to_vec())));
                    result.push(RespValue::BulkString(Some(value.as_bytes().to_vec())));
                }
                Ok(RespValue::Array(Some(result)))
            }
            None => Ok(RespValue::Array(Some(vec![]))),
        }
    }
}

// --- HEXISTS ---

#[derive(Clone)]
pub struct HExistsCommand {
    store: DataStore,
}

impl HExistsCommand {
    pub fn new(store: DataStore) -> Self {
        HExistsCommand { store }
    }
}

#[async_trait]
impl CommandHandler for HExistsCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'hexists' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let field = match extract_string(&args[1]) {
            Some(f) => f,
            None => return Ok(RespValue::Error("ERR invalid field".to_string())),
        };
        let store = self.store.hashes.read().await;
        Ok(RespValue::Integer(
            if store.get(&key).is_some_and(|h| h.contains_key(&field)) {
                1
            } else {
                0
            },
        ))
    }
}

// --- HDEL ---

#[derive(Clone)]
pub struct HDelCommand {
    store: DataStore,
}

impl HDelCommand {
    pub fn new(store: DataStore) -> Self {
        HDelCommand { store }
    }
}

#[async_trait]
impl CommandHandler for HDelCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'hdel' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let mut store = self.store.hashes.write().await;
        match store.get_mut(&key) {
            Some(hash) => {
                let mut deleted = 0i64;
                for arg in args.iter().skip(1) {
                    let field = match extract_string(arg) {
                        Some(f) => f,
                        None => return Ok(RespValue::Error("ERR invalid field".to_string())),
                    };
                    if hash.remove(&field).is_some() {
                        deleted += 1;
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

// --- HLEN ---

#[derive(Clone)]
pub struct HLenCommand {
    store: DataStore,
}

impl HLenCommand {
    pub fn new(store: DataStore) -> Self {
        HLenCommand { store }
    }
}

#[async_trait]
impl CommandHandler for HLenCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'hlen' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let store = self.store.hashes.read().await;
        match store.get(&key) {
            Some(hash) => Ok(RespValue::Integer(hash.len() as i64)),
            None => Ok(RespValue::Integer(0)),
        }
    }
}

// --- HKEYS ---

#[derive(Clone)]
pub struct HKeysCommand {
    store: DataStore,
}

impl HKeysCommand {
    pub fn new(store: DataStore) -> Self {
        HKeysCommand { store }
    }
}

#[async_trait]
impl CommandHandler for HKeysCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'hkeys' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let store = self.store.hashes.read().await;
        match store.get(&key) {
            Some(hash) => {
                let keys: Vec<RespValue> = hash
                    .keys()
                    .map(|k| RespValue::BulkString(Some(k.as_bytes().to_vec())))
                    .collect();
                Ok(RespValue::Array(Some(keys)))
            }
            None => Ok(RespValue::Array(Some(vec![]))),
        }
    }
}

// --- HVALS ---

#[derive(Clone)]
pub struct HValsCommand {
    store: DataStore,
}

impl HValsCommand {
    pub fn new(store: DataStore) -> Self {
        HValsCommand { store }
    }
}

#[async_trait]
impl CommandHandler for HValsCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'hvals' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let store = self.store.hashes.read().await;
        match store.get(&key) {
            Some(hash) => {
                let values: Vec<RespValue> = hash
                    .values()
                    .map(|v| RespValue::BulkString(Some(v.as_bytes().to_vec())))
                    .collect();
                Ok(RespValue::Array(Some(values)))
            }
            None => Ok(RespValue::Array(Some(vec![]))),
        }
    }
}

// --- HMGET ---

#[derive(Clone)]
pub struct HMGetCommand {
    store: DataStore,
}

impl HMGetCommand {
    pub fn new(store: DataStore) -> Self {
        HMGetCommand { store }
    }
}

#[async_trait]
impl CommandHandler for HMGetCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'hmget' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let store = self.store.hashes.read().await;
        let hash = store.get(&key);
        let results: Vec<RespValue> = args[1..]
            .iter()
            .map(|arg| {
                let field = match extract_string(arg) {
                    Some(f) => f,
                    None => return RespValue::BulkString(None),
                };
                match hash.and_then(|h| h.get(&field)) {
                    Some(v) => RespValue::BulkString(Some(v.as_bytes().to_vec())),
                    None => RespValue::BulkString(None),
                }
            })
            .collect();
        Ok(RespValue::Array(Some(results)))
    }
}

// --- HINCRBY ---

#[derive(Clone)]
pub struct HIncrByCommand {
    store: DataStore,
}

impl HIncrByCommand {
    pub fn new(store: DataStore) -> Self {
        HIncrByCommand { store }
    }
}

#[async_trait]
impl CommandHandler for HIncrByCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'hincrby' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let field = match extract_string(&args[1]) {
            Some(f) => f,
            None => return Ok(RespValue::Error("ERR invalid field".to_string())),
        };
        let increment: i64 = match extract_string(&args[2]).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => {
                return Ok(RespValue::Error(
                    "ERR value is not an integer or out of range".to_string(),
                ))
            }
        };
        let mut store = self.store.hashes.write().await;
        let hash = store.entry(key).or_default();
        let current: i64 = hash.get(&field).and_then(|v| v.parse().ok()).unwrap_or(0);
        let new_val = current + increment;
        hash.insert(field, new_val.to_string());
        Ok(RespValue::Integer(new_val))
    }
}

// --- HINCRBYFLOAT ---

#[derive(Clone)]
pub struct HIncrByFloatCommand {
    store: DataStore,
}

impl HIncrByFloatCommand {
    pub fn new(store: DataStore) -> Self {
        HIncrByFloatCommand { store }
    }
}

#[async_trait]
impl CommandHandler for HIncrByFloatCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'hincrbyfloat' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let field = match extract_string(&args[1]) {
            Some(f) => f,
            None => return Ok(RespValue::Error("ERR invalid field".to_string())),
        };
        let increment: f64 = match extract_string(&args[2]).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => {
                return Ok(RespValue::Error(
                    "ERR value is not a valid float".to_string(),
                ))
            }
        };
        let mut store = self.store.hashes.write().await;
        let hash = store.entry(key).or_default();
        let current: f64 = hash.get(&field).and_then(|v| v.parse().ok()).unwrap_or(0.0);
        let new_val = current + increment;
        let s = format!("{}", new_val);
        hash.insert(field, s.clone());
        Ok(RespValue::BulkString(Some(s.into_bytes())))
    }
}

// --- HSETNX ---

#[derive(Clone)]
pub struct HSetNxCommand {
    store: DataStore,
}

impl HSetNxCommand {
    pub fn new(store: DataStore) -> Self {
        HSetNxCommand { store }
    }
}

#[async_trait]
impl CommandHandler for HSetNxCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'hsetnx' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let field = match extract_string(&args[1]) {
            Some(f) => f,
            None => return Ok(RespValue::Error("ERR invalid field".to_string())),
        };
        let value = match extract_string(&args[2]) {
            Some(v) => v,
            None => return Ok(RespValue::Error("ERR invalid value".to_string())),
        };
        let mut store = self.store.hashes.write().await;
        let hash = store.entry(key).or_default();
        if let std::collections::hash_map::Entry::Vacant(e) = hash.entry(field) {
            e.insert(value);
            Ok(RespValue::Integer(1))
        } else {
            Ok(RespValue::Integer(0))
        }
    }
}

// --- HSTRLEN ---

#[derive(Clone)]
pub struct HStrLenCommand {
    store: DataStore,
}

impl HStrLenCommand {
    pub fn new(store: DataStore) -> Self {
        HStrLenCommand { store }
    }
}

#[async_trait]
impl CommandHandler for HStrLenCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'hstrlen' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let field = match extract_string(&args[1]) {
            Some(f) => f,
            None => return Ok(RespValue::Error("ERR invalid field".to_string())),
        };
        let store = self.store.hashes.read().await;
        match store.get(&key).and_then(|h| h.get(&field)) {
            Some(v) => Ok(RespValue::Integer(v.len() as i64)),
            None => Ok(RespValue::Integer(0)),
        }
    }
}

// --- HSCAN ---

fn glob_match(pattern: &str, value: &str) -> bool {
    let p = pattern.as_bytes();
    let v = value.as_bytes();
    glob_match_bytes(p, 0, v, 0)
}

fn glob_match_bytes(p: &[u8], pi: usize, v: &[u8], vi: usize) -> bool {
    if pi == p.len() {
        return vi == v.len();
    }
    match p[pi] {
        b'*' => {
            for i in vi..=v.len() {
                if glob_match_bytes(p, pi + 1, v, i) {
                    return true;
                }
            }
            false
        }
        b'?' => {
            if vi < v.len() {
                glob_match_bytes(p, pi + 1, v, vi + 1)
            } else {
                false
            }
        }
        c => {
            if vi < v.len() && c == v[vi] {
                glob_match_bytes(p, pi + 1, v, vi + 1)
            } else {
                false
            }
        }
    }
}

#[derive(Clone)]
pub struct HScanCommand {
    store: DataStore,
}

impl HScanCommand {
    pub fn new(store: DataStore) -> Self {
        HScanCommand { store }
    }
}

#[async_trait]
impl CommandHandler for HScanCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'hscan' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let cursor: usize = match extract_string(&args[1]).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => return Ok(RespValue::Error("ERR invalid cursor".to_string())),
        };
        let mut pattern: Option<String> = None;
        let mut count: usize = 10;
        let mut i = 2;
        while i < args.len() {
            let flag = match extract_string(&args[i]) {
                Some(f) => f.to_uppercase(),
                None => {
                    i += 1;
                    continue;
                }
            };
            match flag.as_str() {
                "MATCH" => {
                    i += 1;
                    pattern = extract_string(&args[i]);
                }
                "COUNT" => {
                    i += 1;
                    count = extract_string(&args[i])
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(10);
                }
                _ => {}
            }
            i += 1;
        }

        let store = self.store.hashes.read().await;
        match store.get(&key) {
            Some(hash) => {
                let mut fields: Vec<(&String, &String)> = hash.iter().collect();
                fields.sort_by_key(|(k, _)| k.as_str());

                let filtered: Vec<(&String, &String)> = if let Some(ref pat) = pattern {
                    fields
                        .into_iter()
                        .filter(|(k, _)| glob_match(pat, k))
                        .collect()
                } else {
                    fields
                };

                let start = cursor;
                let end = (start + count).min(filtered.len());
                let next_cursor = if end >= filtered.len() { 0 } else { end };

                let mut items = Vec::new();
                if start < filtered.len() {
                    for (field, value) in &filtered[start..end] {
                        items.push(RespValue::BulkString(Some(field.as_bytes().to_vec())));
                        items.push(RespValue::BulkString(Some(value.as_bytes().to_vec())));
                    }
                }

                Ok(RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(next_cursor.to_string().into_bytes())),
                    RespValue::Array(Some(items)),
                ])))
            }
            None => Ok(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"0".to_vec())),
                RespValue::Array(Some(vec![])),
            ]))),
        }
    }
}

// --- HRANDFIELD ---

#[derive(Clone)]
pub struct HRandFieldCommand {
    store: DataStore,
}

impl HRandFieldCommand {
    pub fn new(store: DataStore) -> Self {
        HRandFieldCommand { store }
    }
}

#[async_trait]
impl CommandHandler for HRandFieldCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'hrandfield' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let count: Option<i64> = if args.len() >= 2 {
            extract_string(&args[1]).and_then(|s| s.parse().ok())
        } else {
            None
        };
        let withvalues = args.len() >= 3
            && extract_string(&args[2])
                .map(|s| s.to_uppercase() == "WITHVALUES")
                .unwrap_or(false);

        let store = self.store.hashes.read().await;
        match store.get(&key) {
            Some(hash) if !hash.is_empty() => {
                use rand::seq::SliceRandom;
                let fields: Vec<(&String, &String)> = hash.iter().collect();

                match count {
                    None => {
                        let mut rng = rand::thread_rng();
                        let (field, _) = fields.choose(&mut rng).unwrap();
                        Ok(RespValue::BulkString(Some(field.as_bytes().to_vec())))
                    }
                    Some(n) if n >= 0 => {
                        let mut rng = rand::thread_rng();
                        let n = (n as usize).min(fields.len());
                        let mut indices: Vec<usize> = (0..fields.len()).collect();
                        indices.shuffle(&mut rng);
                        indices.truncate(n);
                        let mut result = Vec::new();
                        for idx in indices {
                            let (field, value) = &fields[idx];
                            result.push(RespValue::BulkString(Some(field.as_bytes().to_vec())));
                            if withvalues {
                                result.push(RespValue::BulkString(Some(value.as_bytes().to_vec())));
                            }
                        }
                        Ok(RespValue::Array(Some(result)))
                    }
                    Some(n) => {
                        // Negative count: allow duplicates
                        use rand::Rng;
                        let mut rng = rand::thread_rng();
                        let abs_n = (-n) as usize;
                        let mut result = Vec::new();
                        for _ in 0..abs_n {
                            let idx = rng.gen_range(0..fields.len());
                            let (field, value) = &fields[idx];
                            result.push(RespValue::BulkString(Some(field.as_bytes().to_vec())));
                            if withvalues {
                                result.push(RespValue::BulkString(Some(value.as_bytes().to_vec())));
                            }
                        }
                        Ok(RespValue::Array(Some(result)))
                    }
                }
            }
            _ => {
                if count.is_some() {
                    Ok(RespValue::Array(Some(vec![])))
                } else {
                    Ok(RespValue::BulkString(None))
                }
            }
        }
    }
}
