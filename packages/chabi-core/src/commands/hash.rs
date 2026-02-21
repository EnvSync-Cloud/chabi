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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::CommandHandler;

    fn bulk(s: &str) -> RespValue {
        RespValue::BulkString(Some(s.as_bytes().to_vec()))
    }

    // Helper: create a shared DataStore and HSET some fields into it.
    async fn seed_hash(store: &DataStore, key: &str, pairs: &[(&str, &str)]) {
        let mut args = vec![bulk(key)];
        for (f, v) in pairs {
            args.push(bulk(f));
            args.push(bulk(v));
        }
        let cmd = HSetCommand::new(store.clone());
        cmd.execute(args).await.unwrap();
    }

    // 1. HSET then HGET
    #[tokio::test]
    async fn test_hset_hget() {
        let store = DataStore::new();
        let hset = HSetCommand::new(store.clone());
        let result = hset
            .execute(vec![bulk("myhash"), bulk("field1"), bulk("value1")])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let hget = HGetCommand::new(store.clone());
        let result = hget
            .execute(vec![bulk("myhash"), bulk("field1")])
            .await
            .unwrap();
        assert_eq!(result, bulk("value1"));

        // Non-existent field returns nil
        let result = hget
            .execute(vec![bulk("myhash"), bulk("nosuchfield")])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }

    // 2. HSET with multiple field-value pairs
    #[tokio::test]
    async fn test_hset_multiple() {
        let store = DataStore::new();
        let hset = HSetCommand::new(store.clone());
        let result = hset
            .execute(vec![
                bulk("myhash"),
                bulk("f1"),
                bulk("v1"),
                bulk("f2"),
                bulk("v2"),
                bulk("f3"),
                bulk("v3"),
            ])
            .await
            .unwrap();
        // All three fields are new
        assert_eq!(result, RespValue::Integer(3));

        // Overwriting existing fields should return 0 for those
        let result = hset
            .execute(vec![
                bulk("myhash"),
                bulk("f1"),
                bulk("updated"),
                bulk("f4"),
                bulk("v4"),
            ])
            .await
            .unwrap();
        // f1 already exists (not counted), f4 is new
        assert_eq!(result, RespValue::Integer(1));

        // Verify overwritten value
        let hget = HGetCommand::new(store.clone());
        let result = hget
            .execute(vec![bulk("myhash"), bulk("f1")])
            .await
            .unwrap();
        assert_eq!(result, bulk("updated"));
    }

    // 3. HGETALL returns all field-value pairs
    #[tokio::test]
    async fn test_hgetall() {
        let store = DataStore::new();
        seed_hash(&store, "h", &[("a", "1"), ("b", "2")]).await;

        let cmd = HGetAllCommand::new(store.clone());
        let result = cmd.execute(vec![bulk("h")]).await.unwrap();

        if let RespValue::Array(Some(items)) = result {
            // Should contain 4 elements: field, value, field, value
            assert_eq!(items.len(), 4);
            // Collect into a HashMap for order-independent checking
            let mut map = std::collections::HashMap::new();
            for chunk in items.chunks(2) {
                if let (RespValue::BulkString(Some(k)), RespValue::BulkString(Some(v))) =
                    (&chunk[0], &chunk[1])
                {
                    map.insert(
                        String::from_utf8(k.clone()).unwrap(),
                        String::from_utf8(v.clone()).unwrap(),
                    );
                }
            }
            assert_eq!(map.get("a").unwrap(), "1");
            assert_eq!(map.get("b").unwrap(), "2");
        } else {
            panic!("Expected Array, got {:?}", result);
        }

        // Non-existent key returns empty array
        let result = cmd.execute(vec![bulk("nokey")]).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    // 4. HEXISTS returns 1/0
    #[tokio::test]
    async fn test_hexists() {
        let store = DataStore::new();
        seed_hash(&store, "h", &[("exists", "yes")]).await;

        let cmd = HExistsCommand::new(store.clone());
        let result = cmd.execute(vec![bulk("h"), bulk("exists")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = cmd.execute(vec![bulk("h"), bulk("nope")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // Non-existent key
        let result = cmd.execute(vec![bulk("nokey"), bulk("f")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    // 5. HDEL removes fields
    #[tokio::test]
    async fn test_hdel() {
        let store = DataStore::new();
        seed_hash(&store, "h", &[("a", "1"), ("b", "2"), ("c", "3")]).await;

        let cmd = HDelCommand::new(store.clone());
        // Delete two fields, one existing and one not
        let result = cmd
            .execute(vec![bulk("h"), bulk("a"), bulk("nonexistent")])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(1)); // only "a" existed

        // Verify "a" is gone
        let hget = HGetCommand::new(store.clone());
        let result = hget.execute(vec![bulk("h"), bulk("a")]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));

        // Delete from non-existent key
        let result = cmd.execute(vec![bulk("nokey"), bulk("x")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    // 6. HLEN returns count
    #[tokio::test]
    async fn test_hlen() {
        let store = DataStore::new();
        let cmd = HLenCommand::new(store.clone());

        // Non-existent key
        let result = cmd.execute(vec![bulk("h")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));

        seed_hash(&store, "h", &[("a", "1"), ("b", "2"), ("c", "3")]).await;
        let result = cmd.execute(vec![bulk("h")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(3));
    }

    // 7. HKEYS returns field names
    #[tokio::test]
    async fn test_hkeys() {
        let store = DataStore::new();
        seed_hash(&store, "h", &[("x", "1"), ("y", "2")]).await;

        let cmd = HKeysCommand::new(store.clone());
        let result = cmd.execute(vec![bulk("h")]).await.unwrap();

        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
            let mut keys: Vec<String> = items
                .into_iter()
                .map(|v| {
                    if let RespValue::BulkString(Some(b)) = v {
                        String::from_utf8(b).unwrap()
                    } else {
                        panic!("unexpected value");
                    }
                })
                .collect();
            keys.sort();
            assert_eq!(keys, vec!["x", "y"]);
        } else {
            panic!("Expected Array");
        }

        // Empty key
        let result = cmd.execute(vec![bulk("nokey")]).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    // 8. HVALS returns values
    #[tokio::test]
    async fn test_hvals() {
        let store = DataStore::new();
        seed_hash(&store, "h", &[("a", "10"), ("b", "20")]).await;

        let cmd = HValsCommand::new(store.clone());
        let result = cmd.execute(vec![bulk("h")]).await.unwrap();

        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
            let mut vals: Vec<String> = items
                .into_iter()
                .map(|v| {
                    if let RespValue::BulkString(Some(b)) = v {
                        String::from_utf8(b).unwrap()
                    } else {
                        panic!("unexpected value");
                    }
                })
                .collect();
            vals.sort();
            assert_eq!(vals, vec!["10", "20"]);
        } else {
            panic!("Expected Array");
        }
    }

    // 9. HMGET returns array of values/nulls
    #[tokio::test]
    async fn test_hmget() {
        let store = DataStore::new();
        seed_hash(&store, "h", &[("f1", "v1"), ("f2", "v2")]).await;

        let cmd = HMGetCommand::new(store.clone());
        let result = cmd
            .execute(vec![bulk("h"), bulk("f1"), bulk("missing"), bulk("f2")])
            .await
            .unwrap();

        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                bulk("v1"),
                RespValue::BulkString(None),
                bulk("v2"),
            ]))
        );

        // Non-existent key: all nil
        let result = cmd
            .execute(vec![bulk("nokey"), bulk("a"), bulk("b")])
            .await
            .unwrap();
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(None),
                RespValue::BulkString(None),
            ]))
        );
    }

    // 10. HINCRBY increments integer field
    #[tokio::test]
    async fn test_hincrby() {
        let store = DataStore::new();
        let cmd = HIncrByCommand::new(store.clone());

        // Increment non-existent field (starts at 0)
        let result = cmd
            .execute(vec![bulk("h"), bulk("counter"), bulk("5")])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(5));

        // Increment again
        let result = cmd
            .execute(vec![bulk("h"), bulk("counter"), bulk("3")])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(8));

        // Negative increment
        let result = cmd
            .execute(vec![bulk("h"), bulk("counter"), bulk("-2")])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(6));

        // Verify via HGET
        let hget = HGetCommand::new(store.clone());
        let result = hget
            .execute(vec![bulk("h"), bulk("counter")])
            .await
            .unwrap();
        assert_eq!(result, bulk("6"));
    }

    // 11. HINCRBYFLOAT increments float field
    #[tokio::test]
    async fn test_hincrbyfloat() {
        let store = DataStore::new();
        let cmd = HIncrByFloatCommand::new(store.clone());

        // Non-existent field starts at 0.0
        let result = cmd
            .execute(vec![bulk("h"), bulk("price"), bulk("10.5")])
            .await
            .unwrap();
        assert_eq!(result, bulk("10.5"));

        // Increment by negative
        let result = cmd
            .execute(vec![bulk("h"), bulk("price"), bulk("-3.2")])
            .await
            .unwrap();
        // 10.5 - 3.2 = 7.3
        if let RespValue::BulkString(Some(bytes)) = result {
            let val: f64 = String::from_utf8(bytes).unwrap().parse().unwrap();
            assert!((val - 7.3).abs() < 1e-9);
        } else {
            panic!("Expected BulkString");
        }
    }

    // 12. HSETNX only sets if field doesn't exist
    #[tokio::test]
    async fn test_hsetnx() {
        let store = DataStore::new();
        let cmd = HSetNxCommand::new(store.clone());

        // Field doesn't exist -> set it
        let result = cmd
            .execute(vec![bulk("h"), bulk("f"), bulk("first")])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Field already exists -> no-op
        let result = cmd
            .execute(vec![bulk("h"), bulk("f"), bulk("second")])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // Verify original value is retained
        let hget = HGetCommand::new(store.clone());
        let result = hget.execute(vec![bulk("h"), bulk("f")]).await.unwrap();
        assert_eq!(result, bulk("first"));
    }

    // 13. HSTRLEN returns length of field value
    #[tokio::test]
    async fn test_hstrlen() {
        let store = DataStore::new();
        seed_hash(&store, "h", &[("greeting", "hello")]).await;

        let cmd = HStrLenCommand::new(store.clone());
        let result = cmd
            .execute(vec![bulk("h"), bulk("greeting")])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(5));

        // Non-existent field
        let result = cmd.execute(vec![bulk("h"), bulk("nope")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // Non-existent key
        let result = cmd.execute(vec![bulk("nokey"), bulk("f")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    // 14. HSCAN basic iteration
    #[tokio::test]
    async fn test_hscan() {
        let store = DataStore::new();
        seed_hash(
            &store,
            "h",
            &[("alpha", "1"), ("beta", "2"), ("gamma", "3")],
        )
        .await;

        let cmd = HScanCommand::new(store.clone());

        // Full scan with cursor 0 and high count
        let result = cmd
            .execute(vec![bulk("h"), bulk("0"), bulk("COUNT"), bulk("100")])
            .await
            .unwrap();
        if let RespValue::Array(Some(outer)) = result {
            assert_eq!(outer.len(), 2);
            // Next cursor should be "0" (all returned)
            assert_eq!(outer[0], bulk("0"));
            if let RespValue::Array(Some(ref items)) = outer[1] {
                // 3 fields * 2 (field+value) = 6 elements
                assert_eq!(items.len(), 6);
            } else {
                panic!("Expected inner array");
            }
        } else {
            panic!("Expected Array");
        }

        // MATCH pattern
        let result = cmd
            .execute(vec![
                bulk("h"),
                bulk("0"),
                bulk("MATCH"),
                bulk("a*"),
                bulk("COUNT"),
                bulk("100"),
            ])
            .await
            .unwrap();
        if let RespValue::Array(Some(outer)) = result {
            if let RespValue::Array(Some(ref items)) = outer[1] {
                // Only "alpha" matches "a*"
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], bulk("alpha"));
                assert_eq!(items[1], bulk("1"));
            } else {
                panic!("Expected inner array");
            }
        } else {
            panic!("Expected Array");
        }

        // Non-existent key
        let result = cmd.execute(vec![bulk("nokey"), bulk("0")]).await.unwrap();
        if let RespValue::Array(Some(outer)) = result {
            assert_eq!(outer[0], bulk("0"));
            assert_eq!(outer[1], RespValue::Array(Some(vec![])));
        } else {
            panic!("Expected Array");
        }
    }

    // 15. HRANDFIELD returns random field(s)
    #[tokio::test]
    async fn test_hrandfield() {
        let store = DataStore::new();
        seed_hash(&store, "h", &[("a", "1"), ("b", "2"), ("c", "3")]).await;

        let cmd = HRandFieldCommand::new(store.clone());

        // Single random field (no count arg)
        let result = cmd.execute(vec![bulk("h")]).await.unwrap();
        if let RespValue::BulkString(Some(bytes)) = result {
            let field = String::from_utf8(bytes).unwrap();
            assert!(
                ["a", "b", "c"].contains(&field.as_str()),
                "unexpected field: {}",
                field
            );
        } else {
            panic!("Expected BulkString, got {:?}", result);
        }

        // Positive count: returns up to N unique fields
        let result = cmd.execute(vec![bulk("h"), bulk("2")]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
            // All returned fields should be valid
            for item in &items {
                if let RespValue::BulkString(Some(b)) = item {
                    let f = String::from_utf8(b.clone()).unwrap();
                    assert!(["a", "b", "c"].contains(&f.as_str()));
                } else {
                    panic!("Expected BulkString");
                }
            }
        } else {
            panic!("Expected Array");
        }

        // Positive count with WITHVALUES
        let result = cmd
            .execute(vec![bulk("h"), bulk("2"), bulk("WITHVALUES")])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            // 2 fields * 2 (field+value) = 4 elements
            assert_eq!(items.len(), 4);
        } else {
            panic!("Expected Array");
        }

        // Negative count: may return duplicates, length = abs(count)
        let result = cmd.execute(vec![bulk("h"), bulk("-5")]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 5);
        } else {
            panic!("Expected Array");
        }

        // Count exceeding hash size: returns at most hash.len() unique fields
        let result = cmd.execute(vec![bulk("h"), bulk("100")]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3); // only 3 fields exist
        } else {
            panic!("Expected Array");
        }

        // Non-existent key with count
        let result = cmd.execute(vec![bulk("nokey"), bulk("2")]).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![])));

        // Non-existent key without count
        let result = cmd.execute(vec![bulk("nokey")]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }
}
