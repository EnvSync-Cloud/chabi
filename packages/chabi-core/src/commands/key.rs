use crate::commands::CommandHandler;
use crate::resp::RespValue;
use crate::storage::DataStore;
use crate::Result;
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

fn extract_string(val: &RespValue) -> Option<String> {
    match val {
        RespValue::BulkString(Some(bytes)) => Some(String::from_utf8_lossy(bytes).to_string()),
        _ => None,
    }
}

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
            // Try matching zero or more characters
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
        b'[' => {
            if vi >= v.len() {
                return false;
            }
            let mut negate = false;
            let mut pi2 = pi + 1;
            if pi2 < p.len() && p[pi2] == b'^' {
                negate = true;
                pi2 += 1;
            }
            let mut matched = false;
            while pi2 < p.len() && p[pi2] != b']' {
                if pi2 + 2 < p.len() && p[pi2 + 1] == b'-' {
                    if v[vi] >= p[pi2] && v[vi] <= p[pi2 + 2] {
                        matched = true;
                    }
                    pi2 += 3;
                } else {
                    if v[vi] == p[pi2] {
                        matched = true;
                    }
                    pi2 += 1;
                }
            }
            if pi2 < p.len() {
                pi2 += 1; // skip ']'
            }
            if matched != negate {
                glob_match_bytes(p, pi2, v, vi + 1)
            } else {
                false
            }
        }
        b'\\' => {
            if pi + 1 < p.len() && vi < v.len() && p[pi + 1] == v[vi] {
                glob_match_bytes(p, pi + 2, v, vi + 1)
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

fn key_exists_in_any(
    key: &str,
    strings: &HashMap<String, String>,
    lists: &HashMap<String, Vec<String>>,
    sets: &HashMap<String, HashSet<String>>,
    hashes: &HashMap<String, HashMap<String, String>>,
) -> bool {
    strings.contains_key(key)
        || lists.contains_key(key)
        || sets.contains_key(key)
        || hashes.contains_key(key)
}

fn key_type_str(
    key: &str,
    strings: &HashMap<String, String>,
    lists: &HashMap<String, Vec<String>>,
    sets: &HashMap<String, HashSet<String>>,
    hashes: &HashMap<String, HashMap<String, String>>,
) -> &'static str {
    if strings.contains_key(key) {
        "string"
    } else if lists.contains_key(key) {
        "list"
    } else if sets.contains_key(key) {
        "set"
    } else if hashes.contains_key(key) {
        "hash"
    } else {
        "none"
    }
}

// --- KEYS (cross-type with glob support) ---

#[derive(Clone)]
pub struct KeysCommand {
    store: DataStore,
}

impl KeysCommand {
    pub fn new(store: DataStore) -> Self {
        KeysCommand { store }
    }
}

#[async_trait]
impl CommandHandler for KeysCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'keys' command".to_string(),
            ));
        }
        let pattern = match extract_string(&args[0]) {
            Some(p) => p,
            None => return Ok(RespValue::Error("ERR invalid pattern".to_string())),
        };

        let strings = self.store.strings.read().await;
        let lists = self.store.lists.read().await;
        let sets = self.store.sets.read().await;
        let hashes = self.store.hashes.read().await;

        let mut all_keys: HashSet<&String> = HashSet::new();
        for k in strings.keys() {
            all_keys.insert(k);
        }
        for k in lists.keys() {
            all_keys.insert(k);
        }
        for k in sets.keys() {
            all_keys.insert(k);
        }
        for k in hashes.keys() {
            all_keys.insert(k);
        }

        let filtered: Vec<RespValue> = all_keys
            .into_iter()
            .filter(|k| glob_match(&pattern, k))
            .map(|k| RespValue::BulkString(Some(k.as_bytes().to_vec())))
            .collect();

        Ok(RespValue::Array(Some(filtered)))
    }
}

// --- TTL (cross-type) ---

#[derive(Clone)]
pub struct TTLCommand {
    store: DataStore,
}

impl TTLCommand {
    pub fn new(store: DataStore) -> Self {
        TTLCommand { store }
    }
}

#[async_trait]
impl CommandHandler for TTLCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'ttl' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let now = Instant::now();
        {
            let exps = self.store.expirations.read().await;
            if let Some(&deadline) = exps.get(&key) {
                if deadline <= now {
                    drop(exps);
                    let mut exps_w = self.store.expirations.write().await;
                    exps_w.remove(&key);
                    drop(exps_w);
                    self.store.strings.write().await.remove(&key);
                    self.store.lists.write().await.remove(&key);
                    self.store.sets.write().await.remove(&key);
                    self.store.hashes.write().await.remove(&key);
                    return Ok(RespValue::Integer(-2));
                } else {
                    let remaining = deadline.saturating_duration_since(now).as_secs() as i64;
                    return Ok(RespValue::Integer(remaining.max(0)));
                }
            }
        }

        let strings = self.store.strings.read().await;
        let lists = self.store.lists.read().await;
        let sets = self.store.sets.read().await;
        let hashes = self.store.hashes.read().await;
        if key_exists_in_any(&key, &strings, &lists, &sets, &hashes) {
            Ok(RespValue::Integer(-1))
        } else {
            Ok(RespValue::Integer(-2))
        }
    }
}

// --- PTTL ---

#[derive(Clone)]
pub struct PTTLCommand {
    store: DataStore,
}

impl PTTLCommand {
    pub fn new(store: DataStore) -> Self {
        PTTLCommand { store }
    }
}

#[async_trait]
impl CommandHandler for PTTLCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'pttl' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let now = Instant::now();
        let exps = self.store.expirations.read().await;
        if let Some(&deadline) = exps.get(&key) {
            if deadline <= now {
                return Ok(RespValue::Integer(-2));
            } else {
                let remaining = deadline.saturating_duration_since(now).as_millis() as i64;
                return Ok(RespValue::Integer(remaining.max(0)));
            }
        }
        drop(exps);
        let strings = self.store.strings.read().await;
        let lists = self.store.lists.read().await;
        let sets = self.store.sets.read().await;
        let hashes = self.store.hashes.read().await;
        if key_exists_in_any(&key, &strings, &lists, &sets, &hashes) {
            Ok(RespValue::Integer(-1))
        } else {
            Ok(RespValue::Integer(-2))
        }
    }
}

// --- EXPIRE (cross-type) ---

#[derive(Clone)]
pub struct ExpireCommand {
    store: DataStore,
}

impl ExpireCommand {
    pub fn new(store: DataStore) -> Self {
        ExpireCommand { store }
    }
}

#[async_trait]
impl CommandHandler for ExpireCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'expire' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let seconds: i64 = match extract_string(&args[1]).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => {
                return Ok(RespValue::Error(
                    "ERR value is not an integer or out of range".to_string(),
                ))
            }
        };

        let strings = self.store.strings.read().await;
        let lists = self.store.lists.read().await;
        let sets = self.store.sets.read().await;
        let hashes = self.store.hashes.read().await;
        let exists = key_exists_in_any(&key, &strings, &lists, &sets, &hashes);
        drop(strings);
        drop(lists);
        drop(sets);
        drop(hashes);

        if !exists {
            return Ok(RespValue::Integer(0));
        }

        if seconds <= 0 {
            let mut exps = self.store.expirations.write().await;
            exps.remove(&key);
            drop(exps);
            self.store.strings.write().await.remove(&key);
            self.store.lists.write().await.remove(&key);
            self.store.sets.write().await.remove(&key);
            self.store.hashes.write().await.remove(&key);
            return Ok(RespValue::Integer(1));
        }

        let deadline = Instant::now() + Duration::from_secs(seconds as u64);
        let mut exps = self.store.expirations.write().await;
        exps.insert(key, deadline);
        Ok(RespValue::Integer(1))
    }
}

// --- PEXPIRE ---

#[derive(Clone)]
pub struct PExpireCommand {
    store: DataStore,
}

impl PExpireCommand {
    pub fn new(store: DataStore) -> Self {
        PExpireCommand { store }
    }
}

#[async_trait]
impl CommandHandler for PExpireCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'pexpire' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let ms: i64 = match extract_string(&args[1]).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => {
                return Ok(RespValue::Error(
                    "ERR value is not an integer or out of range".to_string(),
                ))
            }
        };

        let strings = self.store.strings.read().await;
        let lists = self.store.lists.read().await;
        let sets = self.store.sets.read().await;
        let hashes = self.store.hashes.read().await;
        let exists = key_exists_in_any(&key, &strings, &lists, &sets, &hashes);
        drop(strings);
        drop(lists);
        drop(sets);
        drop(hashes);

        if !exists {
            return Ok(RespValue::Integer(0));
        }

        if ms <= 0 {
            self.store.expirations.write().await.remove(&key);
            self.store.strings.write().await.remove(&key);
            self.store.lists.write().await.remove(&key);
            self.store.sets.write().await.remove(&key);
            self.store.hashes.write().await.remove(&key);
            return Ok(RespValue::Integer(1));
        }

        let deadline = Instant::now() + Duration::from_millis(ms as u64);
        self.store.expirations.write().await.insert(key, deadline);
        Ok(RespValue::Integer(1))
    }
}

// --- EXPIREAT ---

#[derive(Clone)]
pub struct ExpireAtCommand {
    store: DataStore,
}

impl ExpireAtCommand {
    pub fn new(store: DataStore) -> Self {
        ExpireAtCommand { store }
    }
}

#[async_trait]
impl CommandHandler for ExpireAtCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'expireat' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let timestamp: u64 = match extract_string(&args[1]).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => {
                return Ok(RespValue::Error(
                    "ERR value is not an integer or out of range".to_string(),
                ))
            }
        };
        let strings = self.store.strings.read().await;
        let lists = self.store.lists.read().await;
        let sets = self.store.sets.read().await;
        let hashes = self.store.hashes.read().await;
        let exists = key_exists_in_any(&key, &strings, &lists, &sets, &hashes);
        drop(strings);
        drop(lists);
        drop(sets);
        drop(hashes);
        if !exists {
            return Ok(RespValue::Integer(0));
        }
        let now_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if timestamp <= now_epoch {
            self.store.expirations.write().await.remove(&key);
            self.store.strings.write().await.remove(&key);
            self.store.lists.write().await.remove(&key);
            self.store.sets.write().await.remove(&key);
            self.store.hashes.write().await.remove(&key);
            return Ok(RespValue::Integer(1));
        }
        let delta = Duration::from_secs(timestamp - now_epoch);
        self.store
            .expirations
            .write()
            .await
            .insert(key, Instant::now() + delta);
        Ok(RespValue::Integer(1))
    }
}

// --- PEXPIREAT ---

#[derive(Clone)]
pub struct PExpireAtCommand {
    store: DataStore,
}

impl PExpireAtCommand {
    pub fn new(store: DataStore) -> Self {
        PExpireAtCommand { store }
    }
}

#[async_trait]
impl CommandHandler for PExpireAtCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'pexpireat' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let ts_ms: u64 = match extract_string(&args[1]).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => {
                return Ok(RespValue::Error(
                    "ERR value is not an integer or out of range".to_string(),
                ))
            }
        };
        let strings = self.store.strings.read().await;
        let lists = self.store.lists.read().await;
        let sets = self.store.sets.read().await;
        let hashes = self.store.hashes.read().await;
        let exists = key_exists_in_any(&key, &strings, &lists, &sets, &hashes);
        drop(strings);
        drop(lists);
        drop(sets);
        drop(hashes);
        if !exists {
            return Ok(RespValue::Integer(0));
        }
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        if ts_ms <= now_ms {
            self.store.expirations.write().await.remove(&key);
            self.store.strings.write().await.remove(&key);
            self.store.lists.write().await.remove(&key);
            self.store.sets.write().await.remove(&key);
            self.store.hashes.write().await.remove(&key);
            return Ok(RespValue::Integer(1));
        }
        let delta = Duration::from_millis(ts_ms - now_ms);
        self.store
            .expirations
            .write()
            .await
            .insert(key, Instant::now() + delta);
        Ok(RespValue::Integer(1))
    }
}

// --- RENAME ---

#[derive(Clone)]
pub struct RenameCommand {
    store: DataStore,
}

impl RenameCommand {
    pub fn new(store: DataStore) -> Self {
        RenameCommand { store }
    }
}

#[async_trait]
impl CommandHandler for RenameCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'rename' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let newkey = match extract_string(&args[1]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid newkey".to_string())),
        };
        let mut store = self.store.strings.write().await;
        match store.remove(&key) {
            Some(value) => {
                store.insert(newkey.clone(), value);
                drop(store);
                let mut exps = self.store.expirations.write().await;
                if let Some(deadline) = exps.remove(&key) {
                    exps.insert(newkey, deadline);
                }
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            None => Ok(RespValue::Error("ERR no such key".to_string())),
        }
    }
}

// --- RENAMENX ---

#[derive(Clone)]
pub struct RenameNxCommand {
    store: DataStore,
}

impl RenameNxCommand {
    pub fn new(store: DataStore) -> Self {
        RenameNxCommand { store }
    }
}

#[async_trait]
impl CommandHandler for RenameNxCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'renamenx' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let newkey = match extract_string(&args[1]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid newkey".to_string())),
        };
        let mut store = self.store.strings.write().await;
        if !store.contains_key(&key) {
            return Ok(RespValue::Error("ERR no such key".to_string()));
        }
        if store.contains_key(&newkey) {
            return Ok(RespValue::Integer(0));
        }
        let value = store.remove(&key).unwrap();
        store.insert(newkey.clone(), value);
        drop(store);
        let mut exps = self.store.expirations.write().await;
        if let Some(deadline) = exps.remove(&key) {
            exps.insert(newkey, deadline);
        }
        Ok(RespValue::Integer(1))
    }
}

// --- TYPE (cross-type) ---

#[derive(Clone)]
pub struct TypeCommand {
    store: DataStore,
}

impl TypeCommand {
    pub fn new(store: DataStore) -> Self {
        TypeCommand { store }
    }
}

#[async_trait]
impl CommandHandler for TypeCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'type' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let strings = self.store.strings.read().await;
        let lists = self.store.lists.read().await;
        let sets = self.store.sets.read().await;
        let hashes = self.store.hashes.read().await;
        let t = key_type_str(&key, &strings, &lists, &sets, &hashes);
        Ok(RespValue::SimpleString(t.to_string()))
    }
}

// --- PERSIST ---

#[derive(Clone)]
pub struct PersistCommand {
    store: DataStore,
}

impl PersistCommand {
    pub fn new(store: DataStore) -> Self {
        PersistCommand { store }
    }
}

#[async_trait]
impl CommandHandler for PersistCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'persist' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let mut exps = self.store.expirations.write().await;
        if exps.remove(&key).is_some() {
            Ok(RespValue::Integer(1))
        } else {
            Ok(RespValue::Integer(0))
        }
    }
}

// --- UNLINK (alias for DEL) ---

#[derive(Clone)]
pub struct UnlinkCommand {
    store: DataStore,
}

impl UnlinkCommand {
    pub fn new(store: DataStore) -> Self {
        UnlinkCommand { store }
    }
}

#[async_trait]
impl CommandHandler for UnlinkCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'unlink' command".to_string(),
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

// --- RANDOMKEY ---

#[derive(Clone)]
pub struct RandomKeyCommand {
    store: DataStore,
}

impl RandomKeyCommand {
    pub fn new(store: DataStore) -> Self {
        RandomKeyCommand { store }
    }
}

#[async_trait]
impl CommandHandler for RandomKeyCommand {
    async fn execute(&self, _args: Vec<RespValue>) -> Result<RespValue> {
        use rand::seq::SliceRandom;
        let strings = self.store.strings.read().await;
        let lists = self.store.lists.read().await;
        let sets = self.store.sets.read().await;
        let hashes = self.store.hashes.read().await;
        let mut all_keys: Vec<&String> = Vec::new();
        all_keys.extend(strings.keys());
        all_keys.extend(lists.keys());
        all_keys.extend(sets.keys());
        all_keys.extend(hashes.keys());
        if all_keys.is_empty() {
            return Ok(RespValue::BulkString(None));
        }
        let mut rng = rand::thread_rng();
        let key = all_keys.choose(&mut rng).unwrap();
        Ok(RespValue::BulkString(Some(key.as_bytes().to_vec())))
    }
}

// --- SCAN ---

#[derive(Clone)]
pub struct ScanCommand {
    store: DataStore,
}

impl ScanCommand {
    pub fn new(store: DataStore) -> Self {
        ScanCommand { store }
    }
}

#[async_trait]
impl CommandHandler for ScanCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'scan' command".to_string(),
            ));
        }
        let cursor: usize = match extract_string(&args[0]).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => return Ok(RespValue::Error("ERR invalid cursor".to_string())),
        };
        let mut pattern: Option<String> = None;
        let mut count: usize = 10;
        let mut i = 1;
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

        let strings = self.store.strings.read().await;
        let lists = self.store.lists.read().await;
        let sets = self.store.sets.read().await;
        let hashes = self.store.hashes.read().await;
        let mut all_keys: Vec<String> = Vec::new();
        for k in strings.keys() {
            all_keys.push(k.clone());
        }
        for k in lists.keys() {
            if !all_keys.contains(k) {
                all_keys.push(k.clone());
            }
        }
        for k in sets.keys() {
            if !all_keys.contains(k) {
                all_keys.push(k.clone());
            }
        }
        for k in hashes.keys() {
            if !all_keys.contains(k) {
                all_keys.push(k.clone());
            }
        }
        all_keys.sort();

        let filtered: Vec<String> = if let Some(ref pat) = pattern {
            all_keys
                .into_iter()
                .filter(|k| glob_match(pat, k))
                .collect()
        } else {
            all_keys
        };

        let start = cursor;
        let end = (start + count).min(filtered.len());
        let next_cursor = if end >= filtered.len() { 0 } else { end };

        let keys: Vec<RespValue> = if start < filtered.len() {
            filtered[start..end]
                .iter()
                .map(|k| RespValue::BulkString(Some(k.as_bytes().to_vec())))
                .collect()
        } else {
            vec![]
        };

        Ok(RespValue::Array(Some(vec![
            RespValue::BulkString(Some(next_cursor.to_string().into_bytes())),
            RespValue::Array(Some(keys)),
        ])))
    }
}

// --- COPY ---

#[derive(Clone)]
pub struct CopyCommand {
    store: DataStore,
}

impl CopyCommand {
    pub fn new(store: DataStore) -> Self {
        CopyCommand { store }
    }
}

#[async_trait]
impl CommandHandler for CopyCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'copy' command".to_string(),
            ));
        }
        let source = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let dest = match extract_string(&args[1]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let mut replace = false;
        let mut i = 2;
        while i < args.len() {
            if let Some(f) = extract_string(&args[i]) {
                if f.to_uppercase() == "REPLACE" {
                    replace = true;
                }
            }
            i += 1;
        }

        let mut strings = self.store.strings.write().await;
        let mut lists = self.store.lists.write().await;
        let mut sets = self.store.sets.write().await;
        let mut hashes = self.store.hashes.write().await;

        // Check dest exists
        let dest_exists = strings.contains_key(&dest)
            || lists.contains_key(&dest)
            || sets.contains_key(&dest)
            || hashes.contains_key(&dest);
        if dest_exists && !replace {
            return Ok(RespValue::Integer(0));
        }

        // Copy from source
        if let Some(v) = strings.get(&source).cloned() {
            if replace {
                lists.remove(&dest);
                sets.remove(&dest);
                hashes.remove(&dest);
            }
            strings.insert(dest, v);
            return Ok(RespValue::Integer(1));
        }
        if let Some(v) = lists.get(&source).cloned() {
            if replace {
                strings.remove(&dest);
                sets.remove(&dest);
                hashes.remove(&dest);
            }
            lists.insert(dest, v);
            return Ok(RespValue::Integer(1));
        }
        if let Some(v) = sets.get(&source).cloned() {
            if replace {
                strings.remove(&dest);
                lists.remove(&dest);
                hashes.remove(&dest);
            }
            sets.insert(dest, v);
            return Ok(RespValue::Integer(1));
        }
        if let Some(v) = hashes.get(&source).cloned() {
            if replace {
                strings.remove(&dest);
                lists.remove(&dest);
                sets.remove(&dest);
            }
            hashes.insert(dest, v);
            return Ok(RespValue::Integer(1));
        }

        Ok(RespValue::Integer(0))
    }
}

// --- TOUCH ---

#[derive(Clone)]
pub struct TouchCommand {
    store: DataStore,
}

impl TouchCommand {
    pub fn new(store: DataStore) -> Self {
        TouchCommand { store }
    }
}

#[async_trait]
impl CommandHandler for TouchCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'touch' command".to_string(),
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
            if key_exists_in_any(&key, &strings, &lists, &sets, &hashes) {
                count += 1;
            }
        }
        Ok(RespValue::Integer(count))
    }
}

// --- OBJECT (stub) ---

#[derive(Clone)]
pub struct ObjectCommand;

impl ObjectCommand {
    pub fn new() -> Self {
        ObjectCommand
    }
}

impl Default for ObjectCommand {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CommandHandler for ObjectCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'object' command".to_string(),
            ));
        }
        let subcmd = match extract_string(&args[0]) {
            Some(s) => s.to_uppercase(),
            None => return Ok(RespValue::Error("ERR invalid subcommand".to_string())),
        };
        match subcmd.as_str() {
            "ENCODING" => Ok(RespValue::BulkString(Some(b"raw".to_vec()))),
            "REFCOUNT" => Ok(RespValue::Integer(1)),
            "IDLETIME" => Ok(RespValue::Integer(0)),
            "HELP" => Ok(RespValue::Array(Some(vec![RespValue::BulkString(Some(
                b"OBJECT ENCODING|REFCOUNT|IDLETIME <key>".to_vec(),
            ))]))),
            _ => Ok(RespValue::Error(
                "ERR unknown OBJECT subcommand".to_string(),
            )),
        }
    }
}
