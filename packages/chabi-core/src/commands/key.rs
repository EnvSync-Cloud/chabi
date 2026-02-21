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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::CommandHandler;
    use crate::storage::DataStore;

    fn bulk(s: &str) -> RespValue {
        RespValue::BulkString(Some(s.as_bytes().to_vec()))
    }

    /// Extract the Vec<RespValue> from an Array response.
    fn unwrap_array(resp: RespValue) -> Vec<RespValue> {
        match resp {
            RespValue::Array(Some(v)) => v,
            other => panic!("expected Array, got {:?}", other),
        }
    }

    /// Extract an integer from a RespValue::Integer.
    fn unwrap_int(resp: RespValue) -> i64 {
        match resp {
            RespValue::Integer(i) => i,
            other => panic!("expected Integer, got {:?}", other),
        }
    }

    /// Extract the inner string from a BulkString.
    fn unwrap_bulk(resp: RespValue) -> String {
        match resp {
            RespValue::BulkString(Some(bytes)) => String::from_utf8(bytes).unwrap(),
            other => panic!("expected BulkString(Some), got {:?}", other),
        }
    }

    /// Extract SimpleString value.
    fn unwrap_simple(resp: RespValue) -> String {
        match resp {
            RespValue::SimpleString(s) => s,
            other => panic!("expected SimpleString, got {:?}", other),
        }
    }

    /// Check if resp is an Error variant containing a substring.
    fn is_error_containing(resp: &RespValue, substr: &str) -> bool {
        match resp {
            RespValue::Error(msg) => msg.contains(substr),
            _ => false,
        }
    }

    // ---------------------------------------------------------------
    // 1. KEYS * returns all keys across stores
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn test_keys_pattern() {
        let store = DataStore::new();
        store
            .strings
            .write()
            .await
            .insert("foo".to_string(), "v1".to_string());
        store
            .lists
            .write()
            .await
            .insert("bar".to_string(), vec!["a".to_string()]);
        store
            .sets
            .write()
            .await
            .insert("baz".to_string(), HashSet::from(["x".to_string()]));

        let cmd = KeysCommand::new(store);
        let resp = cmd.execute(vec![bulk("*")]).await.unwrap();
        let arr = unwrap_array(resp);
        let mut keys: Vec<String> = arr.into_iter().map(|v| unwrap_bulk(v)).collect();
        keys.sort();
        assert_eq!(keys, vec!["bar", "baz", "foo"]);
    }

    // ---------------------------------------------------------------
    // 2. KEYS with h?llo glob pattern
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn test_keys_glob() {
        let store = DataStore::new();
        {
            let mut s = store.strings.write().await;
            s.insert("hello".to_string(), "v".to_string());
            s.insert("hallo".to_string(), "v".to_string());
            s.insert("hxllo".to_string(), "v".to_string());
            s.insert("world".to_string(), "v".to_string());
        }

        let cmd = KeysCommand::new(store);
        let resp = cmd.execute(vec![bulk("h?llo")]).await.unwrap();
        let arr = unwrap_array(resp);
        let mut keys: Vec<String> = arr.into_iter().map(|v| unwrap_bulk(v)).collect();
        keys.sort();
        assert_eq!(keys, vec!["hallo", "hello", "hxllo"]);
    }

    // ---------------------------------------------------------------
    // 3. TTL returns -1 for key without expiry
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn test_ttl_no_expiry() {
        let store = DataStore::new();
        store
            .strings
            .write()
            .await
            .insert("mykey".to_string(), "val".to_string());

        let cmd = TTLCommand::new(store);
        let resp = cmd.execute(vec![bulk("mykey")]).await.unwrap();
        assert_eq!(unwrap_int(resp), -1);
    }

    // ---------------------------------------------------------------
    // 4. TTL returns -2 for non-existent key
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn test_ttl_no_key() {
        let store = DataStore::new();
        let cmd = TTLCommand::new(store);
        let resp = cmd.execute(vec![bulk("nokey")]).await.unwrap();
        assert_eq!(unwrap_int(resp), -2);
    }

    // ---------------------------------------------------------------
    // 5. EXPIRE sets TTL, TTL returns positive value
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn test_expire() {
        let store = DataStore::new();
        store
            .strings
            .write()
            .await
            .insert("mykey".to_string(), "val".to_string());

        let expire_cmd = ExpireCommand::new(store.clone());
        let resp = expire_cmd
            .execute(vec![bulk("mykey"), bulk("100")])
            .await
            .unwrap();
        assert_eq!(unwrap_int(resp), 1);

        let ttl_cmd = TTLCommand::new(store);
        let resp = ttl_cmd.execute(vec![bulk("mykey")]).await.unwrap();
        let ttl = unwrap_int(resp);
        // Should be roughly 100 seconds (allowing some tolerance)
        assert!(ttl > 0 && ttl <= 100, "TTL was {}", ttl);
    }

    // ---------------------------------------------------------------
    // 6. PEXPIRE sets TTL in ms, PTTL returns positive value
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn test_pexpire() {
        let store = DataStore::new();
        store
            .strings
            .write()
            .await
            .insert("mykey".to_string(), "val".to_string());

        let pexpire_cmd = PExpireCommand::new(store.clone());
        let resp = pexpire_cmd
            .execute(vec![bulk("mykey"), bulk("50000")])
            .await
            .unwrap();
        assert_eq!(unwrap_int(resp), 1);

        let pttl_cmd = PTTLCommand::new(store);
        let resp = pttl_cmd.execute(vec![bulk("mykey")]).await.unwrap();
        let pttl = unwrap_int(resp);
        // Should be roughly 50000 ms (allowing some tolerance)
        assert!(pttl > 0 && pttl <= 50000, "PTTL was {}", pttl);
    }

    // ---------------------------------------------------------------
    // 7. RENAME existing key
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn test_rename() {
        let store = DataStore::new();
        store
            .strings
            .write()
            .await
            .insert("oldkey".to_string(), "val".to_string());

        let cmd = RenameCommand::new(store.clone());
        let resp = cmd
            .execute(vec![bulk("oldkey"), bulk("newkey")])
            .await
            .unwrap();
        assert_eq!(unwrap_simple(resp), "OK");

        let strings = store.strings.read().await;
        assert!(!strings.contains_key("oldkey"));
        assert_eq!(strings.get("newkey").unwrap(), "val");
    }

    // ---------------------------------------------------------------
    // 8. RENAME non-existent key returns error
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn test_rename_no_key() {
        let store = DataStore::new();
        let cmd = RenameCommand::new(store);
        let resp = cmd
            .execute(vec![bulk("nokey"), bulk("newkey")])
            .await
            .unwrap();
        assert!(is_error_containing(&resp, "no such key"));
    }

    // ---------------------------------------------------------------
    // 9. RENAMENX: succeeds if dest doesn't exist, fails if it does
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn test_renamenx() {
        let store = DataStore::new();
        {
            let mut s = store.strings.write().await;
            s.insert("src".to_string(), "val".to_string());
            s.insert("existing".to_string(), "other".to_string());
        }

        // Rename to a key that doesn't exist => 1
        let cmd = RenameNxCommand::new(store.clone());
        let resp = cmd.execute(vec![bulk("src"), bulk("dest")]).await.unwrap();
        assert_eq!(unwrap_int(resp), 1);

        // Now "dest" exists; try renaming "existing" to "dest" => 0
        let resp = cmd
            .execute(vec![bulk("existing"), bulk("dest")])
            .await
            .unwrap();
        assert_eq!(unwrap_int(resp), 0);
    }

    // ---------------------------------------------------------------
    // 10. TYPE command for each type
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn test_type_command() {
        let store = DataStore::new();
        store
            .strings
            .write()
            .await
            .insert("skey".to_string(), "v".to_string());
        store
            .lists
            .write()
            .await
            .insert("lkey".to_string(), vec!["a".to_string()]);
        store
            .sets
            .write()
            .await
            .insert("setkey".to_string(), HashSet::from(["x".to_string()]));
        store.hashes.write().await.insert(
            "hkey".to_string(),
            HashMap::from([("f".to_string(), "v".to_string())]),
        );

        let cmd = TypeCommand::new(store);
        assert_eq!(
            unwrap_simple(cmd.execute(vec![bulk("skey")]).await.unwrap()),
            "string"
        );
        assert_eq!(
            unwrap_simple(cmd.execute(vec![bulk("lkey")]).await.unwrap()),
            "list"
        );
        assert_eq!(
            unwrap_simple(cmd.execute(vec![bulk("setkey")]).await.unwrap()),
            "set"
        );
        assert_eq!(
            unwrap_simple(cmd.execute(vec![bulk("hkey")]).await.unwrap()),
            "hash"
        );
        assert_eq!(
            unwrap_simple(cmd.execute(vec![bulk("nokey")]).await.unwrap()),
            "none"
        );
    }

    // ---------------------------------------------------------------
    // 11. PERSIST removes TTL
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn test_persist() {
        let store = DataStore::new();
        store
            .strings
            .write()
            .await
            .insert("mykey".to_string(), "val".to_string());

        // Set an expiration first
        let expire_cmd = ExpireCommand::new(store.clone());
        expire_cmd
            .execute(vec![bulk("mykey"), bulk("100")])
            .await
            .unwrap();

        // Verify TTL is set
        let ttl_cmd = TTLCommand::new(store.clone());
        let ttl = unwrap_int(ttl_cmd.execute(vec![bulk("mykey")]).await.unwrap());
        assert!(ttl > 0);

        // PERSIST removes the TTL
        let persist_cmd = PersistCommand::new(store.clone());
        let resp = persist_cmd.execute(vec![bulk("mykey")]).await.unwrap();
        assert_eq!(unwrap_int(resp), 1);

        // Now TTL should be -1 (no expiry)
        let ttl = unwrap_int(ttl_cmd.execute(vec![bulk("mykey")]).await.unwrap());
        assert_eq!(ttl, -1);
    }

    // ---------------------------------------------------------------
    // 12. UNLINK deletes keys across types
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn test_unlink() {
        let store = DataStore::new();
        store
            .strings
            .write()
            .await
            .insert("s1".to_string(), "v".to_string());
        store
            .lists
            .write()
            .await
            .insert("l1".to_string(), vec!["a".to_string()]);
        store
            .sets
            .write()
            .await
            .insert("set1".to_string(), HashSet::from(["x".to_string()]));

        let cmd = UnlinkCommand::new(store.clone());
        let resp = cmd
            .execute(vec![bulk("s1"), bulk("l1"), bulk("set1"), bulk("nokey")])
            .await
            .unwrap();
        // 3 keys deleted (nokey doesn't exist so not counted)
        assert_eq!(unwrap_int(resp), 3);

        assert!(store.strings.read().await.is_empty());
        assert!(store.lists.read().await.is_empty());
        assert!(store.sets.read().await.is_empty());
    }

    // ---------------------------------------------------------------
    // 13. RANDOMKEY returns a key or nil
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn test_randomkey() {
        let store = DataStore::new();

        // Empty store => nil
        let cmd = RandomKeyCommand::new(store.clone());
        let resp = cmd.execute(vec![]).await.unwrap();
        assert!(matches!(resp, RespValue::BulkString(None)));

        // Insert a key, should return something
        store
            .strings
            .write()
            .await
            .insert("onlykey".to_string(), "v".to_string());
        let resp = cmd.execute(vec![]).await.unwrap();
        assert_eq!(unwrap_bulk(resp), "onlykey");
    }

    // ---------------------------------------------------------------
    // 14. SCAN with cursor 0
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn test_scan_basic() {
        let store = DataStore::new();
        {
            let mut s = store.strings.write().await;
            s.insert("a".to_string(), "1".to_string());
            s.insert("b".to_string(), "2".to_string());
            s.insert("c".to_string(), "3".to_string());
        }

        let cmd = ScanCommand::new(store);
        let resp = cmd.execute(vec![bulk("0")]).await.unwrap();
        let outer = unwrap_array(resp);
        assert_eq!(outer.len(), 2);

        // Next cursor
        let next_cursor = unwrap_bulk(outer[0].clone());
        // With default count=10 and only 3 keys, cursor should be "0" (complete)
        assert_eq!(next_cursor, "0");

        // Keys returned
        let keys_arr = unwrap_array(outer[1].clone());
        let mut keys: Vec<String> = keys_arr.into_iter().map(|v| unwrap_bulk(v)).collect();
        keys.sort();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    // ---------------------------------------------------------------
    // 15. COPY source to dest (basic)
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn test_copy_basic() {
        let store = DataStore::new();
        store
            .strings
            .write()
            .await
            .insert("src".to_string(), "hello".to_string());

        let cmd = CopyCommand::new(store.clone());
        let resp = cmd.execute(vec![bulk("src"), bulk("dst")]).await.unwrap();
        assert_eq!(unwrap_int(resp), 1);

        let strings = store.strings.read().await;
        assert_eq!(strings.get("src").unwrap(), "hello");
        assert_eq!(strings.get("dst").unwrap(), "hello");
    }

    // ---------------------------------------------------------------
    // 16. COPY with REPLACE flag
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn test_copy_replace() {
        let store = DataStore::new();
        {
            let mut s = store.strings.write().await;
            s.insert("src".to_string(), "new_val".to_string());
            s.insert("dst".to_string(), "old_val".to_string());
        }

        // Without REPLACE => 0 (dest exists)
        let cmd = CopyCommand::new(store.clone());
        let resp = cmd.execute(vec![bulk("src"), bulk("dst")]).await.unwrap();
        assert_eq!(unwrap_int(resp), 0);

        // With REPLACE => 1
        let resp = cmd
            .execute(vec![bulk("src"), bulk("dst"), bulk("REPLACE")])
            .await
            .unwrap();
        assert_eq!(unwrap_int(resp), 1);

        let strings = store.strings.read().await;
        assert_eq!(strings.get("dst").unwrap(), "new_val");
    }

    // ---------------------------------------------------------------
    // 17. TOUCH returns count of existing keys
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn test_touch() {
        let store = DataStore::new();
        store
            .strings
            .write()
            .await
            .insert("a".to_string(), "v".to_string());
        store
            .lists
            .write()
            .await
            .insert("b".to_string(), vec!["x".to_string()]);

        let cmd = TouchCommand::new(store);
        let resp = cmd
            .execute(vec![bulk("a"), bulk("b"), bulk("missing")])
            .await
            .unwrap();
        assert_eq!(unwrap_int(resp), 2);
    }

    // ---------------------------------------------------------------
    // 18. OBJECT ENCODING returns "raw"
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn test_object_encoding() {
        let cmd = ObjectCommand::new();
        let resp = cmd
            .execute(vec![bulk("ENCODING"), bulk("somekey")])
            .await
            .unwrap();
        assert_eq!(unwrap_bulk(resp), "raw");
    }

    // ---------------------------------------------------------------
    // 19. glob_match helper function
    // ---------------------------------------------------------------
    #[test]
    fn test_glob_match_fn() {
        // Star matches everything
        assert!(glob_match("*", "anything"));
        assert!(glob_match("*", ""));

        // Question mark matches single char
        assert!(glob_match("h?llo", "hello"));
        assert!(glob_match("h?llo", "hallo"));
        assert!(!glob_match("h?llo", "hllo")); // ? requires exactly 1 char

        // Character class
        assert!(glob_match("h[ae]llo", "hello"));
        assert!(glob_match("h[ae]llo", "hallo"));
        assert!(!glob_match("h[ae]llo", "hillo"));

        // Negated character class
        assert!(!glob_match("h[^ae]llo", "hello"));
        assert!(glob_match("h[^ae]llo", "hillo"));

        // Exact match
        assert!(glob_match("hello", "hello"));
        assert!(!glob_match("hello", "world"));

        // Star in the middle
        assert!(glob_match("he*lo", "hello"));
        assert!(glob_match("he*lo", "helo"));
        assert!(glob_match("he*lo", "he123lo"));

        // Backslash escape
        assert!(glob_match("h\\*llo", "h*llo"));
        assert!(!glob_match("h\\*llo", "hello"));
    }
}
