use crate::commands::CommandHandler;
use crate::resp::RespValue;
use crate::storage::DataStore;
use crate::Result;
use async_trait::async_trait;
use std::collections::HashSet;

fn extract_string(val: &RespValue) -> Option<String> {
    match val {
        RespValue::BulkString(Some(bytes)) => Some(String::from_utf8_lossy(bytes).to_string()),
        _ => None,
    }
}

fn glob_match(pattern: &str, text: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let txt: Vec<char> = text.chars().collect();
    let (plen, tlen) = (pat.len(), txt.len());
    let mut dp = vec![vec![false; tlen + 1]; plen + 1];
    dp[0][0] = true;

    for i in 1..=plen {
        if pat[i - 1] == '*' {
            dp[i][0] = dp[i - 1][0];
        }
    }

    for i in 1..=plen {
        for j in 1..=tlen {
            if pat[i - 1] == '*' {
                dp[i][j] = dp[i - 1][j] || dp[i][j - 1];
            } else if pat[i - 1] == '?' || pat[i - 1] == txt[j - 1] {
                dp[i][j] = dp[i - 1][j - 1];
            }
        }
    }

    dp[plen][tlen]
}

// ─── SADD ───────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SAddCommand {
    store: DataStore,
}

impl SAddCommand {
    pub fn new(store: DataStore) -> Self {
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

        let mut store = self.store.sets.write().await;
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

// ─── SMEMBERS ───────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SMembersCommand {
    store: DataStore,
}

impl SMembersCommand {
    pub fn new(store: DataStore) -> Self {
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

        let store = self.store.sets.read().await;

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

// ─── SISMEMBER ──────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SIsMemberCommand {
    store: DataStore,
}

impl SIsMemberCommand {
    pub fn new(store: DataStore) -> Self {
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

        let store = self.store.sets.read().await;

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

// ─── SCARD ──────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SCardCommand {
    store: DataStore,
}

impl SCardCommand {
    pub fn new(store: DataStore) -> Self {
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

        let store = self.store.sets.read().await;

        match store.get(&key) {
            Some(set) => Ok(RespValue::Integer(set.len() as i64)),
            None => Ok(RespValue::Integer(0)),
        }
    }
}

// ─── SREM ───────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SRemCommand {
    store: DataStore,
}

impl SRemCommand {
    pub fn new(store: DataStore) -> Self {
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

        let mut store = self.store.sets.write().await;

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

// ─── SPOP ───────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SPopCommand {
    store: DataStore,
}

impl SPopCommand {
    pub fn new(store: DataStore) -> Self {
        SPopCommand { store }
    }
}

#[async_trait]
impl CommandHandler for SPopCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() || args.len() > 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'spop' command".to_string(),
            ));
        }

        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let count = if args.len() == 2 {
            match extract_string(&args[1]) {
                Some(s) => match s.parse::<i64>() {
                    Ok(n) if n >= 0 => Some(n as usize),
                    _ => {
                        return Ok(RespValue::Error(
                            "ERR value is not an integer or out of range".to_string(),
                        ))
                    }
                },
                None => return Ok(RespValue::Error("ERR invalid count".to_string())),
            }
        } else {
            None
        };

        let mut store = self.store.sets.write().await;

        let set = match store.get_mut(&key) {
            Some(s) => s,
            None => {
                return if count.is_some() {
                    Ok(RespValue::Array(Some(vec![])))
                } else {
                    Ok(RespValue::BulkString(None))
                };
            }
        };

        if set.is_empty() {
            return if count.is_some() {
                Ok(RespValue::Array(Some(vec![])))
            } else {
                Ok(RespValue::BulkString(None))
            };
        }

        use rand::seq::SliceRandom;
        let mut rng = rand::thread_rng();

        match count {
            Some(n) => {
                let mut members: Vec<String> = set.iter().cloned().collect();
                members.shuffle(&mut rng);
                let to_pop = n.min(members.len());
                let popped: Vec<String> = members[..to_pop].to_vec();
                for m in &popped {
                    set.remove(m);
                }
                if set.is_empty() {
                    store.remove(&key);
                }
                let result: Vec<RespValue> = popped
                    .into_iter()
                    .map(|s| RespValue::BulkString(Some(s.into_bytes())))
                    .collect();
                Ok(RespValue::Array(Some(result)))
            }
            None => {
                let members: Vec<String> = set.iter().cloned().collect();
                let chosen = members.choose(&mut rng).unwrap().clone();
                set.remove(&chosen);
                if set.is_empty() {
                    store.remove(&key);
                }
                Ok(RespValue::BulkString(Some(chosen.into_bytes())))
            }
        }
    }
}

// ─── SRANDMEMBER ────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SRandMemberCommand {
    store: DataStore,
}

impl SRandMemberCommand {
    pub fn new(store: DataStore) -> Self {
        SRandMemberCommand { store }
    }
}

#[async_trait]
impl CommandHandler for SRandMemberCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() || args.len() > 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'srandmember' command".to_string(),
            ));
        }

        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let count = if args.len() == 2 {
            match extract_string(&args[1]) {
                Some(s) => match s.parse::<i64>() {
                    Ok(n) => Some(n),
                    Err(_) => {
                        return Ok(RespValue::Error(
                            "ERR value is not an integer or out of range".to_string(),
                        ))
                    }
                },
                None => return Ok(RespValue::Error("ERR invalid count".to_string())),
            }
        } else {
            None
        };

        let store = self.store.sets.read().await;

        let set = match store.get(&key) {
            Some(s) => s,
            None => {
                return if count.is_some() {
                    Ok(RespValue::Array(Some(vec![])))
                } else {
                    Ok(RespValue::BulkString(None))
                };
            }
        };

        if set.is_empty() {
            return if count.is_some() {
                Ok(RespValue::Array(Some(vec![])))
            } else {
                Ok(RespValue::BulkString(None))
            };
        }

        use rand::seq::SliceRandom;
        let mut rng = rand::thread_rng();
        let members: Vec<String> = set.iter().cloned().collect();

        match count {
            Some(n) if n < 0 => {
                // Negative count: allow duplicates, return abs(n) elements
                let abs_n = n.unsigned_abs() as usize;
                let mut result = Vec::with_capacity(abs_n);
                for _ in 0..abs_n {
                    let chosen = members.choose(&mut rng).unwrap().clone();
                    result.push(RespValue::BulkString(Some(chosen.into_bytes())));
                }
                Ok(RespValue::Array(Some(result)))
            }
            Some(n) => {
                // Positive count: unique elements, up to set size
                let n = n as usize;
                let take = n.min(members.len());
                let mut shuffled = members.clone();
                shuffled.shuffle(&mut rng);
                let result: Vec<RespValue> = shuffled[..take]
                    .iter()
                    .map(|s| RespValue::BulkString(Some(s.as_bytes().to_vec())))
                    .collect();
                Ok(RespValue::Array(Some(result)))
            }
            None => {
                let chosen = members.choose(&mut rng).unwrap().clone();
                Ok(RespValue::BulkString(Some(chosen.into_bytes())))
            }
        }
    }
}

// ─── SMOVE ──────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SMoveCommand {
    store: DataStore,
}

impl SMoveCommand {
    pub fn new(store: DataStore) -> Self {
        SMoveCommand { store }
    }
}

#[async_trait]
impl CommandHandler for SMoveCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'smove' command".to_string(),
            ));
        }

        let source = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid source key".to_string())),
        };

        let dest = match extract_string(&args[1]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid destination key".to_string())),
        };

        let member = match extract_string(&args[2]) {
            Some(m) => m,
            None => return Ok(RespValue::Error("ERR invalid member".to_string())),
        };

        let mut store = self.store.sets.write().await;

        // Check if source set exists and contains the member
        let removed = match store.get_mut(&source) {
            Some(src_set) => {
                if src_set.remove(&member) {
                    if src_set.is_empty() {
                        store.remove(&source);
                    }
                    true
                } else {
                    false
                }
            }
            None => false,
        };

        if removed {
            let dest_set = store.entry(dest).or_insert_with(HashSet::new);
            dest_set.insert(member);
            Ok(RespValue::Integer(1))
        } else {
            Ok(RespValue::Integer(0))
        }
    }
}

// ─── SINTER ─────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SInterCommand {
    store: DataStore,
}

impl SInterCommand {
    pub fn new(store: DataStore) -> Self {
        SInterCommand { store }
    }
}

#[async_trait]
impl CommandHandler for SInterCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'sinter' command".to_string(),
            ));
        }

        let keys: Vec<String> = args
            .iter()
            .map(extract_string)
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();

        if keys.len() != args.len() {
            return Ok(RespValue::Error("ERR invalid key".to_string()));
        }

        let store = self.store.sets.read().await;
        let empty_set = HashSet::new();

        let mut result: Option<HashSet<String>> = None;
        for key in &keys {
            let set = store.get(key).unwrap_or(&empty_set);
            result = Some(match result {
                Some(acc) => acc.intersection(set).cloned().collect(),
                None => set.clone(),
            });
        }

        let result_set = result.unwrap_or_default();
        let members: Vec<RespValue> = result_set
            .into_iter()
            .map(|s| RespValue::BulkString(Some(s.into_bytes())))
            .collect();

        Ok(RespValue::Array(Some(members)))
    }
}

// ─── SUNION ─────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SUnionCommand {
    store: DataStore,
}

impl SUnionCommand {
    pub fn new(store: DataStore) -> Self {
        SUnionCommand { store }
    }
}

#[async_trait]
impl CommandHandler for SUnionCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'sunion' command".to_string(),
            ));
        }

        let keys: Vec<String> = args
            .iter()
            .map(extract_string)
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();

        if keys.len() != args.len() {
            return Ok(RespValue::Error("ERR invalid key".to_string()));
        }

        let store = self.store.sets.read().await;
        let empty_set = HashSet::new();

        let mut result = HashSet::new();
        for key in &keys {
            let set = store.get(key).unwrap_or(&empty_set);
            for member in set {
                result.insert(member.clone());
            }
        }

        let members: Vec<RespValue> = result
            .into_iter()
            .map(|s| RespValue::BulkString(Some(s.into_bytes())))
            .collect();

        Ok(RespValue::Array(Some(members)))
    }
}

// ─── SDIFF ──────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SDiffCommand {
    store: DataStore,
}

impl SDiffCommand {
    pub fn new(store: DataStore) -> Self {
        SDiffCommand { store }
    }
}

#[async_trait]
impl CommandHandler for SDiffCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'sdiff' command".to_string(),
            ));
        }

        let keys: Vec<String> = args
            .iter()
            .map(extract_string)
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();

        if keys.len() != args.len() {
            return Ok(RespValue::Error("ERR invalid key".to_string()));
        }

        let store = self.store.sets.read().await;
        let empty_set = HashSet::new();

        let first_set = store.get(&keys[0]).unwrap_or(&empty_set).clone();
        let mut result = first_set;

        for key in keys.iter().skip(1) {
            let set = store.get(key).unwrap_or(&empty_set);
            result = result.difference(set).cloned().collect();
        }

        let members: Vec<RespValue> = result
            .into_iter()
            .map(|s| RespValue::BulkString(Some(s.into_bytes())))
            .collect();

        Ok(RespValue::Array(Some(members)))
    }
}

// ─── SINTERSTORE ────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SInterStoreCommand {
    store: DataStore,
}

impl SInterStoreCommand {
    pub fn new(store: DataStore) -> Self {
        SInterStoreCommand { store }
    }
}

#[async_trait]
impl CommandHandler for SInterStoreCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'sinterstore' command".to_string(),
            ));
        }

        let dest = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid destination key".to_string())),
        };

        let keys: Vec<String> = args[1..]
            .iter()
            .map(extract_string)
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();

        if keys.len() != args.len() - 1 {
            return Ok(RespValue::Error("ERR invalid key".to_string()));
        }

        let mut store = self.store.sets.write().await;
        let empty_set = HashSet::new();

        let mut result: Option<HashSet<String>> = None;
        for key in &keys {
            let set = store.get(key).unwrap_or(&empty_set);
            result = Some(match result {
                Some(acc) => acc.intersection(set).cloned().collect(),
                None => set.clone(),
            });
        }

        let result_set = result.unwrap_or_default();
        let count = result_set.len() as i64;

        if result_set.is_empty() {
            store.remove(&dest);
        } else {
            store.insert(dest, result_set);
        }

        Ok(RespValue::Integer(count))
    }
}

// ─── SUNIONSTORE ────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SUnionStoreCommand {
    store: DataStore,
}

impl SUnionStoreCommand {
    pub fn new(store: DataStore) -> Self {
        SUnionStoreCommand { store }
    }
}

#[async_trait]
impl CommandHandler for SUnionStoreCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'sunionstore' command".to_string(),
            ));
        }

        let dest = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid destination key".to_string())),
        };

        let keys: Vec<String> = args[1..]
            .iter()
            .map(extract_string)
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();

        if keys.len() != args.len() - 1 {
            return Ok(RespValue::Error("ERR invalid key".to_string()));
        }

        let mut store = self.store.sets.write().await;
        let empty_set = HashSet::new();

        let mut result = HashSet::new();
        for key in &keys {
            let set = store.get(key).unwrap_or(&empty_set);
            for member in set {
                result.insert(member.clone());
            }
        }

        let count = result.len() as i64;

        if result.is_empty() {
            store.remove(&dest);
        } else {
            store.insert(dest, result);
        }

        Ok(RespValue::Integer(count))
    }
}

// ─── SDIFFSTORE ─────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SDiffStoreCommand {
    store: DataStore,
}

impl SDiffStoreCommand {
    pub fn new(store: DataStore) -> Self {
        SDiffStoreCommand { store }
    }
}

#[async_trait]
impl CommandHandler for SDiffStoreCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'sdiffstore' command".to_string(),
            ));
        }

        let dest = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid destination key".to_string())),
        };

        let keys: Vec<String> = args[1..]
            .iter()
            .map(extract_string)
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();

        if keys.len() != args.len() - 1 {
            return Ok(RespValue::Error("ERR invalid key".to_string()));
        }

        let mut store = self.store.sets.write().await;
        let empty_set = HashSet::new();

        let first_set = store.get(&keys[0]).unwrap_or(&empty_set).clone();
        let mut result = first_set;

        for key in keys.iter().skip(1) {
            let set = store.get(key).unwrap_or(&empty_set);
            result = result.difference(set).cloned().collect();
        }

        let count = result.len() as i64;

        if result.is_empty() {
            store.remove(&dest);
        } else {
            store.insert(dest, result);
        }

        Ok(RespValue::Integer(count))
    }
}

// ─── SSCAN ──────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SScanCommand {
    store: DataStore,
}

impl SScanCommand {
    pub fn new(store: DataStore) -> Self {
        SScanCommand { store }
    }
}

#[async_trait]
impl CommandHandler for SScanCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        // SSCAN key cursor [MATCH pattern] [COUNT count]
        if args.len() < 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'sscan' command".to_string(),
            ));
        }

        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let cursor = match extract_string(&args[1]) {
            Some(s) => match s.parse::<usize>() {
                Ok(c) => c,
                Err(_) => return Ok(RespValue::Error("ERR invalid cursor".to_string())),
            },
            None => return Ok(RespValue::Error("ERR invalid cursor".to_string())),
        };

        let mut pattern: Option<String> = None;
        let mut count: usize = 10;

        let mut i = 2;
        while i < args.len() {
            let opt = match extract_string(&args[i]) {
                Some(s) => s.to_uppercase(),
                None => return Ok(RespValue::Error("ERR syntax error".to_string())),
            };

            match opt.as_str() {
                "MATCH" => {
                    i += 1;
                    if i >= args.len() {
                        return Ok(RespValue::Error("ERR syntax error".to_string()));
                    }
                    pattern = extract_string(&args[i]);
                    if pattern.is_none() {
                        return Ok(RespValue::Error("ERR syntax error".to_string()));
                    }
                }
                "COUNT" => {
                    i += 1;
                    if i >= args.len() {
                        return Ok(RespValue::Error("ERR syntax error".to_string()));
                    }
                    match extract_string(&args[i]) {
                        Some(s) => match s.parse::<usize>() {
                            Ok(c) if c > 0 => count = c,
                            _ => {
                                return Ok(RespValue::Error(
                                    "ERR value is not an integer or out of range".to_string(),
                                ))
                            }
                        },
                        None => {
                            return Ok(RespValue::Error(
                                "ERR value is not an integer or out of range".to_string(),
                            ))
                        }
                    }
                }
                _ => return Ok(RespValue::Error("ERR syntax error".to_string())),
            }
            i += 1;
        }

        let store = self.store.sets.read().await;

        let set = match store.get(&key) {
            Some(s) => s,
            None => {
                // Empty set: return cursor 0 and empty array
                let cursor_val = RespValue::BulkString(Some(b"0".to_vec()));
                let members = RespValue::Array(Some(vec![]));
                return Ok(RespValue::Array(Some(vec![cursor_val, members])));
            }
        };

        // Collect all members into a sorted vec for deterministic iteration
        let mut all_members: Vec<String> = set.iter().cloned().collect();
        all_members.sort();

        // Filter by pattern if provided
        let filtered: Vec<String> = if let Some(ref pat) = pattern {
            all_members
                .into_iter()
                .filter(|m| glob_match(pat, m))
                .collect()
        } else {
            all_members
        };

        let total = filtered.len();

        if total == 0 || cursor >= total {
            let cursor_val = RespValue::BulkString(Some(b"0".to_vec()));
            let members = RespValue::Array(Some(vec![]));
            return Ok(RespValue::Array(Some(vec![cursor_val, members])));
        }

        let end = (cursor + count).min(total);
        let batch: Vec<RespValue> = filtered[cursor..end]
            .iter()
            .map(|s| RespValue::BulkString(Some(s.as_bytes().to_vec())))
            .collect();

        let next_cursor = if end >= total { 0 } else { end };
        let cursor_val = RespValue::BulkString(Some(next_cursor.to_string().into_bytes()));
        let members = RespValue::Array(Some(batch));

        Ok(RespValue::Array(Some(vec![cursor_val, members])))
    }
}

// ─── SMISMEMBER ─────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SMisMemberCommand {
    store: DataStore,
}

impl SMisMemberCommand {
    pub fn new(store: DataStore) -> Self {
        SMisMemberCommand { store }
    }
}

#[async_trait]
impl CommandHandler for SMisMemberCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'smismember' command".to_string(),
            ));
        }

        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let store = self.store.sets.read().await;
        let empty_set = HashSet::new();
        let set = store.get(&key).unwrap_or(&empty_set);

        let mut results = Vec::with_capacity(args.len() - 1);
        for arg in args.iter().skip(1) {
            match extract_string(arg) {
                Some(member) => {
                    results.push(RespValue::Integer(if set.contains(&member) {
                        1
                    } else {
                        0
                    }));
                }
                None => return Ok(RespValue::Error("ERR invalid member".to_string())),
            }
        }

        Ok(RespValue::Array(Some(results)))
    }
}

// ─── SINTERCARD ─────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SInterCardCommand {
    store: DataStore,
}

impl SInterCardCommand {
    pub fn new(store: DataStore) -> Self {
        SInterCardCommand { store }
    }
}

#[async_trait]
impl CommandHandler for SInterCardCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        // SINTERCARD numkeys key [key ...] [LIMIT limit]
        if args.is_empty() {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'sintercard' command".to_string(),
            ));
        }

        let numkeys = match extract_string(&args[0]) {
            Some(s) => match s.parse::<usize>() {
                Ok(n) if n > 0 => n,
                _ => {
                    return Ok(RespValue::Error(
                        "ERR numkeys can't be non-positive value".to_string(),
                    ))
                }
            },
            None => {
                return Ok(RespValue::Error(
                    "ERR value is not an integer or out of range".to_string(),
                ))
            }
        };

        if args.len() < 1 + numkeys {
            return Ok(RespValue::Error(
                "ERR Number of keys can't be greater than number of args".to_string(),
            ));
        }

        let keys: Vec<String> = args[1..1 + numkeys]
            .iter()
            .map(extract_string)
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();

        if keys.len() != numkeys {
            return Ok(RespValue::Error("ERR invalid key".to_string()));
        }

        let mut limit: usize = 0; // 0 means no limit

        let mut i = 1 + numkeys;
        while i < args.len() {
            let opt = match extract_string(&args[i]) {
                Some(s) => s.to_uppercase(),
                None => return Ok(RespValue::Error("ERR syntax error".to_string())),
            };

            if opt == "LIMIT" {
                i += 1;
                if i >= args.len() {
                    return Ok(RespValue::Error("ERR syntax error".to_string()));
                }
                match extract_string(&args[i]) {
                    Some(s) => match s.parse::<usize>() {
                        Ok(l) => limit = l,
                        Err(_) => {
                            return Ok(RespValue::Error(
                                "ERR value is not an integer or out of range".to_string(),
                            ))
                        }
                    },
                    None => {
                        return Ok(RespValue::Error(
                            "ERR value is not an integer or out of range".to_string(),
                        ))
                    }
                }
            } else {
                return Ok(RespValue::Error("ERR syntax error".to_string()));
            }
            i += 1;
        }

        let store = self.store.sets.read().await;
        let empty_set = HashSet::new();

        let mut result: Option<HashSet<String>> = None;
        for key in &keys {
            let set = store.get(key).unwrap_or(&empty_set);
            result = Some(match result {
                Some(acc) => acc.intersection(set).cloned().collect(),
                None => set.clone(),
            });
            // Early termination: if intersection is already empty, no need to continue
            if let Some(ref r) = result {
                if r.is_empty() {
                    return Ok(RespValue::Integer(0));
                }
            }
        }

        let result_set = result.unwrap_or_default();
        let count = result_set.len() as i64;

        if limit > 0 && count > limit as i64 {
            Ok(RespValue::Integer(limit as i64))
        } else {
            Ok(RespValue::Integer(count))
        }
    }
}
