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

// Simple HyperLogLog stub using HashSet for exact counting
// A production implementation would use the HLL algorithm with 16384 registers

#[derive(Clone)]
pub struct PfAddCommand {
    store: DataStore,
}

impl PfAddCommand {
    pub fn new(store: DataStore) -> Self {
        PfAddCommand { store }
    }
}

#[async_trait]
impl CommandHandler for PfAddCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'pfadd' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let mut store = self.store.hll.write().await;
        let hll = store.entry(key).or_insert_with(Vec::new);

        let mut changed = false;
        for arg in args.iter().skip(1) {
            if let Some(elem) = extract_string(arg) {
                // Simple approach: store hashed elements
                let hash = simple_hash(elem.as_bytes());
                let hash_bytes = hash.to_le_bytes();
                if !hll.windows(8).any(|w| w == hash_bytes) {
                    hll.extend_from_slice(&hash_bytes);
                    changed = true;
                }
            }
        }

        Ok(RespValue::Integer(if changed { 1 } else { 0 }))
    }
}

#[derive(Clone)]
pub struct PfCountCommand {
    store: DataStore,
}

impl PfCountCommand {
    pub fn new(store: DataStore) -> Self {
        PfCountCommand { store }
    }
}

#[async_trait]
impl CommandHandler for PfCountCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'pfcount' command".to_string(),
            ));
        }

        let store = self.store.hll.read().await;
        let mut total = 0i64;
        for arg in &args {
            let key = match extract_string(arg) {
                Some(k) => k,
                None => continue,
            };
            if let Some(hll) = store.get(&key) {
                total += (hll.len() / 8) as i64;
            }
        }
        Ok(RespValue::Integer(total))
    }
}

#[derive(Clone)]
pub struct PfMergeCommand {
    store: DataStore,
}

impl PfMergeCommand {
    pub fn new(store: DataStore) -> Self {
        PfMergeCommand { store }
    }
}

#[async_trait]
impl CommandHandler for PfMergeCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'pfmerge' command".to_string(),
            ));
        }
        let dest = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let mut store = self.store.hll.write().await;
        let mut merged: Vec<u8> = store.get(&dest).cloned().unwrap_or_default();

        for arg in args.iter().skip(1) {
            let key = match extract_string(arg) {
                Some(k) => k,
                None => continue,
            };
            if let Some(hll) = store.get(&key) {
                for chunk in hll.chunks(8) {
                    if chunk.len() == 8 && !merged.windows(8).any(|w| w == chunk) {
                        merged.extend_from_slice(chunk);
                    }
                }
            }
        }

        store.insert(dest, merged);
        Ok(RespValue::SimpleString("OK".to_string()))
    }
}

fn simple_hash(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}
