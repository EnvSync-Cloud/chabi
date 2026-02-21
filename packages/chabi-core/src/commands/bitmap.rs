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

// --- SETBIT ---

#[derive(Clone)]
pub struct SetBitCommand {
    store: DataStore,
}

impl SetBitCommand {
    pub fn new(store: DataStore) -> Self {
        SetBitCommand { store }
    }
}

#[async_trait]
impl CommandHandler for SetBitCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'setbit' command".to_string(),
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
                    "ERR bit offset is not an integer or out of range".to_string(),
                ))
            }
        };
        let bit: u8 = match extract_string(&args[2]).and_then(|s| s.parse().ok()) {
            Some(v) if v <= 1 => v,
            _ => {
                return Ok(RespValue::Error(
                    "ERR bit is not an integer or out of range".to_string(),
                ))
            }
        };

        let byte_idx = offset / 8;
        let bit_idx = 7 - (offset % 8);

        let mut store = self.store.strings.write().await;
        let val = store.entry(key).or_insert_with(String::new);
        let mut bytes = val.as_bytes().to_vec();
        if bytes.len() <= byte_idx {
            bytes.resize(byte_idx + 1, 0);
        }
        let old_bit = (bytes[byte_idx] >> bit_idx) & 1;
        if bit == 1 {
            bytes[byte_idx] |= 1 << bit_idx;
        } else {
            bytes[byte_idx] &= !(1 << bit_idx);
        }
        *val = String::from_utf8_lossy(&bytes).to_string();
        Ok(RespValue::Integer(old_bit as i64))
    }
}

// --- GETBIT ---

#[derive(Clone)]
pub struct GetBitCommand {
    store: DataStore,
}

impl GetBitCommand {
    pub fn new(store: DataStore) -> Self {
        GetBitCommand { store }
    }
}

#[async_trait]
impl CommandHandler for GetBitCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'getbit' command".to_string(),
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
                    "ERR bit offset is not an integer or out of range".to_string(),
                ))
            }
        };

        let byte_idx = offset / 8;
        let bit_idx = 7 - (offset % 8);

        let store = self.store.strings.read().await;
        match store.get(&key) {
            Some(val) => {
                let bytes = val.as_bytes();
                if byte_idx >= bytes.len() {
                    Ok(RespValue::Integer(0))
                } else {
                    Ok(RespValue::Integer(
                        ((bytes[byte_idx] >> bit_idx) & 1) as i64,
                    ))
                }
            }
            None => Ok(RespValue::Integer(0)),
        }
    }
}

// --- BITCOUNT ---

#[derive(Clone)]
pub struct BitCountCommand {
    store: DataStore,
}

impl BitCountCommand {
    pub fn new(store: DataStore) -> Self {
        BitCountCommand { store }
    }
}

#[async_trait]
impl CommandHandler for BitCountCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'bitcount' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let store = self.store.strings.read().await;
        let bytes = match store.get(&key) {
            Some(val) => val.as_bytes().to_vec(),
            None => return Ok(RespValue::Integer(0)),
        };

        let (start, end) = if args.len() >= 3 {
            let len = bytes.len() as i64;
            let s: i64 = extract_string(&args[1])
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let e: i64 = extract_string(&args[2])
                .and_then(|s| s.parse().ok())
                .unwrap_or(-1);
            let s = if s < 0 { (len + s).max(0) } else { s.min(len) } as usize;
            let e = if e < 0 {
                (len + e).max(0)
            } else {
                e.min(len - 1)
            } as usize;
            (s, e)
        } else {
            (0, bytes.len().saturating_sub(1))
        };

        if start > end || start >= bytes.len() {
            return Ok(RespValue::Integer(0));
        }

        let count: u32 = bytes[start..=end].iter().map(|b| b.count_ones()).sum();
        Ok(RespValue::Integer(count as i64))
    }
}

// --- BITPOS ---

#[derive(Clone)]
pub struct BitPosCommand {
    store: DataStore,
}

impl BitPosCommand {
    pub fn new(store: DataStore) -> Self {
        BitPosCommand { store }
    }
}

#[async_trait]
impl CommandHandler for BitPosCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'bitpos' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let bit: u8 = match extract_string(&args[1]).and_then(|s| s.parse().ok()) {
            Some(v) if v <= 1 => v,
            _ => {
                return Ok(RespValue::Error(
                    "ERR bit is not an integer or out of range".to_string(),
                ))
            }
        };

        let store = self.store.strings.read().await;
        let bytes = match store.get(&key) {
            Some(val) => val.as_bytes().to_vec(),
            None => {
                return Ok(RespValue::Integer(if bit == 0 { 0 } else { -1 }));
            }
        };

        let len = bytes.len() as i64;
        let start = if args.len() >= 3 {
            let s: i64 = extract_string(&args[2])
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            if s < 0 {
                (len + s).max(0) as usize
            } else {
                s as usize
            }
        } else {
            0
        };
        let end = if args.len() >= 4 {
            let e: i64 = extract_string(&args[3])
                .and_then(|s| s.parse().ok())
                .unwrap_or(-1);
            if e < 0 {
                (len + e).max(0) as usize
            } else {
                (e as usize).min(bytes.len().saturating_sub(1))
            }
        } else {
            bytes.len().saturating_sub(1)
        };

        if start > end || start >= bytes.len() {
            return Ok(RespValue::Integer(-1));
        }

        for (i, &byte) in bytes[start..=end].iter().enumerate() {
            let byte_idx = start + i;
            for bit_idx in (0..8).rev() {
                let b = (byte >> bit_idx) & 1;
                if b == bit {
                    let pos = byte_idx * 8 + (7 - bit_idx);
                    return Ok(RespValue::Integer(pos as i64));
                }
            }
        }
        Ok(RespValue::Integer(-1))
    }
}
