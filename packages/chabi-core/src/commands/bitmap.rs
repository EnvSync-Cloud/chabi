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

        let mut store = self.store.bitmaps.write().await;
        let bytes = store.entry(key).or_insert_with(Vec::new);
        if bytes.len() <= byte_idx {
            bytes.resize(byte_idx + 1, 0);
        }
        let old_bit = (bytes[byte_idx] >> bit_idx) & 1;
        if bit == 1 {
            bytes[byte_idx] |= 1 << bit_idx;
        } else {
            bytes[byte_idx] &= !(1 << bit_idx);
        }
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

        let store = self.store.bitmaps.read().await;
        match store.get(&key) {
            Some(bytes) => {
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

        let store = self.store.bitmaps.read().await;
        let bytes = match store.get(&key) {
            Some(val) => val.clone(),
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

        let store = self.store.bitmaps.read().await;
        let bytes = match store.get(&key) {
            Some(val) => val.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::CommandHandler;

    fn bulk(s: &str) -> RespValue {
        RespValue::BulkString(Some(s.as_bytes().to_vec()))
    }

    #[tokio::test]
    async fn test_setbit_getbit() {
        let store = DataStore::new();
        let setbit = SetBitCommand::new(store.clone());
        let getbit = GetBitCommand::new(store.clone());
        // SETBIT key 7 1 -> old bit 0
        let r = setbit
            .execute(vec![bulk("mykey"), bulk("7"), bulk("1")])
            .await
            .unwrap();
        assert_eq!(r, RespValue::Integer(0));
        // GETBIT key 7 -> 1
        let r = getbit
            .execute(vec![bulk("mykey"), bulk("7")])
            .await
            .unwrap();
        assert_eq!(r, RespValue::Integer(1));
        // GETBIT key 0 -> 0
        let r = getbit
            .execute(vec![bulk("mykey"), bulk("0")])
            .await
            .unwrap();
        assert_eq!(r, RespValue::Integer(0));
        // GETBIT nonexistent key
        let r = getbit
            .execute(vec![bulk("nokey"), bulk("0")])
            .await
            .unwrap();
        assert_eq!(r, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_setbit_overwrite() {
        let store = DataStore::new();
        let setbit = SetBitCommand::new(store.clone());
        // Set bit 7 to 1
        setbit
            .execute(vec![bulk("k"), bulk("7"), bulk("1")])
            .await
            .unwrap();
        // Set bit 7 to 0 -> old bit 1
        let r = setbit
            .execute(vec![bulk("k"), bulk("7"), bulk("0")])
            .await
            .unwrap();
        assert_eq!(r, RespValue::Integer(1));
    }

    #[tokio::test]
    async fn test_bitcount() {
        let store = DataStore::new();
        let setbit = SetBitCommand::new(store.clone());
        let bitcount = BitCountCommand::new(store.clone());
        // Set bits that produce valid ASCII (low bits only)
        setbit
            .execute(vec![bulk("k"), bulk("6"), bulk("1")])
            .await
            .unwrap();
        setbit
            .execute(vec![bulk("k"), bulk("7"), bulk("1")])
            .await
            .unwrap();
        // Byte 0 is 0x03 (valid ASCII), BITCOUNT key -> 2
        let r = bitcount.execute(vec![bulk("k")]).await.unwrap();
        assert_eq!(r, RespValue::Integer(2));
        // BITCOUNT nonexistent
        let r = bitcount.execute(vec![bulk("nokey")]).await.unwrap();
        assert_eq!(r, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_bitcount_range() {
        let store = DataStore::new();
        let setbit = SetBitCommand::new(store.clone());
        let bitcount = BitCountCommand::new(store.clone());
        // Set bits that produce valid ASCII in byte 0 and byte 1
        setbit
            .execute(vec![bulk("k"), bulk("7"), bulk("1")])
            .await
            .unwrap(); // byte 0, low bit -> 0x01
        setbit
            .execute(vec![bulk("k"), bulk("15"), bulk("1")])
            .await
            .unwrap(); // byte 1, low bit -> 0x01
                       // BITCOUNT key 0 0 -> count bits in byte 0 only
        let r = bitcount
            .execute(vec![bulk("k"), bulk("0"), bulk("0")])
            .await
            .unwrap();
        assert_eq!(r, RespValue::Integer(1));
    }

    #[tokio::test]
    async fn test_bitpos() {
        let store = DataStore::new();
        let setbit = SetBitCommand::new(store.clone());
        let bitpos = BitPosCommand::new(store.clone());
        // Set bit 7 to 1
        setbit
            .execute(vec![bulk("k"), bulk("7"), bulk("1")])
            .await
            .unwrap();
        // BITPOS key 1 -> 7
        let r = bitpos.execute(vec![bulk("k"), bulk("1")]).await.unwrap();
        assert_eq!(r, RespValue::Integer(7));
        // BITPOS key 0 -> 0 (first 0 bit)
        let r = bitpos.execute(vec![bulk("k"), bulk("0")]).await.unwrap();
        assert_eq!(r, RespValue::Integer(0));
        // BITPOS nonexistent key for bit 1 -> -1
        let r = bitpos
            .execute(vec![bulk("nokey"), bulk("1")])
            .await
            .unwrap();
        assert_eq!(r, RespValue::Integer(-1));
    }

    #[tokio::test]
    async fn test_setbit_wrong_args() {
        let store = DataStore::new();
        let cmd = SetBitCommand::new(store);
        let r = cmd.execute(vec![bulk("k"), bulk("0")]).await.unwrap();
        assert!(matches!(r, RespValue::Error(_)));
    }
}
