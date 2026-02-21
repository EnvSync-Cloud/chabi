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

// --- LPUSH ---

#[derive(Clone)]
pub struct LPushCommand {
    store: DataStore,
}

impl LPushCommand {
    pub fn new(store: DataStore) -> Self {
        LPushCommand { store }
    }
}

#[async_trait]
impl CommandHandler for LPushCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'lpush' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let mut store = self.store.lists.write().await;
        let list = store.entry(key).or_default();
        for arg in args.iter().skip(1) {
            match extract_string(arg) {
                Some(v) => list.insert(0, v),
                None => return Ok(RespValue::Error("ERR invalid value".to_string())),
            }
        }
        Ok(RespValue::Integer(list.len() as i64))
    }
}

// --- RPUSH ---

#[derive(Clone)]
pub struct RPushCommand {
    store: DataStore,
}

impl RPushCommand {
    pub fn new(store: DataStore) -> Self {
        RPushCommand { store }
    }
}

#[async_trait]
impl CommandHandler for RPushCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'rpush' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let mut store = self.store.lists.write().await;
        let list = store.entry(key).or_default();
        for arg in args.iter().skip(1) {
            match extract_string(arg) {
                Some(v) => list.push(v),
                None => return Ok(RespValue::Error("ERR invalid value".to_string())),
            }
        }
        Ok(RespValue::Integer(list.len() as i64))
    }
}

// --- LPOP ---

#[derive(Clone)]
pub struct LPopCommand {
    store: DataStore,
}

impl LPopCommand {
    pub fn new(store: DataStore) -> Self {
        LPopCommand { store }
    }
}

#[async_trait]
impl CommandHandler for LPopCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() || args.len() > 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'lpop' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let count: Option<usize> = if args.len() == 2 {
            Some(
                extract_string(&args[1])
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1),
            )
        } else {
            None
        };

        let mut store = self.store.lists.write().await;
        match store.get_mut(&key) {
            Some(list) if !list.is_empty() => {
                if let Some(n) = count {
                    let n = n.min(list.len());
                    let popped: Vec<RespValue> = (0..n)
                        .map(|_| {
                            let v = list.remove(0);
                            RespValue::BulkString(Some(v.into_bytes()))
                        })
                        .collect();
                    if list.is_empty() {
                        store.remove(&key);
                    }
                    Ok(RespValue::Array(Some(popped)))
                } else {
                    let value = list.remove(0);
                    if list.is_empty() {
                        store.remove(&key);
                    }
                    Ok(RespValue::BulkString(Some(value.into_bytes())))
                }
            }
            _ => {
                if count.is_some() {
                    Ok(RespValue::Array(None))
                } else {
                    Ok(RespValue::BulkString(None))
                }
            }
        }
    }
}

// --- RPOP ---

#[derive(Clone)]
pub struct RPopCommand {
    store: DataStore,
}

impl RPopCommand {
    pub fn new(store: DataStore) -> Self {
        RPopCommand { store }
    }
}

#[async_trait]
impl CommandHandler for RPopCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() || args.len() > 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'rpop' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let count: Option<usize> = if args.len() == 2 {
            Some(
                extract_string(&args[1])
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1),
            )
        } else {
            None
        };

        let mut store = self.store.lists.write().await;
        match store.get_mut(&key) {
            Some(list) if !list.is_empty() => {
                if let Some(n) = count {
                    let n = n.min(list.len());
                    let popped: Vec<RespValue> = (0..n)
                        .map(|_| {
                            let v = list.pop().unwrap();
                            RespValue::BulkString(Some(v.into_bytes()))
                        })
                        .collect();
                    if list.is_empty() {
                        store.remove(&key);
                    }
                    Ok(RespValue::Array(Some(popped)))
                } else {
                    let value = list.pop().unwrap();
                    if list.is_empty() {
                        store.remove(&key);
                    }
                    Ok(RespValue::BulkString(Some(value.into_bytes())))
                }
            }
            _ => {
                if count.is_some() {
                    Ok(RespValue::Array(None))
                } else {
                    Ok(RespValue::BulkString(None))
                }
            }
        }
    }
}

// --- LRANGE ---

#[derive(Clone)]
pub struct LRangeCommand {
    store: DataStore,
}

impl LRangeCommand {
    pub fn new(store: DataStore) -> Self {
        LRangeCommand { store }
    }
}

#[async_trait]
impl CommandHandler for LRangeCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'lrange' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let start: i64 = match extract_string(&args[1]).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => return Ok(RespValue::Error("ERR value is not an integer".to_string())),
        };
        let stop: i64 = match extract_string(&args[2]).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => return Ok(RespValue::Error("ERR value is not an integer".to_string())),
        };
        let store = self.store.lists.read().await;
        match store.get(&key) {
            Some(list) => {
                let len = list.len() as i64;
                let mut s = if start < 0 { len + start } else { start };
                let mut e = if stop < 0 { len + stop } else { stop };
                if s < 0 {
                    s = 0;
                }
                if e >= len {
                    e = len - 1;
                }
                if s > e || s >= len {
                    return Ok(RespValue::Array(Some(vec![])));
                }
                let result: Vec<RespValue> = list[(s as usize)..=(e as usize)]
                    .iter()
                    .map(|v| RespValue::BulkString(Some(v.as_bytes().to_vec())))
                    .collect();
                Ok(RespValue::Array(Some(result)))
            }
            None => Ok(RespValue::Array(Some(vec![]))),
        }
    }
}

// --- LLEN ---

#[derive(Clone)]
pub struct LLenCommand {
    store: DataStore,
}

impl LLenCommand {
    pub fn new(store: DataStore) -> Self {
        LLenCommand { store }
    }
}

#[async_trait]
impl CommandHandler for LLenCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'llen' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let store = self.store.lists.read().await;
        match store.get(&key) {
            Some(list) => Ok(RespValue::Integer(list.len() as i64)),
            None => Ok(RespValue::Integer(0)),
        }
    }
}

// --- LINDEX ---

#[derive(Clone)]
pub struct LIndexCommand {
    store: DataStore,
}

impl LIndexCommand {
    pub fn new(store: DataStore) -> Self {
        LIndexCommand { store }
    }
}

#[async_trait]
impl CommandHandler for LIndexCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'lindex' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let index: i64 = match extract_string(&args[1]).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => {
                return Ok(RespValue::Error(
                    "ERR value is not an integer or out of range".to_string(),
                ))
            }
        };
        let store = self.store.lists.read().await;
        match store.get(&key) {
            Some(list) => {
                let len = list.len() as i64;
                let idx = if index < 0 { len + index } else { index };
                if idx < 0 || idx >= len {
                    Ok(RespValue::BulkString(None))
                } else {
                    Ok(RespValue::BulkString(Some(
                        list[idx as usize].as_bytes().to_vec(),
                    )))
                }
            }
            None => Ok(RespValue::BulkString(None)),
        }
    }
}

// --- LSET ---

#[derive(Clone)]
pub struct LSetCommand {
    store: DataStore,
}

impl LSetCommand {
    pub fn new(store: DataStore) -> Self {
        LSetCommand { store }
    }
}

#[async_trait]
impl CommandHandler for LSetCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'lset' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let index: i64 = match extract_string(&args[1]).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => {
                return Ok(RespValue::Error(
                    "ERR value is not an integer or out of range".to_string(),
                ))
            }
        };
        let value = match extract_string(&args[2]) {
            Some(v) => v,
            None => return Ok(RespValue::Error("ERR invalid value".to_string())),
        };
        let mut store = self.store.lists.write().await;
        match store.get_mut(&key) {
            Some(list) => {
                let len = list.len() as i64;
                let idx = if index < 0 { len + index } else { index };
                if idx < 0 || idx >= len {
                    Ok(RespValue::Error("ERR index out of range".to_string()))
                } else {
                    list[idx as usize] = value;
                    Ok(RespValue::SimpleString("OK".to_string()))
                }
            }
            None => Ok(RespValue::Error("ERR no such key".to_string())),
        }
    }
}

// --- LTRIM ---

#[derive(Clone)]
pub struct LTrimCommand {
    store: DataStore,
}

impl LTrimCommand {
    pub fn new(store: DataStore) -> Self {
        LTrimCommand { store }
    }
}

#[async_trait]
impl CommandHandler for LTrimCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'ltrim' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let start: i64 = match extract_string(&args[1]).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => return Ok(RespValue::Error("ERR value is not an integer".to_string())),
        };
        let stop: i64 = match extract_string(&args[2]).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => return Ok(RespValue::Error("ERR value is not an integer".to_string())),
        };
        let mut store = self.store.lists.write().await;
        if let Some(list) = store.get_mut(&key) {
            let len = list.len() as i64;
            let mut s = if start < 0 { len + start } else { start };
            let mut e = if stop < 0 { len + stop } else { stop };
            if s < 0 {
                s = 0;
            }
            if e >= len {
                e = len - 1;
            }
            if s > e || s >= len {
                store.remove(&key);
            } else {
                *list = list[(s as usize)..=(e as usize)].to_vec();
            }
        }
        Ok(RespValue::SimpleString("OK".to_string()))
    }
}

// --- LINSERT ---

#[derive(Clone)]
pub struct LInsertCommand {
    store: DataStore,
}

impl LInsertCommand {
    pub fn new(store: DataStore) -> Self {
        LInsertCommand { store }
    }
}

#[async_trait]
impl CommandHandler for LInsertCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 4 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'linsert' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let position = match extract_string(&args[1]) {
            Some(p) => p.to_uppercase(),
            None => return Ok(RespValue::Error("ERR syntax error".to_string())),
        };
        let pivot = match extract_string(&args[2]) {
            Some(v) => v,
            None => return Ok(RespValue::Error("ERR invalid pivot".to_string())),
        };
        let element = match extract_string(&args[3]) {
            Some(v) => v,
            None => return Ok(RespValue::Error("ERR invalid element".to_string())),
        };

        let mut store = self.store.lists.write().await;
        match store.get_mut(&key) {
            Some(list) => {
                if let Some(pos) = list.iter().position(|v| v == &pivot) {
                    match position.as_str() {
                        "BEFORE" => list.insert(pos, element),
                        "AFTER" => list.insert(pos + 1, element),
                        _ => return Ok(RespValue::Error("ERR syntax error".to_string())),
                    }
                    Ok(RespValue::Integer(list.len() as i64))
                } else {
                    Ok(RespValue::Integer(-1))
                }
            }
            None => Ok(RespValue::Integer(0)),
        }
    }
}

// --- LREM ---

#[derive(Clone)]
pub struct LRemCommand {
    store: DataStore,
}

impl LRemCommand {
    pub fn new(store: DataStore) -> Self {
        LRemCommand { store }
    }
}

#[async_trait]
impl CommandHandler for LRemCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'lrem' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let count: i64 = match extract_string(&args[1]).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => {
                return Ok(RespValue::Error(
                    "ERR value is not an integer or out of range".to_string(),
                ))
            }
        };
        let element = match extract_string(&args[2]) {
            Some(v) => v,
            None => return Ok(RespValue::Error("ERR invalid element".to_string())),
        };

        let mut store = self.store.lists.write().await;
        match store.get_mut(&key) {
            Some(list) => {
                let mut removed = 0i64;
                if count > 0 {
                    let mut i = 0;
                    while i < list.len() && removed < count {
                        if list[i] == element {
                            list.remove(i);
                            removed += 1;
                        } else {
                            i += 1;
                        }
                    }
                } else if count < 0 {
                    let target = (-count) as i64;
                    let mut i = list.len();
                    while i > 0 && removed < target {
                        i -= 1;
                        if list[i] == element {
                            list.remove(i);
                            removed += 1;
                        }
                    }
                } else {
                    list.retain(|v| {
                        if v == &element {
                            removed += 1;
                            false
                        } else {
                            true
                        }
                    });
                }
                if list.is_empty() {
                    store.remove(&key);
                }
                Ok(RespValue::Integer(removed))
            }
            None => Ok(RespValue::Integer(0)),
        }
    }
}

// --- LPOS ---

#[derive(Clone)]
pub struct LPosCommand {
    store: DataStore,
}

impl LPosCommand {
    pub fn new(store: DataStore) -> Self {
        LPosCommand { store }
    }
}

#[async_trait]
impl CommandHandler for LPosCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'lpos' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let element = match extract_string(&args[1]) {
            Some(v) => v,
            None => return Ok(RespValue::Error("ERR invalid element".to_string())),
        };
        let mut count: Option<usize> = None;
        let mut rank: i64 = 1;
        let mut maxlen: usize = 0;
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
                "RANK" => {
                    i += 1;
                    rank = extract_string(&args[i])
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(1);
                }
                "COUNT" => {
                    i += 1;
                    count = extract_string(&args[i]).and_then(|s| s.parse().ok());
                }
                "MAXLEN" => {
                    i += 1;
                    maxlen = extract_string(&args[i])
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                }
                _ => {}
            }
            i += 1;
        }

        let store = self.store.lists.read().await;
        match store.get(&key) {
            Some(list) => {
                let len = list.len();
                let scan_len = if maxlen > 0 { maxlen.min(len) } else { len };
                let mut positions = Vec::new();
                if rank > 0 {
                    let mut skip = (rank - 1) as usize;
                    for (idx, item) in list.iter().enumerate().take(scan_len) {
                        if *item == element {
                            if skip > 0 {
                                skip -= 1;
                            } else {
                                positions.push(idx as i64);
                                if let Some(c) = count {
                                    if c > 0 && positions.len() >= c {
                                        break;
                                    }
                                } else {
                                    break;
                                }
                            }
                        }
                    }
                } else {
                    let mut skip = ((-rank) - 1) as usize;
                    let start = len.saturating_sub(scan_len);
                    for idx in (start..len).rev() {
                        if list[idx] == element {
                            if skip > 0 {
                                skip -= 1;
                            } else {
                                positions.push(idx as i64);
                                if let Some(c) = count {
                                    if c > 0 && positions.len() >= c {
                                        break;
                                    }
                                } else {
                                    break;
                                }
                            }
                        }
                    }
                }

                if count.is_some() {
                    Ok(RespValue::Array(Some(
                        positions.into_iter().map(RespValue::Integer).collect(),
                    )))
                } else if positions.is_empty() {
                    Ok(RespValue::BulkString(None))
                } else {
                    Ok(RespValue::Integer(positions[0]))
                }
            }
            None => {
                if count.is_some() {
                    Ok(RespValue::Array(Some(vec![])))
                } else {
                    Ok(RespValue::BulkString(None))
                }
            }
        }
    }
}

// --- LPUSHX ---

#[derive(Clone)]
pub struct LPushXCommand {
    store: DataStore,
}

impl LPushXCommand {
    pub fn new(store: DataStore) -> Self {
        LPushXCommand { store }
    }
}

#[async_trait]
impl CommandHandler for LPushXCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'lpushx' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let mut store = self.store.lists.write().await;
        match store.get_mut(&key) {
            Some(list) => {
                for arg in args.iter().skip(1) {
                    match extract_string(arg) {
                        Some(v) => list.insert(0, v),
                        None => return Ok(RespValue::Error("ERR invalid value".to_string())),
                    }
                }
                Ok(RespValue::Integer(list.len() as i64))
            }
            None => Ok(RespValue::Integer(0)),
        }
    }
}

// --- RPUSHX ---

#[derive(Clone)]
pub struct RPushXCommand {
    store: DataStore,
}

impl RPushXCommand {
    pub fn new(store: DataStore) -> Self {
        RPushXCommand { store }
    }
}

#[async_trait]
impl CommandHandler for RPushXCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'rpushx' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let mut store = self.store.lists.write().await;
        match store.get_mut(&key) {
            Some(list) => {
                for arg in args.iter().skip(1) {
                    match extract_string(arg) {
                        Some(v) => list.push(v),
                        None => return Ok(RespValue::Error("ERR invalid value".to_string())),
                    }
                }
                Ok(RespValue::Integer(list.len() as i64))
            }
            None => Ok(RespValue::Integer(0)),
        }
    }
}

// --- LMOVE ---

#[derive(Clone)]
pub struct LMoveCommand {
    store: DataStore,
}

impl LMoveCommand {
    pub fn new(store: DataStore) -> Self {
        LMoveCommand { store }
    }
}

#[async_trait]
impl CommandHandler for LMoveCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 4 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'lmove' command".to_string(),
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
        let wherefrom = match extract_string(&args[2]) {
            Some(d) => d.to_uppercase(),
            None => return Ok(RespValue::Error("ERR syntax error".to_string())),
        };
        let whereto = match extract_string(&args[3]) {
            Some(d) => d.to_uppercase(),
            None => return Ok(RespValue::Error("ERR syntax error".to_string())),
        };

        let mut store = self.store.lists.write().await;
        let src_list = match store.get_mut(&source) {
            Some(l) if !l.is_empty() => l,
            _ => return Ok(RespValue::BulkString(None)),
        };

        let element = match wherefrom.as_str() {
            "LEFT" => src_list.remove(0),
            "RIGHT" => src_list.pop().unwrap(),
            _ => return Ok(RespValue::Error("ERR syntax error".to_string())),
        };

        if src_list.is_empty() && source != dest {
            store.remove(&source);
        }

        let dest_list = store.entry(dest).or_default();
        match whereto.as_str() {
            "LEFT" => dest_list.insert(0, element.clone()),
            "RIGHT" => dest_list.push(element.clone()),
            _ => return Ok(RespValue::Error("ERR syntax error".to_string())),
        }

        Ok(RespValue::BulkString(Some(element.into_bytes())))
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

    // 1. LPUSH inserts at head, RPUSH appends at tail; verify with LRANGE 0 -1
    #[tokio::test]
    async fn test_lpush_rpush() {
        let store = DataStore::new();
        let lpush = LPushCommand::new(store.clone());
        let rpush = RPushCommand::new(store.clone());
        let lrange = LRangeCommand::new(store.clone());

        // LPUSH mylist a b c  =>  list is [c, b, a]
        let res = lpush
            .execute(vec![bulk("mylist"), bulk("a"), bulk("b"), bulk("c")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Integer(3));

        // RPUSH mylist d e  =>  list is [c, b, a, d, e]
        let res = rpush
            .execute(vec![bulk("mylist"), bulk("d"), bulk("e")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Integer(5));

        // Verify full list via LRANGE 0 -1
        let res = lrange
            .execute(vec![bulk("mylist"), bulk("0"), bulk("-1")])
            .await
            .unwrap();
        assert_eq!(
            res,
            RespValue::Array(Some(vec![
                bulk("c"),
                bulk("b"),
                bulk("a"),
                bulk("d"),
                bulk("e"),
            ]))
        );
    }

    // 2. LPOP without count returns a single bulk string
    #[tokio::test]
    async fn test_lpop_single() {
        let store = DataStore::new();
        let rpush = RPushCommand::new(store.clone());
        let lpop = LPopCommand::new(store.clone());

        rpush
            .execute(vec![bulk("k"), bulk("a"), bulk("b"), bulk("c")])
            .await
            .unwrap();

        let res = lpop.execute(vec![bulk("k")]).await.unwrap();
        assert_eq!(res, RespValue::BulkString(Some(b"a".to_vec())));

        // Pop from missing key returns nil
        let res = lpop.execute(vec![bulk("missing")]).await.unwrap();
        assert_eq!(res, RespValue::BulkString(None));
    }

    // 3. LPOP with count returns an array
    #[tokio::test]
    async fn test_lpop_count() {
        let store = DataStore::new();
        let rpush = RPushCommand::new(store.clone());
        let lpop = LPopCommand::new(store.clone());

        rpush
            .execute(vec![bulk("k"), bulk("a"), bulk("b"), bulk("c"), bulk("d")])
            .await
            .unwrap();

        let res = lpop.execute(vec![bulk("k"), bulk("2")]).await.unwrap();
        assert_eq!(
            res,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"a".to_vec())),
                RespValue::BulkString(Some(b"b".to_vec())),
            ]))
        );

        // Pop with count from missing key returns nil array
        let res = lpop
            .execute(vec![bulk("missing"), bulk("2")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Array(None));
    }

    // 4. RPOP without count and with count
    #[tokio::test]
    async fn test_rpop() {
        let store = DataStore::new();
        let rpush = RPushCommand::new(store.clone());
        let rpop = RPopCommand::new(store.clone());

        rpush
            .execute(vec![bulk("k"), bulk("a"), bulk("b"), bulk("c")])
            .await
            .unwrap();

        // RPOP without count
        let res = rpop.execute(vec![bulk("k")]).await.unwrap();
        assert_eq!(res, RespValue::BulkString(Some(b"c".to_vec())));

        // RPOP with count=2 from remaining [a, b]
        let res = rpop.execute(vec![bulk("k"), bulk("2")]).await.unwrap();
        assert_eq!(
            res,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"b".to_vec())),
                RespValue::BulkString(Some(b"a".to_vec())),
            ]))
        );

        // RPOP from now-empty key
        let res = rpop.execute(vec![bulk("k")]).await.unwrap();
        assert_eq!(res, RespValue::BulkString(None));
    }

    // 5. LRANGE with various indices including negative
    #[tokio::test]
    async fn test_lrange() {
        let store = DataStore::new();
        let rpush = RPushCommand::new(store.clone());
        let lrange = LRangeCommand::new(store.clone());

        // list = [a, b, c, d, e]
        rpush
            .execute(vec![
                bulk("k"),
                bulk("a"),
                bulk("b"),
                bulk("c"),
                bulk("d"),
                bulk("e"),
            ])
            .await
            .unwrap();

        // Positive range: 1..3
        let res = lrange
            .execute(vec![bulk("k"), bulk("1"), bulk("3")])
            .await
            .unwrap();
        assert_eq!(
            res,
            RespValue::Array(Some(vec![bulk("b"), bulk("c"), bulk("d")]))
        );

        // Negative range: last two elements
        let res = lrange
            .execute(vec![bulk("k"), bulk("-2"), bulk("-1")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Array(Some(vec![bulk("d"), bulk("e")])));

        // Out of range returns empty array
        let res = lrange
            .execute(vec![bulk("k"), bulk("10"), bulk("20")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Array(Some(vec![])));

        // Missing key returns empty array
        let res = lrange
            .execute(vec![bulk("nope"), bulk("0"), bulk("-1")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Array(Some(vec![])));
    }

    // 6. LLEN for existing and missing key
    #[tokio::test]
    async fn test_llen() {
        let store = DataStore::new();
        let rpush = RPushCommand::new(store.clone());
        let llen = LLenCommand::new(store.clone());

        // Missing key => 0
        let res = llen.execute(vec![bulk("k")]).await.unwrap();
        assert_eq!(res, RespValue::Integer(0));

        rpush
            .execute(vec![bulk("k"), bulk("a"), bulk("b"), bulk("c")])
            .await
            .unwrap();

        let res = llen.execute(vec![bulk("k")]).await.unwrap();
        assert_eq!(res, RespValue::Integer(3));
    }

    // 7. LINDEX with positive and negative indices
    #[tokio::test]
    async fn test_lindex() {
        let store = DataStore::new();
        let rpush = RPushCommand::new(store.clone());
        let lindex = LIndexCommand::new(store.clone());

        // list = [a, b, c]
        rpush
            .execute(vec![bulk("k"), bulk("a"), bulk("b"), bulk("c")])
            .await
            .unwrap();

        // Positive index
        let res = lindex.execute(vec![bulk("k"), bulk("0")]).await.unwrap();
        assert_eq!(res, RespValue::BulkString(Some(b"a".to_vec())));

        let res = lindex.execute(vec![bulk("k"), bulk("2")]).await.unwrap();
        assert_eq!(res, RespValue::BulkString(Some(b"c".to_vec())));

        // Negative index: -1 = last
        let res = lindex.execute(vec![bulk("k"), bulk("-1")]).await.unwrap();
        assert_eq!(res, RespValue::BulkString(Some(b"c".to_vec())));

        // Out of range returns nil
        let res = lindex.execute(vec![bulk("k"), bulk("10")]).await.unwrap();
        assert_eq!(res, RespValue::BulkString(None));
    }

    // 8. LSET at valid and invalid index
    #[tokio::test]
    async fn test_lset() {
        let store = DataStore::new();
        let rpush = RPushCommand::new(store.clone());
        let lset = LSetCommand::new(store.clone());
        let lindex = LIndexCommand::new(store.clone());

        // list = [a, b, c]
        rpush
            .execute(vec![bulk("k"), bulk("a"), bulk("b"), bulk("c")])
            .await
            .unwrap();

        // Valid LSET at index 1
        let res = lset
            .execute(vec![bulk("k"), bulk("1"), bulk("B")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::SimpleString("OK".to_string()));

        let res = lindex.execute(vec![bulk("k"), bulk("1")]).await.unwrap();
        assert_eq!(res, RespValue::BulkString(Some(b"B".to_vec())));

        // LSET with negative index
        let res = lset
            .execute(vec![bulk("k"), bulk("-1"), bulk("C")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::SimpleString("OK".to_string()));

        // Out of range index
        let res = lset
            .execute(vec![bulk("k"), bulk("99"), bulk("x")])
            .await
            .unwrap();
        assert!(matches!(res, RespValue::Error(_)));

        // Non-existent key
        let res = lset
            .execute(vec![bulk("nope"), bulk("0"), bulk("x")])
            .await
            .unwrap();
        assert!(matches!(res, RespValue::Error(_)));
    }

    // 9. LTRIM keeps only the specified range
    #[tokio::test]
    async fn test_ltrim() {
        let store = DataStore::new();
        let rpush = RPushCommand::new(store.clone());
        let ltrim = LTrimCommand::new(store.clone());
        let lrange = LRangeCommand::new(store.clone());

        // list = [a, b, c, d, e]
        rpush
            .execute(vec![
                bulk("k"),
                bulk("a"),
                bulk("b"),
                bulk("c"),
                bulk("d"),
                bulk("e"),
            ])
            .await
            .unwrap();

        // LTRIM 1 3 => keep [b, c, d]
        let res = ltrim
            .execute(vec![bulk("k"), bulk("1"), bulk("3")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::SimpleString("OK".to_string()));

        let res = lrange
            .execute(vec![bulk("k"), bulk("0"), bulk("-1")])
            .await
            .unwrap();
        assert_eq!(
            res,
            RespValue::Array(Some(vec![bulk("b"), bulk("c"), bulk("d")]))
        );
    }

    // 10. LINSERT BEFORE and AFTER
    #[tokio::test]
    async fn test_linsert_before_after() {
        let store = DataStore::new();
        let rpush = RPushCommand::new(store.clone());
        let linsert = LInsertCommand::new(store.clone());
        let lrange = LRangeCommand::new(store.clone());

        // list = [a, b, c]
        rpush
            .execute(vec![bulk("k"), bulk("a"), bulk("b"), bulk("c")])
            .await
            .unwrap();

        // LINSERT BEFORE "b" "x"  =>  [a, x, b, c]
        let res = linsert
            .execute(vec![bulk("k"), bulk("BEFORE"), bulk("b"), bulk("x")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Integer(4));

        // LINSERT AFTER "b" "y"  =>  [a, x, b, y, c]
        let res = linsert
            .execute(vec![bulk("k"), bulk("AFTER"), bulk("b"), bulk("y")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Integer(5));

        let res = lrange
            .execute(vec![bulk("k"), bulk("0"), bulk("-1")])
            .await
            .unwrap();
        assert_eq!(
            res,
            RespValue::Array(Some(vec![
                bulk("a"),
                bulk("x"),
                bulk("b"),
                bulk("y"),
                bulk("c"),
            ]))
        );

        // Pivot not found returns -1
        let res = linsert
            .execute(vec![bulk("k"), bulk("BEFORE"), bulk("zzz"), bulk("w")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Integer(-1));

        // Missing key returns 0
        let res = linsert
            .execute(vec![bulk("nope"), bulk("BEFORE"), bulk("a"), bulk("w")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Integer(0));
    }

    // 11. LREM with positive count (remove from head)
    #[tokio::test]
    async fn test_lrem_positive() {
        let store = DataStore::new();
        let rpush = RPushCommand::new(store.clone());
        let lrem = LRemCommand::new(store.clone());
        let lrange = LRangeCommand::new(store.clone());

        // list = [a, b, a, c, a]
        rpush
            .execute(vec![
                bulk("k"),
                bulk("a"),
                bulk("b"),
                bulk("a"),
                bulk("c"),
                bulk("a"),
            ])
            .await
            .unwrap();

        // LREM k 2 a  => remove first 2 "a" from head => [b, c, a]
        let res = lrem
            .execute(vec![bulk("k"), bulk("2"), bulk("a")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Integer(2));

        let res = lrange
            .execute(vec![bulk("k"), bulk("0"), bulk("-1")])
            .await
            .unwrap();
        assert_eq!(
            res,
            RespValue::Array(Some(vec![bulk("b"), bulk("c"), bulk("a")]))
        );
    }

    // 12. LREM with count=0 (remove all occurrences)
    #[tokio::test]
    async fn test_lrem_zero() {
        let store = DataStore::new();
        let rpush = RPushCommand::new(store.clone());
        let lrem = LRemCommand::new(store.clone());
        let lrange = LRangeCommand::new(store.clone());

        // list = [a, b, a, c, a]
        rpush
            .execute(vec![
                bulk("k"),
                bulk("a"),
                bulk("b"),
                bulk("a"),
                bulk("c"),
                bulk("a"),
            ])
            .await
            .unwrap();

        // LREM k 0 a  => remove ALL "a" => [b, c]
        let res = lrem
            .execute(vec![bulk("k"), bulk("0"), bulk("a")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Integer(3));

        let res = lrange
            .execute(vec![bulk("k"), bulk("0"), bulk("-1")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Array(Some(vec![bulk("b"), bulk("c")])));
    }

    // 13. LPOS basic (find position of an element)
    #[tokio::test]
    async fn test_lpos() {
        let store = DataStore::new();
        let rpush = RPushCommand::new(store.clone());
        let lpos = LPosCommand::new(store.clone());

        // list = [a, b, c, b, d]
        rpush
            .execute(vec![
                bulk("k"),
                bulk("a"),
                bulk("b"),
                bulk("c"),
                bulk("b"),
                bulk("d"),
            ])
            .await
            .unwrap();

        // Basic LPOS: first occurrence of "b" is at index 1
        let res = lpos.execute(vec![bulk("k"), bulk("b")]).await.unwrap();
        assert_eq!(res, RespValue::Integer(1));

        // LPOS with COUNT 0 => all occurrences
        let res = lpos
            .execute(vec![bulk("k"), bulk("b"), bulk("COUNT"), bulk("0")])
            .await
            .unwrap();
        assert_eq!(
            res,
            RespValue::Array(Some(vec![RespValue::Integer(1), RespValue::Integer(3),]))
        );

        // LPOS with RANK 2 => second occurrence of "b" at index 3
        let res = lpos
            .execute(vec![bulk("k"), bulk("b"), bulk("RANK"), bulk("2")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Integer(3));

        // Element not found returns nil
        let res = lpos.execute(vec![bulk("k"), bulk("z")]).await.unwrap();
        assert_eq!(res, RespValue::BulkString(None));
    }

    // 14. LPUSHX/RPUSHX on existing and missing key
    #[tokio::test]
    async fn test_lpushx_rpushx() {
        let store = DataStore::new();
        let rpush = RPushCommand::new(store.clone());
        let lpushx = LPushXCommand::new(store.clone());
        let rpushx = RPushXCommand::new(store.clone());
        let lrange = LRangeCommand::new(store.clone());

        // LPUSHX on missing key does nothing, returns 0
        let res = lpushx.execute(vec![bulk("k"), bulk("x")]).await.unwrap();
        assert_eq!(res, RespValue::Integer(0));

        // RPUSHX on missing key does nothing, returns 0
        let res = rpushx.execute(vec![bulk("k"), bulk("x")]).await.unwrap();
        assert_eq!(res, RespValue::Integer(0));

        // Create the list first: [a, b]
        rpush
            .execute(vec![bulk("k"), bulk("a"), bulk("b")])
            .await
            .unwrap();

        // LPUSHX on existing key: [x, a, b]
        let res = lpushx.execute(vec![bulk("k"), bulk("x")]).await.unwrap();
        assert_eq!(res, RespValue::Integer(3));

        // RPUSHX on existing key: [x, a, b, y]
        let res = rpushx.execute(vec![bulk("k"), bulk("y")]).await.unwrap();
        assert_eq!(res, RespValue::Integer(4));

        let res = lrange
            .execute(vec![bulk("k"), bulk("0"), bulk("-1")])
            .await
            .unwrap();
        assert_eq!(
            res,
            RespValue::Array(Some(vec![bulk("x"), bulk("a"), bulk("b"), bulk("y"),]))
        );
    }

    // 15. LMOVE source LEFT -> dest RIGHT
    #[tokio::test]
    async fn test_lmove() {
        let store = DataStore::new();
        let rpush = RPushCommand::new(store.clone());
        let lmove = LMoveCommand::new(store.clone());
        let lrange = LRangeCommand::new(store.clone());

        // source = [a, b, c]
        rpush
            .execute(vec![bulk("src"), bulk("a"), bulk("b"), bulk("c")])
            .await
            .unwrap();

        // dest = [x]
        rpush.execute(vec![bulk("dst"), bulk("x")]).await.unwrap();

        // LMOVE src dst LEFT RIGHT => pops "a" from src left, pushes to dst right
        let res = lmove
            .execute(vec![bulk("src"), bulk("dst"), bulk("LEFT"), bulk("RIGHT")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::BulkString(Some(b"a".to_vec())));

        // src = [b, c]
        let res = lrange
            .execute(vec![bulk("src"), bulk("0"), bulk("-1")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Array(Some(vec![bulk("b"), bulk("c")])));

        // dst = [x, a]
        let res = lrange
            .execute(vec![bulk("dst"), bulk("0"), bulk("-1")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Array(Some(vec![bulk("x"), bulk("a")])));

        // LMOVE src dst RIGHT LEFT => pops "c" from src right, pushes to dst left
        let res = lmove
            .execute(vec![bulk("src"), bulk("dst"), bulk("RIGHT"), bulk("LEFT")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::BulkString(Some(b"c".to_vec())));

        // src = [b]
        let res = lrange
            .execute(vec![bulk("src"), bulk("0"), bulk("-1")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::Array(Some(vec![bulk("b")])));

        // dst = [c, x, a]
        let res = lrange
            .execute(vec![bulk("dst"), bulk("0"), bulk("-1")])
            .await
            .unwrap();
        assert_eq!(
            res,
            RespValue::Array(Some(vec![bulk("c"), bulk("x"), bulk("a")]))
        );

        // LMOVE from missing key returns nil
        let res = lmove
            .execute(vec![bulk("nope"), bulk("dst"), bulk("LEFT"), bulk("RIGHT")])
            .await
            .unwrap();
        assert_eq!(res, RespValue::BulkString(None));
    }
}
