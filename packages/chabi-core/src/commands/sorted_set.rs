use crate::commands::CommandHandler;
use crate::resp::RespValue;
use crate::storage::DataStore;
use crate::Result;
use async_trait::async_trait;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

fn extract_string(val: &RespValue) -> Option<String> {
    match val {
        RespValue::BulkString(Some(bytes)) => Some(String::from_utf8_lossy(bytes).to_string()),
        _ => None,
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SortedSet {
    pub scores: HashMap<String, f64>,
    pub members: BTreeMap<(OrderedFloat<f64>, String), ()>,
}

impl SortedSet {
    pub fn new() -> Self {
        SortedSet {
            scores: HashMap::new(),
            members: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, member: String, score: f64) -> bool {
        let is_new = if let Some(&old_score) = self.scores.get(&member) {
            self.members
                .remove(&(OrderedFloat(old_score), member.clone()));
            false
        } else {
            true
        };
        self.scores.insert(member.clone(), score);
        self.members.insert((OrderedFloat(score), member), ());
        is_new
    }

    pub fn remove(&mut self, member: &str) -> bool {
        if let Some(score) = self.scores.remove(member) {
            self.members
                .remove(&(OrderedFloat(score), member.to_string()));
            true
        } else {
            false
        }
    }

    pub fn score(&self, member: &str) -> Option<f64> {
        self.scores.get(member).copied()
    }

    pub fn len(&self) -> usize {
        self.scores.len()
    }

    pub fn is_empty(&self) -> bool {
        self.scores.is_empty()
    }

    pub fn rank(&self, member: &str) -> Option<usize> {
        let score = self.scores.get(member)?;
        let target = (OrderedFloat(*score), member.to_string());
        Some(self.members.range(..&target).count())
    }

    pub fn rev_rank(&self, member: &str) -> Option<usize> {
        let rank = self.rank(member)?;
        Some(self.len() - 1 - rank)
    }

    pub fn range_by_index(&self, start: i64, stop: i64) -> Vec<(String, f64)> {
        let len = self.len() as i64;
        let s = if start < 0 {
            (len + start).max(0)
        } else {
            start
        };
        let e = if stop < 0 {
            (len + stop).max(0)
        } else {
            stop.min(len - 1)
        };
        if s > e || s >= len {
            return vec![];
        }
        self.members
            .iter()
            .skip(s as usize)
            .take((e - s + 1) as usize)
            .map(|((score, member), _)| (member.clone(), score.0))
            .collect()
    }

    pub fn rev_range_by_index(&self, start: i64, stop: i64) -> Vec<(String, f64)> {
        let len = self.len() as i64;
        let s = if start < 0 {
            (len + start).max(0)
        } else {
            start
        };
        let e = if stop < 0 {
            (len + stop).max(0)
        } else {
            stop.min(len - 1)
        };
        if s > e || s >= len {
            return vec![];
        }
        self.members
            .iter()
            .rev()
            .skip(s as usize)
            .take((e - s + 1) as usize)
            .map(|((score, member), _)| (member.clone(), score.0))
            .collect()
    }

    pub fn range_by_score(&self, min: f64, max: f64) -> Vec<(String, f64)> {
        let lo = (OrderedFloat(min), String::new());
        let hi = (OrderedFloat(max), String::from("\x7f\x7f\x7f\x7f"));
        self.members
            .range(lo..=hi)
            .map(|((score, member), _)| (member.clone(), score.0))
            .collect()
    }

    pub fn count_in_score_range(&self, min: f64, max: f64) -> usize {
        let lo = (OrderedFloat(min), String::new());
        let hi = (OrderedFloat(max), String::from("\x7f\x7f\x7f\x7f"));
        self.members.range(lo..=hi).count()
    }

    pub fn pop_min(&mut self, count: usize) -> Vec<(String, f64)> {
        let mut result = Vec::new();
        for _ in 0..count {
            if let Some(((score, member), _)) =
                self.members.iter().next().map(|(k, v)| (k.clone(), *v))
            {
                self.members.remove(&(score, member.clone()));
                self.scores.remove(&member);
                result.push((member, score.0));
            } else {
                break;
            }
        }
        result
    }

    pub fn pop_max(&mut self, count: usize) -> Vec<(String, f64)> {
        let mut result = Vec::new();
        for _ in 0..count {
            if let Some(((score, member), _)) = self
                .members
                .iter()
                .next_back()
                .map(|(k, v)| (k.clone(), *v))
            {
                self.members.remove(&(score, member.clone()));
                self.scores.remove(&member);
                result.push((member, score.0));
            } else {
                break;
            }
        }
        result
    }
}

fn format_score(s: f64) -> String {
    if s.fract() == 0.0 && s.abs() < 1e17 {
        format!("{:.0}", s)
    } else {
        format!("{}", s)
    }
}

fn score_to_resp(member: &str, score: f64, withscores: bool) -> Vec<RespValue> {
    let mut v = vec![RespValue::BulkString(Some(member.as_bytes().to_vec()))];
    if withscores {
        v.push(RespValue::BulkString(Some(
            format_score(score).into_bytes(),
        )));
    }
    v
}

// --- ZADD ---

#[derive(Clone)]
pub struct ZAddCommand {
    store: DataStore,
}

impl ZAddCommand {
    pub fn new(store: DataStore) -> Self {
        ZAddCommand { store }
    }
}

#[async_trait]
impl CommandHandler for ZAddCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 3 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'zadd' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };

        let mut nx = false;
        let mut xx = false;
        let mut gt = false;
        let mut lt = false;
        let mut ch = false;
        let mut i = 1;

        // Parse flags
        while i < args.len() {
            let maybe_flag = extract_string(&args[i]).map(|s| s.to_uppercase());
            match maybe_flag.as_deref() {
                Some("NX") => {
                    nx = true;
                    i += 1;
                }
                Some("XX") => {
                    xx = true;
                    i += 1;
                }
                Some("GT") => {
                    gt = true;
                    i += 1;
                }
                Some("LT") => {
                    lt = true;
                    i += 1;
                }
                Some("CH") => {
                    ch = true;
                    i += 1;
                }
                _ => break,
            }
        }

        // Remaining args are score member pairs
        if !(args.len() - i).is_multiple_of(2) || args.len() - i == 0 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'zadd' command".to_string(),
            ));
        }

        let mut store = self.store.sorted_sets.write().await;
        let zset = store.entry(key).or_insert_with(SortedSet::new);
        let mut added = 0i64;
        let mut changed = 0i64;

        while i + 1 < args.len() {
            let score: f64 = match extract_string(&args[i]).and_then(|s| s.parse().ok()) {
                Some(v) => v,
                None => {
                    return Ok(RespValue::Error(
                        "ERR value is not a valid float".to_string(),
                    ))
                }
            };
            let member = match extract_string(&args[i + 1]) {
                Some(m) => m,
                None => return Ok(RespValue::Error("ERR invalid member".to_string())),
            };

            let existing_score = zset.score(&member);

            if nx && existing_score.is_some() {
                i += 2;
                continue;
            }
            if xx && existing_score.is_none() {
                i += 2;
                continue;
            }

            let should_update = match existing_score {
                Some(old) => !(gt && score <= old || lt && score >= old),
                None => true,
            };

            if should_update {
                let is_new = zset.insert(member, score);
                if is_new {
                    added += 1;
                } else {
                    changed += 1;
                }
            }
            i += 2;
        }

        if ch {
            Ok(RespValue::Integer(added + changed))
        } else {
            Ok(RespValue::Integer(added))
        }
    }
}

// --- ZREM ---

#[derive(Clone)]
pub struct ZRemCommand {
    store: DataStore,
}

impl ZRemCommand {
    pub fn new(store: DataStore) -> Self {
        ZRemCommand { store }
    }
}

#[async_trait]
impl CommandHandler for ZRemCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'zrem' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let mut store = self.store.sorted_sets.write().await;
        match store.get_mut(&key) {
            Some(zset) => {
                let mut removed = 0i64;
                for arg in args.iter().skip(1) {
                    let member = match extract_string(arg) {
                        Some(m) => m,
                        None => continue,
                    };
                    if zset.remove(&member) {
                        removed += 1;
                    }
                }
                if zset.is_empty() {
                    store.remove(&key);
                }
                Ok(RespValue::Integer(removed))
            }
            None => Ok(RespValue::Integer(0)),
        }
    }
}

// --- ZSCORE ---

#[derive(Clone)]
pub struct ZScoreCommand {
    store: DataStore,
}

impl ZScoreCommand {
    pub fn new(store: DataStore) -> Self {
        ZScoreCommand { store }
    }
}

#[async_trait]
impl CommandHandler for ZScoreCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'zscore' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let member = match extract_string(&args[1]) {
            Some(m) => m,
            None => return Ok(RespValue::Error("ERR invalid member".to_string())),
        };
        let store = self.store.sorted_sets.read().await;
        match store.get(&key).and_then(|z| z.score(&member)) {
            Some(s) => Ok(RespValue::BulkString(Some(format_score(s).into_bytes()))),
            None => Ok(RespValue::BulkString(None)),
        }
    }
}

// --- ZCARD ---

#[derive(Clone)]
pub struct ZCardCommand {
    store: DataStore,
}

impl ZCardCommand {
    pub fn new(store: DataStore) -> Self {
        ZCardCommand { store }
    }
}

#[async_trait]
impl CommandHandler for ZCardCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'zcard' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let store = self.store.sorted_sets.read().await;
        match store.get(&key) {
            Some(z) => Ok(RespValue::Integer(z.len() as i64)),
            None => Ok(RespValue::Integer(0)),
        }
    }
}

// --- ZCOUNT ---

#[derive(Clone)]
pub struct ZCountCommand {
    store: DataStore,
}

impl ZCountCommand {
    pub fn new(store: DataStore) -> Self {
        ZCountCommand { store }
    }
}

fn parse_score_bound(s: &str) -> Option<f64> {
    if s == "-inf" {
        Some(f64::NEG_INFINITY)
    } else if s == "+inf" || s == "inf" {
        Some(f64::INFINITY)
    } else if let Some(stripped) = s.strip_prefix('(') {
        stripped.parse::<f64>().ok().map(|v| v + f64::EPSILON)
    } else {
        s.parse().ok()
    }
}

#[async_trait]
impl CommandHandler for ZCountCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'zcount' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let min = match extract_string(&args[1]).and_then(|s| parse_score_bound(&s)) {
            Some(v) => v,
            None => {
                return Ok(RespValue::Error(
                    "ERR min or max is not a float".to_string(),
                ))
            }
        };
        let max = match extract_string(&args[2]).and_then(|s| parse_score_bound(&s)) {
            Some(v) => v,
            None => {
                return Ok(RespValue::Error(
                    "ERR min or max is not a float".to_string(),
                ))
            }
        };
        let store = self.store.sorted_sets.read().await;
        match store.get(&key) {
            Some(z) => Ok(RespValue::Integer(z.count_in_score_range(min, max) as i64)),
            None => Ok(RespValue::Integer(0)),
        }
    }
}

// --- ZRANGE ---

#[derive(Clone)]
pub struct ZRangeCommand {
    store: DataStore,
}

impl ZRangeCommand {
    pub fn new(store: DataStore) -> Self {
        ZRangeCommand { store }
    }
}

#[async_trait]
impl CommandHandler for ZRangeCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 3 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'zrange' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let start_str = match extract_string(&args[1]) {
            Some(s) => s,
            None => return Ok(RespValue::Error("ERR invalid start".to_string())),
        };
        let stop_str = match extract_string(&args[2]) {
            Some(s) => s,
            None => return Ok(RespValue::Error("ERR invalid stop".to_string())),
        };

        let mut withscores = false;
        let mut rev = false;
        let mut byscore = false;
        let mut limit_offset: Option<usize> = None;
        let mut limit_count: Option<usize> = None;

        let mut i = 3;
        while i < args.len() {
            let flag = match extract_string(&args[i]) {
                Some(f) => f.to_uppercase(),
                None => {
                    i += 1;
                    continue;
                }
            };
            match flag.as_str() {
                "WITHSCORES" => withscores = true,
                "REV" => rev = true,
                "BYSCORE" => byscore = true,
                "LIMIT" => {
                    if i + 2 < args.len() {
                        limit_offset = extract_string(&args[i + 1]).and_then(|s| s.parse().ok());
                        limit_count = extract_string(&args[i + 2]).and_then(|s| s.parse().ok());
                        i += 2;
                    }
                }
                _ => {}
            }
            i += 1;
        }

        let store = self.store.sorted_sets.read().await;
        let zset = match store.get(&key) {
            Some(z) => z,
            None => return Ok(RespValue::Array(Some(vec![]))),
        };

        let items = if byscore {
            let min = parse_score_bound(&start_str).unwrap_or(f64::NEG_INFINITY);
            let max = parse_score_bound(&stop_str).unwrap_or(f64::INFINITY);
            let mut result = if rev {
                let mut r = zset.range_by_score(min, max);
                r.reverse();
                r
            } else {
                zset.range_by_score(min, max)
            };
            if let (Some(offset), Some(count)) = (limit_offset, limit_count) {
                result = result.into_iter().skip(offset).take(count).collect();
            }
            result
        } else {
            let start: i64 = start_str.parse().unwrap_or(0);
            let stop: i64 = stop_str.parse().unwrap_or(-1);
            if rev {
                zset.rev_range_by_index(start, stop)
            } else {
                zset.range_by_index(start, stop)
            }
        };

        let mut result = Vec::new();
        for (member, score) in &items {
            result.extend(score_to_resp(member, *score, withscores));
        }
        Ok(RespValue::Array(Some(result)))
    }
}

// --- ZREVRANGE ---

#[derive(Clone)]
pub struct ZRevRangeCommand {
    store: DataStore,
}

impl ZRevRangeCommand {
    pub fn new(store: DataStore) -> Self {
        ZRevRangeCommand { store }
    }
}

#[async_trait]
impl CommandHandler for ZRevRangeCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 3 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'zrevrange' command".to_string(),
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
        let withscores = args.len() > 3
            && extract_string(&args[3])
                .map(|s| s.to_uppercase() == "WITHSCORES")
                .unwrap_or(false);

        let store = self.store.sorted_sets.read().await;
        match store.get(&key) {
            Some(zset) => {
                let items = zset.rev_range_by_index(start, stop);
                let mut result = Vec::new();
                for (member, score) in &items {
                    result.extend(score_to_resp(member, *score, withscores));
                }
                Ok(RespValue::Array(Some(result)))
            }
            None => Ok(RespValue::Array(Some(vec![]))),
        }
    }
}

// --- ZRANGEBYSCORE ---

#[derive(Clone)]
pub struct ZRangeByScoreCommand {
    store: DataStore,
}

impl ZRangeByScoreCommand {
    pub fn new(store: DataStore) -> Self {
        ZRangeByScoreCommand { store }
    }
}

#[async_trait]
impl CommandHandler for ZRangeByScoreCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 3 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'zrangebyscore' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let min = match extract_string(&args[1]).and_then(|s| parse_score_bound(&s)) {
            Some(v) => v,
            None => {
                return Ok(RespValue::Error(
                    "ERR min or max is not a float".to_string(),
                ))
            }
        };
        let max = match extract_string(&args[2]).and_then(|s| parse_score_bound(&s)) {
            Some(v) => v,
            None => {
                return Ok(RespValue::Error(
                    "ERR min or max is not a float".to_string(),
                ))
            }
        };

        let mut withscores = false;
        let mut limit_offset: Option<usize> = None;
        let mut limit_count: Option<usize> = None;
        let mut i = 3;
        while i < args.len() {
            let flag = match extract_string(&args[i]) {
                Some(f) => f.to_uppercase(),
                None => {
                    i += 1;
                    continue;
                }
            };
            match flag.as_str() {
                "WITHSCORES" => withscores = true,
                "LIMIT" => {
                    if i + 2 < args.len() {
                        limit_offset = extract_string(&args[i + 1]).and_then(|s| s.parse().ok());
                        limit_count = extract_string(&args[i + 2]).and_then(|s| s.parse().ok());
                        i += 2;
                    }
                }
                _ => {}
            }
            i += 1;
        }

        let store = self.store.sorted_sets.read().await;
        match store.get(&key) {
            Some(zset) => {
                let mut items = zset.range_by_score(min, max);
                if let (Some(offset), Some(count)) = (limit_offset, limit_count) {
                    items = items.into_iter().skip(offset).take(count).collect();
                }
                let mut result = Vec::new();
                for (member, score) in &items {
                    result.extend(score_to_resp(member, *score, withscores));
                }
                Ok(RespValue::Array(Some(result)))
            }
            None => Ok(RespValue::Array(Some(vec![]))),
        }
    }
}

// --- ZREVRANGEBYSCORE ---

#[derive(Clone)]
pub struct ZRevRangeByScoreCommand {
    store: DataStore,
}

impl ZRevRangeByScoreCommand {
    pub fn new(store: DataStore) -> Self {
        ZRevRangeByScoreCommand { store }
    }
}

#[async_trait]
impl CommandHandler for ZRevRangeByScoreCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 3 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'zrevrangebyscore' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        // Note: in ZREVRANGEBYSCORE, max comes first, then min
        let max = match extract_string(&args[1]).and_then(|s| parse_score_bound(&s)) {
            Some(v) => v,
            None => {
                return Ok(RespValue::Error(
                    "ERR min or max is not a float".to_string(),
                ))
            }
        };
        let min = match extract_string(&args[2]).and_then(|s| parse_score_bound(&s)) {
            Some(v) => v,
            None => {
                return Ok(RespValue::Error(
                    "ERR min or max is not a float".to_string(),
                ))
            }
        };

        let mut withscores = false;
        let mut limit_offset: Option<usize> = None;
        let mut limit_count: Option<usize> = None;
        let mut i = 3;
        while i < args.len() {
            let flag = match extract_string(&args[i]) {
                Some(f) => f.to_uppercase(),
                None => {
                    i += 1;
                    continue;
                }
            };
            match flag.as_str() {
                "WITHSCORES" => withscores = true,
                "LIMIT" => {
                    if i + 2 < args.len() {
                        limit_offset = extract_string(&args[i + 1]).and_then(|s| s.parse().ok());
                        limit_count = extract_string(&args[i + 2]).and_then(|s| s.parse().ok());
                        i += 2;
                    }
                }
                _ => {}
            }
            i += 1;
        }

        let store = self.store.sorted_sets.read().await;
        match store.get(&key) {
            Some(zset) => {
                let mut items = zset.range_by_score(min, max);
                items.reverse();
                if let (Some(offset), Some(count)) = (limit_offset, limit_count) {
                    items = items.into_iter().skip(offset).take(count).collect();
                }
                let mut result = Vec::new();
                for (member, score) in &items {
                    result.extend(score_to_resp(member, *score, withscores));
                }
                Ok(RespValue::Array(Some(result)))
            }
            None => Ok(RespValue::Array(Some(vec![]))),
        }
    }
}

// --- ZRANK ---

#[derive(Clone)]
pub struct ZRankCommand {
    store: DataStore,
}

impl ZRankCommand {
    pub fn new(store: DataStore) -> Self {
        ZRankCommand { store }
    }
}

#[async_trait]
impl CommandHandler for ZRankCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'zrank' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let member = match extract_string(&args[1]) {
            Some(m) => m,
            None => return Ok(RespValue::Error("ERR invalid member".to_string())),
        };
        let store = self.store.sorted_sets.read().await;
        match store.get(&key).and_then(|z| z.rank(&member)) {
            Some(r) => Ok(RespValue::Integer(r as i64)),
            None => Ok(RespValue::BulkString(None)),
        }
    }
}

// --- ZREVRANK ---

#[derive(Clone)]
pub struct ZRevRankCommand {
    store: DataStore,
}

impl ZRevRankCommand {
    pub fn new(store: DataStore) -> Self {
        ZRevRankCommand { store }
    }
}

#[async_trait]
impl CommandHandler for ZRevRankCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'zrevrank' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let member = match extract_string(&args[1]) {
            Some(m) => m,
            None => return Ok(RespValue::Error("ERR invalid member".to_string())),
        };
        let store = self.store.sorted_sets.read().await;
        match store.get(&key).and_then(|z| z.rev_rank(&member)) {
            Some(r) => Ok(RespValue::Integer(r as i64)),
            None => Ok(RespValue::BulkString(None)),
        }
    }
}

// --- ZINCRBY ---

#[derive(Clone)]
pub struct ZIncrByCommand {
    store: DataStore,
}

impl ZIncrByCommand {
    pub fn new(store: DataStore) -> Self {
        ZIncrByCommand { store }
    }
}

#[async_trait]
impl CommandHandler for ZIncrByCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'zincrby' command".to_string(),
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
        let member = match extract_string(&args[2]) {
            Some(m) => m,
            None => return Ok(RespValue::Error("ERR invalid member".to_string())),
        };
        let mut store = self.store.sorted_sets.write().await;
        let zset = store.entry(key).or_insert_with(SortedSet::new);
        let old_score = zset.score(&member).unwrap_or(0.0);
        let new_score = old_score + increment;
        zset.insert(member, new_score);
        Ok(RespValue::BulkString(Some(
            format_score(new_score).into_bytes(),
        )))
    }
}

// --- ZPOPMIN ---

#[derive(Clone)]
pub struct ZPopMinCommand {
    store: DataStore,
}

impl ZPopMinCommand {
    pub fn new(store: DataStore) -> Self {
        ZPopMinCommand { store }
    }
}

#[async_trait]
impl CommandHandler for ZPopMinCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'zpopmin' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let count: usize = if args.len() > 1 {
            extract_string(&args[1])
                .and_then(|s| s.parse().ok())
                .unwrap_or(1)
        } else {
            1
        };
        let mut store = self.store.sorted_sets.write().await;
        match store.get_mut(&key) {
            Some(zset) => {
                let popped = zset.pop_min(count);
                if zset.is_empty() {
                    store.remove(&key);
                }
                let mut result = Vec::new();
                for (member, score) in popped {
                    result.push(RespValue::BulkString(Some(member.into_bytes())));
                    result.push(RespValue::BulkString(Some(
                        format_score(score).into_bytes(),
                    )));
                }
                Ok(RespValue::Array(Some(result)))
            }
            None => Ok(RespValue::Array(Some(vec![]))),
        }
    }
}

// --- ZPOPMAX ---

#[derive(Clone)]
pub struct ZPopMaxCommand {
    store: DataStore,
}

impl ZPopMaxCommand {
    pub fn new(store: DataStore) -> Self {
        ZPopMaxCommand { store }
    }
}

#[async_trait]
impl CommandHandler for ZPopMaxCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'zpopmax' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let count: usize = if args.len() > 1 {
            extract_string(&args[1])
                .and_then(|s| s.parse().ok())
                .unwrap_or(1)
        } else {
            1
        };
        let mut store = self.store.sorted_sets.write().await;
        match store.get_mut(&key) {
            Some(zset) => {
                let popped = zset.pop_max(count);
                if zset.is_empty() {
                    store.remove(&key);
                }
                let mut result = Vec::new();
                for (member, score) in popped {
                    result.push(RespValue::BulkString(Some(member.into_bytes())));
                    result.push(RespValue::BulkString(Some(
                        format_score(score).into_bytes(),
                    )));
                }
                Ok(RespValue::Array(Some(result)))
            }
            None => Ok(RespValue::Array(Some(vec![]))),
        }
    }
}

// --- ZRANDMEMBER ---

#[derive(Clone)]
pub struct ZRandMemberCommand {
    store: DataStore,
}

impl ZRandMemberCommand {
    pub fn new(store: DataStore) -> Self {
        ZRandMemberCommand { store }
    }
}

#[async_trait]
impl CommandHandler for ZRandMemberCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'zrandmember' command".to_string(),
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
        let withscores = args.len() >= 3
            && extract_string(&args[2])
                .map(|s| s.to_uppercase() == "WITHSCORES")
                .unwrap_or(false);

        let store = self.store.sorted_sets.read().await;
        match store.get(&key) {
            Some(zset) if !zset.is_empty() => {
                use rand::seq::SliceRandom;
                use rand::Rng;
                let members: Vec<(&String, &f64)> = zset.scores.iter().collect();

                match count {
                    None => {
                        let mut rng = rand::thread_rng();
                        let idx = rng.gen_range(0..members.len());
                        let (m, _) = members[idx];
                        Ok(RespValue::BulkString(Some(m.as_bytes().to_vec())))
                    }
                    Some(n) if n >= 0 => {
                        let mut rng = rand::thread_rng();
                        let n = (n as usize).min(members.len());
                        let mut indices: Vec<usize> = (0..members.len()).collect();
                        indices.shuffle(&mut rng);
                        indices.truncate(n);
                        let mut result = Vec::new();
                        for idx in indices {
                            let (m, s) = members[idx];
                            result.push(RespValue::BulkString(Some(m.as_bytes().to_vec())));
                            if withscores {
                                result.push(RespValue::BulkString(Some(
                                    format_score(*s).into_bytes(),
                                )));
                            }
                        }
                        Ok(RespValue::Array(Some(result)))
                    }
                    Some(n) => {
                        let mut rng = rand::thread_rng();
                        let abs_n = (-n) as usize;
                        let mut result = Vec::new();
                        for _ in 0..abs_n {
                            let idx = rng.gen_range(0..members.len());
                            let (m, s) = members[idx];
                            result.push(RespValue::BulkString(Some(m.as_bytes().to_vec())));
                            if withscores {
                                result.push(RespValue::BulkString(Some(
                                    format_score(*s).into_bytes(),
                                )));
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

// --- ZMSCORE ---

#[derive(Clone)]
pub struct ZMScoreCommand {
    store: DataStore,
}

impl ZMScoreCommand {
    pub fn new(store: DataStore) -> Self {
        ZMScoreCommand { store }
    }
}

#[async_trait]
impl CommandHandler for ZMScoreCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'zmscore' command".to_string(),
            ));
        }
        let key = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let store = self.store.sorted_sets.read().await;
        let zset = store.get(&key);
        let results: Vec<RespValue> = args[1..]
            .iter()
            .map(|arg| {
                let member = match extract_string(arg) {
                    Some(m) => m,
                    None => return RespValue::BulkString(None),
                };
                match zset.and_then(|z| z.score(&member)) {
                    Some(s) => RespValue::BulkString(Some(format_score(s).into_bytes())),
                    None => RespValue::BulkString(None),
                }
            })
            .collect();
        Ok(RespValue::Array(Some(results)))
    }
}

// --- ZUNIONSTORE ---

#[derive(Clone)]
pub struct ZUnionStoreCommand {
    store: DataStore,
}

impl ZUnionStoreCommand {
    pub fn new(store: DataStore) -> Self {
        ZUnionStoreCommand { store }
    }
}

#[async_trait]
impl CommandHandler for ZUnionStoreCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 3 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'zunionstore' command".to_string(),
            ));
        }
        let dest = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let numkeys: usize = match extract_string(&args[1]).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => {
                return Ok(RespValue::Error(
                    "ERR value is not an integer or out of range".to_string(),
                ))
            }
        };
        if args.len() < 2 + numkeys {
            return Ok(RespValue::Error("ERR syntax error".to_string()));
        }

        let mut store = self.store.sorted_sets.write().await;
        let mut result = SortedSet::new();

        for i in 0..numkeys {
            let key = match extract_string(&args[2 + i]) {
                Some(k) => k,
                None => continue,
            };
            if let Some(zset) = store.get(&key) {
                for (member, &score) in &zset.scores {
                    let existing = result.score(member).unwrap_or(0.0);
                    result.insert(member.clone(), existing + score);
                }
            }
        }

        let len = result.len() as i64;
        store.insert(dest, result);
        Ok(RespValue::Integer(len))
    }
}

// --- ZINTERSTORE ---

#[derive(Clone)]
pub struct ZInterStoreCommand {
    store: DataStore,
}

impl ZInterStoreCommand {
    pub fn new(store: DataStore) -> Self {
        ZInterStoreCommand { store }
    }
}

#[async_trait]
impl CommandHandler for ZInterStoreCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 3 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'zinterstore' command".to_string(),
            ));
        }
        let dest = match extract_string(&args[0]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let numkeys: usize = match extract_string(&args[1]).and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => {
                return Ok(RespValue::Error(
                    "ERR value is not an integer or out of range".to_string(),
                ))
            }
        };
        if numkeys == 0 || args.len() < 2 + numkeys {
            return Ok(RespValue::Error("ERR syntax error".to_string()));
        }

        let mut store = self.store.sorted_sets.write().await;

        // Start with the first set's members
        let first_key = match extract_string(&args[2]) {
            Some(k) => k,
            None => return Ok(RespValue::Error("ERR invalid key".to_string())),
        };
        let first_set = match store.get(&first_key) {
            Some(z) => z.scores.clone(),
            None => {
                store.insert(dest, SortedSet::new());
                return Ok(RespValue::Integer(0));
            }
        };

        let mut result_scores: HashMap<String, f64> = first_set;

        for i in 1..numkeys {
            let key = match extract_string(&args[2 + i]) {
                Some(k) => k,
                None => continue,
            };
            match store.get(&key) {
                Some(zset) => {
                    result_scores.retain(|member, score| {
                        if let Some(&other_score) = zset.scores.get(member) {
                            *score += other_score;
                            true
                        } else {
                            false
                        }
                    });
                }
                None => {
                    result_scores.clear();
                    break;
                }
            }
        }

        let mut result = SortedSet::new();
        for (member, score) in result_scores {
            result.insert(member, score);
        }
        let len = result.len() as i64;
        store.insert(dest, result);
        Ok(RespValue::Integer(len))
    }
}

// --- ZSCAN ---

#[derive(Clone)]
pub struct ZScanCommand {
    store: DataStore,
}

impl ZScanCommand {
    pub fn new(store: DataStore) -> Self {
        ZScanCommand { store }
    }
}

#[async_trait]
impl CommandHandler for ZScanCommand {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'zscan' command".to_string(),
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
            if flag == "COUNT" {
                i += 1;
                count = extract_string(&args[i])
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(10);
            }
            i += 1;
        }

        let store = self.store.sorted_sets.read().await;
        match store.get(&key) {
            Some(zset) => {
                let members: Vec<(&String, &f64)> = {
                    let mut v: Vec<_> = zset.scores.iter().collect();
                    v.sort_by_key(|(k, _)| k.as_str());
                    v
                };
                let start = cursor;
                let end = (start + count).min(members.len());
                let next_cursor = if end >= members.len() { 0 } else { end };

                let mut items = Vec::new();
                if start < members.len() {
                    for (member, score) in &members[start..end] {
                        items.push(RespValue::BulkString(Some(member.as_bytes().to_vec())));
                        items.push(RespValue::BulkString(Some(
                            format_score(**score).into_bytes(),
                        )));
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
