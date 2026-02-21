//! Unified data store for Chabi.

use crate::commands::sorted_set::SortedSet;
use crate::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub type StringStore = Arc<RwLock<HashMap<String, String>>>;
pub type ListStore = Arc<RwLock<HashMap<String, Vec<String>>>>;
pub type SetStore = Arc<RwLock<HashMap<String, HashSet<String>>>>;
pub type HashStore = Arc<RwLock<HashMap<String, HashMap<String, String>>>>;
pub type SortedSetStore = Arc<RwLock<HashMap<String, SortedSet>>>;
pub type HllStore = Arc<RwLock<HashMap<String, Vec<u8>>>>;
pub type ExpirationStore = Arc<RwLock<HashMap<String, Instant>>>;

/// Unified data store holding all Redis-like data structures.
/// Each field is `Arc<RwLock<...>>`, so `Clone` is cheap (atomic ref-count bumps).
#[derive(Clone)]
pub struct DataStore {
    pub strings: StringStore,
    pub lists: ListStore,
    pub sets: SetStore,
    pub hashes: HashStore,
    pub sorted_sets: SortedSetStore,
    pub hll: HllStore,
    pub expirations: ExpirationStore,
}

impl DataStore {
    pub fn new() -> Self {
        DataStore {
            strings: Arc::new(RwLock::new(HashMap::new())),
            lists: Arc::new(RwLock::new(HashMap::new())),
            sets: Arc::new(RwLock::new(HashMap::new())),
            hashes: Arc::new(RwLock::new(HashMap::new())),
            sorted_sets: Arc::new(RwLock::new(HashMap::new())),
            hll: Arc::new(RwLock::new(HashMap::new())),
            expirations: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Build a point-in-time snapshot of all stores (including sorted_sets and hll).
    pub async fn build_snapshot(&self) -> Snapshot {
        let strings_guard = self.strings.read().await;
        let lists_guard = self.lists.read().await;
        let sets_guard = self.sets.read().await;
        let hashes_guard = self.hashes.read().await;
        let sorted_sets_guard = self.sorted_sets.read().await;
        let hll_guard = self.hll.read().await;
        let expirations_guard = self.expirations.read().await;

        let now_instant = Instant::now();
        let now_system = SystemTime::now();
        let mut exps: HashMap<String, u64> = HashMap::new();
        for (k, inst) in expirations_guard.iter() {
            let delta = inst.saturating_duration_since(now_instant);
            let ts = now_system
                .checked_add(delta)
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or_else(|| UNIX_EPOCH.elapsed().map(|d| d.as_secs()).unwrap_or(0));
            exps.insert(k.clone(), ts);
        }

        Snapshot {
            strings: strings_guard.clone(),
            lists: lists_guard.clone(),
            sets: sets_guard.clone(),
            hashes: hashes_guard.clone(),
            sorted_sets: sorted_sets_guard.clone(),
            hll: hll_guard.clone(),
            expirations_epoch_secs: exps,
        }
    }

    /// Restore all stores from a snapshot.
    pub async fn restore_from_snapshot(&self, snapshot: Snapshot) {
        *self.strings.write().await = snapshot.strings;
        *self.lists.write().await = snapshot.lists;
        *self.sets.write().await = snapshot.sets;
        *self.hashes.write().await = snapshot.hashes;
        *self.sorted_sets.write().await = snapshot.sorted_sets;
        *self.hll.write().await = snapshot.hll;

        let mut exp = self.expirations.write().await;
        exp.clear();
        let now_system = SystemTime::now();
        let now_instant = Instant::now();
        for (k, ts) in snapshot.expirations_epoch_secs {
            let target_time = UNIX_EPOCH + Duration::from_secs(ts);
            if let Ok(delta) = target_time.duration_since(now_system) {
                if !delta.is_zero() {
                    exp.insert(k, now_instant + delta);
                }
            }
        }
    }
}

impl Default for DataStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Serializable snapshot of all data stores.
#[derive(Serialize, Deserialize)]
pub struct Snapshot {
    pub strings: HashMap<String, String>,
    pub lists: HashMap<String, Vec<String>>,
    pub sets: HashMap<String, HashSet<String>>,
    pub hashes: HashMap<String, HashMap<String, String>>,
    pub sorted_sets: HashMap<String, SortedSet>,
    pub hll: HashMap<String, Vec<u8>>,
    pub expirations_epoch_secs: HashMap<String, u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_datastore_new_empty() {
        let store = DataStore::new();
        assert!(store.strings.read().await.is_empty());
        assert!(store.lists.read().await.is_empty());
        assert!(store.sets.read().await.is_empty());
        assert!(store.hashes.read().await.is_empty());
        assert!(store.sorted_sets.read().await.is_empty());
        assert!(store.hll.read().await.is_empty());
        assert!(store.expirations.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_build_and_restore_snapshot() {
        let store = DataStore::new();
        store
            .strings
            .write()
            .await
            .insert("k1".to_string(), "v1".to_string());
        store
            .lists
            .write()
            .await
            .insert("l1".to_string(), vec!["a".to_string()]);

        let snapshot = store.build_snapshot().await;
        assert_eq!(snapshot.strings.len(), 1);
        assert_eq!(snapshot.lists.len(), 1);

        let store2 = DataStore::new();
        store2.restore_from_snapshot(snapshot).await;
        assert_eq!(store2.strings.read().await.get("k1").unwrap(), "v1");
        assert_eq!(store2.lists.read().await.get("l1").unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_default_impl() {
        let store = DataStore::default();
        assert!(store.strings.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_snapshot_with_expirations() {
        let store = DataStore::new();
        store
            .strings
            .write()
            .await
            .insert("k1".to_string(), "v1".to_string());
        // Set an expiration 1 hour from now
        let deadline = Instant::now() + Duration::from_secs(3600);
        store
            .expirations
            .write()
            .await
            .insert("k1".to_string(), deadline);

        let snapshot = store.build_snapshot().await;
        assert_eq!(snapshot.strings.len(), 1);
        assert_eq!(snapshot.expirations_epoch_secs.len(), 1);
        // The epoch timestamp should be roughly now + 3600
        let ts = snapshot.expirations_epoch_secs.get("k1").unwrap();
        let now_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(*ts > now_epoch + 3500);
        assert!(*ts < now_epoch + 3700);
    }

    #[tokio::test]
    async fn test_restore_snapshot_with_expirations() {
        let store = DataStore::new();
        store
            .strings
            .write()
            .await
            .insert("k1".to_string(), "v1".to_string());
        let deadline = Instant::now() + Duration::from_secs(3600);
        store
            .expirations
            .write()
            .await
            .insert("k1".to_string(), deadline);

        let snapshot = store.build_snapshot().await;

        let store2 = DataStore::new();
        store2.restore_from_snapshot(snapshot).await;
        assert_eq!(store2.strings.read().await.get("k1").unwrap(), "v1");
        assert!(store2.expirations.read().await.contains_key("k1"));
    }

    #[tokio::test]
    async fn test_snapshot_with_sorted_sets_and_hll() {
        use crate::commands::sorted_set::SortedSet;
        let store = DataStore::new();
        let mut zset = SortedSet::new();
        zset.insert("member1".to_string(), 1.0);
        zset.insert("member2".to_string(), 2.0);
        store
            .sorted_sets
            .write()
            .await
            .insert("zkey".to_string(), zset);
        store
            .hll
            .write()
            .await
            .insert("hll1".to_string(), vec![1, 2, 3, 4, 5, 6, 7, 8]);

        let snapshot = store.build_snapshot().await;
        assert_eq!(snapshot.sorted_sets.len(), 1);
        assert_eq!(snapshot.hll.len(), 1);

        let store2 = DataStore::new();
        store2.restore_from_snapshot(snapshot).await;
        let ss = store2.sorted_sets.read().await;
        let zset = ss.get("zkey").unwrap();
        assert_eq!(zset.len(), 2);
        assert_eq!(zset.score("member1"), Some(1.0));
    }

    #[tokio::test]
    async fn test_snapshot_all_types() {
        let store = DataStore::new();
        store
            .strings
            .write()
            .await
            .insert("s1".to_string(), "v1".to_string());
        store
            .lists
            .write()
            .await
            .insert("l1".to_string(), vec!["a".to_string()]);
        store.sets.write().await.insert(
            "set1".to_string(),
            std::collections::HashSet::from(["m1".to_string()]),
        );
        store.hashes.write().await.insert(
            "h1".to_string(),
            std::collections::HashMap::from([("f1".to_string(), "v1".to_string())]),
        );

        let snapshot = store.build_snapshot().await;
        assert_eq!(snapshot.strings.len(), 1);
        assert_eq!(snapshot.lists.len(), 1);
        assert_eq!(snapshot.sets.len(), 1);
        assert_eq!(snapshot.hashes.len(), 1);

        let store2 = DataStore::new();
        store2.restore_from_snapshot(snapshot).await;
        assert_eq!(store2.strings.read().await.len(), 1);
        assert_eq!(store2.lists.read().await.len(), 1);
        assert_eq!(store2.sets.read().await.len(), 1);
        assert_eq!(store2.hashes.read().await.len(), 1);
    }

    #[tokio::test]
    async fn test_clone_is_cheap() {
        let store = DataStore::new();
        store
            .strings
            .write()
            .await
            .insert("k".to_string(), "v".to_string());
        let store2 = store.clone();
        // Both clones share the same underlying data
        assert_eq!(store2.strings.read().await.get("k").unwrap(), "v");
    }
}
