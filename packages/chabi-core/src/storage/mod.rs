//! Storage implementation for Chabi

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// The main storage type for Chabi
pub type Store = Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>;

/// Creates a new empty storage instance
pub fn create_store() -> Store {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Storage manager that handles data persistence and recovery
pub struct StorageManager {
    store: Store,
}

impl StorageManager {
    pub fn new() -> Self {
        Self {
            store: create_store(),
        }
    }

    /// Get the store instance
    pub fn get_store(&self) -> Store {
        Arc::clone(&self.store)
    }

    /// Clear all data from the store
    pub async fn clear(&self) {
        let mut store = self.store.write().await;
        store.clear();
    }

    /// Get the number of keys in the store
    pub async fn len(&self) -> usize {
        let store = self.store.read().await;
        store.len()
    }

    /// Check if the store is empty
    pub async fn is_empty(&self) -> bool {
        let store = self.store.read().await;
        store.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_storage_manager() {
        let manager = StorageManager::new();
        let store = manager.get_store();

        // Test empty store
        assert!(manager.is_empty().await);
        assert_eq!(manager.len().await, 0);

        // Add some data
        {
            let mut storage = store.write().await;
            storage.insert(b"key1".to_vec(), b"value1".to_vec());
            storage.insert(b"key2".to_vec(), b"value2".to_vec());
        }

        // Test non-empty store
        assert!(!manager.is_empty().await);
        assert_eq!(manager.len().await, 2);

        // Test clear
        manager.clear().await;
        assert!(manager.is_empty().await);
    }
}