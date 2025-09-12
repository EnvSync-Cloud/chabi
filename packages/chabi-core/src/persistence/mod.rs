//! Persistence module for handling disk I/O and data persistence

use std::{
    collections::HashMap,
    sync::Arc,
    time::Duration,
};
use dashmap::DashMap;
use crate::types::{Value, SerializedValue};

/// Core KV store using DashMap for thread safety
pub type Store = Arc<DashMap<String, Value>>;

/// Create a new empty key-value store
pub fn new_store() -> Store {
    Arc::new(DashMap::new())
}

/// Persistence options for configuring how data is saved
pub struct PersistenceOptions {
    pub path: String,
    pub auto_persist: bool,
    pub persist_interval_ms: Option<u64>, // If set, automatically persist at this interval
}

/// Persistence manager for handling disk I/O
#[derive(Debug)]
pub struct PersistenceManager {
    store: Store,
    persistence_path: Option<String>,
    pub auto_persist: bool,
    dirty_keys: DashMap<String, bool>, // Track modified keys for efficient persistence
}

impl PersistenceManager {
    /// Create a new persistence manager with optional persistence
    pub fn new(store: Store, options: Option<PersistenceOptions>) -> Arc<Self> {
        let manager = match options {
            Some(opts) => {
                let mgr = Arc::new(Self {
                    store,
                    persistence_path: Some(opts.path),
                    auto_persist: opts.auto_persist,
                    dirty_keys: DashMap::new(),
                });
                
                // Create persistence directory if it doesn't exist
                if let Some(path) = &mgr.persistence_path {
                    std::fs::create_dir_all(std::path::Path::new(path).parent().unwrap_or_else(|| std::path::Path::new("."))).ok();
                }
                
                // Load existing data if available
                mgr.load().ok();
                
                // Setup background persistence if requested
                if let Some(interval_ms) = opts.persist_interval_ms {
                    let mgr_clone = mgr.clone();
                    tokio::spawn(async move {
                        let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
                        loop {
                            interval.tick().await;
                            if mgr_clone.auto_persist {
                                mgr_clone.persist().ok();
                            }
                        }
                    });
                }
                
                mgr
            },
            None => {
                Arc::new(Self {
                    store,
                    persistence_path: None,
                    auto_persist: false,
                    dirty_keys: DashMap::new(),
                })
            }
        };
        
        manager
    }
    
    /// Mark a key as dirty (modified)
    pub fn mark_dirty(&self, key: &str) {
        if self.auto_persist {
            self.dirty_keys.insert(key.to_string(), true);
        }
    }
    
    /// Persist the entire store to disk
    pub fn persist(&self) -> anyhow::Result<()> {
        if let Some(path) = &self.persistence_path {
            // Create a serializable representation of the store
            let mut data = HashMap::new();
            
            for item in self.store.iter() {
                let key = item.key().clone();
                let value = SerializedValue::from(&*item.value());
                data.insert(key, value);
            }
            
            // Write to a temporary file first for atomic updates
            let temp_path = format!("{}.tmp", path);
            let serialized = bincode::serialize(&data)?;
            
            std::fs::write(&temp_path, serialized)?;
            std::fs::rename(temp_path, path)?;
            
            // Clear dirty flags
            self.dirty_keys.clear();
        }
        
        Ok(())
    }
    
    /// Load data from disk into the store
    pub fn load(&self) -> anyhow::Result<()> {
        if let Some(path) = &self.persistence_path {
            // Check if file exists
            if std::path::Path::new(path).exists() {
                let data = std::fs::read(path)?;
                let deserialized: HashMap<String, SerializedValue> = bincode::deserialize(&data)?;
                
                // Populate the store
                for (key, value) in deserialized {
                    self.store.insert(key, Value::from(&value));
                }
            }
        }
        
        Ok(())
    }
    
    /// Get the underlying store
    pub fn get_store(&self) -> Store {
        self.store.clone()
    }
}
