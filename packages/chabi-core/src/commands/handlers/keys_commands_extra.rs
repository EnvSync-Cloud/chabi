//! Additional key commands handlers (EXPIRE, RENAME, TYPE, etc.)

use std::time::{Duration, Instant};
use crate::persistence::{Store, PersistenceManager};
use crate::protocol::{simple_string, bulk_string};
use crate::types::Value;

/// Handler for EXPIRE command
pub fn handle_expire(tokens: &[String], store: Store, persistence: &PersistenceManager) -> String {
    if tokens.len() != 3 {
        return "-ERR wrong number of arguments for 'expire' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    let seconds = match tokens[2].parse::<u64>() {
        Ok(s) => s,
        Err(_) => return "-ERR value is not an integer or out of range\r\n".to_string(),
    };
    
    // Check if the key exists
    if let Some(value) = store.get(key) {
        match &*value {
            Value::String { data, .. } => {
                // Set with expiry time
                let expires_at = Some(Instant::now() + Duration::from_secs(seconds));
                store.insert(key.clone(), Value::String {
                    data: data.clone(),
                    expires_at,
                });
                
                // Mark key as modified for persistence
                persistence.mark_dirty(key);
                
                ":1\r\n".to_string()
            },
            Value::Hash { .. } => {
                // TODO: Apply expiry to the entire hash
                // This would require modifying the Value enum to support whole-hash expiry
                // For now, we'll just mark the key as modified
                
                // Mark key as modified for persistence
                persistence.mark_dirty(key);
                
                ":1\r\n".to_string()
            },
            Value::List { data, .. } => {
                // Set with expiry time
                let expires_at = Some(Instant::now() + Duration::from_secs(seconds));
                store.insert(key.clone(), Value::List {
                    data: data.clone(),
                    expires_at,
                });
                
                // Mark key as modified for persistence
                persistence.mark_dirty(key);
                
                ":1\r\n".to_string()
            },
            Value::Set { data, .. } => {
                // Set with expiry time
                let expires_at = Some(Instant::now() + Duration::from_secs(seconds));
                store.insert(key.clone(), Value::Set {
                    data: data.clone(),
                    expires_at,
                });
                
                // Mark key as modified for persistence
                persistence.mark_dirty(key);
                
                ":1\r\n".to_string()
            },
            Value::SortedSet { data, .. } => {
                // Set with expiry time
                let expires_at = Some(Instant::now() + Duration::from_secs(seconds));
                store.insert(key.clone(), Value::SortedSet {
                    data: data.clone(),
                    expires_at,
                });
                
                // Mark key as modified for persistence
                persistence.mark_dirty(key);
                
                ":1\r\n".to_string()
            },
        }
    } else {
        // Key doesn't exist
        ":0\r\n".to_string()
    }
}

/// Handler for RENAME command
pub fn handle_rename(tokens: &[String], store: Store, persistence: &PersistenceManager) -> String {
    if tokens.len() != 3 {
        return "-ERR wrong number of arguments for 'rename' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    let new_key = &tokens[2];
    
    if key == new_key {
        return "-ERR source and destination objects are the same\r\n".to_string();
    }
    
    // Check if the key exists
    if let Some(value_ref) = store.get(key) {
        // Clone the value
        let value_to_move = (*value_ref).clone();
        
        // Release the reference before modifying the map
        drop(value_ref);
        
        // Insert the value with the new key
        store.insert(new_key.clone(), value_to_move);
        
        // Remove the old key
        store.remove(key);
        
        // Mark keys as modified for persistence
        persistence.mark_dirty(key);
        persistence.mark_dirty(new_key);
        
        simple_string("OK")
    } else {
        // Key doesn't exist
        "-ERR no such key\r\n".to_string()
    }
}

/// Handler for TYPE command
pub fn handle_type(tokens: &[String], store: Store) -> String {
    if tokens.len() != 2 {
        return "-ERR wrong number of arguments for 'type' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    
    // Check if the key exists
    if let Some(value) = store.get(key) {
        match &*value {
            Value::String { expires_at, .. } => {
                // Check if the key is expired
                if let Some(expiry) = expires_at {
                    if Instant::now() > *expiry {
                        // Expired key, remove it and return none
                        drop(value);
                        store.remove(key);
                        return bulk_string("none");
                    }
                }
                bulk_string("string")
            },
            Value::Hash { .. } => bulk_string("hash"),
            Value::List { expires_at, .. } => {
                // Check if the key is expired
                if let Some(expiry) = expires_at {
                    if Instant::now() > *expiry {
                        // Expired key, remove it and return none
                        drop(value);
                        store.remove(key);
                        return bulk_string("none");
                    }
                }
                bulk_string("list")
            },
            Value::Set { expires_at, .. } => {
                // Check if the key is expired
                if let Some(expiry) = expires_at {
                    if Instant::now() > *expiry {
                        // Expired key, remove it and return none
                        drop(value);
                        store.remove(key);
                        return bulk_string("none");
                    }
                }
                bulk_string("set")
            },
            Value::SortedSet { expires_at, .. } => {
                // Check if the key is expired
                if let Some(expiry) = expires_at {
                    if Instant::now() > *expiry {
                        // Expired key, remove it and return none
                        drop(value);
                        store.remove(key);
                        return bulk_string("none");
                    }
                }
                bulk_string("zset")
            },
        }
    } else {
        // Key doesn't exist
        bulk_string("none")
    }
}
