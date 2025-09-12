//! Set command handlers (SADD, SMEMBERS, SISMEMBER, etc.)

use std::collections::HashSet;
use std::time::Instant;
use crate::persistence::{Store, PersistenceManager};
use crate::protocol::{array_response, integer_response};
use crate::types::Value;

/// Handler for SADD command
pub fn handle_sadd(tokens: &[String], store: Store, persistence: &PersistenceManager) -> String {
    if tokens.len() < 3 {
        return "-ERR wrong number of arguments for 'sadd' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    let mut elements_added = 0;
    
    // Get existing set or create new one
    let mut set = match store.get(key) {
        Some(value) => {
            match &*value {
                Value::Set { data, .. } => data.clone(),
                _ => {
                    return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string();
                }
            }
        },
        None => HashSet::new(),
    };
    
    // Add elements to the set
    for i in 2..tokens.len() {
        if set.insert(tokens[i].clone()) {
            elements_added += 1;
        }
    }
    
    // Store the updated set
    store.insert(key.clone(), Value::Set {
        data: set,
        expires_at: None,
    });
    
    // Mark key as modified for persistence
    persistence.mark_dirty(key);
    
    // Return the number of elements added to the set
    integer_response(elements_added)
}

/// Handler for SMEMBERS command
pub fn handle_smembers(tokens: &[String], store: Store) -> String {
    if tokens.len() != 2 {
        return "-ERR wrong number of arguments for 'smembers' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    
    // Get the set
    match store.get(key) {
        Some(value) => {
            match &*value {
                Value::Set { data, expires_at } => {
                    // Check if the key is expired
                    if let Some(expiry) = expires_at {
                        if Instant::now() > *expiry {
                            // Expired key, remove it and return empty array
                            drop(value);
                            store.remove(key);
                            return "*0\r\n".to_string();
                        }
                    }
                    
                    // Convert HashSet to Vec for response
                    let members: Vec<String> = data.iter().cloned().collect();
                    array_response(&members)
                },
                _ => {
                    "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string()
                }
            }
        },
        None => "*0\r\n".to_string() // Empty array for non-existent keys
    }
}

/// Handler for SISMEMBER command
pub fn handle_sismember(tokens: &[String], store: Store) -> String {
    if tokens.len() != 3 {
        return "-ERR wrong number of arguments for 'sismember' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    let member = &tokens[2];
    
    // Check if member exists in the set
    match store.get(key) {
        Some(value) => {
            match &*value {
                Value::Set { data, expires_at } => {
                    // Check if the key is expired
                    if let Some(expiry) = expires_at {
                        if Instant::now() > *expiry {
                            // Expired key, remove it and return 0
                            drop(value);
                            store.remove(key);
                            return ":0\r\n".to_string();
                        }
                    }
                    
                    if data.contains(member) {
                        ":1\r\n".to_string() // Member exists
                    } else {
                        ":0\r\n".to_string() // Member does not exist
                    }
                },
                _ => {
                    "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string()
                }
            }
        },
        None => ":0\r\n".to_string() // Return 0 for non-existent keys
    }
}

/// Handler for SCARD command
pub fn handle_scard(tokens: &[String], store: Store) -> String {
    if tokens.len() != 2 {
        return "-ERR wrong number of arguments for 'scard' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    
    // Get the set cardinality (size)
    match store.get(key) {
        Some(value) => {
            match &*value {
                Value::Set { data, expires_at } => {
                    // Check if the key is expired
                    if let Some(expiry) = expires_at {
                        if Instant::now() > *expiry {
                            // Expired key, remove it and return 0
                            drop(value);
                            store.remove(key);
                            return ":0\r\n".to_string();
                        }
                    }
                    
                    integer_response(data.len() as i64)
                },
                _ => {
                    "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string()
                }
            }
        },
        None => ":0\r\n".to_string() // Return 0 for non-existent keys
    }
}

/// Handler for SREM command
pub fn handle_srem(tokens: &[String], store: Store, persistence: &PersistenceManager) -> String {
    if tokens.len() < 3 {
        return "-ERR wrong number of arguments for 'srem' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    
    // Get the set with mutable access
    if let Some(mut value_ref) = store.get_mut(key) {
        match &mut *value_ref {
            Value::Set { data, expires_at } => {
                // Check if the key is expired
                if let Some(expiry) = expires_at {
                    if Instant::now() > *expiry {
                        // Expired key, remove it and return 0
                        drop(value_ref);
                        store.remove(key);
                        return ":0\r\n".to_string();
                    }
                }
                
                let mut removed_count = 0;
                
                // Remove members from the set
                for i in 2..tokens.len() {
                    if data.remove(&tokens[i]) {
                        removed_count += 1;
                    }
                }
                
                // If the set is now empty, remove the key
                if data.is_empty() {
                    drop(value_ref); // Release the reference before removal
                    store.remove(key);
                }
                
                // Mark key as modified for persistence
                if removed_count > 0 {
                    persistence.mark_dirty(key);
                }
                
                integer_response(removed_count)
            },
            _ => {
                "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string()
            }
        }
    } else {
        ":0\r\n".to_string() // Return 0 for non-existent keys
    }
}
