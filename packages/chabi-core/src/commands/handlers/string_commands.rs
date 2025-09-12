//! String command handlers (SET, GET, etc.)

use std::time::{Duration, Instant};
use crate::persistence::{Store, PersistenceManager};
use crate::protocol::bulk_string;
use crate::types::Value;

/// Handler for SET command
pub fn handle_set(tokens: &[String], store: Store, persistence: &PersistenceManager) -> String {
    if tokens.len() < 3 {
        return "-ERR wrong number of arguments for 'set' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    let value = &tokens[2];
    
    // Check for EX option (expiry in seconds)
    let mut expires_at = None;
    if tokens.len() > 3 && tokens[3].to_uppercase() == "EX" && tokens.len() > 4 {
        if let Ok(secs) = tokens[4].parse::<u64>() {
            expires_at = Some(Instant::now() + Duration::from_secs(secs));
        }
    }
    
    store.insert(key.clone(), Value::String {
        data: value.clone(),
        expires_at,
    });
    
    // Mark key as modified for persistence
    persistence.mark_dirty(key);
    
    // Auto-persist if enabled
    if persistence.auto_persist {
        persistence.persist().ok();
    }
    
    "+OK\r\n".to_string()
}

/// Handler for SETEX command
pub fn handle_setex(tokens: &[String], store: Store, persistence: &PersistenceManager) -> String {
    if tokens.len() < 4 {
        return "-ERR wrong number of arguments for 'setex' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    let seconds = match tokens[2].parse::<u64>() {
        Ok(s) => s,
        Err(_) => return "-ERR value is not an integer or out of range\r\n".to_string(),
    };
    let value = &tokens[3];
    
    // Set with expiry time
    let expires_at = Some(Instant::now() + Duration::from_secs(seconds));
    store.insert(key.clone(), Value::String {
        data: value.clone(),
        expires_at,
    });
    
    // Mark key as modified for persistence
    persistence.mark_dirty(key);
    
    // Auto-persist if enabled
    if persistence.auto_persist {
        persistence.persist().ok();
    }
    
    "+OK\r\n".to_string()
}

/// Handler for GET command
pub fn handle_get(tokens: &[String], store: Store) -> String {
    if tokens.len() < 2 {
        return "-ERR wrong number of arguments for 'get' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    
    if let Some(value) = store.get(key) {
        match &*value {
            Value::String { data, expires_at } => {
                // Check if the key is expired
                if let Some(expiry) = expires_at {
                    if Instant::now() > *expiry {
                        // Expired key, remove it and return nil
                        drop(value); // Release the reference before removal
                        store.remove(key);
                        return "$-1\r\n".to_string();
                    }
                }
                bulk_string(data)
            },
            Value::Hash { .. } => {
                "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string()
            },
            Value::List { .. } | Value::Set { .. } | Value::SortedSet { .. } => {
                "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string()
            }
        }
    } else {
        "$-1\r\n".to_string() // Redis nil response
    }
}

/// Handler for TTL command
pub fn handle_ttl(tokens: &[String], store: Store) -> String {
    if tokens.len() < 2 {
        return "-ERR wrong number of arguments for 'ttl' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    
    if let Some(value) = store.get(key) {
        match &*value {
            Value::String { expires_at, .. } => {
                if let Some(expiry) = expires_at {
                    let now = Instant::now();
                    if now > *expiry {
                        // Already expired
                        return ":-2\r\n".to_string();
                    }
                    let secs = expiry.duration_since(now).as_secs();
                    return format!(":{}\r\n", secs);
                } else {
                    // No expiration
                    return ":-1\r\n".to_string();
                }
            },
            Value::Hash { field_expiry, .. } => {
                if field_expiry.is_empty() {
                    // No expiration on any field
                    return ":-1\r\n".to_string();
                }
                
                // Find the soonest expiry among all fields
                let now = Instant::now();
                let mut min_ttl = None;
                
                for expiry in field_expiry.values() {
                    if now < *expiry {
                        let ttl = expiry.duration_since(now).as_secs();
                        min_ttl = Some(min_ttl.map_or(ttl, |current| std::cmp::min(current, ttl)));
                    }
                }
                
                match min_ttl {
                    Some(ttl) => format!(":{}\r\n", ttl),
                    None => ":-2\r\n".to_string(), // All fields expired
                }
            },
            Value::List { expires_at, .. } | Value::Set { expires_at, .. } | Value::SortedSet { expires_at, .. } => {
                if let Some(expiry) = expires_at {
                    let now = Instant::now();
                    if now > *expiry {
                        // Already expired
                        return ":-2\r\n".to_string();
                    }
                    let secs = expiry.duration_since(now).as_secs();
                    return format!(":{}\r\n", secs);
                } else {
                    // No expiration
                    return ":-1\r\n".to_string();
                }
            }
        }
    } else {
        ":-2\r\n".to_string() // Key does not exist
    }
}
