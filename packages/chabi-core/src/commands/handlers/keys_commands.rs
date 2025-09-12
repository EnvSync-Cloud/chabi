//! Keys command handlers (DEL, KEYS, EXISTS, etc.)

use std::time::Instant;
use crate::persistence::{Store, PersistenceManager};
use crate::protocol::array_response;
use crate::types::Value;

/// Handler for DEL command
pub fn handle_del(tokens: &[String], store: Store, persistence: &PersistenceManager) -> String {
    if tokens.len() < 2 {
        return "-ERR wrong number of arguments for 'del' command\r\n".to_string();
    }
    
    let mut deleted = 0;
    
    for i in 1..tokens.len() {
        let key = &tokens[i];
        if store.remove(key).is_some() {
            deleted += 1;
            
            // Mark as modified for persistence
            persistence.mark_dirty(key);
        }
    }
    
    // Auto-persist if enabled
    if persistence.auto_persist && deleted > 0 {
        persistence.persist().ok();
    }
    
    format!(":{}\r\n", deleted)
}

/// Handler for KEYS command with simple glob pattern matching
pub fn handle_keys(tokens: &[String], store: Store) -> String {
    if tokens.len() < 2 {
        return "-ERR wrong number of arguments for 'keys' command\r\n".to_string();
    }
    
    let pattern = &tokens[1];
    let is_all = pattern == "*";
    
    let mut matched_keys = Vec::new();
    
    for item in store.iter() {
        let key = item.key();
        
        // Check for expiry (for all value types)
        match &*item.value() {
            Value::String { expires_at, .. } => {
                if let Some(expiry) = expires_at {
                    if Instant::now() > *expiry {
                        continue; // Skip expired keys
                    }
                }
            },
            Value::Hash { field_expiry, .. } => {
                // Skip empty hashes (all fields expired)
                if !field_expiry.is_empty() {
                    let now = Instant::now();
                    if field_expiry.values().all(|expiry| now > *expiry) {
                        continue;
                    }
                }
            },
            Value::List { expires_at, .. } | 
            Value::Set { expires_at, .. } | 
            Value::SortedSet { expires_at, .. } => {
                if let Some(expiry) = expires_at {
                    if Instant::now() > *expiry {
                        continue; // Skip expired keys
                    }
                }
            }
        }
        
        // Match pattern
        if is_all || glob_matches(key, pattern) {
            matched_keys.push(key.clone());
        }
    }
    
    array_response(&matched_keys)
}

/// Handler for EXISTS command
pub fn handle_exists(tokens: &[String], store: Store) -> String {
    if tokens.len() < 2 {
        return "-ERR wrong number of arguments for 'exists' command\r\n".to_string();
    }
    
    let mut count = 0;
    
    for i in 1..tokens.len() {
        let key = &tokens[i];
        if let Some(value) = store.get(key) {
            // Check for expiry
            match &*value {
                Value::String { expires_at, .. } => {
                    if let Some(expiry) = expires_at {
                        if Instant::now() > *expiry {
                            continue; // Skip expired keys
                        }
                    }
                    count += 1;
                },
                Value::Hash { .. } => {
                    count += 1;
                },
                Value::List { expires_at, .. } | 
                Value::Set { expires_at, .. } | 
                Value::SortedSet { expires_at, .. } => {
                    if let Some(expiry) = expires_at {
                        if Instant::now() > *expiry {
                            continue; // Skip expired keys
                        }
                    }
                    count += 1;
                }
            }
        }
    }
    
    format!(":{}\r\n", count)
}

// Helper function for glob pattern matching
fn glob_matches(s: &str, pattern: &str) -> bool {
    // Very simple glob implementation that only supports * wildcard
    if pattern == "*" {
        return true;
    }
    
    let parts: Vec<&str> = pattern.split('*').collect();
    
    if parts.len() == 1 {
        return s == pattern;
    }
    
    let mut pos = 0;
    
    // Check if starts with
    if !pattern.starts_with('*') && !s.starts_with(parts[0]) {
        return false;
    } else if !pattern.starts_with('*') {
        pos = parts[0].len();
    }
    
    // Check middle parts
    for part in parts.iter().skip(if pattern.starts_with('*') { 0 } else { 1 })
                          .take(parts.len() - if pattern.ends_with('*') { 1 } else { 0 } - if pattern.starts_with('*') { 0 } else { 1 })
    {
        if part.is_empty() {
            continue;
        }
        
        match s[pos..].find(part) {
            Some(idx) => pos += idx + part.len(),
            None => return false,
        }
    }
    
    // Check if ends with
    if !pattern.ends_with('*') {
        s.ends_with(parts.last().unwrap())
    } else {
        true
    }
}
