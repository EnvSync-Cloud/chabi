//! Hash command handlers (HSET, HGET, etc.)

use std::time::{Duration, Instant};
use std::collections::HashMap;
use crate::persistence::{Store, PersistenceManager};
use crate::protocol::{bulk_string, array_response};
use crate::types::Value;

/// Handler for HSET command
pub fn handle_hset(tokens: &[String], store: Store, persistence: &PersistenceManager) -> String {
    if tokens.len() < 4 {
        println!("HSET error: Not enough arguments");
        return "-ERR wrong number of arguments for 'hset' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    println!("HSET: Key = {}", key);
    
    let num_field_value_pairs = (tokens.len() - 2) / 2;
    println!("HSET: Processing {} field-value pairs", num_field_value_pairs);
    
    let mut fields_set = 0;
    
    let mut hash_entry = store.entry(key.clone()).or_insert_with(|| {
        println!("HSET: Creating new hash for key {}", key);
        Value::Hash {
            data: HashMap::new(),
            field_expiry: HashMap::new(),
        }
    });
    
    match &mut *hash_entry {
        Value::Hash { data, .. } => {
            // Process all field-value pairs
            for i in 0..num_field_value_pairs {
                let field_idx = 2 + i * 2;
                let value_idx = field_idx + 1;
                
                if value_idx < tokens.len() {
                    let field = &tokens[field_idx];
                    let value = &tokens[value_idx];
                    println!("HSET: Setting {}[{}] = {}", key, field, value);
                    data.insert(field.clone(), value.clone());
                    fields_set += 1;
                }
            }
            
            // Mark as modified for persistence
            persistence.mark_dirty(key);
            println!("HSET: Marked key {} as dirty for persistence", key);
            
            // Auto-persist if needed
            if persistence.auto_persist {
                println!("HSET: Auto-persisting changes");
                persistence.persist().ok();
            }
            
            println!("HSET completed: {} fields set", fields_set);
            
            // Return the number of fields that were added
            // Ensure format is ":N\r\n" for integer responses in Redis protocol
            let resp = format!(":{}\r\n", fields_set);
            println!("HSET response raw: {:?}", resp.as_bytes());
            println!("HSET RESPONSE READY TO RETURN: '{}'", resp);
            println!("!!! IMPORTANT !!! Returning HSET response: {} bytes", resp.len());
            resp
        },
        _ => {
            println!("HSET: Wrong type error for key {}", key);
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string()
        }
    }
}

/// Handler for HSETEX command (custom command for hash field with expiry)
pub fn handle_hsetex(tokens: &[String], store: Store, persistence: &PersistenceManager) -> String {
    if tokens.len() < 5 {
        return "-ERR wrong number of arguments for 'hsetex' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    let field = &tokens[2];
    let seconds = match tokens[3].parse::<u64>() {
        Ok(s) => s,
        Err(_) => return "-ERR value is not an integer or out of range\r\n".to_string(),
    };
    let value = &tokens[4];
    
    let expires_at = Instant::now() + Duration::from_secs(seconds);
    
    let mut hash_entry = store.entry(key.clone()).or_insert_with(|| {
        Value::Hash {
            data: HashMap::new(),
            field_expiry: HashMap::new(),
        }
    });
    
    match &mut *hash_entry {
        Value::Hash { data, field_expiry } => {
            // Set the value and expiry
            data.insert(field.clone(), value.clone());
            field_expiry.insert(field.clone(), expires_at);
            
            // Mark as modified for persistence
            persistence.mark_dirty(key);
            
            // Auto-persist if enabled
            if persistence.auto_persist {
                persistence.persist().ok();
            }
            
            "+OK\r\n".to_string()
        },
        _ => {
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string()
        }
    }
}

/// Handler for HGET command
pub fn handle_hget(tokens: &[String], store: Store) -> String {
    if tokens.len() < 3 {
        return "-ERR wrong number of arguments for 'hget' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    let field = &tokens[2];
    
    if let Some(value) = store.get(key) {
        match &*value {
            Value::Hash { data, field_expiry } => {
                // Check if field exists and is not expired
                if let Some(field_value) = data.get(field) {
                    // Check for field expiration
                    if let Some(expiry) = field_expiry.get(field) {
                        if Instant::now() > *expiry {
                            // Field expired
                            return "$-1\r\n".to_string();
                        }
                    }
                    
                    bulk_string(field_value)
                } else {
                    "$-1\r\n".to_string() // Field doesn't exist
                }
            },
            _ => {
                "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string()
            }
        }
    } else {
        "$-1\r\n".to_string() // Key doesn't exist
    }
}

/// Handler for HGETALL command
pub fn handle_hgetall(tokens: &[String], store: Store) -> String {
    if tokens.len() < 2 {
        return "-ERR wrong number of arguments for 'hgetall' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    
    if let Some(value) = store.get(key) {
        match &*value {
            Value::Hash { data, field_expiry } => {
                let mut result = Vec::new();
                
                for (field, value) in data {
                    // Skip expired fields
                    if let Some(expiry) = field_expiry.get(field) {
                        if Instant::now() > *expiry {
                            continue;
                        }
                    }
                    
                    result.push(field.clone());
                    result.push(value.clone());
                }
                
                if result.is_empty() {
                    "*0\r\n".to_string() // Empty array
                } else {
                    array_response(&result)
                }
            },
            _ => {
                "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string()
            }
        }
    } else {
        "*0\r\n".to_string() // Empty array for non-existing key
    }
}

/// Handler for HEXISTS command
pub fn handle_hexists(tokens: &[String], store: Store) -> String {
    if tokens.len() < 3 {
        return "-ERR wrong number of arguments for 'hexists' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    let field = &tokens[2];
    
    if let Some(value) = store.get(key) {
        match &*value {
            Value::Hash { data, field_expiry } => {
                if data.contains_key(field) {
                    // Check if field is expired
                    if let Some(expiry) = field_expiry.get(field) {
                        if Instant::now() > *expiry {
                            return ":0\r\n".to_string(); // Expired field
                        }
                    }
                    
                    ":1\r\n".to_string() // Field exists
                } else {
                    ":0\r\n".to_string() // Field does not exist
                }
            },
            _ => {
                "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string()
            }
        }
    } else {
        ":0\r\n".to_string() // Key does not exist
    }
}
