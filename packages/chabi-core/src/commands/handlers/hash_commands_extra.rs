//! Additional hash commands handlers (HDEL, HLEN, HKEYS, HVALS)

use std::time::Instant;
use crate::persistence::{Store, PersistenceManager};
use crate::protocol::{integer_response, array_response};
use crate::types::Value;

/// Handler for HDEL command
pub fn handle_hdel(tokens: &[String], store: Store, persistence: &PersistenceManager) -> String {
    if tokens.len() < 3 {
        return "-ERR wrong number of arguments for 'hdel' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    let mut deleted_count = 0;
    
    // Check if the key exists and is a hash
    if let Some(mut value_ref) = store.get_mut(key) {
        match &mut *value_ref {
            Value::Hash { data, field_expiry } => {
                // Process each field to delete
                for i in 2..tokens.len() {
                    let field = &tokens[i];
                    if data.remove(field).is_some() {
                        // Also remove any expiry for this field
                        field_expiry.remove(field);
                        deleted_count += 1;
                    }
                }
                
                // If the hash is now empty, remove the key after dropping the reference
                if data.is_empty() {
                    drop(value_ref); // Release the mutable reference before removal
                    store.remove(key);
                }
                
                // Mark key as modified for persistence if any fields were deleted
                if deleted_count > 0 {
                    persistence.mark_dirty(key);
                }
            },
            _ => {
                return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string();
            }
        }
    }
    
    // Return the number of fields that were deleted
    integer_response(deleted_count)
}

/// Handler for HLEN command
pub fn handle_hlen(tokens: &[String], store: Store) -> String {
    if tokens.len() != 2 {
        return "-ERR wrong number of arguments for 'hlen' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    
    // Get the hash length
    if let Some(value) = store.get(key) {
        match &*value {
            Value::Hash { data, field_expiry } => {
                // Count only non-expired fields
                let now = Instant::now();
                let count = data.iter().filter(|(field, _)| {
                    !field_expiry.get(field.as_str()).map_or(false, |&exp| exp < now)
                }).count();
                
                integer_response(count as i64)
            },
            _ => {
                return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string();
            }
        }
    } else {
        // Hash doesn't exist
        integer_response(0)
    }
}

/// Handler for HKEYS command
pub fn handle_hkeys(tokens: &[String], store: Store) -> String {
    if tokens.len() != 2 {
        return "-ERR wrong number of arguments for 'hkeys' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    
    // Get the hash keys
    if let Some(value) = store.get(key) {
        match &*value {
            Value::Hash { data, field_expiry } => {
                // Filter out expired fields
                let now = Instant::now();
                let keys: Vec<String> = data.keys()
                    .filter(|field| {
                        !field_expiry.get(field.as_str()).map_or(false, |&exp| exp < now)
                    })
                    .cloned()
                    .collect();
                
                array_response(&keys)
            },
            _ => {
                return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string();
            }
        }
    } else {
        // Hash doesn't exist
        "*0\r\n".to_string() // Empty array
    }
}

/// Handler for HVALS command
pub fn handle_hvals(tokens: &[String], store: Store) -> String {
    if tokens.len() != 2 {
        return "-ERR wrong number of arguments for 'hvals' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    
    // Get the hash values
    if let Some(value) = store.get(key) {
        match &*value {
            Value::Hash { data, field_expiry } => {
                // Filter out expired fields
                let now = Instant::now();
                let values: Vec<String> = data.iter()
                    .filter(|(field, _)| {
                        !field_expiry.get(field.as_str()).map_or(false, |&exp| exp < now)
                    })
                    .map(|(_, value)| value.clone())
                    .collect();
                
                array_response(&values)
            },
            _ => {
                return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string();
            }
        }
    } else {
        // Hash doesn't exist
        "*0\r\n".to_string() // Empty array
    }
}
