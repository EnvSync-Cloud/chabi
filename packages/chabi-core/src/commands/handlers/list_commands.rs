//! List command handlers (LPUSH, RPUSH, LRANGE, etc.)

use std::collections::VecDeque;
use std::time::Instant;
use crate::persistence::{Store, PersistenceManager};
use crate::protocol::{bulk_string, array_response, integer_response};
use crate::types::Value;

/// Handler for LPUSH command
pub fn handle_lpush(tokens: &[String], store: Store, persistence: &PersistenceManager) -> String {
    if tokens.len() < 3 {
        return "-ERR wrong number of arguments for 'lpush' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    let mut elements_pushed = 0;
    
    // Get existing list or create new one
    let mut list = match store.get(key) {
        Some(value) => {
            match &*value {
                Value::List { data, .. } => data.clone(),
                _ => {
                    return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string();
                }
            }
        },
        None => VecDeque::new(),
    };
    
    // Push elements to the left (front) of the list
    for i in 2..tokens.len() {
        list.push_front(tokens[i].clone());
        elements_pushed += 1;
    }
    
    // Store the updated list
    store.insert(key.clone(), Value::List {
        data: list,
        expires_at: None,
    });
    
    // Mark key as modified for persistence
    persistence.mark_dirty(key);
    
    // Return the length of the list after the push operations
    integer_response(elements_pushed)
}

/// Handler for RPUSH command
pub fn handle_rpush(tokens: &[String], store: Store, persistence: &PersistenceManager) -> String {
    if tokens.len() < 3 {
        return "-ERR wrong number of arguments for 'rpush' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    let mut elements_pushed = 0;
    
    // Get existing list or create new one
    let mut list = match store.get(key) {
        Some(value) => {
            match &*value {
                Value::List { data, .. } => data.clone(),
                _ => {
                    return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string();
                }
            }
        },
        None => VecDeque::new(),
    };
    
    // Push elements to the right (back) of the list
    for i in 2..tokens.len() {
        list.push_back(tokens[i].clone());
        elements_pushed += 1;
    }
    
    // Store the updated list
    store.insert(key.clone(), Value::List {
        data: list,
        expires_at: None,
    });
    
    // Mark key as modified for persistence
    persistence.mark_dirty(key);
    
    // Return the length of the list after the push operations
    integer_response(elements_pushed)
}

/// Handler for LRANGE command
pub fn handle_lrange(tokens: &[String], store: Store) -> String {
    if tokens.len() != 4 {
        return "-ERR wrong number of arguments for 'lrange' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    
    // Parse start and stop indices
    let start = match tokens[2].parse::<isize>() {
        Ok(idx) => idx,
        Err(_) => return "-ERR value is not an integer or out of range\r\n".to_string(),
    };
    
    let stop = match tokens[3].parse::<isize>() {
        Ok(idx) => idx,
        Err(_) => return "-ERR value is not an integer or out of range\r\n".to_string(),
    };
    
    // Get the list
    match store.get(key) {
        Some(value) => {
            match &*value {
                Value::List { data, expires_at } => {
                    // Check if the key is expired
                    if let Some(expiry) = expires_at {
                        if Instant::now() > *expiry {
                            // Expired key, remove it and return empty array
                            drop(value);
                            store.remove(key);
                            return "*0\r\n".to_string();
                        }
                    }
                    
                    // Handle negative indices (count from the end of the list)
                    let len = data.len() as isize;
                    if len == 0 {
                        return "*0\r\n".to_string();
                    }
                    
                    let mut start_idx = if start < 0 { len + start } else { start };
                    if start_idx < 0 { start_idx = 0; }
                    
                    let mut stop_idx = if stop < 0 { len + stop } else { stop };
                    if stop_idx >= len { stop_idx = len - 1; }
                    
                    // Return empty array for invalid ranges
                    if start_idx > stop_idx || start_idx >= len {
                        return "*0\r\n".to_string();
                    }
                    
                    // Extract the range
                    let mut result = Vec::new();
                    for i in start_idx..=stop_idx {
                        if let Some(item) = data.get(i as usize) {
                            result.push(item.clone());
                        }
                    }
                    
                    array_response(&result)
                },
                _ => {
                    "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string()
                }
            }
        },
        None => "*0\r\n".to_string() // Empty array for non-existent keys
    }
}

/// Handler for LPOP command
pub fn handle_lpop(tokens: &[String], store: Store, persistence: &PersistenceManager) -> String {
    if tokens.len() < 2 || tokens.len() > 3 {
        return "-ERR wrong number of arguments for 'lpop' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    
    // Parse count if provided
    let count = if tokens.len() == 3 {
        match tokens[2].parse::<usize>() {
            Ok(c) if c > 0 => c,
            Ok(_) => return "*0\r\n".to_string(), // Count of 0 returns empty array
            Err(_) => return "-ERR value is not an integer or out of range\r\n".to_string(),
        }
    } else {
        1 // Default to popping one element
    };
    
    // Get the list with mutable access
    if let Some(mut value_ref) = store.get_mut(key) {
        match &mut *value_ref {
            Value::List { data, expires_at } => {
                // Check if the key is expired
                if let Some(expiry) = expires_at {
                    if Instant::now() > *expiry {
                        // Expired key, remove it and return nil
                        drop(value_ref);
                        store.remove(key);
                        return "$-1\r\n".to_string();
                    }
                }
                
                if data.is_empty() {
                    if count == 1 {
                        return "$-1\r\n".to_string(); // Nil for single pop on empty list
                    } else {
                        return "*0\r\n".to_string(); // Empty array for multi-pop on empty list
                    }
                }
                
                let mut popped = Vec::new();
                
                // Pop elements from the left (front)
                for _ in 0..count {
                    if let Some(element) = data.pop_front() {
                        popped.push(element);
                    } else {
                        break;
                    }
                }
                
                // If the list is now empty, remove the key
                if data.is_empty() {
                    drop(value_ref); // Release the reference before removal
                    store.remove(key);
                }
                
                // Mark key as modified for persistence
                persistence.mark_dirty(key);
                
                // Return single value or array based on count
                if count == 1 && popped.len() == 1 {
                    bulk_string(&popped[0])
                } else {
                    array_response(&popped)
                }
            },
            _ => {
                "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string()
            }
        }
    } else {
        // Key doesn't exist
        if count == 1 {
            "$-1\r\n".to_string() // Nil for single pop on non-existent key
        } else {
            "*0\r\n".to_string() // Empty array for multi-pop on non-existent key
        }
    }
}

/// Handler for RPOP command
pub fn handle_rpop(tokens: &[String], store: Store, persistence: &PersistenceManager) -> String {
    if tokens.len() < 2 || tokens.len() > 3 {
        return "-ERR wrong number of arguments for 'rpop' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    
    // Parse count if provided
    let count = if tokens.len() == 3 {
        match tokens[2].parse::<usize>() {
            Ok(c) if c > 0 => c,
            Ok(_) => return "*0\r\n".to_string(), // Count of 0 returns empty array
            Err(_) => return "-ERR value is not an integer or out of range\r\n".to_string(),
        }
    } else {
        1 // Default to popping one element
    };
    
    // Get the list with mutable access
    if let Some(mut value_ref) = store.get_mut(key) {
        match &mut *value_ref {
            Value::List { data, expires_at } => {
                // Check if the key is expired
                if let Some(expiry) = expires_at {
                    if Instant::now() > *expiry {
                        // Expired key, remove it and return nil
                        drop(value_ref);
                        store.remove(key);
                        return "$-1\r\n".to_string();
                    }
                }
                
                if data.is_empty() {
                    if count == 1 {
                        return "$-1\r\n".to_string(); // Nil for single pop on empty list
                    } else {
                        return "*0\r\n".to_string(); // Empty array for multi-pop on empty list
                    }
                }
                
                let mut popped = Vec::new();
                
                // Pop elements from the right (back)
                for _ in 0..count {
                    if let Some(element) = data.pop_back() {
                        popped.push(element);
                    } else {
                        break;
                    }
                }
                
                // If the list is now empty, remove the key
                if data.is_empty() {
                    drop(value_ref); // Release the reference before removal
                    store.remove(key);
                }
                
                // Mark key as modified for persistence
                persistence.mark_dirty(key);
                
                // Return single value or array based on count
                if count == 1 && popped.len() == 1 {
                    bulk_string(&popped[0])
                } else {
                    array_response(&popped)
                }
            },
            _ => {
                "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string()
            }
        }
    } else {
        // Key doesn't exist
        if count == 1 {
            "$-1\r\n".to_string() // Nil for single pop on non-existent key
        } else {
            "*0\r\n".to_string() // Empty array for multi-pop on non-existent key
        }
    }
}

/// Handler for LLEN command
pub fn handle_llen(tokens: &[String], store: Store) -> String {
    if tokens.len() != 2 {
        return "-ERR wrong number of arguments for 'llen' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    
    // Get the list length
    match store.get(key) {
        Some(value) => {
            match &*value {
                Value::List { data, expires_at } => {
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
