//! Additional string command handlers (INCR, DECR, APPEND, STRLEN, etc.)

use std::time::Instant;
use crate::persistence::{Store, PersistenceManager};
use crate::protocol::integer_response;
use crate::types::Value;

/// Handler for INCR command
pub fn handle_incr(tokens: &[String], store: Store, persistence: &PersistenceManager) -> String {
    if tokens.len() != 2 {
        return "-ERR wrong number of arguments for 'incr' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    let result;
    
    // Check if the key exists and is a string
    let value_opt = store.get(key);
    
    if let Some(value) = value_opt {
        match &*value {
            Value::String { data, expires_at } => {
                // Check if the key is expired
                if let Some(expiry) = expires_at {
                    if Instant::now() > *expiry {
                        // Expired key, remove it and treat as non-existent
                        drop(value);
                        store.remove(key);
                        result = 1;
                        store.insert(key.clone(), Value::String {
                            data: "1".to_string(),
                            expires_at: None,
                        });
                    } else {
                        // Parse the string value as an integer and increment
                        match data.parse::<i64>() {
                            Ok(num) => {
                                let new_num = num + 1;
                                result = new_num;
                                store.insert(key.clone(), Value::String {
                                    data: new_num.to_string(),
                                    expires_at: *expires_at,
                                });
                            }
                            Err(_) => {
                                return "-ERR value is not an integer or out of range\r\n".to_string();
                            }
                        }
                    }
                } else {
                    // Parse the string value as an integer and increment
                    match data.parse::<i64>() {
                        Ok(num) => {
                            let new_num = num + 1;
                            result = new_num;
                            store.insert(key.clone(), Value::String {
                                data: new_num.to_string(),
                                expires_at: None,
                            });
                        }
                        Err(_) => {
                            return "-ERR value is not an integer or out of range\r\n".to_string();
                        }
                    }
                }
            },
            _ => {
                return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string();
            }
        }
    } else {
        // Key doesn't exist, create it with value "1"
        result = 1;
        store.insert(key.clone(), Value::String {
            data: "1".to_string(),
            expires_at: None,
        });
    }
    
    // Mark key as modified for persistence
    persistence.mark_dirty(key);
    
    integer_response(result)
}

/// Handler for DECR command
pub fn handle_decr(tokens: &[String], store: Store, persistence: &PersistenceManager) -> String {
    if tokens.len() != 2 {
        return "-ERR wrong number of arguments for 'decr' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    let result;
    
    // Check if the key exists and is a string
    let value_opt = store.get(key);
    
    if let Some(value) = value_opt {
        match &*value {
            Value::String { data, expires_at } => {
                // Check if the key is expired
                if let Some(expiry) = expires_at {
                    if Instant::now() > *expiry {
                        // Expired key, remove it and treat as non-existent
                        drop(value);
                        store.remove(key);
                        result = -1;
                        store.insert(key.clone(), Value::String {
                            data: "-1".to_string(),
                            expires_at: None,
                        });
                    } else {
                        // Parse the string value as an integer and decrement
                        match data.parse::<i64>() {
                            Ok(num) => {
                                let new_num = num - 1;
                                result = new_num;
                                store.insert(key.clone(), Value::String {
                                    data: new_num.to_string(),
                                    expires_at: *expires_at,
                                });
                            }
                            Err(_) => {
                                return "-ERR value is not an integer or out of range\r\n".to_string();
                            }
                        }
                    }
                } else {
                    // Parse the string value as an integer and decrement
                    match data.parse::<i64>() {
                        Ok(num) => {
                            let new_num = num - 1;
                            result = new_num;
                            store.insert(key.clone(), Value::String {
                                data: new_num.to_string(),
                                expires_at: None,
                            });
                        }
                        Err(_) => {
                            return "-ERR value is not an integer or out of range\r\n".to_string();
                        }
                    }
                }
            },
            _ => {
                return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string();
            }
        }
    } else {
        // Key doesn't exist, create it with value "-1"
        result = -1;
        store.insert(key.clone(), Value::String {
            data: "-1".to_string(),
            expires_at: None,
        });
    }
    
    // Mark key as modified for persistence
    persistence.mark_dirty(key);
    
    integer_response(result)
}

/// Handler for APPEND command
pub fn handle_append(tokens: &[String], store: Store, persistence: &PersistenceManager) -> String {
    if tokens.len() != 3 {
        return "-ERR wrong number of arguments for 'append' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    let value_to_append = &tokens[2];
    let new_len;
    
    // Check if the key exists and is a string
    if let Some(value) = store.get(key) {
        match &*value {
            Value::String { data, expires_at } => {
                // Check if the key is expired
                if let Some(expiry) = expires_at {
                    if Instant::now() > *expiry {
                        // Expired key, remove it and create new one
                        drop(value);
                        store.remove(key);
                        new_len = value_to_append.len();
                        store.insert(key.clone(), Value::String {
                            data: value_to_append.to_string(),
                            expires_at: None,
                        });
                    } else {
                        // Append to the existing string
                        let mut new_data = data.clone();
                        new_data.push_str(value_to_append);
                        new_len = new_data.len();
                        store.insert(key.clone(), Value::String {
                            data: new_data,
                            expires_at: *expires_at,
                        });
                    }
                } else {
                    // Append to the existing string
                    let mut new_data = data.clone();
                    new_data.push_str(value_to_append);
                    new_len = new_data.len();
                    store.insert(key.clone(), Value::String {
                        data: new_data,
                        expires_at: None,
                    });
                }
            },
            _ => {
                return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string();
            }
        }
    } else {
        // Key doesn't exist, create it
        new_len = value_to_append.len();
        store.insert(key.clone(), Value::String {
            data: value_to_append.to_string(),
            expires_at: None,
        });
    }
    
    // Mark key as modified for persistence
    persistence.mark_dirty(key);
    
    integer_response(new_len as i64)
}

/// Handler for STRLEN command
pub fn handle_strlen(tokens: &[String], store: Store) -> String {
    if tokens.len() != 2 {
        return "-ERR wrong number of arguments for 'strlen' command\r\n".to_string();
    }
    
    let key = &tokens[1];
    
    // Check if the key exists and is a string
    if let Some(value) = store.get(key) {
        match &*value {
            Value::String { data, expires_at } => {
                // Check if the key is expired
                if let Some(expiry) = expires_at {
                    if Instant::now() > *expiry {
                        // Expired key, remove it and return 0
                        drop(value);
                        store.remove(key);
                        return integer_response(0);
                    }
                }
                integer_response(data.len() as i64)
            },
            _ => {
                return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string();
            }
        }
    } else {
        // Key doesn't exist
        integer_response(0)
    }
}
