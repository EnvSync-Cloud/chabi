//! INFO command handlers

use std::collections::HashMap;

/// Handler for INFO command
pub fn handle_info(_tokens: &[String]) -> String {
    // Get server information
    let mut info = HashMap::new();
    
    // Server section
    info.insert("server_name".to_string(), "Chabi".to_string());
    info.insert("version".to_string(), env!("CARGO_PKG_VERSION").to_string());
    info.insert("os".to_string(), std::env::consts::OS.to_string());
    info.insert("arch".to_string(), std::env::consts::ARCH.to_string());
    info.insert("rust_version".to_string(), env!("CARGO_PKG_RUST_VERSION").to_string());
    
    // Memory section
    // We can't easily get memory usage in safe Rust, so just report placeholders
    info.insert("used_memory".to_string(), "N/A".to_string());
    info.insert("used_memory_human".to_string(), "N/A".to_string());
    
    // Format the response as a bulk string
    let mut response = String::new();
    
    for (key, value) in info {
        response.push_str(&format!("{}:{}\r\n", key, value));
    }
    
    // Format as a bulk string in RESP protocol
    format!("${}\r\n{}\r\n", response.len(), response)
}
