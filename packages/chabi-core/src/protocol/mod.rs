//! Protocol module for handling Redis RESP protocol parsing and formatting

/// Helper function to parse RESP (Redis Serialization Protocol) commands
pub fn parse_resp(input: &str) -> Vec<String> {
    // First, try to parse as RESP protocol
    if let Some(resp_tokens) = parse_resp_protocol(input, false) {
        return resp_tokens;
    }
    
    // Fallback to simple space-delimited parsing for direct CLI input
    input.trim()
        .split_whitespace()
        .map(|s| s.to_string())
        .collect()
}

/// Helper function to parse RESP (Redis Serialization Protocol) commands with debug output
pub fn parse_resp_debug(input: &str, debug_mode: bool) -> Vec<String> {
    // Print the raw input for debugging if debug mode is enabled
    if debug_mode {
        println!("Raw input: {:?}", input);
    }
    
    // First, try to parse as RESP protocol
    if let Some(resp_tokens) = parse_resp_protocol(input, debug_mode) {
        return resp_tokens;
    }
    
    // Fallback to simple space-delimited parsing for direct CLI input
    input.trim()
        .split_whitespace()
        .map(|s| s.to_string())
        .collect()
}

/// Parse Redis RESP protocol format
fn parse_resp_protocol(input: &str, debug_mode: bool) -> Option<Vec<String>> {
    let mut tokens: Vec<String> = Vec::new();
    
    // Split the input by CRLF
    let lines: Vec<&str> = input.split("\r\n").collect();
    
    if lines.is_empty() {
        return None;
    }
    
    let first_line = lines[0];
    
    // Check if it's an array command starting with *
    if !first_line.starts_with('*') {
        return None;
    }
    
    // Extract number of elements from *N
    let count = match first_line[1..].parse::<usize>() {
        Ok(n) => n,
        Err(_) => return None,
    };
    
    if debug_mode {
        println!("RESP Protocol: Array with {} elements", count);
    }
    
    // We should have enough data for the specified number of elements
    // Each element has a $ line and a data line
    let mut line_idx = 1;
    
    for i in 0..count {
        if line_idx >= lines.len() {
            if debug_mode {
                println!("RESP Protocol error: Not enough lines for element {}", i);
            }
            break;
        }
        
        let len_line = lines[line_idx];
        line_idx += 1;
        
        // Check if this is a bulk string indicator
        if !len_line.starts_with('$') {
            if debug_mode {
                println!("RESP Protocol error: Expected $ at line {}, got: {}", line_idx-1, len_line);
            }
            continue;
        }
        
        // Get the actual string value on the next line
        if line_idx < lines.len() {
            if debug_mode {
                println!("RESP Protocol: Element {}: '{}'", i, lines[line_idx]);
            }
            tokens.push(lines[line_idx].to_string());
            line_idx += 1;
        } else {
            if debug_mode {
                println!("RESP Protocol error: Missing value for element {}", i);
            }
        }
    }
    
    if tokens.is_empty() {
        None
    } else {
        Some(tokens)
    }
}

/// Format a simple string response according to RESP protocol
/// Used for simple success messages like "OK"
pub fn simple_string(s: &str) -> String {
    format!("+{}\r\n", s)
}

/// Format an error response according to RESP protocol
pub fn error_response(s: &str) -> String {
    format!("-{}\r\n", s)
}

/// Format an integer response according to RESP protocol
pub fn integer_response(i: i64) -> String {
    format!(":{}\r\n", i)
}

/// Format a bulk string response according to RESP protocol
pub fn bulk_string(s: &str) -> String {
    format!("${}\r\n{}\r\n", s.len(), s)
}

/// Format a nil response according to RESP protocol
pub fn null_response() -> String {
    "$-1\r\n".to_string()
}

/// Format an array response according to RESP protocol
pub fn array_response(items: &[String]) -> String {
    let mut response = format!("*{}\r\n", items.len());
    for item in items {
        response.push_str(&bulk_string(item));
    }
    response
}

/// Format an empty array response according to RESP protocol
pub fn empty_array() -> String {
    "*0\r\n".to_string()
}
