//! Connection command handlers (ECHO, PING, etc.)

use crate::protocol::bulk_string;

/// Handler for ECHO command
pub fn handle_echo(tokens: &[String]) -> String {
    if tokens.len() < 2 {
        return "-ERR wrong number of arguments for 'echo' command\r\n".to_string();
    }
    
    // Simply return the first argument as a bulk string
    bulk_string(&tokens[1])
}
