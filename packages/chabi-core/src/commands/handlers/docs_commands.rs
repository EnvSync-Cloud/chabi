//! Documentation command handlers (DOCS, COMMAND)

use crate::protocol::{bulk_string, simple_string, array_response};
use crate::persistence::Store;

/// Handler for DOCS command
/// Returns documentation for a specific command or all commands
pub fn handle_docs(tokens: &[String]) -> String {
    if tokens.len() > 2 {
        return "-ERR wrong number of arguments for 'docs' command\r\n".to_string();
    }
    
    // If a specific command is requested, return docs for that command
    if tokens.len() == 2 {
        let cmd = tokens[1].to_uppercase();
        return get_command_docs(&cmd);
    }
    
    // Otherwise, return a list of all available commands
    let commands = get_all_commands();
    simple_string(&format!("Available commands: {}", commands.join(", ")))
}

/// Handler for COMMAND command
/// Returns information about Redis commands
pub fn handle_command(tokens: &[String], _store: Store) -> String {
    if tokens.len() == 1 {
        // Return array of all commands with their metadata
        let command_info = get_all_command_info();
        return array_response(&command_info);
    }
    
    if tokens.len() >= 2 {
        let subcommand = &tokens[1].to_uppercase();
        
        match subcommand.as_str() {
            "DOCS" => {
                if tokens.len() < 3 {
                    return "-ERR wrong number of arguments for 'command docs' command\r\n".to_string();
                }
                let cmd = &tokens[2].to_uppercase();
                return bulk_string(&get_command_detailed_docs(cmd));
            },
            "COUNT" => {
                let count = get_all_commands().len();
                return format!(":{}\r\n", count);
            },
            "INFO" => {
                if tokens.len() < 3 {
                    return "-ERR wrong number of arguments for 'command info' command\r\n".to_string();
                }
                let cmd = &tokens[2].to_uppercase();
                return get_command_info(cmd);
            },
            _ => {
                return "-ERR Unknown subcommand or wrong number of arguments\r\n".to_string();
            }
        }
    }
    
    "-ERR wrong number of arguments for 'command' command\r\n".to_string()
}

// Helper function to get documentation for a specific command
fn get_command_docs(cmd: &str) -> String {
    match cmd {
        "PING" => bulk_string("PING [message] - Returns PONG if no argument is provided, otherwise returns the message."),
        "ECHO" => bulk_string("ECHO message - Returns the message."),
        "GET" => bulk_string("GET key - Returns the value of the specified key, or nil if it doesn't exist."),
        "SET" => bulk_string("SET key value [EX seconds] - Sets the value of the specified key."),
        "SETEX" => bulk_string("SETEX key seconds value - Sets the value and expiration of the specified key."),
        "DEL" => bulk_string("DEL key [key ...] - Deletes the specified keys."),
        "EXISTS" => bulk_string("EXISTS key [key ...] - Returns the number of keys that exist."),
        "KEYS" => bulk_string("KEYS pattern - Returns all keys matching the specified pattern."),
        "TTL" => bulk_string("TTL key - Returns the remaining time to live of a key."),
        "EXPIRE" => bulk_string("EXPIRE key seconds - Sets the expiration time of a key."),
        "RENAME" => bulk_string("RENAME key newkey - Renames a key."),
        "TYPE" => bulk_string("TYPE key - Returns the type of value stored at key."),
        "INCR" => bulk_string("INCR key - Increments the integer value of a key by one."),
        "DECR" => bulk_string("DECR key - Decrements the integer value of a key by one."),
        "APPEND" => bulk_string("APPEND key value - Appends a value to a key."),
        "STRLEN" => bulk_string("STRLEN key - Returns the length of the string value stored at key."),
        "HGET" => bulk_string("HGET key field - Returns the value of the specified hash field."),
        "HSET" => bulk_string("HSET key field value [field value ...] - Sets the specified hash field."),
        "HSETEX" => bulk_string("HSETEX key field seconds value - Sets a hash field with an expiration time."),
        "HGETALL" => bulk_string("HGETALL key - Returns all fields and values of the hash."),
        "HEXISTS" => bulk_string("HEXISTS key field - Returns if field is an existing field in the hash."),
        "HDEL" => bulk_string("HDEL key field [field ...] - Deletes one or more hash fields."),
        "HLEN" => bulk_string("HLEN key - Returns the number of fields in the hash."),
        "HKEYS" => bulk_string("HKEYS key - Returns all field names in the hash."),
        "HVALS" => bulk_string("HVALS key - Returns all values in the hash."),
        "LPUSH" => bulk_string("LPUSH key element [element ...] - Inserts elements at the beginning of a list."),
        "RPUSH" => bulk_string("RPUSH key element [element ...] - Inserts elements at the end of a list."),
        "LPOP" => bulk_string("LPOP key [count] - Removes and returns the first element(s) of a list."),
        "RPOP" => bulk_string("RPOP key [count] - Removes and returns the last element(s) of a list."),
        "LRANGE" => bulk_string("LRANGE key start stop - Returns a range of elements from a list."),
        "LLEN" => bulk_string("LLEN key - Returns the length of a list."),
        "SADD" => bulk_string("SADD key member [member ...] - Adds members to a set."),
        "SMEMBERS" => bulk_string("SMEMBERS key - Returns all members of a set."),
        "SISMEMBER" => bulk_string("SISMEMBER key member - Returns if member is a member of a set."),
        "SCARD" => bulk_string("SCARD key - Returns the number of members in a set."),
        "SREM" => bulk_string("SREM key member [member ...] - Removes members from a set."),
        "INFO" => bulk_string("INFO [section] - Returns information and statistics about the server."),
        "SAVE" => bulk_string("SAVE - Synchronously save the dataset to disk."),
        "DOCS" => bulk_string("DOCS [command] - Returns documentation for a specific command or all commands."),
        "COMMAND" => bulk_string("COMMAND - Returns array of command details.\n\
                               COMMAND COUNT - Returns the total number of commands.\n\
                               COMMAND INFO command - Returns information about a specific command.\n\
                               COMMAND DOCS command - Returns detailed documentation for a specific command."),
        _ => bulk_string(&format!("No documentation found for command: {}", cmd))
    }
}

// Helper function to get detailed documentation for a command
fn get_command_detailed_docs(cmd: &str) -> String {
    match cmd {
        "PING" => {
            r#"PING [message]
        
        Returns PONG if no argument is provided, otherwise returns the message.
        
        Examples:
          PING
          PING "hello world"
          
        Return value: Simple string or Bulk string"#.to_string()
        },
        "SET" => {
            r#"SET key value [EX seconds]
            
        Set key to hold the string value. If key already holds a value, it is overwritten.
        
        Options:
          EX seconds -- Set the specified expire time, in seconds.
        
        Examples:
          SET mykey "Hello"
          SET mykey "Hello" EX 10
        
        Return value: Simple string"#.to_string()
        },
        "DOCS" => {
            r#"DOCS [command]
            
        Returns documentation for the specified command, or a list of all commands if no command is specified.
        
        Examples:
          DOCS
          DOCS GET
        
        Return value: Simple string or Bulk string"#.to_string()
        },
        "COMMAND" => {
            r#"COMMAND subcommand [argument [argument ...]]
        
        Get array of Redis command details or specific information about commands.
        
        Subcommands:
          COMMAND (no subcommand) -- Returns an array of command details
          COMMAND COUNT -- Returns the total number of commands
          COMMAND INFO command -- Returns information about a specific command
          COMMAND DOCS command -- Returns detailed documentation for a specific command
        
        Examples:
          COMMAND
          COMMAND COUNT
          COMMAND INFO GET
          COMMAND DOCS SET
        
        Return value: Varies by subcommand"#.to_string()
        },
        _ => get_command_docs(cmd).replace("$", "").replace("\r\n", "")
    }
}

// Helper function to get information about a specific command
fn get_command_info(cmd: &str) -> String {
    let info = match cmd {
        "PING" => vec!["ping".to_string(), "1".to_string(), "readonly".to_string(), "0".to_string(), "1".to_string(), "1".to_string()],
        "ECHO" => vec!["echo".to_string(), "2".to_string(), "readonly".to_string(), "0".to_string(), "1".to_string(), "1".to_string()],
        "GET" => vec!["get".to_string(), "2".to_string(), "readonly".to_string(), "0".to_string(), "1".to_string(), "1".to_string()],
        "SET" => vec!["set".to_string(), "3".to_string(), "write".to_string(), "0".to_string(), "1".to_string(), "1".to_string()],
        "DEL" => vec!["del".to_string(), "-2".to_string(), "write".to_string(), "1".to_string(), "-1".to_string(), "1".to_string()],
        "EXISTS" => vec!["exists".to_string(), "-2".to_string(), "readonly".to_string(), "1".to_string(), "-1".to_string(), "1".to_string()],
        "KEYS" => vec!["keys".to_string(), "2".to_string(), "readonly".to_string(), "0".to_string(), "1".to_string(), "1".to_string()],
        "LPUSH" => vec!["lpush".to_string(), "-3".to_string(), "write".to_string(), "1".to_string(), "1".to_string(), "1".to_string()],
        "RPUSH" => vec!["rpush".to_string(), "-3".to_string(), "write".to_string(), "1".to_string(), "1".to_string(), "1".to_string()],
        "LRANGE" => vec!["lrange".to_string(), "4".to_string(), "readonly".to_string(), "1".to_string(), "1".to_string(), "1".to_string()],
        "SADD" => vec!["sadd".to_string(), "-3".to_string(), "write".to_string(), "1".to_string(), "1".to_string(), "1".to_string()],
        "SMEMBERS" => vec!["smembers".to_string(), "2".to_string(), "readonly".to_string(), "1".to_string(), "1".to_string(), "1".to_string()],
        "DOCS" => vec!["docs".to_string(), "-1".to_string(), "readonly".to_string(), "0".to_string(), "0".to_string(), "0".to_string()],
        "COMMAND" => vec!["command".to_string(), "-1".to_string(), "readonly".to_string(), "0".to_string(), "0".to_string(), "0".to_string()],
        _ => return format!("*0\r\n") // Return empty array if command not found
    };
    
    array_response(&info)
}

// Helper function to get all available commands
fn get_all_commands() -> Vec<String> {
    vec![
        "PING".to_string(), "ECHO".to_string(), 
        "GET".to_string(), "SET".to_string(), "SETEX".to_string(), "INCR".to_string(), "DECR".to_string(), "APPEND".to_string(), "STRLEN".to_string(),
        "DEL".to_string(), "EXISTS".to_string(), "KEYS".to_string(), "TTL".to_string(), "EXPIRE".to_string(), "RENAME".to_string(), "TYPE".to_string(),
        "HGET".to_string(), "HSET".to_string(), "HSETEX".to_string(), "HGETALL".to_string(), "HEXISTS".to_string(), "HDEL".to_string(), "HLEN".to_string(), "HKEYS".to_string(), "HVALS".to_string(),
        "LPUSH".to_string(), "RPUSH".to_string(), "LPOP".to_string(), "RPOP".to_string(), "LRANGE".to_string(), "LLEN".to_string(),
        "SADD".to_string(), "SMEMBERS".to_string(), "SISMEMBER".to_string(), "SCARD".to_string(), "SREM".to_string(),
        "INFO".to_string(), "SAVE".to_string(), "DOCS".to_string(), "COMMAND".to_string()
    ]
}

// Helper function to get information about all commands
fn get_all_command_info() -> Vec<String> {
    let mut result = Vec::new();
    
    for cmd in get_all_commands() {
        let info = get_command_info(&cmd);
        result.push(info);
    }
    
    result
}
