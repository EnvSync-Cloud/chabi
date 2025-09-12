//! Commands module for handling Redis commands

pub mod handlers;

use crate::persistence::PersistenceManager;
use crate::protocol::{parse_resp, parse_resp_debug, error_response, simple_string};
use crate::commands::handlers::Channels;
use crate::types::ConnectionManager;

/// Type alias for connection manager reference
type ConnectionManagerRef = std::sync::Arc<ConnectionManager>;

/// Main command handler that routes commands to their specific handlers
pub fn handle_redis_command(
    input: &str, 
    persistence: &PersistenceManager, 
    channels: &Channels,
    connection_manager: &ConnectionManagerRef,
    debug_mode: bool,
    client_context: Option<&crate::types::ConnectionContext>
) -> String {
    let tokens = if debug_mode {
        parse_resp_debug(input, debug_mode)
    } else {
        parse_resp(input)
    };
    let store = persistence.get_store();
    
    if tokens.is_empty() {
        return error_response("empty command");
    }
    
    // Print debug information about the received tokens if debug mode is enabled
    if debug_mode {
        println!("Processing command: {:?}", tokens);
    };
    
    // Record command activity for the client if available
    if let Some(context) = client_context {
        connection_manager.record_activity(&context.id);
    }
    
    // Convert command to uppercase for case-insensitive matching
    match tokens[0].to_uppercase().as_str() {
        // Connection commands
        "PING" => simple_string("PONG"),
        "ECHO" => {
            if tokens.len() < 2 {
                error_response("wrong number of arguments for 'echo' command")
            } else {
                handlers::handle_echo(&tokens)
            }
        },
        
        // String commands
        "SET" => handlers::handle_set(&tokens, store, persistence),
        "SETEX" => handlers::handle_setex(&tokens, store, persistence),
        "GET" => handlers::handle_get(&tokens, store),
        "INCR" => handlers::handle_incr(&tokens, store, persistence),
        "DECR" => handlers::handle_decr(&tokens, store, persistence),
        "APPEND" => handlers::handle_append(&tokens, store, persistence),
        "STRLEN" => handlers::handle_strlen(&tokens, store),
        
        // Hash commands
        "HSET" => handlers::handle_hset(&tokens, store, persistence),
        "HSETEX" => handlers::handle_hsetex(&tokens, store, persistence),
        "HGET" => handlers::handle_hget(&tokens, store),
        "HGETALL" => handlers::handle_hgetall(&tokens, store),
        "HEXISTS" => handlers::handle_hexists(&tokens, store),
        "HDEL" => handlers::handle_hdel(&tokens, store, persistence),
        "HLEN" => handlers::handle_hlen(&tokens, store),
        "HKEYS" => handlers::handle_hkeys(&tokens, store),
        "HVALS" => handlers::handle_hvals(&tokens, store),
        
        // List commands
        "LPUSH" => handlers::handle_lpush(&tokens, store, persistence),
        "RPUSH" => handlers::handle_rpush(&tokens, store, persistence),
        "LPOP" => handlers::handle_lpop(&tokens, store, persistence),
        "RPOP" => handlers::handle_rpop(&tokens, store, persistence),
        "LRANGE" => handlers::handle_lrange(&tokens, store),
        "LLEN" => handlers::handle_llen(&tokens, store),
        
        // Set commands
        "SADD" => handlers::handle_sadd(&tokens, store, persistence),
        "SMEMBERS" => handlers::handle_smembers(&tokens, store),
        "SISMEMBER" => handlers::handle_sismember(&tokens, store),
        "SCARD" => handlers::handle_scard(&tokens, store),
        "SREM" => handlers::handle_srem(&tokens, store, persistence),
        
        // Keys commands
        "DEL" => handlers::handle_del(&tokens, store, persistence),
        "KEYS" => handlers::handle_keys(&tokens, store),
        "EXISTS" => handlers::handle_exists(&tokens, store),
        "TTL" => handlers::handle_ttl(&tokens, store),
        "EXPIRE" => handlers::handle_expire(&tokens, store, persistence),
        "RENAME" => handlers::handle_rename(&tokens, store, persistence),
        "TYPE" => handlers::handle_type(&tokens, store),
        
        // Server commands
        "INFO" => handlers::handle_info(&tokens),
        "SAVE" => {
            if let Err(e) = persistence.persist() {
                return error_response(&format!("failed to save: {}", e));
            }
            simple_string("OK")
        },
        
        // Documentation commands
        "DOCS" => handlers::handle_docs(&tokens),
        "COMMAND" => handlers::handle_command(&tokens, store),
        
        // PUB/SUB commands
        "PUBLISH" => {
            handlers::handle_publish(&tokens, channels, connection_manager)
        },
        "SUBSCRIBE" => {
            if let Some(ref context) = client_context {
                handlers::handle_subscribe(&tokens, channels, connection_manager, context)
            } else {
                error_response("client context required for SUBSCRIBE command")
            }
        },
        "UNSUBSCRIBE" => {
            if let Some(ref context) = client_context {
                handlers::handle_unsubscribe(&tokens, connection_manager, context)
            } else {
                error_response("client context required for UNSUBSCRIBE command")
            }
        },
        "PUBSUB" => {
            handlers::handle_pubsub(&tokens, channels, connection_manager)
        },
        
        // Client commands
        "CLIENT" => {
            if let Some(ref context) = client_context {
                handlers::handle_client_with_context(&tokens, connection_manager, Some(context))
            } else {
                handlers::handle_client_with_context(&tokens, connection_manager, None)
            }
        },
        
        _ => error_response(&format!("unknown command '{}'", tokens[0])),
    }
}
