//! Client command handlers (CLIENT commands)

use crate::protocol::{error_response, simple_string, array_response, bulk_string};
use crate::types::{ConnectionContext, ConnectionManager};
use std::collections::HashMap;
use std::sync::Arc;

/// Handler for CLIENT command
pub fn handle_client(tokens: &[String]) -> String {
    // Legacy implementation without connection manager
    handle_client_legacy(tokens)
}

/// Handler for CLIENT command with connection manager
pub fn handle_client_with_context(tokens: &[String], connection_manager: &Arc<ConnectionManager>, context: Option<&ConnectionContext>) -> String {
    if tokens.len() < 2 {
        return error_response("wrong number of arguments for 'client' command");
    }
    
    // Convert subcommand to uppercase for case-insensitive matching
    match tokens[1].to_uppercase().as_str() {
        "ID" => {
            if let Some(ctx) = context {
                bulk_string(&ctx.id)
            } else {
                bulk_string("0") // No client context available
            }
        },
        "LIST" => {
            let clients = connection_manager.get_all_clients();
            let mut result = String::new();
            
            for client in clients {
                let client_info = format!(
                    "id={} addr={} name={} age={} idle={} flags={} db={} sub={} psub={} multi=-1 cmd={}",
                    client.id,
                    client.addr,
                    client.get_name(),
                    client.age(),
                    client.idle_time(),
                    if client.is_in_pubsub_mode { "P" } else { "N" },
                    client.db_index,
                    client.subscription_count(),
                    0, // Pattern subscriptions not implemented
                    tokens[0].to_lowercase()
                );
                
                if !result.is_empty() {
                    result.push('\n');
                }
                result.push_str(&client_info);
            }
            
            simple_string(&result)
        },
        "GETNAME" => {
            if let Some(ctx) = context {
                if let Some(name) = &ctx.name {
                    bulk_string(name)
                } else {
                    "$-1\r\n".to_string() // No name set
                }
            } else {
                "$-1\r\n".to_string()
            }
        },
        "SETNAME" => {
            if tokens.len() < 3 {
                error_response("wrong number of arguments for 'client setname' command")
            } else if let Some(ctx) = context {
                if connection_manager.set_client_name(&ctx.id, &tokens[2]) {
                    simple_string("OK")
                } else {
                    error_response("could not set client name")
                }
            } else {
                error_response("no client context available")
            }
        },
        "KILL" => {
            if tokens.len() < 3 {
                error_response("wrong number of arguments for 'client kill' command")
            } else {
                let client_id = &tokens[2];
                connection_manager.remove_client(client_id);
                simple_string("OK")
            }
        },
        "HELP" => handle_client_help(),
        _ => error_response(&format!("unknown subcommand '{}'", tokens[1])),
    }
}

/// Legacy handler for CLIENT command without connection manager
fn handle_client_legacy(tokens: &[String]) -> String {
    if tokens.len() < 2 {
        return error_response("wrong number of arguments for 'client' command");
    }
    
    // Convert subcommand to uppercase for case-insensitive matching
    match tokens[1].to_uppercase().as_str() {
        "ID" => handle_client_id(),
        "LIST" => handle_client_list(),
        "GETNAME" => handle_client_getname(),
        "SETNAME" => {
            if tokens.len() < 3 {
                error_response("wrong number of arguments for 'client setname' command")
            } else {
                handle_client_setname(&tokens[2])
            }
        },
        "KILL" => handle_client_kill(),
        "HELP" => handle_client_help(),
        _ => error_response(&format!("unknown subcommand '{}'", tokens[1])),
    }
}

/// Handler for CLIENT ID subcommand
fn handle_client_id() -> String {
    // In a real implementation, this would return the client's connection ID
    // For now, we'll return a mock ID
    bulk_string("1")
}

/// Handler for CLIENT LIST subcommand
fn handle_client_list() -> String {
    // In a real implementation, this would return a list of connected clients
    // For now, we'll return a mock client list
    simple_string("id=1 addr=127.0.0.1:12345 fd=5 name= age=3600 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=32768 obl=0 oll=0 omem=0 events=r cmd=client")
}

/// Handler for CLIENT GETNAME subcommand
fn handle_client_getname() -> String {
    // In a real implementation, this would return the client's name if set
    // For now, we'll return a nil response indicating no name is set
    "$-1\r\n".to_string()
}

/// Handler for CLIENT SETNAME subcommand (legacy)
fn handle_client_setname(_name: &str) -> String {
    // In a real implementation, this would set the client's name
    // For now, we'll just acknowledge the command
    simple_string("OK")
}

/// Handler for CLIENT KILL subcommand
fn handle_client_kill() -> String {
    // In a real implementation, this would kill a client connection
    // For now, we'll return an error
    error_response("no matching client")
}

/// Handler for CLIENT HELP subcommand
fn handle_client_help() -> String {
    let help = vec![
        "CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:".to_string(),
        "ID -- Return the client ID".to_string(),
        "LIST -- Return information about client connections".to_string(),
        "GETNAME -- Get the name of the current connection".to_string(),
        "SETNAME <name> -- Set the name of the current connection".to_string(),
        "KILL -- Kill a client".to_string(),
        "HELP -- Print this help".to_string(),
    ];
    
    array_response(&help)
}
