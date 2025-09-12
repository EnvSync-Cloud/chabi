//! PUB/SUB command handlers

use crate::protocol::{error_response, integer_response, array_response, bulk_string};
use crate::types::{ConnectionContext, ConnectionManager};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use tokio::sync::broadcast;


// Type aliases for channel management
pub type Channels = Arc<Mutex<HashMap<String, broadcast::Sender<String>>>>;

/// Maximum number of messages in a channel's history
const MAX_CHANNEL_CAPACITY: usize = 1000;

// Connection manager for client tracking
type ConnectionManagerRef = Arc<ConnectionManager>;

/// Get or create a channel
pub fn get_or_create_channel(channels: &Channels, channel: &str) -> broadcast::Sender<String> {
    let mut channels_lock = channels.lock().unwrap();
    channels_lock
        .entry(channel.to_string())
        .or_insert_with(|| broadcast::channel(MAX_CHANNEL_CAPACITY).0)
        .clone()
}

/// Handle PUBLISH command
pub fn handle_publish(
    tokens: &[String], 
    channels: &Channels, 
    connection_manager: &ConnectionManagerRef
) -> String {
    if tokens.len() != 3 {
        return error_response("wrong number of arguments for 'publish' command");
    }
    
    let channel = &tokens[1];
    let message = &tokens[2];
    
    // Get or create the channel
    let tx = get_or_create_channel(channels, channel);
    
    // Get the number of clients subscribed to this channel using connection manager
    let subscribers = connection_manager.get_clients_subscribed_to(channel);
    let subscriber_count = subscribers.len();
    
    // Always send the message, regardless of receiver count
    // This is important because new subscribers might join after we check
    match tx.send(message.clone()) {
        Ok(receiver_count) => {
            // Just print the success without the log crate
            println!("Successfully published message '{}' to channel '{}', receiver count: {}", 
                message, channel, receiver_count);
        },
        Err(e) => {
            // Only report error if there are actual subscribers
            if subscriber_count > 0 {
                return error_response(&format!("failed to publish message: {}", e));
            }
            // Just print the error without the log crate
            println!("Failed to publish message '{}' to channel '{}': {}", 
                message, channel, e);
        }
    }
    
    // Return the number of clients that received the message
    integer_response(subscriber_count as i64)
}

/// Handle SUBSCRIBE command
pub fn handle_subscribe(
    tokens: &[String],
    _channels: &Channels,
    connection_manager: &ConnectionManagerRef,
    context: &ConnectionContext,
) -> String {
    if tokens.len() < 2 {
        return error_response("wrong number of arguments for 'subscribe' command");
    }
    
    let mut responses = Vec::new();
    let mut subscribed_count = context.subscription_count();
    
    // Subscribe to each channel
    for i in 1..tokens.len() {
        let channel = &tokens[i];
        
        // Add subscription using the connection manager
        if let Some(_) = connection_manager.add_subscription(&context.id, channel) {
            subscribed_count += 1;
            
            // Create subscription response for this channel
            let mut subscribe_response = Vec::new();
            subscribe_response.push("subscribe".to_string());
            subscribe_response.push(channel.clone());
            subscribe_response.push(subscribed_count.to_string());
            
            responses.push(array_response(&subscribe_response));
        }
    }
    
    // Combine all responses
    responses.join("")
}

/// Handle UNSUBSCRIBE command
pub fn handle_unsubscribe(
    tokens: &[String],
    connection_manager: &ConnectionManagerRef,
    context: &ConnectionContext,
) -> String {
    let mut responses = Vec::new();
    
    // If no channels specified, unsubscribe from all
    if tokens.len() == 1 {
        let channels = context.get_subscriptions();
        
        if let Some(updated_context) = connection_manager.remove_all_subscriptions(&context.id) {
            let subscribed_count = updated_context.subscription_count();
            
            for channel in channels {
                // Create unsubscribe response for this channel
                let mut unsubscribe_response = Vec::new();
                unsubscribe_response.push("unsubscribe".to_string());
                unsubscribe_response.push(channel);
                unsubscribe_response.push(subscribed_count.to_string());
                
                responses.push(array_response(&unsubscribe_response));
            }
        }
    } else {
        // Unsubscribe from specified channels
        for i in 1..tokens.len() {
            let channel = &tokens[i];
            
            if let Some(updated_context) = connection_manager.remove_subscription(&context.id, channel) {
                let subscribed_count = updated_context.subscription_count();
                
                // Create unsubscribe response for this channel
                let mut unsubscribe_response = Vec::new();
                unsubscribe_response.push("unsubscribe".to_string());
                unsubscribe_response.push(channel.clone());
                unsubscribe_response.push(subscribed_count.to_string());
                
                responses.push(array_response(&unsubscribe_response));
            }
        }
    }
    
    // Combine all responses
    responses.join("")
}

/// Handle PUBSUB CHANNELS command
pub fn handle_pubsub_channels(tokens: &[String], channels: &Channels) -> String {
    let channels_lock = channels.lock().unwrap();
    
    // Get all channels, optionally with pattern matching
    let pattern = if tokens.len() > 2 { Some(&tokens[2]) } else { None };
    
    let mut channel_list = Vec::new();
    
    for channel_name in channels_lock.keys() {
        // Filter by pattern if provided
        if let Some(pat) = pattern {
            // Simple glob-style matching (only * wildcard supported)
            if matches_pattern(channel_name, pat) {
                channel_list.push(channel_name.clone());
            }
        } else {
            channel_list.push(channel_name.clone());
        }
    }
    
    // Convert channel names to bulk strings
    let results: Vec<String> = channel_list.iter()
        .map(|c| bulk_string(c))
        .collect();
    
    array_response(&results)
}

// Legacy handle_pubsub_numsub function removed

// Legacy pubsub function removed

/// Handle PUBSUB command
pub fn handle_pubsub(
    tokens: &[String], 
    _channels: &Channels,
    connection_manager: &ConnectionManagerRef
) -> String {
    if tokens.len() < 2 {
        return error_response("wrong number of arguments for 'pubsub' command");
    }
    
    let subcommand = &tokens[1].to_uppercase();
    
    match subcommand.as_str() {
        "CHANNELS" => handle_pubsub_channels(tokens, _channels),
        "NUMSUB" => handle_pubsub_numsub(tokens, _channels, connection_manager),
        "NUMPAT" => bulk_string("0"), // Pattern subscriptions not implemented
        _ => error_response(&format!("Unknown PUBSUB subcommand: {}", subcommand)),
    }
}

/// Handle PUBSUB NUMSUB command with connection manager
pub fn handle_pubsub_numsub(
    tokens: &[String],
    _channels: &Channels,
    connection_manager: &ConnectionManagerRef
) -> String {
    let mut results = Vec::new();
    
    // If no channels specified, return empty array
    if tokens.len() <= 2 {
        return array_response(&results);
    }
    
    // Get subscriber count for each specified channel
    for i in 2..tokens.len() {
        let channel = &tokens[i];
        results.push(bulk_string(channel));
        
        // Get all clients subscribed to this channel
        let subscribers = connection_manager.get_clients_subscribed_to(channel);
        let count = subscribers.len();
        
        results.push(bulk_string(&count.to_string()));
    }
    
    array_response(&results)
}

/// Get all channels a client is subscribed to
pub fn get_client_subscriptions(connection_manager: &ConnectionManagerRef, client_id: &str) -> Vec<String> {
    if let Some(context) = connection_manager.get_client(client_id) {
        context.get_subscriptions()
    } else {
        Vec::new()
    }
}

/// Simple pattern matching for PUBSUB CHANNELS
/// Only supports the * wildcard
fn matches_pattern(s: &str, pattern: &str) -> bool {
    // Convert glob pattern to regex
    let pattern_regex = pattern
        .replace(".", "\\.")
        .replace("*", ".*");
    
    // Create regex from pattern
    match regex::Regex::new(&format!("^{}$", pattern_regex)) {
        Ok(re) => re.is_match(s),
        Err(_) => false,
    }
}

/// Create new PUB/SUB channels and connection manager
pub fn create_pubsub_state() -> (Channels, ConnectionManagerRef) {
    (
        Arc::new(Mutex::new(HashMap::new())),
        Arc::new(ConnectionManager::new()),
    )
}

/// Remove a client
pub fn remove_client(connection_manager: &ConnectionManagerRef, client_id: &str) {
    connection_manager.remove_client(client_id);
}
