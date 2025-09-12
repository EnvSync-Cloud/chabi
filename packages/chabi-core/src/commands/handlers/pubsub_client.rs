//! Context-based PUB/SUB API for Redis compatibility

use crate::types::{ConnectionContext, ConnectionManager};
use crate::protocol::{error_response, simple_string, array_response, bulk_string, integer_response};
use crate::commands::handlers::pubsub_commands::{Channels, get_or_create_channel};
use std::sync::Arc;

/// PubSub client implementation using the ConnectionContext approach
pub struct PubSubClient {
    /// Channel manager
    channels: Channels,
    /// Connection manager
    connection_manager: Arc<ConnectionManager>,
}

impl PubSubClient {
    /// Create a new PubSub client
    pub fn new(channels: Channels, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            channels,
            connection_manager,
        }
    }
    
    /// Handle client connection
    pub fn connect(&self, addr: String) -> ConnectionContext {
        self.connection_manager.new_connection(addr)
    }
    
    /// Handle client disconnection
    pub fn disconnect(&self, context: &ConnectionContext) {
        self.connection_manager.remove_client(&context.id);
    }
    
    /// Handle SUBSCRIBE command
    pub fn subscribe(&self, tokens: &[String], context: &ConnectionContext) -> String {
        if tokens.len() < 2 {
            return error_response("wrong number of arguments for 'subscribe' command");
        }
        
        let mut responses = Vec::new();
        let mut subscribed_count = context.subscription_count();
        
        // Subscribe to each channel
        for i in 1..tokens.len() {
            let channel = &tokens[i];
            
            // Add subscription using the connection manager
            if let Some(_) = self.connection_manager.add_subscription(&context.id, channel) {
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
    pub fn unsubscribe(&self, tokens: &[String], context: &ConnectionContext) -> String {
        let mut responses = Vec::new();
        
        // If no channels specified, unsubscribe from all
        if tokens.len() == 1 {
            let channels = context.get_subscriptions();
            
            if let Some(updated_context) = self.connection_manager.remove_all_subscriptions(&context.id) {
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
                
                if let Some(updated_context) = self.connection_manager.remove_subscription(&context.id, channel) {
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
    
    /// Handle PUBLISH command
    pub fn publish(&self, tokens: &[String]) -> String {
        if tokens.len() != 3 {
            return error_response("wrong number of arguments for 'publish' command");
        }
        
        let channel = &tokens[1];
        let message = &tokens[2];
        
        // Get or create the channel
        let tx = get_or_create_channel(&self.channels, channel);
        
        // Get the number of clients subscribed to this channel using connection manager
        let subscribers = self.connection_manager.get_clients_subscribed_to(channel);
        let subscriber_count = subscribers.len();
        
        // Always send the message, regardless of receiver count
        // This is important because new subscribers might join after we check
        if let Err(e) = tx.send(message.clone()) {
            // Only report error if there are actual subscribers
            if subscriber_count > 0 {
                return error_response(&format!("failed to publish message: {}", e));
            }
        }
        
        // Return the number of clients that received the message
        integer_response(subscriber_count as i64)
    }
    
    /// Handle PUBSUB command
    pub fn pubsub(&self, tokens: &[String]) -> String {
        if tokens.len() < 2 {
            return error_response("wrong number of arguments for 'pubsub' command");
        }
        
        let subcommand = &tokens[1].to_uppercase();
        
        match subcommand.as_str() {
            "CHANNELS" => self.pubsub_channels(tokens),
            "NUMSUB" => self.pubsub_numsub(tokens),
            "NUMPAT" => bulk_string("0"), // Pattern subscriptions not implemented
            _ => error_response(&format!("Unknown PUBSUB subcommand: {}", subcommand)),
        }
    }
    
    /// Handle PUBSUB CHANNELS command
    fn pubsub_channels(&self, tokens: &[String]) -> String {
        let channels_lock = self.channels.lock().unwrap();
        
        // Get all channels, optionally with pattern matching
        let pattern = if tokens.len() > 2 { Some(&tokens[2]) } else { None };
        
        let mut channel_list = Vec::new();
        
        for channel_name in channels_lock.keys() {
            // Filter by pattern if provided
            if let Some(pat) = pattern {
                // Simple glob-style matching would be implemented here
                // For now, just check if channel contains the pattern
                if channel_name.contains(pat) {
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
    
    /// Handle PUBSUB NUMSUB command
    fn pubsub_numsub(&self, tokens: &[String]) -> String {
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
            let subscribers = self.connection_manager.get_clients_subscribed_to(channel);
            let count = subscribers.len();
            
            results.push(bulk_string(&count.to_string()));
        }
        
        array_response(&results)
    }
    
    /// Check if client is in PubSub mode
    pub fn is_in_pubsub_mode(&self, client_id: &str) -> bool {
        if let Some(context) = self.connection_manager.get_client(client_id) {
            context.is_in_pubsub_mode
        } else {
            false
        }
    }
    
    /// Get a client context
    pub fn get_client(&self, client_id: &str) -> Option<ConnectionContext> {
        self.connection_manager.get_client(client_id)
    }
}
