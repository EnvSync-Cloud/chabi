//! Connection context for client management

use std::collections::HashSet;
use std::time::{Duration, Instant};

/// Structure to represent a client connection
#[derive(Debug, Clone)]
pub struct ConnectionContext {
    /// Unique identifier for the client
    pub id: String,
    
    /// Optional client name (set by CLIENT SETNAME)
    pub name: Option<String>,
    
    /// Client address (host:port)
    pub addr: String,
    
    /// Time when the client connected
    pub connected_at: Instant,
    
    /// Last activity time
    pub last_active: Instant,
    
    /// Database index the client is using
    pub db_index: usize,
    
    /// Channels the client is subscribed to
    pub subscriptions: HashSet<String>,
    
    /// Flag indicating if client is in pub/sub mode
    pub is_in_pubsub_mode: bool,
}

impl ConnectionContext {
    /// Create a new connection context
    pub fn new(id: String, addr: String) -> Self {
        let now = Instant::now();
        Self {
            id,
            name: None,
            addr,
            connected_at: now,
            last_active: now,
            db_index: 0,
            subscriptions: HashSet::new(),
            is_in_pubsub_mode: false,
        }
    }
    
    /// Set the client name
    pub fn set_name(&mut self, name: String) {
        self.name = Some(name);
    }
    
    /// Get the client name or id if not set
    pub fn get_name(&self) -> String {
        self.name.clone().unwrap_or_else(|| self.id.clone())
    }
    
    /// Record activity to update last_active time
    pub fn record_activity(&mut self) {
        self.last_active = Instant::now();
    }
    
    /// Get connection age in seconds
    pub fn age(&self) -> u64 {
        self.connected_at.elapsed().as_secs()
    }
    
    /// Get idle time in seconds
    pub fn idle_time(&self) -> u64 {
        self.last_active.elapsed().as_secs()
    }
    
    /// Subscribe to a channel
    pub fn subscribe(&mut self, channel: String) {
        self.subscriptions.insert(channel);
        self.is_in_pubsub_mode = true;
    }
    
    /// Unsubscribe from a channel
    pub fn unsubscribe(&mut self, channel: &str) {
        self.subscriptions.remove(channel);
        if self.subscriptions.is_empty() {
            self.is_in_pubsub_mode = false;
        }
    }
    
    /// Unsubscribe from all channels
    pub fn unsubscribe_all(&mut self) {
        self.subscriptions.clear();
        self.is_in_pubsub_mode = false;
    }
    
    /// Check if subscribed to a channel
    pub fn is_subscribed_to(&self, channel: &str) -> bool {
        self.subscriptions.contains(channel)
    }
    
    /// Get all subscriptions
    pub fn get_subscriptions(&self) -> Vec<String> {
        self.subscriptions.iter().cloned().collect()
    }
    
    /// Get subscription count
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }
}
