//! Connection management for client tracking

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::types::ConnectionContext;
use uuid::Uuid;

/// Connection manager for tracking clients
#[derive(Clone, Debug)]
pub struct ConnectionManager {
    clients: Arc<Mutex<HashMap<String, ConnectionContext>>>,
}

impl ConnectionManager {
    /// Create a new connection manager
    pub fn new() -> Self {
        Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    
    /// Create a new client connection
    pub fn new_connection(&self, addr: String) -> ConnectionContext {
        let client_id = Uuid::new_v4().to_string();
        let context = ConnectionContext::new(client_id.clone(), addr);
        
        let mut clients = self.clients.lock().unwrap();
        clients.insert(client_id.clone(), context.clone());
        
        context
    }
    
    /// Get a client by ID
    pub fn get_client(&self, id: &str) -> Option<ConnectionContext> {
        let clients = self.clients.lock().unwrap();
        clients.get(id).cloned()
    }
    
    /// Update a client's context
    pub fn update_client(&self, context: &ConnectionContext) {
        let mut clients = self.clients.lock().unwrap();
        clients.insert(context.id.clone(), context.clone());
    }
    
    /// Remove a client connection
    pub fn remove_client(&self, id: &str) {
        let mut clients = self.clients.lock().unwrap();
        clients.remove(id);
    }
    
    /// Get all connected clients
    pub fn get_all_clients(&self) -> Vec<ConnectionContext> {
        let clients = self.clients.lock().unwrap();
        clients.values().cloned().collect()
    }
    
    /// Get client count
    pub fn client_count(&self) -> usize {
        let clients = self.clients.lock().unwrap();
        clients.len()
    }
    
    /// Get clients subscribed to a channel
    pub fn get_clients_subscribed_to(&self, channel: &str) -> Vec<ConnectionContext> {
        let clients = self.clients.lock().unwrap();
        clients
            .values()
            .filter(|c| c.is_subscribed_to(channel))
            .cloned()
            .collect()
    }
    
    /// Add a subscription for a client
    pub fn add_subscription(&self, client_id: &str, channel: &str) -> Option<ConnectionContext> {
        let mut clients = self.clients.lock().unwrap();
        
        if let Some(client) = clients.get_mut(client_id) {
            client.subscribe(channel.to_string());
            client.record_activity();
            return Some(client.clone());
        }
        
        None
    }
    
    /// Remove a subscription for a client
    pub fn remove_subscription(&self, client_id: &str, channel: &str) -> Option<ConnectionContext> {
        let mut clients = self.clients.lock().unwrap();
        
        if let Some(client) = clients.get_mut(client_id) {
            client.unsubscribe(channel);
            client.record_activity();
            return Some(client.clone());
        }
        
        None
    }
    
    /// Remove all subscriptions for a client
    pub fn remove_all_subscriptions(&self, client_id: &str) -> Option<ConnectionContext> {
        let mut clients = self.clients.lock().unwrap();
        
        if let Some(client) = clients.get_mut(client_id) {
            client.unsubscribe_all();
            client.record_activity();
            return Some(client.clone());
        }
        
        None
    }
    
    /// Set a client's name
    pub fn set_client_name(&self, client_id: &str, name: &str) -> bool {
        let mut clients = self.clients.lock().unwrap();
        
        if let Some(client) = clients.get_mut(client_id) {
            client.set_name(name.to_string());
            client.record_activity();
            return true;
        }
        
        false
    }
    
    /// Record activity for a client
    pub fn record_activity(&self, client_id: &str) {
        let mut clients = self.clients.lock().unwrap();
        
        if let Some(client) = clients.get_mut(client_id) {
            client.record_activity();
        }
    }
    
    /// Check if a client exists by ID
    pub fn has_client(&self, id: &str) -> bool {
        let clients = self.clients.lock().unwrap();
        clients.contains_key(id)
    }
    
    /// Create a new client with the given ID
    pub fn create_client(&self, id: &str) -> ConnectionContext {
        let context = ConnectionContext::new(id.to_string(), "unknown".to_string());
        
        let mut clients = self.clients.lock().unwrap();
        clients.insert(id.to_string(), context.clone());
        
        context
    }
}
