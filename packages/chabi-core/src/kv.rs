//! Main ChabiKV API module

use std::sync::Arc;
use crate::persistence::{PersistenceManager, PersistenceOptions};
use crate::commands::handle_redis_command;
use crate::commands::handlers::{create_pubsub_state, Channels};
use crate::types::ConnectionManager;

/// ChabiKV main struct
#[derive(Debug)]
pub struct ChabiKV {
    persistence: Arc<PersistenceManager>,
    debug_mode: bool,
    // PUB/SUB functionality
    pub channels: Channels,
    pub connection_manager: Arc<ConnectionManager>,
}

impl Clone for ChabiKV {
    fn clone(&self) -> Self {
        Self {
            persistence: self.persistence.clone(),
            debug_mode: self.debug_mode,
            channels: self.channels.clone(),
            connection_manager: self.connection_manager.clone(),
        }
    }
}

impl ChabiKV {
    /// Create a new KV store with optional persistence
    pub fn new(persistence_options: Option<PersistenceOptions>) -> Self {
        let store = crate::persistence::new_store();
        let persistence = PersistenceManager::new(store, persistence_options);
        let (channels, connection_manager) = create_pubsub_state();
        Self { 
            persistence, 
            debug_mode: false,
            channels,
            connection_manager,
        }
    }
    
    /// Create a new KV store with debug mode enabled
    pub fn new_with_debug(persistence_options: Option<PersistenceOptions>, debug_mode: bool) -> Self {
        let store = crate::persistence::new_store();
        let persistence = PersistenceManager::new(store, persistence_options);
        let (channels, connection_manager) = create_pubsub_state();
        Self { 
            persistence, 
            debug_mode,
            channels,
            connection_manager,
        }
    }
    
    /// Get the store for direct access
    pub fn store(&self) -> crate::persistence::Store {
        self.persistence.get_store()
    }
    
    /// Save all data to disk manually
    pub fn save(&self) -> anyhow::Result<()> {
        self.persistence.persist()
    }
    
    /// Handle a Redis command
    pub fn handle_command(&self, input: &str) -> String {
        let response = handle_redis_command(
            input, 
            &self.persistence, 
            &self.channels,
            &self.connection_manager,
            self.debug_mode,
            None // No connection context provided
        );
        if self.debug_mode {
            println!("ChabiKV handle_command response: {:?}", response);
        }
        response
    }
    
    /// Handle a Redis command with client address (legacy)
    pub fn handle_command_with_addr(&self, input: &str, client_addr: String) -> String {
        // Get or create client context
        let context = match self.connection_manager.get_client(&client_addr) {
            Some(ctx) => ctx,
            None => self.connection_manager.new_connection(client_addr)
        };
        
        self.handle_command_with_context(input, Some(&context))
    }
    
    /// Handle a Redis command with connection context
    pub fn handle_command_with_context(&self, input: &str, client_context: Option<&crate::types::ConnectionContext>) -> String {
        let response = handle_redis_command(
            input, 
            &self.persistence, 
            &self.channels,
            &self.connection_manager,
            self.debug_mode,
            client_context
        );
        if self.debug_mode {
            println!("ChabiKV handle_command response: {:?}", response);
        }
        response
    }
    
    /// Check if debug mode is enabled
    pub fn is_debug_mode(&self) -> bool {
        self.debug_mode
    }
    
    /// Set debug mode
    pub fn set_debug_mode(&mut self, debug_mode: bool) {
        self.debug_mode = debug_mode;
    }
}
