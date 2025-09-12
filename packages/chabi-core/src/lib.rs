//! Chabi-core - A Redis-compatible key-value store library
//!
//! This library provides the core functionality for the Chabi key-value store,
//! including data storage, persistence, and command handling.

pub mod types;
pub mod persistence;
pub mod protocol;
pub mod commands;
mod kv;

// Re-export main types
pub use types::{Value, SerializedValue};
pub use persistence::{PersistenceManager, PersistenceOptions, Store, new_store};
pub use kv::ChabiKV;
