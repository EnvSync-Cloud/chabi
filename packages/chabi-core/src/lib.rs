use std::error::Error;

pub mod commands;
pub mod resp;
pub mod storage;

pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

// Re-export tokio's RwLock to be used consistently across the codebase
pub use tokio::sync::RwLock;

pub use storage::DataStore;
