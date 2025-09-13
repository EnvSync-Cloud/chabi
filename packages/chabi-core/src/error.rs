//! Error types for Chabi

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ChabiError {
    #[error("Invalid command format: {0}")]
    InvalidCommand(String),

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Protocol error: {0}")]
    ProtocolError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Authentication error: {0}")]
    AuthError(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),
}

pub type Result<T> = std::result::Result<T, ChabiError>;