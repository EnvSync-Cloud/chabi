//! Command implementations for the Redis server.

pub mod string;
pub mod hash;
pub mod list;
pub mod set;
pub mod key;
pub mod pubsub;
pub mod server;
pub mod docs;
pub mod connection;

use async_trait::async_trait;
use crate::resp::RespValue;
use crate::Result;

#[async_trait]
pub trait CommandHandler: Send + Sync {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue>;
}