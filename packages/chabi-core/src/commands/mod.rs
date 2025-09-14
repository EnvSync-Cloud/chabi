//! Command implementations for the Redis server.

pub mod connection;
pub mod docs;
pub mod hash;
pub mod key;
pub mod list;
pub mod pubsub;
pub mod server;
pub mod set;
pub mod string;

use crate::resp::RespValue;
use crate::Result;
use async_trait::async_trait;

#[async_trait]
pub trait CommandHandler: Send + Sync {
    async fn execute(&self, args: Vec<RespValue>) -> Result<RespValue>;
}
