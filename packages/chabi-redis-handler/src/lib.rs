//! Redis protocol handler implementation

use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio_util::codec::Framed;
use futures::{SinkExt, StreamExt};
use tracing::{debug, error};

use chabi_core::resp::{RespParser, RespValue};
use chabi_core::commands::CommandHandler;

/// Redis protocol handler that manages connections and command execution
pub struct RedisHandler {
    registry: Arc<RwLock<HashMap<String, Box<dyn CommandHandler>>>>,
}

impl RedisHandler {
    /// Create a new Redis handler instance
    pub fn new(registry: HashMap<String, Box<dyn CommandHandler>>) -> Self {
        Self {
            registry: Arc::new(RwLock::new(registry)),
        }
    }

    /// Start the Redis protocol server
    pub async fn run(&self, port: u16) -> Result<(), Box<dyn Error + Send + Sync>> {
        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
        let listener = TcpListener::bind(addr).await?;
        debug!("Redis protocol server listening on {}", addr);

        loop {
            let (socket, addr) = listener.accept().await?;
            debug!("New connection from {}", addr);

            let registry = Arc::clone(&self.registry);

            tokio::spawn(async move {
                let mut framed = Framed::new(socket, RespParser::new());

                while let Some(Ok(message)) = framed.next().await {
                    match message {
                        RespValue::Array(Some(array)) => {
                            if let Some(RespValue::BulkString(Some(cmd_bytes))) = array.get(0) {
                                if let Ok(cmd) = String::from_utf8(cmd_bytes.clone()) {
                                    let args = array[1..].to_vec();
                                    let registry_lock = registry.read().await;

                                    match registry_lock.get(&cmd.to_lowercase()) {
                                        Some(handler) => {
                                            match handler.execute(args).await {
                                                Ok(response) => {
                                                    if let Err(e) = framed.feed(response).await {
                                                        error!("Failed to send response: {}", e);
                                                        break;
                                                    }
                                                }
                                                Err(e) => {
                                                    if let Err(e) = framed
                                                        .feed(RespValue::Error(format!("ERR {}", e)))
                                                        .await
                                                    {
                                                        error!("Failed to send error response: {}", e);
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                        None => {
                                            if let Err(e) = framed
                                                .feed(RespValue::Error(format!("ERR unknown command '{}'", cmd)))
                                                .await
                                            {
                                                error!("Failed to send error response: {}", e);
                                                break;
                                            }
                                        }
                                    }
                                } else {
                                    if let Err(e) = framed
                                        .feed(RespValue::Error("ERR invalid command format".to_string()))
                                        .await
                                    {
                                        error!("Failed to send error response: {}", e);
                                        break;
                                    }
                                }
                            } else {
                                if let Err(e) = framed
                                    .feed(RespValue::Error("ERR invalid command format".to_string()))
                                    .await
                                {
                                    error!("Failed to send error response: {}", e);
                                    break;
                                }
                            }
                        }
                        _ => {
                            if let Err(e) = framed
                                .feed(RespValue::Error("ERR invalid command format".to_string()))
                                .await
                            {
                                error!("Failed to send error response: {}", e);
                                break;
                            }
                        }
                    }
                }

                debug!("Client disconnected: {}", addr);
            });
        }
    }
}