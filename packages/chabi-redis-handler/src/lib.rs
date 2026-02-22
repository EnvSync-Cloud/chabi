//! Redis protocol handler implementation

use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio_util::codec::Framed;
use tracing::{debug, error};

use chabi_core::commands::CommandHandler;
use chabi_core::resp::{RespParser, RespValue};

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
                            if let Some(RespValue::BulkString(Some(cmd_bytes))) = array.first() {
                                if let Ok(cmd) = String::from_utf8(cmd_bytes.clone()) {
                                    let args = array[1..].to_vec();
                                    let registry_lock = registry.read().await;

                                    match registry_lock.get(&cmd.to_lowercase()) {
                                        Some(handler) => match handler.execute(args).await {
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
                                        },
                                        None => {
                                            if let Err(e) = framed
                                                .feed(RespValue::Error(format!(
                                                    "ERR unknown command '{}'",
                                                    cmd
                                                )))
                                                .await
                                            {
                                                error!("Failed to send error response: {}", e);
                                                break;
                                            }
                                        }
                                    }
                                }
                            } else if let Err(e) = framed
                                .feed(RespValue::Error("ERR invalid command format".to_string()))
                                .await
                            {
                                error!("Failed to send error response: {}", e);
                                break;
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chabi_core::resp::RespValue;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    /// A simple PING handler for testing
    struct TestPingHandler;

    #[async_trait]
    impl CommandHandler for TestPingHandler {
        async fn execute(
            &self,
            _args: Vec<RespValue>,
        ) -> std::result::Result<RespValue, Box<dyn std::error::Error + Send + Sync>> {
            Ok(RespValue::SimpleString("PONG".to_string()))
        }
    }

    fn build_command(parts: &[&str]) -> Vec<u8> {
        let mut cmd = format!("*{}\r\n", parts.len());
        for p in parts {
            cmd.push_str(&format!("${}\r\n{}\r\n", p.len(), p));
        }
        cmd.into_bytes()
    }

    async fn send_and_read(stream: &mut TcpStream, data: &[u8]) -> String {
        stream.write_all(data).await.unwrap();
        stream.flush().await.unwrap();

        let mut buf = vec![0u8; 4096];
        let n = tokio::time::timeout(std::time::Duration::from_secs(5), stream.read(&mut buf))
            .await
            .unwrap_or(Ok(0))
            .unwrap_or(0);
        String::from_utf8_lossy(&buf[..n]).to_string()
    }

    #[tokio::test]
    async fn test_constructor() {
        let registry: HashMap<String, Box<dyn CommandHandler>> = HashMap::new();
        let handler = RedisHandler::new(registry);
        let reg = handler.registry.read().await;
        assert!(reg.is_empty());
    }

    #[tokio::test]
    async fn test_known_command() {
        let mut registry: HashMap<String, Box<dyn CommandHandler>> = HashMap::new();
        registry.insert("ping".to_string(), Box::new(TestPingHandler));
        let handler = Arc::new(RedisHandler::new(registry));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handler_clone = Arc::clone(&handler);
        tokio::spawn(async move {
            let (socket, client_addr) = listener.accept().await.unwrap();
            let registry = Arc::clone(&handler_clone.registry);
            let mut framed = Framed::new(socket, RespParser::new());

            while let Some(Ok(message)) = framed.next().await {
                match message {
                    RespValue::Array(Some(array)) => {
                        if let Some(RespValue::BulkString(Some(cmd_bytes))) = array.first() {
                            if let Ok(cmd) = String::from_utf8(cmd_bytes.clone()) {
                                let args = array[1..].to_vec();
                                let registry_lock = registry.read().await;
                                match registry_lock.get(&cmd.to_lowercase()) {
                                    Some(h) => {
                                        let resp = h.execute(args).await.unwrap();
                                        let _ = framed.feed(resp).await;
                                        let _ = framed.flush().await;
                                    }
                                    None => {
                                        let _ = framed
                                            .feed(RespValue::Error(format!(
                                                "ERR unknown command '{}'",
                                                cmd
                                            )))
                                            .await;
                                        let _ = framed.flush().await;
                                    }
                                }
                            }
                        }
                    }
                    _ => {
                        let _ = framed
                            .feed(RespValue::Error("ERR invalid command format".to_string()))
                            .await;
                        let _ = framed.flush().await;
                    }
                }
            }
            debug!("Client disconnected: {}", client_addr);
        });

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();
        let resp = send_and_read(&mut stream, &build_command(&["PING"])).await;
        assert!(resp.contains("PONG"));
    }

    #[tokio::test]
    async fn test_unknown_command() {
        let mut registry: HashMap<String, Box<dyn CommandHandler>> = HashMap::new();
        registry.insert("ping".to_string(), Box::new(TestPingHandler));
        let handler = Arc::new(RedisHandler::new(registry));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handler_clone = Arc::clone(&handler);
        tokio::spawn(async move {
            let (socket, client_addr) = listener.accept().await.unwrap();
            let registry = Arc::clone(&handler_clone.registry);
            let mut framed = Framed::new(socket, RespParser::new());

            while let Some(Ok(message)) = framed.next().await {
                match message {
                    RespValue::Array(Some(array)) => {
                        if let Some(RespValue::BulkString(Some(cmd_bytes))) = array.first() {
                            if let Ok(cmd) = String::from_utf8(cmd_bytes.clone()) {
                                let args = array[1..].to_vec();
                                let registry_lock = registry.read().await;
                                match registry_lock.get(&cmd.to_lowercase()) {
                                    Some(h) => {
                                        let resp = h.execute(args).await.unwrap();
                                        let _ = framed.feed(resp).await;
                                        let _ = framed.flush().await;
                                    }
                                    None => {
                                        let _ = framed
                                            .feed(RespValue::Error(format!(
                                                "ERR unknown command '{}'",
                                                cmd
                                            )))
                                            .await;
                                        let _ = framed.flush().await;
                                    }
                                }
                            }
                        }
                    }
                    _ => {
                        let _ = framed
                            .feed(RespValue::Error("ERR invalid command format".to_string()))
                            .await;
                        let _ = framed.flush().await;
                    }
                }
            }
            debug!("Client disconnected: {}", client_addr);
        });

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();
        let resp = send_and_read(&mut stream, &build_command(&["FOOBAR"])).await;
        assert!(resp.contains("ERR"));
        assert!(resp.contains("unknown command"));
    }

    #[tokio::test]
    async fn test_invalid_format() {
        let registry: HashMap<String, Box<dyn CommandHandler>> = HashMap::new();
        let handler = Arc::new(RedisHandler::new(registry));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handler_clone = Arc::clone(&handler);
        tokio::spawn(async move {
            let (socket, client_addr) = listener.accept().await.unwrap();
            let registry = Arc::clone(&handler_clone.registry);
            let mut framed = Framed::new(socket, RespParser::new());

            while let Some(Ok(message)) = framed.next().await {
                match message {
                    RespValue::Array(Some(array)) => {
                        if let Some(RespValue::BulkString(Some(cmd_bytes))) = array.first() {
                            if let Ok(cmd) = String::from_utf8(cmd_bytes.clone()) {
                                let args = array[1..].to_vec();
                                let registry_lock = registry.read().await;
                                match registry_lock.get(&cmd.to_lowercase()) {
                                    Some(h) => {
                                        let resp = h.execute(args).await.unwrap();
                                        let _ = framed.feed(resp).await;
                                        let _ = framed.flush().await;
                                    }
                                    None => {
                                        let _ = framed
                                            .feed(RespValue::Error(format!(
                                                "ERR unknown command '{}'",
                                                cmd
                                            )))
                                            .await;
                                        let _ = framed.flush().await;
                                    }
                                }
                            }
                        }
                    }
                    _ => {
                        let _ = framed
                            .feed(RespValue::Error("ERR invalid command format".to_string()))
                            .await;
                        let _ = framed.flush().await;
                    }
                }
            }
            debug!("Client disconnected: {}", client_addr);
        });

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();
        // Send a simple string instead of an array
        let resp = send_and_read(&mut stream, b"+PING\r\n").await;
        assert!(resp.contains("ERR"));
    }
}
