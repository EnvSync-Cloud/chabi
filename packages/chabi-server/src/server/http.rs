use chabi_core::Result;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::redis::RedisServer;

pub struct HttpServer {
    redis: Arc<RedisServer>,
}

impl HttpServer {
    pub fn new(redis: Arc<RedisServer>) -> Self {
        HttpServer { redis }
    }

    async fn handle_connection(&self, mut stream: TcpStream) -> Result<()> {
        let mut buffer = [0; 2048];

        let n = stream.read(&mut buffer).await?;
        if n == 0 {
            return Ok(());
        }

        let request = String::from_utf8_lossy(&buffer[..n]);
        let first_line = request.lines().next().unwrap_or("");
        let mut parts = first_line.split_whitespace();
        let method = parts.next().unwrap_or("");
        let path = parts.next().unwrap_or("/");

        match (method, path) {
            ("GET", "/snapshot") => {
                let snapshot = self.redis.build_snapshot().await;
                match serde_json::to_vec_pretty(&snapshot) {
                    Ok(body) => {
                        let headers = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                            body.len()
                        );
                        stream.write_all(headers.as_bytes()).await?;
                        stream.write_all(&body).await?;
                        stream.flush().await?;
                    }
                    Err(e) => {
                        let body = format!("{{\"error\":\"serialize failure: {}\"}}", e);
                        let headers = format!(
                            "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                            body.len()
                        );
                        stream.write_all(headers.as_bytes()).await?;
                        stream.write_all(body.as_bytes()).await?;
                        stream.flush().await?;
                    }
                }
            }
            _ => {
                let body = "{\"status\": \"ok\"}";
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                    body.len()
                );
                stream.write_all(headers.as_bytes()).await?;
                stream.write_all(body.as_bytes()).await?;
                stream.flush().await?;
            }
        }

        Ok(())
    }

    pub async fn run_server(&self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        tracing::info!("HTTP server listening on {}", addr);

        loop {
            let (socket, addr) = listener.accept().await?;
            tracing::debug!("New HTTP connection from {}", addr);

            let server = self.clone();
            tokio::spawn(async move {
                if let Err(e) = server.handle_connection(socket).await {
                    tracing::error!("Error handling HTTP connection from {}: {}", addr, e);
                }
            });
        }
    }
}

impl Clone for HttpServer {
    fn clone(&self) -> Self {
        HttpServer { redis: Arc::clone(&self.redis) }
    }
}
