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
            ("GET", "/metrics") => {
                let body = self.redis.prometheus_metrics().await;
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\n\r\n",
                    body.len()
                );
                stream.write_all(headers.as_bytes()).await?;
                stream.write_all(body.as_bytes()).await?;
                stream.flush().await?;
            }
            ("GET", "/snapshot") => {
                let snapshots = self.redis.build_all_snapshots().await;
                let mut map: std::collections::HashMap<String, _> = snapshots
                    .into_iter()
                    .map(|(idx, snap)| (idx.to_string(), snap))
                    .collect();
                // Always include DB 0 for backward compatibility
                map.entry("0".to_string()).or_insert_with(|| {
                    chabi_core::storage::Snapshot {
                        strings: Default::default(),
                        lists: Default::default(),
                        sets: Default::default(),
                        hashes: Default::default(),
                        sorted_sets: Default::default(),
                        hll: Default::default(),
                        expirations_epoch_secs: Default::default(),
                    }
                });
                match serde_json::to_vec_pretty(&map) {
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
        HttpServer {
            redis: Arc::clone(&self.redis),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn start_http_server() -> SocketAddr {
        let redis = Arc::new(RedisServer::new());
        let http_server = HttpServer::new(redis);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((socket, _)) => {
                        let srv = http_server.clone();
                        tokio::spawn(async move {
                            let _ = srv.handle_connection(socket).await;
                        });
                    }
                    Err(_) => break,
                }
            }
        });

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        addr
    }

    async fn send_http(addr: SocketAddr, request: &str) -> String {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();

        // The HTTP handler reads, writes response, then the function returns and
        // the connection is dropped. Read until EOF to get the full response.
        let mut result = Vec::new();
        match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            stream.read_to_end(&mut result),
        )
        .await
        {
            Ok(Ok(_)) => {}
            _ => {}
        }
        String::from_utf8_lossy(&result).to_string()
    }

    #[tokio::test]
    async fn test_metrics_endpoint() {
        let addr = start_http_server().await;
        let resp = send_http(addr, "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n").await;
        assert!(resp.contains("200 OK"));
        assert!(resp.contains("chabi_connected_clients"));
    }

    #[tokio::test]
    async fn test_snapshot_endpoint() {
        let addr = start_http_server().await;
        let resp = send_http(addr, "GET /snapshot HTTP/1.1\r\nHost: localhost\r\n\r\n").await;
        assert!(resp.contains("200 OK"));
        assert!(resp.contains("application/json"));
    }

    #[tokio::test]
    async fn test_default_endpoint() {
        let addr = start_http_server().await;
        let resp = send_http(addr, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n").await;
        assert!(resp.contains("200 OK"));
        assert!(resp.contains("status") && resp.contains("ok"));
    }

    #[tokio::test]
    async fn test_clone() {
        let redis = Arc::new(RedisServer::new());
        let server = HttpServer::new(redis);
        let cloned = server.clone();
        assert!(Arc::ptr_eq(&server.redis, &cloned.redis));
    }
}
