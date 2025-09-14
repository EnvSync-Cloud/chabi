use crate::server::http::HttpServer;
use crate::server::redis::RedisServer;
use std::net::SocketAddr;

mod http;
mod redis;

pub async fn run_server(
    redis_port: u16,
    http_port: u16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let redis_addr = SocketAddr::from(([127, 0, 0, 1], redis_port));
    let http_addr = SocketAddr::from(([127, 0, 0, 1], http_port));

    let redis_server = RedisServer::new();
    let http_server = HttpServer::new();

    println!(
        "Starting Redis server on port {} and HTTP server on port {}",
        redis_port, http_port
    );

    let redis_handle = tokio::spawn(async move {
        if let Err(e) = redis_server.run_server(redis_addr).await {
            eprintln!("Redis server error: {}", e);
        }
    });

    let http_handle = tokio::spawn(async move {
        if let Err(e) = http_server.run_server(http_addr).await {
            eprintln!("HTTP server error: {}", e);
        }
    });

    tokio::try_join!(redis_handle, http_handle)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

    Ok(())
}
