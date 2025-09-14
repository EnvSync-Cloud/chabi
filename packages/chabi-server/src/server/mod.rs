use crate::server::http::HttpServer;
use crate::server::redis::RedisServer;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

mod http;
mod redis;

pub async fn run_server(
    redis_port: u16,
    http_port: u16,
    snapshot_path: Option<String>,
    snapshot_interval_secs: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let redis_addr = SocketAddr::from(([127, 0, 0, 1], redis_port));
    let http_addr = SocketAddr::from(([127, 0, 0, 1], http_port));

    let redis_server = Arc::new(RedisServer::new());

    if let Some(ref path) = snapshot_path {
        // Load existing snapshot before starting background snapshotting
        if let Err(e) = redis_server.load_snapshot_from_path(path).await {
            tracing::error!("Failed to load snapshot from {}: {}", path, e);
        }
        let interval = Duration::from_secs(snapshot_interval_secs);
        redis_server.start_snapshot_task(path.clone(), interval);
    }

    let http_server = HttpServer::new(Arc::clone(&redis_server));

    tracing::info!(
        "Starting Redis server on port {} and HTTP server on port {}",
        redis_port,
        http_port
    );

    let redis_handle = {
        let redis_server = Arc::clone(&redis_server);
        tokio::spawn(async move {
            if let Err(e) = redis_server.run_server(redis_addr).await {
                tracing::error!("Redis server error: {}", e);
            }
        })
    };

    let http_handle = tokio::spawn(async move {
        if let Err(e) = http_server.run_server(http_addr).await {
            tracing::error!("HTTP server error: {}", e);
        }
    });

    tokio::try_join!(redis_handle, http_handle)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

    Ok(())
}
