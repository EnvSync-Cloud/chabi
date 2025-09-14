use std::env;

mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize logging based on --debug flag
    let is_debug = std::env::args().any(|arg| arg == "--debug");
    let default_level = if is_debug { "debug" } else { "info" };
    let env_filter = std::env::var("RUST_LOG").unwrap_or_else(|_| default_level.to_string());
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .compact()
        .init();

    // Basic ports via env (unchanged)
    let redis_port = env::var("REDIS_PORT")
        .unwrap_or_else(|_| "6379".to_string())
        .parse()
        .unwrap();
    let http_port = env::var("HTTP_PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse()
        .unwrap();

    // Snapshot configuration with CLI overrides
    // Start with env vars
    let mut snapshot_path = env::var("SNAPSHOT_PATH").ok();
    let mut snapshot_interval_secs = env::var("SNAPSHOT_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(60);

    // Parse simple CLI flags: --snapshot-path <path>, --snapshot-interval-secs <u64>
    let mut args = std::env::args().peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--snapshot-path" => {
                if let Some(val) = args.next() { snapshot_path = Some(val); }
            }
            "--snapshot-interval-secs" => {
                if let Some(val) = args.next() { if let Ok(v) = val.parse::<u64>() { snapshot_interval_secs = v; } }
            }
            _ => {}
        }
    }

    if let Some(ref path) = snapshot_path {
        tracing::info!(
            "Snapshotting enabled: path={}, interval={}s",
            path, snapshot_interval_secs
        );
    } else {
        tracing::info!("Snapshotting disabled (no path configured)");
    }

    tracing::info!("Starting server with Redis port {} and HTTP port {}", redis_port, http_port);
    server::run_server(redis_port, http_port, snapshot_path, snapshot_interval_secs).await
}
