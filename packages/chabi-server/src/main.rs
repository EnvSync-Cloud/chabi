use clap::Parser;
use std::env;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

mod server;

#[derive(Parser, Debug)]
#[command(author, version, about = "Chabi Redis-compatible server")]
struct Args {
    /// Enable debug logging
    #[arg(long)]
    debug: bool,

    /// Snapshot directory path
    #[arg(long)]
    snapshot_path: Option<String>,

    /// Snapshot interval in seconds
    #[arg(long, default_value_t = 60)]
    snapshot_interval_secs: u64,
}

fn parse_bind_host() -> [u8; 4] {
    let host_str = env::var("BIND_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let parts: Vec<&str> = host_str.split('.').collect();
    if parts.len() == 4 {
        if let (Ok(a), Ok(b), Ok(c), Ok(d)) = (
            parts[0].parse::<u8>(),
            parts[1].parse::<u8>(),
            parts[2].parse::<u8>(),
            parts[3].parse::<u8>(),
        ) {
            return [a, b, c, d];
        }
    }
    tracing::warn!(
        "Invalid BIND_HOST '{}', falling back to 127.0.0.1",
        host_str
    );
    [127, 0, 0, 1]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();

    // Initialize logging based on --debug flag
    let default_level = if args.debug { "debug" } else { "info" };
    let env_filter = env::var("RUST_LOG").unwrap_or_else(|_| default_level.to_string());
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .compact()
        .init();

    let redis_port = env::var("REDIS_PORT")
        .unwrap_or_else(|_| "6379".to_string())
        .parse()
        .unwrap();
    let http_port = env::var("HTTP_PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse()
        .unwrap();

    // Snapshot configuration: CLI args override env vars
    let mut snapshot_path = args
        .snapshot_path
        .or_else(|| env::var("SNAPSHOT_PATH").ok());

    let snapshot_interval_secs = if args.snapshot_interval_secs != 60 {
        args.snapshot_interval_secs
    } else {
        env::var("SNAPSHOT_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(args.snapshot_interval_secs)
    };

    // If no directory provided, create an OS-specific temp directory: chabi-${timestamp}
    if snapshot_path.is_none() {
        let tmp_dir_root = std::env::temp_dir();
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let dir: PathBuf = tmp_dir_root.join(format!("chabi-{}", ts));
        if let Err(e) = std::fs::create_dir_all(&dir) {
            tracing::error!(
                "Failed to create temp snapshot directory {}: {}",
                dir.display(),
                e
            );
        } else {
            snapshot_path = Some(dir.to_string_lossy().to_string());
        }
    }

    if let Some(ref dir) = snapshot_path {
        tracing::info!(
            "Snapshotting enabled: dir={}, interval={}s",
            dir,
            snapshot_interval_secs
        );
    } else {
        tracing::info!("Snapshotting disabled (no directory configured)");
    }

    let bind_host = parse_bind_host();

    tracing::info!(
        "Starting server on {}.{}.{}.{} with Redis port {} and HTTP port {}",
        bind_host[0],
        bind_host[1],
        bind_host[2],
        bind_host[3],
        redis_port,
        http_port
    );
    server::run_server(
        redis_port,
        http_port,
        snapshot_path,
        snapshot_interval_secs,
        bind_host,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bind_host_default() {
        // Clear BIND_HOST to get the default
        env::remove_var("BIND_HOST");
        let result = parse_bind_host();
        assert_eq!(result, [127, 0, 0, 1]);
    }

    #[test]
    fn test_parse_bind_host_custom() {
        env::set_var("BIND_HOST", "0.0.0.0");
        let result = parse_bind_host();
        assert_eq!(result, [0, 0, 0, 0]);
        env::remove_var("BIND_HOST");
    }

    #[test]
    fn test_parse_bind_host_invalid() {
        env::set_var("BIND_HOST", "not-an-ip");
        let result = parse_bind_host();
        assert_eq!(result, [127, 0, 0, 1]); // fallback
        env::remove_var("BIND_HOST");
    }

    #[test]
    fn test_parse_bind_host_partial() {
        env::set_var("BIND_HOST", "192.168.1");
        let result = parse_bind_host();
        assert_eq!(result, [127, 0, 0, 1]); // fallback - only 3 parts
        env::remove_var("BIND_HOST");
    }
}
