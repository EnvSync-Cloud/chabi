use std::env;

mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let redis_port = env::var("REDIS_PORT")
        .unwrap_or_else(|_| "6379".to_string())
        .parse()
        .unwrap();
    let http_port = env::var("HTTP_PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse()
        .unwrap();

    println!(
        "Starting server with Redis port {} and HTTP port {}",
        redis_port, http_port
    );
    server::run_server(redis_port, http_port).await
}
