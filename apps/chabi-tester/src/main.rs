use clap::Parser;
use prettytable::{Table, Row, Cell};
use std::error::Error;
use tracing::{info, error, Level};

mod tests;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Redis server host
    #[arg(long, default_value = "localhost")]
    redis_host: String,

    /// Redis server port
    #[arg(long, default_value = "6379")]
    redis_port: u16,

    /// HTTP server host
    #[arg(long, default_value = "localhost")]
    http_host: String,

    /// HTTP server port
    #[arg(long, default_value = "8080")]
    http_port: u16,

    /// Enable debug logging
    #[arg(short, long)]
    debug: bool,
}

#[derive(Debug)]
struct TestResult {
    name: String,
    protocol: String,
    success: bool,
    message: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    // Initialize logging
    let level = if args.debug { Level::DEBUG } else { Level::INFO };
    tracing_subscriber::fmt().with_max_level(level).init();

    info!("Starting Chabi integration tests...");

    let mut results = Vec::new();

    // Run Redis protocol tests
    match tests::redis::run_tests(&args.redis_host, args.redis_port).await {
        Ok(redis_results) => results.extend(redis_results),
        Err(e) => error!("Redis tests failed: {}", e),
    }

    // Run HTTP tests
    match tests::http::run_tests(&args.http_host, args.http_port).await {
        Ok(http_results) => results.extend(http_results),
        Err(e) => error!("HTTP tests failed: {}", e),
    }

    // Print results table
    let mut table = Table::new();
    table.add_row(Row::new(vec![
        Cell::new("Test Name"),
        Cell::new("Protocol"),
        Cell::new("Status"),
        Cell::new("Message"),
    ]));

    let mut total = 0;
    let mut passed = 0;

    for result in results {
        total += 1;
        if result.success {
            passed += 1;
        }

        table.add_row(Row::new(vec![
            Cell::new(&result.name),
            Cell::new(&result.protocol),
            Cell::new(if result.success { "✓" } else { "✗" }),
            Cell::new(result.message.as_deref().unwrap_or(""))
        ]));
    }

    table.printstd();
    info!("Test Summary: {}/{} tests passed", passed, total);

    Ok(())
}