use axum::{
    routing::{post},
    Router,
    extract::{State, Json},
    http::StatusCode,
};
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use chabi_core::ChabiKV;
use tower_http::trace::TraceLayer;
use std::net::SocketAddr;

// Request type for Redis commands
#[derive(Deserialize)]
struct CommandRequest {
    command: String,
}

// Response type for Redis commands
#[derive(Serialize)]
struct CommandResponse {
    response: String,
}

// Error response type - might be used in the future for error handling
#[allow(dead_code)]
#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

// Application state
struct AppState {
    kv: Arc<ChabiKV>,
}

// Handler for Redis commands via HTTP
async fn handle_command(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<CommandRequest>,
) -> (StatusCode, Json<CommandResponse>) {
    let response = state.kv.handle_command(&payload.command);
    (
        StatusCode::OK,
        Json(CommandResponse { response }),
    )
}

// Setup and run the HTTP server
pub async fn run_http_server(
    kv: Arc<ChabiKV>,
    host: String,
    port: u16,
    _debug_mode: bool, // Keeping parameter for future debugging features
) -> anyhow::Result<()> {
    // Create app state
    let app_state = Arc::new(AppState { kv });

    // Build our application with a route
    let app = Router::new()
        .route("/command", post(handle_command))
        .layer(TraceLayer::new_for_http())
        .with_state(app_state);

    // Run the server
    let addr = format!("{}:{}", host, port).parse::<SocketAddr>()?;
    log::info!("HTTP server listening on {}", addr);
    
    // Start the server
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}
