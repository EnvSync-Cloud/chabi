//! HTTP endpoint handler implementation

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::error;

#[derive(Debug, Serialize, Deserialize)]
pub struct KeyValue {
    value: String,
}

/// HTTP handler that provides RESTful endpoints for key-value operations
pub struct HttpHandler {
    store: Arc<RwLock<std::collections::HashMap<Vec<u8>, Vec<u8>>>>,
    port: u16,
}

type SharedStore = Arc<RwLock<std::collections::HashMap<Vec<u8>, Vec<u8>>>>;

impl HttpHandler {
    /// Create a new HTTP handler instance
    pub fn new(store: Arc<RwLock<std::collections::HashMap<Vec<u8>, Vec<u8>>>>, port: u16) -> Self {
        Self { store, port }
    }

    /// Start the HTTP server
    pub async fn run(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let store = Arc::clone(&self.store);

        let app = Router::new()
            .route("/kv/:key", get(get_value))
            .route("/kv/:key", post(set_value))
            .route("/kv/:key", delete(delete_value))
            .route("/kv/:key/exists", get(key_exists))
            .with_state(store);

        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], self.port));
        axum::serve(tokio::net::TcpListener::bind(addr).await?, app)
            .await
            .map_err(|e| e.into())
    }
}

async fn get_value(Path(key): Path<String>, State(store): State<SharedStore>) -> impl IntoResponse {
    let store = store.read().await;
    match store.get(key.as_bytes()) {
        Some(value) => match String::from_utf8(value.clone()) {
            Ok(value) => (StatusCode::OK, Json(KeyValue { value })).into_response(),
            Err(e) => {
                error!("Failed to convert value to string: {}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        },
        None => (StatusCode::NOT_FOUND, "Key not found".to_string()).into_response(),
    }
}

async fn set_value(
    Path(key): Path<String>,
    State(store): State<SharedStore>,
    Json(value): Json<KeyValue>,
) -> impl IntoResponse {
    let mut store = store.write().await;
    store.insert(key.into_bytes(), value.value.into_bytes());
    StatusCode::OK
}

async fn delete_value(
    Path(key): Path<String>,
    State(store): State<SharedStore>,
) -> impl IntoResponse {
    let mut store = store.write().await;
    store.remove(key.as_bytes());
    StatusCode::OK
}

async fn key_exists(
    Path(key): Path<String>,
    State(store): State<SharedStore>,
) -> impl IntoResponse {
    let store = store.read().await;
    Json(store.contains_key(key.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn make_app() -> (SharedStore, Router) {
        let store: SharedStore = Arc::new(RwLock::new(std::collections::HashMap::new()));
        let app = Router::new()
            .route("/kv/:key", get(get_value))
            .route("/kv/:key", post(set_value))
            .route("/kv/:key", delete(delete_value))
            .route("/kv/:key/exists", get(key_exists))
            .with_state(store.clone());
        (store, app)
    }

    #[tokio::test]
    async fn test_set_and_get() {
        let (_store, app) = make_app();

        // POST to set a value
        let body = serde_json::json!({"value": "hello"}).to_string();
        let req = Request::builder()
            .method("POST")
            .uri("/kv/mykey")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // GET to retrieve the value
        let req = Request::builder()
            .method("GET")
            .uri("/kv/mykey")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let kv: KeyValue = serde_json::from_slice(&body).unwrap();
        assert_eq!(kv.value, "hello");
    }

    #[tokio::test]
    async fn test_get_missing() {
        let (_store, app) = make_app();

        let req = Request::builder()
            .method("GET")
            .uri("/kv/nokey")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete() {
        let (_store, app) = make_app();

        // Set a value first
        let body = serde_json::json!({"value": "test"}).to_string();
        let req = Request::builder()
            .method("POST")
            .uri("/kv/delkey")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        let _ = app.clone().oneshot(req).await.unwrap();

        // Delete it
        let req = Request::builder()
            .method("DELETE")
            .uri("/kv/delkey")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // Verify it's gone
        let req = Request::builder()
            .method("GET")
            .uri("/kv/delkey")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_key_exists() {
        let (_store, app) = make_app();

        // Key doesn't exist
        let req = Request::builder()
            .method("GET")
            .uri("/kv/mykey/exists")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"false");

        // Set a value
        let body = serde_json::json!({"value": "val"}).to_string();
        let req = Request::builder()
            .method("POST")
            .uri("/kv/mykey")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        let _ = app.clone().oneshot(req).await.unwrap();

        // Now it exists
        let req = Request::builder()
            .method("GET")
            .uri("/kv/mykey/exists")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"true");
    }

    #[tokio::test]
    async fn test_constructor() {
        let store: SharedStore = Arc::new(RwLock::new(std::collections::HashMap::new()));
        let handler = HttpHandler::new(store, 9999);
        assert_eq!(handler.port, 9999);
    }
}
