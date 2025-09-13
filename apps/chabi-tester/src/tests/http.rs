use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::TestResult;

#[derive(Debug, Serialize)]
struct CommandRequest {
    command: String,
    args: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CommandResponse {
    status: String,
    data: Option<String>,
    error: Option<String>,
}

pub async fn run_tests(host: &str, port: u16) -> Result<Vec<TestResult>, Box<dyn std::error::Error>> {
    let client = Client::new();
    let base_url = format!("http://{}:{}", host, port);
    let mut results = Vec::new();

    // Test health check endpoint (only supported HTTP endpoint currently)
    let health_result = test_health_check(&client, &base_url).await;
    results.push(health_result);

    Ok(results)
}

async fn test_health_check(client: &Client, base_url: &str) -> TestResult {
    debug!("Running health check test");
    match client.get(base_url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                TestResult {
                    name: "Health Check".to_string(),
                    protocol: "HTTP".to_string(),
                    success: true,
                    message: None,
                }
            } else {
                TestResult {
                    name: "Health Check".to_string(),
                    protocol: "HTTP".to_string(),
                    success: false,
                    message: Some(format!("Unexpected status: {}", response.status())),
                }
            }
        }
        Err(e) => TestResult {
            name: "Health Check".to_string(),
            protocol: "HTTP".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

// Below helpers are kept for potential future HTTP handlers that expose
// a command or key-value API; they are not invoked by run_tests currently.
async fn test_set(client: &Client, base_url: &str) -> TestResult {
    debug!("Running SET command test");
    let request = CommandRequest {
        command: "SET".to_string(),
        args: vec!["test_key".to_string(), "test_value".to_string()],
    };

    match client
        .post(format!("{}/command", base_url))
        .json(&request)
        .send()
        .await
    {
        Ok(response) => {
            match response.json::<CommandResponse>().await {
                Ok(resp) => {
                    if resp.status == "success" {
                        TestResult {
                            name: "SET Command".to_string(),
                            protocol: "HTTP".to_string(),
                            success: true,
                            message: None,
                        }
                    } else {
                        TestResult {
                            name: "SET Command".to_string(),
                            protocol: "HTTP".to_string(),
                            success: false,
                            message: resp.error,
                        }
                    }
                }
                Err(e) => TestResult {
                    name: "SET Command".to_string(),
                    protocol: "HTTP".to_string(),
                    success: false,
                    message: Some(format!("Failed to parse response: {}", e)),
                },
            }
        }
        Err(e) => TestResult {
            name: "SET Command".to_string(),
            protocol: "HTTP".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_get(client: &Client, base_url: &str) -> TestResult {
    debug!("Running GET command test");
    let request = CommandRequest {
        command: "GET".to_string(),
        args: vec!["test_key".to_string()],
    };

    match client
        .post(format!("{}/command", base_url))
        .json(&request)
        .send()
        .await
    {
        Ok(response) => {
            match response.json::<CommandResponse>().await {
                Ok(resp) => {
                    if resp.status == "success" && resp.data.as_deref() == Some("test_value") {
                        TestResult {
                            name: "GET Command".to_string(),
                            protocol: "HTTP".to_string(),
                            success: true,
                            message: None,
                        }
                    } else {
                        TestResult {
                            name: "GET Command".to_string(),
                            protocol: "HTTP".to_string(),
                            success: false,
                            message: Some(format!("Unexpected value: {:?}", resp.data)),
                        }
                    }
                }
                Err(e) => TestResult {
                    name: "GET Command".to_string(),
                    protocol: "HTTP".to_string(),
                    success: false,
                    message: Some(format!("Failed to parse response: {}", e)),
                },
            }
        }
        Err(e) => TestResult {
            name: "GET Command".to_string(),
            protocol: "HTTP".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_del(client: &Client, base_url: &str) -> TestResult {
    debug!("Running DEL command test");
    let request = CommandRequest {
        command: "DEL".to_string(),
        args: vec!["test_key".to_string()],
    };

    match client
        .post(format!("{}/command", base_url))
        .json(&request)
        .send()
        .await
    {
        Ok(response) => {
            match response.json::<CommandResponse>().await {
                Ok(resp) => {
                    if resp.status == "success" {
                        TestResult {
                            name: "DEL Command".to_string(),
                            protocol: "HTTP".to_string(),
                            success: true,
                            message: None,
                        }
                    } else {
                        TestResult {
                            name: "DEL Command".to_string(),
                            protocol: "HTTP".to_string(),
                            success: false,
                            message: resp.error,
                        }
                    }
                }
                Err(e) => TestResult {
                    name: "DEL Command".to_string(),
                    protocol: "HTTP".to_string(),
                    success: false,
                    message: Some(format!("Failed to parse response: {}", e)),
                },
            }
        }
        Err(e) => TestResult {
            name: "DEL Command".to_string(),
            protocol: "HTTP".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_exists(client: &Client, base_url: &str) -> TestResult {
    debug!("Running EXISTS command test");
    let request = CommandRequest {
        command: "EXISTS".to_string(),
        args: vec!["test_key".to_string()],
    };

    match client
        .post(format!("{}/command", base_url))
        .json(&request)
        .send()
        .await
    {
        Ok(response) => {
            match response.json::<CommandResponse>().await {
                Ok(resp) => {
                    if resp.status == "success" {
                        TestResult {
                            name: "EXISTS Command".to_string(),
                            protocol: "HTTP".to_string(),
                            success: true,
                            message: None,
                        }
                    } else {
                        TestResult {
                            name: "EXISTS Command".to_string(),
                            protocol: "HTTP".to_string(),
                            success: false,
                            message: resp.error,
                        }
                    }
                }
                Err(e) => TestResult {
                    name: "EXISTS Command".to_string(),
                    protocol: "HTTP".to_string(),
                    success: false,
                    message: Some(format!("Failed to parse response: {}", e)),
                },
            }
        }
        Err(e) => TestResult {
            name: "EXISTS Command".to_string(),
            protocol: "HTTP".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}