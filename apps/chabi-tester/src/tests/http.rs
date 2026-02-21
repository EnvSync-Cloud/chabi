use reqwest::Client;
use tracing::debug;

use crate::TestResult;

pub async fn run_tests(
    host: &str,
    port: u16,
) -> Result<Vec<TestResult>, Box<dyn std::error::Error>> {
    let client = Client::new();
    let base_url = format!("http://{}:{}", host, port);
    let mut results = Vec::new();

    // Test health check endpoint
    let health_result = test_health_check(&client, &base_url).await;
    results.push(health_result);

    // Test snapshot endpoint
    let snapshot_result = test_snapshot_endpoint(&client, &base_url).await;
    results.push(snapshot_result);

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

async fn test_snapshot_endpoint(client: &Client, base_url: &str) -> TestResult {
    debug!("Running snapshot endpoint test");
    let url = format!("{}/snapshot", base_url);
    match client.get(&url).send().await {
        Ok(response) => {
            if !response.status().is_success() {
                return TestResult {
                    name: "Snapshot Endpoint".to_string(),
                    protocol: "HTTP".to_string(),
                    success: false,
                    message: Some(format!("Unexpected status: {}", response.status())),
                };
            }
            match response.json::<serde_json::Value>().await {
                Ok(json) => {
                    let required_keys = [
                        "strings",
                        "lists",
                        "sets",
                        "hashes",
                        "expirations_epoch_secs",
                    ];
                    let missing: Vec<&str> = required_keys
                        .iter()
                        .filter(|k| !json.as_object().is_some_and(|o| o.contains_key(**k)))
                        .copied()
                        .collect();
                    if missing.is_empty() {
                        TestResult {
                            name: "Snapshot Endpoint".to_string(),
                            protocol: "HTTP".to_string(),
                            success: true,
                            message: None,
                        }
                    } else {
                        TestResult {
                            name: "Snapshot Endpoint".to_string(),
                            protocol: "HTTP".to_string(),
                            success: false,
                            message: Some(format!("Missing keys: {:?}", missing)),
                        }
                    }
                }
                Err(e) => TestResult {
                    name: "Snapshot Endpoint".to_string(),
                    protocol: "HTTP".to_string(),
                    success: false,
                    message: Some(format!("Failed to parse JSON: {}", e)),
                },
            }
        }
        Err(e) => TestResult {
            name: "Snapshot Endpoint".to_string(),
            protocol: "HTTP".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}
