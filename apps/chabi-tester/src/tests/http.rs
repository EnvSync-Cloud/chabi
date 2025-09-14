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
