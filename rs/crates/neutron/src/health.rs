//! Health check utilities.
//!
//! Provides liveness and readiness endpoints with configurable checks.
//!
//! # Example
//!
//! ```rust,ignore
//! use neutron::prelude::*;
//! use neutron::health::HealthCheck;
//! use std::time::Duration;
//!
//! let health = HealthCheck::new()
//!     .check("database", || async {
//!         // check DB connection
//!         Ok(())
//!     })
//!     .check("redis", || async {
//!         // check Redis connection
//!         Ok(())
//!     })
//!     .timeout(Duration::from_secs(5));
//!
//! let router = Router::new()
//!     .get("/healthz", health.liveness())
//!     .get("/readyz", health.readiness());
//! ```
//!
//! ## Response Format
//!
//! ```json
//! {
//!   "status": "healthy",
//!   "checks": {
//!     "database": { "status": "pass", "duration_ms": 2 },
//!     "redis": { "status": "pass", "duration_ms": 1 }
//!   }
//! }
//! ```
//!
//! On failure:
//!
//! ```json
//! {
//!   "status": "unhealthy",
//!   "checks": {
//!     "database": { "status": "fail", "error": "connection refused", "duration_ms": 5000 }
//!   }
//! }
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use http::StatusCode;
use serde::Serialize;

use crate::handler::{Body, Response};

// ---------------------------------------------------------------------------
// Check function type
// ---------------------------------------------------------------------------

type CheckFn =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync>;

struct NamedCheck {
    name: String,
    check: CheckFn,
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    checks: Option<std::collections::HashMap<String, CheckResult>>,
}

#[derive(Serialize)]
struct CheckResult {
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    duration_ms: u64,
}

// ---------------------------------------------------------------------------
// HealthCheck
// ---------------------------------------------------------------------------

/// Health check builder.
///
/// Register named checks and use [`liveness()`](HealthCheck::liveness) or
/// [`readiness()`](HealthCheck::readiness) to get handler functions.
pub struct HealthCheck {
    checks: Vec<NamedCheck>,
    timeout: Duration,
}

impl HealthCheck {
    /// Create a new health check builder with no checks.
    pub fn new() -> Self {
        Self {
            checks: Vec::new(),
            timeout: Duration::from_secs(10),
        }
    }

    /// Add a named health check.
    ///
    /// The check function should return `Ok(())` if healthy, or
    /// `Err(message)` if unhealthy.
    pub fn check<F, Fut>(mut self, name: &str, f: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        let name = name.to_string();
        self.checks.push(NamedCheck {
            name,
            check: Arc::new(move || Box::pin(f())),
        });
        self
    }

    /// Set the timeout per check (default: 10 seconds).
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Get a liveness endpoint handler (`/healthz`).
    ///
    /// Always returns 200 OK — indicates the process is alive.
    /// Does not run custom checks.
    pub fn liveness(
        &self,
    ) -> impl Fn() -> Pin<Box<dyn Future<Output = Response> + Send>>
           + Clone
           + Send
           + Sync
           + 'static {
        || {
            Box::pin(async {
                let body = serde_json::to_vec(&HealthResponse {
                    status: "alive",
                    checks: None,
                })
                .unwrap();

                http::Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/json")
                    .body(Body::full(body))
                    .unwrap()
            })
        }
    }

    /// Get a readiness endpoint handler (`/readyz`).
    ///
    /// Runs all registered checks. Returns 200 if all pass, 503 if any fail.
    pub fn readiness(
        &self,
    ) -> impl Fn() -> Pin<Box<dyn Future<Output = Response> + Send>>
           + Clone
           + Send
           + Sync
           + 'static {
        let checks: Vec<(String, CheckFn)> = self
            .checks
            .iter()
            .map(|c| (c.name.clone(), Arc::clone(&c.check)))
            .collect();
        let timeout = self.timeout;

        move || {
            let checks = checks.clone();
            let timeout = timeout;
            Box::pin(async move {
                let mut results = std::collections::HashMap::new();
                let mut all_pass = true;

                for (name, check_fn) in &checks {
                    let start = Instant::now();
                    let result =
                        tokio::time::timeout(timeout, check_fn()).await;
                    let duration_ms = start.elapsed().as_millis() as u64;

                    match result {
                        Ok(Ok(())) => {
                            results.insert(
                                name.clone(),
                                CheckResult {
                                    status: "pass",
                                    error: None,
                                    duration_ms,
                                },
                            );
                        }
                        Ok(Err(e)) => {
                            all_pass = false;
                            results.insert(
                                name.clone(),
                                CheckResult {
                                    status: "fail",
                                    error: Some(e),
                                    duration_ms,
                                },
                            );
                        }
                        Err(_) => {
                            all_pass = false;
                            results.insert(
                                name.clone(),
                                CheckResult {
                                    status: "fail",
                                    error: Some("check timed out".to_string()),
                                    duration_ms,
                                },
                            );
                        }
                    }
                }

                let status_text = if all_pass { "healthy" } else { "unhealthy" };
                let http_status = if all_pass {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                };

                let body = serde_json::to_vec(&HealthResponse {
                    status: status_text,
                    checks: if results.is_empty() {
                        None
                    } else {
                        Some(results)
                    },
                })
                .unwrap();

                http::Response::builder()
                    .status(http_status)
                    .header("content-type", "application/json")
                    .body(Body::full(body))
                    .unwrap()
            })
        }
    }
}

impl Default for HealthCheck {
    fn default() -> Self {
        Self::new()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::Router;
    use crate::testing::TestClient;

    #[tokio::test]
    async fn liveness_returns_200() {
        let health = HealthCheck::new();
        let client = TestClient::new(
            Router::new().get("/healthz", health.liveness()),
        );

        let resp = client.get("/healthz").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.header("content-type").unwrap(), "application/json");

        let body = resp.text().await;
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["status"], "alive");
    }

    #[tokio::test]
    async fn liveness_always_200_regardless_of_checks() {
        let health = HealthCheck::new().check("failing", || async {
            Err("down".to_string())
        });

        let client = TestClient::new(
            Router::new().get("/healthz", health.liveness()),
        );

        // Liveness doesn't run checks — always 200
        let resp = client.get("/healthz").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readiness_all_pass_returns_200() {
        let health = HealthCheck::new()
            .check("db", || async { Ok(()) })
            .check("cache", || async { Ok(()) });

        let client = TestClient::new(
            Router::new().get("/readyz", health.readiness()),
        );

        let resp = client.get("/readyz").send().await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body = resp.text().await;
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["status"], "healthy");
        assert_eq!(parsed["checks"]["db"]["status"], "pass");
        assert_eq!(parsed["checks"]["cache"]["status"], "pass");
    }

    #[tokio::test]
    async fn readiness_one_fails_returns_503() {
        let health = HealthCheck::new()
            .check("db", || async { Ok(()) })
            .check("cache", || async {
                Err("connection refused".to_string())
            });

        let client = TestClient::new(
            Router::new().get("/readyz", health.readiness()),
        );

        let resp = client.get("/readyz").send().await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = resp.text().await;
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["status"], "unhealthy");
        assert_eq!(parsed["checks"]["db"]["status"], "pass");
        assert_eq!(parsed["checks"]["cache"]["status"], "fail");
        assert_eq!(parsed["checks"]["cache"]["error"], "connection refused");
    }

    #[tokio::test]
    async fn readiness_no_checks_returns_200() {
        let health = HealthCheck::new();
        let client = TestClient::new(
            Router::new().get("/readyz", health.readiness()),
        );

        let resp = client.get("/readyz").send().await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body = resp.text().await;
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["status"], "healthy");
    }

    #[tokio::test]
    async fn readiness_includes_duration() {
        let health = HealthCheck::new().check("slow", || async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(())
        });

        let client = TestClient::new(
            Router::new().get("/readyz", health.readiness()),
        );

        let resp = client.get("/readyz").send().await;
        let body = resp.text().await;
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();

        let duration = parsed["checks"]["slow"]["duration_ms"].as_u64().unwrap();
        assert!(duration >= 10);
    }

    #[tokio::test]
    async fn readiness_check_timeout() {
        let health = HealthCheck::new()
            .timeout(Duration::from_millis(50))
            .check("stuck", || async {
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok(())
            });

        let client = TestClient::new(
            Router::new().get("/readyz", health.readiness()),
        );

        let resp = client.get("/readyz").send().await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = resp.text().await;
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["checks"]["stuck"]["status"], "fail");
        assert_eq!(parsed["checks"]["stuck"]["error"], "check timed out");
    }

    #[tokio::test]
    async fn both_endpoints_on_same_router() {
        let health = HealthCheck::new()
            .check("db", || async { Ok(()) });

        let client = TestClient::new(
            Router::new()
                .get("/healthz", health.liveness())
                .get("/readyz", health.readiness()),
        );

        let liveness = client.get("/healthz").send().await;
        assert_eq!(liveness.status(), StatusCode::OK);

        let readiness = client.get("/readyz").send().await;
        assert_eq!(readiness.status(), StatusCode::OK);
    }
}
