//! Structured request/response logging middleware.
//!
//! Logs every request with method, path, status, and duration via `tracing`.
//! Log level varies by status: 5xx = ERROR, 4xx = WARN, 2xx/3xx = INFO.
//!
//! ```rust,ignore
//! Router::new().middleware(Logger::new())
//! ```

use std::future::Future;
use std::pin::Pin;
use std::time::Instant;

use crate::handler::{Request, Response};
use crate::middleware::{MiddlewareTrait, Next};

/// Built-in request/response logging middleware.
///
/// Logs every request with method, path, status code, and duration using
/// the `tracing` crate. Log level is based on response status:
///
/// - 5xx → `ERROR`
/// - 4xx → `WARN`
/// - 2xx/3xx → `INFO`
///
/// # Example
///
/// ```rust,ignore
/// use neutron::prelude::*;
///
/// let router = Router::new()
///     .middleware(Logger::new())
///     .get("/", || async { "hello" });
/// ```
pub struct Logger;

impl Logger {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Logger {
    fn default() -> Self {
        Self::new()
    }
}

impl MiddlewareTrait for Logger {
    fn call(&self, req: Request, next: Next) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let method = req.method().clone();
        let path = req.uri().path().to_string();
        let start = Instant::now();

        Box::pin(async move {
            let resp = next.run(req).await;
            let elapsed = start.elapsed();
            let status = resp.status().as_u16();
            let duration_ms = elapsed.as_secs_f64() * 1000.0;

            if status >= 500 {
                tracing::error!(
                    %method,
                    %path,
                    status,
                    duration_ms = format_args!("{duration_ms:.2}"),
                    "{method} {path} -> {status} ({duration_ms:.2}ms)",
                );
            } else if status >= 400 {
                tracing::warn!(
                    %method,
                    %path,
                    status,
                    duration_ms = format_args!("{duration_ms:.2}"),
                    "{method} {path} -> {status} ({duration_ms:.2}ms)",
                );
            } else {
                tracing::info!(
                    %method,
                    %path,
                    status,
                    duration_ms = format_args!("{duration_ms:.2}"),
                    "{method} {path} -> {status} ({duration_ms:.2}ms)",
                );
            }

            resp
        })
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
    use http::StatusCode;

    #[tokio::test]
    async fn logger_does_not_alter_response() {
        let client = TestClient::new(
            Router::new()
                .middleware(Logger::new())
                .get("/", || async { "hello" }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "hello");
    }

    #[tokio::test]
    async fn logger_does_not_alter_status() {
        let client = TestClient::new(
            Router::new()
                .middleware(Logger::new())
                .get("/", || async { (StatusCode::CREATED, "created") }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn logger_works_with_404() {
        let client = TestClient::new(
            Router::new()
                .middleware(Logger::new())
                .get("/", || async { "root" }),
        );

        let resp = client.get("/missing").send().await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn logger_works_with_other_middleware() {
        use http::HeaderValue;

        async fn add_header(req: Request, next: Next) -> Response {
            let mut resp = next.run(req).await;
            resp.headers_mut()
                .insert("x-test", HeaderValue::from_static("yes"));
            resp
        }

        let client = TestClient::new(
            Router::new()
                .middleware(Logger::new())
                .middleware(add_header)
                .get("/", || async { "ok" }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.header("x-test").unwrap(), "yes");
    }
}
