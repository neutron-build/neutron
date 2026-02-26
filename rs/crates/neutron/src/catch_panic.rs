//! Panic recovery middleware.
//!
//! Catches panics in downstream handlers and middleware, returning a
//! `500 Internal Server Error` response instead of dropping the connection.
//!
//! ```rust,ignore
//! Router::new().middleware(CatchPanic::new())
//! ```

use std::future::Future;
use std::pin::Pin;

use http::StatusCode;

use crate::handler::{Body, Request, Response};
use crate::middleware::{MiddlewareTrait, Next};

/// Middleware that catches panics and returns 500 Internal Server Error.
///
/// Without this middleware, a panic in a handler will abort the task and
/// silently drop the connection. `CatchPanic` wraps the downstream chain
/// in [`std::panic::AssertUnwindSafe`] + [`futures::FutureExt::catch_unwind`]
/// so the server stays healthy.
///
/// # Example
///
/// ```rust,ignore
/// use neutron::prelude::*;
///
/// let router = Router::new()
///     .middleware(CatchPanic::new())
///     .get("/", handler);
/// ```
#[derive(Clone, Copy)]
pub struct CatchPanic {
    _priv: (),
}

impl CatchPanic {
    /// Create a new panic recovery middleware.
    pub fn new() -> Self {
        Self { _priv: () }
    }
}

impl Default for CatchPanic {
    fn default() -> Self {
        Self::new()
    }
}

impl MiddlewareTrait for CatchPanic {
    fn call(&self, req: Request, next: Next) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        Box::pin(async move {
            let fut = std::panic::AssertUnwindSafe(next.run(req));

            match tokio::task::spawn(fut).await {
                // Handler completed normally
                Ok(response) => response,
                // Task panicked or was cancelled
                Err(join_err) => {
                    if join_err.is_panic() {
                        let panic_info = join_err.into_panic();

                        // Extract a useful message from the panic payload
                        let msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                            (*s).to_string()
                        } else if let Some(s) = panic_info.downcast_ref::<String>() {
                            s.clone()
                        } else {
                            "unknown panic".to_string()
                        };

                        tracing::error!(panic = %msg, "handler panicked");

                        http::Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .header("content-type", "application/json; charset=utf-8")
                            .body(Body::full(
                                serde_json::json!({ "error": "Internal Server Error" })
                                    .to_string(),
                            ))
                            .unwrap()
                    } else {
                        // Task was cancelled (unlikely in practice)
                        tracing::error!("handler task cancelled");

                        http::Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .header("content-type", "application/json; charset=utf-8")
                            .body(Body::full(
                                serde_json::json!({ "error": "Internal Server Error" })
                                    .to_string(),
                            ))
                            .unwrap()
                    }
                }
            }
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

    #[tokio::test]
    async fn normal_request_unaffected() {
        let client = TestClient::new(
            Router::new()
                .middleware(CatchPanic::new())
                .get("/", || async { "hello" }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "hello");
    }

    #[tokio::test]
    async fn catches_panic_returns_500() {
        let client = TestClient::new(
            Router::new()
                .middleware(CatchPanic::new())
                .get("/boom", || async {
                    panic!("handler exploded");
                    #[allow(unreachable_code)]
                    "never"
                }),
        );

        let resp = client.get("/boom").send().await;
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let body: serde_json::Value = resp.json().await;
        assert_eq!(body["error"], "Internal Server Error");
    }

    #[tokio::test]
    async fn catches_string_panic() {
        let client = TestClient::new(
            Router::new()
                .middleware(CatchPanic::new())
                .get("/boom", || async {
                    panic!("{}", "formatted panic message");
                    #[allow(unreachable_code)]
                    "never"
                }),
        );

        let resp = client.get("/boom").send().await;
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn other_routes_still_work_after_panic() {
        let client = TestClient::new(
            Router::new()
                .middleware(CatchPanic::new())
                .get("/ok", || async { "fine" })
                .get("/boom", || async {
                    panic!("oops");
                    #[allow(unreachable_code)]
                    "never"
                }),
        );

        // Panic route
        let resp = client.get("/boom").send().await;
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);

        // Normal route still works
        let resp = client.get("/ok").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "fine");
    }

    #[tokio::test]
    async fn works_with_other_middleware() {
        use crate::request_id::RequestId;

        let client = TestClient::new(
            Router::new()
                .middleware(CatchPanic::new())
                .middleware(RequestId::new())
                .get("/", || async { "ok" }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.header("x-request-id").is_some());
    }

    #[tokio::test]
    async fn json_handler_panic() {
        use crate::handler::Json;

        let client = TestClient::new(
            Router::new()
                .middleware(CatchPanic::new())
                .get("/data", || async {
                    if true {
                        panic!("json handler panic");
                    }
                    #[allow(unreachable_code)]
                    Json(serde_json::json!({"ok": true}))
                }),
        );

        let resp = client.get("/data").send().await;
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
