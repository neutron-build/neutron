//! P2.4 — Cross-SDK contract conformance suite (Rust slice).
//!
//! Asserts the `FRAMEWORK_CONTRACT.md` invariants against a live Neutron app
//! over a real TCP connection (`TestServer`, HTTP/1). These same clauses are the
//! ones every language SDK must satisfy, so this file is the Rust reference.
//!
//! Clauses covered:
//!   - `GET /health` returns the three-key contract shape (P0.4).
//!   - Every built-in error class is RFC 7807 `application/problem+json` (P2.1).
//!   - 405 carries a correct `Allow` header (P0.3).
//!   - `default_stack` installs the contract middleware order (P1.4).
//!   - Graceful shutdown drains an in-flight request (P2.4 shutdown harness).

use std::time::Duration;

use neutron::health::HealthCheck;
use neutron::prelude::*;
use neutron::testing::TestServer;

/// Build the app under test: a contract `/health`, a couple of routes, and the
/// default middleware stack so the conformance assertions see the real wiring.
fn conformance_app() -> Router {
    let health = HealthCheck::new();
    Router::new()
        .default_stack(Duration::from_secs(30))
        .get("/health", health.contract(None, "9.9.9"))
        .get("/users/:id", |Path(id): Path<u64>| async move {
            Json(serde_json::json!({ "id": id }))
        })
        .post("/users", |Json(v): Json<serde_json::Value>| async move {
            Json(v)
        })
}

// ---------------------------------------------------------------------------
// /health shape (P0.4)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn health_returns_contract_shape() {
    let server = TestServer::start(conformance_app()).await;
    let resp = server.client().get("/health").send().await;

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await;

    // Exactly the three contract keys, no more.
    let obj = body.as_object().expect("health body is a JSON object");
    assert!(obj.contains_key("status"), "missing `status`");
    assert!(obj.contains_key("nucleus"), "missing `nucleus`");
    assert!(obj.contains_key("version"), "missing `version`");
    assert_eq!(
        obj.len(),
        3,
        "health body must have exactly 3 keys: {obj:?}"
    );

    assert_eq!(body["status"], "ok");
    assert_eq!(body["nucleus"], "unconfigured");
    assert_eq!(body["version"], "9.9.9");
}

// ---------------------------------------------------------------------------
// RFC 7807 on every built-in error class (P2.1)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn not_found_is_problem_json() {
    let server = TestServer::start(conformance_app()).await;
    let resp = server.client().get("/does-not-exist").send().await;

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        resp.header("content-type").unwrap(),
        "application/problem+json"
    );
    let body: serde_json::Value = resp.json().await;
    assert_eq!(body["status"], 404);
    assert!(body["title"].is_string());
    // `instance` carries the request path.
    assert_eq!(body["instance"], "/does-not-exist");
}

#[tokio::test]
async fn method_not_allowed_is_problem_json_with_allow() {
    let server = TestServer::start(conformance_app()).await;
    // /users only has POST; a GET there is a 405 (the :id route is a different path).
    let resp = server
        .client()
        .request(Method::DELETE, "/users")
        .send()
        .await;

    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        resp.header("content-type").unwrap(),
        "application/problem+json"
    );
    // P0.3: 405 MUST carry an Allow header listing the supported methods.
    let allow = resp.header("allow").expect("405 must set Allow");
    assert!(allow.contains("POST"), "Allow header was {allow:?}");
}

#[tokio::test]
async fn bad_json_is_problem_json() {
    let server = TestServer::start(conformance_app()).await;
    let resp = server
        .client()
        .post("/users")
        .header("content-type", "application/json")
        .body("{ not valid json")
        .send()
        .await;

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        resp.header("content-type").unwrap(),
        "application/problem+json"
    );
}

// Note: the 413 (payload-too-large) problem+json path is enforced by the
// production accept loop's Content-Length pre-check and the streaming per-frame
// cap, exercised in `tests/integration.rs` against a body-limit-configured
// server (the `TestServer` harness does not impose a global cap).

// ---------------------------------------------------------------------------
// default_stack middleware order markers (P1.4)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn default_stack_installs_request_id() {
    let server = TestServer::start(conformance_app()).await;
    let resp = server.client().get("/health").send().await;
    // RequestId is the first (outermost) contract layer; its header is always set.
    assert!(
        resp.header("x-request-id").is_some(),
        "default_stack must install RequestId"
    );
}

// ---------------------------------------------------------------------------
// Graceful shutdown drains an in-flight request (P2.4 shutdown harness)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn graceful_shutdown_drains_in_flight_request() {
    use std::sync::Arc;
    use tokio::sync::Notify;

    // A handler that blocks until the test releases it, so we can hold a request
    // open across the shutdown signal.
    let release = Arc::new(Notify::new());
    let release_h = Arc::clone(&release);

    let app = Router::new().get("/slow", move || {
        let release = Arc::clone(&release_h);
        async move {
            release.notified().await;
            "drained"
        }
    });

    let server = TestServer::start(app).await;
    let client = server.client();

    // Fire the slow request; it parks in the handler.
    let in_flight = tokio::spawn(async move { client.get("/slow").send().await });
    // Give it a moment to reach the handler.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Trigger shutdown by dropping the server (sends the stop signal), then
    // release the handler. A correctly draining server completes the in-flight
    // request rather than dropping the connection.
    let drop_task = tokio::spawn(async move {
        drop(server);
    });
    release.notify_one();
    drop_task.await.unwrap();

    let resp = in_flight.await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.text().await, "drained");
}
