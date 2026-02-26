//! Testing utilities for Neutron applications.
//!
//! Two complementary testing strategies:
//!
//! ## In-memory: `TestClient`
//!
//! Executes requests through the full middleware → router → handler pipeline
//! **without** starting a TCP server.  Zero network overhead, ideal for unit
//! tests of routing, extractors, and middleware.
//!
//! ## Real TCP: `TestServer`
//!
//! Starts a real Neutron server bound to a random OS-assigned port.  Requests
//! travel over a real TCP connection and through the full hyper HTTP stack,
//! enabling integration tests of WebSocket upgrades, HTTP/2, TLS, compression,
//! and any other protocol-level behaviour.
//!
//! ```rust,ignore
//! use neutron::prelude::*;
//! use neutron::testing::{TestClient, TestServer};
//!
//! // ── In-memory ────────────────────────────────────────────────────────────
//! let client = TestClient::new(Router::new().get("/", || async { "hello" }));
//! let resp = client.get("/").send().await;
//! assert_eq!(resp.text().await, "hello");
//!
//! // ── Real TCP ─────────────────────────────────────────────────────────────
//! let server = TestServer::start(Router::new().get("/", || async { "hello" })).await;
//! let client = server.client();
//! let resp = client.get("/").send().await;
//! assert_eq!(resp.text().await, "hello");
//!
//! // Server shuts down when dropped.
//! drop(server);
//! ```

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as AutoBuilder;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::app::{build_dispatch, DispatchChain};
use crate::handler::{Body, Request as NeutronRequest, Response, StateMap};
use crate::router::Router;

/// Build a dispatch chain and state map from a router.
fn make_chain(mut router: Router) -> (DispatchChain, Arc<StateMap>) {
    router.ensure_built();
    let state_map = Arc::new(
        router
            .state_map
            .iter()
            .map(|(k, v)| (*k, Arc::clone(v)))
            .collect::<StateMap>(),
    );
    let chain = build_dispatch(Arc::new(router));
    (chain, state_map)
}

/// Test client that sends requests through the Neutron dispatch chain
/// without a TCP server.
pub struct TestClient {
    chain: DispatchChain,
    state_map: Arc<StateMap>,
}

impl TestClient {
    /// Create a test client from a router.
    ///
    /// This builds the same middleware → router → handler chain that
    /// [`Neutron::serve`] uses, so tests exercise the full pipeline.
    pub fn new(router: Router) -> Self {
        let (chain, state_map) = make_chain(router);
        Self { chain, state_map }
    }

    /// Start building a GET request.
    pub fn get(&self, path: &str) -> TestRequest<'_> {
        TestRequest::new(self, Method::GET, path)
    }

    /// Start building a POST request.
    pub fn post(&self, path: &str) -> TestRequest<'_> {
        TestRequest::new(self, Method::POST, path)
    }

    /// Start building a PUT request.
    pub fn put(&self, path: &str) -> TestRequest<'_> {
        TestRequest::new(self, Method::PUT, path)
    }

    /// Start building a DELETE request.
    pub fn delete(&self, path: &str) -> TestRequest<'_> {
        TestRequest::new(self, Method::DELETE, path)
    }

    /// Start building a PATCH request.
    pub fn patch(&self, path: &str) -> TestRequest<'_> {
        TestRequest::new(self, Method::PATCH, path)
    }

    /// Start building a request with an arbitrary method.
    pub fn request(&self, method: Method, path: &str) -> TestRequest<'_> {
        TestRequest::new(self, method, path)
    }
}

/// A request being built for the test client.
///
/// Use the builder methods to set headers, body, and then call [`send`](Self::send)
/// to execute the request.
pub struct TestRequest<'a> {
    client: &'a TestClient,
    method: Method,
    uri: String,
    headers: HeaderMap,
    body: Bytes,
}

impl<'a> TestRequest<'a> {
    fn new(client: &'a TestClient, method: Method, path: &str) -> Self {
        Self {
            client,
            method,
            uri: path.to_string(),
            headers: HeaderMap::new(),
            body: Bytes::new(),
        }
    }

    /// Add a header to the request.
    pub fn header(mut self, key: &str, value: &str) -> Self {
        self.headers.insert(
            http::header::HeaderName::from_bytes(key.as_bytes()).expect("invalid header name"),
            HeaderValue::from_str(value).expect("invalid header value"),
        );
        self
    }

    /// Set a JSON body (also sets `Content-Type: application/json`).
    pub fn json<T: Serialize>(mut self, body: &T) -> Self {
        self.body = Bytes::from(serde_json::to_vec(body).expect("JSON serialization failed"));
        self.headers.insert(
            "content-type",
            HeaderValue::from_static("application/json"),
        );
        self
    }

    /// Set a raw body.
    pub fn body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = body.into();
        self
    }

    /// Execute the request and return the response.
    pub async fn send(self) -> TestResponse {
        let req = NeutronRequest::with_state(
            self.method,
            self.uri.parse().expect("invalid URI"),
            self.headers,
            self.body,
            Arc::clone(&self.client.state_map),
        );
        let resp = (self.client.chain)(req).await;
        TestResponse { inner: resp }
    }
}

/// Response from a test request.
///
/// Provides convenience methods for inspecting status, headers, and body.
pub struct TestResponse {
    inner: Response,
}

impl TestResponse {
    /// Get the response status code.
    pub fn status(&self) -> StatusCode {
        self.inner.status()
    }

    /// Get a reference to the response headers.
    pub fn headers(&self) -> &HeaderMap {
        self.inner.headers()
    }

    /// Get a specific header value.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.inner
            .headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
    }

    /// Consume the response and return the body as a UTF-8 string.
    pub async fn text(self) -> String {
        let bytes = self.bytes().await;
        String::from_utf8(bytes.to_vec()).expect("response body is not valid UTF-8")
    }

    /// Consume the response and deserialize the body as JSON.
    pub async fn json<T: DeserializeOwned>(self) -> T {
        let bytes = self.bytes().await;
        serde_json::from_slice(&bytes).expect("failed to deserialize response body as JSON")
    }

    /// Consume the response and return the raw body bytes.
    pub async fn bytes(self) -> Bytes {
        self.inner
            .into_body()
            .collect()
            .await
            .expect("failed to collect response body")
            .to_bytes()
    }

    /// Convert into the underlying `http::Response`.
    pub fn into_response(self) -> Response {
        self.inner
    }
}

// ===========================================================================
// TestServer — real TCP integration test server
// ===========================================================================

/// A real TCP server bound to a random OS-assigned port for integration tests.
///
/// Starts the full Neutron accept loop (hyper HTTP/1 + HTTP/2, WebSocket
/// upgrades, middleware, etc.) on `127.0.0.1:0`.  The OS picks an available
/// port; call [`addr`](TestServer::addr) or [`url`](TestServer::url) to
/// discover it.
///
/// The server shuts down automatically when the `TestServer` is dropped.
///
/// # Example
///
/// ```rust,ignore
/// let server = TestServer::start(Router::new().get("/ping", || async { "pong" })).await;
/// let resp   = server.client().get("/ping").send().await;
/// assert_eq!(resp.status(), StatusCode::OK);
/// assert_eq!(resp.text().await, "pong");
/// ```
pub struct TestServer {
    addr:     SocketAddr,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl TestServer {
    /// Start a test server backed by the given router.
    ///
    /// Binds `127.0.0.1:0` (random port), spawns the accept loop, and returns
    /// immediately.  Use [`client`](TestServer::client) to make requests.
    pub async fn start(router: Router) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("TestServer: failed to bind");

        let addr = listener.local_addr().expect("TestServer: no local addr");
        let (chain, state_map) = make_chain(router);
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        tokio::spawn(test_server_accept_loop(listener, chain, state_map, rx));

        Self { addr, shutdown: Some(tx) }
    }

    /// The address the server is listening on.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Build a full URL for the given path, e.g. `http://127.0.0.1:12345/foo`.
    pub fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.addr, path)
    }

    /// Create a [`TestServerClient`] that sends real HTTP requests to this server.
    pub fn client(&self) -> TestServerClient {
        TestServerClient { addr: self.addr }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

// Accept loop for the test server.
async fn test_server_accept_loop(
    listener:   tokio::net::TcpListener,
    chain:      DispatchChain,
    state_map:  Arc<StateMap>,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, remote_addr) = match result {
                    Ok(conn) => conn,
                    Err(_)   => break,
                };
                let chain     = Arc::clone(&chain);
                let state_map = Arc::clone(&state_map);
                tokio::spawn(serve_test_conn(stream, remote_addr, chain, state_map));
            }
            _ = &mut shutdown => break,
        }
    }
}

// Serve a single hyper connection for the test server.
async fn serve_test_conn(
    stream:      tokio::net::TcpStream,
    remote_addr: SocketAddr,
    chain:       DispatchChain,
    state_map:   Arc<StateMap>,
) {
    let service = service_fn(move |mut req: http::Request<Incoming>| {
        let chain = Arc::clone(&chain);
        let state = Arc::clone(&state_map);
        async move {
            let on_upgrade  = hyper::upgrade::on(&mut req);
            let (parts, body) = req.into_parts();
            let body_bytes  = match body.collect().await {
                Ok(c)  => c.to_bytes(),
                Err(_) => {
                    return Ok::<_, std::convert::Infallible>(
                        http::Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::full("Failed to read body"))
                            .unwrap(),
                    );
                }
            };

            let mut neutron_req = NeutronRequest::with_state(
                parts.method,
                parts.uri,
                parts.headers,
                body_bytes,
                state,
            );
            neutron_req.set_on_upgrade(on_upgrade);
            neutron_req.set_remote_addr(remote_addr);

            let response = chain(neutron_req).await;
            Ok::<_, std::convert::Infallible>(response)
        }
    });

    let builder = AutoBuilder::new(TokioExecutor::new());
    let conn    = builder.serve_connection_with_upgrades(TokioIo::new(stream), service);
    let _ = conn.await;
}

// ===========================================================================
// TestServerClient — HTTP client for TestServer
// ===========================================================================

/// HTTP client for [`TestServer`].
///
/// Makes real TCP connections to the test server.  Has the same builder API
/// as [`TestClient`] so tests can be switched between in-memory and TCP with
/// minimal changes.
#[derive(Clone)]
pub struct TestServerClient {
    addr: SocketAddr,
}

impl TestServerClient {
    /// Start a GET request.
    pub fn get(&self, path: &str) -> TestServerRequest<'_> {
        TestServerRequest::new(self, Method::GET, path)
    }

    /// Start a POST request.
    pub fn post(&self, path: &str) -> TestServerRequest<'_> {
        TestServerRequest::new(self, Method::POST, path)
    }

    /// Start a PUT request.
    pub fn put(&self, path: &str) -> TestServerRequest<'_> {
        TestServerRequest::new(self, Method::PUT, path)
    }

    /// Start a DELETE request.
    pub fn delete(&self, path: &str) -> TestServerRequest<'_> {
        TestServerRequest::new(self, Method::DELETE, path)
    }

    /// Start a PATCH request.
    pub fn patch(&self, path: &str) -> TestServerRequest<'_> {
        TestServerRequest::new(self, Method::PATCH, path)
    }

    /// Start a request with an arbitrary method.
    pub fn request(&self, method: Method, path: &str) -> TestServerRequest<'_> {
        TestServerRequest::new(self, method, path)
    }
}

/// A request being built for [`TestServerClient`].
pub struct TestServerRequest<'a> {
    client:  &'a TestServerClient,
    method:  Method,
    path:    String,
    headers: HeaderMap,
    body:    Bytes,
}

impl<'a> TestServerRequest<'a> {
    fn new(client: &'a TestServerClient, method: Method, path: &str) -> Self {
        Self {
            client,
            method,
            path: path.to_string(),
            headers: HeaderMap::new(),
            body: Bytes::new(),
        }
    }

    /// Add a header.
    pub fn header(mut self, key: &str, value: &str) -> Self {
        self.headers.insert(
            http::header::HeaderName::from_bytes(key.as_bytes()).expect("invalid header name"),
            HeaderValue::from_str(value).expect("invalid header value"),
        );
        self
    }

    /// Set a JSON body (also sets `Content-Type: application/json`).
    pub fn json<T: Serialize>(mut self, body: &T) -> Self {
        self.body = Bytes::from(serde_json::to_vec(body).expect("JSON serialization failed"));
        self.headers.insert(
            "content-type",
            HeaderValue::from_static("application/json"),
        );
        self
    }

    /// Set a raw body.
    pub fn body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = body.into();
        self
    }

    /// Execute the request over a real TCP connection.
    pub async fn send(self) -> TestResponse {
        let addr = self.client.addr;

        // Build the HTTP/1.1 request.
        let mut builder = http::Request::builder()
            .method(self.method)
            .uri(&self.path)
            .header("host", addr.to_string());

        for (name, value) in &self.headers {
            builder = builder.header(name, value);
        }

        if !self.body.is_empty() {
            builder = builder.header("content-length", self.body.len().to_string());
        }

        let req = builder
            .body(Full::new(self.body))
            .expect("failed to build HTTP request");

        // Open TCP connection.
        let stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("TestServerClient: failed to connect");

        let io = TokioIo::new(stream);
        let (mut sender, conn) =
            hyper::client::conn::http1::Builder::new()
                .handshake(io)
                .await
                .expect("TestServerClient: HTTP/1 handshake failed");

        // Drive the connection in the background.
        tokio::spawn(conn);

        // Send the request and collect the response body.
        let response = sender
            .send_request(req)
            .await
            .expect("TestServerClient: send_request failed");

        let (parts, resp_body) = response.into_parts();
        let body_bytes = resp_body
            .collect()
            .await
            .expect("TestServerClient: failed to collect response body")
            .to_bytes();

        TestResponse {
            inner: http::Response::from_parts(parts, Body::full(body_bytes)),
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    // -----------------------------------------------------------------------
    // Basic routing
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn get_static_route() {
        let client = TestClient::new(Router::new().get("/", || async { "hello" }));

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "hello");
    }

    #[tokio::test]
    async fn post_route() {
        let client = TestClient::new(Router::new().post("/items", || async { "created" }));

        let resp = client.post("/items").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "created");
    }

    #[tokio::test]
    async fn not_found() {
        let client = TestClient::new(Router::new().get("/", || async { "root" }));

        let resp = client.get("/nope").send().await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn method_not_allowed() {
        let client = TestClient::new(Router::new().get("/items", || async { "items" }));

        let resp = client.post("/items").send().await;
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    // -----------------------------------------------------------------------
    // Path params
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn path_param_extraction() {
        use crate::extract::Path;
        use crate::handler::Json;

        let client = TestClient::new(
            Router::new().get("/users/:id", |Path(id): Path<u64>| async move {
                Json(serde_json::json!({ "id": id }))
            }),
        );

        let resp = client.get("/users/42").send().await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body: serde_json::Value = resp.json().await;
        assert_eq!(body["id"], 42);
    }

    // -----------------------------------------------------------------------
    // JSON body
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn json_request_body() {
        use crate::handler::Json;

        #[derive(Deserialize)]
        struct Input {
            name: String,
        }

        #[derive(Serialize, Deserialize)]
        struct Output {
            greeting: String,
        }

        let client = TestClient::new(
            Router::new().post("/greet", |Json(input): Json<Input>| async move {
                Json(Output {
                    greeting: format!("Hello, {}!", input.name),
                })
            }),
        );

        let resp = client
            .post("/greet")
            .json(&serde_json::json!({ "name": "Alice" }))
            .send()
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.header("content-type").unwrap(), "application/json");

        let body: Output = resp.json().await;
        assert_eq!(body.greeting, "Hello, Alice!");
    }

    // -----------------------------------------------------------------------
    // Headers
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn custom_request_headers() {
        let client = TestClient::new(
            Router::new().get(
                "/echo",
                |headers: HeaderMap| async move {
                    headers
                        .get("x-custom")
                        .map(|v| v.to_str().unwrap().to_string())
                        .unwrap_or_default()
                },
            ),
        );

        let resp = client
            .get("/echo")
            .header("x-custom", "test-value")
            .send()
            .await;

        assert_eq!(resp.text().await, "test-value");
    }

    #[tokio::test]
    async fn response_header_inspection() {
        use crate::handler::IntoResponse;

        let client = TestClient::new(Router::new().get("/", || async {
            let mut headers = HeaderMap::new();
            headers.insert("x-powered-by", HeaderValue::from_static("neutron"));
            (headers, "ok").into_response()
        }));

        let resp = client.get("/").send().await;
        assert_eq!(resp.header("x-powered-by").unwrap(), "neutron");
        assert_eq!(resp.text().await, "ok");
    }

    // -----------------------------------------------------------------------
    // Status code responses
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn status_tuple_response() {
        let client = TestClient::new(
            Router::new().post("/items", || async { (StatusCode::CREATED, "done") }),
        );

        let resp = client.post("/items").send().await;
        assert_eq!(resp.status(), StatusCode::CREATED);
        assert_eq!(resp.text().await, "done");
    }

    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn state_extraction() {
        use crate::extract::State;

        #[derive(Clone)]
        struct AppName(String);

        let client = TestClient::new(
            Router::new()
                .state(AppName("Neutron".into()))
                .get("/name", |State(name): State<AppName>| async move {
                    name.0
                }),
        );

        let resp = client.get("/name").send().await;
        assert_eq!(resp.text().await, "Neutron");
    }

    // -----------------------------------------------------------------------
    // Middleware
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn middleware_runs() {
        use crate::middleware::Next;

        async fn add_header(req: crate::handler::Request, next: Next) -> crate::handler::Response {
            let mut resp = next.run(req).await;
            resp.headers_mut()
                .insert("x-test", HeaderValue::from_static("middleware-ran"));
            resp
        }

        let client = TestClient::new(
            Router::new()
                .middleware(add_header)
                .get("/", || async { "ok" }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(resp.header("x-test").unwrap(), "middleware-ran");
        assert_eq!(resp.text().await, "ok");
    }

    // -----------------------------------------------------------------------
    // Fallback
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn fallback_handler() {
        use crate::handler::Json;

        let client = TestClient::new(
            Router::new()
                .get("/", || async { "root" })
                .fallback(|| async {
                    (
                        StatusCode::NOT_FOUND,
                        Json(serde_json::json!({ "error": "not found" })),
                    )
                }),
        );

        let resp = client.get("/missing").send().await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let body: serde_json::Value = resp.json().await;
        assert_eq!(body["error"], "not found");
    }

    // -----------------------------------------------------------------------
    // Nested routers
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn nested_router() {
        let api = Router::new()
            .get("/health", || async { "ok" })
            .get("/version", || async { "1.0" });

        let client = TestClient::new(
            Router::new()
                .get("/", || async { "root" })
                .nest("/api", api),
        );

        assert_eq!(client.get("/").send().await.text().await, "root");
        assert_eq!(client.get("/api/health").send().await.text().await, "ok");
        assert_eq!(client.get("/api/version").send().await.text().await, "1.0");
    }

    // -----------------------------------------------------------------------
    // CORS through TestClient
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn cors_preflight_through_client() {
        use crate::cors::Cors;

        let client = TestClient::new(
            Router::new()
                .middleware(Cors::new().allow_any_origin().allow_any_method().max_age(600))
                .get("/data", || async { "data" }),
        );

        // Preflight
        let resp = client
            .request(Method::OPTIONS, "/data")
            .header("origin", "http://example.com")
            .header("access-control-request-method", "GET")
            .send()
            .await;

        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert_eq!(resp.header("access-control-allow-origin").unwrap(), "*");
        assert_eq!(resp.header("access-control-max-age").unwrap(), "600");

        // Normal request
        let resp = client
            .get("/data")
            .header("origin", "http://example.com")
            .send()
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.header("access-control-allow-origin").unwrap(), "*");
        assert_eq!(resp.text().await, "data");
    }

    // -----------------------------------------------------------------------
    // Raw body
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn raw_body_request() {
        let client = TestClient::new(
            Router::new().post("/echo", |body: String| async move { body }),
        );

        let resp = client
            .post("/echo")
            .body("raw content")
            .send()
            .await;

        assert_eq!(resp.text().await, "raw content");
    }

    // -----------------------------------------------------------------------
    // Multiple requests reuse the client
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn client_reusable() {
        let client = TestClient::new(
            Router::new()
                .get("/a", || async { "a" })
                .get("/b", || async { "b" })
                .get("/c", || async { "c" }),
        );

        assert_eq!(client.get("/a").send().await.text().await, "a");
        assert_eq!(client.get("/b").send().await.text().await, "b");
        assert_eq!(client.get("/c").send().await.text().await, "c");
        // Reuse same route
        assert_eq!(client.get("/a").send().await.text().await, "a");
    }

    // -----------------------------------------------------------------------
    // Bytes response
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn bytes_response() {
        let client = TestClient::new(
            Router::new().get("/bin", || async { vec![0u8, 1, 2, 3] }),
        );

        let resp = client.get("/bin").send().await;
        assert_eq!(resp.bytes().await.as_ref(), &[0, 1, 2, 3]);
    }

    // -----------------------------------------------------------------------
    // All HTTP methods
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn all_methods() {
        let client = TestClient::new(
            Router::new()
                .get("/r", || async { "GET" })
                .post("/r", || async { "POST" })
                .put("/r", || async { "PUT" })
                .delete("/r", || async { "DELETE" })
                .patch("/r", || async { "PATCH" }),
        );

        assert_eq!(client.get("/r").send().await.text().await, "GET");
        assert_eq!(client.post("/r").send().await.text().await, "POST");
        assert_eq!(client.put("/r").send().await.text().await, "PUT");
        assert_eq!(client.delete("/r").send().await.text().await, "DELETE");
        assert_eq!(client.patch("/r").send().await.text().await, "PATCH");
    }

    // -----------------------------------------------------------------------
    // Redirect
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn redirect_response() {
        use crate::handler::Redirect;

        let client = TestClient::new(
            Router::new().get("/old", || async { Redirect::to("/new") }),
        );

        let resp = client.get("/old").send().await;
        assert_eq!(resp.status(), StatusCode::SEE_OTHER);
        assert_eq!(resp.header("location").unwrap(), "/new");
    }

    // -----------------------------------------------------------------------
    // TestServer — real TCP integration tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn tcp_simple_get() {
        let server = TestServer::start(
            Router::new().get("/ping", || async { "pong" }),
        )
        .await;

        let resp = server.client().get("/ping").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "pong");
    }

    #[tokio::test]
    async fn tcp_post_json_body() {
        use crate::handler::Json;
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct Input { value: u32 }

        let server = TestServer::start(
            Router::new().post(
                "/double",
                |Json(i): Json<Input>| async move {
                    Json(serde_json::json!({ "result": i.value * 2 }))
                },
            ),
        )
        .await;

        let resp = server
            .client()
            .post("/double")
            .json(&serde_json::json!({ "value": 21 }))
            .send()
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = resp.json().await;
        assert_eq!(body["result"], 42);
    }

    #[tokio::test]
    async fn tcp_path_params() {
        use crate::extract::Path;
        use crate::handler::Json;

        let server = TestServer::start(
            Router::new().get(
                "/users/:id",
                |Path(id): Path<u64>| async move {
                    Json(serde_json::json!({ "id": id }))
                },
            ),
        )
        .await;

        let resp = server.client().get("/users/99").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = resp.json().await;
        assert_eq!(body["id"], 99);
    }

    #[tokio::test]
    async fn tcp_state_injection() {
        use crate::extract::State;

        #[derive(Clone)]
        struct AppVersion(&'static str);

        let server = TestServer::start(
            Router::new()
                .state(AppVersion("1.2.3"))
                .get("/version", |State(v): State<AppVersion>| async move { v.0 }),
        )
        .await;

        let resp = server.client().get("/version").send().await;
        assert_eq!(resp.text().await, "1.2.3");
    }

    #[tokio::test]
    async fn tcp_middleware_runs() {
        use crate::middleware::Next;

        async fn stamp(req: NeutronRequest, next: Next) -> Response {
            let mut resp = next.run(req).await;
            resp.headers_mut()
                .insert("x-via", HeaderValue::from_static("tcp-test"));
            resp
        }

        let server = TestServer::start(
            Router::new()
                .middleware(stamp)
                .get("/", || async { "ok" }),
        )
        .await;

        let resp = server.client().get("/").send().await;
        assert_eq!(resp.header("x-via").unwrap(), "tcp-test");
        assert_eq!(resp.text().await, "ok");
    }

    #[tokio::test]
    async fn tcp_not_found() {
        let server = TestServer::start(Router::new().get("/exists", || async { "yes" })).await;

        let resp = server.client().get("/missing").send().await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn tcp_multiple_requests_reuse_client() {
        let server = TestServer::start(
            Router::new()
                .get("/a", || async { "a" })
                .get("/b", || async { "b" }),
        )
        .await;

        let client = server.client();
        assert_eq!(client.get("/a").send().await.text().await, "a");
        assert_eq!(client.get("/b").send().await.text().await, "b");
        assert_eq!(client.get("/a").send().await.text().await, "a");
    }

    #[tokio::test]
    async fn tcp_custom_request_header() {
        let server = TestServer::start(
            Router::new().get(
                "/echo",
                |headers: HeaderMap| async move {
                    headers
                        .get("x-token")
                        .map(|v| v.to_str().unwrap().to_string())
                        .unwrap_or_default()
                },
            ),
        )
        .await;

        let resp = server
            .client()
            .get("/echo")
            .header("x-token", "secret")
            .send()
            .await;

        assert_eq!(resp.text().await, "secret");
    }

    #[tokio::test]
    async fn tcp_server_url_helper() {
        let server = TestServer::start(Router::new().get("/", || async { "root" })).await;
        let url = server.url("/");
        assert!(url.starts_with("http://127.0.0.1:"));
        assert!(url.ends_with('/'));
    }

    #[tokio::test]
    async fn tcp_status_codes() {
        let server = TestServer::start(
            Router::new()
                .post("/created", || async { (StatusCode::CREATED, "done") }),
        )
        .await;

        let resp = server.client().post("/created").send().await;
        assert_eq!(resp.status(), StatusCode::CREATED);
        assert_eq!(resp.text().await, "done");
    }
}
