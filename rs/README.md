# Neutron Rust

This repository is the Neutron Rust implementation within the broader Neutron ecosystem.

A lightweight, high-performance async web framework for Rust, built on [hyper](https://hyper.rs/).

Neutron provides trie-based routing, type-safe extractors, composable middleware, and batteries-included features for building production APIs and real-time services.

## Features

| Category | Features |
|----------|----------|
| **Routing** | Trie-based router, path params (`:id`), wildcards (`*`), nested routers, all HTTP methods, `.any()`, `.route()` |
| **Extractors** | `Path<T>`, `Query<T>`, `Json<T>`, `Form<T>`, `State<T>`, `Extension<T>`, `ConnectInfo`, `HeaderMap` |
| **Middleware** | Logger, RequestId, Timeout, CORS, Helmet, RateLimiter, BodyLimit, Compress, CatchPanic |
| **Caching** | ResponseCache (in-memory with TTL), Deduplicate (in-flight dedup), ETag/304 on static files |
| **Resilience** | CircuitBreaker, RateLimiter, Timeout, CatchPanic |
| **Auth** | JWT (sign/verify/middleware), Cookie (plain/signed/private), Session (pluggable stores), CSRF |
| **Real-time** | WebSocket (upgrade, send/recv, ping/pong), SSE streaming, PubSub (in-memory topics) |
| **Data** | DataLoader (batching + caching), `join_all` / `try_join_all` (parallel loading) |
| **API** | OpenAPI spec generation, Swagger UI, content negotiation, request validation |
| **Serving** | Static files with ETag, NamedFile, content-type detection |
| **Server** | HTTP/1.1, HTTP/2, TLS (rustls), graceful shutdown, shutdown hooks, connection limits, TCP tuning |
| **DX** | TestClient (no-TCP testing), CLI (`neutron new`, `neutron dev`), tracing integration, metrics |

## Quick Start

```rust
use neutron::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let router = Router::new()
        .middleware(Logger)
        .middleware(RequestId::new())
        .middleware(CatchPanic::new())
        .get("/", || async { "Hello, Neutron!" })
        .get("/users/:id", get_user)
        .post("/users", create_user)
        .get("/health", || async { Json(serde_json::json!({ "status": "ok" })) });

    Neutron::new().router(router).serve(3000).await
}

async fn get_user(Path(id): Path<u64>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "id": id, "name": "Alice" }))
}

async fn create_user(Json(body): Json<serde_json::Value>) -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::CREATED, Json(serde_json::json!({ "created": body })))
}
```

## Routing

```rust
let api = Router::new()
    .middleware(JwtAuth::new(jwt_config))
    .get("/items", list_items)
    .get("/items/:id", get_item)
    .post("/items", create_item)
    .put("/items/:id", update_item)
    .delete("/items/:id", delete_item);

let router = Router::new()
    .middleware(Logger)
    .middleware(Cors::new().allow_any_origin())
    .nest("/api", api)                        // nested router with scoped middleware
    .static_files("/assets", "./public")      // static file serving
    .get("/health", || async { "ok" })
    .fallback(|| async { (StatusCode::NOT_FOUND, "not found") });
```

## Middleware

Middleware wraps the request/response pipeline. Built-in middleware covers the common production needs:

```rust
Router::new()
    .middleware(CatchPanic::new())                          // recover from panics
    .middleware(Logger)                                      // structured logging
    .middleware(RequestId::new())                            // unique request IDs
    .middleware(Timeout::from_secs(30))                      // per-request timeout
    .middleware(Cors::new().allow_any_origin())              // CORS headers
    .middleware(Helmet::new())                               // security headers
    .middleware(RateLimiter::new(100, Duration::from_secs(60))) // rate limiting
    .middleware(BodyLimit::new(1024 * 1024))                 // 1MB body limit
    .middleware(Compress::default())                         // gzip/brotli compression
    .middleware(TracingLayer::new())                         // distributed tracing
```

Custom middleware is any `async fn(Request, Next) -> Response`:

```rust
async fn timing(req: Request, next: Next) -> Response {
    let start = std::time::Instant::now();
    let mut resp = next.run(req).await;
    resp.headers_mut().insert(
        "x-response-time",
        format!("{}ms", start.elapsed().as_millis()).parse().unwrap(),
    );
    resp
}
```

## Real-time

### WebSocket

```rust
async fn ws_handler(ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(|mut socket| async move {
        while let Some(msg) = socket.recv().await {
            if let Message::Text(text) = msg {
                let _ = socket.send(Message::text(format!("Echo: {text}"))).await;
            }
        }
    })
}
```

### Server-Sent Events

```rust
async fn events(State(ps): State<PubSub>) -> Sse {
    let mut rx = ps.subscribe::<String>("events");
    Sse::new(async_stream::stream! {
        yield SseEvent::new().data("connected");
        while let Ok(msg) = rx.recv().await {
            yield SseEvent::new().event("message").data(&msg);
        }
    })
}
```

## Auth

### JWT

```rust
let config = JwtConfig::new(b"secret-key").issuer("my-app");
let api = Router::new()
    .middleware(JwtAuth::new(config))
    .get("/protected", |Extension(claims): Extension<Claims>| async move {
        Json(serde_json::json!({ "user": claims.sub }))
    });
```

### Sessions

```rust
let router = Router::new()
    .middleware(SessionLayer::new(MemoryStore::new(), key))
    .get("/me", |session: Session| async move {
        let name: Option<String> = session.get("name");
        Json(serde_json::json!({ "name": name }))
    });
```

## Server Configuration

```rust
Neutron::new()
    .router(router)
    .http2(Http2Config::new().max_concurrent_streams(100))
    .max_connections(10_000)
    .tcp_nodelay(true)
    .shutdown_timeout(Duration::from_secs(30))
    .on_shutdown(|| async { tracing::info!("cleaning up...") })
    .listen("0.0.0.0:3000".parse().unwrap())
    .await?;
```

## Testing

Two modes depending on what you're testing. No TCP server needed for either:

```rust
use tower::ServiceExt; // oneshot

#[tokio::test]
async fn test_handler_direct() {
    // Unit test: direct Service invocation, fastest (~0.1ms vs ~10ms with HTTP)
    let app = Router::new().route("/users/:id", get(get_user));

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/users/42")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}
```

For integration tests that exercise middleware, auth, and extractors together, use `axum-test`:

```rust
use axum_test::TestServer;

#[tokio::test]
async fn test_api() {
    let app = Router::new()
        .route("/users/:id", get(get_user))
        .layer(JwtAuth::new(config));

    let server = TestServer::new(app).unwrap();
    let resp = server.get("/users/42").add_header("Authorization", "Bearer ...").await;
    assert_eq!(resp.status_code(), StatusCode::OK);

    let body: serde_json::Value = resp.json();
    assert_eq!(body["id"], 42);
}
```

## io_uring (Linux)

On Linux, enabling the `io_uring` feature switches the runtime from Tokio to [Monoio](https://github.com/bytedance/monoio), ByteDance's thread-per-core runtime. Monoio avoids work-stealing overhead and uses io_uring for all I/O — benchmarks show ~20% higher throughput than Tokio on Linux for I/O-bound workloads.

```toml
# Cargo.toml — Linux-only, deployment target only
neutron = { version = "0.1", features = ["io_uring"] }
```

Tradeoffs: Linux-only (no macOS/Windows), thread-per-core model (no `tokio::spawn` across threads), and Monoio's API is not drop-in compatible with Tokio. Use Tokio (the default) for development and cross-platform deployments; switch to io_uring on Linux production if profiling confirms I/O bottleneck.

## HTTP/3 (QUIC)

The `h3` feature enables HTTP/3 via [Quinn](https://github.com/quinn-rs/quinn) (pure Rust QUIC). HTTP/3 reduces TTFB by 50–100ms on high-packet-loss networks (mobile, geo-distributed) through connection migration and 0-RTT resumption. On typical datacenter deployments the difference is <10ms — stick with HTTP/2 unless you're targeting high-loss networks.

```toml
neutron = { version = "0.1", features = ["h3"] }
```

## Performance

Benchmarks on the full middleware-to-handler pipeline (criterion, single core):

| Benchmark | Time |
|-----------|------|
| Plaintext GET `/` | 681 ns |
| JSON GET `/user` | 1.57 us |
| Path param + JSON | 1.53 us |
| JSON body POST | 1.40 us |
| Query string parse | 1.65 us |
| 3 real middleware (RequestId + Logger + Timeout) | 3.06 us |
| Router lookup (500 routes) | 277 ns |
| Router miss (404) | 100 ns |
| IntoResponse `&str` | 315 ns |
| IntoResponse `Json<T>` (small) | 931 ns |

Middleware overhead is ~250ns per layer. Router uses [matchit](https://github.com/ibraheemdev/matchit) — a compressing radix trie with 2.4μs lookup across 130 routes (~19ns per route). Performance is O(path segments), independent of total route count.

The competitive bar: Actix-web achieves ~440k req/sec on plaintext, Axum ~400k. Neutron targets this range on HTTP/1.1 with the default Tokio runtime and the full middleware stack shown above.

```bash
# Run benchmarks
cargo bench --bench pipeline
cargo bench --bench router
```

## Feature Flags

Neutron uses feature flags to keep the dependency tree minimal. The `full` feature (default) enables everything:

| Feature | What it enables |
|---------|-----------------|
| `tls` | HTTPS via rustls |
| `compress` | gzip/brotli response compression |
| `ws` | WebSocket support |
| `multipart` | Multipart form data parsing |
| `jwt` | JWT authentication |
| `cookie` | Cookie, signed/private cookies, sessions, CSRF |
| `openapi` | OpenAPI spec generation + Swagger UI |
| `io_uring` | Monoio runtime on Linux (~20% more throughput, thread-per-core) |
| `h3` | HTTP/3 via Quinn QUIC (useful for high-loss networks) |

```toml
# Use only what you need
neutron = { version = "0.1", default-features = false, features = ["jwt", "compress"] }
```

## CLI

```bash
# Create a new project
cargo run -p neutron-cli -- new my-api

# Development server with auto-restart
cargo run -p neutron-cli -- dev --port 3000
```

## Examples

```bash
cargo run --example hello      # basic routes, middleware, state, WebSocket
cargo run --example rest_api   # JWT auth, CRUD, OpenAPI spec
cargo run --example realtime   # SSE streaming, WebSocket echo, PubSub
cargo run --example bench      # performance testing
```

## Architecture

```
crates/
  neutron/           # core framework library
    src/
      app.rs         # server lifecycle, graceful shutdown
      router.rs      # trie-based router
      handler.rs     # request/response types, Handler trait
      extract.rs     # Path, Query, Json, Form, State, Extension
      middleware.rs   # middleware trait, dispatch chain
      ...40+ modules
    examples/
    benches/
    tests/
  neutron-cli/       # CLI for scaffolding and dev server
```

## License

MIT
