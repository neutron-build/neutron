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

## HTTP/3 (QUIC)

The `http3` feature enables HTTP/3 via [Quinn](https://github.com/quinn-rs/quinn) (pure Rust QUIC). HTTP/3 reduces TTFB by 50–100ms on high-packet-loss networks (mobile, geo-distributed) through connection migration and 0-RTT resumption. On typical datacenter deployments the difference is <10ms — stick with HTTP/2 unless you're targeting high-loss networks.

```toml
neutron = { version = "0.1", features = ["http3"] }
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
| `http3` | HTTP/3 via Quinn QUIC (useful for high-loss networks) |

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

## Parity and deliberate non-goals

Neutron tracks Axum/Actix/Rocket ergonomics closely. A few parity points are
deliberate, stated decisions rather than oversights:

- **Per-route body limit — supported.** The global cap is set with
  `Neutron::max_body_size` (default 2 MiB) and enforced per-frame at the
  streaming boundary. For a stricter, scoped limit, apply the `BodyLimit(n)`
  middleware to a sub-router or `nest`ed scope:
  `Router::new().middleware(BodyLimit::new(512 * 1024)).post("/upload", ...)`.
  The limit is enforced during streaming (413 the instant the running total
  exceeds the cap), so an oversized chunked body is never fully buffered.

- **Rocket `Outcome::Forward` — won't do (intentional).** Neutron uses
  single-match routing: a path+method resolves to exactly one handler. There is
  no "guard fails → fall through to the next matching route" semantics. Guard
  logic belongs in middleware or in the handler (return the appropriate status),
  which keeps routing predictable and the 405/404 contract unambiguous.

- **Actix per-route extractor config — partially supported, by design.**
  Per-route body limits are available (above). Per-route error handlers are not
  a separate mechanism: every built-in failure already renders as RFC 7807
  `application/problem+json` via `AppError`, and a handler that wants a custom
  error shape returns it directly (handlers own their responses). This keeps one
  error contract across the whole app rather than a patchwork of per-route
  formatters.

- **Composable `MethodRouter` — supported.** `route("/x", get(h).post(h2))`
  composes per-method handlers as a value (`neutron::router::{get, post, ...}`),
  with a correct `405 + Allow` for unhandled methods falling out automatically.

- **Handler diagnostics — `#[neutron::debug_handler]`.** Annotate a handler to
  get span-targeted errors when an argument isn't a valid extractor or the
  return type isn't a response, instead of an opaque `Handler` error at the
  route site.

## License

MIT

## Legacy SQL migration boundary

`neutron_postgres::migrate` and `neutron_nucleusdb::migrate` retain their existing filename-only ledgers for legacy PostgreSQL applications. These histories are unverified and drift-blind: changing the SQL of an applied filename does not rerun it or detect drift. New managed applications should use the language-neutral Neutron CLI migration authority. The Rust runners refuse CLI or the other Rust runner's history in the admitted schema; they do not adopt or reset it.

The filename runners require PostgreSQL catalog and transaction semantics and refuse Nucleus or unavailable catalog evidence before metadata creation. This restriction applies to migration runners; query clients remain independent. History must be an ordinary permanent table with native `TEXT` name primary key and native `TIMESTAMPTZ` applied time in a persistent application schema. Temporary shadows, later-schema history ambiguity and incompatible history shapes are refused.

Regular UTF-8 `.sql` files are captured before a connection or migration lock is awaited, then applied in filename order. `.up.sql`/`.down.sql` naming is refused; use the CLI for paired migrations. A session advisory lock serializes the invocation with the CLI key. Each pending file commits separately; a failed step rolls back while earlier committed steps remain. Supplied SQL is trusted application migration SQL, not a hostile-author sandbox, and must not manage the runner's transactions or protected metadata.

Cancellation or an unconfirmed unlock discards the checked-out connection instead of returning an uncertain session to the pool. PostgreSQL may finish an in-flight statement before its driver disconnects: cancellation does not prove immediate rollback, immediate lock release, or absence of committed effects. An unknown commit outcome is reported without automatic retry; inspect the database before deciding how to proceed.

Optional live regressions use `NEUTRON_TEST_DATABASE_URL`, a disposable TCP PostgreSQL admin URL with permission to create databases. Run `cargo test -p neutron-postgres -p neutron-nucleusdb --no-default-features --test migration_containment -- --test-threads=1` from `rust/`. Tests create and drop their own `v10_rust_containment_*` databases; without the URL, they report that live coverage was skipped.

### Custom session stores

`SessionStore` is now an async version 2 contract: implement `load_versioned`,
`commit`, and `revoke`, returning `SessionFuture` with
`SessionStoreError` (`Conflict`, `Capacity`, `Unavailable`, or `Corrupt`). The
future alias already wraps `Result`; its type parameter is the success value.
Load the value and revision atomically. Both commit and revoke require the
expected revision, and a stale nonzero revision must fail after expiry/deletion.
Missing data is `Ok(None)`; backend errors and malformed stored JSON are errors,
never new anonymous sessions. Storage futures must yield rather than block a
request worker. No synchronous custom-store adapter is accepted by the layer.

`SessionLayer::operation_timeout` defaults to five seconds per operation on any
Tokio runtime. A failed load stops handler dispatch. Persistence/deletion failure
or deadline expiry returns 503 without confirming cookie changes. A dispatched
write that times out may have completed remotely; reconcile rather than assume
rollback or retry it automatically. Legacy synchronous convenience methods exist
only on `MemoryStore`, and do not supply middleware guarantees. No atomic session
rotation API is provided here, and CAS cannot revoke already executing handler
work.

Redis sessions target standalone Redis. Existing records receive a revision
atomically on first load. Stop/drain all older unconditional session writers
before cutover, including acknowledgment/background paths; signature changes do
not protect against old binaries. Custom stores must implement the same atomic
revision contract. Use a separate key namespace if mixed writer semantics cannot
be ruled out.

### Durable job store rollout

Persisted jobs start with `claim_token = 0` (PostgreSQL adds a default-zero column;
Redis accepts a missing legacy token only before claim). Assign each token
atomically at claim. Every completion, failure, retry and recovery must predicate
its write on the current token and running state. Custom `JobStore`
implementations and callers must migrate together; use the token returned by
`claim_due`, never an ID-only acknowledgment. Register named startup queues using
`PersistentJobQueue::with_queues` so delayed and preexisting jobs are discovered.

Stop admission, drain/stop old workers and all unconditional acknowledgment
writers before enabling fencing. A rolling deployment with old writers can
invalidate the new ownership contract. After old workers are gone, recover
legacy running records through the backend's stale-recovery policy (including
older Redis running indexes without a status); do not reinterpret active work as
pending during cutover. Redis scripts target standalone Redis, with bounded
claim/recovery batches and whole-selected-batch corruption preflight; corruption
returns an error before mutation and requires operator repair. Redis Cluster is
unsupported. Delivery remains at least once. External effects require application
idempotency, and stale timeout must exceed maximum handler duration; job-state
fencing does not fence arbitrary remote effects.

### Shared responses and protocol limits

`Deduplicate::new()` does no implicit response sharing. `public_routes` is an
explicit assertion that a route tree's response is public and does not depend on
IP, application principals, extensions or unkeyed inputs. Authorization and audit
middleware must run outside the sharing layer for every request. Credentials are
always excluded, including with custom keys; any Vary, private/revalidation or
cookie response is refused. Flights and buffered snapshots are bounded; a stream
is returned without polling. Prefer sharing a typed application computation when
per-request work cannot be separated this way.

Memory/Redis caches reject conditional/range requests and inspect every
Cache-Control value, including field-qualified private/no-cache. Custom keys and
unmarked origins must assert that cached bodies vary only by the represented
inputs. Memory public routes match path segments, and `limits` caps retained body
bytes and active fills. Invalidation removes all Host/query variants by canonical
path and conservatively fences every active fill through a bounded global epoch.
Age reduces admitted freshness and increases on hits from the original admission
age plus residence; repeated hits never reset it. Memory uses monotonic insertion
time independently of LRU access. Redis uses a required v4 age/TTL record, atomic
GET/PTTL observations, and a Redis TIME admission anchor so cross-process clocks
and write delay cannot grant a fresh budget. Both default and custom Redis keys
use a v4 namespace; old v2/v3 or incompatible records are deliberately abandoned.
Capacity bypass dispatches independently.

Raising `Neutron::max_body_size` raises the transport ceiling, but ordinary
String/Bytes/JSON/Form extractors and the Tower bridge deliberately retain their
independent 2 MiB collection cap. Use explicit `collect_body`/`BodyStream` limits
for larger input. Lower transport ceilings still apply to all consumers. H3
buffers request input within its configured ceiling, sends response frames with
backpressure, and bounds active drain/transport teardown by the server deadline.
Upgraded callbacks receive `WebSocket::shutdown_signal`; server-owned callbacks
and socket halves drain, then abort/join before hooks. Applications remain
responsible for any tasks they spawn themselves.

### Raw pooled SQL and outbound operations

`PooledConn::raw_client_nonreusable` exposes trusted raw SQL and permanently
marks the lease for disposal, even on success; `client` is a compatibility alias
with the same behavior. Arbitrary SQL may change transactions, settings, locks or
session state, so generic Db methods also discard their raw leases. Untouched
leases may return to idle. Disposal does not prove immediate server rollback,
lock release or a known commit outcome. Reconcile uncertain mutations.

OAuth custom providers must configure `identity(provider_namespace, subject_field,
allow_numeric_subject)`. Built-in presets use the genuine provider subject and
namespace. Link by `(OAuthUser.provider, OAuthUser.id)`, never bare ID; `from_json`
is only an unnamespaced format helper, not authenticated identity admission.
Non-success provider HTTP responses and malformed/empty subjects are refused.

Stripe operations have a 30-second connect/TLS/header/body/parsing deadline and a
2 MiB response limit, adjustable via `operation_limits`. The HTTP driver is owned
by the same future. A timeout/error can leave a dispatched payment's outcome
unknown; no mutation is automatically retried. OTLP span closure admits into a
bounded synchronous queue before returning and counts refused spans; one owned
worker exports, and shutdown fences admission, joins the worker and attempts a
final flush within one five-second deadline covering public flush queues,
shutdown serialization, worker joining and export. Shutdown interrupts active
public exports and refuses queued public flushes. Failed/canceled batches remain
bounded and retained; `pending_count` includes an active export batch, and
`exported_count`/`dropped_count` distinguish delivery from loss. A shutdown error
means drain is incomplete; retained spans can be inspected and a later shutdown
can retry the terminal export.


### Generated TypeScript clients

`OpenApi::try_generate_typescript` returns an explicit error for unsupported
schema/reference/operation/path identifiers, reserved names, missing path
parameters, duplicate declarations or parameters, and collisions with generated
locals. Ordinary generated route names map punctuation to ASCII camelCase;
collisions after mapping are refused. Schema references must name an existing
local component. The compatible string API emits a valid module that throws on
generation refusal. Path literals and base URLs are JSON string literals, path
values are URI encoded, and summaries cannot terminate the emitted comment.
Request bodies support application/json; other content types refuse before fetch.
