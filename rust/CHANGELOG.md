# Changelog

## 0.1.0 — 2026-03-16

Initial release.

### Core Framework (`neutron`)

- Trie-based router via matchit — 277ns lookup with 500 routes
- Type-safe extractors: `Path<T>`, `Query<T>`, `Json<T>`, `Form<T>`, `State<T>`, `Extension<T>`, `ConnectInfo`, `HeaderMap`
- Composable middleware pipeline with per-route and scoped middleware
- HTTP/1.1, HTTP/2, HTTP/3 (QUIC via quinn), TLS (rustls)
- Graceful shutdown with configurable timeout and hooks
- 25+ built-in middleware: Logger, CORS, Compress, BodyLimit, Timeout, RateLimit, CircuitBreaker, CatchPanic, RequestId, Helmet, Dedup, Metrics, Health, Negotiate, Validate, Tracing, CatchPanic
- JWT auth (sign/verify/middleware), cookie sessions (encrypted), CSRF protection
- WebSocket (fastwebsockets), Server-Sent Events, in-memory PubSub
- Static file serving with ETag, NamedFile, content-type detection
- Multipart form data / file uploads
- DataLoader with batching + caching (N+1 prevention)
- OpenAPI 3.1 spec generation + Swagger UI
- TestClient for in-memory integration testing (no TCP)
- Feature-gated builds: `default-features = false` for minimal core, `web` for standard, `full` for everything
- Optional: simd-json, jemalloc, mimalloc allocators
- Optional: io_uring via Monoio on Linux

### Satellite Crates

- `neutron-nucleusdb` — Nucleus database client with typed model APIs
- `neutron-jobs` — Background job queue with cron scheduling
- `neutron-storage` — S3/R2/GCS object storage client with SigV4 signing
- `neutron-oauth` — OAuth2/OIDC with PKCE
- `neutron-webauthn` — WebAuthn/Passkey authentication
- `neutron-graphql` — GraphQL HTTP transport
- `neutron-grpc` — gRPC support
- `neutron-redis` — Redis client + caching
- `neutron-postgres` — PostgreSQL client
- `neutron-stripe` — Stripe payments + webhook verification
- `neutron-smtp` — Email via SMTP
- `neutron-inference` — ML inference client
- `neutron-otel` — OpenTelemetry tracing (OTLP/JSON, no protobuf dep)
- `neutron-cache` — L1/L2 tiered caching (in-process + Redis)
- `neutron-rpc` — RPC protocol support
- `neutron-config` — Environment-based configuration
- `neutron-cli` — Project scaffolding + dev server

### Performance

- Plaintext GET: 681ns
- JSON GET: 1.57us
- Path param + JSON: 1.53us
- Router lookup (500 routes): 277ns
- 3 middleware pipeline: 3.06us

### Testing

- 541 tests, 100% pass rate
- 4 working examples: hello, bench, rest_api, realtime
