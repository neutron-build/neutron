//! Neutron -- a lightweight, async-first Rust web framework built on hyper.
//!
//! Provides trie-based routing, type-safe extractors, composable middleware,
//! and optional features for TLS, WebSockets, compression, sessions, and more.
//!
//! # Feature Tiers
//!
//! ```toml
//! # Bare core — router, handler, middleware, extractors. No JSON, no logging.
//! neutron = { version = "0.1", default-features = false }
//!
//! # Standard web API — JSON, auth, TLS, CORS, compression, logging.
//! neutron = { version = "0.1", default-features = false, features = ["web"] }
//!
//! # Full batteries — everything enabled (default).
//! neutron = { version = "0.1" }
//! ```
//!
//! # Quick Start
//!
//! ```rust,ignore
//! use neutron::prelude::*;
//!
//! let router = Router::new()
//!     .middleware(Logger::new())
//!     .get("/", || async { "Hello, Neutron!" });
//!
//! Neutron::new().router(router).serve(3000).await?;
//! ```

// Framework types are inherently complex (async fn pointers in Arc) and Response
// is large by design — these are standard patterns in Rust web frameworks.
#![allow(clippy::type_complexity, clippy::result_large_err)]

// ---------------------------------------------------------------------------
// Phase 5: opt-in global allocators
//
// Enable with `features = ["jemalloc"]` or `features = ["mimalloc"]` in your
// binary's Cargo.toml.  Do NOT enable both simultaneously.
// ---------------------------------------------------------------------------

#[cfg(all(feature = "jemalloc", not(feature = "mimalloc")))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(all(feature = "mimalloc", not(feature = "jemalloc")))]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

// ---------------------------------------------------------------------------
// Core — always compiled, zero extra deps beyond hyper/tokio/http/bytes
// ---------------------------------------------------------------------------
pub mod app;
pub mod config;
pub mod error;
pub mod extract;
pub mod handler;
pub mod http2;
pub mod middleware;
pub mod router;

// ---------------------------------------------------------------------------
// Stateless middleware — no extra deps, opt-in
// ---------------------------------------------------------------------------
#[cfg(feature = "body-limit")]
pub mod body_limit;

#[cfg(feature = "catch-panic")]
pub mod catch_panic;

#[cfg(feature = "cors")]
pub mod cors;

#[cfg(feature = "helmet")]
pub mod helmet;

#[cfg(feature = "timeout")]
pub mod timeout;

// ---------------------------------------------------------------------------
// Infrastructure middleware — no extra deps, opt-in
// ---------------------------------------------------------------------------
#[cfg(feature = "cache")]
pub mod cache;

#[cfg(feature = "circuit-breaker")]
pub mod circuit_breaker;

#[cfg(feature = "dedup")]
pub mod dedup;

#[cfg(feature = "rate-limit")]
pub mod rate_limit;

// ---------------------------------------------------------------------------
// Observability — logging/tracing/metrics
// ---------------------------------------------------------------------------
#[cfg(feature = "logging")]
pub mod logger;

#[cfg(feature = "metrics")]
pub mod metrics;

#[cfg(feature = "tracing-mw")]
pub mod tracing_mw;

// ---------------------------------------------------------------------------
// Request ID (needs sha2 + rand for unique IDs)
// ---------------------------------------------------------------------------
#[cfg(feature = "request-id")]
pub mod request_id;

// ---------------------------------------------------------------------------
// Endpoints & utilities
// ---------------------------------------------------------------------------
#[cfg(feature = "health")]
pub mod health;

#[cfg(feature = "negotiate")]
pub mod negotiate;

#[cfg(feature = "validate")]
pub mod validate;

// ---------------------------------------------------------------------------
// Real-time
// ---------------------------------------------------------------------------
#[cfg(feature = "pubsub")]
pub mod pubsub;

#[cfg(feature = "sse")]
pub mod sse;

// ---------------------------------------------------------------------------
// Data loading
// ---------------------------------------------------------------------------
#[cfg(feature = "data")]
pub mod data;

// ---------------------------------------------------------------------------
// File serving
// ---------------------------------------------------------------------------
#[cfg(feature = "static-files")]
pub mod file_response;

#[cfg(feature = "static-files")]
pub mod static_files;

// ---------------------------------------------------------------------------
// Testing harness
// ---------------------------------------------------------------------------
#[cfg(feature = "testing")]
pub mod testing;

// ---------------------------------------------------------------------------
// Optional protocol features (already gated, unchanged)
// ---------------------------------------------------------------------------
#[cfg(feature = "compress")]
pub mod compress;

#[cfg(feature = "cookie")]
pub mod cookie;

#[cfg(feature = "cookie")]
pub mod csrf;

#[cfg(feature = "cookie")]
pub mod session;

#[cfg(feature = "jwt")]
pub mod jwt;

#[cfg(feature = "multipart")]
pub mod multipart;

#[cfg(feature = "openapi")]
pub mod openapi;

#[cfg(feature = "tls")]
pub mod tls;

#[cfg(feature = "ws")]
pub mod ws;

// ---------------------------------------------------------------------------
// HTTP/3 over QUIC (requires quinn + h3 + tls)
// ---------------------------------------------------------------------------
#[cfg(feature = "http3")]
pub mod http3_server;

// ---------------------------------------------------------------------------
// Prelude — convenience re-exports, respects all feature gates above
// ---------------------------------------------------------------------------
pub mod prelude;
