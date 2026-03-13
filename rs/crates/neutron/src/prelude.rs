// ---------------------------------------------------------------------------
// Core — always available
// ---------------------------------------------------------------------------
pub use crate::app::Neutron;
pub use crate::config::Config;
pub use crate::error::AppError;
pub use crate::handler::{AnyState, Body, IntoResponse, Redirect, Request, Response, StateMap, StateMapBuilder};
pub use crate::http2::Http2Config;
pub use crate::middleware::Next;
pub use crate::router::Router;

// Core extractor traits — implement these for custom extractors
pub use crate::extract::{FromRequest, FromRequestParts};

// Core extractors (no feature required)
pub use crate::extract::{ConnectInfo, Extension, Optional, Path, State};

// Typed header extractor and trait
pub use crate::extract::{TypedHeader, TypedHeaderValue};

// Built-in typed header types
// Note: extract::ContentType (String newtype) is intentionally NOT re-exported here
// because negotiate::ContentType (enum) is already exported when that feature is active.
// Import extract::ContentType directly if you need the typed header version.
pub use crate::extract::{Accept, Authorization, BearerToken, Host, Origin, UserAgent};

// Re-exports from http crate
pub use http::{HeaderMap, Method, StatusCode};

// ---------------------------------------------------------------------------
// JSON — feature = "json"
// ---------------------------------------------------------------------------
#[cfg(feature = "json")]
pub use crate::handler::Json;

// ---------------------------------------------------------------------------
// Form + Query deserialization — feature = "form"
// ---------------------------------------------------------------------------
#[cfg(feature = "form")]
pub use crate::extract::{Form, Query};

// ---------------------------------------------------------------------------
// Stateless middleware — individual feature flags
// ---------------------------------------------------------------------------
#[cfg(feature = "body-limit")]
pub use crate::body_limit::BodyLimit;

#[cfg(feature = "catch-panic")]
pub use crate::catch_panic::CatchPanic;

#[cfg(feature = "cors")]
pub use crate::cors::Cors;

#[cfg(feature = "helmet")]
pub use crate::helmet::Helmet;

#[cfg(feature = "timeout")]
pub use crate::timeout::Timeout;

// ---------------------------------------------------------------------------
// Infrastructure middleware
// ---------------------------------------------------------------------------
#[cfg(feature = "cache")]
pub use crate::cache::{CacheHandle, ResponseCache};

#[cfg(feature = "circuit-breaker")]
pub use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerHandle};

#[cfg(feature = "dedup")]
pub use crate::dedup::Deduplicate;

#[cfg(feature = "rate-limit")]
pub use crate::rate_limit::RateLimiter;

// ---------------------------------------------------------------------------
// Observability
// ---------------------------------------------------------------------------
#[cfg(feature = "logging")]
pub use crate::logger::Logger;

#[cfg(feature = "metrics")]
pub use crate::metrics::{Metrics, MetricsLayer};

#[cfg(feature = "tracing-mw")]
pub use crate::tracing_mw::{TraceId, TracingLayer};

// ---------------------------------------------------------------------------
// Request ID
// ---------------------------------------------------------------------------
#[cfg(feature = "request-id")]
pub use crate::request_id::RequestId;

// ---------------------------------------------------------------------------
// Endpoints & utilities
// ---------------------------------------------------------------------------
#[cfg(feature = "health")]
pub use crate::health::HealthCheck;

#[cfg(feature = "negotiate")]
pub use crate::negotiate::{AcceptHeader, ContentType, Negotiate};

#[cfg(feature = "validate")]
pub use crate::validate::{Validate, Validated};

// ---------------------------------------------------------------------------
// Real-time
// ---------------------------------------------------------------------------
#[cfg(feature = "pubsub")]
pub use crate::pubsub::{PubSub, Subscriber, SubscriberError};

#[cfg(feature = "sse")]
pub use crate::sse::{Sse, SseEvent};

// ---------------------------------------------------------------------------
// Data loading
// ---------------------------------------------------------------------------
#[cfg(feature = "data")]
pub use crate::data::{join_all, try_join_all, DataLoader, Loader};

// ---------------------------------------------------------------------------
// File serving
// ---------------------------------------------------------------------------
#[cfg(feature = "static-files")]
pub use crate::file_response::NamedFile;

#[cfg(feature = "static-files")]
pub use crate::static_files::StaticFiles;

// ---------------------------------------------------------------------------
// Auth — feature-gated protocol features
// ---------------------------------------------------------------------------
#[cfg(feature = "jwt")]
pub use crate::jwt::{Claims, JwtAuth, JwtConfig, JwtError};

#[cfg(feature = "cookie")]
pub use crate::cookie::{CookieJar, Key, PrivateCookieJar, SameSite, SetCookie, SignedCookieJar};

#[cfg(feature = "cookie")]
pub use crate::csrf::{CsrfLayer, CsrfToken};

#[cfg(feature = "cookie")]
pub use crate::session::{MemoryStore, Session, SessionLayer, SessionStore};

// ---------------------------------------------------------------------------
// Compression
// ---------------------------------------------------------------------------
#[cfg(feature = "compress")]
pub use crate::compress::Compress;

// ---------------------------------------------------------------------------
// WebSocket
// ---------------------------------------------------------------------------
#[cfg(feature = "ws")]
pub use crate::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade, WsError, WsSender, WsReceiver};

// ---------------------------------------------------------------------------
// Multipart
// ---------------------------------------------------------------------------
#[cfg(feature = "multipart")]
pub use crate::multipart::{Field, Multipart, MultipartError};

// ---------------------------------------------------------------------------
// TLS
// ---------------------------------------------------------------------------
#[cfg(feature = "tls")]
pub use crate::tls::{TlsConfig, TlsError};

// ---------------------------------------------------------------------------
// OpenAPI
// ---------------------------------------------------------------------------
#[cfg(feature = "openapi")]
pub use crate::openapi::{ApiRoute, OpenApi, Parameter, Schema};

// ---------------------------------------------------------------------------
// Tower compatibility
// ---------------------------------------------------------------------------
#[cfg(feature = "tower-compat")]
pub use crate::tower_compat::TowerLayerAdapter;
