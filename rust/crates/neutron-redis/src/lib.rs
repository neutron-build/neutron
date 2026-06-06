//! Redis-backed backends for `neutron` — distributed session, rate-limit and cache.
//!
//! This crate provides production-grade replacements for the in-memory
//! implementations that ship with the core `neutron` crate:
//!
//! | Feature | neutron (in-memory) | neutron-redis (distributed) |
//! |---------|--------------------|-----------------------------|
//! | Sessions | `MemoryStore` | [`RedisSessionStore`] |
//! | Rate limiting | `RateLimitLayer` | [`RedisRateLimitLayer`] |
//! | Response cache | `CacheLayer` | [`RedisCacheLayer`] |
//!
//! All Redis operations share a single `ConnectionManager` (auto-reconnect,
//! multiplexed) via the cheap-clone [`RedisPool`] handle.
//!
//! # Quick start
//!
//! ```rust,ignore
//! use neutron_redis::{RedisPool, RedisSessionStore, RedisRateLimitLayer, RedisCacheLayer};
//! use neutron::session::SessionLayer;
//! use neutron::cookie::Key;
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() {
//!     let pool = RedisPool::new("redis://127.0.0.1/")
//!         .await
//!         .expect("redis not available");
//!
//!     let session_store = RedisSessionStore::new(pool.clone());
//!     let key           = Key::generate();
//!
//!     let router = Router::new()
//!         .middleware(SessionLayer::new(session_store, key))
//!         .middleware(RedisRateLimitLayer::new(pool.clone(), 100, Duration::from_secs(60)))
//!         .middleware(RedisCacheLayer::new(pool, Duration::from_secs(300)));
//! }
//! ```

pub mod cache;
pub mod error;
pub mod pool;
pub mod rate_limit;
pub mod session;

pub use cache::RedisCacheLayer;
pub use error::RedisError;
pub use pool::RedisPool;
pub use rate_limit::{KeyExtractor, RedisRateLimitLayer};
pub use session::RedisSessionStore;
