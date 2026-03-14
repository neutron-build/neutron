//! Nucleus database integration for the Neutron framework.
//!
//! Provides two levels of API:
//!
//! 1. **`Db` extractor** — low-level SQL access via the connection pool, suitable
//!    for standard relational queries. Register `NucleusPool` as state and extract
//!    `Db` in handler parameters.
//!
//! 2. **`NucleusClient`** — unified multi-model client with typed access to all 14
//!    Nucleus data models (SQL, KV, Vector, TimeSeries, Document, Graph, FTS, Geo,
//!    Blob, Streams, PubSub, Columnar, Datalog, CDC). Auto-detects Nucleus vs plain
//!    PostgreSQL on connection.
//!
//! ## Quick start — Db extractor (SQL only)
//!
//! ```rust,ignore
//! use neutron::prelude::*;
//! use neutron_nucleus::{Db, NucleusConfig, NucleusPool, migrate};
//!
//! #[tokio::main]
//! async fn main() {
//!     let pool = NucleusPool::new(
//!         NucleusConfig::new("127.0.0.1", 5432, "myapp")
//!             .user("postgres")
//!             .password("secret"),
//!     );
//!
//!     migrate(&pool, "migrations").await.unwrap();
//!
//!     let router = Router::new()
//!         .state(pool)
//!         .get("/users", list_users);
//!
//!     Neutron::new().router(router).listen("0.0.0.0:3000".parse().unwrap()).await.unwrap();
//! }
//!
//! async fn list_users(db: Db) -> impl IntoResponse {
//!     let rows = db.query("SELECT id, name FROM users ORDER BY id", &[]).await?;
//!     let users: Vec<serde_json::Value> = rows.iter().map(|r| {
//!         serde_json::json!({ "id": r.get::<_, i64>(0), "name": r.get::<_, String>(1) })
//!     }).collect();
//!     Ok::<_, neutron_nucleus::NucleusError>(Json(users))
//! }
//! ```
//!
//! ## Quick start — NucleusClient (multi-model)
//!
//! ```rust,ignore
//! use neutron_nucleus::{NucleusClient, NucleusConfig};
//!
//! let client = NucleusClient::connect(NucleusConfig::default()).await?;
//!
//! // Key-value
//! client.kv().set("session:abc", "data", None).await?;
//! let val = client.kv().get("session:abc").await?;
//!
//! // Vector search
//! let results = client.vector().search("embeddings", &[1.0, 0.0, 0.0],
//!     neutron_nucleus::models::vector::DistanceMetric::Cosine, 10).await?;
//!
//! // Graph
//! let node_id = client.graph().add_node("Person", None).await?;
//!
//! // Time series
//! client.timeseries().insert("cpu.load", 1700000000000, 0.75).await?;
//!
//! // Standard SQL
//! let rows = client.sql().query("SELECT * FROM users WHERE active = $1", &[&true]).await?;
//! ```

pub mod client;
pub mod db;
pub mod error;
pub mod migrate;
pub mod models;
pub mod pool;

pub use client::{Features, NucleusClient};
pub use db::{Db, NucleusTransaction};
pub use error::NucleusError;
pub use migrate::migrate;
pub use pool::{NucleusConfig, NucleusPool, PooledConn};
