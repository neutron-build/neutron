//! Nucleus database integration for the Neutron framework.
//!
//! ## Quick start
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
//!     // Run pending migrations from the `migrations/` directory.
//!     migrate(&pool, "migrations").await.unwrap();
//!
//!     let router = Router::new()
//!         .state(pool)
//!         .get("/users",     list_users)
//!         .post("/users",    create_user)
//!         .delete("/users/:id", delete_user);
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

pub mod db;
pub mod error;
pub mod migrate;
pub mod pool;

pub use db::{Db, NucleusTransaction};
pub use error::NucleusError;
pub use migrate::migrate;
pub use pool::{NucleusConfig, NucleusPool, PooledConn};
