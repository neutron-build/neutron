//! PostgreSQL integration for the Neutron web framework.
//!
//! Provides a connection pool ([`PgPool`]), a handler extractor ([`Db`]),
//! transaction support ([`PgTransaction`]), and a SQL migration runner
//! ([`migrate`]).
//!
//! # Quick start
//!
//! ```rust,ignore
//! use neutron::router::Router;
//! use neutron::response::Json;
//! use neutron_postgres::{Db, PgConfig, PgPool};
//!
//! #[tokio::main]
//! async fn main() {
//!     let pool = PgPool::new(
//!         PgConfig::from_url("postgres://postgres@localhost/myapp"),
//!     );
//!
//!     // Optional: run SQL migrations on startup.
//!     neutron_postgres::migrate(&pool, "migrations/").await.unwrap();
//!
//!     let router = Router::new()
//!         .state(pool)
//!         .get("/users", list_users);
//!
//!     // ... run server
//! }
//!
//! async fn list_users(db: Db) -> Json<Vec<String>> {
//!     let rows = db.query("SELECT name FROM users", &[]).await.unwrap();
//!     Json(rows.iter().map(|r| r.get::<_, String>(0)).collect())
//! }
//! ```
//!
//! # Transactions
//!
//! ```rust,ignore
//! async fn create_user(db: Db, Json(body): Json<CreateUser>) -> impl IntoResponse {
//!     let tx = db.transaction().await?;
//!     tx.execute("INSERT INTO users (name) VALUES ($1)", &[&body.name]).await?;
//!     tx.execute("INSERT INTO audit_log (action) VALUES ('user_created')", &[]).await?;
//!     tx.commit().await?;
//!     StatusCode::CREATED
//! }
//! ```

mod db;
mod error;
mod migrate;
mod pool;

pub use db::{Db, PgTransaction};
pub use error::PgError;
pub use migrate::migrate;
pub use pool::{PgConfig, PgPool, PooledConn};
