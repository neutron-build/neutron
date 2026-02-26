//! Connection pool for PostgreSQL.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_postgres::{Client, NoTls};

use crate::error::PgError;

// ---------------------------------------------------------------------------
// PgConfig
// ---------------------------------------------------------------------------

/// Connection configuration for a PostgreSQL database.
///
/// Supports both connection-URL and field-by-field construction.
///
/// ```rust,ignore
/// // URL style
/// let cfg = PgConfig::from_url("postgres://user:pass@localhost/mydb");
///
/// // Builder style
/// let cfg = PgConfig::new()
///     .host("localhost")
///     .port(5432)
///     .dbname("mydb")
///     .user("alice")
///     .password("secret");
/// ```
#[derive(Debug, Clone)]
pub struct PgConfig {
    /// Full connection URL (e.g. `postgres://user:pass@host/db`).
    /// When set, takes precedence over the individual fields.
    url: Option<String>,

    pub host:     String,
    pub port:     u16,
    pub dbname:   String,
    pub user:     String,
    pub password: String,
    /// Maximum concurrent connections (default: 16).
    pub max_size: usize,
}

impl Default for PgConfig {
    fn default() -> Self {
        Self {
            url:      None,
            host:     "127.0.0.1".to_string(),
            port:     5432,
            dbname:   "postgres".to_string(),
            user:     "postgres".to_string(),
            password: String::new(),
            max_size: 16,
        }
    }
}

impl PgConfig {
    /// Create a default config (localhost:5432, database=postgres).
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a config from a `postgres://` connection URL.
    ///
    /// The URL is stored verbatim and used as-is when connecting.
    pub fn from_url(url: impl Into<String>) -> Self {
        Self { url: Some(url.into()), ..Default::default() }
    }

    pub fn host(mut self, h: impl Into<String>) -> Self {
        self.host = h.into();
        self
    }

    pub fn port(mut self, p: u16) -> Self {
        self.port = p;
        self
    }

    pub fn dbname(mut self, db: impl Into<String>) -> Self {
        self.dbname = db.into();
        self
    }

    pub fn user(mut self, u: impl Into<String>) -> Self {
        self.user = u.into();
        self
    }

    pub fn password(mut self, pw: impl Into<String>) -> Self {
        self.password = pw.into();
        self
    }

    pub fn max_size(mut self, n: usize) -> Self {
        self.max_size = n;
        self
    }

    /// Return the connection string used by `tokio-postgres::connect`.
    pub(crate) fn connect_string(&self) -> String {
        if let Some(url) = &self.url {
            return url.clone();
        }
        format!(
            "host={} port={} dbname={} user={} password={}",
            self.host, self.port, self.dbname, self.user, self.password,
        )
    }
}

// ---------------------------------------------------------------------------
// Pool internals
// ---------------------------------------------------------------------------

pub(crate) struct PoolInner {
    pub(crate) config:    PgConfig,
    pub(crate) semaphore: Arc<Semaphore>,
    pub(crate) idle:      Mutex<VecDeque<Client>>,
}

// ---------------------------------------------------------------------------
// PgPool
// ---------------------------------------------------------------------------

/// Shared, cheaply-cloneable connection pool for a PostgreSQL database.
///
/// Register once as router state and extract via [`Db`](crate::Db):
///
/// ```rust,ignore
/// let pool = PgPool::new(PgConfig::from_url("postgres://localhost/mydb"));
///
/// let router = Router::new()
///     .state(pool)
///     .get("/users", list_users);
///
/// async fn list_users(db: Db) -> Json<Vec<User>> {
///     let rows = db.query("SELECT id, name FROM users", &[]).await.unwrap();
///     Json(rows.iter().map(User::from_row).collect())
/// }
/// ```
#[derive(Clone)]
pub struct PgPool(pub(crate) Arc<PoolInner>);

impl PgPool {
    /// Create a pool without pre-connecting.  Connections are created on demand.
    pub fn new(config: PgConfig) -> Self {
        let sem = Arc::new(Semaphore::new(config.max_size));
        Self(Arc::new(PoolInner {
            semaphore: sem,
            idle:      Mutex::new(VecDeque::new()),
            config,
        }))
    }

    /// Acquire a [`PooledConn`], waiting until a slot is available.
    pub async fn get(&self) -> Result<PooledConn, PgError> {
        let permit = Arc::clone(&self.0.semaphore)
            .acquire_owned()
            .await
            .map_err(|_| PgError::PoolExhausted)?;

        let client = {
            let mut idle = self.0.idle.lock().unwrap();
            idle.pop_front()
        };

        let client = match client {
            Some(c) if !c.is_closed() => c,
            _ => self.new_client().await?,
        };

        Ok(PooledConn { client: Some(client), pool: Arc::clone(&self.0), permit: Some(permit) })
    }

    async fn new_client(&self) -> Result<Client, PgError> {
        let cs = self.0.config.connect_string();
        let (client, conn) = tokio_postgres::connect(&cs, NoTls)
            .await
            .map_err(PgError::Connect)?;

        // Drive the connection in the background.
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::error!("Postgres connection driver exited: {e}");
            }
        });

        Ok(client)
    }
}

// ---------------------------------------------------------------------------
// PooledConn
// ---------------------------------------------------------------------------

/// A `tokio-postgres` client checked out from a [`PgPool`].
///
/// Automatically returned to the pool (and semaphore slot released) on drop.
pub struct PooledConn {
    pub(crate) client: Option<Client>,
    pool:              Arc<PoolInner>,
    permit:            Option<OwnedSemaphorePermit>,
}

impl PooledConn {
    pub fn client(&self) -> &Client {
        self.client.as_ref().unwrap()
    }
}

impl Drop for PooledConn {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            // Return to idle before releasing the semaphore so that a woken
            // waiter finds the connection immediately.
            if let Ok(mut idle) = self.pool.idle.lock() {
                idle.push_back(client);
            }
        }
        // Semaphore slot is released when permit is dropped.
        drop(self.permit.take());
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_connect_string_fields() {
        let cfg = PgConfig::new()
            .host("db.example.com")
            .port(5433)
            .dbname("myapp")
            .user("alice")
            .password("s3cret");
        let cs = cfg.connect_string();
        assert!(cs.contains("host=db.example.com"));
        assert!(cs.contains("port=5433"));
        assert!(cs.contains("dbname=myapp"));
        assert!(cs.contains("user=alice"));
        assert!(cs.contains("password=s3cret"));
    }

    #[test]
    fn config_connect_string_url_takes_precedence() {
        let url = "postgres://bob:pw@localhost:5432/testdb";
        let cfg = PgConfig::from_url(url);
        assert_eq!(cfg.connect_string(), url);
    }

    #[test]
    fn pool_is_clone() {
        let pool  = PgPool::new(PgConfig::new());
        let _copy = pool.clone(); // must compile
    }

    #[test]
    fn default_config_sensible() {
        let cfg = PgConfig::default();
        assert_eq!(cfg.port, 5432);
        assert_eq!(cfg.max_size, 16);
        assert_eq!(cfg.user, "postgres");
    }

    #[test]
    fn max_size_builder() {
        let cfg = PgConfig::new().max_size(4);
        assert_eq!(cfg.max_size, 4);
    }
}
