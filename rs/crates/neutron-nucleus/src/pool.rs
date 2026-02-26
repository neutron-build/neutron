//! Connection pool for the Nucleus database.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_postgres::{Client, NoTls};

use crate::error::NucleusError;

// ---------------------------------------------------------------------------
// NucleusConfig
// ---------------------------------------------------------------------------

/// Connection parameters for a Nucleus (or any pgwire-compatible) server.
#[derive(Debug, Clone)]
pub struct NucleusConfig {
    pub host:     String,
    pub port:     u16,
    pub dbname:   String,
    pub user:     String,
    pub password: String,
    /// Maximum concurrent connections (default: 16).
    pub max_size: usize,
}

impl Default for NucleusConfig {
    fn default() -> Self {
        Self {
            host:     "127.0.0.1".to_string(),
            port:     5432,
            dbname:   "nucleus".to_string(),
            user:     "postgres".to_string(),
            password: String::new(),
            max_size: 16,
        }
    }
}

impl NucleusConfig {
    pub fn new(host: impl Into<String>, port: u16, dbname: impl Into<String>) -> Self {
        Self { host: host.into(), port, dbname: dbname.into(), ..Default::default() }
    }

    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = user.into();
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

    pub(crate) fn connect_string(&self) -> String {
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
    pub(crate) config:    NucleusConfig,
    pub(crate) semaphore: Arc<Semaphore>,
    pub(crate) idle:      Mutex<VecDeque<Client>>,
}

// ---------------------------------------------------------------------------
// NucleusPool
// ---------------------------------------------------------------------------

/// Shared, cheaply-cloneable connection pool for a Nucleus database.
///
/// Register as shared state and extract via [`Db`](crate::Db):
///
/// ```rust,ignore
/// let pool = NucleusPool::new(NucleusConfig::default());
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
pub struct NucleusPool(pub(crate) Arc<PoolInner>);

impl NucleusPool {
    /// Create a pool without pre-connecting.  Connections are created on demand.
    pub fn new(config: NucleusConfig) -> Self {
        let sem = Arc::new(Semaphore::new(config.max_size));
        Self(Arc::new(PoolInner {
            semaphore: sem,
            idle:      Mutex::new(VecDeque::new()),
            config,
        }))
    }

    /// Acquire a [`PooledConn`], blocking until a slot is available.
    pub async fn get(&self) -> Result<PooledConn, NucleusError> {
        let permit = Arc::clone(&self.0.semaphore)
            .acquire_owned()
            .await
            .map_err(|_| NucleusError::PoolExhausted)?;

        // Prefer an idle connection; create a new one if the idle set is empty.
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

    async fn new_client(&self) -> Result<Client, NucleusError> {
        let cs = self.0.config.connect_string();
        let (client, conn) = tokio_postgres::connect(&cs, NoTls)
            .await
            .map_err(NucleusError::Connect)?;

        // Drive the connection in the background.
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::error!("Nucleus connection driver exited: {e}");
            }
        });

        Ok(client)
    }
}

// ---------------------------------------------------------------------------
// PooledConn
// ---------------------------------------------------------------------------

/// A `tokio-postgres` client checked out from a [`NucleusPool`].
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
        // Semaphore slot released when permit is dropped.
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
    fn config_connect_string() {
        let cfg = NucleusConfig::new("localhost", 5432, "mydb")
            .user("alice")
            .password("secret");
        let cs = cfg.connect_string();
        assert!(cs.contains("host=localhost"));
        assert!(cs.contains("port=5432"));
        assert!(cs.contains("dbname=mydb"));
        assert!(cs.contains("user=alice"));
        assert!(cs.contains("password=secret"));
    }

    #[test]
    fn pool_is_clone() {
        let pool = NucleusPool::new(NucleusConfig::default());
        let _clone = pool.clone(); // must compile
    }

    #[test]
    fn default_config_sensible_values() {
        let cfg = NucleusConfig::default();
        assert_eq!(cfg.port, 5432);
        assert_eq!(cfg.max_size, 16);
        assert_eq!(cfg.user, "postgres");
    }
}
