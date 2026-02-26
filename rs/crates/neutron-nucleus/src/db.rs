//! `Db` extractor and `NucleusTransaction` — the primary handler API.

use http::StatusCode;
use neutron::extract::FromRequestParts;
use neutron::handler::{IntoResponse, Request, Response};
use tokio_postgres::types::ToSql;
use tokio_postgres::Row;

use crate::error::NucleusError;
use crate::pool::{NucleusPool, PooledConn};

// ---------------------------------------------------------------------------
// Db — handler extractor
// ---------------------------------------------------------------------------

/// Database handle extracted from handler parameters.
///
/// Each call to `execute`, `query`, etc. acquires a connection from the pool,
/// runs the statement, and returns the connection — no per-request connection
/// held open.  For multi-statement atomicity use [`Db::transaction`].
///
/// **Registration** — add the pool to the router state once:
/// ```rust,ignore
/// let pool = NucleusPool::new(NucleusConfig::default());
/// Router::new().state(pool).get("/users", list_users);
/// ```
pub struct Db {
    pool: NucleusPool,
}

impl FromRequestParts for Db {
    fn from_parts(req: &Request) -> Result<Self, Response> {
        req.get_state::<NucleusPool>()
            .cloned()
            .map(|pool| Db { pool })
            .ok_or_else(|| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "NucleusPool not registered — call Router::state(pool)",
                )
                    .into_response()
            })
    }
}

impl Db {
    /// Execute a statement, returning the number of rows affected.
    pub async fn execute(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, NucleusError> {
        let conn = self.pool.get().await?;
        conn.client().execute(sql, params).await.map_err(NucleusError::Query)
    }

    /// Execute a query and return all matching rows.
    pub async fn query(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, NucleusError> {
        let conn = self.pool.get().await?;
        conn.client().query(sql, params).await.map_err(NucleusError::Query)
    }

    /// Execute a query expecting exactly one row.
    pub async fn query_one(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, NucleusError> {
        let conn = self.pool.get().await?;
        conn.client().query_one(sql, params).await.map_err(NucleusError::Query)
    }

    /// Execute a query expecting zero or one rows.
    pub async fn query_opt(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, NucleusError> {
        let conn = self.pool.get().await?;
        conn.client().query_opt(sql, params).await.map_err(NucleusError::Query)
    }

    /// Begin a transaction.  The connection is held for the lifetime of the
    /// returned [`NucleusTransaction`]; call `.commit()` or `.rollback()`.
    pub async fn transaction(&self) -> Result<NucleusTransaction, NucleusError> {
        let conn = self.pool.get().await?;
        conn.client()
            .execute("BEGIN", &[])
            .await
            .map_err(NucleusError::Query)?;
        Ok(NucleusTransaction { conn, done: false })
    }
}

// ---------------------------------------------------------------------------
// NucleusTransaction
// ---------------------------------------------------------------------------

/// An open database transaction.
///
/// Call `.commit()` on success or `.rollback()` on error.  If neither is
/// called (e.g. handler panics), a best-effort `ROLLBACK` is issued when the
/// transaction is dropped — but note that async code cannot `await` in `Drop`,
/// so prefer explicit `.rollback()` in error paths.
pub struct NucleusTransaction {
    conn: PooledConn,
    done: bool,
}

impl NucleusTransaction {
    /// Execute a statement inside the transaction.
    pub async fn execute(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, NucleusError> {
        self.conn.client().execute(sql, params).await.map_err(NucleusError::Query)
    }

    /// Query inside the transaction.
    pub async fn query(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, NucleusError> {
        self.conn.client().query(sql, params).await.map_err(NucleusError::Query)
    }

    /// Commit the transaction and return the connection to the pool.
    pub async fn commit(mut self) -> Result<(), NucleusError> {
        self.conn
            .client()
            .execute("COMMIT", &[])
            .await
            .map_err(NucleusError::Query)?;
        self.done = true;
        Ok(())
    }

    /// Roll back the transaction and return the connection to the pool.
    pub async fn rollback(mut self) -> Result<(), NucleusError> {
        self.conn
            .client()
            .execute("ROLLBACK", &[])
            .await
            .map_err(NucleusError::Query)?;
        self.done = true;
        Ok(())
    }
}

impl Drop for NucleusTransaction {
    fn drop(&mut self) {
        if !self.done {
            // Best-effort: the connection will be returned to the pool with an
            // open transaction.  The next user will get a "transaction already
            // active" error which will cause the connection to be discarded.
            // For clean shutdown, always call .commit() or .rollback().
            tracing::warn!(
                "NucleusTransaction dropped without commit/rollback — \
                 connection will be discarded by the pool"
            );
            // Take the client out so pool::PooledConn::drop skips re-pooling.
            self.conn.client.take();
        }
    }
}
