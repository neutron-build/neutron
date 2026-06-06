//! `Db` extractor and `PgTransaction` — the primary handler API.

use http::StatusCode;
use neutron::extract::FromRequestParts;
use neutron::handler::{IntoResponse, Request, Response};
use tokio_postgres::types::ToSql;
use tokio_postgres::Row;

use crate::error::PgError;
use crate::pool::{PgPool, PooledConn};

// ---------------------------------------------------------------------------
// Db — handler extractor
// ---------------------------------------------------------------------------

/// Database handle extracted from handler parameters.
///
/// Each call to `execute`, `query`, etc. acquires a connection from the pool,
/// runs the statement, and returns the connection to the pool.  For
/// multi-statement atomicity use [`Db::transaction`].
///
/// **Registration** — add the pool to the router state once:
/// ```rust,ignore
/// let pool = PgPool::new(PgConfig::from_url("postgres://localhost/mydb"));
/// Router::new().state(pool).get("/users", list_users);
/// ```
pub struct Db {
    pool: PgPool,
}

impl FromRequestParts for Db {
    fn from_parts(req: &Request) -> Result<Self, Response> {
        req.get_state::<PgPool>()
            .cloned()
            .map(|pool| Db { pool })
            .ok_or_else(|| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "PgPool not registered — call Router::state(pool)",
                )
                    .into_response()
            })
    }
}

impl Db {
    /// Execute a statement, returning the number of rows affected.
    pub async fn execute(
        &self,
        sql:    &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, PgError> {
        let conn = self.pool.get().await?;
        conn.client().execute(sql, params).await.map_err(PgError::Query)
    }

    /// Execute a query and return all matching rows.
    pub async fn query(
        &self,
        sql:    &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, PgError> {
        let conn = self.pool.get().await?;
        conn.client().query(sql, params).await.map_err(PgError::Query)
    }

    /// Execute a query expecting exactly one row.
    pub async fn query_one(
        &self,
        sql:    &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, PgError> {
        let conn = self.pool.get().await?;
        conn.client().query_one(sql, params).await.map_err(PgError::Query)
    }

    /// Execute a query expecting zero or one rows.
    pub async fn query_opt(
        &self,
        sql:    &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, PgError> {
        let conn = self.pool.get().await?;
        conn.client().query_opt(sql, params).await.map_err(PgError::Query)
    }

    /// Execute multiple semicolon-separated statements (e.g. DDL scripts).
    pub async fn batch_execute(&self, sql: &str) -> Result<(), PgError> {
        let conn = self.pool.get().await?;
        conn.client().batch_execute(sql).await.map_err(PgError::Query)
    }

    /// Begin a transaction.  The connection is held for the lifetime of the
    /// returned [`PgTransaction`]; call `.commit()` or `.rollback()`.
    pub async fn transaction(&self) -> Result<PgTransaction, PgError> {
        let conn = self.pool.get().await?;
        conn.client()
            .execute("BEGIN", &[])
            .await
            .map_err(PgError::Query)?;
        Ok(PgTransaction { conn, done: false })
    }
}

// ---------------------------------------------------------------------------
// PgTransaction
// ---------------------------------------------------------------------------

/// An open database transaction.
///
/// Call `.commit()` on success or `.rollback()` on error.  If neither is
/// called (e.g. the handler panics), the connection is discarded on drop
/// rather than re-pooled — always call one of the two termination methods.
pub struct PgTransaction {
    conn: PooledConn,
    done: bool,
}

impl PgTransaction {
    /// Execute a statement inside the transaction.
    pub async fn execute(
        &self,
        sql:    &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, PgError> {
        self.conn.client().execute(sql, params).await.map_err(PgError::Query)
    }

    /// Query inside the transaction.
    pub async fn query(
        &self,
        sql:    &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, PgError> {
        self.conn.client().query(sql, params).await.map_err(PgError::Query)
    }

    /// Query a single row inside the transaction.
    pub async fn query_one(
        &self,
        sql:    &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, PgError> {
        self.conn.client().query_one(sql, params).await.map_err(PgError::Query)
    }

    /// Commit the transaction and return the connection to the pool.
    pub async fn commit(mut self) -> Result<(), PgError> {
        self.conn
            .client()
            .execute("COMMIT", &[])
            .await
            .map_err(PgError::Query)?;
        self.done = true;
        Ok(())
    }

    /// Roll back the transaction and return the connection to the pool.
    pub async fn rollback(mut self) -> Result<(), PgError> {
        self.conn
            .client()
            .execute("ROLLBACK", &[])
            .await
            .map_err(PgError::Query)?;
        self.done = true;
        Ok(())
    }
}

impl Drop for PgTransaction {
    fn drop(&mut self) {
        if !self.done {
            tracing::warn!(
                "PgTransaction dropped without commit/rollback — \
                 connection will be discarded"
            );
            // Remove the client so PooledConn::drop skips re-pooling the
            // connection (it still has an open transaction on the server side).
            self.conn.client.take();
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::PgConfig;

    #[test]
    fn db_requires_pool_in_state() {
        // Verify that FromRequestParts returns an error response when the pool
        // is absent — we can test this without a real DB connection.
        use neutron::handler::Request as NeutronRequest;
        use http::Method;
        use bytes::Bytes;

        let req = NeutronRequest::new(
            Method::GET,
            "/test".parse().unwrap(),
            http::HeaderMap::new(),
            Bytes::new(),
        );
        let result = Db::from_parts(&req);
        assert!(result.is_err());
        // unwrap_err() requires Debug on Ok variant; match instead
        match result {
            Err(resp) => assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR),
            Ok(_)     => panic!("expected Err"),
        }
    }

    #[test]
    fn pg_config_fields() {
        let cfg = PgConfig::new()
            .host("pg.local")
            .dbname("app")
            .user("svc");
        assert_eq!(cfg.host, "pg.local");
        assert_eq!(cfg.dbname, "app");
        assert_eq!(cfg.user, "svc");
    }
}
