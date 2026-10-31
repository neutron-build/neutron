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
/// runs arbitrary SQL, and discards the raw session lease to the pool.  For
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
    async fn from_parts(req: &Request) -> Result<Self, Response> {
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
    pub async fn execute(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64, PgError> {
        let conn = self.pool.get().await?;
        conn.client()
            .execute(sql, params)
            .await
            .map_err(PgError::Query)
    }

    /// Execute a query and return all matching rows.
    pub async fn query(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, PgError> {
        let conn = self.pool.get().await?;
        conn.client()
            .query(sql, params)
            .await
            .map_err(PgError::Query)
    }

    /// Execute a query expecting exactly one row.
    pub async fn query_one(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, PgError> {
        let conn = self.pool.get().await?;
        conn.client()
            .query_one(sql, params)
            .await
            .map_err(PgError::Query)
    }

    /// Execute a query expecting zero or one rows.
    pub async fn query_opt(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, PgError> {
        let conn = self.pool.get().await?;
        conn.client()
            .query_opt(sql, params)
            .await
            .map_err(PgError::Query)
    }

    /// Execute multiple semicolon-separated statements (e.g. DDL scripts).
    pub async fn batch_execute(&self, sql: &str) -> Result<(), PgError> {
        let conn = self.pool.get().await?;
        conn.client()
            .batch_execute(sql)
            .await
            .map_err(PgError::Query)
    }

    /// Begin a transaction.  The connection is held for the lifetime of the
    /// returned [`PgTransaction`]; call `.commit()` or `.rollback()`.
    pub async fn transaction(&self) -> Result<PgTransaction, PgError> {
        let conn = self.pool.get().await?;
        // Arm the transaction guard BEFORE sending BEGIN (RS-18): if the
        // BEGIN await is cancelled or errors, Drop discards the connection
        // instead of letting `PooledConn::drop` recycle it with an
        // open/in-flight transaction that the next borrower would join.
        let tx = PgTransaction { conn, done: false };
        tx.conn
            .client()
            .execute("BEGIN", &[])
            .await
            .map_err(PgError::Query)?;
        Ok(tx)
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
    pub async fn execute(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64, PgError> {
        self.conn
            .client()
            .execute(sql, params)
            .await
            .map_err(PgError::Query)
    }

    /// Query inside the transaction.
    pub async fn query(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, PgError> {
        self.conn
            .client()
            .query(sql, params)
            .await
            .map_err(PgError::Query)
    }

    /// Query a single row inside the transaction.
    pub async fn query_one(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, PgError> {
        self.conn
            .client()
            .query_one(sql, params)
            .await
            .map_err(PgError::Query)
    }

    /// Commit the transaction. The raw session lease is discarded on drop.
    pub async fn commit(mut self) -> Result<(), PgError> {
        self.conn
            .client()
            .execute("COMMIT", &[])
            .await
            .map_err(PgError::Query)?;
        self.done = true;
        Ok(())
    }

    /// Roll back the transaction. The raw session lease is discarded on drop.
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

    #[tokio::test]
    async fn db_requires_pool_in_state() {
        // Verify that FromRequestParts returns an error response when the pool
        // is absent — we can test this without a real DB connection.
        use bytes::Bytes;
        use http::Method;
        use neutron::handler::Request as NeutronRequest;

        let req = NeutronRequest::new(
            Method::GET,
            "/test".parse().unwrap(),
            http::HeaderMap::new(),
            Bytes::new(),
        );
        let result = Db::from_parts(&req).await;
        assert!(result.is_err());
        // unwrap_err() requires Debug on Ok variant; match instead
        match result {
            Err(resp) => assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR),
            Ok(_) => panic!("expected Err"),
        }
    }

    #[test]
    fn pg_config_fields() {
        let cfg = PgConfig::new().host("pg.local").dbname("app").user("svc");
        assert_eq!(cfg.host, "pg.local");
        assert_eq!(cfg.dbname, "app");
        assert_eq!(cfg.user, "svc");
    }
}

#[cfg(test)]
mod begin_cancellation_tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    async fn startup(stream: &mut tokio::net::TcpStream) {
        let mut len = stream.read_u32().await.unwrap() as usize;
        if len == 8 {
            let _ssl_request = stream.read_u32().await.unwrap();
            stream.write_all(b"N").await.unwrap();
            len = stream.read_u32().await.unwrap() as usize;
        }
        let mut bytes = vec![0; len - 4];
        stream.read_exact(&mut bytes).await.unwrap();
        // AuthenticationOk and ReadyForQuery (idle).
        stream
            .write_all(b"R\0\0\0\x08\0\0\0\0Z\0\0\0\x05I")
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn cancelled_in_flight_begin_discards_connection() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (seen, began) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            startup(&mut first).await;
            loop {
                let tag = first.read_u8().await.unwrap();
                let len = first.read_u32().await.unwrap() as usize;
                let mut body = vec![0; len - 4];
                first.read_exact(&mut body).await.unwrap();
                if tag == b'P' && body.windows(5).any(|bytes| bytes == b"BEGIN") {
                    break;
                }
            }
            seen.send(()).unwrap();
            // Leave BEGIN unresolved; a recycled first connection would avoid this accept.
            let (mut second, _) = listener.accept().await.unwrap();
            startup(&mut second).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        });
        let pool = PgPool::new(
            crate::pool::PgConfig::new()
                .host("127.0.0.1")
                .port(port)
                .user("test")
                .password("test")
                .max_size(1),
        );
        let task_pool = pool.clone();
        let transaction = tokio::spawn(async move { Db { pool: task_pool }.transaction().await });
        tokio::time::timeout(Duration::from_secs(5), began)
            .await
            .unwrap()
            .unwrap();
        transaction.abort();
        let _ = transaction.await;
        let _connection = tokio::time::timeout(Duration::from_secs(5), pool.get())
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn raw_begin_and_set_success_and_cancellation_never_repool_session() {
        for cancel in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let (seen, received) = tokio::sync::oneshot::channel();
            let peer = tokio::spawn(async move {
                let (mut first, _) = listener.accept().await.unwrap();
                startup(&mut first).await;
                assert_eq!(first.read_u8().await.unwrap(), b'Q');
                let len = first.read_u32().await.unwrap() as usize;
                let mut sql = vec![0; len - 4];
                first.read_exact(&mut sql).await.unwrap();
                assert!(sql.windows(5).any(|w| w == b"BEGIN"));
                assert!(sql.windows(3).any(|w| w == b"SET"));
                seen.send(()).unwrap();
                if !cancel {
                    first
                        .write_all(b"C\0\0\0\x0aBEGIN\0Z\0\0\0\x05T")
                        .await
                        .unwrap();
                }
                let (mut second, _) = listener.accept().await.unwrap();
                startup(&mut second).await;
            });
            let pool = PgPool::new(
                crate::pool::PgConfig::default()
                    .host("127.0.0.1")
                    .port(port)
                    .max_size(1),
            );
            let pool2 = pool.clone();
            let query = tokio::spawn(async move {
                let conn = pool2.get().await.unwrap();
                conn.raw_client_nonreusable()
                    .batch_execute("BEGIN; SET application_name='raw-fixture'")
                    .await
            });
            tokio::time::timeout(Duration::from_secs(1), received)
                .await
                .unwrap()
                .unwrap();
            if cancel {
                query.abort();
                let _ = query.await;
            } else {
                query.await.unwrap().unwrap();
            }
            let _clean = tokio::time::timeout(Duration::from_secs(1), pool.get())
                .await
                .expect("raw session was reused")
                .unwrap();
            tokio::time::timeout(Duration::from_secs(1), peer)
                .await
                .unwrap()
                .unwrap();
        }
    }
}
