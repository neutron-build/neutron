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
/// runs arbitrary SQL, and discards the raw session lease — no per-request connection
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
    async fn from_parts(req: &Request) -> Result<Self, Response> {
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
        conn.client()
            .execute(sql, params)
            .await
            .map_err(NucleusError::Query)
    }

    /// Execute a query and return all matching rows.
    pub async fn query(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, NucleusError> {
        let conn = self.pool.get().await?;
        conn.client()
            .query(sql, params)
            .await
            .map_err(NucleusError::Query)
    }

    /// Execute a query expecting exactly one row.
    pub async fn query_one(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, NucleusError> {
        let conn = self.pool.get().await?;
        conn.client()
            .query_one(sql, params)
            .await
            .map_err(NucleusError::Query)
    }

    /// Execute a query expecting zero or one rows.
    pub async fn query_opt(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, NucleusError> {
        let conn = self.pool.get().await?;
        conn.client()
            .query_opt(sql, params)
            .await
            .map_err(NucleusError::Query)
    }

    /// Begin a transaction.  The connection is held for the lifetime of the
    /// returned [`NucleusTransaction`]; call `.commit()` or `.rollback()`.
    pub async fn transaction(&self) -> Result<NucleusTransaction, NucleusError> {
        let conn = self.pool.get().await?;
        // Arm the transaction guard BEFORE sending BEGIN (RS-18): if the
        // BEGIN await is cancelled (future dropped) or errors, `tx`'s Drop
        // takes the client out of the pooled connection so it is discarded
        // instead of recycled with an in-flight/open transaction. The old
        // ordering constructed the guard only after BEGIN resolved, so a
        // cancellation between the two recycled the connection through the
        // ordinary `PooledConn::drop` and the next borrower unknowingly
        // executed inside this transaction.
        let tx = NucleusTransaction { conn, done: false };
        tx.conn
            .client()
            .execute("BEGIN", &[])
            .await
            .map_err(NucleusError::Query)?;
        Ok(tx)
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
        self.conn
            .client()
            .execute(sql, params)
            .await
            .map_err(NucleusError::Query)
    }

    /// Query inside the transaction.
    pub async fn query(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, NucleusError> {
        self.conn
            .client()
            .query(sql, params)
            .await
            .map_err(NucleusError::Query)
    }

    /// Commit the transaction. The raw session lease is discarded on drop.
    pub async fn commit(mut self) -> Result<(), NucleusError> {
        self.conn
            .client()
            .execute("COMMIT", &[])
            .await
            .map_err(NucleusError::Query)?;
        self.done = true;
        Ok(())
    }

    /// Roll back the transaction. The raw session lease is discarded on drop.
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
        let pool = NucleusPool::new(
            crate::pool::NucleusConfig::new("127.0.0.1", port, "test")
                .user("test")
                .max_size(1)
                .sslmode(crate::pool::SslMode::Disable),
        );
        let task_pool = pool.clone();
        let transaction = tokio::spawn(async move { Db { pool: task_pool }.transaction().await });
        tokio::time::timeout(Duration::from_secs(1), began)
            .await
            .unwrap()
            .unwrap();
        transaction.abort();
        let _ = transaction.await;
        let _connection = tokio::time::timeout(Duration::from_secs(1), pool.get())
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), server)
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
            let pool = NucleusPool::new(
                crate::pool::NucleusConfig::new("127.0.0.1", port, "test").max_size(1),
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
