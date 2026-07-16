//! Transaction management methods for the executor.
//!
//! Handles BEGIN, COMMIT, ROLLBACK, and savepoint operations.  When the
//! storage engine supports MVCC the work is delegated to the engine's
//! snapshot-isolation layer; otherwise a legacy clone-all-tables approach
//! is used.  All specialty stores (KV, Graph, Doc, Datalog, FTS, TimeSeries,
//! Blob, Vector) are snapshotted at BEGIN and restored on ROLLBACK.

use std::collections::HashMap;

use super::session::CrossModelSnapshots;
use super::{ExecError, ExecResult, Executor};

impl Executor {
    // ====================================================================
    // Transaction management
    // ====================================================================

    /// BEGIN -- start a new transaction.
    ///
    /// When the storage engine supports MVCC, this delegates to the engine's
    /// snapshot-based transaction management. Otherwise, falls back to the
    /// legacy approach of cloning all table data for rollback.
    pub(super) async fn begin_transaction(&self) -> Result<ExecResult, ExecError> {
        let sess = self.current_session();
        let mut txn = sess.txn_state.write().await;
        if txn.active {
            return Ok(ExecResult::Command {
                tag: "WARNING: already in a transaction".into(),
                rows_affected: 0,
            });
        }

        if self.storage.supports_mvcc() {
            // MVCC engine handles snapshot isolation internally.
            self.storage.begin_txn().await?;
        } else {
            // Legacy: capture a snapshot of every table's rows for rollback.
            let table_names = self.catalog.table_names().await;
            let mut snapshot = HashMap::new();
            for name in &table_names {
                if let Ok(rows) = self.storage.scan(name).await {
                    snapshot.insert(name.clone(), rows);
                }
            }
            txn.snapshot = Some(snapshot);
        }

        // Capture cross-model snapshots for rollback of all specialty stores.
        txn.cross_model = Some(CrossModelSnapshots {
            kv: Some(self.kv_store.txn_snapshot()),
            graph: Some(self.graph_store.read().txn_snapshot()),
            doc: Some(self.doc_store.read().txn_snapshot()),
            datalog: Some(self.datalog_store.read().txn_snapshot()),
            fts: Some(self.fts_index.read().begin_undo_log()),
            ts: Some(self.ts_store.read().txn_snapshot()),
            blob: Some(self.blob_store.read().txn_snapshot()),
            vector: Some(self.vector_indexes.read().clone()),
        });
        txn.security_snapshot = Some(self.security.read().clone_policy_state());
        txn.security_pending = None;
        txn.security_savepoints.clear();
        txn.policy_dirty = false;
        txn.gin_dirty = false;
        txn.derived_dirty_tables.clear();

        txn.active = true;
        self.metrics.open_transactions.inc();

        Ok(ExecResult::Command {
            tag: "BEGIN".into(),
            rows_affected: 0,
        })
    }

    /// COMMIT -- end the transaction, making all changes permanent.
    pub(super) async fn commit_transaction(&self) -> Result<ExecResult, ExecError> {
        let sess = self.current_session();
        let mut txn = sess.txn_state.write().await;

        if !txn.active {
            return Ok(ExecResult::Command {
                tag: "WARNING: no transaction in progress".into(),
                rows_affected: 0,
            });
        }

        // Policy metadata is durable before COMMIT is acknowledged. A
        // persistence failure leaves the transaction active so callers can
        // retry or roll it back without exposing a partial security catalog.
        if txn.policy_dirty {
            let pending = txn.security_pending.take().ok_or_else(|| {
                ExecError::Runtime("policy transaction has no staged security catalog".into())
            })?;
            *self.security.write() = pending;
            self.bump_policy_gen();
            #[cfg(feature = "server")]
            {
                if let Err(error) = self
                    .storage
                    .flush_schema()
                    .await
                    .map_err(ExecError::Storage)
                {
                    let staged = self.security.read().clone_policy_state();
                    *self.security.write() = txn
                        .security_snapshot
                        .as_ref()
                        .expect("BEGIN captured security state")
                        .clone_policy_state();
                    txn.security_pending = Some(staged);
                    self.bump_policy_gen();
                    return Err(error);
                }
                if let Err(error) = self.persist_catalog().await {
                    let staged = self.security.read().clone_policy_state();
                    *self.security.write() = txn
                        .security_snapshot
                        .as_ref()
                        .expect("BEGIN captured security state")
                        .clone_policy_state();
                    txn.security_pending = Some(staged);
                    self.bump_policy_gen();
                    return Err(error);
                }
            }
        }

        if self.storage.supports_mvcc()
            && let Err(error) = self.storage.commit_txn().await
        {
            if txn.policy_dirty
                && let Some(previous) = txn.security_snapshot.as_ref()
            {
                let staged = self.security.read().clone_policy_state();
                *self.security.write() = previous.clone_policy_state();
                txn.security_pending = Some(staged);
                self.bump_policy_gen();
                #[cfg(feature = "server")]
                self.persist_catalog().await?;
            }
            return Err(error.into());
        }

        let gin_dirty = txn.gin_dirty;
        let derived_dirty_tables: Vec<String> = txn.derived_dirty_tables.iter().cloned().collect();
        txn.active = false;
        txn.snapshot = None;
        txn.savepoints.clear();
        txn.cross_model = None; // Discard cross-model snapshots on commit
        txn.security_snapshot = None;
        txn.security_pending = None;
        txn.security_savepoints.clear();
        txn.policy_dirty = false;
        txn.gin_dirty = false;
        txn.derived_dirty_tables.clear();
        self.metrics.open_transactions.dec();
        drop(txn);

        // GIN is shared across sessions, so DML deliberately leaves it on the
        // committed image while a transaction is open. Refresh only after the
        // storage commit has succeeded and the session is no longer active.
        if gin_dirty {
            self.mark_gin_committed_write();
            self.rebuild_all_gin_indexes().await;
        }
        for table in derived_dirty_tables {
            self.rebuild_table_derived_state(&table).await;
        }

        Ok(ExecResult::Command {
            tag: "COMMIT".into(),
            rows_affected: 0,
        })
    }

    /// ROLLBACK -- abort the transaction, undoing all changes.
    ///
    /// With MVCC, this marks the transaction as aborted so its writes become
    /// invisible. Without MVCC, restores all tables from the cloned snapshot.
    pub(super) async fn rollback_transaction(&self) -> Result<ExecResult, ExecError> {
        let sess = self.current_session();
        let mut txn = sess.txn_state.write().await;

        if self.storage.supports_mvcc() {
            self.storage.abort_txn().await?;
        } else if let Some(snapshot) = txn.snapshot.take() {
            // Legacy: restore each table to its snapshotted state.
            for (table_name, original_rows) in &snapshot {
                if let Ok(current_rows) = self.storage.scan(table_name).await
                    && !current_rows.is_empty()
                {
                    let positions: Vec<usize> = (0..current_rows.len()).collect();
                    let _ = self.storage.delete(table_name, &positions).await;
                }
                for row in original_rows {
                    let _ = self.storage.insert(table_name, row.clone()).await;
                }
            }
        }

        // Restore cross-model snapshots (all specialty stores).
        if let Some(cm) = txn.cross_model.take() {
            if let Some(kv_snap) = cm.kv {
                self.kv_store.txn_restore(kv_snap);
            }
            if let Some(graph_snap) = cm.graph {
                self.graph_store.write().txn_restore(graph_snap);
            }
            if let Some(doc_snap) = cm.doc {
                self.doc_store.write().txn_restore(doc_snap);
            }
            if let Some(datalog_snap) = cm.datalog {
                self.datalog_store.write().txn_restore(datalog_snap);
            }
            if let Some(fts_log) = cm.fts {
                self.fts_index.write().undo(fts_log);
            }
            if let Some(ts_snap) = cm.ts {
                self.ts_store.write().txn_restore(ts_snap);
            }
            if let Some(blob_snap) = cm.blob {
                self.blob_store.write().txn_restore(blob_snap);
            }
            if let Some(vector_snap) = cm.vector {
                *self.vector_indexes.write() = vector_snap;
            }
        }

        let derived_dirty_tables: Vec<String> = txn.derived_dirty_tables.iter().cloned().collect();
        txn.active = false;
        txn.snapshot = None;
        txn.savepoints.clear();
        txn.security_snapshot = None;
        txn.security_pending = None;
        txn.security_savepoints.clear();
        txn.policy_dirty = false;
        txn.gin_dirty = false;
        txn.derived_dirty_tables.clear();

        self.metrics.open_transactions.dec();
        drop(txn);

        // Incremental index maintenance may have observed transaction-local
        // rows. Rebuild after abort from the now-authoritative committed image.
        for table in derived_dirty_tables {
            self.rebuild_table_derived_state(&table).await;
        }

        Ok(ExecResult::Command {
            tag: "ROLLBACK".into(),
            rows_affected: 0,
        })
    }

    /// SAVEPOINT -- capture current state within a transaction.
    pub(super) async fn execute_savepoint(&self, name: &str) -> Result<ExecResult, ExecError> {
        let sess = self.current_session();
        let mut txn = sess.txn_state.write().await;
        if !txn.active {
            return Err(ExecError::Unsupported(
                "SAVEPOINT outside of transaction".into(),
            ));
        }

        if self.storage.supports_mvcc() {
            self.storage.savepoint(name).await?;
        } else {
            // Legacy: capture current state of all tables
            let table_names = self.catalog.table_names().await;
            let mut snapshot = HashMap::new();
            for tbl in &table_names {
                if let Ok(rows) = self.storage.scan(tbl).await {
                    snapshot.insert(tbl.clone(), rows);
                }
            }
            txn.savepoints.push((name.to_string(), snapshot));
        }
        let security_snapshot = txn
            .security_pending
            .as_ref()
            .map(|security| security.clone_policy_state())
            .unwrap_or_else(|| self.security.read().clone_policy_state());
        txn.security_savepoints
            .push((name.to_string(), security_snapshot));

        Ok(ExecResult::Command {
            tag: "SAVEPOINT".to_string(),
            rows_affected: 0,
        })
    }

    /// RELEASE SAVEPOINT -- discard a savepoint (keep changes).
    pub(super) async fn execute_release_savepoint(
        &self,
        name: &str,
    ) -> Result<ExecResult, ExecError> {
        let sess = self.current_session();
        let mut txn = sess.txn_state.write().await;
        if self.storage.supports_mvcc() {
            self.storage.release_savepoint(name).await?;
        } else {
            if let Some(pos) = txn.savepoints.iter().rposition(|(n, _)| n == name) {
                txn.savepoints.truncate(pos);
            }
        }
        if let Some(pos) = txn.security_savepoints.iter().rposition(|(n, _)| n == name) {
            txn.security_savepoints.truncate(pos);
        }
        Ok(ExecResult::Command {
            tag: "RELEASE SAVEPOINT".into(),
            rows_affected: 0,
        })
    }

    /// ROLLBACK TO SAVEPOINT -- restore state to the named savepoint.
    pub(super) async fn execute_rollback_to_savepoint(
        &self,
        name: &str,
    ) -> Result<ExecResult, ExecError> {
        let sess = self.current_session();
        let mut txn = sess.txn_state.write().await;
        if self.storage.supports_mvcc() {
            self.storage.rollback_to_savepoint(name).await?;
        } else {
            let pos = txn.savepoints.iter().rposition(|(n, _)| n == name);
            if let Some(pos) = pos {
                let (_, snapshot) = txn.savepoints[pos].clone();
                for (table_name, original_rows) in &snapshot {
                    if let Ok(current_rows) = self.storage.scan(table_name).await
                        && !current_rows.is_empty()
                    {
                        let positions: Vec<usize> = (0..current_rows.len()).collect();
                        let _ = self.storage.delete(table_name, &positions).await;
                    }
                    for row in original_rows {
                        let _ = self.storage.insert(table_name, row.clone()).await;
                    }
                }
                txn.savepoints.truncate(pos + 1);
            } else {
                return Err(ExecError::Unsupported(format!(
                    "savepoint {name} does not exist"
                )));
            }
        }
        let security_pos = txn
            .security_savepoints
            .iter()
            .rposition(|(n, _)| n == name)
            .ok_or_else(|| ExecError::Unsupported(format!("savepoint {name} does not exist")))?;
        let security_snapshot = txn.security_savepoints[security_pos].1.clone_policy_state();
        txn.security_pending = Some(security_snapshot);
        self.bump_policy_gen();
        txn.security_savepoints.truncate(security_pos + 1);
        txn.policy_dirty = true;
        let derived_dirty_tables: Vec<String> = txn.derived_dirty_tables.iter().cloned().collect();
        drop(txn);
        for table in derived_dirty_tables {
            self.rebuild_table_derived_state(&table).await;
        }
        Ok(ExecResult::Command {
            tag: "ROLLBACK".into(),
            rows_affected: 0,
        })
    }
}
