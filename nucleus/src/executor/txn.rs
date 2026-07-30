//! Transaction management methods for the executor.
//!
//! Handles BEGIN, COMMIT, ROLLBACK, and savepoint operations.  When the
//! storage engine supports MVCC the work is delegated to the engine's
//! snapshot-isolation layer; otherwise a legacy clone-all-tables approach
//! is used.
//!
//! Eight specialty stores (KV strings, graph, document, datalog, FTS, time
//! series, blob, vector) are enlisted through the per-session write-set in
//! `super::cross_model`: before-images are captured lazily at this session's
//! first write to each store, and ROLLBACK reverts only the entities this
//! session touched.  KV collections, the columnar analytics store, streams,
//! and CDC are **not** enlisted and are never rolled back.

use std::collections::HashMap;

use super::cross_model::{CrossModelLevel, CrossModelTxn};
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

        // Arm per-session cross-model tracking. Before-images are captured
        // lazily, at this session's first write to each store, so a SQL-only
        // transaction no longer deep-clones every specialty store (including
        // every HNSW graph) just to open.
        *sess.cross_model.lock() = Some(CrossModelTxn::new());
        txn.security_snapshot = Some(self.security.read().clone_policy_state());
        txn.security_pending = None;
        txn.security_savepoints.clear();
        txn.policy_dirty = false;
        txn.gin_dirty = false;
        txn.derived_dirty_tables.clear();
        txn.engine_snapshots.clear();
        txn.engine_savepoints.clear();
        txn.aborted = false;

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

        // COMMIT of an aborted transaction becomes a ROLLBACK (PostgreSQL): the
        // transaction hit a statement error, so nothing may be committed.
        if txn.aborted {
            drop(txn);
            self.rollback_transaction().await?;
            return Ok(ExecResult::Command {
                tag: "ROLLBACK".into(),
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
        txn.engine_savepoints.clear();
        txn.security_snapshot = None;
        txn.security_pending = None;
        txn.security_savepoints.clear();
        txn.policy_dirty = false;
        txn.gin_dirty = false;
        txn.derived_dirty_tables.clear();
        txn.engine_snapshots.clear();
        txn.engine_savepoints.clear();
        *sess.cross_model.lock() = None; // Discard the write-set on commit
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
            // Legacy: restore each table to its snapshotted state. Positions
            // come from scan_physical, never from `0..len` — an engine is free
            // to address rows by something other than a dense scan ordinal
            // (the paged engine uses physical (page, slot) addresses), and
            // synthesising ordinals there would write over unrelated pages.
            for (table_name, original_rows) in &snapshot {
                if let Ok(current_rows) = self.storage.scan_physical(table_name).await
                    && !current_rows.is_empty()
                {
                    let positions: Vec<usize> =
                        current_rows.iter().map(|(pos, _)| *pos).collect();
                    let _ = self.storage.delete(table_name, &positions).await;
                }
                for row in original_rows {
                    let _ = self.storage.insert(table_name, row.clone()).await;
                }
            }
        }

        // Revert cross-model writes. Scoped to this session's write-set: an
        // entity another session wrote since this BEGIN is left alone.
        // The guard is dropped before the stores are touched so a concurrent
        // specialty mutation (which takes a store lock, then this one) can
        // never deadlock against the reverse order.
        let cross_model = sess.cross_model.lock().take();
        if let Some(cm) = cross_model {
            self.cross_model_revert(cm.base, cm.fts_ops);
        }

        // Undo writes to tables served by a per-table engine. Those engines
        // provide no transaction of their own, so this is the only thing that
        // reverts them — see `storage_for_write`.
        let engine_snapshots: Vec<(String, Vec<crate::types::Row>)> =
            txn.engine_snapshots.drain().collect();
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
        txn.engine_snapshots.clear();
        txn.engine_savepoints.clear();

        self.metrics.open_transactions.dec();
        drop(txn);

        for (table, original) in &engine_snapshots {
            self.restore_table_from(table, original).await;
        }

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
        // Per-table-engine tables: capture the CURRENT state of the ones this
        // transaction has already written. A table first written after this
        // savepoint needs no entry — its base image, taken at that first write,
        // is already the state as of this savepoint, because nothing had
        // touched it before.
        {
            let touched: Vec<String> = txn.engine_snapshots.keys().cloned().collect();
            let mut level = HashMap::new();
            for tbl in touched {
                if let Ok(rows) = self.storage_for(&tbl).scan(&tbl).await {
                    level.insert(tbl, rows);
                }
            }
            txn.engine_savepoints.push((name.to_string(), level));
        }
        let security_snapshot = txn
            .security_pending
            .as_ref()
            .map(|security| security.clone_policy_state())
            .unwrap_or_else(|| self.security.read().clone_policy_state());
        txn.security_savepoints
            .push((name.to_string(), security_snapshot));

        // Open a cross-model level for this savepoint. Its before-images are
        // captured lazily at the first write after this point, so a savepoint
        // in a SQL-only transaction costs nothing.
        if let Some(cm) = sess.cross_model.lock().as_mut() {
            let mark = cm.fts_ops.len();
            cm.savepoints
                .push((name.to_string(), CrossModelLevel::default(), mark));
        }

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
        if let Some(pos) = txn.engine_savepoints.iter().rposition(|(n, _)| n == name) {
            txn.engine_savepoints.truncate(pos);
        }
        if let Some(pos) = txn.security_savepoints.iter().rposition(|(n, _)| n == name) {
            txn.security_savepoints.truncate(pos);
        }
        // Releasing keeps the writes; every level below already recorded them,
        // so the level is simply discarded.
        if let Some(cm) = sess.cross_model.lock().as_mut()
            && let Some(pos) = cm.savepoints.iter().rposition(|(n, _, _)| n == name)
        {
            cm.savepoints.truncate(pos);
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
                    // scan_physical, not `0..len` — see rollback above.
                    if let Ok(current_rows) = self.storage.scan_physical(table_name).await
                        && !current_rows.is_empty()
                    {
                        let positions: Vec<usize> =
                            current_rows.iter().map(|(pos, _)| *pos).collect();
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
        // Per-table-engine tables: revert to this level's image. Tables first
        // written after the savepoint are absent from the level and must be
        // reverted to their base image, which is the state as of this savepoint
        // for exactly the same reason they were absent.
        let engine_revert: Vec<(String, Vec<crate::types::Row>)> = {
            match txn.engine_savepoints.iter().rposition(|(n, _)| n == name) {
                Some(pos) => {
                    let level = txn.engine_savepoints[pos].1.clone();
                    let mut out: Vec<(String, Vec<crate::types::Row>)> = level.into_iter().collect();
                    for (tbl, base) in txn.engine_snapshots.iter() {
                        if !out.iter().any(|(t, _)| t == tbl) {
                            out.push((tbl.clone(), base.clone()));
                        }
                    }
                    txn.engine_savepoints.truncate(pos + 1);
                    out
                }
                None => Vec::new(),
            }
        };

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

        for (table, original) in &engine_revert {
            self.restore_table_from(table, original).await;
        }

        // Revert cross-model writes made after the savepoint. Taken out from
        // under the mutex first so the stores are only locked afterwards.
        let reverted = {
            let mut guard = sess.cross_model.lock();
            guard.as_mut().and_then(|cm| {
                let pos = cm.savepoints.iter().rposition(|(n, _, _)| n == name)?;
                let mark = cm.savepoints[pos].2;
                let level = std::mem::take(&mut cm.savepoints[pos].1);
                let fts_tail = cm.fts_ops.split_off(mark.min(cm.fts_ops.len()));
                cm.savepoints.truncate(pos + 1);
                Some((level, fts_tail))
            })
        };
        if let Some((level, fts_tail)) = reverted {
            self.cross_model_revert(level, fts_tail);
        }

        for table in derived_dirty_tables {
            self.rebuild_table_derived_state(&table).await;
        }
        Ok(ExecResult::Command {
            tag: "ROLLBACK".into(),
            rows_affected: 0,
        })
    }
}
