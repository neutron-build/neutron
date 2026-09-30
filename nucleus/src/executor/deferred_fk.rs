//! Deferred foreign-key checking: `DEFERRABLE` / `INITIALLY DEFERRED` and
//! `SET CONSTRAINTS`.
//!
//! A foreign key declared DEFERRABLE is checked at COMMIT instead of at the end
//! of each statement while the session is inside an explicit transaction and the
//! constraint is deferred (INITIALLY DEFERRED, or `SET CONSTRAINTS ... DEFERRED`).
//! Outside an explicit transaction the statement is its own transaction, so the
//! check stays immediate.
//!
//! Only the key that a write touched is remembered. Each entry is judged again
//! against the state at COMMIT (violated when a child row carries the key and no
//! parent row does), so a write rolled back to a savepoint, or repaired later in
//! the same transaction, leaves nothing behind to fail.
//!
//! Only foreign keys are deferrable. UNIQUE and PRIMARY KEY are still checked
//! per statement and reject the DEFERRABLE clause (see
//! `validate_immediate_constraint_characteristics`).

use std::sync::Arc;

use crate::catalog::{Deferrable, TableConstraint};
use crate::types::Value;

use super::session::Session;
use super::{ExecError, ExecResult, Executor};

/// Reserved setting name a parsed `SET CONSTRAINTS` travels under, see
/// `sql::rewrite_set_constraints`.
pub(crate) const SET_CONSTRAINTS_SETTING: &str = "nucleus.set_constraints";

/// One foreign-key key that was written while its check was deferred.
#[derive(Clone, PartialEq)]
pub(super) struct PendingFk {
    pub name: Option<String>,
    pub child_table: String,
    pub columns: Vec<String>,
    pub ref_table: String,
    pub ref_columns: Vec<String>,
    pub key: Vec<Value>,
}

/// Per-session deferral state for the open transaction.
#[derive(Default, Clone)]
pub(super) struct DeferredFks {
    /// `SET CONSTRAINTS ALL`: `Some(true)` = deferred, `Some(false)` = immediate.
    all: Option<bool>,
    /// `SET CONSTRAINTS name`: overrides `all` for that constraint.
    named: Vec<(String, bool)>,
    pending: Vec<PendingFk>,
}

impl DeferredFks {
    fn is_deferred(&self, name: Option<&str>, deferrable: Deferrable) -> bool {
        if deferrable == Deferrable::NotDeferrable {
            return false;
        }
        if let Some(name) = name
            && let Some((_, deferred)) = self.named.iter().find(|(n, _)| n == name)
        {
            return *deferred;
        }
        self.all
            .unwrap_or(deferrable == Deferrable::InitiallyDeferred)
    }
}

impl Session {
    /// Forget deferral state: the transaction that owned it has ended.
    pub(super) fn reset_deferred_fks(&self) {
        *self.deferred_fks.lock() = DeferredFks::default();
        self.deferred_fk_savepoints.lock().clear();
    }

    pub(super) fn has_pending_deferred_fks(&self) -> bool {
        !self.deferred_fks.lock().pending.is_empty()
    }

    pub(super) fn savepoint_deferred_fks(&self, name: &str) {
        let snapshot = self.deferred_fks.lock().clone();
        self.deferred_fk_savepoints
            .lock()
            .push((name.to_string(), snapshot));
    }

    pub(super) fn release_deferred_fks_savepoint(&self, name: &str) {
        let mut snapshots = self.deferred_fk_savepoints.lock();
        if let Some(pos) = snapshots.iter().rposition(|(n, _)| n == name) {
            snapshots.truncate(pos);
        }
    }

    pub(super) fn rollback_deferred_fks_savepoint(&self, name: &str) {
        let mut snapshots = self.deferred_fk_savepoints.lock();
        if let Some(pos) = snapshots.iter().rposition(|(n, _)| n == name) {
            *self.deferred_fks.lock() = snapshots[pos].1.clone();
            snapshots.truncate(pos + 1);
        }
    }
}

impl Executor {
    /// True when a check of this foreign key must wait for COMMIT.
    pub(super) fn fk_check_deferred(&self, name: Option<&str>, deferrable: Deferrable) -> bool {
        if deferrable == Deferrable::NotDeferrable {
            return false;
        }
        let sess = self.current_session();
        sess.txn_active.load(std::sync::atomic::Ordering::SeqCst)
            && sess.deferred_fks.lock().is_deferred(name, deferrable)
    }

    /// Remember a key whose foreign-key check was deferred.
    pub(super) fn defer_fk_check(&self, pending: PendingFk) {
        let sess = self.current_session();
        let mut state = sess.deferred_fks.lock();
        if !state.pending.contains(&pending) {
            state.pending.push(pending);
        }
    }

    /// Judge one remembered key against the current state.
    async fn verify_pending_fk(&self, p: &PendingFk) -> Result<(), ExecError> {
        let violation = || {
            ExecError::ConstraintViolation(format!(
                "insert or update on table \"{}\" violates foreign key constraint{} referencing \"{}\"",
                p.child_table,
                p.name
                    .as_ref()
                    .map(|n| format!(" \"{n}\""))
                    .unwrap_or_default(),
                p.ref_table
            ))
        };
        // A dropped table or constraint has nothing left to check.
        let child_def = self.get_table(&p.child_table).await?;
        let ref_def = self.get_table(&p.ref_table).await?;
        let indices = |def: &crate::catalog::TableDef, names: &[String]| -> Option<Vec<usize>> {
            let v: Vec<usize> = names.iter().filter_map(|c| def.column_index(c)).collect();
            (v.len() == names.len()).then_some(v)
        };
        let (Some(child_idx), Some(ref_idx)) = (
            indices(&child_def, &p.columns),
            indices(&ref_def, &p.ref_columns),
        ) else {
            return Err(ExecError::ConstraintViolation(
                "pending foreign-key columns are missing; cannot verify deferred constraint".into(),
            ));
        };
        let matches = |row: &[Value], idx: &[usize]| {
            idx.iter()
                .zip(p.key.iter())
                .all(|(&i, k)| i < row.len() && &row[i] == k)
        };
        let parents = self.storage_for(&p.ref_table).scan(&p.ref_table).await?;
        if parents.iter().any(|row| matches(row, &ref_idx)) {
            return Ok(());
        }
        let children = self
            .storage_for(&p.child_table)
            .scan(&p.child_table)
            .await?;
        if children.iter().any(|row| matches(row, &child_idx)) {
            return Err(violation());
        }
        Ok(())
    }

    /// COMMIT-time check of every deferred key. Called before the transaction
    /// commits; on a violation the transaction is rolled back, as PostgreSQL
    /// does for a failed COMMIT.
    pub(super) async fn check_deferred_fks_at_commit(&self) -> Result<(), ExecError> {
        let sess = self.current_session();
        let pending = sess.deferred_fks.lock().pending.clone();
        if pending.is_empty() {
            return Ok(());
        }
        for p in &pending {
            if let Err(error) = self.verify_pending_fk(p).await {
                self.rollback_transaction().await?;
                return Err(error);
            }
        }
        Ok(())
    }

    /// `SET CONSTRAINTS { ALL | name [, ...] } { DEFERRED | IMMEDIATE }`.
    /// `spec` is `"<ALL|name,name>|<DEFERRED|IMMEDIATE>"`.
    pub(super) async fn execute_set_constraints(
        &self,
        spec: &str,
    ) -> Result<ExecResult, ExecError> {
        let (targets, mode) = spec
            .rsplit_once('|')
            .ok_or_else(|| ExecError::Runtime("malformed SET CONSTRAINTS".into()))?;
        let deferred = mode == "DEFERRED";
        let sess: Arc<Session> = self.current_session();
        let tag = || ExecResult::Command {
            tag: "SET CONSTRAINTS".into(),
            rows_affected: 0,
        };
        // Outside a transaction block PostgreSQL warns and does nothing.
        if !sess.txn_active.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(tag());
        }
        let names: Vec<String> = if targets == "ALL" {
            Vec::new()
        } else {
            targets.split(',').map(|n| n.to_string()).collect()
        };
        if !names.is_empty() {
            // Every name must be a foreign key of some table, and deferrable.
            let tables = self.catalog.list_tables().await;
            for name in &names {
                let found = tables
                    .iter()
                    .flat_map(|t| &t.constraints)
                    .find_map(|c| match c {
                        TableConstraint::ForeignKey {
                            name: Some(n),
                            deferrable,
                            ..
                        } if n == name => Some(*deferrable),
                        _ => None,
                    });
                match found {
                    Some(Deferrable::NotDeferrable) => {
                        return Err(ExecError::Runtime(format!(
                            "constraint \"{name}\" is not deferrable"
                        )));
                    }
                    Some(_) => {}
                    None => {
                        return Err(ExecError::Runtime(format!(
                            "constraint \"{name}\" does not exist"
                        )));
                    }
                }
            }
        }
        if !deferred {
            // Validate before changing modes or discarding any pending check.
            // A failure and subsequent ROLLBACK TO must retain the original
            // deferred keys. Successful switches are savepoint-scoped too.
            let due: Vec<PendingFk> = sess
                .deferred_fks
                .lock()
                .pending
                .iter()
                .filter(|p| names.is_empty() || p.name.as_ref().is_some_and(|n| names.contains(n)))
                .cloned()
                .collect();
            for p in &due {
                self.verify_pending_fk(p).await?;
            }
        }
        let mut state = sess.deferred_fks.lock();
        if names.is_empty() {
            state.all = Some(deferred);
            state.named.clear();
        } else {
            for name in &names {
                state.named.retain(|(n, _)| n != name);
                state.named.push((name.clone(), deferred));
            }
        }
        Ok(tag())
    }
}
