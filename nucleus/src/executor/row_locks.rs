//! Row-level locking for `FOR UPDATE` / `FOR SHARE` clauses.
//!
//! The clause was previously refused outright (and before that, parsed and
//! silently dropped — the guarantee-removal class this engine rejects on
//! principle). This module makes it real: a locking SELECT takes a
//! transaction-scoped lock on every row it emits, so two concurrent claim
//! queries cannot take the same row.
//!
//! # Where locks are taken
//!
//! In `execute_query_planned`, on the pre-projection row set, after WHERE,
//! after ORDER BY, and BEFORE LIMIT — the same plan position PostgreSQL's
//! LockRows node occupies (Limit → LockRows → Sort → Scan). That position is
//! what makes `SKIP LOCKED LIMIT n` behave: rows are locked in result order,
//! locked rows are skipped, and LIMIT then keeps the first n UNLOCKED rows,
//! so two workers paging the same queue receive disjoint sets rather than
//! worker B receiving nothing while worker A's locks age out.
//!
//! # Row identity
//!
//! A row's lock identity is its base table plus its PRIMARY KEY values as the
//! catalog constrains them, taken from the pre-projection row exactly as the
//! heap produced it — keyed by `Value`'s canonical `Hash`/`Eq`, so `Int32(7)`
//! and `Int64(7)` are one row. Not a position (positions shift under
//! concurrent DML), not a hash (a collision would skip a row nobody holds).
//! Tables without a primary key are refused: the engine has no tuple ids to
//! lock with, and guessing an identity would silently narrow the guarantee —
//! the exact failure mode this module exists to remove.
//!
//! # What is supported
//!
//! Single-table plain SELECTs: the claim shape (`SELECT ... WHERE ... ORDER BY
//! ... FOR UPDATE [SKIP LOCKED|NOWAIT] LIMIT n`), including the same query as
//! the `IN (subquery)` of a claiming UPDATE, which executes through this same
//! path. Joins, set operations, DISTINCT, GROUP BY, aggregates and window
//! functions are refused with `0A000` — PostgreSQL refuses most of these too
//! ("FOR UPDATE is not allowed with DISTINCT/GROUP BY/aggregates"), and the
//! ones it does not (joins) are honestly out of scope rather than
//! approximately locked. `FOR SHARE` is honoured as `FOR UPDATE`: the engine
//! has one row-lock strength, and the coarser lock never under-delivers the
//! clause's guarantee (it excludes strictly more).
//!
//! # Locks and the query cache
//!
//! A locking query never consults the cached fast paths that bypass the AST
//! row pipeline (plan execution, index-only scan, top-K truncation): they
//! either drop the columns the PK is read from or truncate the row set before
//! locks could be evaluated. The fail-closed backstop is in
//! [`apply`]: if the primary key is not resolvable in the emitted columns,
//! the query errors rather than returning unlocked rows.

use sqlparser::ast::{self, SetExpr, TableFactor};

use super::{ExecError, Executor};
use crate::storage::lock_manager::{RowLockKey, RowTry};
use crate::types::Value;

/// One query's locking request, resolved and validated.
pub(crate) struct RowLockContext {
    /// Base table the rows belong to (lock identity component).
    pub(crate) table: String,
    /// PRIMARY KEY column names, in constraint order.
    pub(crate) pk_columns: Vec<String>,
    /// `SKIP LOCKED`, `NOWAIT`, or none (plain blocking acquisition). Read by
    /// the LIMIT interplay in `execute_query_planned`.
    pub(crate) nonblock: Option<ast::NonBlock>,
    /// The query's WHERE clause, re-evaluated on a locked row that changed
    /// between the scan and its lock.
    pub(crate) selection: Option<ast::Expr>,
}

/// Validate `query`'s locking clauses and resolve the lock context, or `None`
/// when the query carries no locking clause (the overwhelmingly common case —
/// one `is_empty` check, no more).
///
/// Runs inside `execute_query_planned`, so a top-level SELECT, a CTE body, a
/// derived table and an `IN (subquery)` claim all pass through the same
/// validation — a clause is honoured everywhere it can appear or refused
/// everywhere it cannot.
pub(crate) async fn lock_context(
    ex: &Executor,
    query: &ast::Query,
) -> Result<Option<RowLockContext>, ExecError> {
    if query.locks.is_empty() {
        return Ok(None);
    }

    let SetExpr::Select(select) = &*query.body else {
        return Err(ExecError::Unsupported(
            "FOR UPDATE is only supported on plain SELECTs; it cannot be applied to a \
             set operation or parenthesized query body"
                .into(),
        ));
    };

    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err(ExecError::Unsupported(
            "FOR UPDATE over joins or multiple tables is not implemented. Locking a \
             joined result needs per-table row identity on every arm; until then the \
             clause is refused rather than locking only one side of the join"
                .into(),
        ));
    }
    let TableFactor::Table {
        name, args: None, ..
    } = &select.from[0].relation
    else {
        return Err(ExecError::Unsupported(
            "FOR UPDATE requires a plain table in FROM (not a subquery, table function \
             or nested join)"
                .into(),
        ));
    };
    let table = crate::sql::object_name_key(name);

    for lock in &query.locks {
        if let Some(of) = &lock.of {
            let names_it = of.0.last().and_then(|p| p.as_ident());
            match names_it {
                Some(id) if id.value == table => {}
                _ => {
                    return Err(ExecError::Unsupported(format!(
                        "FOR UPDATE OF {of} does not name the queried table '{table}'"
                    )));
                }
            }
        }
    }
    let mut nonblock = None;
    for lock in &query.locks {
        if let Some(nb) = &lock.nonblock
            && nonblock.is_some_and(|prior| &prior != nb)
        {
            return Err(ExecError::Unsupported(
                "conflicting SKIP LOCKED / NOWAIT options in one locking clause set".into(),
            ));
        }
        nonblock = nonblock.or(lock.nonblock);
    }

    if select.distinct.is_some() {
        return Err(ExecError::Unsupported(
            "FOR UPDATE is not allowed with DISTINCT".into(),
        ));
    }
    if matches!(&select.group_by, ast::GroupByExpr::Expressions(e, _) if !e.is_empty()) {
        return Err(ExecError::Unsupported(
            "FOR UPDATE is not allowed with GROUP BY".into(),
        ));
    }
    if select.having.is_some() {
        return Err(ExecError::Unsupported(
            "FOR UPDATE is not allowed with HAVING".into(),
        ));
    }
    for item in &select.projection {
        let expr = match item {
            ast::SelectItem::UnnamedExpr(e) | ast::SelectItem::ExprWithAlias { expr: e, .. } => e,
            _ => continue,
        };
        if super::helpers::contains_window_function(expr) {
            return Err(ExecError::Unsupported(
                "FOR UPDATE is not allowed with window functions".into(),
            ));
        }
        if super::helpers::contains_aggregate(expr) {
            return Err(ExecError::Unsupported(
                "FOR UPDATE is not allowed with aggregate functions".into(),
            ));
        }
    }

    let table_def = ex.get_table(&table).await?;
    let Some(pk) = table_def.primary_key_columns() else {
        return Err(ExecError::Unsupported(format!(
            "FOR UPDATE requires a primary key on '{table}': row locks are keyed by the \
             primary key, and this engine has no tuple ids to lock a keyless table with"
        )));
    };

    // A locked row is re-read after its lock is taken, and that read is raw:
    // under row security or masking the scan's rows are filtered or masked,
    // so the re-read could neither be compared with them nor be shown to the
    // caller. Refuse rather than let the stale-lock window stay open silently.
    if ex.session_is_policed_on(&table) {
        return Err(ExecError::Unsupported(format!(
            "FOR UPDATE / FOR SHARE is not supported on '{table}': the current role is \
             subject to row-level security or column masking on it, and a locked row \
             cannot be re-checked after its lock is taken without bypassing the policy"
        )));
    }

    Ok(Some(RowLockContext {
        table,
        pk_columns: pk.to_vec(),
        nonblock,
        selection: select.selection.clone(),
    }))
}

impl Executor {
    /// Take the locks `ctx` describes on the emitted rows, in place.
    ///
    /// `limit_hint` is `offset + limit` when the caller could resolve them:
    /// a row that cannot survive the statement is never locked, exactly as
    /// PostgreSQL's Limit node stops pulling rows through LockRows. The hint
    /// is the number of rows to FILL, not a pre-truncation: a row that is
    /// skipped (held, `SKIP LOCKED`) or dropped (changed while its lock was
    /// awaited) frees its slot to the next candidate, so the walk continues
    /// until the budget is full of locked, re-checked rows or the candidates
    /// run out. A row is never returned without its lock.
    ///
    /// The scan that produced `rows` ran BEFORE any lock was taken, so a row
    /// can change between the scan and its lock (a holder commits and
    /// releases in that window). Every locked candidate is therefore re-read
    /// by primary key and re-judged — see [`Executor::recheck_locked`].
    pub(crate) async fn apply_row_locks(
        &self,
        ctx: &RowLockContext,
        col_meta: &[super::ColMeta],
        rows: &mut Vec<crate::types::Row>,
        limit_hint: Option<usize>,
    ) -> Result<(), ExecError> {
        let session = super::unique_gate::gate_session_id();

        // Resolve the PK against the emitted columns. Fail closed: an
        // unresolvable key means a fast path dropped the column, and returning
        // unlocked rows would reinstate the silent-guarantee-drop this module
        // replaced.
        let mut pk_idx = Vec::with_capacity(ctx.pk_columns.len());
        for col in &ctx.pk_columns {
            let pos = col_meta
                .iter()
                .position(|m| m.name.eq_ignore_ascii_case(col))
                .ok_or_else(|| {
                    ExecError::Unsupported(format!(
                        "FOR UPDATE cannot read the primary key of '{}' from this query's \
                         row set (column '{col}' is absent); refusing rather than \
                         returning unlocked rows",
                        ctx.table
                    ))
                })?;
            pk_idx.push(pos);
        }
        let key_of = |row: &crate::types::Row| -> RowLockKey {
            (
                ctx.table.clone(),
                pk_idx
                    .iter()
                    .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                    .collect(),
            )
        };

        let candidates = std::mem::take(rows);
        // A locked candidate: its position in the scan (output order), the row
        // image, its lock key, and whether the session already owned the lock
        // before this statement (an owned lock is never released early).
        struct Locked {
            idx: usize,
            row: crate::types::Row,
            key: RowLockKey,
            held_before: bool,
        }
        let mut kept: Vec<Locked> = Vec::with_capacity(candidates.len());
        let mut next = 0usize;
        while next < candidates.len() {
            let need = match limit_hint {
                Some(b) if kept.len() >= b => break,
                Some(b) => Some(b - kept.len()),
                None => None,
            };

            // One round: lock up to `need` candidates.
            let mut batch: Vec<Locked> = Vec::new();
            match ctx.nonblock {
                Some(ast::NonBlock::SkipLocked) => {
                    // Walk in result order, filling the budget — PostgreSQL's
                    // Limit-over-LockRows composition. A held row is skipped
                    // and its slot passes to the next candidate; rows past the
                    // budget are neither locked nor returned (locking them
                    // parked rows the statement never emitted, and the next
                    // claimant found every row held and starved).
                    while next < candidates.len() && need.is_none_or(|n| batch.len() < n) {
                        let idx = next;
                        next += 1;
                        let key = key_of(&candidates[idx]);
                        let held_before = self.row_locks.holds(session, &key);
                        if self.row_locks.try_lock(session, &key)? == RowTry::Acquired {
                            batch.push(Locked {
                                idx,
                                row: candidates[idx].clone(),
                                key,
                                held_before,
                            });
                        }
                    }
                }
                Some(ast::NonBlock::Nowait) | None => {
                    let end = need.map_or(candidates.len(), |n| (next + n).min(candidates.len()));
                    let mut round: Vec<Locked> = (next..end)
                        .map(|idx| {
                            let key = key_of(&candidates[idx]);
                            let held_before = self.row_locks.holds(session, &key);
                            Locked {
                                idx,
                                row: candidates[idx].clone(),
                                key,
                                held_before,
                            }
                        })
                        .collect();
                    next = end;
                    round.sort_by(|a, b| a.key.cmp(&b.key));
                    round.dedup_by(|a, b| a.key == b.key);

                    if ctx.nonblock.is_some() {
                        // NOWAIT never waits: contention is the error.
                        for l in &round {
                            if self.row_locks.try_lock(session, &l.key)? == RowTry::HeldElsewhere {
                                // The `lock_not_available` wording is what the
                                // wire codec maps to SQLSTATE 55P03,
                                // PostgreSQL's code for exactly this refusal.
                                return Err(ExecError::Storage(crate::storage::StorageError::Io(
                                    format!(
                                        "lock_not_available: row in table '{}' could not \
                                         be locked (key {:?}): NOWAIT was requested and \
                                         another transaction holds it",
                                        l.key.0, l.key.1
                                    ),
                                )));
                            }
                        }
                    } else if kept.is_empty() {
                        // Plain FOR UPDATE, nothing held yet by this
                        // statement: wait for each holder in sorted key
                        // order, bounded by lock_timeout (55P03 on expiry,
                        // not 40001 — a held row is not a conflict a retry
                        // can win). Nothing this statement already took is
                        // held while it waits, and every statement climbs the
                        // same (table, key) order, so claims cannot wait on
                        // each other in a cycle.
                        for l in &round {
                            self.row_locks
                                .lock(session, &l.key)
                                .await
                                .map_err(ExecError::Storage)?;
                        }
                    } else {
                        // A refill: this statement already holds `kept`. Waiting
                        // for a later candidate while holding them would break
                        // the global order (a candidate can sort BEFORE a held
                        // row, and two claims with opposite ORDER BY then wait
                        // on each other). Take the new candidates without
                        // waiting; on any contention give back everything this
                        // statement newly took and re-take the whole set in
                        // sorted order, holding nothing while waiting.
                        let mut acquired = 0usize;
                        let mut contended = false;
                        for l in &round {
                            if self.row_locks.try_lock(session, &l.key)? == RowTry::Acquired {
                                acquired += 1;
                            } else {
                                contended = true;
                                break;
                            }
                        }
                        if contended {
                            for l in round.iter().take(acquired) {
                                if !l.held_before {
                                    self.row_locks.release_key(session, &l.key);
                                }
                            }
                            for k in &kept {
                                if !k.held_before {
                                    self.row_locks.release_key(session, &k.key);
                                }
                            }
                            round.append(&mut kept);
                            round.sort_by(|a, b| a.key.cmp(&b.key));
                            for l in &round {
                                self.row_locks
                                    .lock(session, &l.key)
                                    .await
                                    .map_err(ExecError::Storage)?;
                            }
                        }
                    }
                    // Back to result order for the recheck and the output.
                    round.sort_by_key(|l| l.idx);
                    batch = round;
                }
            }
            if batch.is_empty() {
                break;
            }

            let keys: Vec<RowLockKey> = batch.iter().map(|l| l.key.clone()).collect();
            let table_def = self.get_table(&ctx.table).await?;
            let current = self.read_locked_rows(ctx, &table_def, &keys).await?;
            for Locked {
                idx,
                row,
                key,
                held_before,
            } in batch
            {
                match self.recheck_locked(ctx, &table_def, col_meta, row, current.get(&key.1))? {
                    Some(row) => kept.push(Locked {
                        idx,
                        row,
                        key,
                        held_before,
                    }),
                    None => {
                        if !held_before {
                            self.row_locks.release_key(session, &key);
                        }
                    }
                }
            }
        }
        kept.sort_by_key(|l| l.idx);
        *rows = kept.into_iter().map(|l| l.row).collect();
        self.metrics
            .row_locks_held
            .set(self.row_locks.held_count() as i64);
        Ok(())
    }

    /// Read the current image of each locked row by primary key: the latest
    /// committed version plus this transaction's own writes. A single-column
    /// key goes through the index (cost proportional to the rows locked, not
    /// to the table); a composite key has no index to descend and takes one
    /// scan for the whole round.
    async fn read_locked_rows(
        &self,
        ctx: &RowLockContext,
        table_def: &crate::catalog::TableDef,
        keys: &[RowLockKey],
    ) -> Result<std::collections::HashMap<Vec<Value>, crate::types::Row>, ExecError> {
        let mut pk_pos: Vec<usize> = Vec::with_capacity(ctx.pk_columns.len());
        for name in &ctx.pk_columns {
            pk_pos.push(table_def.column_index(name).ok_or_else(|| {
                ExecError::Unsupported(format!(
                    "FOR UPDATE cannot resolve primary key column '{name}' of '{}'",
                    ctx.table
                ))
            })?);
        }
        let key_at = |row: &crate::types::Row| -> Vec<Value> {
            pk_pos
                .iter()
                .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                .collect()
        };
        let mut out = std::collections::HashMap::with_capacity(keys.len());
        // Point read through any single-column index on a key column (a
        // single-column key's own index; for a composite key, an index on one
        // of its columns, the full key then matched on the candidates). No
        // such index means one scan for the round.
        for (slot, &col) in pk_pos.iter().enumerate() {
            let mut indexed = true;
            out.clear();
            for key in keys {
                match self
                    .indexed_eq_positions(&ctx.table, table_def, col, &key.1[slot])
                    .await?
                {
                    Some(hits) => {
                        for (_, row) in hits {
                            let k = key_at(&row);
                            if k == key.1 {
                                out.insert(k, row);
                            }
                        }
                    }
                    None => {
                        indexed = false;
                        break;
                    }
                }
            }
            if indexed {
                return Ok(out);
            }
        }
        out.clear();
        let wanted: std::collections::HashSet<&Vec<Value>> = keys.iter().map(|k| &k.1).collect();
        for row in self.storage_for(&ctx.table).scan(&ctx.table).await? {
            let k = key_at(&row);
            if wanted.contains(&k) {
                out.insert(k, row);
            }
        }
        Ok(out)
    }

    /// Judge a locked candidate against the row as it is NOW — PostgreSQL's
    /// EvalPlanQual. The scan ran before the lock, so the row may have been
    /// changed (or deleted) by a transaction that held it in between.
    ///
    /// - gone: dropped (`None`);
    /// - unchanged: kept as scanned;
    /// - changed: the WHERE clause is re-evaluated on the new image. Still
    ///   matching returns the NEW image (a read-modify-write caller sees the
    ///   committed value, not a stale one and not "not found"); no longer
    ///   matching drops it.
    ///
    /// A WHERE that cannot be evaluated against a bare row (a subquery, say)
    /// cannot be re-judged, so a changed row fails closed with a retryable
    /// conflict instead of being returned stale.
    pub(super) fn recheck_locked(
        &self,
        ctx: &RowLockContext,
        table_def: &crate::catalog::TableDef,
        col_meta: &[super::ColMeta],
        scanned: crate::types::Row,
        current: Option<&crate::types::Row>,
    ) -> Result<Option<crate::types::Row>, ExecError> {
        let Some(current) = current else {
            return Ok(None);
        };
        // The scan's rows are laid out by `col_meta`; the table's row layout
        // is the catalog's. Re-lay the fresh row out the same way.
        let fresh: crate::types::Row = col_meta
            .iter()
            .map(|m| {
                table_def
                    .column_index(&m.name)
                    .and_then(|i| current.get(i).cloned())
                    .unwrap_or(Value::Null)
            })
            .collect();
        if fresh == scanned {
            return Ok(Some(scanned));
        }
        let Some(selection) = &ctx.selection else {
            return Ok(Some(fresh));
        };
        match self.eval_where(selection, &fresh, col_meta) {
            Ok(true) => Ok(Some(fresh)),
            Ok(false) => Ok(None),
            Err(_) => Err(ExecError::Storage(
                crate::storage::StorageError::WriteConflict(format!(
                    "a locked row of '{}' changed while its lock was awaited and the \
                     WHERE clause cannot be re-evaluated on it; retry the statement",
                    ctx.table
                )),
            )),
        }
    }

    /// Release every row lock this session holds. Called at COMMIT, ROLLBACK,
    /// autocommit statement end and session teardown — the same lifecycle that
    /// releases the unique-key gate and the storage table locks.
    pub(crate) fn release_row_locks(&self, session: u64) {
        self.row_locks.release_session(session);
        self.metrics
            .row_locks_held
            .set(self.row_locks.held_count() as i64);
    }
}
