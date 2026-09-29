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

    Ok(Some(RowLockContext {
        table,
        pk_columns: pk.to_vec(),
        nonblock,
    }))
}

impl Executor {
    /// Take the locks `ctx` describes on the emitted rows, in place.
    ///
    /// `limit_hint` is `offset + limit` when the caller could resolve them:
    /// a row that cannot survive the statement is never locked, exactly as
    /// PostgreSQL's Limit node stops pulling rows through LockRows. For
    /// `SKIP LOCKED` the hint is the number of rows to FILL, not a
    /// pre-truncation: a skipped row frees its result slot to a later
    /// candidate, so the walk keeps trying until the budget is full or the
    /// candidate set is exhausted.
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

        // Structural order for the waiting/refusing modes: every statement
        // climbs the same (table, key) order, so two locking statements — the
        // claim pattern — cannot wait on each other. Sorted acquisition is
        // why single-statement claims need no deadlock detector. Only the
        // rows that can still be emitted need locks.
        let n = limit_hint.map_or(rows.len(), |l| l.min(rows.len()));
        let mut keys: Vec<RowLockKey> = rows[..n].iter().map(&key_of).collect();
        keys.sort();
        keys.dedup();

        match ctx.nonblock {
            Some(ast::NonBlock::SkipLocked) => {
                // Walk in result order, filling the statement's row budget —
                // PostgreSQL's Limit-over-LockRows composition. A row is
                // locked only when a result slot can still consume it; a
                // held row is skipped and its slot passes to the next
                // candidate; rows past the budget are neither locked nor
                // returned. Locking candidates past the budget parks rows
                // the statement never emits — measured: `LIMIT 3` over six
                // rows locked all six, and the next claimant found every
                // row held and starved.
                //
                // Each round locks up to the remaining budget, then re-reads
                // the locked rows (see `retain_current`): a candidate that
                // changed while the walk was reaching it is dropped and its
                // slot goes back to the walk.
                let mut kept: Vec<crate::types::Row> = Vec::with_capacity(rows.len());
                let mut candidates = std::mem::take(rows).into_iter();
                loop {
                    let mut locked: Vec<crate::types::Row> = Vec::new();
                    for row in candidates.by_ref() {
                        if let Some(b) = limit_hint
                            && kept.len() + locked.len() >= b
                        {
                            break;
                        }
                        let key = key_of(&row);
                        if self.row_locks.try_lock(session, &key)? == RowTry::Acquired {
                            locked.push(row);
                        }
                    }
                    if locked.is_empty() {
                        break;
                    }
                    kept.extend(self.retain_current(ctx, col_meta, locked, &pk_idx).await?);
                    if limit_hint.is_some_and(|b| kept.len() >= b) {
                        break;
                    }
                }
                *rows = kept;
            }
            Some(ast::NonBlock::Nowait) => {
                for key in &keys {
                    if self.row_locks.try_lock(session, key)? == RowTry::HeldElsewhere {
                        // The `lock_not_available` wording is what the wire
                        // codec maps to SQLSTATE 55P03, PostgreSQL's code for
                        // exactly this refusal.
                        return Err(ExecError::Storage(crate::storage::StorageError::Io(
                            format!(
                                "lock_not_available: row in table '{}' could not be \
                                 locked (key {:?}): NOWAIT was requested and another \
                                 transaction holds it",
                                key.0, key.1
                            ),
                        )));
                    }
                }
            }
            None => {
                // Plain FOR UPDATE: wait for each holder's transaction to end,
                // bounded by lock_timeout (55P03 on expiry, not 40001 — a
                // held row is not a conflict a retry can win).
                for key in &keys {
                    self.row_locks
                        .lock(session, key)
                        .await
                        .map_err(ExecError::Storage)?;
                }
            }
        }
        if ctx.nonblock != Some(ast::NonBlock::SkipLocked) {
            let locked = std::mem::take(rows);
            *rows = self.retain_current(ctx, col_meta, locked, &pk_idx).await?;
        }
        self.metrics
            .row_locks_held
            .set(self.row_locks.held_count() as i64);
        Ok(())
    }

    /// Keep only the locked rows that are still what the scan saw.
    ///
    /// The scan that produced `rows` ran BEFORE their locks were taken, so a
    /// transaction that held a row when the scan ran can commit its change
    /// and release the lock in between. The lock then succeeds against a row
    /// that no longer matches the query, and the claimer would act on a stale
    /// image: a claim UPDATE either fails at commit with WriteConflict
    /// ("buffered row changed before commit") or, if its own read landed after
    /// the commit, applies twice. PostgreSQL re-evaluates the row after
    /// locking it (EvalPlanQual) and drops it if it no longer qualifies; this
    /// is the same recheck, done by re-reading the table now that the locks
    /// are held and comparing the row image. A row that changed or vanished
    /// is dropped.
    ///
    /// Tables under row security or masking are not rechecked: the scan's
    /// rows are filtered or masked, so they cannot be compared with a raw
    /// read.
    async fn retain_current(
        &self,
        ctx: &RowLockContext,
        col_meta: &[super::ColMeta],
        locked: Vec<crate::types::Row>,
        pk_idx: &[usize],
    ) -> Result<Vec<crate::types::Row>, ExecError> {
        if locked.is_empty() || self.table_is_secured(&ctx.table) {
            return Ok(locked);
        }
        let table_def = self.get_table(&ctx.table).await?;
        let mut columns: Vec<(usize, usize)> = Vec::new();
        let mut current_pk: Vec<usize> = Vec::with_capacity(ctx.pk_columns.len());
        for (ti, col) in table_def.columns.iter().enumerate() {
            if let Some(mi) = col_meta
                .iter()
                .position(|m| m.name.eq_ignore_ascii_case(&col.name))
            {
                columns.push((mi, ti));
            }
        }
        for name in &ctx.pk_columns {
            match table_def
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(name))
            {
                Some(ti) => current_pk.push(ti),
                None => return Ok(locked),
            }
        }
        let key_at = |row: &crate::types::Row, idx: &[usize]| -> Vec<Value> {
            idx.iter()
                .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                .collect()
        };
        let wanted: std::collections::HashSet<Vec<Value>> =
            locked.iter().map(|r| key_at(r, pk_idx)).collect();
        let now = self.storage_for(&ctx.table).scan(&ctx.table).await?;
        let mut latest: std::collections::HashMap<Vec<Value>, crate::types::Row> =
            std::collections::HashMap::with_capacity(wanted.len());
        for row in now {
            let key = key_at(&row, &current_pk);
            if wanted.contains(&key) {
                latest.insert(key, row);
            }
        }
        Ok(locked
            .into_iter()
            .filter(|row| {
                latest
                    .get(&key_at(row, pk_idx))
                    .is_some_and(|cur| columns.iter().all(|&(mi, ti)| row.get(mi) == cur.get(ti)))
            })
            .collect())
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
