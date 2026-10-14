//! The t6c checks (C-T6c1a item 4): every successful read equals the
//! model at the statement's snapshot `S` plus the txn's own writes,
//! rendered with the schema of the reported `def_version`; an RR/SER
//! read that *succeeded* although a foreign TRUNCATE committed in
//! `(S, read]` is a violation (C-T0 §4's relation rule: the engine must
//! answer 40001, never the post-truncate storage); the final full scans
//! equal the model; amcheck is clean at the end and at each mid-run
//! snapshot; and no intent leaks once the resolver drains.
//!
//! Read equality is decided only on recorded data — statement snapshots,
//! commit ts, result rows, storage ids — never on scheduling or
//! wall-clock time. Which storage a statement answered from is exact, not
//! inferred: the runner records the storage id of every table handle it
//! opens and the new storage id every TRUNCATE allocates, and ids are
//! never reused (§10), so equality pins the storage without any ordering
//! argument.
//!
//! One window is legal and accepted rather than flagged: an **RC**
//! statement that took its snapshot, then had a foreign TRUNCATE commit
//! before its relation lock was granted (relation locks are held to txn
//! end, and a TRUNCATE's AccessExclusive cannot be granted — so cannot
//! commit — between that grant and the txn's end; §4 takes the lock
//! before the catalog read). Such a statement answers from the
//! post-truncate storage at its snapshot `S`, where every foreign row
//! has ts `> T > S`: its read is its own writes alone
//! ([`truncate_window`]). RR/SER reads get no such window — §4's
//! relation rule must answer 40001 there — which is what the dedicated
//! check asserts.
//!
//! Write-effect and SER-specific checks are C-T6c1b's; a write op here
//! only feeds the model's overlay (from the recorded effect).

use std::collections::BTreeMap;

use nucleus_codec::row::value_repr_eq;
use nucleus_codec::Value;

use super::model::{Model, OwnWrites, U_LIVE};
use super::{Fate, History, Level, OpDetail, OpKind, OpOut, Run, TxnRec};

/// Counters the soak prints (never asserts on).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub txns: usize,
    pub committed: usize,
    pub failed: usize,
    /// Attempts that failed 40001/40P01 and were retried (the runner's
    /// cap bounds the chain; the last attempt's fate lands in
    /// `committed`/`failed`).
    pub retryable_failures: usize,
    pub reads_checked: usize,
    pub writes: usize,
    pub ddl: usize,
    pub truncates_committed: usize,
    pub amcheck_points: usize,
}

/// The verdict: every violation found, plus the stats.
#[derive(Debug, Clone, Default)]
pub struct Verdict {
    pub violations: Vec<String>,
    pub stats: Stats,
}

impl Verdict {
    pub fn is_clean(&self) -> bool {
        self.violations.is_empty()
    }

    /// A printable report of the violations.
    pub fn report(&self) -> String {
        let mut out = format!("t6c verdict: {} violation(s)\n", self.violations.len());
        for v in &self.violations {
            out.push_str("  ");
            out.push_str(v);
            out.push('\n');
        }
        out
    }
}

/// Checks a finished run.
pub fn check(run: &Run) -> Verdict {
    let mut v = Verdict::default();
    let model = Model::build(&run.history);
    for a in &model.anomalies {
        v.violations.push(format!("model anomaly: {a}"));
    }
    let truncates = committed_truncates(&run.history);
    check_reads(&run.history, &model, &truncates, &mut v);
    check_truncate_windows(&run.history, &truncates, &mut v);
    check_final(run, &model, &mut v);
    for rec in &run.amcheck {
        if !rec.report.is_clean() {
            v.violations.push(format!(
                "amcheck ({} at DDL index {}): {}",
                rec.table,
                rec.ddl_index,
                rec.report.violations.join("; ")
            ));
        }
        v.stats.amcheck_points += 1;
    }
    if run.leaked_intents != 0 {
        v.violations.push(format!(
            "{} intent(s) leaked after the resolver drained:\n{}",
            run.leaked_intents, run.leak_report
        ));
    }
    v
}

/// The committed TRUNCATEs of the history: `(commit ts, new storage id,
/// session)`.
fn committed_truncates(h: &History) -> Vec<(u64, u64, u16)> {
    h.txns
        .iter()
        .filter_map(|t| match t.fate {
            Fate::Committed(ts) => {
                let new_sid = t.ops.iter().find_map(|op| match (&op.detail, &op.out) {
                    (OpDetail::Truncate { new_sid }, OpOut::Ok) => Some(*new_sid),
                    _ => None,
                })?;
                Some((ts, new_sid, t.session))
            }
            Fate::Failed(_) => None,
        })
        .collect()
}

/// Whether a foreign TRUNCATE landed inside a statement's
/// snapshot-to-catalog-read window: committed at `T > S`, with the new
/// storage the statement's handle actually read.
///
/// Storage ids are never reused (§10), so `new_sid == op.sid` pins, with
/// no tick or ordering argument, that the statement's catalog read named
/// the post-truncate storage. The relation lock a statement takes before
/// its catalog read (§4) is held to the txn's end, so a TRUNCATE's
/// AccessExclusive — and so its commit — cannot intervene between that
/// grant and the txn's end; the swap can only have happened before the
/// grant. Such a statement legitimately answers from the new storage at
/// `S`, where every foreign row has ts `> T > S`: its read is its own
/// writes alone. (RR/SER reads get no such window: §4's relation rule
/// must answer 40001 there, which is what the dedicated check asserts.)
fn truncate_window(truncates: &[(u64, u64, u16)], txn: &TxnRec, op_index: usize) -> bool {
    let op = &txn.ops[op_index];
    let Some(sid) = op.sid else {
        return false;
    };
    truncates
        .iter()
        .any(|(ts, new_sid, session)| op.snap < *ts && *new_sid == sid && *session != txn.session)
}

/// Where a record sits, for violation context.
fn where_txn(t: &TxnRec) -> String {
    format!(
        "txn {} (session {}, prog {}, attempt {}, {})",
        t.uid,
        t.session,
        t.prog,
        t.attempt,
        t.level.short()
    )
}

fn where_op(t: &TxnRec, idx: usize) -> String {
    let op = &t.ops[idx];
    format!(
        "{} op {idx} ({} at S={}, def v{:?})",
        where_txn(t),
        op.kind.tag(),
        op.snap,
        op.def_version
    )
}

/// Whether two recorded rows are equal (representation-faithful).
fn rows_eq(a: &[(u16, Option<Value>)], b: &[(u16, Option<Value>)]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|(x, y)| {
            x.0 == y.0
                && match (&x.1, &y.1) {
                    (None, None) => true,
                    (Some(x), Some(y)) => value_repr_eq(x, y),
                    _ => false,
                }
        })
}

fn diff(a: &[(u16, Option<Value>)], b: &[(u16, Option<Value>)]) -> String {
    format!("{a:?} != {b:?}")
}

/// The read checks: for every successful read op of every txn (committed
/// or not — an aborted txn's observations were still real), the observed
/// rows equal the model at the statement's snapshot plus the txn's own
/// writes so far, rendered at the reported `def_version`. An RC statement
/// whose snapshot-to-catalog-read window contains a foreign TRUNCATE
/// answers from the post-truncate storage instead (see
/// [`rc_truncate_window`]).
fn check_reads(h: &History, model: &Model, truncates: &[(u64, u64, u16)], v: &mut Verdict) {
    for txn in &h.txns {
        v.stats.txns += 1;
        match &txn.fate {
            Fate::Committed(_) => v.stats.committed += 1,
            Fate::Failed(code) => {
                v.stats.failed += 1;
                if super::retryable(code.as_str()) {
                    v.stats.retryable_failures += 1;
                }
            }
        }
        let mut own = OwnWrites::default();
        for (idx, op) in txn.ops.iter().enumerate() {
            // `t`'s base: the model at the statement's snapshot, or the
            // empty floor when a foreign TRUNCATE landed inside the RC
            // statement's snapshot-to-catalog-read window (`u` is never
            // truncated and always reads the model at its snapshot).
            let base = if txn.level == Level::ReadCommitted && truncate_window(truncates, txn, idx)
            {
                model.state_at(0)
            } else {
                model.state_at(op.snap)
            };
            match (&op.detail, &op.out) {
                // ---- reads on t ----------------------------------------
                (OpDetail::Get { pk }, OpOut::Row(obs)) => {
                    let Some(ver) = op.def_version else {
                        v.violations.push(format!(
                            "{}: a Get recorded no def version",
                            where_op(txn, idx)
                        ));
                        continue;
                    };
                    let expected = own.t_row(base, *pk).map(|row| model.render(ver, row));
                    match (obs, expected) {
                        (Some(obs), Some(exp)) => {
                            if !rows_eq(obs, &exp) {
                                v.violations.push(format!(
                                    "{}: Get pk {pk} returned {}",
                                    where_op(txn, idx),
                                    diff(obs, &exp)
                                ));
                            }
                        }
                        (None, None) => {}
                        (Some(obs), None) => v.violations.push(format!(
                            "{}: Get pk {pk} returned a row the model does not have: {:?}",
                            where_op(txn, idx),
                            obs
                        )),
                        (None, Some(exp)) => v.violations.push(format!(
                            "{}: Get pk {pk} returned no row; the model has {:?}",
                            where_op(txn, idx),
                            exp
                        )),
                    }
                    v.stats.reads_checked += 1;
                }
                (OpDetail::ScanT, OpOut::RowsT(obs)) => {
                    let Some(ver) = op.def_version else {
                        v.violations.push(format!(
                            "{}: a ScanT recorded no def version",
                            where_op(txn, idx)
                        ));
                        continue;
                    };
                    let mut expected: BTreeMap<i64, Vec<(u16, Option<Value>)>> = base
                        .t
                        .iter()
                        .map(|(pk, row)| (*pk, model.render(ver, row)))
                        .collect();
                    for (pk, ov) in &own.t {
                        match ov {
                            super::model::Overlay::Row(row) => {
                                expected.insert(*pk, model.render(ver, row));
                            }
                            super::model::Overlay::Gone => {
                                expected.remove(pk);
                            }
                        }
                    }
                    let mut got: BTreeMap<i64, &Vec<(u16, Option<Value>)>> =
                        obs.iter().map(|(pk, r)| (*pk, r)).collect();
                    if got.len() != obs.len() {
                        v.violations.push(format!(
                            "{}: ScanT returned a duplicate pk",
                            where_op(txn, idx)
                        ));
                    }
                    let mut pks: Vec<i64> = expected.keys().copied().collect();
                    pks.extend(got.keys().copied().filter(|pk| !expected.contains_key(pk)));
                    for pk in pks {
                        match (got.remove(&pk), expected.get(&pk)) {
                            (Some(g), Some(e)) => {
                                if !rows_eq(g, e) {
                                    v.violations.push(format!(
                                        "{}: ScanT pk {pk}: {}",
                                        where_op(txn, idx),
                                        diff(g, e)
                                    ));
                                }
                            }
                            (Some(g), None) => v.violations.push(format!(
                                "{}: ScanT returned pk {pk} the model does not have: {:?}",
                                where_op(txn, idx),
                                g
                            )),
                            (None, Some(e)) => v.violations.push(format!(
                                "{}: ScanT missed pk {pk}: model has {:?}",
                                where_op(txn, idx),
                                e
                            )),
                            (None, None) => {}
                        }
                    }
                    v.stats.reads_checked += 1;
                }
                // ---- reads on u ----------------------------------------
                (OpDetail::ScanU, OpOut::RowsU(obs)) => {
                    let expected: BTreeMap<i64, Vec<(u16, Option<Value>)>> = own
                        .u_rows(model.state_at(op.snap))
                        .into_iter()
                        .map(|(a, row)| (a, super::model::Model::render_u(row)))
                        .collect();
                    let mut got: BTreeMap<i64, &Vec<(u16, Option<Value>)>> = BTreeMap::new();
                    for (key, row) in obs {
                        let a = u_identity_of(row);
                        match a {
                            Some(a) => {
                                if got.insert(a, row).is_some() {
                                    v.violations.push(format!(
                                        "{}: ScanU returned two rows with the same identity",
                                        where_op(txn, idx)
                                    ));
                                }
                            }
                            None => v.violations.push(format!(
                                "{}: ScanU returned a row with no int8 value in u's first column ({key:?})",
                                where_op(txn, idx)
                            )),
                        }
                    }
                    let mut ids: Vec<i64> = expected.keys().copied().collect();
                    ids.extend(got.keys().copied().filter(|a| !expected.contains_key(a)));
                    for a in ids {
                        match (got.remove(&a), expected.get(&a)) {
                            (Some(g), Some(e)) => {
                                if !rows_eq(g, e) {
                                    v.violations.push(format!(
                                        "{}: ScanU row {a}: {}",
                                        where_op(txn, idx),
                                        diff(g, e)
                                    ));
                                }
                            }
                            (Some(g), None) => v.violations.push(format!(
                                "{}: ScanU returned a row the model does not have: {:?}",
                                where_op(txn, idx),
                                g
                            )),
                            (None, Some(e)) => v.violations.push(format!(
                                "{}: ScanU missed the model's row: {:?}",
                                where_op(txn, idx),
                                e
                            )),
                            (None, None) => {}
                        }
                    }
                    v.stats.reads_checked += 1;
                }
                // ---- a read that failed with something no read raises ----
                (OpDetail::Get { .. } | OpDetail::ScanT | OpDetail::ScanU, OpOut::Err(code)) => {
                    if code != "40001" && code != "40P01" {
                        v.violations
                            .push(format!("{}: a read failed with {code}", where_op(txn, idx)));
                    }
                }
                // ---- everything else is a write or DDL: feed the model --
                _ => {
                    if matches!(
                        op.kind,
                        OpKind::InsertT
                            | OpKind::UpdateSet
                            | OpKind::DeleteT
                            | OpKind::InsertU
                            | OpKind::DeleteU
                    ) {
                        v.stats.writes += 1;
                    } else if super::model::is_ddl(op.kind) {
                        v.stats.ddl += 1;
                        if op.kind == OpKind::Truncate && matches!(op.out, OpOut::Ok) {
                            v.stats.truncates_committed += match txn.fate {
                                Fate::Committed(_) => 1,
                                Fate::Failed(_) => 0,
                            };
                        }
                    }
                }
            }
            own.apply(op, &model.u_bind);
        }
    }
}

/// The `a` identity of a recorded `u` row.
fn u_identity_of(row: &[(u16, Option<Value>)]) -> Option<i64> {
    match row.iter().find(|(attnum, _)| *attnum == U_LIVE[0]) {
        Some((_, Some(Value::Int8(a)))) => Some(*a),
        _ => None,
    }
}

/// The TRUNCATE-window check: an RR/SER read of `t` that *succeeded*
/// although it answered from the storage a foreign TRUNCATE installed
/// after its snapshot `S` — the engine must have answered 40001 (C-T0
/// §4, C-T6 §4), never the post-truncate rows. The statement's recorded
/// `sid` against the truncate's recorded `new_sid` is the exact test
/// (see [`truncate_window`]): equality means the catalog read the
/// statement made under its relation lock named the post-truncate
/// storage, i.e. the truncate committed inside `(S, read]`.
fn check_truncate_windows(h: &History, truncates: &[(u64, u64, u16)], v: &mut Verdict) {
    for txn in &h.txns {
        if !matches!(txn.level, Level::RepeatableRead | Level::Serializable) {
            continue;
        }
        for (idx, op) in txn.ops.iter().enumerate() {
            let successful_read = matches!(
                (&op.detail, &op.out),
                (OpDetail::Get { .. }, OpOut::Row(_)) | (OpDetail::ScanT, OpOut::RowsT(_))
            );
            if successful_read && truncate_window(truncates, txn, idx) {
                let ts = truncates
                    .iter()
                    .find(|(ts, new_sid, session)| {
                        op.snap < *ts
                            && *new_sid == op.sid.unwrap_or(u64::MAX)
                            && *session != txn.session
                    })
                    .map(|(ts, _, _)| ts)
                    .expect("truncate_window found one");
                v.violations.push(format!(
                    "{}: a successful {} read at S={} answered from the storage a \
                     foreign TRUNCATE committed at ts {ts} inside (S, read] — 40001 was required",
                    where_op(txn, idx),
                    op.kind.tag(),
                    op.snap
                ));
            }
        }
    }
}

/// The final-state check: the run's final full scans equal the model's
/// final state, rendered at the reported def version (`t` by pk, `u` by
/// its unique `a` identity).
fn check_final(run: &Run, model: &Model, v: &mut Verdict) {
    let (ver, rows) = &run.final_t;
    let mut expected: BTreeMap<i64, Vec<(u16, Option<Value>)>> = model
        .final_state
        .t
        .iter()
        .map(|(pk, row)| (*pk, model.render(*ver, row)))
        .collect();
    let mut got: BTreeMap<i64, &Vec<(u16, Option<Value>)>> =
        rows.iter().map(|(pk, r)| (*pk, r)).collect();
    let mut pks: Vec<i64> = expected.keys().copied().collect();
    pks.extend(got.keys().copied().filter(|pk| !expected.contains_key(pk)));
    for pk in pks {
        match (got.remove(&pk), expected.remove(&pk)) {
            (Some(g), Some(e)) => {
                if !rows_eq(g, &e) {
                    v.violations
                        .push(format!("final state of t, pk {pk}: {}", diff(g, &e)));
                }
            }
            (Some(g), None) => v.violations.push(format!(
                "final state of t has pk {pk} the model does not: {:?}",
                g
            )),
            (None, Some(e)) => v.violations.push(format!(
                "final state of t is missing pk {pk}: model has {:?}",
                e
            )),
            (None, None) => {}
        }
    }

    let (_ver, u_rows) = &run.final_u;
    let mut u_expected: BTreeMap<i64, Vec<(u16, Option<Value>)>> = model
        .final_state
        .u
        .iter()
        .map(|(a, row)| (*a, super::model::Model::render_u(row)))
        .collect();
    let mut u_got: BTreeMap<i64, &Vec<(u16, Option<Value>)>> = BTreeMap::new();
    for (key, row) in u_rows {
        match u_identity_of(row) {
            Some(a) => {
                if u_got.insert(a, row).is_some() {
                    v.violations.push(format!(
                        "final state of u returned a duplicate identity ({key:?})"
                    ));
                }
            }
            None => v.violations.push(format!(
                "final state of u returned a row with no int8 first column ({key:?})"
            )),
        }
    }
    let mut ids: Vec<i64> = u_expected.keys().copied().collect();
    ids.extend(
        u_got
            .keys()
            .copied()
            .filter(|a| !u_expected.contains_key(a)),
    );
    for a in ids {
        match (u_got.remove(&a), u_expected.remove(&a)) {
            (Some(g), Some(e)) => {
                if !rows_eq(g, &e) {
                    v.violations
                        .push(format!("final state of u, row {a}: {}", diff(g, &e)));
                }
            }
            (Some(g), None) => v.violations.push(format!(
                "final state of u has a row the model does not: {g:?}"
            )),
            (None, Some(e)) => v.violations.push(format!(
                "final state of u is missing the model's row: {e:?}"
            )),
            (None, None) => {}
        }
    }
}
