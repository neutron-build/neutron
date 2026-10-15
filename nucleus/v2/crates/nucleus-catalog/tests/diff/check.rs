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
//! Write-effect and SERIALIZABLE checks are C-T6c1b's ([`Effects`]):
//! every committed write is re-derived at its commit position, SET NOT
//! NULL outcomes are validated against the rows the validating scan must
//! have seen, and the committed SERIALIZABLE sub-history's serialization
//! graph (read-from plus final-writer rw edges, both forced in every
//! equivalent serial order) must be acyclic — the card's "re-execute each
//! committed SER txn on the model state just before its commit_ts"
//! restated in a form that is sound under the workload's mixed isolation
//! levels (an RC writer may legally commit between a SER reader's
//! snapshot and its commit, so the literal re-execution would flag legal
//! histories; see [`Effects`]'s docs).

use std::collections::{BTreeMap, BTreeSet};

use nucleus_codec::row::value_repr_eq;
use nucleus_codec::Value;

use super::model::{Model, OwnWrites, State, U_LIVE};
use super::{Fate, History, Level, OpDetail, OpKind, OpOut, OpRec, RowVals, Run, TxnRec};

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
    // ---- C-T6c1b ------------------------------------------------------
    /// Write ops of committed txns run through the write-effect oracle.
    pub writes_checked: usize,
    /// Applied UpdateSets whose recorded effect was recomputed from the
    /// latest committed row plus own writes.
    pub effects_recomputed: usize,
    /// RC "no row" verdicts whose witness needed the delete/truncate
    /// carve-out (a tombstone committed inside `(S, commit]` — the EPQ
    /// skip of §5.2). Reported, never a violation.
    pub rc_window_carveouts: usize,
    /// Successful SET NOT NULLs verified against their commit position,
    /// plus 23502s whose NULL the model could witness.
    pub snn_checked: usize,
    /// 23502s the model could not witness at the statement's snapshot
    /// (the NULL may have committed while the DDL waited for its
    /// AccessExclusive — the card's documented skip).
    pub snn_23502_skipped: usize,
    /// Committed SERIALIZABLE txns (the serialization graph's nodes).
    pub ser_txns: usize,
    /// Reads of committed SERIALIZABLE txns the graph was derived from.
    pub ser_reads: usize,
    /// Serialization-graph edges: read-from (`wr`) and final-writer
    /// antidependency (`rw`).
    pub ser_wr_edges: usize,
    pub ser_rw_edges: usize,
    // ---- C-T6d2: the large-value surface ------------------------------
    /// Committed SetBig/NullBig write ops.
    pub big_writes: usize,
    /// Committed SetBigs per [`BIG_LENS`] class (inline / one chunk /
    /// three chunks).
    pub big_lens: [usize; 3],
    /// Committed DropBig DDLs.
    pub big_drops: usize,
    /// Committed AddBig DDLs.
    pub big_adds: usize,
    // ---- C-T6e3: the online-build surface (§7) ------------------------
    /// `CREATE INDEX` driver calls that published (returned Ok).
    pub idx_builds_ok: usize,
    /// `CREATE INDEX` driver calls that failed (55000s from the seeded
    /// sabotage, or anything else).
    pub idx_builds_failed: usize,
    /// `RESUME` driver calls that ran on a target (published or failed);
    /// no-target no-ops are counted by `idx_resumes_noop`.
    pub idx_resumes: usize,
    /// `RESUME` no-target no-ops (nothing non-Public existed).
    pub idx_resumes_noop: usize,
    /// Committed `DROP INDEX` statements that named a target.
    pub idx_drops: usize,
    /// Index-scan statements that ran against a target (the no-target
    /// no-ops are `idx_scans_noop`).
    pub idx_scans: usize,
    /// Index scans the engine served (`index_scan` returned rows); each
    /// was checked against the model at its snapshot.
    pub idx_scans_served: usize,
    /// Served scans whose post-sight was not Public with `built_at <= S`
    /// — impossible per the bracket, a violation.
    pub idx_scans_served_bad: usize,
    /// Refusals (0A000) whose pre-sight already served at the statement's
    /// snapshot — the engine refused an index §4 obliges it to serve, a
    /// violation.
    pub idx_scans_refused_bad: usize,
    /// Refusals (0A000) justified by the recorded sights (non-Public, or
    /// Public with `built_at > S`).
    pub idx_scans_refused: usize,
    /// 42704 refusals (the target absent at the engine's read).
    pub idx_scans_absent: usize,
    /// No-target no-op index scans (no live index existed).
    pub idx_scans_noop: usize,
    /// Recorded sights (pre or post) of a DeleteOnly index — the soak's
    /// coverage bar for the ladder's first state (I-IDX-DO's subject).
    pub idx_sights_delete_only: usize,
    /// The same for WriteOnly (I-IDX-WO's subject).
    pub idx_sights_write_only: usize,
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
    let truncates = committed_truncates(&run.history);
    // One replay serves both cards: the hook carries C-T6c1b's write
    // effects, SET NOT NULL and SERIALIZABLE read records; the model it
    // returns is exactly `Model::build`'s (the hook cannot mutate it).
    let mut fx = Effects::new(&run.history);
    let model = Model::build_with(&run.history, &mut |m, st, txn, idx, op, own| {
        fx.op(m, st, txn, idx, op, own, &truncates);
    });
    for a in &model.anomalies {
        v.violations.push(format!("model anomaly: {a}"));
    }
    check_reads(&run.history, &model, &truncates, &mut v);
    check_truncate_windows(&run.history, &truncates, &mut v);
    check_final(run, &model, &mut v);
    fx.finish(&run.history, &model, &mut v);
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
            // C-T6e3: every index-scan record's bracketed sights feed the
            // ladder-state coverage counters (I-IDX-DO/I-IDX-WO's
            // subjects must actually have been seen mid-run).
            if let OpDetail::IndexScan { pre, post, .. } = &op.detail {
                for sight in [*pre].into_iter().chain(*post) {
                    match sight {
                        super::IdxSight::Seen {
                            state: nucleus_catalog::IndexState::DeleteOnly,
                            ..
                        } => v.stats.idx_sights_delete_only += 1,
                        super::IdxSight::Seen {
                            state: nucleus_catalog::IndexState::WriteOnly,
                            ..
                        } => v.stats.idx_sights_write_only += 1,
                        _ => {}
                    }
                }
            }
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
                // ---- C-T6e3: an index scan of t (§4, §7) -----------------
                // The no-target no-op (no live index existed).
                (OpDetail::IndexScan { oid: None, .. }, OpOut::Ok) => {
                    v.stats.idx_scans_noop += 1;
                }
                (OpDetail::IndexScan { oid: Some(oid), lo, hi, post, .. }, OpOut::RowsT(obs)) => {
                    v.stats.idx_scans += 1;
                    // Served: §4's rule at the statement's snapshot, from
                    // the bracket's post-sight (the engine's own def read
                    // sits at or below it in publicness; a DROP cannot
                    // intervene under the statement's AccessShare).
                    let Some(post) = *post else {
                        v.violations.push(format!(
                            "{}: a served index scan of {oid} recorded no post-sight",
                            where_op(txn, idx)
                        ));
                        continue;
                    };
                    if !post.serves(op.snap) {
                        v.stats.idx_scans_served_bad += 1;
                        v.violations.push(format!(
                            "{}: index_scan of {oid} served a read at S={} although the \
                             post-sight is {:?} — not Public with built_at <= S",
                            where_op(txn, idx),
                            op.snap,
                            post
                        ));
                    }
                    let Some(ver) = op.def_version else {
                        v.violations.push(format!(
                            "{}: an index scan recorded no def version",
                            where_op(txn, idx)
                        ));
                        continue;
                    };
                    // Exactly the ScanT expectation, filtered by the
                    // recorded `a`-range: the model at the statement's
                    // snapshot plus own writes (an RC statement whose
                    // window contains a foreign TRUNCATE answers from the
                    // post-truncate storage, where every foreign row has
                    // ts > S — the empty floor), rendered at the reported
                    // def version.
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
                    expected.retain(|_, row| in_a_range(row, *lo, *hi));
                    let mut got: BTreeMap<i64, &Vec<(u16, Option<Value>)>> =
                        obs.iter().map(|(pk, r)| (*pk, r)).collect();
                    if got.len() != obs.len() {
                        v.violations.push(format!(
                            "{}: the index scan returned a duplicate pk",
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
                                        "{}: index scan pk {pk}: {}",
                                        where_op(txn, idx),
                                        diff(g, e)
                                    ));
                                }
                            }
                            (Some(g), None) => v.violations.push(format!(
                                "{}: the index scan returned pk {pk} the model does not have \
                                 (a-range {:?}..{:?}): {:?}",
                                where_op(txn, idx),
                                lo,
                                hi,
                                g
                            )),
                            (None, Some(e)) => v.violations.push(format!(
                                "{}: the index scan missed pk {pk} (a-range {:?}..{:?}): \
                                 model has {:?}",
                                where_op(txn, idx),
                                lo,
                                hi,
                                e
                            )),
                            (None, None) => {}
                        }
                    }
                    v.stats.idx_scans_served += 1;
                    v.stats.reads_checked += 1;
                }
                (OpDetail::IndexScan { oid: Some(oid), pre, .. }, OpOut::Err(code)) => {
                    v.stats.idx_scans += 1;
                    match code.as_str() {
                        // §4's refusal: the engine found the def
                        // non-Public or `built_at > S`. Sound against the
                        // pre-sight (the engine's own def read sits at or
                        // above it in publicness): if the pre-sight
                        // already served, the engine's read did too and
                        // the refusal was wrong.
                        "0A000" => {
                            if pre.serves(op.snap) {
                                v.stats.idx_scans_refused_bad += 1;
                                v.violations.push(format!(
                                    "{}: index_scan of {oid} refused with 0A000 at S={} although \
                                     the pre-sight is {:?} — Public with built_at <= S must serve",
                                    where_op(txn, idx),
                                    op.snap,
                                    pre
                                ));
                            } else {
                                v.stats.idx_scans_refused += 1;
                            }
                        }
                        // The engine's catalog read found no index row;
                        // the pre-sight must agree (a DROP cannot
                        // intervene under the statement's AccessShare, and
                        // an oid is never re-created).
                        "42704" => {
                            if *pre != super::IdxSight::Absent {
                                v.violations.push(format!(
                                    "{}: index_scan of {oid} answered 42704 although the \
                                     pre-sight is {:?}",
                                    where_op(txn, idx),
                                    pre
                                ));
                            }
                            v.stats.idx_scans_absent += 1;
                        }
                        // The RR/SER relation rule's answer, like every
                        // other read of `t`.
                        "40001" | "40P01" => {}
                        _ => {
                            v.violations.push(format!(
                                "{}: an index scan failed with {code}",
                                where_op(txn, idx)
                            ));
                        }
                    }
                }
                // A shape the runner never records.
                (OpDetail::IndexScan { oid: None, .. }, _) => {
                    v.violations.push(format!(
                        "{}: a no-target index scan recorded an outcome",
                        where_op(txn, idx)
                    ));
                }
                (OpDetail::IndexScan { oid: Some(_), .. }, OpOut::Ok) => {
                    v.violations.push(format!(
                        "{}: a targeted index scan recorded no rows and no error",
                        where_op(txn, idx)
                    ));
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
                            | OpKind::SetBig
                            | OpKind::NullBig
                    ) {
                        v.stats.writes += 1;
                        // C-T6d2: the large-value surface must actually
                        // have run — committed SetBigs at every length
                        // class the card names.
                        if matches!(op.out, OpOut::Applied(true))
                            && matches!(txn.fate, Fate::Committed(_))
                            && matches!(op.kind, OpKind::SetBig | OpKind::NullBig)
                        {
                            v.stats.big_writes += 1;
                        }
                        if let (OpDetail::SetBig { len, .. }, OpOut::Applied(true)) =
                            (&op.detail, &op.out)
                        {
                            if matches!(txn.fate, Fate::Committed(_)) {
                                if let Some(class) =
                                    super::BIG_LENS.iter().position(|l| *l == *len as usize)
                                {
                                    v.stats.big_lens[class] += 1;
                                }
                            }
                        }
                    } else if super::model::is_ddl(op.kind) {
                        v.stats.ddl += 1;
                        let committed =
                            matches!(txn.fate, Fate::Committed(_)) && matches!(op.out, OpOut::Ok);
                        if committed && op.kind == OpKind::DropBig {
                            v.stats.big_drops += 1;
                        }
                        if committed && op.kind == OpKind::AddBig {
                            v.stats.big_adds += 1;
                        }
                        // C-T6e3's per-kind index-DDL counters (a failed
                        // CreateIndex is the seeded sabotage's 55000, or
                        // a genuine engine refusal).
                        match op.kind {
                            OpKind::CreateIndex => {
                                if matches!(txn.fate, Fate::Committed(_)) {
                                    v.stats.idx_builds_ok += 1;
                                } else {
                                    v.stats.idx_builds_failed += 1;
                                }
                            }
                            OpKind::ResumeBuild => {
                                if matches!(
                                    &op.detail,
                                    OpDetail::ResumeBuild { oid: None, .. }
                                ) {
                                    v.stats.idx_resumes_noop += 1;
                                } else {
                                    v.stats.idx_resumes += 1;
                                }
                            }
                            OpKind::DropIndex => {
                                if committed
                                    && matches!(&op.detail, OpDetail::DropIndex { oid: Some(_) })
                                {
                                    v.stats.idx_drops += 1;
                                }
                            }
                            _ => {}
                        }
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

/// The attnum of the fixture's `a` column (t's second column) — the
/// indexed column the recorded a-ranges filter on. The workload's DDL
/// never drops it (DROP COLUMN targets only added columns, and DropBig
/// owns `big`'s attnum 4).
const A_ATTNUM: u16 = 2;

/// Whether a rendered row of `t` falls inside an index scan's recorded
/// `a`-range: `[lo, hi]` inclusive over the int8 value, with NULL (or a
/// non-int8 value, which no write of this workload produces) excluded —
/// NULLs sort last in the index key, so no bounded range reaches them.
/// An unbounded scan (`None` bounds) keeps everything.
fn in_a_range(row: &[(u16, Option<Value>)], lo: Option<i64>, hi: Option<i64>) -> bool {
    let (Some(lo), Some(hi)) = (lo, hi) else {
        return true;
    };
    match row.iter().find(|(a, _)| *a == A_ATTNUM) {
        Some((_, Some(Value::Int8(v)))) => lo <= *v && *v <= hi,
        _ => false,
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
                (OpDetail::Get { .. }, OpOut::Row(_))
                    | (OpDetail::ScanT, OpOut::RowsT(_))
                    | (OpDetail::IndexScan { oid: Some(_), .. }, OpOut::RowsT(_))
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

// ----- C-T6c1b: write effects, SET NOT NULL, SERIALIZABLE ---------------------

/// One read of a committed SERIALIZABLE txn, as the serialization-graph
/// derivation needs it (the graph is built after the replay, once every
/// write and so every key's final writer is known).
#[derive(Debug, Clone)]
struct SerRead {
    /// The reading txn.
    uid: u32,
    /// The snapshot the read ran at (`op.snap`).
    snap: u64,
    /// The read's target and what it saw.
    scope: Scope,
}

/// What a SER read covered. `seen`/`pks`/`ids` describe **base rows**
/// (the model at `S`), never the txn's own writes: a txn reading its own
/// write adds no read-from edge.
#[derive(Debug, Clone)]
enum Scope {
    /// A point read of `t` at `pk` (a Get, or the base read an
    /// UpdateSet/DeleteT statement made) — whether a base row was seen.
    PkT { pk: i64, seen: bool },
    /// A full scan of `t` — the base pks it returned.
    ScanT { pks: Vec<i64> },
    /// A point read of `u`'s row of identity `a` (a DeleteU's base read).
    PkU { a: i64, seen: bool },
    /// A full scan of `u` — the base row identities it returned.
    ScanU { ids: Vec<i64> },
}

/// C-T6c1b's oracle. Runs inside [`Model::build_with`]'s replay, so every
/// check sees each committed txn at its **commit position**: the model
/// state every earlier commit built, plus the txn's own earlier writes.
///
/// **Write effects** (card item 1):
///
/// - An `InsertT` that finds its pk live in the model at the commit
///   position means "the engine accepted a duplicate" — the §5.3 unique
///   check must have seen that live row (its writer committed before this
///   txn's placement, and the insert's `NoKeyUpdate` keeps every foreign
///   writer off the key from placement to commit).
/// - An `UpdateSet`/`DeleteT` reporting *affected* needs a live row at
///   the commit position: the row op's base (the EPQ re-base at RC, the
///   read row at RR/SER) is the newest committed version at placement,
///   and the row lock keeps every foreign writer off the key until the
///   txn ends, so that version is still the live one at the commit
///   position. *No row* needs none at `S` plus own writes at RR/SER (the
///   read found nothing, and any newer version would have been 40001);
///   at RC the verdict is exact only to a point — `Applied(false)` is
///   also the §5.2 EPQ skip, whose newest committed version was a
///   tombstone — so the check fires only when the row was live at `S`
///   **and** still is at the commit position **and** no delete or
///   truncate of the pk committed inside `(S, commit]` (the carve-out;
///   counted, never a violation). A `DeleteU` is the runner's raw
///   `row_op`, which is blind (§5.0: it places a tombstone whether or
///   not the key has a version), so its *affected* check is
///   one-directional: a row live at `S` must still be live at the commit
///   position.
/// - An applied `UpdateSet`'s effect is recomputed — the base row at the
///   commit position (own writes first, then the latest committed row)
///   rendered at the reported `def_version`, with the set applied — and
///   must equal the recorded read-back.
///
/// **SET NOT NULL** (item 3): success means the validating scan (which
/// runs under the AccessExclusive, every writer waited out, so the
/// scanned state is exactly the commit position) found no NULL in the
/// column; a NULL the model has at that position is a violation. A 23502
/// must have had a NULL: the model state at the statement's snapshot is
/// a sound lower bound of the scanned state (writers the DDL waited out
/// may have committed NULLs in between); when even that shows none, the
/// op is skipped and counted — the card's documented escape.
///
/// **SERIALIZABLE** (item 2): the card's literal "re-execute each
/// committed SER txn on the model state just before its `commit_ts`"
/// cannot be checked as written — an RC writer may legally commit
/// between a SER reader's snapshot `S` and its commit (no SSI edge is
/// recorded to a non-SER writer, §8.5), and the re-execution would then
/// see writes the recorded read never did. The sound restatement checks
/// what SSI actually guarantees: the committed SERIALIZABLE sub-history
/// is serializable, i.e. its serialization graph is acyclic. Edges are
/// added only where they are **forced in every view-equivalent serial
/// order**, so a cycle is a real violation, never a false positive:
///
/// - `wr` (read-from): the read returned a base row whose committed
///   version the model's provenance names — that writer precedes the
///   reader in every equivalent order.
/// - `rw` (antidependency): the reader read the key (a row it saw, or a
///   scan's whole table) and the key's **final** committed writer wrote
///   after the reader's snapshot. The final-write rule is what makes the
///   edge forced: the final writer is the last writer of the key in any
///   equivalent serial order, and the reader's read-from pins it on the
///   far side of the reader. A key a scan did *not* see contributes its
///   final writer only when the key is live at the end (a final
///   tombstone could always be ordered before the scan).
///
/// Every cycle in this graph contains the two-consecutive-rw structure
/// whose danger SSI's §8.3 check exists to abort, so a cycle here is an
/// SSI bug the soak's read and final-state checks cannot see.
struct Effects {
    /// Committed SERIALIZABLE txns: uid → commit ts (the graph's nodes).
    ser: BTreeMap<u32, u64>,
    /// `t` pk → the commit ts of every committed delete/truncate of it
    /// (the RC "no row" carve-out window).
    dead_t: BTreeMap<i64, Vec<u64>>,
    /// The same for `u`, keyed by row identity (`a`).
    dead_u: BTreeMap<i64, Vec<u64>>,
    /// `t` pk → the uid of the last txn that committed a write to it
    /// (insert, update or delete; a truncate hands every key back to "no
    /// writer" — the DDL session is never SERIALIZABLE).
    last_t_write: BTreeMap<i64, u32>,
    /// The same for `u`.
    last_u_write: BTreeMap<i64, u32>,
    /// Reads of committed SER txns, collected during the replay.
    ser_reads: Vec<SerRead>,
    violations: Vec<String>,
    stats: Stats,
}

impl Effects {
    fn new(h: &History) -> Effects {
        let mut ser = BTreeMap::new();
        for txn in &h.txns {
            if txn.level == Level::Serializable {
                if let Fate::Committed(ts) = txn.fate {
                    ser.insert(txn.uid, ts);
                }
            }
        }
        Effects {
            ser,
            dead_t: BTreeMap::new(),
            dead_u: BTreeMap::new(),
            last_t_write: BTreeMap::new(),
            last_u_write: BTreeMap::new(),
            ser_reads: Vec::new(),
            violations: Vec::new(),
            stats: Stats::default(),
        }
    }

    fn violation(&mut self, msg: String) {
        self.violations.push(msg);
    }

    /// The hook body: `state` is this committed txn's commit position,
    /// `own` its writes so far. Never mutates the model's replay inputs.
    #[allow(clippy::too_many_arguments)] // the replay hands exactly this context to every op
    fn op(
        &mut self,
        model: &Model,
        state: &State,
        txn: &TxnRec,
        idx: usize,
        op: &OpRec,
        own: &OwnWrites,
        truncates: &[(u64, u64, u16)],
    ) {
        let Fate::Committed(ts) = txn.fate else {
            return; // build_with replays committed txns only; the guard stays local
        };
        let where_ = where_op(txn, idx);
        match (&op.detail, &op.out) {
            // ---- writes on t -----------------------------------------
            (OpDetail::InsertT { pk, .. }, OpOut::Ok) => {
                self.stats.writes_checked += 1;
                if own.t_row(state, *pk).is_some() {
                    self.violation(format!(
                        "{where_}: engine accepted a duplicate — pk {pk} is live in the \
                         model at its commit position"
                    ));
                }
                self.last_t_write.insert(*pk, txn.uid);
            }
            (
                OpDetail::UpdateSet {
                    pk,
                    attnum,
                    value,
                    new_row,
                },
                OpOut::Applied(applied),
            ) => {
                self.stats.writes_checked += 1;
                self.check_row_write(model, state, txn, idx, op, own, *pk, *applied, ts);
                if *applied {
                    self.recompute_effect(
                        where_, op, own, state, model, *pk, attnum, value, new_row,
                    );
                    self.last_t_write.insert(*pk, txn.uid);
                }
            }
            // C-T6d2: the large-value writes get the same oracle — the
            // applied effect is the base at the commit position with the
            // derived value (or NULL) set on the resolved attnum.
            (
                OpDetail::SetBig {
                    pk,
                    len,
                    tag,
                    attnum,
                    new_row,
                },
                OpOut::Applied(applied),
            ) => {
                self.stats.writes_checked += 1;
                self.check_row_write(model, state, txn, idx, op, own, *pk, *applied, ts);
                if *applied {
                    let value = Some(super::big_blob(*tag, *len as usize));
                    self.recompute_effect(
                        where_, op, own, state, model, *pk, attnum, &value, new_row,
                    );
                    self.last_t_write.insert(*pk, txn.uid);
                }
            }
            (
                OpDetail::NullBig {
                    pk,
                    attnum,
                    new_row,
                },
                OpOut::Applied(applied),
            ) => {
                self.stats.writes_checked += 1;
                self.check_row_write(model, state, txn, idx, op, own, *pk, *applied, ts);
                if *applied {
                    let value = None;
                    self.recompute_effect(
                        where_, op, own, state, model, *pk, attnum, &value, new_row,
                    );
                    self.last_t_write.insert(*pk, txn.uid);
                }
            }
            (OpDetail::DeleteT { pk }, OpOut::Applied(applied)) => {
                self.stats.writes_checked += 1;
                self.check_row_write(model, state, txn, idx, op, own, *pk, *applied, ts);
                if *applied {
                    self.dead_t.entry(*pk).or_default().push(ts);
                    self.last_t_write.insert(*pk, txn.uid);
                }
            }
            // ---- writes on u -----------------------------------------
            (OpDetail::InsertU { row }, OpOut::Ok) => {
                self.stats.writes_checked += 1;
                // No duplicate oracle for `u`: rows are keyed by their
                // unique-per-write `a` constant, so two equal identities
                // cannot be submitted (c1a's ScanU check flags any the
                // engine nonetheless duplicated).
                if let Some(a) = u_identity_of(row) {
                    self.last_u_write.insert(a, txn.uid);
                }
            }
            (OpDetail::DeleteU { key }, OpOut::Applied(applied)) => {
                self.stats.writes_checked += 1;
                // `key: None` is the documented no-op statement; an
                // unbound rowid is already a c1a model anomaly.
                if let Some(key) = key {
                    if let Some(a) = model.u_bind.get(key).copied() {
                        let at_s = model.state_at(op.snap);
                        let live_s = own.u_row(at_s, a).is_some();
                        if *applied {
                            // Blind row op (§5.0): applying over an
                            // already-dead row is legal; only a row live
                            // at S that vanished by the commit position
                            // is a violation.
                            if live_s && own.u_row(state, a).is_none() {
                                self.violation(format!(
                                    "{where_}: Du reported a row written, and the model \
                                     had the row {a} at S={}, but it is gone at its \
                                     commit position",
                                    op.snap
                                ));
                            }
                            self.dead_u.entry(a).or_default().push(ts);
                            self.last_u_write.insert(a, txn.uid);
                        } else {
                            self.check_no_row_u(txn, idx, op, a, live_s, own, state, ts);
                        }
                    }
                }
            }
            // ---- SET NOT NULL (the DDL session's own RC txn) ----------
            (OpDetail::SetNotNull { attnum }, OpOut::Ok) => {
                self.stats.snn_checked += 1;
                let ver = state.schema.version;
                let schema = model.schema_at(ver);
                if !schema.live().any(|c| c.attnum == *attnum) {
                    self.violation(format!(
                        "{where_}: SET NOT NULL succeeded on attnum {attnum}, which is \
                         not live in the model at version {ver}"
                    ));
                } else if let Some(pk) = state.t.iter().find_map(|(pk, row)| {
                    model
                        .render(ver, row)
                        .iter()
                        .any(|(a, v)| a == attnum && v.is_none())
                        .then_some(*pk)
                }) {
                    self.violation(format!(
                        "{where_}: SET NOT NULL on attnum {attnum} succeeded but pk {pk} \
                         is NULL in the model at its commit position"
                    ));
                }
            }
            // ---- the dead-window bookkeeping of a committed TRUNCATE --
            (OpDetail::Truncate { .. }, OpOut::Ok) => {
                for pk in state.t.keys() {
                    self.dead_t.entry(*pk).or_default().push(ts);
                    self.last_t_write.remove(pk);
                }
            }
            _ => {}
        }
        // ---- SERIALIZABLE read collection ------------------------------
        if txn.level == Level::Serializable && !truncate_window(truncates, txn, idx) {
            self.collect_ser_read(model, txn, op, own);
        }
    }

    /// The recomputed effect of an applied UpdateSet: the base at the
    /// commit position (own writes first, then the latest committed
    /// row), rendered at the reported def version, with the set applied.
    #[allow(clippy::too_many_arguments)] // one struct per call site would not be plainer
    fn recompute_effect(
        &mut self,
        where_: String,
        op: &OpRec,
        own: &OwnWrites,
        state: &State,
        model: &Model,
        pk: i64,
        attnum: &u16,
        value: &Option<Value>,
        new_row: &Option<RowVals>,
    ) {
        let Some(ver) = op.def_version else {
            self.violation(format!("{where_}: an UpdateSet recorded no def version"));
            return;
        };
        let Some(recorded) = new_row else {
            self.violation(format!(
                "{where_}: an applied UpdateSet recorded no read-back row"
            ));
            return;
        };
        if let Some(base) = own.t_row(state, pk) {
            let mut expected = model.render(ver, base);
            match expected.iter_mut().find(|(a, _)| a == attnum) {
                Some(slot) => slot.1 = value.clone(),
                None => self.violation(format!(
                    "{where_}: set attnum {attnum} is not live at def version {ver}"
                )),
            }
            if !rows_eq(recorded, &expected) {
                self.violation(format!(
                    "{where_}: update effect {recorded:?} differs from the recompute \
                     {expected:?} (the latest committed row plus own writes)"
                ));
            }
            self.stats.effects_recomputed += 1;
        }
        // A missing base row is already flagged by check_row_write.
    }

    /// The affected/no-row oracle shared by UpdateSet and DeleteT (both
    /// read their row through §4 before the row op).
    #[allow(clippy::too_many_arguments)] // one struct per call site would not be plainer
    fn check_row_write(
        &mut self,
        model: &Model,
        state: &State,
        txn: &TxnRec,
        idx: usize,
        op: &OpRec,
        own: &OwnWrites,
        pk: i64,
        applied: bool,
        ts: u64,
    ) {
        let where_ = where_op(txn, idx);
        if applied {
            if own.t_row(state, pk).is_none() {
                self.violation(format!(
                    "{where_}: {} reported a row written but the model has no pk {pk} \
                     row at its commit position",
                    op.kind.tag()
                ));
            }
            return;
        }
        let at_s = model.state_at(op.snap);
        let live_s = own.t_row(at_s, pk).is_some();
        match txn.level {
            Level::RepeatableRead | Level::Serializable => {
                if live_s {
                    self.violation(format!(
                        "{where_}: {} pk {pk} reported no row but the model has one at \
                         S={} plus its own writes",
                        op.kind.tag(),
                        op.snap
                    ));
                }
            }
            Level::ReadCommitted => {
                // The §5.2 EPQ skip makes a live-at-S row a legal "no
                // row" when a tombstone landed inside (S, commit]; only
                // a row live at both ends with no tombstone in between
                // is a violation.
                let live_c = own.t_row(state, pk).is_some();
                let carved = self
                    .dead_t
                    .get(&pk)
                    .is_some_and(|ds| ds.iter().any(|&d| op.snap < d && d < ts));
                if carved {
                    self.stats.rc_window_carveouts += 1;
                } else if live_s && live_c {
                    self.violation(format!(
                        "{where_}: {} pk {pk} reported no row but the model had it live \
                         at S={} and still at its commit position, with no delete or \
                         truncate in between",
                        op.kind.tag(),
                        op.snap
                    ));
                }
            }
        }
    }

    /// The `u` half of the no-row oracle (a DeleteU that skipped).
    #[allow(clippy::too_many_arguments)] // one struct per call site would not be plainer
    fn check_no_row_u(
        &mut self,
        txn: &TxnRec,
        idx: usize,
        op: &OpRec,
        a: i64,
        live_s: bool,
        own: &OwnWrites,
        state: &State,
        ts: u64,
    ) {
        let where_ = where_op(txn, idx);
        match txn.level {
            Level::RepeatableRead | Level::Serializable => {
                if live_s {
                    self.violation(format!(
                        "{where_}: Du row {a} reported no row but the model has one at \
                         S={} plus its own writes",
                        op.snap
                    ));
                }
            }
            Level::ReadCommitted => {
                let live_c = own.u_row(state, a).is_some();
                let carved = self
                    .dead_u
                    .get(&a)
                    .is_some_and(|ds| ds.iter().any(|&d| op.snap < d && d < ts));
                if carved {
                    self.stats.rc_window_carveouts += 1;
                } else if live_s && live_c {
                    self.violation(format!(
                        "{where_}: Du row {a} reported no row but the model had it live \
                         at S={} and still at its commit position, with no delete in between",
                        op.snap
                    ));
                }
            }
        }
    }

    /// Records one SER txn's read for the graph derivation. Write ops'
    /// base reads count as reads (they are real §4 reads the engine made
    /// at the same snapshot). Only base rows are recorded — a txn reading
    /// its own write adds no read-from edge.
    fn collect_ser_read(&mut self, model: &Model, txn: &TxnRec, op: &OpRec, own: &OwnWrites) {
        let Fate::Committed(_) = txn.fate else {
            return;
        };
        let at_s = model.state_at(op.snap);
        let base_t = |pk: i64| match own.t.get(&pk) {
            Some(_) => false, // own write: the read saw the overlay, not a base row
            None => at_s.t.contains_key(&pk),
        };
        let read = match &op.detail {
            OpDetail::Get { pk } => Some(Scope::PkT {
                pk: *pk,
                seen: matches!(op.out, OpOut::Row(Some(_))) && base_t(*pk),
            }),
            OpDetail::ScanT => Some(Scope::ScanT {
                pks: match &op.out {
                    OpOut::RowsT(rows) => rows
                        .iter()
                        .map(|(pk, _)| *pk)
                        .filter(|pk| base_t(*pk))
                        .collect(),
                    _ => Vec::new(),
                },
            }),
            OpDetail::ScanU => Some(Scope::ScanU {
                ids: match &op.out {
                    OpOut::RowsU(rows) => rows
                        .iter()
                        .filter_map(|(_, r)| u_identity_of(r))
                        .filter(|a| !own.u.contains_key(a))
                        .collect(),
                    _ => Vec::new(),
                },
            }),
            OpDetail::UpdateSet { pk, .. } | OpDetail::DeleteT { pk } => Some(Scope::PkT {
                pk: *pk,
                seen: base_t(*pk),
            }),
            OpDetail::SetBig { pk, .. } | OpDetail::NullBig { pk, .. } => Some(Scope::PkT {
                pk: *pk,
                seen: base_t(*pk),
            }),
            OpDetail::DeleteU { key } => key
                .as_ref()
                .and_then(|k| model.u_bind.get(k).copied())
                .map(|a| Scope::PkU {
                    a,
                    seen: !own.u.contains_key(&a) && at_s.u.contains_key(&a),
                }),
            // C-T6e3: an index scan read exactly the rows it returned —
            // the entry-range SIREAD needs no phantom edges (a row
            // outside the a-range was never read), so each returned base
            // row contributes its own PkT (read-from its version's
            // writer, antidependency to the key's final writer), which is
            // exactly what the row fetch's point read forces.
            OpDetail::IndexScan { oid: Some(_), .. } => {
                if let OpOut::RowsT(rows) = &op.out {
                    for (pk, _) in rows.iter().filter(|(pk, _)| base_t(*pk)) {
                        self.stats.ser_reads += 1;
                        self.ser_reads.push(SerRead {
                            uid: txn.uid,
                            snap: op.snap,
                            scope: Scope::PkT { pk: *pk, seen: true },
                        });
                    }
                }
                return;
            }
            _ => None,
        };
        if let Some(scope) = read {
            self.stats.ser_reads += 1;
            self.ser_reads.push(SerRead {
                uid: txn.uid,
                snap: op.snap,
                scope,
            });
        }
    }

    /// After the replay: the 23502 witnesses, the serialization graph,
    /// and the merge into the verdict.
    fn finish(&mut self, h: &History, model: &Model, v: &mut Verdict) {
        self.check_snn_23502(h, model);
        let cyclic = self.ser_graph(model);
        if let Some(path) = cyclic {
            self.violation(format!(
                "the committed SERIALIZABLE history is not serializable: a cycle in its \
                 serialization graph — {path}"
            ));
        }
        self.stats.ser_txns = self.ser.len();
        v.violations.append(&mut self.violations);
        let s = std::mem::take(&mut self.stats);
        v.stats.writes_checked = s.writes_checked;
        v.stats.effects_recomputed = s.effects_recomputed;
        v.stats.rc_window_carveouts = s.rc_window_carveouts;
        v.stats.snn_checked = s.snn_checked;
        v.stats.snn_23502_skipped = s.snn_23502_skipped;
        v.stats.ser_txns = s.ser_txns;
        v.stats.ser_reads = s.ser_reads;
        v.stats.ser_wr_edges = s.ser_wr_edges;
        v.stats.ser_rw_edges = s.ser_rw_edges;
    }

    /// The 23502 half of SET NOT NULL: the model at the statement's
    /// snapshot is a sound lower bound of the state the validating scan
    /// saw, so a NULL there witnesses the refusal; without one the op is
    /// skipped (the NULL may have committed while the DDL waited out its
    /// AccessExclusive — the card's documented skip, counted below).
    fn check_snn_23502(&mut self, h: &History, model: &Model) {
        for txn in &h.txns {
            for op in &txn.ops {
                let OpDetail::SetNotNull { attnum } = &op.detail else {
                    continue;
                };
                if !matches!(&op.out, OpOut::Err(code) if code == "23502") {
                    continue;
                }
                let at_s = model.state_at(op.snap);
                let ver = at_s.schema.version;
                let witnessed = model.schema_at(ver).live().any(|c| c.attnum == *attnum)
                    && at_s.t.values().any(|row| {
                        model
                            .render(ver, row)
                            .iter()
                            .any(|(a, val)| a == attnum && val.is_none())
                    });
                if witnessed {
                    self.stats.snn_checked += 1;
                } else {
                    self.stats.snn_23502_skipped += 1;
                }
            }
        }
    }

    /// Builds the serialization graph over committed SER txns and returns
    /// a cycle's path if one exists (see the struct docs for the edge
    /// rules and their forcing arguments).
    fn ser_graph(&mut self, model: &Model) -> Option<String> {
        /// One graph edge: target txn and its kind (`"wr"` read-from,
        /// `"rw"` antidependency).
        type Edge = (u32, &'static str);
        /// The graph: node → its out-edges.
        type Graph = BTreeMap<u32, BTreeSet<Edge>>;
        let mut edges: Graph = self.ser.keys().map(|n| (*n, BTreeSet::new())).collect();
        let stats = &mut self.stats;
        let ser = &self.ser;
        let mut add = |from: u32, to: u32, kind: &'static str, snap: u64, edges: &mut Graph| {
            // `rw` edges need the write after the read; `wr` edges are
            // read-from (the writer's ts is below the reader's snapshot
            // by construction of the provenance state).
            if from == to || !ser.contains_key(&from) || !ser.contains_key(&to) {
                return;
            }
            if kind == "rw" && ser[&to] <= snap {
                return;
            }
            if edges
                .get_mut(&from)
                .is_some_and(|set| set.insert((to, kind)))
            {
                match kind {
                    "wr" => stats.ser_wr_edges += 1,
                    _ => stats.ser_rw_edges += 1,
                }
            }
        };
        for r in &self.ser_reads {
            let at_s = model.state_at(r.snap);
            match &r.scope {
                Scope::PkT { pk, seen } => {
                    if *seen {
                        if let Some(w) = at_s.t_prov.get(pk) {
                            add(*w, r.uid, "wr", r.snap, &mut edges);
                        }
                        // The final writer wrote a version after the one
                        // read; whether it updated or deleted the key,
                        // ordering it before the reader would change what
                        // the reader saw.
                        if let Some(w) = self.last_t_write.get(pk) {
                            add(r.uid, *w, "rw", r.snap, &mut edges);
                        }
                    } else if model.final_state.t.contains_key(pk) {
                        // Absent at S but live at the end: its final
                        // writer is an inserter that cannot precede the
                        // read. (A final tombstone could — a delete
                        // before the reader also reads absent — so an
                        // absent read over a finally-dead key adds no
                        // edge.)
                        if let Some(w) = self.last_t_write.get(pk) {
                            add(r.uid, *w, "rw", r.snap, &mut edges);
                        }
                    }
                }
                Scope::ScanT { pks } => {
                    for pk in pks {
                        if let Some(w) = at_s.t_prov.get(pk) {
                            add(*w, r.uid, "wr", r.snap, &mut edges);
                        }
                        if let Some(w) = self.last_t_write.get(pk) {
                            add(r.uid, *w, "rw", r.snap, &mut edges);
                        }
                    }
                    // Phantoms: keys live at the end that the scan did
                    // not return — their final writer is forced after the
                    // scan. (A key absent at the end contributes nothing:
                    // its final tombstone can always be ordered before
                    // the scan.)
                    for pk in model.final_state.t.keys() {
                        if !pks.contains(pk) {
                            if let Some(w) = self.last_t_write.get(pk) {
                                add(r.uid, *w, "rw", r.snap, &mut edges);
                            }
                        }
                    }
                }
                Scope::PkU { a, seen } => {
                    if *seen {
                        if let Some(w) = at_s.u_prov.get(a) {
                            add(*w, r.uid, "wr", r.snap, &mut edges);
                        }
                        if let Some(w) = self.last_u_write.get(a) {
                            add(r.uid, *w, "rw", r.snap, &mut edges);
                        }
                    } else if model.final_state.u.contains_key(a) {
                        // As PkT: only a finally-live row's inserter is
                        // forced after an absent read.
                        if let Some(w) = self.last_u_write.get(a) {
                            add(r.uid, *w, "rw", r.snap, &mut edges);
                        }
                    }
                }
                Scope::ScanU { ids } => {
                    for a in ids {
                        if let Some(w) = at_s.u_prov.get(a) {
                            add(*w, r.uid, "wr", r.snap, &mut edges);
                        }
                        if let Some(w) = self.last_u_write.get(a) {
                            add(r.uid, *w, "rw", r.snap, &mut edges);
                        }
                    }
                    for a in model.final_state.u.keys() {
                        if !ids.contains(a) {
                            if let Some(w) = self.last_u_write.get(a) {
                                add(r.uid, *w, "rw", r.snap, &mut edges);
                            }
                        }
                    }
                }
            }
        }
        // Cycle detection: iterative three-colour DFS; a gray back-edge
        // closes a cycle.
        #[derive(Clone, Copy, PartialEq)]
        enum Colour {
            White,
            Gray,
            Black,
        }
        let mut colour: BTreeMap<u32, Colour> =
            self.ser.keys().map(|n| (*n, Colour::White)).collect();
        for root in self.ser.keys() {
            if colour[root] != Colour::White {
                continue;
            }
            let mut stack: Vec<(u32, Vec<Edge>)> =
                vec![(*root, edges[root].iter().copied().collect())];
            colour.insert(*root, Colour::Gray);
            while let Some((node, mut outs)) = stack.pop() {
                let Some((next, kind)) = outs.pop() else {
                    colour.insert(node, Colour::Black);
                    continue;
                };
                stack.push((node, outs));
                match colour.get(&next).copied().unwrap_or(Colour::White) {
                    Colour::Gray => {
                        let mut path: Vec<String> =
                            stack.iter().map(|(n, _)| format!("T{n}")).collect();
                        path.push(format!("T{next}"));
                        return Some(format!(
                            "{} (the {kind} edge T{node} -> T{next} closes it)",
                            path.join(" -> ")
                        ));
                    }
                    Colour::White => {
                        colour.insert(next, Colour::Gray);
                        let outs = edges[&next].iter().copied().collect();
                        stack.push((next, outs));
                    }
                    Colour::Black => {}
                }
            }
        }
        None
    }
}
