//! C-T6c1afix's test-side extensions to the t6c differential checker:
//! the write half of the TRUNCATE-window rule (c1b FU-B) and the
//! per-level read split (c1a FU-5 / c1b FU-C).
//!
//! These live in a module of the test binaries rather than in
//! `tests/diff/check.rs` because the merged `diff/` tree is outside
//! this card's enforced Touch list — every check the merged tree
//! already makes is delegated to it unchanged; what this module adds
//! runs on top of its verdict, over the same recorded data and with
//! the same exactness argument. Shared by the `t6c_soak` and
//! `t6c_regress` binaries — the `t6b_support` pattern, dead code
//! allowed per binary.
#![allow(dead_code)]

use crate::diff::check;
use crate::diff::{Fate, History, Level, OpDetail, OpOut, OpRec, Run, TxnRec, Verdict};

// ----- the write half of the TRUNCATE window (c1b FU-B) -----------------------

/// The committed TRUNCATEs of a history: `(commit ts, new storage id,
/// session)` — the same triple the merged checker derives internally
/// (its copy is private).
pub fn committed_truncates(h: &History) -> Vec<(u64, u64, u16)> {
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
            _ => None,
        })
        .collect()
}

/// Whether a foreign TRUNCATE landed inside a statement's
/// snapshot-to-catalog-read window (the merged checker's own test,
/// restated here because its copy is private): committed at `T > S`,
/// with the new storage the statement's handle actually read. Storage
/// ids are never reused (§10), so `new_sid == op.sid` pins the storage
/// with no tick or ordering argument; the relation lock a statement
/// takes before its catalog read (§4) is held to the txn's end, so the
/// swap can only have happened before the grant.
pub fn truncate_window(truncates: &[(u64, u64, u16)], txn: &TxnRec, op: &OpRec) -> bool {
    let Some(sid) = op.sid else {
        return false;
    };
    truncates
        .iter()
        .any(|(ts, new_sid, session)| op.snap < *ts && *new_sid == sid && *session != txn.session)
}

/// Where a record sits, for violation context (the merged checker's
/// format, restated because its copy is private).
fn where_op(t: &TxnRec, idx: usize) -> String {
    let op = &t.ops[idx];
    format!(
        "txn {} (session {}, prog {}, attempt {}, {}) op {idx} ({} at S={}, def v{:?})",
        t.uid,
        t.session,
        t.prog,
        t.attempt,
        t.level.short(),
        op.kind.tag(),
        op.snap,
        op.def_version
    )
}

/// The write half of the TRUNCATE-window rule (c1b FU-B): §4's relation
/// rule obliges 40001 for *any* RR/SER access whose catalog read names a
/// post-`S` storage — a completed write is exactly such an access, and
/// an in-window insert is data-consistent with the model (it commits
/// after the truncate, so the model's clear-then-write ordering agrees),
/// so no data check can flag it — only this pass can. A completed write
/// is an insert that returned `Ok`, or an update/delete that returned
/// `Applied(_)` (the refusal is owed for the access itself, whether or
/// not the row op then matched); writes on `u` are exempt — `u` is
/// never truncated, so its storage id can never equal a `t` truncate's
/// `new_sid`.
pub fn check_write_windows(h: &History, truncates: &[(u64, u64, u16)], v: &mut Verdict) {
    for txn in &h.txns {
        if !matches!(txn.level, Level::RepeatableRead | Level::Serializable) {
            continue;
        }
        for (idx, op) in txn.ops.iter().enumerate() {
            let completed_write = matches!(
                (&op.detail, &op.out),
                (OpDetail::InsertT { .. }, OpOut::Ok)
                    | (OpDetail::UpdateSet { .. }, OpOut::Applied(_))
                    | (OpDetail::DeleteT { .. }, OpOut::Applied(_))
            );
            if !completed_write || !truncate_window(truncates, txn, op) {
                continue;
            }
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
                "{}: a successful {} write at S={} answered from the storage a \
                 foreign TRUNCATE committed at ts {ts} inside (S, statement] — 40001 was required",
                where_op(txn, idx),
                op.kind.tag(),
                op.snap
            ));
        }
    }
}

/// The full verdict this card's binaries assert on: every check the
/// merged `diff::check` makes, plus the write-side window pass above.
pub fn check_with_write_windows(run: &Run) -> Verdict {
    let mut v = check(run);
    let truncates = committed_truncates(&run.history);
    check_write_windows(&run.history, &truncates, &mut v);
    v
}

// ----- the per-level read split (c1a FU-5 / c1b FU-C) -------------------------

/// Successful reads by the reading txn's level — the same three read
/// shapes the merged checker counts (`reads_checked`: a Get, a ScanT, a
/// ScanU that did not fail). Pins that reads *succeed* at every level:
/// an over-refusing engine that 40001'd all RR/SER reads starves two of
/// the three (refused txns leave no effects, so no other stat notices).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReadLevels {
    pub rc: usize,
    pub rr: usize,
    pub ser: usize,
}

pub fn read_levels(h: &History) -> ReadLevels {
    let mut out = ReadLevels::default();
    for txn in &h.txns {
        for op in &txn.ops {
            let successful_read = matches!(
                (&op.detail, &op.out),
                (OpDetail::Get { .. }, OpOut::Row(_))
                    | (OpDetail::ScanT, OpOut::RowsT(_))
                    | (OpDetail::ScanU, OpOut::RowsU(_))
            );
            if !successful_read {
                continue;
            }
            match txn.level {
                Level::ReadCommitted => out.rc += 1,
                Level::RepeatableRead => out.rr += 1,
                Level::Serializable => out.ser += 1,
            }
        }
    }
    out
}
