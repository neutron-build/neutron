//! The pure read rule of C-T0 §4. No I/O, no locks: callers supply the intent,
//! the status lookup and the versions from one KV snapshot taken after `S`
//! (I-SNAP-ORDER). The G0 model and the real read path both call this.

use crate::{HistoryValue, Intent, IntentKind, Seq, Ts, TxnId, TxnStatus};

/// The reader's position.
#[derive(Debug, Clone, Copy)]
pub struct ReadCtx {
    pub txn: TxnId,
    pub snapshot: Ts,
    /// Start seq of the current statement (or cursor). Own writes with
    /// `seq >= stmt_seq` are invisible (I-HALLOWEEN).
    pub stmt_seq: Seq,
}

/// A committed version of the logical key: `None` value = tombstone.
#[derive(Debug, Clone, Copy)]
pub struct Version<'a> {
    pub ts: Ts,
    pub value: Option<&'a [u8]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Read<'a> {
    Found(&'a [u8]),
    NotFound,
}

/// What the reader must report to SSI (§8), if running SERIALIZABLE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RwEdge {
    /// Skipped a foreign intent that was not visible.
    ToIntentOwner(TxnId),
    /// Skipped a committed version newer than the snapshot.
    ToVersionWriter(Ts),
}

/// C-T0 §4. `versions` must be newest first. `status` must not block and must
/// not miss (I-TRUNC); a missing entry for an older-epoch txn means Aborted.
pub fn read<'a>(
    ctx: &ReadCtx,
    intent: Option<&'a Intent>,
    status: impl Fn(TxnId) -> TxnStatus,
    versions: impl IntoIterator<Item = Version<'a>>,
    edges: &mut Vec<RwEdge>,
) -> Read<'a> {
    if let Some(i) = intent {
        if i.txn == ctx.txn {
            if let Some(r) = own_value(i, ctx.stmt_seq) {
                return r;
            }
        } else {
            match status(i.txn) {
                TxnStatus::Committed(c) if c <= ctx.snapshot => match &i.kind {
                    IntentKind::Write(v) => return Read::Found(v),
                    IntentKind::Delete => return Read::NotFound,
                    IntentKind::LockOnly(_) => {}
                },
                _ => {
                    if !matches!(i.kind, IntentKind::LockOnly(_)) {
                        edges.push(RwEdge::ToIntentOwner(i.txn));
                    }
                }
            }
        }
    }
    for v in versions {
        if v.ts > ctx.snapshot {
            edges.push(RwEdge::ToVersionWriter(v.ts));
            continue;
        }
        return match v.value {
            Some(b) => Read::Found(b),
            None => Read::NotFound,
        };
    }
    Read::NotFound
}

/// Newest own value with seq < stmt_seq, or `None` to fall through to versions.
fn own_value(i: &Intent, stmt_seq: Seq) -> Option<Read<'_>> {
    if i.seq < stmt_seq {
        return match &i.kind {
            IntentKind::Write(v) => Some(Read::Found(v)),
            IntentKind::Delete => Some(Read::NotFound),
            IntentKind::LockOnly(_) => newest_history(i, stmt_seq),
        };
    }
    newest_history(i, stmt_seq)
}

fn newest_history(i: &Intent, stmt_seq: Seq) -> Option<Read<'_>> {
    let (_, h) = i.history.iter().rev().find(|(s, _)| *s < stmt_seq)?;
    match h {
        HistoryValue::Write(v) => Some(Read::Found(v)),
        HistoryValue::Delete => Some(Read::NotFound),
        HistoryValue::Absent => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RowLockMode;

    const R: TxnId = TxnId { epoch: 1, n: 1 };
    const T: TxnId = TxnId { epoch: 1, n: 2 };

    fn ctx(s: u64, seq: Seq) -> ReadCtx {
        ReadCtx { txn: R, snapshot: Ts(s), stmt_seq: seq }
    }

    fn intent(txn: TxnId, kind: IntentKind, seq: Seq) -> Intent {
        Intent { txn, kind, seq, history: vec![] }
    }

    #[test]
    fn pending_foreign_intent_is_invisible_and_records_edge() {
        let i = intent(T, IntentKind::Write(b"new".to_vec()), 1);
        let mut e = vec![];
        let v = [Version { ts: Ts(5), value: Some(b"old") }];
        let r = read(&ctx(10, 1), Some(&i), |_| TxnStatus::Pending, v, &mut e);
        assert_eq!(r, Read::Found(b"old"));
        assert_eq!(e, vec![RwEdge::ToIntentOwner(T)]);
    }

    #[test]
    fn committed_intent_respects_snapshot() {
        let i = intent(T, IntentKind::Write(b"new".to_vec()), 1);
        let mut e = vec![];
        let st = |_| TxnStatus::Committed(Ts(11));
        assert_eq!(read(&ctx(10, 1), Some(&i), st, [], &mut e), Read::NotFound);
        assert_eq!(read(&ctx(11, 1), Some(&i), st, [], &mut e), Read::Found(b"new"));
    }

    #[test]
    fn halloween_own_write_in_current_statement_is_invisible() {
        let i = intent(R, IntentKind::Write(b"mine".to_vec()), 3);
        let mut e = vec![];
        let v = [Version { ts: Ts(5), value: Some(b"old") }];
        let st = |_| TxnStatus::Pending;
        assert_eq!(read(&ctx(10, 3), Some(&i), st, v, &mut e), Read::Found(b"old"));
        assert_eq!(read(&ctx(10, 4), Some(&i), st, v, &mut e), Read::Found(b"mine"));
    }

    #[test]
    fn own_history_used_for_earlier_statement() {
        let mut i = intent(R, IntentKind::Delete, 5);
        i.history = vec![(1, HistoryValue::Absent), (3, HistoryValue::Write(b"v3".to_vec()))];
        let mut e = vec![];
        let v = [Version { ts: Ts(5), value: Some(b"old") }];
        let st = |_| TxnStatus::Pending;
        assert_eq!(read(&ctx(10, 2), Some(&i), st, v, &mut e), Read::Found(b"old"));
        assert_eq!(read(&ctx(10, 4), Some(&i), st, v, &mut e), Read::Found(b"v3"));
        assert_eq!(read(&ctx(10, 6), Some(&i), st, v, &mut e), Read::NotFound);
    }

    #[test]
    fn lock_only_intent_falls_through() {
        let i = intent(T, IntentKind::LockOnly(RowLockMode::Update), 1);
        let mut e = vec![];
        let v = [Version { ts: Ts(5), value: Some(b"old") }];
        let r = read(&ctx(10, 1), Some(&i), |_| TxnStatus::Committed(Ts(6)), v, &mut e);
        assert_eq!(r, Read::Found(b"old"));
        assert!(e.is_empty());
    }

    #[test]
    fn newer_version_skipped_with_edge_and_tombstone_hides() {
        let mut e = vec![];
        let v = [
            Version { ts: Ts(12), value: Some(b"future") },
            Version { ts: Ts(8), value: None },
            Version { ts: Ts(3), value: Some(b"ancient") },
        ];
        let r = read(&ctx(10, 1), None, |_| TxnStatus::Pending, v, &mut e);
        assert_eq!(r, Read::NotFound);
        assert_eq!(e, vec![RwEdge::ToVersionWriter(Ts(12))]);
    }

    #[test]
    fn row_lock_matrix_matches_postgres() {
        use RowLockMode::*;
        assert!(!KeyShare.conflicts_with(NoKeyUpdate));
        assert!(KeyShare.conflicts_with(Update));
        assert!(!Share.conflicts_with(Share));
        assert!(Share.conflicts_with(NoKeyUpdate));
        assert!(NoKeyUpdate.conflicts_with(NoKeyUpdate));
        assert!(!KeyShare.conflicts_with(KeyShare));
    }
}
