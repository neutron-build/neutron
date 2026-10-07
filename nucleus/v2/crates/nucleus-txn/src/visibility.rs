//! The pure read rule of C-T0 §4. No I/O, no locks: callers supply the intent,
//! the status lookup and the versions from one KV snapshot taken after `S`
//! (I-SNAP-ORDER). The G0 model and the real read path both call this.

use crate::{Intent, LayerData, Seq, Ts, TxnId, TxnStatus};

/// The reader's position.
#[derive(Debug, Clone, Copy)]
pub struct ReadCtx {
    pub txn: TxnId,
    pub snapshot: Ts,
    /// Start seq of the current statement (or cursor). Own writes with
    /// `seq >= stmt_seq` are invisible (I-HALLOWEEN).
    pub stmt_seq: Seq,
}

/// A committed version of the logical key, header already decoded (§2.2):
/// `None` value = tombstone or moved-tombstone; both read as not-found.
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
/// not miss (I-TRUNC): it aborts the process on a missing current-epoch entry
/// and returns Aborted for a missing older-epoch entry.
pub fn read<'a>(
    ctx: &ReadCtx,
    intent: Option<&'a Intent>,
    status: impl Fn(TxnId) -> TxnStatus,
    versions: impl IntoIterator<Item = Version<'a>>,
    edges: &mut Vec<RwEdge>,
) -> Read<'a> {
    if let Some(i) = intent {
        if i.txn == ctx.txn {
            let own = i.layers.iter().rev().find(|l| l.seq < ctx.stmt_seq);
            if let Some(r) = own.and_then(|l| data_read(&l.data)) {
                return r;
            }
        } else if let Some(top) = i.top() {
            if top.data != LayerData::Absent {
                match status(i.txn) {
                    TxnStatus::Committed(c) if c <= ctx.snapshot => {
                        if let Some(r) = data_read(&top.data) {
                            return r;
                        }
                    }
                    TxnStatus::Aborted => {}
                    _ => edges.push(RwEdge::ToIntentOwner(i.txn)),
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

/// `None` = `Absent`: fall through to versions.
fn data_read(d: &LayerData) -> Option<Read<'_>> {
    match d {
        LayerData::Write { value, .. } => Some(Read::Found(value)),
        LayerData::Delete { .. } => Some(Read::NotFound),
        LayerData::Absent => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Layer, RowLockMode};

    const R: TxnId = TxnId { epoch: 1, n: 1 };
    const T: TxnId = TxnId { epoch: 1, n: 2 };
    const OLD: [Version<'static>; 1] = [Version {
        ts: Ts(5),
        value: Some(b"old"),
    }];

    fn ctx(s: u64, seq: Seq) -> ReadCtx {
        ReadCtx {
            txn: R,
            snapshot: Ts(s),
            stmt_seq: seq,
        }
    }

    fn layer(seq: Seq, data: LayerData, lock: RowLockMode) -> Layer {
        Layer {
            seq,
            data_seq: seq,
            data,
            lock,
        }
    }

    fn w(v: &[u8]) -> LayerData {
        LayerData::Write {
            value: v.to_vec(),
            key_changed: false,
        }
    }

    fn write(seq: Seq, v: &[u8]) -> Layer {
        layer(seq, w(v), RowLockMode::NoKeyUpdate)
    }

    fn intent(txn: TxnId, layers: Vec<Layer>) -> Intent {
        Intent { txn, layers }
    }

    #[test]
    fn pending_foreign_intent_is_invisible_and_records_edge() {
        let i = intent(T, vec![write(1, b"new")]);
        let mut e = vec![];
        let r = read(&ctx(10, 1), Some(&i), |_| TxnStatus::Pending, OLD, &mut e);
        assert_eq!(r, Read::Found(b"old"));
        assert_eq!(e, vec![RwEdge::ToIntentOwner(T)]);
    }

    #[test]
    fn committed_intent_respects_snapshot() {
        let i = intent(T, vec![write(1, b"new")]);
        let mut e = vec![];
        let st = |_| TxnStatus::Committed(Ts(11));
        assert_eq!(read(&ctx(10, 1), Some(&i), st, [], &mut e), Read::NotFound);
        assert_eq!(
            read(&ctx(11, 1), Some(&i), st, [], &mut e),
            Read::Found(b"new")
        );
    }

    #[test]
    fn halloween_own_write_in_current_statement_is_invisible() {
        let i = intent(R, vec![write(3, b"mine")]);
        let mut e = vec![];
        let st = |_| TxnStatus::Pending;
        assert_eq!(
            read(&ctx(10, 3), Some(&i), st, OLD, &mut e),
            Read::Found(b"old")
        );
        assert_eq!(
            read(&ctx(10, 4), Some(&i), st, OLD, &mut e),
            Read::Found(b"mine")
        );
    }

    #[test]
    fn own_layers_used_for_earlier_statement() {
        let i = intent(
            R,
            vec![
                layer(1, LayerData::Absent, RowLockMode::Update),
                layer(3, w(b"v3"), RowLockMode::Update),
                layer(5, LayerData::Delete { moved: false }, RowLockMode::Update),
            ],
        );
        let mut e = vec![];
        let st = |_| TxnStatus::Pending;
        assert_eq!(
            read(&ctx(10, 2), Some(&i), st, OLD, &mut e),
            Read::Found(b"old")
        );
        assert_eq!(
            read(&ctx(10, 4), Some(&i), st, OLD, &mut e),
            Read::Found(b"v3")
        );
        assert_eq!(read(&ctx(10, 6), Some(&i), st, OLD, &mut e), Read::NotFound);
    }

    #[test]
    fn aborted_foreign_intent_is_invisible_without_edge() {
        let i = intent(T, vec![write(1, b"new")]);
        let mut e = vec![];
        let r = read(&ctx(10, 1), Some(&i), |_| TxnStatus::Aborted, OLD, &mut e);
        assert_eq!(r, Read::Found(b"old"));
        assert!(e.is_empty());
    }

    #[test]
    fn lock_only_intent_falls_through_without_edge() {
        let i = intent(T, vec![layer(1, LayerData::Absent, RowLockMode::Update)]);
        let mut e = vec![];
        for st in [TxnStatus::Committed(Ts(6)), TxnStatus::Pending] {
            assert_eq!(
                read(&ctx(10, 1), Some(&i), |_| st, OLD, &mut e),
                Read::Found(b"old")
            );
        }
        assert!(e.is_empty());
    }

    /// F01 / DATA-1: locking an own write keeps the data, so a committed
    /// write-then-lock intent is still a visible write.
    #[test]
    fn lock_after_own_write_keeps_data_visible() {
        let i = intent(
            T,
            vec![write(1, b"a"), layer(2, w(b"a"), RowLockMode::Update)],
        );
        let mut e = vec![];
        let r = read(
            &ctx(10, 1),
            Some(&i),
            |_| TxnStatus::Committed(Ts(10)),
            [],
            &mut e,
        );
        assert_eq!(r, Read::Found(b"a"));
    }

    #[test]
    fn newer_version_skipped_with_edge_and_tombstone_hides() {
        let mut e = vec![];
        let v = [
            Version {
                ts: Ts(12),
                value: Some(b"future"),
            },
            Version {
                ts: Ts(8),
                value: None,
            },
            Version {
                ts: Ts(3),
                value: Some(b"ancient"),
            },
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
