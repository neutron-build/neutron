//! C-T1a tests: the read path (§4) — a table-driven walk over every §4 case,
//! plus scans. Over MemKv flat and LSM mode.

use std::ops::Bound;

use nucleus_kv::{Batch, Durability, MemKv, OrderedKv};
use nucleus_txn::boot::Core;
use nucleus_txn::encoding::{encode_intent, encode_version, intent_key, version_key};
use nucleus_txn::read::{read_key, scan, NoSsi, ReadObserver};
use nucleus_txn::visibility::{ReadCtx, RwEdge};
use nucleus_txn::{Intent, Layer, LayerData, RowLockMode, Ts, TxnError, TxnId};

fn ok<T, E: std::fmt::Debug>(r: Result<T, E>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

fn layer(seq: u32, data: LayerData, lock: RowLockMode) -> Layer {
    Layer {
        seq,
        data_seq: seq,
        data,
        lock,
    }
}

fn write(value: &[u8]) -> LayerData {
    LayerData::Write {
        value: value.to_vec(),
        key_changed: false,
    }
}

/// Records SSI edges, to check the §4 hook.
#[derive(Default)]
struct Collect(Vec<RwEdge>);

impl ReadObserver for Collect {
    fn on_edge(&mut self, edge: RwEdge) {
        self.0.push(edge);
    }
}

struct Fixture {
    core: Core<MemKv>,
    reader: TxnId,
    writer: TxnId,
}

fn fixture(make: fn() -> MemKv) -> Fixture {
    let core = ok(Core::open(make()));
    let reader = core.status.begin();
    let writer = core.status.begin();
    core.advance_visible_ts(Ts(10));
    Fixture {
        core,
        reader,
        writer,
    }
}

impl Fixture {
    fn put_intent(&self, key: &[u8], txn: TxnId, layers: Vec<Layer>) {
        let mut batch = Batch::default();
        batch.put(intent_key(key), ok(encode_intent(&Intent { txn, layers })));
        ok(self.core.kv.write(batch, Durability::No));
    }

    fn put_version(&self, key: &[u8], ts: u64, data: LayerData) {
        let mut batch = Batch::default();
        batch.put(
            version_key(key, Ts(ts)),
            encode_version(&data).unwrap_or_default(),
        );
        ok(self.core.kv.write(batch, Durability::No));
    }

    fn ctx(&self, snapshot: u64, stmt_seq: u32) -> ReadCtx {
        ReadCtx {
            txn: self.reader,
            snapshot: Ts(snapshot),
            stmt_seq,
        }
    }

    fn read(
        &self,
        key: &[u8],
        snapshot: u64,
        stmt_seq: u32,
    ) -> (Result<Option<Vec<u8>>, TxnError>, Vec<RwEdge>) {
        let view = self.core.open_view();
        let mut obs = Collect::default();
        let r = read_key(
            &self.core,
            &view,
            key,
            &self.ctx(snapshot, stmt_seq),
            &mut obs,
        );
        (r, obs.0)
    }
}

/// What a case expects.
enum Want {
    Val(Option<Vec<u8>>),
    Invariant,
}

fn v(x: &[u8]) -> Option<Vec<u8>> {
    Some(x.to_vec())
}

#[test]
fn every_section4_case() {
    type Case = Box<dyn Fn(&Fixture) -> (Want, Vec<RwEdge>)>;
    let cases: Vec<(&str, Case)> = vec![
        (
            "own layers by seq (halloween at seq0)",
            Box::new(|f| {
                f.put_intent(
                    b"k",
                    f.reader,
                    vec![
                        layer(2, write(b"v2"), RowLockMode::NoKeyUpdate),
                        layer(4, write(b"v4"), RowLockMode::Update),
                    ],
                );
                f.put_version(b"k", 1, write(b"old"));
                assert_eq!(f.read(b"k", 10, 2).0, Ok(v(b"old"))); // seq < 2: none
                assert_eq!(f.read(b"k", 10, 3).0, Ok(v(b"v2"))); // newest layer seq < 3
                assert_eq!(f.read(b"k", 10, 5).0, Ok(v(b"v4")));
                (Want::Val(v(b"v4")), vec![])
            }),
        ),
        (
            "own delete hides even a live version",
            Box::new(|f| {
                f.put_intent(
                    b"k",
                    f.reader,
                    vec![layer(
                        3,
                        LayerData::Delete { moved: false },
                        RowLockMode::Update,
                    )],
                );
                f.put_version(b"k", 1, write(b"old"));
                (Want::Val(None), vec![])
            }),
        ),
        (
            "own lock-only layer falls through to versions",
            Box::new(|f| {
                f.put_intent(
                    b"k",
                    f.reader,
                    vec![layer(1, LayerData::Absent, RowLockMode::Update)],
                );
                f.put_version(b"k", 1, write(b"old"));
                (Want::Val(v(b"old")), vec![])
            }),
        ),
        (
            "foreign lock-only intent falls through, no edge",
            Box::new(|f| {
                f.put_intent(
                    b"k",
                    f.writer,
                    vec![layer(1, LayerData::Absent, RowLockMode::Update)],
                );
                f.put_version(b"k", 1, write(b"old"));
                (Want::Val(v(b"old")), vec![])
            }),
        ),
        (
            "foreign committed at c <= S is the newest version",
            Box::new(|f| {
                f.put_intent(
                    b"k",
                    f.writer,
                    vec![layer(1, write(b"new"), RowLockMode::NoKeyUpdate)],
                );
                ok(f.core.status.set_committed(f.writer, Ts(6)));
                f.put_version(b"k", 1, write(b"old"));
                (Want::Val(v(b"new")), vec![])
            }),
        ),
        (
            "foreign committed at c > S is invisible with an edge",
            Box::new(|f| {
                f.put_intent(
                    b"k",
                    f.writer,
                    vec![layer(1, write(b"new"), RowLockMode::NoKeyUpdate)],
                );
                ok(f.core.status.set_committed(f.writer, Ts(12)));
                f.core.advance_visible_ts(Ts(12));
                f.put_version(b"k", 1, write(b"old"));
                (Want::Val(v(b"old")), vec![RwEdge::ToIntentOwner(f.writer)])
            }),
        ),
        (
            "foreign pending is invisible with an edge",
            Box::new(|f| {
                f.put_intent(
                    b"k",
                    f.writer,
                    vec![layer(1, write(b"new"), RowLockMode::NoKeyUpdate)],
                );
                f.put_version(b"k", 1, write(b"old"));
                (Want::Val(v(b"old")), vec![RwEdge::ToIntentOwner(f.writer)])
            }),
        ),
        (
            "foreign aborted is invisible without an edge",
            Box::new(|f| {
                f.put_intent(
                    b"k",
                    f.writer,
                    vec![layer(1, write(b"new"), RowLockMode::NoKeyUpdate)],
                );
                ok(f.core.status.set_aborted(f.writer));
                f.put_version(b"k", 1, write(b"old"));
                (Want::Val(v(b"old")), vec![])
            }),
        ),
        (
            "versions above S skipped with edges, first <= S wins",
            Box::new(|f| {
                f.put_version(b"k", 12, write(b"future"));
                f.put_version(b"k", 11, LayerData::Delete { moved: false });
                f.put_version(b"k", 8, write(b"old"));
                (
                    Want::Val(v(b"old")),
                    vec![
                        RwEdge::ToVersionWriter(Ts(12)),
                        RwEdge::ToVersionWriter(Ts(11)),
                    ],
                )
            }),
        ),
        (
            "tombstone at <= S is not-found",
            Box::new(|f| {
                f.put_version(b"k", 8, LayerData::Delete { moved: false });
                (Want::Val(None), vec![])
            }),
        ),
        (
            "moved tombstone at <= S is not-found",
            Box::new(|f| {
                f.put_version(b"k", 8, LayerData::Delete { moved: true });
                (Want::Val(None), vec![])
            }),
        ),
        (
            "tombstone above S skipped with an edge, older live returned",
            Box::new(|f| {
                f.put_version(b"k", 12, LayerData::Delete { moved: true });
                f.put_version(b"k", 5, write(b"old"));
                (Want::Val(v(b"old")), vec![RwEdge::ToVersionWriter(Ts(12))])
            }),
        ),
        (
            "missing current-epoch status for an intent in a view is fatal",
            Box::new(|f| {
                let ghost = TxnId {
                    epoch: f.core.epoch(),
                    n: 9999,
                };
                f.put_intent(
                    b"k",
                    ghost,
                    vec![layer(1, write(b"boo"), RowLockMode::Update)],
                );
                (Want::Invariant, vec![])
            }),
        ),
        (
            "empty key reads not-found",
            Box::new(|_f| (Want::Val(None), vec![])),
        ),
    ];

    for (mode, make) in [("flat", MemKv::new as fn() -> MemKv), ("lsm", MemKv::lsm)] {
        for (name, case) in &cases {
            let f = fixture(make);
            let (want, edges) = case(&f);
            let (got, got_edges) = f.read(b"k", 10, 5);
            match want {
                Want::Val(expected) => {
                    assert_eq!(got, Ok(expected), "{mode}/{name}: value");
                    assert_eq!(got_edges, edges, "{mode}/{name}: edges");
                }
                Want::Invariant => {
                    assert!(
                        matches!(got, Err(TxnError::Invariant(_))),
                        "{mode}/{name}: expected invariant error, got {got:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn scan_returns_logical_rows_in_key_order() {
    for (mode, make) in [("flat", MemKv::new as fn() -> MemKv), ("lsm", MemKv::lsm)] {
        let f = fixture(make);
        // Encoded order of the logical keys: the be32 length prefix sorts
        // first, so "a" < "b" < "c" < "ab".
        let pending = f.core.status.begin();
        f.put_version(b"a", 1, write(b"a1"));
        f.put_intent(
            b"a",
            pending,
            vec![layer(1, write(b"a2"), RowLockMode::NoKeyUpdate)],
        ); // pending: invisible, one edge
        f.put_version(b"b", 2, LayerData::Delete { moved: false }); // tombstone: excluded
        f.put_version(b"c", 4, write(b"c4"));
        ok(f.core.status.set_committed(f.writer, Ts(9)));
        f.put_intent(
            b"c",
            f.writer,
            vec![layer(1, write(b"c5"), RowLockMode::NoKeyUpdate)],
        ); // committed at 9 <= 10: visible
        f.put_version(b"ab", 3, write(b"ab3"));
        f.put_version(b"ab", 7, write(b"ab7")); // newest <= 10 wins
                                                // A system key among the data must be ignored by scans.
        let mut batch = Batch::default();
        batch.put(
            nucleus_txn::encoding::sys_gc_w_key(),
            0u64.to_be_bytes().to_vec(),
        );
        ok(f.core.kv.write(batch, Durability::No));

        let view = f.core.open_view();
        let mut obs = Collect::default();
        let rows = ok(scan(
            &f.core,
            &view,
            (Bound::Unbounded, Bound::Unbounded),
            &f.ctx(10, 1),
            &mut obs,
        ));
        assert_eq!(
            rows,
            vec![
                (b"a".to_vec(), b"a1".to_vec()),
                (b"c".to_vec(), b"c5".to_vec()),
                (b"ab".to_vec(), b"ab7".to_vec()),
            ],
            "{mode}: unbounded scan"
        );
        // The pending intent on "a" produced one edge.
        assert_eq!(obs.0, vec![RwEdge::ToIntentOwner(pending)], "{mode}: edges");

        // Bounded scans over logical keys.
        let mut obs = Collect::default();
        let rows = ok(scan(
            &f.core,
            &view,
            (Bound::Included(b"a"), Bound::Excluded(b"ab")),
            &f.ctx(10, 1),
            &mut obs,
        ));
        assert_eq!(
            rows,
            vec![
                (b"a".to_vec(), b"a1".to_vec()),
                (b"c".to_vec(), b"c5".to_vec())
            ],
            "{mode}: [a, ab)"
        );

        let mut obs = Collect::default();
        let rows = ok(scan(
            &f.core,
            &view,
            (Bound::Excluded(b"a"), Bound::Unbounded),
            &f.ctx(10, 1),
            &mut obs,
        ));
        assert_eq!(
            rows,
            vec![
                (b"c".to_vec(), b"c5".to_vec()),
                (b"ab".to_vec(), b"ab7".to_vec())
            ],
            "{mode}: (a, ..)"
        );

        let mut obs = Collect::default();
        let rows = ok(scan(
            &f.core,
            &view,
            (Bound::Included(b"b"), Bound::Included(b"c")),
            &f.ctx(10, 1),
            &mut obs,
        ));
        assert_eq!(
            rows,
            vec![(b"c".to_vec(), b"c5".to_vec())],
            "{mode}: [b, c]"
        );

        // Own intents are read by scans too (the pending txn reads "a").
        let own = ReadCtx {
            txn: pending,
            snapshot: Ts(10),
            stmt_seq: 2,
        };
        let mut obs = Collect::default();
        let rows = ok(scan(
            &f.core,
            &view,
            (Bound::Included(b"a"), Bound::Excluded(b"b")),
            &own,
            &mut obs,
        ));
        assert_eq!(
            rows,
            vec![(b"a".to_vec(), b"a2".to_vec())],
            "{mode}: own scan"
        );
    }
}

#[test]
fn scan_missing_current_epoch_status_is_fatal() {
    let f = fixture(MemKv::new);
    let ghost = TxnId {
        epoch: f.core.epoch(),
        n: 4242,
    };
    f.put_intent(
        b"k",
        ghost,
        vec![layer(1, write(b"boo"), RowLockMode::Update)],
    );
    let view = f.core.open_view();
    let mut obs = Collect::default();
    let r = scan(
        &f.core,
        &view,
        (Bound::Unbounded, Bound::Unbounded),
        &f.ctx(10, 1),
        &mut obs,
    );
    assert!(matches!(r, Err(TxnError::Invariant(_))));
}

#[test]
fn corrupt_intent_in_a_view_is_an_error_not_a_panic() {
    let f = fixture(MemKv::new);
    let mut batch = Batch::default();
    batch.put(intent_key(b"k"), b"not an intent".to_vec());
    ok(f.core.kv.write(batch, Durability::No));
    let view = f.core.open_view();
    let r = read_key(&f.core, &view, b"k", &f.ctx(10, 1), &mut NoSsi);
    assert!(matches!(r, Err(TxnError::Corrupt(_))));
    let r = scan(
        &f.core,
        &view,
        (Bound::Unbounded, Bound::Unbounded),
        &f.ctx(10, 1),
        &mut NoSsi,
    );
    assert!(matches!(r, Err(TxnError::Corrupt(_))));
}
