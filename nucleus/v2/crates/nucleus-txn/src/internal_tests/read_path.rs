//! C-T1a tests: the read path (§4) — a table-driven walk over every §4 case,
//! plus scans. Over MemKv flat and LSM mode.

use std::ops::Bound;

use crate::boot::Core;
use crate::encoding::{encode_intent, encode_version, intent_key, version_key};
use crate::read::{read_key, scan, NoSsi, ReadObserver};
use crate::visibility::{ReadCtx, RwEdge};
use crate::{Intent, Layer, LayerData, RowLockMode, Ts, TxnError, TxnId};
use nucleus_kv::{Batch, Durability, MemKv};

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
        ok(self.core.write(batch, Durability::No));
    }

    fn put_version(&self, key: &[u8], ts: u64, data: LayerData) {
        let mut batch = Batch::default();
        batch.put(
            version_key(key, Ts(ts)),
            encode_version(&data).unwrap_or_default(),
        );
        ok(self.core.write(batch, Durability::No));
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
    // With `L` stored as-is (draft 7.2 §2.2), stored order over prefix-free
    // logical keys is the byte order of the logical keys: "a" < "ab" < "b"
    // < "c". (The old length-prefix layout ordered them a < b < c < ab.)
    for (mode, make) in [("flat", MemKv::new as fn() -> MemKv), ("lsm", MemKv::lsm)] {
        let f = fixture(make);
        let pending = f.core.status.begin();
        f.put_version(b"a", 1, write(b"a1"));
        f.put_intent(
            b"a",
            pending,
            vec![layer(1, write(b"a2"), RowLockMode::NoKeyUpdate)],
        ); // pending: invisible, one edge
        f.put_version(b"ab", 3, write(b"ab3"));
        f.put_version(b"ab", 7, write(b"ab7")); // newest <= 10 wins
        f.put_version(b"b", 2, LayerData::Delete { moved: false }); // tombstone: excluded
        f.put_version(b"c", 4, write(b"c4"));
        ok(f.core.status.set_committed(f.writer, Ts(9)));
        f.put_intent(
            b"c",
            f.writer,
            vec![layer(1, write(b"c5"), RowLockMode::NoKeyUpdate)],
        ); // committed at 9 <= 10: visible
           // A system key among the data must be ignored by scans.
        let mut batch = Batch::default();
        batch.put(crate::encoding::sys_gc_w_key(), 0u64.to_be_bytes().to_vec());
        ok(f.core.write(batch, Durability::No));

        let view = f.core.open_view();
        let mut obs = Collect::default();
        let rows = ok(scan(
            &f.core,
            &view,
            (Bound::Unbounded, Bound::Unbounded),
            &f.ctx(10, 1),
            &mut obs,
        )
        .collect::<Result<Vec<_>, _>>());
        assert_eq!(
            rows,
            vec![
                (b"a".to_vec(), b"a1".to_vec()),
                (b"ab".to_vec(), b"ab7".to_vec()),
                (b"c".to_vec(), b"c5".to_vec()),
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
        )
        .collect::<Result<Vec<_>, _>>());
        assert_eq!(
            rows,
            vec![(b"a".to_vec(), b"a1".to_vec())],
            "{mode}: [a, ab)"
        );

        let mut obs = Collect::default();
        let rows = ok(scan(
            &f.core,
            &view,
            (Bound::Excluded(b"a"), Bound::Unbounded),
            &f.ctx(10, 1),
            &mut obs,
        )
        .collect::<Result<Vec<_>, _>>());
        assert_eq!(
            rows,
            vec![
                (b"ab".to_vec(), b"ab7".to_vec()),
                (b"c".to_vec(), b"c5".to_vec())
            ],
            "{mode}: (a, ..)"
        );

        let mut obs = Collect::default();
        let rows = ok(scan(
            &f.core,
            &view,
            (Bound::Included(b"ab"), Bound::Included(b"c")),
            &f.ctx(10, 1),
            &mut obs,
        )
        .collect::<Result<Vec<_>, _>>());
        assert_eq!(
            rows,
            vec![
                (b"ab".to_vec(), b"ab7".to_vec()),
                (b"c".to_vec(), b"c5".to_vec())
            ],
            "{mode}: [ab, c]"
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
            (Bound::Included(b"a"), Bound::Excluded(b"ab")),
            &own,
            &mut obs,
        )
        .collect::<Result<Vec<_>, _>>());
        assert_eq!(
            rows,
            vec![(b"a".to_vec(), b"a2".to_vec())],
            "{mode}: own scan"
        );
    }
}

#[test]
fn scan_order_is_the_sql_order_of_codec_encoded_keys() {
    // The point of `L` as-is (draft 7.2 §2.2): logical keys produced by
    // `nucleus_codec::encode_key` are prefix-free and order-preserving
    // (C-Q3s P-ORDER), so a scan returns rows in the SQL order of the keys.
    // Single text column, then a composite (text, int4) key.
    use nucleus_codec::{Collation, KeyColumn, KeyType, Value as CodecValue};

    fn text_key(s: &str) -> Vec<u8> {
        let mut out = b"/t/1/".to_vec();
        nucleus_codec::encode_key(
            &[KeyColumn::asc(KeyType::Text(Collation::C))],
            &[Some(CodecValue::Text(s.into()))],
            &mut out,
        )
        .unwrap_or_else(|e| panic!("{e:?}"));
        out
    }

    fn composite_key(s: &str, i: i32) -> Vec<u8> {
        let mut out = b"/t/2/".to_vec();
        nucleus_codec::encode_key(
            &[
                KeyColumn::asc(KeyType::Text(Collation::C)),
                KeyColumn::asc(KeyType::Int4),
            ],
            &[Some(CodecValue::Text(s.into())), Some(CodecValue::Int4(i))],
            &mut out,
        )
        .unwrap_or_else(|e| panic!("{e:?}"));
        out
    }

    for (mode, make) in [("flat", MemKv::new as fn() -> MemKv), ("lsm", MemKv::lsm)] {
        let f = fixture(make);
        // Relation 1 has a single text key column, relation 2 a composite
        // (text, int4) key; different relations never interleave.
        // Single text column: "a" < "ab" < "b" (a prefix pair: a length
        // prefix would order them a, b, ab).
        for (s, v) in [("a", b"1"), ("ab", b"2"), ("b", b"3")] {
            f.put_version(&text_key(s), 4, write(v));
        }
        // Composite (text, int4): ("a",2) < ("a",10) < ("b",1) — int4 order
        // is numeric, not string order ("10" < "2" as bytes of the digits).
        for ((s, i), v) in [(("a", 2), b"x"), (("a", 10), b"y"), (("b", 1), b"z")] {
            f.put_version(&composite_key(s, i), 4, write(v));
        }

        let view = f.core.open_view();
        let mut obs = Collect::default();
        let rows = ok(scan(
            &f.core,
            &view,
            (Bound::Unbounded, Bound::Unbounded),
            &f.ctx(10, 1),
            &mut obs,
        )
        .collect::<Result<Vec<_>, _>>());
        assert_eq!(
            rows,
            vec![
                (text_key("a"), b"1".to_vec()),
                (text_key("ab"), b"2".to_vec()),
                (text_key("b"), b"3".to_vec()),
                (composite_key("a", 2), b"x".to_vec()),
                (composite_key("a", 10), b"y".to_vec()),
                (composite_key("b", 1), b"z".to_vec()),
            ],
            "{mode}: SQL key order"
        );

        // A SQL range over the composite text prefix ("a", ..) returns
        // exactly the ("a", *) rows, in numeric int4 order.
        let mut obs = Collect::default();
        let rows = ok(scan(
            &f.core,
            &view,
            (
                Bound::Included(&composite_key("a", 2)),
                Bound::Excluded(&composite_key("b", 1)),
            ),
            &f.ctx(10, 1),
            &mut obs,
        )
        .collect::<Result<Vec<_>, _>>());
        assert_eq!(
            rows,
            vec![
                (composite_key("a", 2), b"x".to_vec()),
                (composite_key("a", 10), b"y".to_vec()),
            ],
            "{mode}: composite prefix range"
        );
    }
}

#[test]
fn scan_is_lazy_and_streams_rows() {
    // The iterator stays lazy: rows are produced before the range is
    // exhausted, and stopping early reads only what was needed. Checked by
    // yielding rows one by one and asserting after each step.
    let f = fixture(MemKv::new);
    f.put_version(b"k1", 1, write(b"v1"));
    f.put_version(b"k2", 2, write(b"v2"));
    f.put_version(b"k3", 3, write(b"v3"));
    let view = f.core.open_view();
    let mut obs = Collect::default();
    let ctx = f.ctx(10, 1);
    let mut rows = scan(
        &f.core,
        &view,
        (Bound::Unbounded, Bound::Unbounded),
        &ctx,
        &mut obs,
    );
    assert_eq!(
        ok(rows.next().transpose()),
        Some((b"k1".to_vec(), b"v1".to_vec()))
    );
    assert_eq!(
        ok(rows.next().transpose()),
        Some((b"k2".to_vec(), b"v2".to_vec()))
    );
    assert_eq!(
        ok(rows.next().transpose()),
        Some((b"k3".to_vec(), b"v3".to_vec()))
    );
    assert_eq!(rows.next().transpose(), Ok(None));
    assert_eq!(
        rows.next().transpose(),
        Ok(None),
        "exhausted stays exhausted"
    );

    // It stops at the range end: an upper bound past nothing yields nothing.
    let mut obs = Collect::default();
    let rows = ok(scan(
        &f.core,
        &view,
        (Bound::Included(b"k9"), Bound::Unbounded),
        &f.ctx(10, 1),
        &mut obs,
    )
    .collect::<Result<Vec<_>, _>>());
    assert_eq!(rows, Vec::<(Vec<u8>, Vec<u8>)>::new());
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
    )
    .collect::<Result<Vec<_>, _>>();
    assert!(matches!(r, Err(TxnError::Invariant(_))));
}

#[test]
fn corrupt_intent_in_a_view_is_an_error_not_a_panic() {
    let f = fixture(MemKv::new);
    let mut batch = Batch::default();
    batch.put(intent_key(b"k"), b"not an intent".to_vec());
    ok(f.core.write(batch, Durability::No));
    let view = f.core.open_view();
    let r = read_key(&f.core, &view, b"k", &f.ctx(10, 1), &mut NoSsi);
    assert!(matches!(r, Err(TxnError::Corrupt(_))));
    let r = scan(
        &f.core,
        &view,
        (Bound::Unbounded, Bound::Unbounded),
        &f.ctx(10, 1),
        &mut NoSsi,
    )
    .collect::<Result<Vec<_>, _>>();
    assert!(matches!(r, Err(TxnError::Corrupt(_))));
}
