//! C-T6e1 tests: [`Core::ingest_versions`] (C-T0 §10 bulk ingest, the
//! C-T6 §7 step-6 primitive) over a `Fault<MemKv>` hand-driven pipeline.
//!
//! - `ingest_ts_is_s`: every ingested entry lands at `version_key(L, S)`
//!   with a live header, and nothing else lands in the range.
//! - `ingest_resolves_shadowing_intent`: a committed `Delete` intent below
//!   `S` (unresolved) would shadow the ingested entry (§4 step 1); the
//!   ingest resolves it, discards an `Aborted` intent in the span, and
//!   leaves `Pending` and committed-above-`S` intents in place.
//! - `invalid_input_writes_nothing`: a key outside the ranges, unsorted
//!   keys and duplicate keys are `Invariant` with nothing written and no
//!   intent of the span removed (validation precedes the resolution pass).
//! - `sync_before_ingest`: a `SyncCommit::Off` commit's record and
//!   resolution become durable only through the ingest's WAL sync (the
//!   sync-spy records `sync_wal` before `ingest_sorted`; `crash(0)` keeps
//!   the commit's data and the ingested entry) — visible because the
//!   Fault's ingest is durable on return but not a barrier.

mod common;

use std::ops::Bound;
use std::path::Path;
use std::sync::{Arc, Mutex};

use nucleus_kv::fault::Fault;
use nucleus_kv::{Batch, Durability, GcFilter, Key, MemKv, OrderedKv, Result, Snapshot, Value};
use nucleus_txn::boot::Core;
use nucleus_txn::catalog::index_ranges;
use nucleus_txn::commit::{CommitPipeline, SyncCommit};
use nucleus_txn::encoding::{
    decode_intent, encode_intent, intent_key, parse_key, version_key, Entry, HEADER_LIVE,
    HEADER_TOMBSTONE,
};
use nucleus_txn::read::{read_key, NoSsi};
use nucleus_txn::resolver::Resolver;
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::{Ts, TxnError, TxnId};

use common::ok;

/// The index storage id whose `/i/` range the tests ingest into (a
/// hand-chosen sid: no catalog is involved).
const SID: u64 = 9;

/// The index's storage ranges (`/u/{sid}/…` and `/i/{sid}/…`), the
/// `ranges` argument shape the C-T6 §7 build passes.
fn idx_ranges() -> Vec<(Key, Key)> {
    index_ranges(SID)
}

/// A logical entry key inside the index's non-unique `/i/{sid}/` range.
fn ikey(suffix: &str) -> Key {
    // index_ranges hands back [/u/…, /i/…]; the /i/ range is the last.
    let mut lo = idx_ranges().last().expect("two ranges").0.clone();
    lo.extend_from_slice(suffix.as_bytes());
    lo
}

// ---- the faulting sync-spy KV ---------------------------------------------

/// One recorded KV call, in call order.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Call {
    Write,
    Sync,
    Ingest,
}

/// An `OrderedKv` wrapper over `Arc<Fault<MemKv>>`: the fault stays
/// reachable so a test can crash it while the core holds the wrapper, and
/// every write / sync / ingest is recorded in call order (the sync spy).
#[derive(Clone)]
struct SpyKv {
    fault: Arc<Fault<MemKv>>,
    calls: Arc<Mutex<Vec<Call>>>,
}

impl SpyKv {
    fn new() -> SpyKv {
        SpyKv {
            fault: Arc::new(Fault::new(MemKv::new(), || Ok(MemKv::new()))),
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().expect("spy calls").clone()
    }
}

impl OrderedKv for SpyKv {
    type Snap = <MemKv as OrderedKv>::Snap;

    fn write(&self, batch: Batch, sync: Durability) -> Result<()> {
        if !batch.ops.is_empty() {
            self.calls.lock().expect("spy calls").push(Call::Write);
        }
        self.fault.write(batch, sync)
    }

    fn sync_wal(&self) -> Result<()> {
        self.calls.lock().expect("spy calls").push(Call::Sync);
        self.fault.sync_wal()
    }

    fn snapshot(&self) -> Self::Snap {
        self.fault.snapshot()
    }

    fn get_latest(&self, key: &[u8]) -> Result<Option<Value>> {
        self.fault.get_latest(key)
    }

    fn ingest_sorted(&self, entries: &mut dyn Iterator<Item = (Key, Value)>) -> Result<()> {
        self.calls.lock().expect("spy calls").push(Call::Ingest);
        self.fault.ingest_sorted(entries)
    }

    fn checkpoint(&self, dir: &Path) -> Result<()> {
        self.fault.checkpoint(dir)
    }

    fn set_gc_filter(&self, filter: Box<dyn GcFilter>) {
        self.fault.set_gc_filter(filter)
    }

    fn set_gc_watermark(&self, watermark: u64) -> Result<()> {
        self.fault.set_gc_watermark(watermark)
    }
}

// ---- the rig ----------------------------------------------------------------

/// A hand-driven core + pipeline over the faulting spy KV (the
/// `write_path` rig shape; the KV is a `Fault<MemKv>` so the tests can
/// crash under the core).
struct Rig {
    kv: SpyKv,
    core: Arc<Core<SpyKv>>,
    pipeline: CommitPipeline<SpyKv>,
}

impl Rig {
    fn new() -> Rig {
        let kv = SpyKv::new();
        let core = Arc::new(ok(Core::open(kv.clone())));
        let pipeline = ok(CommitPipeline::new(Arc::clone(&core)));
        Rig { kv, core, pipeline }
    }

    fn txn(&self) -> Txn {
        self.core.begin(Isolation::ReadCommitted)
    }

    /// submit + drain + process + wait: one commit at a chosen point.
    fn commit(&mut self, txn: Txn, sync: SyncCommit) -> Ts {
        let ticket = ok(self.core.commit_submit(txn, sync));
        let group = self.pipeline.drain_available();
        self.pipeline.process_group(group);
        ok(ticket.wait())
    }

    /// A §4 read of `key` at `snapshot` through a fresh registered view.
    fn read_at(&self, key: &[u8], snapshot: Ts) -> Option<Vec<u8>> {
        let view = self.core.open_view();
        let ctx = ReadCtx {
            txn: TxnId {
                epoch: self.core.epoch(),
                n: u64::MAX,
            },
            snapshot,
            stmt_seq: 1,
        };
        ok(read_key(&self.core, &view, key, &ctx, &mut NoSsi))
    }

    /// The raw stored value of `key`, latest state.
    fn raw(&self, key: &[u8]) -> Option<Value> {
        ok(self.core.latest_get(key))
    }
}

/// A hand-placed delete intent (`Delete { moved: false }`), §5.1's shape
/// (log, count, latch, write — `common::place_intent`'s Delete arm).
fn place_delete(core: &Core<SpyKv>, txn: &Txn, key: &[u8]) {
    let seq = ok(txn.next_seq());
    txn.log_write(seq, key, None);
    ok(core.count_placement(txn));
    let intent = nucleus_txn::Intent {
        txn: txn.id,
        layers: vec![nucleus_txn::Layer {
            seq,
            data_seq: seq,
            data: nucleus_txn::LayerData::Delete { moved: false },
            lock: nucleus_txn::RowLockMode::Update,
        }],
    };
    let mut batch = Batch::default();
    batch.put(intent_key(key), ok(encode_intent(&intent)));
    let _latch = core.latches.lock(key);
    ok(core.write(batch, Durability::No));
}

/// A whole-store raw dump (stored keys), for "nothing was written".
fn raw_dump(kv: &SpyKv) -> Vec<(Key, Value)> {
    let snap = kv.fault.snapshot();
    snap.scan((Bound::Unbounded, Bound::Unbounded), false)
        .map(ok)
        .collect()
}

/// A §2.2 live version value: `header ‖ payload`.
fn live_value(payload: &[u8]) -> Value {
    let mut v = Vec::with_capacity(1 + payload.len());
    v.push(HEADER_LIVE);
    v.extend_from_slice(payload);
    v
}

/// Unwraps a `Result`'s error side (tests may not `unwrap`; clippy denies
/// it workspace-wide).
fn err<T>(r: std::result::Result<T, TxnError>) -> TxnError {
    match r {
        Ok(_) => panic!("expected TxnError, got Ok"),
        Err(e) => e,
    }
}

// ---- the tests ---------------------------------------------------------------

/// Every ingested entry lands at `version_key(L, snap.ts())` with a live
/// header ‖ payload, and nothing else lands in the range.
#[test]
fn ingest_ts_is_s() {
    let mut rig = Rig::new();
    // One committed, resolved row the entries derive from (ts below S).
    let w = rig.txn();
    common::place_intent(&rig.core, &w, b"/t/1/src", b"row");
    let c = rig.commit(w, SyncCommit::On);
    ok(Resolver::run_once(&rig.core));
    let snap = rig.core.registry.take_snapshot();
    let s = snap.ts();
    assert!(s >= c, "the source commit is visible at S ({c:?} <= {s:?})");

    let e1 = ikey("ent-a");
    let e2 = ikey("ent-b");
    // The /i/ range is empty before the ingest.
    assert!(
        raw_dump(&rig.kv)
            .into_iter()
            .filter(|(k, _)| k.starts_with(&idx_ranges().last().expect("ranges").0))
            .count()
            == 0
    );

    ok(rig.core.ingest_versions(
        &snap,
        &idx_ranges(),
        &mut vec![(e1.clone(), b"p1".to_vec()), (e2.clone(), b"p2".to_vec())].into_iter(),
    ));

    for (key, payload) in [(&e1, &b"p1"[..]), (&e2, &b"p2"[..])] {
        let stored = version_key(key, s);
        assert_eq!(
            rig.raw(&stored),
            Some(live_value(payload)),
            "ingested {key:?} at ts {s:?}"
        );
        assert!(matches!(
            parse_key(&stored),
            Some((l, Entry::Version(ts))) if l == key && ts == s
        ));
    }
    // Exactly the two version keys landed in the /i/ range — no other ts,
    // no intent, no end key.
    let landed: Vec<Key> = raw_dump(&rig.kv)
        .into_iter()
        .map(|(k, _)| k)
        .filter(|k| k.starts_with(&idx_ranges().last().expect("ranges").0))
        .collect();
    assert_eq!(landed, vec![version_key(&e1, s), version_key(&e2, s)]);
}

/// The ingest resolves a committed (below-S) `Delete` intent that would
/// shadow the ingested entry, discards an `Aborted` intent in the span,
/// and leaves `Pending` and committed-above-`S` intents in place.
#[test]
fn ingest_resolves_shadowing_intent() {
    let mut rig = Rig::new();
    let ka = ikey("e-a"); // the committed Delete below S
    let kb = ikey("e-b"); // aborted
    let kp = ikey("e-p"); // pending
    let kd = ikey("e-z"); // committed above S

    // (1) A committed Delete intent on ka, unresolved: its owner is a
    // visible commit, but the resolver has not run.
    let d = rig.txn();
    place_delete(&rig.core, &d, &ka);
    let c_del = rig.commit(d, SyncCommit::On);
    // (2) A filler commit so S lands strictly above the Delete's ts.
    let f = rig.txn();
    common::place_intent(&rig.core, &f, b"/t/1/x", b"f");
    let _c_fill = rig.commit(f, SyncCommit::On);
    // (3) An aborted intent in the span.
    let ab = rig.txn();
    common::place_intent(&rig.core, &ab, &kb, b"doomed");
    ok(rig.core.abort(ab));
    // (4) A Pending intent in the span: its txn never commits.
    let p = rig.txn();
    let p_id = p.id;
    common::place_intent(&rig.core, &p, &kp, b"pending");
    drop(p);

    // Through a local Arc so the guard does not borrow the rig (the
    // above-S commit below needs `&mut rig`); the Arc outlives the guard.
    let core = Arc::clone(&rig.core);
    let snap = core.registry.take_snapshot();
    let s = snap.ts();
    assert!(s > c_del, "the Delete committed strictly below S");
    // (5) A commit above S holding an intent in the span.
    let above = rig.txn();
    let above_id = above.id;
    common::place_intent(&rig.core, &above, &kd, b"later");
    let c_above = rig.commit(above, SyncCommit::On);
    assert!(c_above > s);

    // The premise: the unresolved committed Delete shadows ka at S.
    assert_eq!(
        rig.read_at(&ka, s),
        None,
        "before the ingest the committed Delete shadows the key"
    );

    ok(rig.core.ingest_versions(
        &snap,
        &idx_ranges(),
        &mut vec![
            (ka.clone(), b"pa".to_vec()),
            (kb.clone(), b"pb".to_vec()),
            (kp.clone(), b"pp".to_vec()),
            (kd.clone(), b"pd".to_vec()),
        ]
        .into_iter(),
    ));

    // The shadowed key now reads back (the card's `read_key at S → found`),
    // and so do the keys under the removed and kept intents.
    assert_eq!(rig.read_at(&ka, s), Some(b"pa".to_vec()));
    assert_eq!(rig.read_at(&kb, s), Some(b"pb".to_vec()));
    assert_eq!(rig.read_at(&kp, s), Some(b"pp".to_vec()));
    assert_eq!(rig.read_at(&kd, s), Some(b"pd".to_vec()));
    // Raw: the committed Delete was resolved — no intent left, its top
    // layer became a tombstone at its commit ts.
    assert_eq!(rig.raw(&intent_key(&ka)), None, "no intent left on ka");
    assert_eq!(
        rig.raw(&version_key(&ka, c_del)).as_deref(),
        Some(&[HEADER_TOMBSTONE][..])
    );
    // The aborted intent in the span was discarded.
    assert_eq!(rig.raw(&intent_key(&kb)), None, "aborted intent discarded");
    // The Pending intent was left in place, still owned by its txn.
    let raw = rig.raw(&intent_key(&kp)).expect("pending intent kept");
    assert_eq!(ok(decode_intent(&raw)).txn, p_id);
    // The committed-above-S intent was left in place too (§10 resolves
    // only commits at or below S).
    let raw = rig.raw(&intent_key(&kd)).expect("above-S intent kept");
    assert_eq!(ok(decode_intent(&raw)).txn, above_id);
}

/// Invalid input (a key outside every range, unsorted keys, duplicate
/// keys) is `Invariant`, writes nothing and removes no intent of the span.
#[test]
fn invalid_input_writes_nothing() {
    let mut rig = Rig::new();
    // A committed, unresolved Delete intent inside the would-be span: a
    // validation pass that ran after the resolution pass would remove it
    // before noticing the bad input.
    let d = rig.txn();
    let ka = ikey("e-a");
    place_delete(&rig.core, &d, &ka);
    let _c = rig.commit(d, SyncCommit::On);
    let snap = rig.core.registry.take_snapshot();

    let kb = ikey("e-b");
    let kout = b"/t/1/outside".to_vec();
    let ranges = idx_ranges();
    let before = raw_dump(&rig.kv);
    let calls_before = rig.kv.calls();

    // A key outside every range.
    let e = err(rig.core.ingest_versions(
        &snap,
        &ranges,
        &mut vec![(ka.clone(), b"p".to_vec()), (kout, b"p".to_vec())].into_iter(),
    ));
    assert!(matches!(e, TxnError::Invariant(_)), "{e:?}");
    // Unsorted keys.
    let e = err(rig.core.ingest_versions(
        &snap,
        &ranges,
        &mut vec![(kb.clone(), b"p".to_vec()), (ka.clone(), b"p".to_vec())].into_iter(),
    ));
    assert!(matches!(e, TxnError::Invariant(_)), "{e:?}");
    // Duplicate keys are unsorted.
    let e = err(rig.core.ingest_versions(
        &snap,
        &ranges,
        &mut vec![(ka.clone(), b"p".to_vec()), (ka.clone(), b"q".to_vec())].into_iter(),
    ));
    assert!(matches!(e, TxnError::Invariant(_)), "{e:?}");

    // Nothing written, no intent removed.
    assert_eq!(
        rig.kv.calls(),
        calls_before,
        "a rejected ingest performs no write, sync or ingest"
    );
    assert_eq!(
        raw_dump(&rig.kv),
        before,
        "a rejected ingest writes nothing"
    );
    assert!(
        rig.raw(&intent_key(&ka)).is_some(),
        "no intent of the span was removed"
    );

    // The store is still usable: a valid ingest of the same span resolves
    // the intent and lands the entry.
    ok(rig.core.ingest_versions(
        &snap,
        &ranges,
        &mut vec![(ka.clone(), b"p".to_vec())].into_iter(),
    ));
    assert_eq!(rig.raw(&intent_key(&ka)), None);
    assert_eq!(rig.read_at(&ka, snap.ts()), Some(b"p".to_vec()));
}

/// The ingest's WAL sync covers the commits and resolutions the entries
/// were derived from: a `SyncCommit::Off` commit survives `crash(0)`, and
/// the sync-spy records `sync_wal` before `ingest_sorted`.
#[test]
fn sync_before_ingest() {
    let mut rig = Rig::new();
    // An Off commit: its /sys/txn record (and its resolution below) stay
    // unsynced — only the ingest's step-3 sync may make them durable.
    let w = rig.txn();
    common::place_intent(&rig.core, &w, b"/t/1/src", b"row");
    let c = rig.commit(w, SyncCommit::Off);
    // Resolve it (the resolution batch is unsynced too).
    ok(Resolver::run_once(&rig.core));
    let snap = rig.core.registry.take_snapshot();
    let s = snap.ts();
    assert!(s >= c);

    // The index entry derived from that row.
    let ent = ikey("e-src");
    let calls_before = rig.kv.calls();
    ok(rig.core.ingest_versions(
        &snap,
        &idx_ranges(),
        &mut vec![(ent.clone(), b"/t/1/src".to_vec())].into_iter(),
    ));

    // The sync spy: one sync, before the ingest, and nothing between.
    let calls = rig.kv.calls();
    let new_calls = &calls[calls_before.len()..];
    assert_eq!(
        new_calls,
        &[Call::Sync, Call::Ingest],
        "ingest_versions must sync the WAL before the ingest"
    );

    // crash(0): everything the sync covered is durable — the Off commit's
    // row (via its resolution), and the ingested entry with them.
    assert_eq!(ok(rig.kv.fault.crash(0)), 0);
    let raw = rig.kv.fault.snapshot();
    assert_eq!(
        ok(raw.get(&version_key(b"/t/1/src", c))),
        Some(live_value(b"row")),
        "the Off commit's row survived crash(0)"
    );
    assert_eq!(
        ok(raw.get(&version_key(&ent, s))),
        Some(live_value(b"/t/1/src")),
        "the ingested entry survived crash(0)"
    );
    assert_eq!(
        ok(raw.get(&intent_key(b"/t/1/src"))),
        None,
        "the source intent was resolved before the sync"
    );
}
