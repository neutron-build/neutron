//! Shared support for the C-B1a tests (`tests/b1_jobs.rs`,
//! `tests/b1_crash_resolver_gc.rs`): a killing KV wrapper over `Fault`,
//! the `CrashAtPoint` job probe, a recording fail-stop hook, a slot test
//! job, the crash-matrix workload builder with its model, and the
//! reopen/recover checkers (I1–I7).
#![allow(dead_code)]

use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nucleus_kv::fault::Fault;
use nucleus_kv::{
    Batch, Durability, GcFilter, Key, KvError, MemKv, OrderedKv, Result, Snapshot, Value,
};
use nucleus_txn::boot::Core;
use nucleus_txn::catalog::{Catalog, Ddl, IdxStorage, RelKind, RetiredRow};
use nucleus_txn::commit::{
    spawn_commit_thread_with, CommitConfig, CommitPipeline, CommitRequest, CommitThreadHandle,
    FailStop, SyncCommit,
};
use nucleus_txn::encoding::{
    encode_intent, intent_key, parse_key, sys_txn_prefix, sys_txn_prefix_end, Entry,
};
use nucleus_txn::jobs::{Job, JobCtx, JobKind, JobOutcome, JobPoint, JobProbe};
use nucleus_txn::locks::LockManager;
use nucleus_txn::read::{read_key, NoSsi};
use nucleus_txn::resolver::Resolver;
use nucleus_txn::ssi::Ssi;
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::write::StmtCtx;
use nucleus_txn::{Ts, TxnError, TxnId};

pub fn ok<T, E: std::fmt::Debug>(r: std::result::Result<T, E>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

// ---- the killing KV ---------------------------------------------------------

/// An `OrderedKv` wrapper over `Arc<Fault<B>>` whose [`KillKv::kill`]
/// crashes the fault and then fails every `write`/`sync_wal`/`ingest` with
/// `KvError::Backend("killed")` until [`KillKv::revive`]. Reads
/// (`snapshot`, `get_latest`, `checkpoint`) pass through — the post-crash
/// live store stays inspectable. Clones share the fault and the dead
/// flag. [`KillKv::set_gc_watermark`] passes through and records every
/// value it was handed (the I5 oracle).
pub struct KillKv<B: OrderedKv> {
    pub fault: Arc<Fault<B>>,
    dead: Arc<AtomicBool>,
    watermarks: Arc<Mutex<Vec<u64>>>,
}

impl<B: OrderedKv> Clone for KillKv<B> {
    fn clone(&self) -> Self {
        KillKv {
            fault: Arc::clone(&self.fault),
            dead: Arc::clone(&self.dead),
            watermarks: Arc::clone(&self.watermarks),
        }
    }
}

impl<B: OrderedKv> KillKv<B> {
    pub fn new(fault: Fault<B>) -> KillKv<B> {
        KillKv {
            fault: Arc::new(fault),
            dead: Arc::new(AtomicBool::new(false)),
            watermarks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Crashes the fault keeping `keep` unsynced batches (clamped), then
    /// kills the wrapper. Returns how many were kept.
    pub fn kill(&self, keep: usize) -> usize {
        let kept = ok(self.fault.crash(keep));
        self.dead.store(true, Ordering::SeqCst);
        kept
    }

    /// Un-kills the wrapper (the crashed store stays as `crash` left it).
    pub fn revive(&self) {
        self.dead.store(false, Ordering::SeqCst);
    }

    pub fn killed(&self) -> bool {
        self.dead.load(Ordering::SeqCst)
    }

    /// Every watermark value handed to `set_gc_watermark`, in order (I5:
    /// the reopened `/sys/gc_w` must be ≥ each of them).
    pub fn watermarks(&self) -> Vec<u64> {
        self.watermarks.lock().expect("b1 watermarks").clone()
    }

    fn check(&self) -> Result<()> {
        if self.dead.load(Ordering::SeqCst) {
            Err(KvError::Backend("killed".into()))
        } else {
            Ok(())
        }
    }
}

impl<B: OrderedKv> OrderedKv for KillKv<B> {
    type Snap = B::Snap;

    fn write(&self, batch: Batch, sync: Durability) -> Result<()> {
        self.check()?;
        self.fault.write(batch, sync)
    }

    fn sync_wal(&self) -> Result<()> {
        self.check()?;
        self.fault.sync_wal()
    }

    fn snapshot(&self) -> Self::Snap {
        self.fault.snapshot()
    }

    fn get_latest(&self, key: &[u8]) -> Result<Option<Value>> {
        self.fault.get_latest(key)
    }

    fn ingest_sorted(&self, entries: &mut dyn Iterator<Item = (Key, Value)>) -> Result<()> {
        self.check()?;
        self.fault.ingest_sorted(entries)
    }

    fn checkpoint(&self, dir: &std::path::Path) -> Result<()> {
        self.fault.checkpoint(dir)
    }

    fn set_gc_filter(&self, filter: Box<dyn GcFilter>) {
        self.fault.set_gc_filter(filter)
    }

    fn set_gc_watermark(&self, watermark: u64) -> Result<()> {
        self.watermarks
            .lock()
            .expect("b1 watermarks")
            .push(watermark);
        self.fault.set_gc_watermark(watermark)
    }
}

// ---- the crash-at-point probe ----------------------------------------------

/// A [`JobProbe`] that kills the KV on the `occurrence`-th hit of one
/// chosen [`JobPoint`] (keeping `keep` unsynced batches) and records hit
/// counts per point.
pub struct CrashAtPoint<B: OrderedKv> {
    kv: KillKv<B>,
    point: JobPoint,
    occurrence: u64,
    keep: usize,
    state: Arc<CrashState>,
}

/// What [`CrashAtPoint`] observed (shared so the driver can read it).
pub struct CrashState {
    fired: AtomicBool,
    /// How many batches the kill kept (`None` before it fired).
    kept: Mutex<Option<usize>>,
    hits: Mutex<std::collections::BTreeMap<JobPoint, u64>>,
}

impl CrashState {
    pub fn new() -> Arc<CrashState> {
        Arc::new(CrashState {
            fired: AtomicBool::new(false),
            kept: Mutex::new(None),
            hits: Mutex::new(std::collections::BTreeMap::new()),
        })
    }

    pub fn fired(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }

    /// How many unsynced batches the kill kept, once it fired.
    pub fn kept(&self) -> Option<usize> {
        *self.kept.lock().expect("b1 kept")
    }

    /// Hits of one point so far.
    pub fn hits_of(&self, point: JobPoint) -> u64 {
        self.hits
            .lock()
            .expect("b1 hits")
            .get(&point)
            .copied()
            .unwrap_or(0)
    }

    /// Total hits of every point.
    pub fn total_hits(&self) -> u64 {
        self.hits.lock().expect("b1 hits").values().sum()
    }
}

impl<B: OrderedKv> CrashAtPoint<B> {
    pub fn new(
        kv: KillKv<B>,
        point: JobPoint,
        occurrence: u64,
        keep: usize,
        state: Arc<CrashState>,
    ) -> CrashAtPoint<B> {
        CrashAtPoint {
            kv,
            point,
            occurrence,
            keep,
            state,
        }
    }

    pub fn state(&self) -> Arc<CrashState> {
        Arc::clone(&self.state)
    }
}

impl<B: OrderedKv> JobProbe for CrashAtPoint<B> {
    fn at(&self, point: JobPoint) {
        let count = {
            let mut hits = self.state.hits.lock().expect("b1 hits");
            let e = hits.entry(point).or_insert(0);
            *e += 1;
            *e
        };
        if point == self.point && count == self.occurrence && !self.state.fired() {
            let kept = self.kv.kill(self.keep);
            self.state.fired.store(true, Ordering::SeqCst);
            *self.state.kept.lock().expect("b1 kept") = Some(kept);
        }
    }
}

// ---- hooks and the slot job --------------------------------------------------

/// A [`FailStop`] that records every error.
#[derive(Default)]
pub struct RecFailStop {
    errs: Mutex<Vec<TxnError>>,
}

impl RecFailStop {
    pub fn new() -> Arc<RecFailStop> {
        Arc::new(RecFailStop::default())
    }

    pub fn errors(&self) -> Vec<TxnError> {
        self.errs.lock().expect("b1 errs").clone()
    }
}

impl FailStop for RecFailStop {
    fn on_kv_error(&self, err: &TxnError) {
        self.errs.lock().expect("b1 errs").push(err.clone());
    }
}

/// The §9.3 slot test job (C-B1a item 8): runs a caller-supplied step body
/// under either reserved slot (`Merge` or `Analyze`).
pub struct SlotJob<F>
where
    F: FnMut(&JobCtx) -> std::result::Result<JobOutcome, TxnError> + Send,
{
    kind: JobKind,
    body: F,
}

impl<F> SlotJob<F>
where
    F: FnMut(&JobCtx) -> std::result::Result<JobOutcome, TxnError> + Send,
{
    /// `kind` must be `JobKind::Merge` or `JobKind::Analyze` (the reserved
    /// slots; the crate has no body for them).
    pub fn new(kind: JobKind, body: F) -> SlotJob<F> {
        assert!(
            matches!(kind, JobKind::Merge | JobKind::Analyze),
            "SlotJob is for the merge/analyze slots"
        );
        SlotJob { kind, body }
    }
}

impl<F> Job for SlotJob<F>
where
    F: FnMut(&JobCtx) -> std::result::Result<JobOutcome, TxnError> + Send,
{
    fn kind(&self) -> JobKind {
        self.kind
    }

    fn step(&mut self, cx: &JobCtx) -> std::result::Result<JobOutcome, TxnError> {
        (self.body)(cx)
    }
}

// ---- the workload and its model ---------------------------------------------

/// One committed txn of the workload, in commit order.
pub struct ModTxn {
    pub id: TxnId,
    pub ts: Ts,
    pub on: bool,
    /// `(key, Some(value))` writes and `(key, None)` deletes.
    pub ops: Vec<(Key, Option<Vec<u8>>)>,
}

/// What the workload did, from which the reopen checks derive everything.
pub struct Model {
    /// The committed txns, in commit order.
    pub txns: Vec<ModTxn>,
    /// The aborted txn's keys (fresh keys, never visible).
    pub aborted_keys: Vec<Key>,
    /// Every key the workload touched (writes, deletes, aborts).
    pub all_keys: Vec<Key>,
    /// Every retired `[lo, hi)` range the workload's DDL recorded (C-B1b:
    /// I8 checks these raw-empty after recovery; the value checks skip
    /// keys inside them — a dropped relation's data is deleted, not
    /// read-back).
    pub retired_ranges: Vec<(Key, Key)>,
}

impl Model {
    /// The value the surviving history implies for `key` at `visible_ts`:
    /// the last op of the last surviving txn touching it.
    fn expected_at(
        &self,
        key: &[u8],
        surviving: &std::collections::HashSet<TxnId>,
    ) -> Option<Vec<u8>> {
        let mut value = None;
        for txn in &self.txns {
            if !surviving.contains(&txn.id) {
                continue;
            }
            for (k, v) in &txn.ops {
                if k.as_slice() == key {
                    value = v.clone();
                }
            }
        }
        value
    }
}

/// One `(key, value)` hand-placed write intent, the §5.1 way.
pub fn place_write<K: OrderedKv>(core: &Core<K>, txn: &Txn, key: &[u8], value: &[u8]) {
    let seq = ok(txn.next_seq());
    txn.log_write(seq, key, None);
    ok(core.count_placement(txn));
    let intent = nucleus_txn::Intent {
        txn: txn.id,
        layers: vec![nucleus_txn::Layer {
            seq,
            data_seq: seq,
            data: nucleus_txn::LayerData::Write {
                value: value.to_vec(),
                key_changed: false,
            },
            lock: nucleus_txn::RowLockMode::NoKeyUpdate,
        }],
    };
    let mut batch = Batch::default();
    batch.put(intent_key(key), ok(encode_intent(&intent)));
    let _latch = core.latches.lock(key);
    ok(core.write(batch, Durability::No));
}

/// One hand-placed delete intent (`Delete { moved: false }`).
pub fn place_delete<K: OrderedKv>(core: &Core<K>, txn: &Txn, key: &[u8]) {
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

/// Commits `txn` through a manual pipeline (§3 steps 1–5, on this thread).
pub fn commit<K: OrderedKv>(
    core: &Core<K>,
    pipeline: &mut CommitPipeline<K>,
    txn: Txn,
    sync: SyncCommit,
) -> std::result::Result<Ts, TxnError> {
    let (req, ack) = CommitRequest::new(txn.id, sync, None, false, txn.write_set_keys());
    core.submit(req)?;
    let group = pipeline.drain_available();
    pipeline.process_group(group);
    match ack.recv_timeout(Duration::from_secs(10)) {
        Ok(r) => r,
        Err(_) => Err(TxnError::Invariant("commit never acked".into())),
    }
}

/// A reader id that owns nothing.
pub fn reader_id<K: OrderedKv>(core: &Core<K>) -> TxnId {
    TxnId {
        epoch: core.epoch(),
        n: u64::MAX,
    }
}

/// Reads one logical key at `snapshot` through a fresh registered view.
pub fn read_at<K: OrderedKv>(
    core: &Core<K>,
    key: &[u8],
    snapshot: Ts,
    reader: TxnId,
) -> Option<Vec<u8>> {
    let view = core.open_view();
    let ctx = ReadCtx {
        txn: reader,
        snapshot,
        stmt_seq: 1,
    };
    ok(read_key(core, &view, key, &ctx, &mut NoSsi))
}

/// The crash-matrix workload (C-B1a item 9): a `Fault<MemKv>` behind a
/// [`KillKv`], a manual pipeline with `set_hwm_block(1)`, three committed
/// writers of two keys each with a mix of `On`/`Off`, one more committed
/// txn deleting one existing key (so GC has a tombstone once the resolver
/// resolves it), and one aborted txn holding intents. Deterministic: no
/// threads, a fixed clock second, fixed keys. The **core is kept**: its
/// in-memory resolve queue (§3 step 5, §7.1) is what the hand-driven
/// resolver drains, so it must survive into the driving phase.
pub struct Workload {
    pub kv: KillKv<MemKv>,
    pub core: Arc<Core<KillKv<MemKv>>>,
    pub model: Model,
}

pub fn build_workload() -> Workload {
    let kv = KillKv::new(Fault::new(MemKv::new(), || Ok(MemKv::new())));
    let core = Arc::new(ok(Core::open(kv.clone())));
    let hook = RecFailStop::new();
    let hook_dyn: Arc<dyn nucleus_txn::commit::FailStop> = hook.clone();
    let mut pipeline = ok(CommitPipeline::with_config(
        Arc::clone(&core),
        nucleus_txn::commit::CommitConfig::new().with_fail_stop(hook_dyn),
    ));
    pipeline.set_hwm_block(1);
    assert!(hook.errors().is_empty(), "the workload itself never fails");

    let mut txns = Vec::new();
    let mut all_keys = Vec::new();
    // Three writers of two keys each: On, Off, On (the lost-Off suffix of
    // I2 comes from the middle one when the crash eats its record).
    for i in 0..3u64 {
        let ka = format!("/t/1/a{i}").into_bytes();
        let kb = format!("/t/1/b{i}").into_bytes();
        let va = format!("v{i}a").into_bytes();
        let vb = format!("v{i}b").into_bytes();
        let txn = core.begin(Isolation::ReadCommitted);
        let id = txn.id;
        place_write(&core, &txn, &ka, &va);
        place_write(&core, &txn, &kb, &vb);
        let sync = if i % 2 == 0 {
            SyncCommit::On
        } else {
            SyncCommit::Off
        };
        let ts = ok(commit(&core, &mut pipeline, txn, sync));
        txns.push(ModTxn {
            id,
            ts,
            on: sync == SyncCommit::On,
            ops: vec![(ka.clone(), Some(va)), (kb.clone(), Some(vb))],
        });
        all_keys.push(ka);
        all_keys.push(kb);
    }
    // The deleting txn (On, durable): deletes a0, so resolving it leaves
    // a tombstone for GC.
    let del = core.begin(Isolation::ReadCommitted);
    let del_id = del.id;
    place_delete(&core, &del, b"/t/1/a0");
    let del_ts = ok(commit(&core, &mut pipeline, del, SyncCommit::On));
    txns.push(ModTxn {
        id: del_id,
        ts: del_ts,
        on: true,
        ops: vec![(b"/t/1/a0".to_vec(), None)],
    });

    // One aborted txn holding intents (fresh keys, never visible).
    let mut aborted_keys = Vec::new();
    let aborted = core.begin(Isolation::ReadCommitted);
    place_write(&core, &aborted, b"/t/1/ax", b"doomed-a");
    place_write(&core, &aborted, b"/t/1/bx", b"doomed-b");
    ok(core.abort(aborted));
    aborted_keys.push(b"/t/1/ax".to_vec());
    aborted_keys.push(b"/t/1/bx".to_vec());
    all_keys.extend(aborted_keys.iter().cloned());
    all_keys.push(b"/t/1/a0".to_vec());

    // The pipeline goes away (its channel driver has nothing left to do);
    // the core stays — its resolve queue is the resolver's work.
    drop(pipeline);
    Workload {
        kv,
        core,
        model: Model {
            txns,
            aborted_keys,
            all_keys,
            retired_ranges: Vec::new(),
        },
    }
}

// ---- the reopen checks (I1, I2, I3, I5) --------------------------------------

/// The surviving `/sys/txn` records of a reopened core, as ids.
fn surviving_records<K: OrderedKv>(core: &Core<K>) -> std::collections::HashSet<TxnId> {
    let mut out = std::collections::HashSet::new();
    let view = core.open_view();
    let lo = sys_txn_prefix();
    let hi = sys_txn_prefix_end();
    for row in view.scan(
        (
            Bound::Included(lo.as_slice()),
            Bound::Excluded(hi.as_slice()),
        ),
        false,
    ) {
        let (k, _) = ok(row);
        if let Some(id) = nucleus_txn::encoding::parse_sys_txn_key(&k) {
            out.insert(id);
        }
    }
    out
}

/// Whether any stored entry of `key`'s family (`[intent_key(k), end_key(k))`)
/// is visible through a fresh view — a version (live or tombstone) or an
/// intent.
fn key_family_live<K: OrderedKv>(core: &Core<K>, key: &[u8]) -> bool {
    let view = core.open_view();
    let mut scan = view.scan(
        (
            Bound::Included(intent_key(key).as_slice()),
            Bound::Excluded(nucleus_txn::encoding::end_key(key).as_slice()),
        ),
        false,
    );
    scan.next().is_some()
}

/// The txns of `model` whose data survived, as ids (C-B1c: extracted from
/// `check_after_reopen` so the crash gate can observe the same
/// classification — a lost `Off` commit is the I2 reachability record).
///
/// "Surviving" is in the **data** sense:
/// - an `On` commit always survived (I-DURABLE: its record is synced at
///   the ack, so an absent record means §7.4 truncated it — its data was
///   resolved first);
/// - an `Off` commit survived when its record is present, or when it left
///   nothing behind **but its versions did** (resolved and truncated), or
///   when its intents are still there (it committed; only the record was
///   lost if they are gone and no version replaced them).
pub fn surviving_txns<K: OrderedKv>(
    model: &Model,
    core: &Core<K>,
) -> std::collections::HashSet<TxnId> {
    let records = surviving_records(core);
    model
        .txns
        .iter()
        .filter(|t| {
            records.contains(&t.id)
                || t.on
                || (t
                    .ops
                    .iter()
                    .all(|(k, _)| ok(core.latest_get(&intent_key(k))).is_none())
                    && t.ops.iter().any(|(k, _)| key_family_live(core, k)))
        })
        .map(|t| t.id)
        .collect()
}

/// The model's `Off` commits that did not survive (the I2 lost-suffix
/// subjects, in commit order) — the C-B1a M4 follow-up's observability
/// hook: the crash gate records one cell where this is non-empty, so
/// I2's lost-`Off` branch is shown reachable and checked.
pub fn lost_off_txns<K: OrderedKv>(model: &Model, core: &Core<K>) -> Vec<TxnId> {
    let surviving = surviving_txns(model, core);
    model
        .txns
        .iter()
        .filter(|t| !t.on && !surviving.contains(&t.id))
        .map(|t| t.id)
        .collect()
}

/// Whether `key` falls inside one of the model's retired ranges (I8's
/// subject; such keys are skipped by the value checks — the cleanup
/// deletes them by design).
fn in_retired_range(model: &Model, key: &[u8]) -> bool {
    model
        .retired_ranges
        .iter()
        .any(|(lo, hi)| lo.as_slice() <= key && key < hi.as_slice())
}

/// The card's `check_after_reopen(model, core, kv)`:
/// - (I1) every acked `On` commit's keys read their value at
///   `visible_ts` (the value the surviving history implies — a surviving
///   later delete of one key is that history);
/// - (I2) each txn is all-or-nothing, and the lost `Off` commits form a
///   suffix in commit order;
/// - (I3) the aborted txn's writes are never visible;
/// - (I5) the reopened `/sys/gc_w` is ≥ every watermark the KV was handed
///   before the crash (the `KillKv` recording).
///
/// Keys inside the model's `retired_ranges` are skipped in the value and
/// all-or-nothing checks (C-B1b: their relation was dropped/truncated, so
/// the cleanup deletes the data — I8 checks the ranges raw-empty after
/// recovery instead); a txn all of whose keys are retired is skipped
/// wholesale.
///
/// "Surviving" (see [`surviving_txns`]) is in the **data** sense: an `On`
/// commit always survived (I-DURABLE); an `Off` commit survived when its
/// record survived, or when it was resolved-and-truncated (its versions
/// are its data), or while any of its intents remains. A record-absent
/// `Off` txn with no intents and no versions is the crash's lost suffix.
pub fn check_after_reopen<K: OrderedKv>(model: &Model, core: &Core<K>, kv: &KillKv<MemKv>) {
    let visible = core.visible_ts();
    let reader = reader_id(core);
    let surviving = surviving_txns(model, core);

    // (I1 precondition / I-DURABLE) Every acked On commit's data survived.
    for txn in &model.txns {
        if txn.on {
            assert!(
                surviving.contains(&txn.id),
                "I-DURABLE/I1: acked On commit {:?} lost",
                txn.id
            );
        }
    }

    // (I2) Lost Off commits form a suffix of the Off commit order.
    let off: Vec<&ModTxn> = model.txns.iter().filter(|t| !t.on).collect();
    let mut lost_seen = false;
    for t in off {
        let lost = !surviving.contains(&t.id);
        if lost {
            lost_seen = true;
        } else if lost_seen {
            panic!(
                "I2: Off commit {:?} survived after a lost one (not a suffix)",
                t.id
            );
        }
    }

    // Every key reads exactly what the surviving history implies, and each
    // txn is all-or-nothing: with the txn in the surviving set, each of
    // its keys reads the value the full surviving history implies; without
    // it, each reads what the history minus the txn implies (fresh keys:
    // nothing; the deleted a0: its writer's value). The delete txn's
    // surviving key a0 is thereby attributed correctly, not as its writer
    // being "lost".
    for key in &model.all_keys {
        if in_retired_range(model, key) {
            continue; // a retired range's data: I8's subject, not a value
        }
        let expected = model.expected_at(key, &surviving);
        let got = read_at(core, key, visible, reader);
        assert_eq!(
            got,
            expected,
            "key {key:?} reads {got:?} at visible_ts {visible:?}, the surviving history implies {expected:?}"
        );
    }
    for txn in &model.txns {
        let live_ops: Vec<&(Key, Option<Vec<u8>>)> = txn
            .ops
            .iter()
            .filter(|(k, _)| !in_retired_range(model, k))
            .collect();
        if live_ops.is_empty() {
            continue; // an all-retired txn: I8 covers its data
        }
        let survived = surviving.contains(&txn.id);
        let without: std::collections::HashSet<TxnId> = {
            let mut s = surviving.clone();
            s.remove(&txn.id);
            s
        };
        for (k, _) in &live_ops {
            let implied = if survived {
                model.expected_at(k, &surviving)
            } else {
                model.expected_at(k, &without)
            };
            let got = read_at(core, k, visible, reader);
            assert_eq!(
                got,
                implied,
                "I2: txn {:?} (survived={survived}) is not all-or-nothing: {k:?} reads {got:?}, implied {implied:?}",
                txn.id
            );
        }
    }

    // (I3) The aborted txn's writes are never visible.
    for key in &model.aborted_keys {
        assert!(
            read_at(core, key, visible, reader).is_none(),
            "I3: aborted write on {key:?} is visible"
        );
    }

    // (I5) The reopened /sys/gc_w covers every watermark the KV was handed.
    let reopened_gc_w = {
        let view = core.open_view();
        match ok(view.get(&nucleus_txn::encoding::sys_gc_w_key())) {
            Some(v) => {
                let mut b = [0u8; 8];
                b.copy_from_slice(&v);
                u64::from_be_bytes(b)
            }
            None => 0,
        }
    };
    for w in kv.watermarks() {
        assert!(
            reopened_gc_w >= w,
            "I5: reopened /sys/gc_w {reopened_gc_w} below a handed watermark {w}"
        );
    }
}

// ---- recovery and the post-recover checks (I4, I6, I7) -----------------------

/// The card's `recover(core)`: the boot sweep (C-B1b's `BootSweep`,
/// replacing C-B1a's hand-written removal walk) to `Finished`, two resolver
/// rounds and one GC round, then the quiescence checks below.
pub fn recover<K: OrderedKv>(core: &Arc<Core<K>>, gc: &nucleus_txn::gc::GcJob<K>) {
    let mut sweep = nucleus_txn::jobs::BootSweep::new(Arc::clone(core));
    while ok(sweep.run_once()) != nucleus_txn::jobs::JobOutcome::Finished {
        // The sweep advances every step; `Finished` ends it.
    }
    ok(Resolver::run_once(core));
    ok(Resolver::run_once(core));
    ok(gc.run_once(0));
    check_quiesced(core);
}

/// The post-recovery checks (I4, I6, I7):
/// - (I4) no intent remains anywhere;
/// - (I6) no `/sys/txn` record remains;
/// - (I7, I-GC-QUIESCE) no logical key's newest version `<= W` is a
///   tombstone or moved-tombstone.
pub fn check_quiesced<K: OrderedKv>(core: &Core<K>) {
    // (I4) No intent remains anywhere.
    {
        let view = core.open_view();
        for row in view.scan((Bound::Unbounded, Bound::Unbounded), false) {
            let (stored, _) = ok(row);
            assert!(
                !matches!(parse_key(&stored), Some((_, Entry::Intent))),
                "I4: intent {stored:?} survived recovery"
            );
        }
    }
    // (I6) No /sys/txn record remains.
    {
        let view = core.open_view();
        let lo = sys_txn_prefix();
        let hi = sys_txn_prefix_end();
        let left: Vec<Key> = view
            .scan(
                (
                    Bound::Included(lo.as_slice()),
                    Bound::Excluded(hi.as_slice()),
                ),
                false,
            )
            .map(ok)
            .map(|(k, _)| k)
            .collect();
        assert!(
            left.is_empty(),
            "I6: /sys/txn records survived recovery: {left:?}"
        );
    }
    // (I7, I-GC-QUIESCE) No logical key's newest version <= W is a
    // tombstone or moved-tombstone — in §10's GC scope, so the catalog
    // included (C-B1c): a dropped relation's catalog row must lose its
    // tombstone too.
    {
        let w = {
            let view = core.open_view();
            match ok(view.get(&nucleus_txn::encoding::sys_gc_w_key())) {
                Some(v) => {
                    let mut b = [0u8; 8];
                    b.copy_from_slice(&v);
                    Ts(u64::from_be_bytes(b))
                }
                None => Ts::ZERO,
            }
        };
        let view = core.open_view();
        let mut decided: Option<Key> = None;
        for row in view.scan((Bound::Unbounded, Bound::Unbounded), false) {
            let (stored, value) = ok(row);
            if stored.starts_with(nucleus_txn::encoding::SYS_PREFIX)
                && !stored.starts_with(nucleus_txn::catalog::SYS_CATALOG_PREFIX)
            {
                continue; // every /sys/ key but the catalog is out of scope
            }
            let Some((logical, Entry::Version(ts))) = parse_key(&stored) else {
                continue;
            };
            if decided.as_deref() == Some(logical) {
                continue;
            }
            if ts > w {
                continue; // newer than W: keep walking this key's versions
            }
            decided = Some(logical.to_vec());
            let decoded = ok(nucleus_txn::encoding::decode_version(&value));
            assert!(
                !matches!(
                    decoded,
                    nucleus_txn::encoding::VersionValue::Tombstone { .. }
                ),
                "I7: newest version <= W of {logical:?} is a tombstone"
            );
        }
    }
}

// ---- the C-B1b DDL rig over KillKv -------------------------------------------

/// The full stack the C-B1b tests drive: a `KillKv<MemKv>` (crash +
/// reopen), `Ssi` + `LockManager` + `Catalog` (DDL writes retired rows),
/// and a **spawned commit thread** — the drop-cleanup job commits its
/// marker delete through `Core::commit`, so it needs a live pipeline
/// (§9.3: the job stops before the commit thread at shutdown). All DDL
/// commits are `SyncCommit::On` (a retired row the crash lost would leave
/// a retirement nobody cleans; the lost-DDL case is the C-B1a matrix's
/// business, not this rig's).
pub struct B1Rig {
    pub kv: KillKv<MemKv>,
    pub core: Arc<Core<KillKv<MemKv>>>,
    pub ssi: Arc<Ssi>,
    pub locks: Arc<LockManager>,
    pub catalog: Catalog,
    pub commit: CommitThreadHandle,
    pub hook: Arc<RecFailStop>,
}

impl B1Rig {
    pub fn new() -> B1Rig {
        B1Rig::over(KillKv::new(Fault::new(MemKv::new(), || Ok(MemKv::new()))))
    }

    pub fn over(kv: KillKv<MemKv>) -> B1Rig {
        let core = Arc::new(ok(Core::open(kv.clone())));
        let ssi = Ssi::install(&core);
        let locks = LockManager::install(&core);
        let catalog = ok(Catalog::open(&core, &ssi, &locks));
        let hook = RecFailStop::new();
        let commit = ok(spawn_commit_thread_with(
            Arc::clone(&core),
            CommitConfig::new().with_fail_stop(Arc::clone(&hook) as Arc<dyn FailStop>),
        ));
        B1Rig {
            kv,
            core,
            ssi,
            locks,
            catalog,
            commit,
            hook,
        }
    }

    /// A fresh RC txn (the DDL session).
    pub fn rc(&self) -> Txn {
        self.core.begin(Isolation::ReadCommitted)
    }

    /// The current statement snapshot (§4: RC re-takes per statement).
    pub fn snap(&self) -> Ts {
        self.core.visible_ts()
    }

    /// The blocking Ddl for `txn` at the current snapshot.
    pub fn ddl<'c>(&'c self, txn: &'c Txn) -> Ddl<'c, KillKv<MemKv>> {
        let seq = ok(txn.next_seq());
        Ddl::new(&self.core, txn, StmtCtx::new(self.snap(), seq, seq))
    }

    pub fn create_table(&self, txn: &Txn, name: &[u8]) -> (u64, u64) {
        ok(self
            .catalog
            .catalog_create_table(&self.ddl(txn), name, RelKind::Table, b""))
    }

    pub fn truncate_table(&self, txn: &Txn, oid: u64) -> u64 {
        ok(self.catalog.catalog_truncate_table(&self.ddl(txn), oid))
    }

    pub fn drop_table(&self, txn: &Txn, oid: u64) {
        ok(self.catalog.catalog_drop_table(&self.ddl(txn), oid));
    }

    /// CREATE INDEX with a fresh storage id; returns `(idx oid, sid)`.
    pub fn create_index(&self, txn: &Txn, owning: u64) -> (u64, u64) {
        let sid = ok(self.catalog.alloc_storage_id(&self.core));
        let (oid, _) =
            ok(self
                .catalog
                .catalog_create_index(&self.ddl(txn), owning, IdxStorage::Id(sid), b""));
        (oid, sid)
    }

    pub fn drop_index(&self, txn: &Txn, idx_oid: u64) {
        ok(self.catalog.catalog_drop_index(&self.ddl(txn), idx_oid));
    }

    /// Inserts `key = value` through the write path in `txn`.
    pub fn insert(&self, txn: &Txn, key: &[u8], value: &[u8]) {
        let seq = ok(txn.next_seq());
        ok(self.core.insert_key(
            txn,
            key,
            None,
            value.to_vec(),
            StmtCtx::new(self.snap(), seq, seq),
            nucleus_txn::write::UniqueRule::None,
        ));
    }

    /// The visible retired rows at `visible_ts` (§4 through a fresh view).
    pub fn retired_rows(&self) -> Vec<(u64, RetiredRow)> {
        let view = self.core.open_view();
        let ctx = ReadCtx {
            txn: reader_id(&self.core),
            snapshot: self.core.visible_ts(),
            stmt_seq: 1,
        };
        ok(Catalog::scan_retired(&self.core, &view, &ctx))
    }

    /// Whether `[lo, hi)` holds no raw entry at all (I8; reads pass
    /// through the KillKv even while it is dead).
    pub fn raw_range_empty(&self, lo: &[u8], hi: &[u8]) -> bool {
        let snap = self.kv.snapshot();
        let empty = snap
            .scan((Bound::Included(lo), Bound::Excluded(hi)), false)
            .next()
            .is_none();
        empty
    }

    /// A no-unique-check point read of `key` at snapshot `s`.
    pub fn read_at(&self, key: &[u8], s: Ts) -> Option<Vec<u8>> {
        let view = self.core.open_view();
        let ctx = ReadCtx {
            txn: reader_id(&self.core),
            snapshot: s,
            stmt_seq: 1,
        };
        ok(read_key(&self.core, &view, key, &ctx, &mut NoSsi))
    }
}

/// Drops the rig's stack in dependency order and reopens a fresh one on
/// the surviving store (new epoch, fresh SSI/lock/catalog state).
pub fn reopen_rig(rig: B1Rig) -> B1Rig {
    let B1Rig {
        kv,
        core,
        ssi,
        locks,
        catalog,
        commit,
        hook: _,
    } = rig;
    drop(catalog);
    let _ = commit.shutdown(); // may report the killed KV; expected
    drop(ssi);
    drop(locks);
    drop(core);
    kv.revive();
    B1Rig::over(kv)
}

impl Default for B1Rig {
    fn default() -> Self {
        B1Rig::new()
    }
}

/// A key inside storage `sid`'s table range (the be64 prefix
/// `table_range` defines, plus a suffix).
pub fn tkey(sid: u64, suffix: &str) -> Vec<u8> {
    let (lo, _) = nucleus_txn::catalog::table_range(sid);
    let mut k = lo;
    k.extend_from_slice(suffix.as_bytes());
    k
}

// ---- the C-B1c crash-gate workload --------------------------------------------

/// Builds the full-gate workload (C-B1c item 5): the C-B1b DDL shape
/// (data commits On/Off, a delete, an abort, a TRUNCATE, a DROP, retired
/// ranges, two never-committed txns whose intents become older-epoch
/// after the first crash) plus the C-B1a M4 tail — one `Off` commit
/// **after the last `On`**, so its unsynced record is losable by the
/// first crash and I2's lost-`Off`-suffix branch is reachable (see
/// [`lost_off_txns`]).
pub fn build_gate_workload() -> (B1Rig, Model) {
    let (rig, mut model) = build_ddl_workload();
    let core = &rig.core;
    let tail = rig.rc();
    let tail_id = tail.id;
    let key = b"/t/1/tail0".to_vec();
    place_write(core, &tail, &key, b"tail");
    let tail_ts = ok(core.commit(tail, SyncCommit::Off));
    model.txns.push(ModTxn {
        id: tail_id,
        ts: tail_ts,
        on: false,
        ops: vec![(key.clone(), Some(b"tail".to_vec()))],
    });
    model.all_keys.push(key);
    (rig, model)
}

/// One full compaction of the live store behind `kv` with the registered
/// filter (the gate's "GC + compaction" recovery step): returns the
/// number of entries dropped.
pub fn compact_all(kv: &KillKv<MemKv>) -> usize {
    kv.fault.with_live(|m: &MemKv| m.compact_all())
}

// ---- the C-B1b crash-matrix workload (C-B1a's + DROP and TRUNCATE) ------------

/// Builds the extended workload: the C-B1a shape (three literal-key
/// writers On/Off/On, a deleting txn, an aborted txn holding intents)
/// plus DDL — CREATE TABLE A (+ index), data in A's old storage, TRUNCATE
/// A (retires the old table + index ranges), data in the new storage,
/// CREATE TABLE B, data, DROP B (retires its range) — and two never-
/// committed txns holding intents (a DDL and a data write). Deterministic
/// and single-threaded: every commit is a blocking `Core::commit`.
///
/// Returns the rig (the workload's core keeps its in-memory queue; the
/// drive phase happens after a crash) and the model, `retired_ranges`
/// included for I8.
pub fn build_ddl_workload() -> (B1Rig, Model) {
    let rig = B1Rig::new();
    let core = &rig.core;
    assert!(
        rig.hook.errors().is_empty(),
        "the workload itself never fails"
    );

    let mut txns = Vec::new();
    let mut all_keys = Vec::new();
    // Three writers of two literal keys each: On, Off, On.
    for i in 0..3u64 {
        let ka = format!("/t/1/a{i}").into_bytes();
        let kb = format!("/t/1/b{i}").into_bytes();
        let va = format!("v{i}a").into_bytes();
        let vb = format!("v{i}b").into_bytes();
        let txn = core.begin(Isolation::ReadCommitted);
        let id = txn.id;
        place_write(core, &txn, &ka, &va);
        place_write(core, &txn, &kb, &vb);
        let sync = if i % 2 == 0 {
            SyncCommit::On
        } else {
            SyncCommit::Off
        };
        let ts = ok(core.commit(txn, sync));
        txns.push(ModTxn {
            id,
            ts,
            on: sync == SyncCommit::On,
            ops: vec![(ka.clone(), Some(va)), (kb.clone(), Some(vb))],
        });
        all_keys.push(ka);
        all_keys.push(kb);
    }
    // The deleting txn (On): deletes a0, so resolving it leaves a
    // tombstone for GC.
    let del = core.begin(Isolation::ReadCommitted);
    let del_id = del.id;
    place_delete(core, &del, b"/t/1/a0");
    let del_ts = ok(core.commit(del, SyncCommit::On));
    txns.push(ModTxn {
        id: del_id,
        ts: del_ts,
        on: true,
        ops: vec![(b"/t/1/a0".to_vec(), None)],
    });
    // One aborted txn holding intents (fresh keys, never visible).
    let mut aborted_keys = Vec::new();
    let aborted = core.begin(Isolation::ReadCommitted);
    place_write(core, &aborted, b"/t/1/ax", b"doomed-a");
    place_write(core, &aborted, b"/t/1/bx", b"doomed-b");
    ok(core.abort(aborted));
    aborted_keys.push(b"/t/1/ax".to_vec());
    aborted_keys.push(b"/t/1/bx".to_vec());
    all_keys.extend(aborted_keys.iter().cloned());
    all_keys.push(b"/t/1/a0".to_vec());

    // The DDL half. CREATE TABLE A (durable), data in its old storage
    // (committed, unresolved), CREATE INDEX (durable), TRUNCATE (retires
    // the old table + index ranges), data in the new storage (an Off
    // commit), CREATE TABLE B (durable), data, DROP B (retires it).
    let mut retired_ranges = Vec::new();
    let t1 = rig.rc();
    let t1_id = t1.id;
    let (oid_a, sid_a0) = rig.create_table(&t1, b"A");
    let t1_ts = ok(core.commit(t1, SyncCommit::On));
    txns.push(ModTxn {
        id: t1_id,
        ts: t1_ts,
        on: true,
        ops: vec![],
    });

    let t2 = rig.rc();
    let t2_id = t2.id;
    let r0 = tkey(sid_a0, "r0");
    let r1 = tkey(sid_a0, "r1");
    place_write(core, &t2, &r0, b"old-a");
    place_write(core, &t2, &r1, b"old-b");
    let t2_ts = ok(core.commit(t2, SyncCommit::On));
    txns.push(ModTxn {
        id: t2_id,
        ts: t2_ts,
        on: true,
        ops: vec![
            (r0.clone(), Some(b"old-a".to_vec())),
            (r1.clone(), Some(b"old-b".to_vec())),
        ],
    });
    all_keys.push(r0);
    all_keys.push(r1);

    let t3 = rig.rc();
    let t3_id = t3.id;
    let (_oid_ix, _sid_ix0) = rig.create_index(&t3, oid_a);
    let t3_ts = ok(core.commit(t3, SyncCommit::On));
    txns.push(ModTxn {
        id: t3_id,
        ts: t3_ts,
        on: true,
        ops: vec![],
    });

    let t4 = rig.rc();
    let t4_id = t4.id;
    let sid_a1 = rig.truncate_table(&t4, oid_a);
    let t4_ts = ok(core.commit(t4, SyncCommit::On));
    txns.push(ModTxn {
        id: t4_id,
        ts: t4_ts,
        on: true,
        ops: vec![],
    });
    retired_ranges.push(nucleus_txn::catalog::table_range(sid_a0));
    for r in nucleus_txn::catalog::index_ranges(_sid_ix0) {
        retired_ranges.push(r);
    }

    let t5 = rig.rc();
    let t5_id = t5.id;
    let s0 = tkey(sid_a1, "s0");
    place_write(core, &t5, &s0, b"new-a");
    let t5_ts = ok(core.commit(t5, SyncCommit::Off));
    txns.push(ModTxn {
        id: t5_id,
        ts: t5_ts,
        on: false,
        ops: vec![(s0.clone(), Some(b"new-a".to_vec()))],
    });
    all_keys.push(s0);

    let t6 = rig.rc();
    let t6_id = t6.id;
    let (oid_b, sid_b) = rig.create_table(&t6, b"B");
    let t6_ts = ok(core.commit(t6, SyncCommit::On));
    txns.push(ModTxn {
        id: t6_id,
        ts: t6_ts,
        on: true,
        ops: vec![],
    });

    let t7 = rig.rc();
    let t7_id = t7.id;
    let b0 = tkey(sid_b, "b0");
    let b1 = tkey(sid_b, "b1");
    place_write(core, &t7, &b0, b"bee0");
    place_write(core, &t7, &b1, b"bee1");
    let t7_ts = ok(core.commit(t7, SyncCommit::On));
    txns.push(ModTxn {
        id: t7_id,
        ts: t7_ts,
        on: true,
        ops: vec![
            (b0.clone(), Some(b"bee0".to_vec())),
            (b1.clone(), Some(b"bee1".to_vec())),
        ],
    });
    all_keys.push(b0);
    all_keys.push(b1);

    let t8 = rig.rc();
    let t8_id = t8.id;
    rig.drop_table(&t8, oid_b);
    let t8_ts = ok(core.commit(t8, SyncCommit::On));
    txns.push(ModTxn {
        id: t8_id,
        ts: t8_ts,
        on: true,
        ops: vec![],
    });
    retired_ranges.push(nucleus_txn::catalog::table_range(sid_b));

    // Two never-committed txns: a pending CREATE TABLE (its catalog-row
    // intent is an older-epoch intent under /sys/catalog/ after the
    // crash) and a pending data write. Their Txns are simply dropped.
    let t9 = rig.rc();
    let _ = rig.create_table(&t9, b"C");
    drop(t9);
    let t10 = rig.rc();
    place_write(core, &t10, b"/t/1/px", b"pend");
    drop(t10);

    let model = Model {
        txns,
        aborted_keys,
        all_keys,
        retired_ranges,
    };
    (rig, model)
}

// ---- the C-B1b recovery driver ------------------------------------------------

/// One recovery drive to quiescence over a reopened core (§7.2, §9.2,
/// §9.3): the boot sweep to `Finished`, then rounds of resolver, GC,
/// SSI retention (C-B1c: `Some` runs one pass per round — the crash
/// gate drives every job kind) and drop-cleanup. When a round leaves
/// retired rows eligible-but-gated (their remembered snapshot equals the
/// durable `W` — `W` only passes a snapshot when `visible_ts` does), the
/// driver commits one heartbeat write so the next publish opens the
/// gate; a live system's own commits do this job. `state`, when given,
/// is the armed crash probe: a drive error after the probe fired is the
/// crash; one before it fired is a determinism break and panics (the
/// C-B1a gate's M3 rule).
pub fn drive_recovery<K: OrderedKv>(
    core: &Arc<Core<K>>,
    gc: &Arc<nucleus_txn::gc::GcJob<K>>,
    retention: Option<&nucleus_txn::ssi::RetentionJob<K>>,
    state: Option<&CrashState>,
    heartbeat: usize,
) {
    let fired = || state.is_some_and(|s| s.fired());
    let mut sweep = nucleus_txn::jobs::BootSweep::new(Arc::clone(core));
    let mut cleanup = nucleus_txn::jobs::DropCleanup::new(Arc::clone(core), Arc::clone(gc));
    let mut sweep_done = false;
    for round in 0..40usize {
        if !sweep_done {
            loop {
                match sweep.run_once() {
                    Ok(nucleus_txn::jobs::JobOutcome::Finished) => {
                        sweep_done = true;
                        break;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        if fired() {
                            return;
                        }
                        panic!("drive: the sweep errored before the probe fired: {e:?}");
                    }
                }
            }
        }
        if fired() {
            return;
        }
        let resolved = Resolver::run_once(core);
        if let Err(e) = resolved {
            if fired() {
                return;
            }
            panic!("drive: the resolver errored before the probe fired: {e:?}");
        }
        if let Err(e) = gc.run_once(0) {
            if fired() {
                return;
            }
            panic!("drive: the GC errored before the probe fired: {e:?}");
        }
        if let Some(retention) = retention {
            retention.run_once();
        }
        if fired() {
            return;
        }
        let cleaned = cleanup.run_once();
        if let Err(e) = cleaned {
            if fired() {
                return;
            }
            panic!("drive: the drop cleanup errored before the probe fired: {e:?}");
        }
        if fired() {
            return;
        }
        let idle = matches!(cleaned, Ok(nucleus_txn::jobs::JobOutcome::Idle));
        let rows_left = !core_retired_rows::<K>(core).is_empty();
        if idle && !rows_left {
            // Settle (I4 as the condition): a commit's cleanup enqueue
            // legally lands after its ack (§3 steps 4→5), so drain until
            // no intent remains anywhere and the queue is empty — every
            // intent is placed before its commit, so this is exactly the
            // work left. Bounded: a stuck intent hangs here loudly.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                ok(Resolver::run_once(core));
                ok(gc.run_once(0));
                if no_intents(core) && core.resolve_queue_len() == 0 {
                    return;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "drive: recovery did not settle (intents or queue left)"
                );
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
        if idle && rows_left && round + 1 < 40 {
            // Retired rows are gated at S == W: advance visible_ts past
            // their snapshot with one heartbeat write (a live system's
            // own commits play this part).
            let txn = core.begin(Isolation::ReadCommitted);
            place_write(
                core,
                &txn,
                format!("/t/1/hb{heartbeat}-{round}").as_bytes(),
                b"hb",
            );
            ok(core.commit(txn, SyncCommit::On));
        }
    }
    panic!("drive: recovery did not quiesce within 40 rounds");
}

/// The visible retired rows at `visible_ts` through a fresh view.
fn core_retired_rows<K: OrderedKv>(core: &Core<K>) -> Vec<(u64, RetiredRow)> {
    let view = core.open_view();
    let ctx = ReadCtx {
        txn: reader_id(core),
        snapshot: core.visible_ts(),
        stmt_seq: 1,
    };
    ok(Catalog::scan_retired(core, &view, &ctx))
}

/// Whether no stored intent remains anywhere (the drive's settle
/// condition and I4's subject).
fn no_intents<K: OrderedKv>(core: &Core<K>) -> bool {
    let view = core.open_view();
    for row in view.scan((Bound::Unbounded, Bound::Unbounded), false) {
        let (stored, _) = ok(row);
        if matches!(parse_key(&stored), Some((_, Entry::Intent))) {
            return false;
        }
    }
    true
}

/// (I8) Every retired range is raw-empty and no retired row remains.
pub fn check_retired_cleaned<K: OrderedKv>(core: &Core<K>, kv: &KillKv<MemKv>, model: &Model) {
    for (lo, hi) in &model.retired_ranges {
        let snap = kv.snapshot();
        let left: Vec<Key> = snap
            .scan(
                (
                    std::ops::Bound::Included(lo.as_slice()),
                    std::ops::Bound::Excluded(hi.as_slice()),
                ),
                false,
            )
            .map(ok)
            .map(|(k, _)| k)
            .collect();
        assert!(
            left.is_empty(),
            "I8: retired range {lo:?}..{hi:?} still holds {left:?}"
        );
    }
    let rows = core_retired_rows(core);
    assert!(
        rows.is_empty(),
        "I8: retired rows remain after recovery: {:?}",
        rows.iter().map(|(id, _)| *id).collect::<Vec<_>>()
    );
}
