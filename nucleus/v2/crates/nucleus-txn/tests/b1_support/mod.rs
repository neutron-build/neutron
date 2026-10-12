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
use nucleus_kv::{Batch, Durability, GcFilter, Key, KvError, MemKv, OrderedKv, Result, Value};
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{CommitPipeline, CommitRequest, FailStop, SyncCommit};
use nucleus_txn::encoding::{
    decode_intent, encode_intent, intent_key, parse_key, sys_txn_prefix, sys_txn_prefix_end, Entry,
};
use nucleus_txn::jobs::{Job, JobCtx, JobKind, JobOutcome, JobPoint, JobProbe};
use nucleus_txn::read::{read_key, NoSsi};
use nucleus_txn::resolver::Resolver;
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::visibility::ReadCtx;
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
/// "Surviving" is in the **data** sense: a txn whose `/sys/txn` record is
/// absent may still have survived — the drive may have resolved and
/// §7.4-truncated it (the truncation delete is an unsynced batch a
/// `keep` may legally keep; by I-WAL-ORDER keeping it implies keeping the
/// resolution batch before it, so the data is intact). A record-absent
/// txn therefore counts as surviving exactly when none of its intents
/// remains (an unresolved txn's record can only be absent if the crash
/// dropped it — the Off suffix case).
pub fn check_after_reopen<K: OrderedKv>(model: &Model, core: &Core<K>, kv: &KillKv<MemKv>) {
    let visible = core.visible_ts();
    let reader = reader_id(core);
    let records = surviving_records(core);
    let surviving: std::collections::HashSet<TxnId> = model
        .txns
        .iter()
        .filter(|t| {
            records.contains(&t.id)
                || t.ops
                    .iter()
                    .all(|(k, _)| ok(core.latest_get(&intent_key(k))).is_none())
        })
        .map(|t| t.id)
        .collect();

    // (I1 precondition / I-DURABLE) Every acked On commit's data survived.
    for txn in &model.txns {
        if txn.on {
            assert!(
                surviving.contains(&txn.id),
                "I-DURABLE/I1: acked On commit {:?} lost (records {records:?})",
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
        let expected = model.expected_at(key, &surviving);
        let got = read_at(core, key, visible, reader);
        assert_eq!(
            got,
            expected,
            "key {key:?} reads {got:?} at visible_ts {visible:?}, the surviving history implies {expected:?}"
        );
    }
    for txn in &model.txns {
        let survived = surviving.contains(&txn.id);
        let without: std::collections::HashSet<TxnId> = {
            let mut s = surviving.clone();
            s.remove(&txn.id);
            s
        };
        for (k, _) in &txn.ops {
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

/// The card's `recover(core)`: C-B1a has no boot sweep, so every
/// older-epoch intent is removed by hand with `remove_intent` (the mode
/// from `lookup_for_intent`), then `record_sweep`, two resolver rounds
/// and one GC round. After it: I4 (no intent remains anywhere), I6 (no
/// `/sys/txn` record remains) and I7 (I-GC-QUIESCE: no logical key's
/// newest version `<= W` is a tombstone).
pub fn recover<K: OrderedKv>(core: &Arc<Core<K>>, gc: &nucleus_txn::gc::GcJob<K>) {
    // The hand sweep: collect the intents through one view, then remove
    // each outside it (remove_intent re-reads under the latch).
    let plan: Vec<(Key, TxnId, nucleus_txn::removal::RemovalMode)> = {
        let view = core.open_view();
        let mut plan = Vec::new();
        for row in view.scan((Bound::Unbounded, Bound::Unbounded), false) {
            let (stored, value) = ok(row);
            let Some((logical, Entry::Intent)) = parse_key(&stored) else {
                continue;
            };
            let intent = decode_intent(&value).expect("intent decodes");
            let mode = match ok(core.status.lookup_for_intent(intent.txn)) {
                nucleus_txn::TxnStatus::Committed(_) => nucleus_txn::removal::RemovalMode::Resolve,
                nucleus_txn::TxnStatus::Aborted => nucleus_txn::removal::RemovalMode::Discard,
                other => panic!("older-epoch intent owner reads {other:?}"),
            };
            plan.push((logical.to_vec(), intent.txn, mode));
        }
        plan
    };
    for (key, owner, mode) in &plan {
        ok(nucleus_txn::removal::remove_intent(
            core, key, None, *owner, *mode,
        ));
    }
    core.registry.record_sweep();
    ok(Resolver::run_once(core));
    ok(Resolver::run_once(core));
    ok(gc.run_once(0));

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
    // tombstone or moved-tombstone.
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
            if stored.starts_with(nucleus_txn::encoding::SYS_PREFIX) {
                continue;
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
