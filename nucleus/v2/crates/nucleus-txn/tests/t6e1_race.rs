//! C-T6e1fix tests: the ingest path's interleaving pins (the C-T6e1 gate's
//! M-1/M-2 follow-ups), over the same `Fault<MemKv>` pipeline shape as
//! `t6e1_ingest.rs` plus a spawned commit thread (the committer that lands
//! in the window).
//!
//! - `resolver_vs_ingest_race`: a resolver thread looping
//!   `Resolver::run_once` races `Core::ingest_versions`'s resolution pass
//!   while the plan scan itself is slow (the `SpyKv` snapshot sleeps ~2 ms
//!   between scan items), so the resolver interleaves **inside** the plan
//!   scan. Per round: no intent leaked, every key reads back at `S`, every
//!   owner's `intent_count == 0` (I-COUNT exact; §7.3 + I-TRUNC).
//! - `commit_between_sync_and_ingest_{on,off}`: the spy's `ingest_sorted`
//!   runs a pre-hook that commits through the real commit thread and waits
//!   for its ack, so an On (phase 1) or Off (phase 2) commit strictly lands
//!   in the sync→ingest window. Pins: the ingest lands at `S`; reads at `S`
//!   see only `E@S`; a fresh snapshot sees the later commit above it; and
//!   after the Off variant `crash(0)` leaves exactly `E@S` in the range
//!   (the ingest made nothing else durable).

mod common;

use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nucleus_kv::fault::Fault;
use nucleus_kv::{Batch, Durability, GcFilter, Key, MemKv, OrderedKv, Result, Snapshot, Value};
use nucleus_txn::boot::Core;
use nucleus_txn::catalog::index_ranges;
use nucleus_txn::commit::{spawn_commit_thread, CommitThreadHandle, SyncCommit};
use nucleus_txn::encoding::{intent_key, parse_key, version_key, Entry, HEADER_LIVE};
use nucleus_txn::read::{read_key, NoSsi};
use nucleus_txn::resolver::Resolver;
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::{Ts, TxnError, TxnId};

use common::ok;

/// The index storage id whose `/i/` range the tests ingest into (a
/// hand-chosen sid: no catalog is involved), as in `t6e1_ingest.rs`.
const SID: u64 = 9;

/// How long the spy's snapshot sleeps between two scan items while the
/// `slow` flag is set — wide enough that the resolver thread interleaves
/// inside one plan scan.
const SCAN_SLEEP: Duration = Duration::from_millis(2);

fn idx_ranges() -> Vec<(Key, Key)> {
    index_ranges(SID)
}

/// A logical entry key inside the index's non-unique `/i/{sid}/` range.
fn ikey(suffix: &str) -> Key {
    let mut lo = idx_ranges().last().expect("two ranges").0.clone();
    lo.extend_from_slice(suffix.as_bytes());
    lo
}

// ---- the spying, slowing, hookable KV ----------------------------------------

/// A one-shot hook, run before the wrapped call is forwarded to the fault.
type Hook = Box<dyn FnOnce() + Send>;

/// An `OrderedKv` wrapper over `Arc<Fault<MemKv>>` (the `t6e1_ingest.rs`
/// spy, slimmed to this file's seams): a `slow` flag that makes every
/// snapshot's scans sleep between items (the resolver race), and a one-shot
/// pre-hook on `ingest_sorted` (the commit that lands in the
/// sync→ingest window). Test-local; `nucleus-kv` is untouched.
#[derive(Clone)]
struct SpyKv {
    fault: Arc<Fault<MemKv>>,
    slow: Arc<AtomicBool>,
    pre_ingest: Arc<Mutex<Option<Hook>>>,
}

impl SpyKv {
    fn new() -> SpyKv {
        SpyKv {
            fault: Arc::new(Fault::new(MemKv::new(), || Ok(MemKv::new()))),
            slow: Arc::new(AtomicBool::new(false)),
            pre_ingest: Arc::new(Mutex::new(None)),
        }
    }

    fn set_slow(&self, slow: bool) {
        self.slow.store(slow, Ordering::SeqCst);
    }

    fn set_pre_ingest(&self, hook: Hook) {
        *self.pre_ingest.lock().expect("pre_ingest slot") = Some(hook);
    }
}

impl OrderedKv for SpyKv {
    type Snap = SlowSnap;

    fn write(&self, batch: Batch, sync: Durability) -> Result<()> {
        self.fault.write(batch, sync)
    }

    fn sync_wal(&self) -> Result<()> {
        self.fault.sync_wal()
    }

    fn snapshot(&self) -> Self::Snap {
        SlowSnap {
            inner: self.fault.snapshot(),
            slow: Arc::clone(&self.slow),
        }
    }

    fn get_latest(&self, key: &[u8]) -> Result<Option<Value>> {
        self.fault.get_latest(key)
    }

    fn ingest_sorted(&self, entries: &mut dyn Iterator<Item = (Key, Value)>) -> Result<()> {
        if let Some(hook) = self.pre_ingest.lock().expect("pre_ingest slot").take() {
            hook();
        }
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

/// A snapshot whose scans sleep between items while the `slow` flag is set
/// (point reads are never slowed): the ingest's plan scan holds the view
/// open for ~2 ms per span item, which is the window the resolver thread
/// interleaves in.
struct SlowSnap {
    inner: <MemKv as OrderedKv>::Snap,
    slow: Arc<AtomicBool>,
}

impl Snapshot for SlowSnap {
    fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        self.inner.get(key)
    }

    fn scan<'a>(
        &'a self,
        range: (Bound<&[u8]>, Bound<&[u8]>),
        reverse: bool,
    ) -> Box<dyn Iterator<Item = Result<(Key, Value)>> + 'a> {
        Box::new(SlowScan {
            inner: self.inner.scan(range, reverse),
            slow: Arc::clone(&self.slow),
            yielded: false,
        })
    }
}

/// The scan iterator that sleeps between two items while `slow` is set (the
/// flag is re-read at every gap, so clearing it unslows a scan mid-flight).
struct SlowScan<'a> {
    inner: Box<dyn Iterator<Item = Result<(Key, Value)>> + 'a>,
    slow: Arc<AtomicBool>,
    yielded: bool,
}

impl Iterator for SlowScan<'_> {
    type Item = Result<(Key, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next()?;
        if self.yielded && self.slow.load(Ordering::SeqCst) {
            std::thread::sleep(SCAN_SLEEP);
        }
        self.yielded = true;
        Some(item)
    }
}

// ---- the rig -----------------------------------------------------------------

/// The `t6e1_ingest.rs` rig shape with a **spawned commit thread**: the
/// resolver races it, and the window commit runs on it (the pre-ingest hook
/// only signals — `Core::commit` submits — and waits for the ack).
struct RaceRig {
    kv: SpyKv,
    core: Arc<Core<SpyKv>>,
    commits: CommitThreadHandle,
}

impl RaceRig {
    fn new() -> RaceRig {
        let kv = SpyKv::new();
        let core = Arc::new(ok(Core::open(kv.clone())));
        let commits = ok(spawn_commit_thread(Arc::clone(&core)));
        RaceRig { kv, core, commits }
    }

    fn txn(&self) -> Txn {
        self.core.begin(Isolation::ReadCommitted)
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

/// A whole-store raw dump of the fault (bypasses the spy, so it is never
/// slowed), for leak and crash-prefix checks.
fn fault_dump(fault: &Fault<MemKv>) -> Vec<(Key, Value)> {
    let snap = fault.snapshot();
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

/// Every `@INTENT` key in a dump, for the no-leak assert.
fn intent_keys(dump: &[(Key, Value)]) -> Vec<Key> {
    dump.iter()
        .filter(|(k, _)| matches!(parse_key(k), Some((_, Entry::Intent))))
        .map(|(k, _)| k.clone())
        .collect()
}

// ---- item 1: the resolver-vs-ingest race --------------------------------------

/// Fresh intents per round, per owner (`intent_count` exactness needs more
/// than one intent racing both removers).
const M: usize = 6;

/// Rounds of: commit M fresh intents (On), take `S`, ingest them with the
/// slow flag set while a resolver thread loops `Resolver::run_once`, then
/// assert no intent leaked, every key reads back at `S`, and every owner's
/// `intent_count == 0` — against a double removal (count negative) and a
/// missed bookkeeping (count stuck at 1) alike.
#[test]
fn resolver_vs_ingest_race() {
    let rig = RaceRig::new();
    let core = Arc::clone(&rig.core);

    // The resolver thread: loops `run_once` until stopped, recording its
    // first error (fail-stop) instead of panicking inside the thread.
    let stop = Arc::new(AtomicBool::new(false));
    let resolver_err: Arc<Mutex<Option<TxnError>>> = Arc::new(Mutex::new(None));
    let resolver = {
        let core = Arc::clone(&core);
        let stop = Arc::clone(&stop);
        let err = Arc::clone(&resolver_err);
        ok(std::thread::Builder::new()
            .name("t6e1-race-resolver".into())
            .spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    if let Err(e) = Resolver::run_once(&core) {
                        let mut slot = err.lock().expect("resolver err slot");
                        if slot.is_none() {
                            *slot = Some(e);
                        }
                        return;
                    }
                }
            }))
    };

    for round in 0..24u32 {
        if let Some(e) = resolver_err.lock().expect("resolver err slot").take() {
            panic!("round {round}: the resolver failed: {e:?}");
        }
        // M fresh intents, committed On; fresh keys per round so every
        // round's span holds exactly its own M intents.
        let mut keys: Vec<Key> = Vec::with_capacity(M);
        let mut owners: Vec<TxnId> = Vec::with_capacity(M);
        let mut c_last = Ts::ZERO;
        for i in 0..M {
            let key = ikey(&format!("r{round:02}k{i}"));
            let txn = rig.txn();
            owners.push(txn.id);
            common::place_intent(
                &core,
                &txn,
                &key,
                format!("round {round} intent {i}").as_bytes(),
            );
            let c = ok(core.commit(txn, SyncCommit::On));
            assert!(c > c_last, "commit ts ascend within the round");
            c_last = c;
            keys.push(key);
        }
        let snap = core.registry.take_snapshot();
        let s = snap.ts();
        assert!(s >= c_last, "S covers every commit of the round");

        // The ingested payloads differ from the intent payloads, so a key
        // still shadowed by its intent reads back wrong at S.
        let mut entries: Vec<(Key, Vec<u8>)> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| (k.clone(), format!("round {round} entry {i}").into_bytes()))
            .collect();
        rig.kv.set_slow(true);
        let ingest = core.ingest_versions(&snap, &idx_ranges(), &mut entries.drain(..));
        rig.kv.set_slow(false);
        ok(ingest);

        if let Some(e) = resolver_err.lock().expect("resolver err slot").take() {
            panic!("round {round}: the resolver failed during the ingest: {e:?}");
        }

        // No intent leaked: the ingest's resolution pass and the async
        // resolver together removed every intent of the round, exactly once.
        let dumped = fault_dump(&rig.kv.fault);
        let leaked = intent_keys(&dumped);
        assert!(
            leaked.is_empty(),
            "round {round}: intents leaked: {leaked:?}"
        );

        // Every key reads back at S: the ingested entry, not shadowed by a
        // surviving committed intent (§4 step 1).
        for (i, key) in keys.iter().enumerate() {
            assert_eq!(
                rig.read_at(key, s),
                Some(format!("round {round} entry {i}").into_bytes()),
                "round {round}: key {key:?} reads back at S"
            );
        }

        // I-COUNT exact: a truncated entry (None) had to pass count == 0
        // first; an entry still present must sit at exactly 0 — a double
        // removal would be negative and never truncate, a missed
        // bookkeeping would stick at 1 and never truncate.
        for id in &owners {
            let entry = core.status.entry(*id);
            assert!(
                entry.is_none_or(|e| e.intent_count == 0),
                "round {round}: owner {id:?} count is {entry:?}, want 0 or truncated"
            );
        }
        drop(snap);
    }

    stop.store(true, Ordering::SeqCst);
    match resolver.join() {
        Ok(()) => {}
        Err(_) => panic!("the resolver thread panicked"),
    }
    if let Some(e) = resolver_err.lock().expect("resolver err slot").take() {
        panic!("the resolver failed: {e:?}");
    }
    ok(rig.commits.shutdown());
}

// ---- item 2: a commit between the sync and the ingest -------------------------

/// A commit strictly inside the sync→ingest window: the spy's
/// `ingest_sorted` pre-hook submits a prepared txn on the commit thread
/// and waits for its ack, so the commit lands after the ingest's step-3
/// sync and before the ingest itself. Pins: the ingest lands at `S`; reads
/// at `S` see only `E@S`; a fresh snapshot sees the later commit above it;
/// and (Off variant) `crash(0)` afterwards leaves exactly `E@S` in the
/// range — the ingest is durable, the window commit is not.
fn commit_between_sync_and_ingest(sync: SyncCommit) {
    let rig = RaceRig::new();
    let core = Arc::clone(&rig.core);

    // One filler commit so `S` is a real commit ts (and the store's epoch
    // write and the filler's batch are durable through the filler's fsync).
    let filler = rig.txn();
    common::place_intent(&core, &filler, b"/t/1/base", b"base");
    let c_fill = ok(core.commit(filler, SyncCommit::On));
    assert!(c_fill > Ts::ZERO);

    // The window txn: created up front, but its intent placement **and**
    // commit both run inside the window (the spy's `ingest_sorted`
    // pre-hook), so nothing of it is covered by the ingest's step-3 sync.
    let ent = ikey("w-entry");
    let txn = rig.txn();
    let txn_id = txn.id;

    let snap = core.registry.take_snapshot();
    let s = snap.ts();
    assert!(s >= c_fill, "the filler commit is visible at S");

    let c_window: Arc<Mutex<Option<Ts>>> = Arc::new(Mutex::new(None));
    {
        let core = Arc::clone(&core);
        let cell = Arc::clone(&c_window);
        let ent2 = ent.clone();
        rig.kv.set_pre_ingest(Box::new(move || {
            // The whole write+commit lands strictly between the ingest's
            // sync and the ingest itself: place, then signal the committer
            // (submit on the commit channel) and wait for its ack.
            common::place_intent(&core, &txn, &ent2, b"window-row");
            let c = ok(core.commit(txn, sync));
            *cell.lock().expect("window ts") = Some(c);
        }));
    }

    ok(core.ingest_versions(
        &snap,
        &idx_ranges(),
        &mut vec![(ent.clone(), b"ingested".to_vec())].into_iter(),
    ));

    let c_w = c_window
        .lock()
        .expect("window ts")
        .take()
        .expect("the window commit ran inside the hook");
    assert!(c_w > s, "the window commit is above S ({c_w:?} > {s:?})");

    // The ingest landed at S — not at the window commit's ts, not at
    // `visible_ts` (which the window commit has since advanced past S).
    assert_eq!(
        rig.raw(&version_key(&ent, s)),
        Some(live_value(b"ingested")),
        "the ingest lands at S"
    );
    assert!(
        rig.raw(&version_key(&ent, c_w)).is_none(),
        "nothing landed at the window commit's ts"
    );
    // The window txn's intent is in the KV (placed in the window, after the
    // ingest's plan scan) and the ingest left it in place.
    let raw = rig
        .raw(&intent_key(&ent))
        .expect("the window intent in place");
    assert_eq!(ok(nucleus_txn::encoding::decode_intent(&raw)).txn, txn_id);

    // Reads at S see only E@S: the window commit is above S, so §4 skips
    // its intent and reads the ingested entry.
    assert_eq!(rig.read_at(&ent, s), Some(b"ingested".to_vec()));
    // A fresh snapshot sees the later commit above it.
    let snap2 = core.registry.take_snapshot();
    let s2 = snap2.ts();
    assert!(
        s2 >= c_w,
        "the window commit is visible at a fresh snapshot"
    );
    assert_eq!(
        rig.read_at(&ent, s2),
        Some(b"window-row".to_vec()),
        "a fresh snapshot reads the window commit above E@S"
    );
    drop(snap2);
    drop(snap);

    if sync == SyncCommit::Off {
        // The ingest made nothing else durable: the Off commit's record and
        // intent are still unsynced (the ingest's sync preceded the window;
        // the ingest itself is durable but not a barrier), so crash(0)
        // leaves exactly E@S in the /i/ range.
        assert_eq!(ok(rig.kv.fault.crash(0)), 0);
        let (lo, hi) = idx_ranges().last().expect("two ranges").clone();
        let landed: Vec<Key> = fault_dump(&rig.kv.fault)
            .into_iter()
            .map(|(k, _)| k)
            .filter(|k| k.as_slice() >= lo.as_slice() && k.as_slice() < hi.as_slice())
            .collect();
        assert_eq!(
            landed,
            vec![version_key(&ent, s)],
            "after crash(0) exactly E@S is in the /i/ range"
        );
    }
    ok(rig.commits.shutdown());
}

/// Phase 1: an `synchronous_commit=on` commit in the window.
#[test]
fn commit_between_sync_and_ingest_on() {
    commit_between_sync_and_ingest(SyncCommit::On);
}

/// Phase 2: an `synchronous_commit=off` commit in the window.
#[test]
fn commit_between_sync_and_ingest_off() {
    commit_between_sync_and_ingest(SyncCommit::Off);
}
