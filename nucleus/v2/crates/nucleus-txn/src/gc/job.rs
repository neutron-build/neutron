//! C-T0 §9: the GC job — watermark publish (§9.1), tombstone removal,
//! retired-storage cleanup (§9.2) and the background runner. See the
//! [`gc`](crate::gc) module docs for the step ordering and the seeds each
//! rule answers.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Duration;

use nucleus_kv::{Batch, Durability, Key, OrderedKv};

use crate::boot::Core;
use crate::commit::{Clock, FailStop};
use crate::encoding::{
    decode_intent, decode_version, end_key, intent_key, parse_key, sys_gc_w_key, version_key,
    Entry, VersionValue, SYS_PREFIX,
};
use crate::jobs::{Job, JobConfig, JobCtx, JobKind, JobOutcome, JobPoint, JobSet};
use crate::kv_err;
use crate::removal::{remove_intent, RemovalMode};
use crate::{Ts, TxnError, TxnId, TxnStatus};

use super::filter::TxnGcFilter;
use super::gc_scope;

/// Stored entries one [`GcRunner`] tombstone-chunk scans per step (the
/// §9.3 bounded steps; tests drive smaller chunks directly through
/// [`GcJob::drop_tombstones_chunk`]).
pub const DEFAULT_TOMBSTONE_CHUNK: usize = 1024;

/// Job configuration: only the AS OF retention window so far (§9.1).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GcConfig {
    /// The AS OF retention window in wall seconds: with it, `publish` floors
    /// `W` at the largest `/sys/ts_clock`-sampled ts whose wall time is at
    /// or before `now - window` (0 when no sample qualifies). Without it
    /// there is no extra floor.
    pub as_of_window_secs: Option<u64>,
}

/// The §9 GC job. Created by [`GcJob::install`] (after [`Core::open`],
/// before the first [`GcJob::publish`]); one internal mutex serialises the
/// job's steps, so two publishes — or a publish and a tombstone pass —
/// never interleave. At most one job is live per core (see `install`).
pub struct GcJob<K: OrderedKv> {
    core: Arc<Core<K>>,
    config: GcConfig,
    /// The **durable** W (§9.1): what `/sys/gc_w` holds, synced. Shared with
    /// the compaction filter, which reads it once per stream — never a W
    /// that is not durable yet (seed 42).
    durable_w: Arc<AtomicU64>,
    /// Serialises the job's steps (§9.1: one publisher).
    step: Mutex<()>,
}

impl<K: OrderedKv> GcJob<K> {
    /// Installs the job. The durable W comes from `/sys/gc_w` itself (read
    /// through a registered view), **not** `registry.published_w()`: only
    /// `/sys/gc_w` is known-synced, and a `publish_computed_w` run directly
    /// on the registry (§9.1 lets it happen outside the job) must not hand
    /// the filter or the KV a W that no GC step may act on yet. The filter
    /// goes to the KV with that W, and the KV's watermark is set to it.
    /// Must run after `Core::open` and before the first `publish`.
    ///
    /// At most one job may be live per core: a second `install` while one
    /// is alive is an error (two publishers would race `/sys/gc_w`). The
    /// slot is released when the job drops, so a reopen-style reinstall is
    /// allowed once the previous job is gone. (`std::mem::forget`ting a
    /// job leaks its slot, like any other resource.)
    pub fn install(core: &Arc<Core<K>>, config: GcConfig) -> Result<GcJob<K>, TxnError> {
        let w = durable_gc_w(core)?;
        {
            let mut installed = installed_jobs()
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if !installed.insert(Arc::as_ptr(core) as usize) {
                return Err(TxnError::Invariant(
                    "a GC job is already installed on this core (C-T0 §9.1: one publisher)".into(),
                ));
            }
        }
        let durable_w = Arc::new(AtomicU64::new(w.0));
        if let Err(e) = core.set_kv_gc_watermark(w.0) {
            // Undo the registration so a later install can proceed.
            installed_jobs()
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&(Arc::as_ptr(core) as usize));
            return Err(e);
        }
        // Filter after the watermark, so a failed install leaves no filter.
        core.set_kv_gc_filter(Box::new(TxnGcFilter::new(Arc::clone(&durable_w))));
        Ok(GcJob {
            core: Arc::clone(core),
            config,
            durable_w,
            step: Mutex::new(()),
        })
    }

    /// The **durable** W (§9.1): what `/sys/gc_w` holds, synced — the
    /// value the filter and every GC step act on, and the gate
    /// [`GcJob::retire_storage`] and the drop-cleanup job (§9.2) compare
    /// against. Public since C-B1b: the cleanup job asks it when a
    /// retired row's snapshot has been passed.
    pub fn durable_w(&self) -> Ts {
        Ts(self.durable_w.load(Ordering::SeqCst))
    }

    /// Computes and publishes `W` (§9.1), in this order (seed 42):
    ///
    /// 1. With a retention window, the `/sys/ts_clock` floor: the largest
    ///    sampled ts whose wall time is `<= now_secs - window` (saturating),
    ///    read through a registered view; `Ts::ZERO` when no sample
    ///    qualifies. Without a window, no floor.
    /// 2. `W = registry.publish_computed_w(floor)` — computed and published
    ///    as `max(old, computed)` in one registry critical section (seed
    ///    21's atomicity; seed 22's monotonicity; `register_at` checks the
    ///    published value, seed 31).
    /// 3. Only if `W` rose above the durable W: write `/sys/gc_w = W` with
    ///    `Durability::Yes`, then hand `W` to the KV's watermark, then store
    ///    it into the durable slot the filter reads. On any error, stop and
    ///    return it with the durable W unchanged.
    pub fn publish(&self, now_secs: u64) -> Result<Ts, TxnError> {
        let _serial = self.lock_step();
        self.publish_locked(now_secs)
    }

    fn publish_locked(&self, now_secs: u64) -> Result<Ts, TxnError> {
        let extra_floor = match self.config.as_of_window_secs {
            Some(window) => Some(self.ts_clock_floor(now_secs.saturating_sub(window))?),
            None => None,
        };
        let w = self.core.registry.publish_computed_w(extra_floor);
        // §9.3 probe: the registry critical section is over (it ended
        // inside `publish_computed_w`); only this job's step serialiser
        // may be held here.
        self.core.job_probe_at(JobPoint::GcAfterPublish);
        if w.0 > self.durable_w.load(Ordering::SeqCst) {
            let mut batch = Batch::default();
            batch.put(sys_gc_w_key(), w.0.to_be_bytes().to_vec());
            self.core.write(batch, Durability::Yes)?;
            // §9.3 probe: right after the synced /sys/gc_w write, before
            // the watermark reaches the KV.
            self.core.job_probe_at(JobPoint::GcAfterWatermarkSync);
            // Only after the synced write: the KV's watermark, then the
            // durable slot the filter reads (§9.1; seed 42).
            self.core.set_kv_gc_watermark(w.0)?;
            self.durable_w.store(w.0, Ordering::SeqCst);
            // §9.3 (C-B1b): the durable W rose — a retired row's gate may
            // have opened. Wake the drop-cleanup job (harmless when none
            // runs; its idle interval is only a backstop).
            self.core.wake_job(JobKind::DropCleanup);
        }
        Ok(w)
    }

    /// The `/sys/ts_clock` floor (§9.1): the largest sampled ts whose wall
    /// time is `<= cutoff`, read through a registered view. A clock that
    /// goes backwards can only lower this. `Ts::ZERO` when no sample
    /// qualifies.
    fn ts_clock_floor(&self, cutoff: u64) -> Result<Ts, TxnError> {
        let lo = ts_clock_prefix();
        let hi = ts_clock_prefix_end();
        let view = self.core.open_view();
        let mut floor = Ts::ZERO;
        for entry in view.scan(
            (
                Bound::Included(lo.as_slice()),
                Bound::Excluded(hi.as_slice()),
            ),
            false,
        ) {
            let (key, value) = entry.map_err(kv_err)?;
            let ts = parse_ts_clock_key(&key)?;
            let wall = decode_secs(&value)?;
            if wall <= cutoff && ts > floor {
                floor = ts;
            }
        }
        Ok(floor)
    }

    /// Removes newest-`<= W` tombstones (§9.2), chunked (C-B1c): this is
    /// the loop to the end of the keyspace — [`GcJob::drop_tombstones_chunk`]
    /// does one chunk at a time. Returns the number of ranges written.
    pub fn drop_tombstones(&self) -> Result<usize, TxnError> {
        let _serial = self.lock_step();
        let mut total = 0usize;
        let mut resume: Option<Key> = None;
        loop {
            let (n, next) = self.drop_tombstones_chunk_locked(resume, DEFAULT_TOMBSTONE_CHUNK)?;
            total += n;
            match next {
                Some(k) => resume = Some(k),
                None => return Ok(total),
            }
        }
    }

    /// One chunk of the §9.2 tombstone pass (C-B1c): through **one**
    /// registered view, walk at most `max_entries` stored entries from
    /// `resume` (`None` starts at the beginning); for each logical key `L`
    /// in scope ([`gc_scope`]: data keys plus `/sys/catalog/`) whose
    /// newest version `<= W` (the durable W, read once per chunk) is a
    /// tombstone or moved-tombstone at `t`, write one
    /// `DeleteRange [version_key(L, t), end_key(L))` — start inclusive
    /// (seed 34). Because versions sort newest first, that range covers
    /// `L@t` and every older version of `L`; intents sort before it and
    /// are never touched.
    ///
    /// A chunk ends only at a **logical-key boundary**: if the entry
    /// budget runs out mid-key, the walk continues through that key's
    /// remaining versions, and the returned resume key is the next
    /// logical key's `intent_key` — so no chunk ever re-decides a key
    /// another chunk already handled, and no key straddles a chunk
    /// undecided. Returns `(ranges written, the next chunk's resume)`;
    /// `None` means the pass reached the end of the keyspace.
    ///
    /// Charges the §9.3 I/O budget one unit per stored entry scanned and
    /// one per `DeleteRange` written.
    pub fn drop_tombstones_chunk(
        &self,
        resume: Option<Key>,
        max_entries: usize,
    ) -> Result<(usize, Option<Key>), TxnError> {
        let _serial = self.lock_step();
        self.drop_tombstones_chunk_locked(resume, max_entries)
    }

    fn drop_tombstones_chunk_locked(
        &self,
        resume: Option<Key>,
        max_entries: usize,
    ) -> Result<(usize, Option<Key>), TxnError> {
        // The durable W, read once per chunk (§9.1).
        let w = self.durable_w.load(Ordering::SeqCst);
        let view = self.core.open_view();
        let lo = match &resume {
            Some(k) => Bound::Excluded(k.as_slice()),
            None => Bound::Unbounded,
        };
        // The logical keys already decided (their newest version <= W seen),
        // so an older version of a decided key is skipped without a decode.
        let mut decided: Option<Key> = None;
        // Deterministic order (BTreeMap): the ranges go out in one batch.
        let mut targets: BTreeMap<Key, Ts> = BTreeMap::new();
        let mut scanned: u64 = 0;
        // Set once `max_entries` stored entries have been consumed: the
        // walk then stops at the next logical-key boundary (for a
        // version entry, a different logical key; anything else is its
        // own boundary), recording where the next chunk resumes.
        let mut at_limit = false;
        let mut limit_key: Option<Key> = None;
        let mut next_resume: Option<Key> = None;
        for entry in view.scan((lo, Bound::<&[u8]>::Unbounded), false) {
            let (stored, value) = entry.map_err(kv_err)?;
            let parsed = if gc_scope(&stored) {
                parse_key(&stored)
            } else {
                None // a kept /sys/ key: its own boundary, never decoded
            };
            if at_limit {
                // Is this entry still inside the logical key the limit
                // fell into? Only same-key versions continue the walk.
                let same_key = match (&limit_key, &parsed) {
                    (Some(l), Some((l2, Entry::Version(_)))) => l == l2,
                    _ => false,
                };
                if !same_key {
                    next_resume = Some(match parsed {
                        Some((l2, _)) => intent_key(l2),
                        None => stored.clone(),
                    });
                    break;
                }
            }
            scanned += 1;
            if let Some((l, entry_kind)) = parsed {
                match entry_kind {
                    // Intents are never touched by the pass (§9.2; only
                    // §7.3 removes them): skipped like a foreign entry,
                    // and their own boundary.
                    Entry::Intent => {}
                    Entry::Version(ts) => {
                        if decided.as_deref() == Some(l) {
                            // an older version of an already-decided key
                        } else if ts.0 > w {
                            // newer than W: keep scanning L's older versions
                        } else {
                            // The first version of `l` with ts <= W in
                            // ascending order is its newest version <= W.
                            decided = Some(l.to_vec());
                            if matches!(decode_version(&value)?, VersionValue::Tombstone { .. }) {
                                targets.insert(l.to_vec(), ts);
                            }
                        }
                    }
                }
            }
            if scanned >= max_entries as u64 {
                at_limit = true;
                // A version entry might have older versions following:
                // remember which key we are inside so the boundary test
                // can continue through them. Any other entry (intent,
                // kept /sys/, foreign layout) is its own boundary.
                limit_key = match parsed {
                    Some((l, Entry::Version(_))) => Some(l.to_vec()),
                    _ => None,
                };
            }
        }
        drop(view);
        self.core.job_units(JobKind::Gc, scanned);
        if targets.is_empty() {
            return Ok((0, next_resume));
        }
        let mut batch = Batch::default();
        for (l, t) in &targets {
            batch.delete_range(version_key(l, *t), end_key(l));
        }
        self.core.write(batch, Durability::No)?;
        // One unit per DeleteRange written, on top of the scan charge.
        self.core.job_units(JobKind::Gc, targets.len() as u64);
        // §9.3 probe: after the chunk's batch write (the registered view
        // is closed; only the step serialiser may be held).
        self.core.job_probe_at(JobPoint::GcAfterTombstones);
        let n = targets.len();
        if n > 0 {
            self.core.job_items(JobKind::Gc, n as u64);
        }
        Ok((n, next_resume))
    }

    /// §9.2 retired-storage cleanup. Returns `Ok(false)` and does nothing
    /// unless the durable `W > retired_at` (the retiring DDL's commit ts).
    /// Otherwise: through one registered view, find every intent key in
    /// `[lo, hi)`; remove each with §7.3's `remove_intent`, using
    /// `latch_prefix(key)` as the deferrable prefix (the caller knows which
    /// indexes are deferrable) — mode `Resolve` when the owner is a visible
    /// commit, `Discard` when it is Aborted (an older-epoch owner without a
    /// record is Aborted, §7.2). An owner that is Pending or
    /// committed-not-visible is an invariant error (AccessExclusive excludes
    /// it). Only after every removal returned: one batch
    /// `DeleteRange(lo, hi)` (seed 35). Returns `Ok(true)`.
    pub fn retire_storage(
        &self,
        lo: &[u8],
        hi: &[u8],
        retired_at: Ts,
        latch_prefix: &dyn Fn(&[u8]) -> Option<usize>,
    ) -> Result<bool, TxnError> {
        let _serial = self.lock_step();
        self.retire_locked(lo, hi, retired_at, latch_prefix)
    }

    fn retire_locked(
        &self,
        lo: &[u8],
        hi: &[u8],
        retired_at: Ts,
        latch_prefix: &dyn Fn(&[u8]) -> Option<usize>,
    ) -> Result<bool, TxnError> {
        // The gate: retired prefixes are deleted only once W passes the
        // retiring DDL (§9.2), so AS OF reads at t >= W still find the
        // storage the catalog at t names.
        if Ts(self.durable_w.load(Ordering::SeqCst)) <= retired_at {
            return Ok(false);
        }
        // The removal plan, through one registered view (§3.1). The owner's
        // status is looked up **inside the scan, while the view is open**:
        // I-TRUNC guarantees the entry as long as the view can see the
        // intent, so the plan cannot hit the window where the async
        // resolver removes the intent and truncates the owner between the
        // scan and a lookup done after the view is gone — a spurious
        // `Invariant`, fatal under `spawn_gc`. `remove_intent` re-reads
        // under the latch and no-ops if the intent is already gone, so a
        // resolver that wins the race after the plan is simply tolerated.
        struct PlannedRemoval {
            key: Key,
            /// The deferrable prefix (§5.0), resolved under the view.
            prefix: Option<Key>,
            owner: TxnId,
            /// The status decision made under the view: `Resolve` for a
            /// visible commit, `Discard` for an Aborted owner.
            mode: RemovalMode,
        }
        let view = self.core.open_view();
        let mut plan: Vec<PlannedRemoval> = Vec::new();
        let mut scanned: u64 = 0;
        for entry in view.scan((Bound::Included(lo), Bound::Excluded(hi)), false) {
            let (stored, value) = entry.map_err(kv_err)?;
            scanned += 1;
            let Some((l, Entry::Intent)) = parse_key(&stored) else {
                continue;
            };
            let intent = decode_intent(&value)?;
            let owner = intent.txn;
            let mode = match self.core.status.lookup_for_intent(owner)? {
                TxnStatus::Committed(c) if c <= self.core.visible_ts() => RemovalMode::Resolve,
                TxnStatus::Aborted => RemovalMode::Discard,
                other => {
                    return Err(TxnError::Invariant(format!(
                        "retire: intent of {owner:?} on {l:?} is {other:?}; \
                         AccessExclusive excludes it (C-T0 §9.2)"
                    )));
                }
            };
            let prefix = match latch_prefix(l) {
                Some(len) => match l.get(..len) {
                    Some(p) => Some(p.to_vec()),
                    None => {
                        return Err(TxnError::Invariant(format!(
                            "latch_prefix returned {len} for key {l:?} of length {}",
                            l.len()
                        )));
                    }
                },
                None => None,
            };
            plan.push(PlannedRemoval {
                key: l.to_vec(),
                prefix,
                owner,
                mode,
            });
        }
        drop(view);
        // §9.3 I/O budget: one unit per stored entry scanned (the intent
        // plan) and one per planned removal (each writes a batch keyed on
        // its intent).
        self.core
            .job_units(JobKind::Gc, scanned + plan.len() as u64);
        for r in &plan {
            remove_intent(&self.core, &r.key, r.prefix.as_deref(), r.owner, r.mode)?;
        }
        // Every removal returned: the prefix itself goes in one batch
        // (seed 35 — removals first, so counts and truncation stay exact).
        let mut batch = Batch::default();
        batch.delete_range(lo.to_vec(), hi.to_vec());
        self.core.write(batch, Durability::No)?;
        // One unit per DeleteRange (§9.3 charge sites).
        self.core.job_units(JobKind::Gc, 1);
        // §9.3 probe (C-B1b): the range's DeleteRange batch is written
        // (the registered view is closed; only this job's step serialiser
        // may be held).
        self.core.job_probe_at(JobPoint::RetireAfterRange);
        Ok(true)
    }

    /// One GC round: `publish(now_secs)`, then `drop_tombstones` — the
    /// whole tombstone pass, chunk by chunk, in one call (the recovery
    /// harnesses' driver; the runner paces the pass one chunk per step).
    pub fn run_once(&self, now_secs: u64) -> Result<(), TxnError> {
        let _serial = self.lock_step();
        self.publish_locked(now_secs)?;
        let mut resume: Option<Key> = None;
        loop {
            let (_, next) = self.drop_tombstones_chunk_locked(resume, DEFAULT_TOMBSTONE_CHUNK)?;
            match next {
                Some(k) => resume = Some(k),
                None => return Ok(()),
            }
        }
    }

    fn lock_step(&self) -> MutexGuard<'_, ()> {
        self.step.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Cores with a live installed job ([`GcJob::install`] refuses a second one
/// while the first is alive). Keyed by the `Arc<Core>` address: an entry
/// exists only while its job (which holds an `Arc` to the core) is alive, so
/// an address is never mistaken for installed once its job is gone; `Drop`
/// for `GcJob` removes the entry. Entry removal happens in `Drop`'s body,
/// before the `Arc` inside the job can be released, so a core freed after
/// its job cannot alias a live entry.
fn installed_jobs() -> &'static Mutex<BTreeSet<usize>> {
    static INSTALLED: OnceLock<Mutex<BTreeSet<usize>>> = OnceLock::new();
    INSTALLED.get_or_init(|| Mutex::new(BTreeSet::new()))
}

/// The durable W as `/sys/gc_w` holds it, read through a registered view
/// (§3.1). Missing key → 0 (a fresh store). This mirrors boot's private
/// `read_ts`; `boot.rs` is off-limits to this card beyond the KV
/// pass-throughs, so the few lines are repeated here.
fn durable_gc_w<K: OrderedKv>(core: &Core<K>) -> Result<Ts, TxnError> {
    let view = core.open_view();
    match view.get(&sys_gc_w_key()).map_err(kv_err)? {
        Some(v) if v.len() == 8 => {
            let mut b = [0u8; 8];
            b.copy_from_slice(&v);
            Ok(Ts(u64::from_be_bytes(b)))
        }
        Some(v) => Err(TxnError::Corrupt(format!(
            "/sys/gc_w value of {} bytes, expected 8",
            v.len()
        ))),
        None => Ok(Ts::ZERO),
    }
}

impl<K: OrderedKv> Drop for GcJob<K> {
    fn drop(&mut self) {
        installed_jobs()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&(Arc::as_ptr(&self.core) as usize));
    }
}

/// `/sys/ts_clock/`: the sample prefix (samples are keyed by ts, §2.3).
fn ts_clock_prefix() -> Key {
    let mut k = SYS_PREFIX.to_vec();
    k.extend_from_slice(b"ts_clock/");
    k
}

/// Exclusive upper bound of the sample prefix, mirroring
/// [`crate::encoding::sys_txn_prefix_end`]'s construction: `/sys/ts_clock0`.
fn ts_clock_prefix_end() -> Key {
    let mut k = ts_clock_prefix();
    let last = k.len() - 1;
    k[last] = k[last].saturating_add(1);
    k
}

/// Splits a `/sys/ts_clock/{ts}` key (the mirror of
/// [`sys_ts_clock_key`]).
fn parse_ts_clock_key(key: &[u8]) -> Result<Ts, TxnError> {
    let tail = key
        .strip_prefix(ts_clock_prefix().as_slice())
        .ok_or_else(|| {
            TxnError::Corrupt(format!("key {key:?} outside the /sys/ts_clock/ prefix"))
        })?;
    if tail.len() != 8 {
        return Err(TxnError::Corrupt(format!(
            "/sys/ts_clock key tail of {} bytes, expected 8",
            tail.len()
        )));
    }
    let mut b = [0u8; 8];
    b.copy_from_slice(tail);
    Ok(Ts(u64::from_be_bytes(b)))
}

/// A `/sys/ts_clock` sample value: the wall time in seconds.
fn decode_secs(value: &[u8]) -> Result<u64, TxnError> {
    if value.len() != 8 {
        return Err(TxnError::Corrupt(format!(
            "/sys/ts_clock sample of {} bytes, expected 8",
            value.len()
        )));
    }
    let mut b = [0u8; 8];
    b.copy_from_slice(value);
    Ok(u64::from_be_bytes(b))
}

/// The GC job as a §9.3 background job (C-B1c bounded steps): one `step`
/// publishes once per full pass (the first step of each pass, so `/sys/gc_w`
/// leads every pass) and then walks **one chunk**
/// ([`DEFAULT_TOMBSTONE_CHUNK`] stored entries) of the §9.2 tombstone pass,
/// holding the resume key between steps — `Progress` until the pass
/// reaches the end of the keyspace, then `Idle` (the runner paces it with
/// the idle interval, as the old interval sleep did).
pub struct GcRunner<K: OrderedKv> {
    job: Arc<GcJob<K>>,
    clock: Arc<dyn Clock>,
    /// The pass's chunk cursor: `None` before a pass starts (the next
    /// step publishes first), `Some(resume)` between chunks.
    resume: Option<Key>,
    /// Stored entries one tombstone step scans.
    chunk: usize,
}

impl<K: OrderedKv> GcRunner<K> {
    pub fn new(job: Arc<GcJob<K>>, clock: Arc<dyn Clock>) -> GcRunner<K> {
        GcRunner {
            job,
            clock,
            resume: None,
            chunk: DEFAULT_TOMBSTONE_CHUNK,
        }
    }

    /// Scans at most `chunk` stored entries per tombstone step (tests).
    pub fn with_chunk(mut self, chunk: usize) -> GcRunner<K> {
        self.chunk = chunk.max(1);
        self
    }
}

impl<K: OrderedKv> Job for GcRunner<K> {
    fn kind(&self) -> JobKind {
        JobKind::Gc
    }

    fn step(&mut self, _cx: &JobCtx) -> Result<JobOutcome, TxnError> {
        if self.resume.is_none() {
            self.job.publish(self.clock.now_secs())?;
        }
        let (_, next) = self
            .job
            .drop_tombstones_chunk(self.resume.take(), self.chunk)?;
        match next {
            Some(k) => {
                self.resume = Some(k);
                Ok(JobOutcome::Progress)
            }
            None => Ok(JobOutcome::Idle),
        }
    }
}

/// Runs the [`GcRunner`] in a background thread (§9.3) every
/// `idle_interval` (the old `interval`), taking `now` from `clock`. On an
/// error the thread reports it through `fail_stop` (the old hook
/// parameter: `with_fail_stop`), stops every job of its set and stops
/// (the caller treats a KV error as fatal, §3); [`GcHandle::stop`] joins
/// the thread and returns that first error. A thread-spawn failure and a
/// thread panic are reported through `fail_stop` as well.
pub fn spawn_gc<K: OrderedKv>(
    job: GcJob<K>,
    interval: Duration,
    clock: Arc<dyn Clock>,
    fail_stop: Arc<dyn FailStop>,
) -> GcHandle {
    let core = Arc::clone(&job.core);
    let mut set = JobSet::new(core).with_fail_stop(Arc::clone(&fail_stop));
    let runner = GcRunner::new(Arc::new(job), clock);
    // A spawn failure is recorded by `start` and reported to the very
    // hook passed above before the error returns; the handle below then
    // carries it out of `stop`, exactly as the pre-C-B1a wrapper did.
    // The config keeps D4's GC budget and takes only the interval from
    // the caller.
    let _spawn_failed = set.start(
        Box::new(runner),
        JobConfig {
            idle_interval: interval,
            ..JobConfig::default_for(JobKind::Gc)
        },
    );
    GcHandle {
        shutdown: Some(Box::new(move || set.shutdown())),
    }
}

/// Handle to a spawned GC thread ([`spawn_gc`]): a one-job [`JobSet`]
/// behind an erasing shutdown closure (the handle stays non-generic, as
/// before C-B1a). `stop` (and `Drop`) run the set's ordered shutdown and
/// return the job's first error, if any.
pub struct GcHandle {
    shutdown: Option<Box<dyn FnOnce() -> Result<(), TxnError> + Send>>,
}

impl GcHandle {
    /// Signals the loop to stop, joins the thread and returns its first
    /// error, if any.
    pub fn stop(mut self) -> Result<(), TxnError> {
        match self.shutdown.take() {
            Some(shutdown) => shutdown(),
            None => Ok(()),
        }
    }
}

impl Drop for GcHandle {
    fn drop(&mut self) {
        // Not stopping explicitly would park the thread for its idle
        // interval forever; stop it even when the handle is dropped
        // unused. A panic observed at the shutdown join is recorded and
        // reported through the fail-stop hook inside the shutdown — there
        // is no caller left to return it to (Rework 6).
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown();
        }
    }
}
