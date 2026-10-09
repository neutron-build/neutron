//! C-T0 §9: the GC job — watermark publish (§9.1), tombstone removal,
//! retired-storage cleanup (§9.2) and the background runner. See the
//! [`gc`](crate::gc) module docs for the step ordering and the seeds each
//! rule answers.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use nucleus_kv::{Batch, Durability, Key, OrderedKv};

use crate::boot::Core;
use crate::commit::{Clock, FailStop};
use crate::encoding::{
    decode_intent, decode_version, end_key, parse_key, sys_gc_w_key, version_key, Entry,
    VersionValue, SYS_PREFIX,
};
use crate::kv_err;
use crate::removal::{remove_intent, RemovalMode};
use crate::{Ts, TxnError, TxnId, TxnStatus};

use super::filter::TxnGcFilter;

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
        core.set_kv_gc_filter(Box::new(TxnGcFilter::new(Arc::clone(&durable_w))));
        if let Err(e) = core.set_kv_gc_watermark(w.0) {
            // Undo the registration so a later install can proceed.
            installed_jobs()
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&(Arc::as_ptr(core) as usize));
            return Err(e);
        }
        Ok(GcJob {
            core: Arc::clone(core),
            config,
            durable_w,
            step: Mutex::new(()),
        })
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
        if w.0 > self.durable_w.load(Ordering::SeqCst) {
            let mut batch = Batch::default();
            batch.put(sys_gc_w_key(), w.0.to_be_bytes().to_vec());
            self.core.write(batch, Durability::Yes)?;
            // Only after the synced write: the KV's watermark, then the
            // durable slot the filter reads (§9.1; seed 42).
            self.core.set_kv_gc_watermark(w.0)?;
            self.durable_w.store(w.0, Ordering::SeqCst);
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

    /// Removes newest-`<= W` tombstones (§9.2): through one registered view,
    /// walk every data key (skipping `/sys/`); for each logical key `L`
    /// whose newest version `<= W` (the durable W) is a tombstone or
    /// moved-tombstone at `t`, write one `DeleteRange [version_key(L, t),
    /// end_key(L))` — start inclusive (seed 34). Because versions sort
    /// newest first, that range covers `L@t` and every older version of
    /// `L`; intents sort before it and are never touched. Returns the
    /// number of ranges written.
    pub fn drop_tombstones(&self) -> Result<usize, TxnError> {
        let _serial = self.lock_step();
        self.drop_tombstones_locked()
    }

    fn drop_tombstones_locked(&self) -> Result<usize, TxnError> {
        let w = self.durable_w.load(Ordering::SeqCst);
        let view = self.core.open_view();
        // The logical keys already decided (their newest version <= W seen),
        // so an older version of a decided key is skipped without a decode.
        let mut decided: Option<Key> = None;
        // Deterministic order (BTreeMap): the ranges go out in one batch.
        let mut targets: BTreeMap<Key, Ts> = BTreeMap::new();
        for entry in view.scan(
            (Bound::<&[u8]>::Unbounded, Bound::<&[u8]>::Unbounded),
            false,
        ) {
            let (stored, value) = entry.map_err(kv_err)?;
            if stored.starts_with(SYS_PREFIX) {
                continue;
            }
            let Some((l, Entry::Version(ts))) = parse_key(&stored) else {
                continue; // intents (never touched), end keys, foreign layouts
            };
            if decided.as_deref() == Some(l) {
                continue; // an older version of an already-decided key
            }
            if ts.0 > w {
                continue; // newer than W: keep scanning L's older versions
            }
            // The first version of `l` with ts <= W in ascending order is
            // its newest version <= W.
            decided = Some(l.to_vec());
            if matches!(decode_version(&value)?, VersionValue::Tombstone { .. }) {
                targets.insert(l.to_vec(), ts);
            }
        }
        drop(view);
        if targets.is_empty() {
            return Ok(0);
        }
        let mut batch = Batch::default();
        for (l, t) in &targets {
            batch.delete_range(version_key(l, *t), end_key(l));
        }
        self.core.write(batch, Durability::No)?;
        Ok(targets.len())
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
        for entry in view.scan((Bound::Included(lo), Bound::Excluded(hi)), false) {
            let (stored, value) = entry.map_err(kv_err)?;
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
        for r in &plan {
            remove_intent(&self.core, &r.key, r.prefix.as_deref(), r.owner, r.mode)?;
        }
        // Every removal returned: the prefix itself goes in one batch
        // (seed 35 — removals first, so counts and truncation stay exact).
        let mut batch = Batch::default();
        batch.delete_range(lo.to_vec(), hi.to_vec());
        self.core.write(batch, Durability::No)?;
        Ok(true)
    }

    /// One GC round: `publish(now_secs)`, then `drop_tombstones`.
    pub fn run_once(&self, now_secs: u64) -> Result<(), TxnError> {
        let _serial = self.lock_step();
        self.publish_locked(now_secs)?;
        self.drop_tombstones_locked()?;
        Ok(())
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

/// The stop signal of a spawned GC thread: a flag plus a condvar so
/// [`GcHandle::stop`] joins promptly even with a long interval.
struct GcStop {
    stop: AtomicBool,
    pair: (Mutex<()>, Condvar),
}

impl GcStop {
    fn new() -> Arc<GcStop> {
        Arc::new(GcStop {
            stop: AtomicBool::new(false),
            pair: (Mutex::new(()), Condvar::new()),
        })
    }

    /// Signals the thread to stop after its current round and wakes a
    /// sleeping round promptly.
    fn signal(&self) {
        self.stop.store(true, Ordering::SeqCst);
        let _woken = self.pair.0.lock().unwrap_or_else(PoisonError::into_inner);
        self.pair.1.notify_all();
    }

    /// Sleeps up to `interval`, waking early on [`GcStop::signal`]. Returns
    /// `true` when stopping was signalled.
    fn sleep_or_stop(&self, interval: Duration) -> bool {
        let deadline = Instant::now() + interval;
        let mut g = self.pair.0.lock().unwrap_or_else(PoisonError::into_inner);
        while !self.stop.load(Ordering::SeqCst) {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let (g2, timeout) = self
                .pair
                .1
                .wait_timeout(g, deadline - now)
                .unwrap_or_else(PoisonError::into_inner);
            g = g2;
            if timeout.timed_out() && Instant::now() >= deadline {
                return self.stop.load(Ordering::SeqCst);
            }
        }
        true
    }
}

/// Runs [`GcJob::run_once`] in a background thread every `interval`, taking
/// `now` from `clock`. On an error the thread reports it through
/// `fail_stop`, stores it as its last result and stops (the caller treats a
/// KV error as fatal, §3); [`GcHandle::stop`] joins the thread and returns
/// that last result. A thread-spawn failure and a thread panic are reported
/// through `fail_stop` as well.
pub fn spawn_gc<K: OrderedKv>(
    job: GcJob<K>,
    interval: Duration,
    clock: Arc<dyn Clock>,
    fail_stop: Arc<dyn FailStop>,
) -> GcHandle {
    let stop = GcStop::new();
    let last: Arc<Mutex<Option<Result<(), TxnError>>>> = Arc::new(Mutex::new(None));
    let thread = {
        let stop = Arc::clone(&stop);
        let last = Arc::clone(&last);
        let fail_stop = Arc::clone(&fail_stop);
        std::thread::Builder::new()
            .name("nucleus-gc".into())
            .spawn(move || loop {
                if stop.stop.load(Ordering::SeqCst) {
                    return;
                }
                match job.run_once(clock.now_secs()) {
                    Ok(()) => {}
                    Err(e) => {
                        // Store first: the fail-stop hook may abort the
                        // process (its default), and the last result must
                        // still be observable.
                        *last.lock().unwrap_or_else(PoisonError::into_inner) = Some(Err(e.clone()));
                        fail_stop.on_kv_error(&e);
                        return;
                    }
                }
                if stop.sleep_or_stop(interval) {
                    return;
                }
            })
    };
    match thread {
        Ok(t) => GcHandle {
            stop,
            last,
            thread: Some(t),
            fail_stop,
        },
        Err(e) => {
            let err = TxnError::Invariant(format!("failed to spawn the gc thread: {e}"));
            fail_stop.on_kv_error(&err);
            GcHandle {
                stop,
                last: Arc::new(Mutex::new(Some(Err(err)))),
                thread: None,
                fail_stop,
            }
        }
    }
}

/// Handle to a spawned GC thread ([`spawn_gc`]).
pub struct GcHandle {
    stop: Arc<GcStop>,
    last: Arc<Mutex<Option<Result<(), TxnError>>>>,
    thread: Option<std::thread::JoinHandle<()>>,
    /// The hook a panicked thread is reported through when the handle is
    /// dropped without [`GcHandle::stop`] (there is no caller to return
    /// the error to).
    fail_stop: Arc<dyn FailStop>,
}

impl GcHandle {
    /// Signals the loop to stop, joins the thread and returns its last
    /// round's result (the first error it hit, if any).
    pub fn stop(mut self) -> Result<(), TxnError> {
        self.stop.signal();
        if let Some(t) = self.thread.take() {
            t.join()
                .map_err(|_| TxnError::Invariant("gc thread panicked".into()))?;
        }
        match self
            .last
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            Some(Err(e)) => Err(e),
            Some(Ok(())) | None => Ok(()),
        }
    }
}

impl Drop for GcHandle {
    fn drop(&mut self) {
        // Not stopping explicitly would park the thread for its interval
        // forever; stop it even when the handle is dropped unused. A panic
        // in the thread is reported through the fail-stop hook — there is
        // no caller left to return it to (Rework 6).
        self.stop.signal();
        if let Some(t) = self.thread.take() {
            match t.join() {
                Ok(()) => {}
                Err(_) => self.fail_stop.on_kv_error(&TxnError::Invariant(
                    "gc thread panicked (observed at handle drop)".into(),
                )),
            }
        }
    }
}
