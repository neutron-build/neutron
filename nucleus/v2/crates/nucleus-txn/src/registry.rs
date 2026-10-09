//! C-T0 §3.1/§9.1: the snapshot and view registry. One mutex guards every
//! live snapshot `S`, every open view's `(counter, vts)`, `view_counter`,
//! the published GC watermark `W` and `sweep_counter`.
//!
//! - Taking a snapshot reads `S = visible_ts` and registers it in one
//!   critical section (I-SNAP-ORDER). `visible_ts` itself is a shared atomic
//!   advanced without this mutex by the commit thread; loading it under the
//!   mutex is what makes the read+insert atomic against every `W` computation
//!   and SSI retirement, which all take the same mutex.
//! - Opening a view registers `c = ++view_counter` and `vts = visible_ts`
//!   under the mutex, releases it, and only then opens the KV snapshot —
//!   never another order.
//! - `W` is computed and published in one critical section (§9.1), which is
//!   what `register_at`'s `t >= W` check depends on.
//!
//! Guards unregister on drop. There is no public way to open a KV snapshot
//! except through `open_view`, so I-SNAP-ORDER holds by construction.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use nucleus_kv::{Key, OrderedKv, Snapshot, Value};

use crate::{Ts, TxnError};

/// The registry state, exposed under the mutex by [`Registry::with_registry`].
/// Fields are private; act through the methods so every caller sees the same
/// invariants.
pub struct RegistryState {
    snapshots: BTreeMap<Ts, u64>,
    views: BTreeMap<u64, Ts>,
    view_counter: u64,
    w: Ts,
    sweep_counter: Option<u64>,
    visible_ts: Arc<AtomicU64>,
}

impl RegistryState {
    /// The current view counter (the last handed out; 0 before any view).
    pub fn view_counter(&self) -> u64 {
        self.view_counter
    }

    /// The smallest registered open-view counter, `u64::MAX` when no view is
    /// open (§7.4 condition 2).
    pub fn min_view_counter(&self) -> u64 {
        self.views.keys().next().copied().unwrap_or(u64::MAX)
    }

    /// The sweep counter (§7.4): `Some(view_counter)` once the boot sweep
    /// has finished, `None` before.
    pub fn sweep_counter(&self) -> Option<u64> {
        self.sweep_counter
    }

    /// The published watermark `W` (§9.1).
    pub fn published_w(&self) -> Ts {
        self.w
    }

    /// The smallest registered snapshot ts, if any (§8.6 reads this under the
    /// registry mutex).
    pub fn min_snapshot_ts(&self) -> Option<Ts> {
        self.snapshots.keys().next().copied()
    }

    /// The §9.1 min: registered snapshots and caller-chosen ts, every open
    /// view's `vts`, `visible_ts`, and `extra_floor` (an AS OF retention
    /// window converted to ts). Read-only.
    pub fn computed_watermark(&self, extra_floor: Option<Ts>) -> Ts {
        let mut min = Ts(self.visible_ts.load(Ordering::SeqCst));
        if let Some(f) = extra_floor {
            min = min.min(f);
        }
        for &ts in self.snapshots.keys() {
            min = min.min(ts);
        }
        for &vts in self.views.values() {
            min = min.min(vts);
        }
        min
    }

    /// Computes the §9.1 min and publishes `W = max(old W, computed)` in one
    /// step (§9.1), inside the caller's registry critical section. There is
    /// no other publisher: `W` cannot be set from a value computed in an
    /// earlier critical section.
    pub fn publish_computed_w(&mut self, extra_floor: Option<Ts>) -> Ts {
        let computed = self.computed_watermark(extra_floor);
        self.w = self.w.max(computed);
        self.w
    }
}

/// The registry (§3.1). Cheap to share through `&Core`.
pub struct Registry {
    st: Mutex<RegistryState>,
    visible_ts: Arc<AtomicU64>,
}

impl Registry {
    pub(crate) fn new(visible_ts: Arc<AtomicU64>, w: Ts) -> Self {
        Registry {
            st: Mutex::new(RegistryState {
                snapshots: BTreeMap::new(),
                views: BTreeMap::new(),
                view_counter: 0,
                w,
                sweep_counter: None,
                visible_ts: visible_ts.clone(),
            }),
            visible_ts,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, RegistryState> {
        self.st.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// `visible_ts` (§3): largest ts such that every commit `<= ts` is
    /// applied. Monotonic; advanced by the commit thread.
    pub fn visible_ts(&self) -> Ts {
        Ts(self.visible_ts.load(Ordering::SeqCst))
    }

    /// Takes a snapshot: reads `S = visible_ts` and registers it in one
    /// critical section (§3.1).
    pub fn take_snapshot(&self) -> SnapshotGuard<'_> {
        let mut st = self.lock();
        let s = Ts(self.visible_ts.load(Ordering::SeqCst));
        *st.snapshots.entry(s).or_insert(0) += 1;
        SnapshotGuard { ts: s, reg: self }
    }

    /// [`Registry::take_snapshot`] that also runs `f(S)` inside the same
    /// registry critical section (§3.1, §8.6): SSI creates the txn's entry
    /// there, so no retention pass can run between taking `S` and the entry
    /// existing (seed 21). `f` must not take the registry mutex; it may take
    /// locks later in the §3.1 order (SSI, graph).
    pub fn take_snapshot_with<R>(&self, f: impl FnOnce(Ts) -> R) -> (SnapshotGuard<'_>, R) {
        let mut st = self.lock();
        let s = Ts(self.visible_ts.load(Ordering::SeqCst));
        let r = f(s);
        *st.snapshots.entry(s).or_insert(0) += 1;
        (SnapshotGuard { ts: s, reg: self }, r)
    }

    /// Registers a caller-chosen ts (`AS OF t`, a segment build's
    /// `built_at`): requires `t >= W` and `t <= visible_ts`, else 72000
    /// (§3.1). This is the primitive AS OF reads are built on.
    pub fn register_at(&self, t: Ts) -> Result<SnapshotGuard<'_>, TxnError> {
        let mut st = self.lock();
        if t >= st.w && t <= Ts(self.visible_ts.load(Ordering::SeqCst)) {
            *st.snapshots.entry(t).or_insert(0) += 1;
            Ok(SnapshotGuard { ts: t, reg: self })
        } else {
            Err(TxnError::SnapshotTooOld)
        }
    }

    /// Opens a view: registers `c = ++view_counter` and `vts = visible_ts`
    /// under the mutex, releases it, then opens the KV snapshot (§3.1,
    /// I-SNAP-ORDER). The view unregisters when the guard drops.
    pub fn open_view<K: OrderedKv>(&self, kv: &K) -> ViewGuard<'_, K::Snap> {
        let counter = {
            let mut st = self.lock();
            st.view_counter += 1;
            let c = st.view_counter;
            st.views
                .insert(c, Ts(self.visible_ts.load(Ordering::SeqCst)));
            c
        };
        let snap = kv.snapshot();
        ViewGuard {
            snap,
            counter,
            reg: self,
        }
    }

    /// The smallest registered open-view counter, `u64::MAX` when none.
    pub fn min_view_counter(&self) -> u64 {
        self.lock().min_view_counter()
    }

    /// The published `W` (§9.1), read-only.
    pub fn published_w(&self) -> Ts {
        self.lock().published_w()
    }

    /// Computes the §9.1 min and publishes `W = max(old W, computed)` in one
    /// critical section, as §9.1 requires: the GC job's only publish path.
    pub fn publish_computed_w(&self, extra_floor: Option<Ts>) -> Ts {
        let mut st = self.lock();
        st.publish_computed_w(extra_floor)
    }

    /// `Some(view_counter)` once the boot sweep has finished, `None` before
    /// (§7.4).
    pub fn sweep_counter(&self) -> Option<u64> {
        self.lock().sweep_counter()
    }

    /// Records that the boot sweep finished, at the current view counter
    /// (§7.4: the sweep records `sweep_counter = view_counter` under the
    /// registry mutex).
    pub fn record_sweep(&self) {
        let mut st = self.lock();
        st.sweep_counter = Some(st.view_counter);
    }

    /// Runs `f` under the registry mutex, for callers that must act
    /// atomically with registration: §7.3 step 4, §7.4, §8.6.
    pub fn with_registry<R>(&self, f: impl FnOnce(&mut RegistryState) -> R) -> R {
        let mut st = self.lock();
        f(&mut st)
    }
}

/// A registered snapshot; unregisters on drop (§3.1: until the txn ends, or
/// the statement ends under RC).
pub struct SnapshotGuard<'a> {
    ts: Ts,
    reg: &'a Registry,
}

impl SnapshotGuard<'_> {
    pub fn ts(&self) -> Ts {
        self.ts
    }
}

impl Drop for SnapshotGuard<'_> {
    fn drop(&mut self) {
        let mut st = self.reg.lock();
        if let Some(count) = st.snapshots.get_mut(&self.ts) {
            *count -= 1;
            if *count == 0 {
                st.snapshots.remove(&self.ts);
            }
        }
    }
}

/// An open view: a registered counter plus the KV snapshot opened after it
/// (§3.1). Exposes `get`/`scan`; unregisters on drop. While it is open
/// `W <= vts`, so GC keeps every version it could return as a key's newest.
pub struct ViewGuard<'a, S: Snapshot> {
    snap: S,
    counter: u64,
    reg: &'a Registry,
}

impl<'a, S: Snapshot> ViewGuard<'a, S> {
    /// The view's registered counter.
    pub fn counter(&self) -> u64 {
        self.counter
    }

    /// The underlying KV snapshot, for read paths that take a
    /// [`Snapshot`] directly.
    pub fn as_snap(&self) -> &S {
        &self.snap
    }

    pub fn get(&self, key: &[u8]) -> nucleus_kv::Result<Option<Value>> {
        self.snap.get(key)
    }

    pub fn scan(
        &self,
        range: (Bound<&[u8]>, Bound<&[u8]>),
        reverse: bool,
    ) -> Box<dyn Iterator<Item = nucleus_kv::Result<(Key, Value)>> + '_> {
        self.snap.scan(range, reverse)
    }
}

impl<S: Snapshot> Drop for ViewGuard<'_, S> {
    fn drop(&mut self) {
        let mut st = self.reg.lock();
        st.views.remove(&self.counter);
    }
}
