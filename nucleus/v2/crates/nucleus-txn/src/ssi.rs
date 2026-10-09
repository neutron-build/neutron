//! C-T0 §8: SERIALIZABLE as SSI. One **SSI mutex** guards every piece of
//! SSI state (`state::State`): per-txn SIREADs, rw-edges, phases, `doomed`,
//! `earliest_out_conflict_commit`, the writer map `commit_ts -> TxnId`, the
//! prepare counter and the storage map. Lock order (§3.1): latch → registry
//! mutex → SSI mutex → graph mutex; nothing here takes the registry mutex
//! or a latch while holding the SSI mutex. The retired-storage notes sit
//! behind a separate leaf mutex so a non-SER commit can test for them
//! without the SSI mutex (§8.4).
//!
//! Plug-in points: [`Ssi`] is the core's [`SsiHook`] (writer-side edges,
//! the unique rule's `covers`, pre-commit, abort) and the commit pipeline's
//! [`CommitObserver`] (§3 step 3, the writer map); its reads feed
//! [`read::read_key`]/[`read::scan`] through a [`ReadObserver`] that
//! collects the reader-side edges.
//!
//! SER reads go through [`Ssi::read_key`] and [`Ssi::scan`] only: each
//! registers its SIREAD (stamped with the registry's `view_counter`, inside
//! the registry critical section) and **then** opens a fresh registered
//! view (I-SSI-ORDER, seeds 5, 6, 30, 36).

mod state;

#[cfg(test)]
pub(crate) mod pause;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use nucleus_kv::{Key, OrderedKv};

use crate::boot::Core;
use crate::commit::{CommitObserver, FailStop};
use crate::read::{self, ReadObserver};
use crate::registry::{Registry, SnapshotGuard};
use crate::txn::{Isolation, Txn};
use crate::visibility::{ReadCtx, RwEdge};
use crate::write::SsiHook;
use crate::{Ts, TxnError, TxnId};

pub use state::{RelOid, Siread};
use state::{Retired, State};

/// The registry of the core the [`Ssi`] is installed on, for hooks that are
/// not handed a core (`before_point_read`, `lock_relation_read`).
trait RegistryHost: Send + Sync {
    fn registry(&self) -> &Registry;
}

impl<K: OrderedKv> RegistryHost for Core<K> {
    fn registry(&self) -> &Registry {
        &self.registry
    }
}

/// Counts of SSI state, for tests (diagnostic, read-only).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SsiStats {
    /// SER txns with an entry (active, prepared, or committed not yet
    /// retired).
    pub entries: usize,
    /// SIREADs over all entries.
    pub sireads: usize,
    /// Writer-map entries.
    pub writers: usize,
}

/// SSI (§8). Create with [`Ssi::install`].
pub struct Ssi {
    st: Mutex<State>,
    /// Retired storage noted per DDL txn (§8.6). Leaf mutex.
    notes: Mutex<BTreeMap<TxnId, Vec<Retired>>>,
    host: Weak<dyn RegistryHost>,
    #[cfg(test)]
    pub(crate) pause_begin: pause::Pause,
    #[cfg(test)]
    pub(crate) pause_precommit: pause::Pause,
}

/// Collects a read's reader-side edges; they are recorded under the SSI
/// mutex after the read.
#[derive(Default)]
struct Collect(Vec<RwEdge>);

impl ReadObserver for Collect {
    fn on_edge(&mut self, edge: RwEdge) {
        self.0.push(edge);
    }
}

impl Ssi {
    /// Creates the SSI state and installs it as `core`'s [`SsiHook`]. The
    /// caller passes [`Ssi::commit_observer`] in the commit pipeline's
    /// config (§3 step 3).
    pub fn install<K: OrderedKv>(core: &Arc<Core<K>>) -> Arc<Ssi> {
        let host: Arc<dyn RegistryHost> = Arc::clone(core) as Arc<dyn RegistryHost>;
        let ssi = Arc::new(Ssi {
            st: Mutex::new(State::default()),
            notes: Mutex::new(BTreeMap::new()),
            host: Arc::downgrade(&host),
            #[cfg(test)]
            pause_begin: pause::Pause::default(),
            #[cfg(test)]
            pause_precommit: pause::Pause::default(),
        });
        core.set_ssi_hook(Arc::clone(&ssi) as Arc<dyn SsiHook>);
        ssi
    }

    /// The writer-map observer for the commit pipeline (§3 step 3, §8.5).
    pub fn commit_observer(self: &Arc<Self>) -> Arc<dyn CommitObserver> {
        Arc::clone(self) as Arc<dyn CommitObserver>
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.st.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn lock_notes(&self) -> MutexGuard<'_, BTreeMap<TxnId, Vec<Retired>>> {
        self.notes.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Takes the SER txn's snapshot and creates its SSI entry in the same
    /// registry critical section (§3.1, §8.6, seed 21). `read_only` is the
    /// declared `READ ONLY`. A non-SERIALIZABLE txn is an invariant error.
    pub fn begin<'c, K: OrderedKv>(
        &self,
        core: &'c Core<K>,
        txn: &Txn,
        read_only: bool,
    ) -> Result<SnapshotGuard<'c>, TxnError> {
        if txn.isolation != Isolation::Serializable {
            return Err(TxnError::Invariant(format!(
                "Ssi::begin on non-SERIALIZABLE txn {:?}",
                txn.id
            )));
        }
        let (guard, created) = core.registry.take_snapshot_with(|s| {
            #[cfg(test)]
            self.pause_begin.hit();
            self.lock().begin(txn.id, s, read_only)
        });
        if created {
            Ok(guard)
        } else {
            Err(TxnError::Invariant(format!(
                "Ssi::begin: txn {:?} already has an SSI entry",
                txn.id
            )))
        }
    }

    /// Registers `sr` for `txn`, stamped with the registry's `view_counter`
    /// inside the registry critical section (registry → SSI). `false` when
    /// `txn` has no SSI entry.
    fn register(&self, reg: &Registry, txn: TxnId, sr: Siread) -> bool {
        reg.with_registry(|r| {
            let stamp = r.view_counter();
            self.lock().register(txn, sr, stamp)
        })
    }

    fn register_or_err(&self, reg: &Registry, txn: TxnId, sr: Siread) -> Result<(), TxnError> {
        if self.register(reg, txn, sr) {
            Ok(())
        } else {
            Err(TxnError::Invariant(format!(
                "SSI read by txn {txn:?}, which has no SSI entry (not SERIALIZABLE, or ended)"
            )))
        }
    }

    fn check_ctx(txn: TxnId, ctx: &ReadCtx) -> Result<(), TxnError> {
        if ctx.txn == txn {
            Ok(())
        } else {
            Err(TxnError::Invariant(format!(
                "SSI read for {txn:?} with a ReadCtx of {:?}",
                ctx.txn
            )))
        }
    }

    /// The SER point read (§4, §8.1), also the index→row fetch: register
    /// `Point { key }`, **then** open a fresh registered view, read, and
    /// record the reader-side edges.
    pub fn read_key<K: OrderedKv>(
        &self,
        core: &Core<K>,
        txn: TxnId,
        key: &[u8],
        ctx: &ReadCtx,
    ) -> Result<Option<Vec<u8>>, TxnError> {
        Self::check_ctx(txn, ctx)?;
        self.register_or_err(&core.registry, txn, Siread::Point { key: key.to_vec() })?;
        let view = core.open_view();
        let mut edges = Collect::default();
        let value = read::read_key(core, &view, key, ctx, &mut edges)?;
        self.record_read(txn, &edges.0, view.counter());
        Ok(value)
    }

    /// The SER scan of logical keys `[lo, hi)` (§4, §8.1): register
    /// `Range { lo, hi }` on the full bound, **then** open the scan's own
    /// registered view and iterate.
    pub fn scan<K: OrderedKv>(
        &self,
        core: &Core<K>,
        txn: TxnId,
        lo: &[u8],
        hi: &[u8],
        ctx: &ReadCtx,
    ) -> Result<Vec<(Key, Vec<u8>)>, TxnError> {
        Self::check_ctx(txn, ctx)?;
        self.register_or_err(
            &core.registry,
            txn,
            Siread::Range {
                lo: lo.to_vec(),
                hi: hi.to_vec(),
            },
        )?;
        let view = core.open_view();
        let mut edges = Collect::default();
        let rows = read::scan(
            core,
            &view,
            (Bound::Included(lo), Bound::Excluded(hi)),
            ctx,
            &mut edges,
        )
        .collect::<Result<Vec<_>, _>>()?;
        self.record_read(txn, &edges.0, view.counter());
        Ok(rows)
    }

    fn record_read(&self, txn: TxnId, edges: &[RwEdge], view_counter: u64) {
        let mut st = self.lock();
        st.record_read(txn, edges);
        if let Some(e) = st.entries.get_mut(&txn) {
            e.last_view = Some(view_counter);
        }
    }

    /// Registers `Relation { rel_oid }` for `txn` (ANN/FTS/columnar paths,
    /// escalation target, §8.1), before the caller opens its view.
    pub fn lock_relation_read(&self, txn: TxnId, rel_oid: RelOid) -> Result<(), TxnError> {
        let host = self.host.upgrade().ok_or_else(|| {
            TxnError::Invariant("Ssi::lock_relation_read: the core is gone".into())
        })?;
        self.register_or_err(host.registry(), txn, Siread::Relation { rel_oid })
    }

    /// 40001 if `txn` was doomed (§8.3); the SQL layer calls it at each
    /// statement start. Read under the SSI mutex.
    pub fn check_doomed(&self, txn: TxnId) -> Result<(), TxnError> {
        if self.lock().entries.get(&txn).is_some_and(|e| e.doomed) {
            Err(TxnError::SerializationFailure)
        } else {
            Ok(())
        }
    }

    /// Maps the storage range `[lo, hi)` to relation `rel_oid` (the catalog
    /// keeps this current; relation SIREADs resolve through it, §12 Q7).
    pub fn map_storage(&self, lo: &[u8], hi: &[u8], rel_oid: RelOid) {
        self.lock().map_storage(lo.to_vec(), hi.to_vec(), rel_oid);
    }

    /// Removes the mapping of `[lo, hi)`.
    pub fn unmap_storage(&self, lo: &[u8], hi: &[u8]) {
        self.lock().unmap_storage(lo, hi);
    }

    /// §8.2 DDL side, called when a DROP/TRUNCATE/rewrite of `rel_oid`
    /// executes, right after AccessExclusive: for a SER `writer`, every
    /// concurrent SIREAD holder on the relation (any granularity, any storage
    /// mapped to it, and the ranges `writer` noted through
    /// [`Ssi::note_retired`], so storage unmapped earlier in this txn still
    /// counts) gets `holder -> writer`. Runs at execution, not at
    /// pre-commit (seeds 32, 60).
    pub fn on_ddl_execute(
        &self,
        writer: TxnId,
        isolation: Isolation,
        rel_oid: RelOid,
    ) -> Result<(), TxnError> {
        if isolation != Isolation::Serializable {
            return Ok(());
        }
        let mut st = self.lock();
        let extra: Vec<(Key, Key)> = self
            .lock_notes()
            .get(&writer)
            .map(|v| {
                v.iter()
                    .filter(|r| r.rel_oid == rel_oid)
                    .flat_map(|r| r.ranges.iter().cloned())
                    .collect()
            })
            .unwrap_or_default();
        st.on_ddl(writer, rel_oid, &extra);
        Ok(())
    }

    /// Records the storage `txn` retires for `rel_oid` (any isolation). At
    /// its pre-commit, every finer SIREAD inside those ranges becomes
    /// `Relation { rel_oid }` for its holder (§8.6).
    pub fn note_retired(&self, txn: TxnId, rel_oid: RelOid, ranges: Vec<(Key, Key)>) {
        self.lock_notes()
            .entry(txn)
            .or_default()
            .push(Retired { rel_oid, ranges });
    }

    /// Runs the retired-id promotion for `txn`'s notes, under the SSI mutex
    /// the caller holds.
    fn promote_noted(&self, st: &mut State, txn: TxnId) {
        let noted = self.lock_notes().remove(&txn);
        for r in noted.iter().flatten() {
            st.promote(r);
        }
    }

    /// §8.6 retention, inside the registry critical section (so no snapshot
    /// can be taken and registered halfway), then the SSI mutex. Returns how
    /// many committed txns were retired.
    pub fn run_retention<K: OrderedKv>(&self, core: &Core<K>) -> usize {
        core.registry.with_registry(|_| {
            let visible = core.visible_ts();
            self.lock().retire(visible)
        })
    }

    // ---- diagnostics (read-only, for tests) ----------------------------

    /// Every recorded edge `(from, to)`, including edges to retired txns.
    /// Diagnostic.
    pub fn edges(&self) -> Vec<(TxnId, TxnId)> {
        let st = self.lock();
        st.entries
            .iter()
            .flat_map(|(x, e)| e.out.keys().map(move |y| (*x, *y)))
            .collect()
    }

    /// Whether `txn` is doomed. Diagnostic.
    pub fn is_doomed(&self, txn: TxnId) -> bool {
        self.lock().entries.get(&txn).is_some_and(|e| e.doomed)
    }

    /// `earliest_out_conflict_commit(txn)` (§8.5). Diagnostic.
    pub fn earliest_out_conflict_commit(&self, txn: TxnId) -> Option<Ts> {
        self.lock().entries.get(&txn).and_then(|e| e.eocc)
    }

    /// The writer map's entry for `ts` (§8.5). Diagnostic.
    pub fn writer_of(&self, ts: Ts) -> Option<TxnId> {
        self.lock().writers.get(&ts).copied()
    }

    /// `txn`'s SIREADs with their registration stamps. Diagnostic.
    pub fn sireads_of(&self, txn: TxnId) -> Vec<(Siread, u64)> {
        self.lock()
            .entries
            .get(&txn)
            .map(|e| e.sireads.iter().map(|(s, c)| (s.clone(), *c)).collect())
            .unwrap_or_default()
    }

    /// The counter of the view `txn`'s last SSI read opened. I-SSI-ORDER
    /// holds for that read iff its covering SIREAD's stamp is smaller.
    /// Diagnostic.
    pub fn last_read_view_counter(&self, txn: TxnId) -> Option<u64> {
        self.lock().entries.get(&txn).and_then(|e| e.last_view)
    }

    /// Sizes of the SSI state. Diagnostic.
    pub fn stats(&self) -> SsiStats {
        let st = self.lock();
        SsiStats {
            entries: st.entries.len(),
            sireads: st.entries.values().map(|e| e.sireads.len()).sum(),
            writers: st.writers.len(),
        }
    }
}

impl CommitObserver for Ssi {
    /// §3 step 3: called by the commit thread for each SER request after
    /// the group's timestamps are assigned and before any status is set.
    fn on_assigned(&self, txn: TxnId, ts: Ts) {
        self.lock().on_assigned(txn, ts);
    }
}

impl SsiHook for Ssi {
    fn covers(&self, txn: TxnId, key: &[u8]) -> bool {
        self.lock().holds_cover(txn, key)
    }

    fn before_point_read(&self, txn: TxnId, key: &[u8]) {
        // A txn without an entry is not SERIALIZABLE: nothing to register.
        if let Some(host) = self.host.upgrade() {
            self.register(host.registry(), txn, Siread::Point { key: key.to_vec() });
        }
    }

    fn on_data_placed(
        &self,
        writer: TxnId,
        isolation: Isolation,
        key: &[u8],
    ) -> Result<(), TxnError> {
        if isolation == Isolation::Serializable {
            self.lock().on_data_placed(writer, key);
        }
        Ok(())
    }

    fn pre_commit(
        &self,
        txn: TxnId,
        isolation: Isolation,
        enqueue: &mut dyn FnMut() -> Result<(), TxnError>,
    ) -> Result<(), TxnError> {
        if isolation != Isolation::Serializable {
            if !self.lock_notes().contains_key(&txn) {
                return enqueue();
            }
            // A non-SER DDL txn: promotion and enqueue in one critical
            // section (§8.4).
            let mut st = self.lock();
            self.promote_noted(&mut st, txn);
            return enqueue();
        }
        // §8.4: doomed check, dangerous-structure check, prepare, promotion
        // and enqueue in one SSI critical section.
        let mut st = self.lock();
        if st.entries.get(&txn).is_some_and(|e| e.doomed) {
            return Err(TxnError::SerializationFailure);
        }
        let doom = st.check(txn)?;
        for v in doom {
            st.doom(v);
        }
        #[cfg(test)]
        self.pause_precommit.hit();
        st.prepare(txn);
        self.promote_noted(&mut st, txn);
        enqueue()
    }

    fn on_abort(&self, txn: TxnId) {
        self.lock().abort(txn);
        self.lock_notes().remove(&txn);
    }
}

/// Handle to a spawned retention thread ([`spawn_retention`]).
pub struct RetentionHandle {
    stop: Option<mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
    fail_stop: Arc<dyn FailStop>,
}

impl RetentionHandle {
    /// Stops the thread and joins it. A panicked thread is reported through
    /// the core's fail-stop hook and returned as an error.
    pub fn stop(mut self) -> Result<(), TxnError> {
        self.stop.take();
        match self.thread.take() {
            Some(t) => t.join().map_err(|_| {
                let e = TxnError::Invariant("ssi retention thread panicked".into());
                self.fail_stop.on_kv_error(&e);
                e
            }),
            None => Ok(()),
        }
    }
}

impl Drop for RetentionHandle {
    fn drop(&mut self) {
        // Dropping the sender wakes and ends the loop; the thread detaches.
        self.stop.take();
    }
}

/// Runs [`Ssi::run_retention`] every `interval` in a background thread
/// until the handle is stopped or dropped. A spawn failure goes to the
/// core's fail-stop hook.
pub fn spawn_retention<K: OrderedKv>(
    core: Arc<Core<K>>,
    ssi: Arc<Ssi>,
    interval: Duration,
) -> RetentionHandle {
    let fail_stop = core.fail_stop();
    let (tx, rx) = mpsc::channel::<()>();
    let spawned = std::thread::Builder::new()
        .name("nucleus-ssi-retention".into())
        .spawn(move || loop {
            ssi.run_retention(&core);
            match rx.recv_timeout(interval) {
                Err(RecvTimeoutError::Timeout) => {}
                Ok(()) | Err(RecvTimeoutError::Disconnected) => return,
            }
        });
    let thread = match spawned {
        Ok(t) => Some(t),
        Err(e) => {
            fail_stop.on_kv_error(&TxnError::Invariant(format!(
                "failed to spawn the ssi retention thread: {e}"
            )));
            None
        }
    };
    RetentionHandle {
        stop: Some(tx),
        thread,
        fail_stop,
    }
}
