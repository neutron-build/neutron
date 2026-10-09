//! C-T0 §3 boot and §7.2 crash recovery: `Core::open`. `Core` is the shared
//! state object every later T card extends: the KV handle, the status table,
//! the registry, the latches and `visible_ts`.
//!
//! Boot order (§7.2): read `/sys/epoch`, increment it and write it with
//! `Durability::Yes` **before anything else**; then load every persisted
//! `Committed` record, read `/sys/ts_hwm` and `/sys/gc_w`, and set
//! `visible_ts = ts_hwm` (§3 boot: `next_ts = ts_hwm + 1`, and anything up to
//! `ts_hwm` without a persisted record never committed). A fresh store starts
//! with epoch 1, `ts_hwm` 0 and `W` 0.

use std::collections::VecDeque;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use nucleus_kv::{Batch, Durability, OrderedKv, Snapshot, Value};

use crate::commit::{AbortFailStop, CommitMsg, FailStop, ReleaseHook};
use crate::encoding::{
    parse_sys_txn_key, sys_epoch_key, sys_gc_w_key, sys_ts_hwm_key, sys_txn_prefix,
    sys_txn_prefix_end,
};
use crate::latch::Latches;
use crate::registry::Registry;
use crate::status::StatusTable;
use crate::wait::Waits;
use crate::write::{NoRowLocks, NoSsiHook, RowLocks, SsiHook};
use crate::{kv_err, Ts, TxnError, TxnId};

/// The shared transaction core (§3, §7.2).
pub struct Core<K: OrderedKv> {
    /// The KV store. Private: reads reachable by later cards go through
    /// registered views ([`Core::open_view`]) or, under the key's latch,
    /// [`Core::latest_get`]; writes through [`Core::write`]. There is no
    /// public handle, so I-SNAP-ORDER and the §3.1 view rules hold by
    /// construction.
    kv: K,
    /// Status table with the boot epoch.
    pub status: StatusTable,
    /// Snapshot/view registry (§3.1, §9.1).
    pub registry: Registry,
    /// Striped latches (§5.0).
    pub latches: Latches,
    /// Waiter tables, parker maker and wait hook (§6).
    pub waits: Waits,
    /// `visible_ts` (§3), shared with the registry so `take_snapshot` reads
    /// and registers it in one critical section.
    visible_ts: Arc<AtomicU64>,
    /// The `/sys/ts_hwm` read once at boot: the single source the commit
    /// pipeline reserves from. Reading it again from the KV later could
    /// observe a newer reservation (or a decode error) and reuse or lose
    /// timestamps (seed 5 of the C-T1b rework).
    boot_ts_hwm: Ts,
    /// The commit channel's sending end, attached by a `CommitPipeline`
    /// (§3). `None` until one is attached; a second attach is an error.
    pub(crate) commit_tx: Mutex<Option<std::sync::mpsc::Sender<CommitMsg>>>,
    /// The fail-stop hook (§3): a KV error on the commit thread or the
    /// background resolver. Default [`AbortFailStop`]; a spawned commit
    /// thread's [`CommitConfig`](crate::commit::CommitConfig) installs
    /// its hook here so both threads report the same way.
    pub(crate) fail_stop: Mutex<Arc<dyn FailStop>>,
    /// The C-T2b shared row-lock table (§6). Default [`NoRowLocks`].
    /// `holders`, `grant` and `release` are called only under the key's
    /// latch.
    row_locks: Mutex<Arc<dyn RowLocks>>,
    /// The C-T3 SSI hook (§8). Default [`NoSsiHook`].
    ssi_hook: Mutex<Arc<dyn SsiHook>>,
    /// The C-T2b release hook (§3 step 5, §7.1); no-op until installed.
    pub(crate) release_hook: Mutex<Arc<dyn ReleaseHook>>,
    /// Queued resolution/cleanup entries (§3 step 5, §7.1), drained by the
    /// resolver.
    pub(crate) resolve_q: Mutex<VecDeque<crate::commit::ResolveEntry>>,
}

impl<K: OrderedKv> Core<K> {
    /// Opens the store (§7.2). See the module docs for the boot order.
    pub fn open(kv: K) -> Result<Core<K>, TxnError> {
        // Epoch first: increment and sync before anything else.
        let epoch0 = match kv.snapshot().get(&sys_epoch_key()).map_err(kv_err)? {
            Some(v) => {
                if v.len() != 4 {
                    return Err(TxnError::Corrupt(format!(
                        "/sys/epoch value of {} bytes, expected 4",
                        v.len()
                    )));
                }
                let mut b = [0u8; 4];
                b.copy_from_slice(&v);
                u32::from_be_bytes(b)
            }
            None => 0,
        };
        let epoch = epoch0
            .checked_add(1)
            .ok_or_else(|| TxnError::Corrupt("/sys/epoch exhausted u32".into()))?;
        let mut batch = Batch::default();
        batch.put(sys_epoch_key(), epoch.to_be_bytes().to_vec());
        kv.write(batch, Durability::Yes).map_err(kv_err)?;

        // Everything else is read from a snapshot taken after the epoch
        // write, so it cannot miss a concurrent write to the same keys (boot
        // runs before any txn starts; this is belt and braces).
        let snap = kv.snapshot();

        let ts_hwm = read_ts(&snap, &sys_ts_hwm_key())?;
        let gc_w = read_ts(&snap, &sys_gc_w_key())?;

        let status = StatusTable::new(epoch);
        let txn_lo = sys_txn_prefix();
        let txn_hi = sys_txn_prefix_end();
        for entry in snap.scan(
            (
                Bound::Included(txn_lo.as_slice()),
                Bound::Excluded(txn_hi.as_slice()),
            ),
            false,
        ) {
            let (key, value) = entry.map_err(kv_err)?;
            let id = parse_sys_txn_key(&key)
                .ok_or_else(|| TxnError::Corrupt(format!("bad /sys/txn key {key:?}")))?;
            if id.epoch >= epoch {
                return Err(TxnError::Corrupt(format!(
                    "/sys/txn record {id:?} at or above boot epoch {epoch}"
                )));
            }
            if value.len() != 8 {
                return Err(TxnError::Corrupt(format!(
                    "/sys/txn {id:?} value of {} bytes, expected 8",
                    value.len()
                )));
            }
            let mut b = [0u8; 8];
            b.copy_from_slice(&value);
            let ts = Ts(u64::from_be_bytes(b));
            if ts == Ts::ZERO || ts > ts_hwm {
                return Err(TxnError::Corrupt(format!(
                    "/sys/txn {id:?} commit ts {ts:?} outside (0, ts_hwm {ts_hwm:?}]"
                )));
            }
            status.insert_committed(id, ts);
        }

        let visible_ts = Arc::new(AtomicU64::new(ts_hwm.0));
        Ok(Core {
            kv,
            status,
            registry: Registry::new(visible_ts.clone(), gc_w),
            latches: Latches::default(),
            waits: Waits::new(),
            visible_ts,
            boot_ts_hwm: ts_hwm,
            commit_tx: Mutex::new(None),
            fail_stop: Mutex::new(Arc::new(AbortFailStop)),
            row_locks: Mutex::new(Arc::new(NoRowLocks)),
            ssi_hook: Mutex::new(Arc::new(NoSsiHook)),
            release_hook: Mutex::new(Arc::new(crate::commit::NoReleaseHook)),
            resolve_q: Mutex::new(VecDeque::new()),
        })
    }

    /// `visible_ts` (§3). Monotonic.
    pub fn visible_ts(&self) -> Ts {
        Ts(self.visible_ts.load(Ordering::SeqCst))
    }

    /// Advances `visible_ts` (commit thread step 4; monotonic).
    /// Crate-private: the pipeline's step 4 is the only advancer.
    pub(crate) fn advance_visible_ts(&self, ts: Ts) {
        self.visible_ts.fetch_max(ts.0, Ordering::SeqCst);
    }

    /// The `/sys/ts_hwm` read at boot: the sequencer's reserved floor
    /// (`next_ts = ts_hwm + 1`, §3). One source, read once — the pipeline
    /// never re-reads the KV for it.
    pub fn ts_hwm(&self) -> Ts {
        self.boot_ts_hwm
    }

    /// The boot epoch.
    pub fn epoch(&self) -> u32 {
        self.status.epoch()
    }

    /// Opens a registered view (§3.1) over this core's KV.
    pub fn open_view(&self) -> crate::registry::ViewGuard<'_, K::Snap> {
        self.registry.open_view(&self.kv)
    }

    /// Latest-state point read of `key` (§3.1, §5.1). **Valid only while the
    /// caller holds `latch(latch_key(key))`**: under the latch no removal of
    /// `key@INTENT` can run (§7.3 step 1), so the read cannot race a
    /// resolution. Every read outside a latch section must go through
    /// [`Core::open_view`] instead.
    pub fn latest_get(&self, key: &[u8]) -> Result<Option<Value>, TxnError> {
        self.kv.get_latest(key).map_err(crate::kv_err)
    }

    /// Writes one atomic batch (§3 step 2). Ordering between concurrent
    /// writers is the caller's latch discipline, not this method.
    pub fn write(&self, batch: Batch, durability: Durability) -> Result<(), TxnError> {
        self.kv.write(batch, durability).map_err(crate::kv_err)
    }

    /// fsyncs the WAL through everything written so far (§3; never on an
    /// async executor thread).
    pub fn sync_wal(&self) -> Result<(), TxnError> {
        self.kv.sync_wal().map_err(crate::kv_err)
    }

    /// Gives up the KV store: the shutdown/reopen path (§7.2 boot, §3). The
    /// `Core` is consumed, so this grants no read path — no view or latch
    /// discipline can be bypassed through it.
    pub fn into_kv(self) -> K {
        self.kv
    }

    /// Truncates `id`'s status entry if and only if the §7.4 conditions
    /// hold, checking them under the registry mutex together with
    /// `min_view_counter`/`sweep_counter` and removing the entry + writing
    /// the `/sys/txn` delete inside the same critical section. Returns
    /// whether the entry was removed.
    pub fn truncate_status(&self, id: TxnId) -> Result<bool, TxnError> {
        self.registry.with_registry(|r| {
            if self
                .status
                .truncate_eligible(id, r.min_view_counter(), r.sweep_counter())
            {
                self.status.truncate(id, &self.kv)
            } else {
                Ok(false)
            }
        })
    }

    /// The shared row-lock table (C-T2b's seam, §6). Crate-private: the
    /// write path is the only caller; `NoRowLocks` until C-T2b installs the
    /// real table through [`Core::set_row_locks`].
    pub(crate) fn row_locks(&self) -> Arc<dyn RowLocks> {
        Arc::clone(
            &self
                .row_locks
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }

    /// Installs the shared row-lock table (C-T2b).
    pub fn set_row_locks(&self, table: Arc<dyn RowLocks>) {
        *self
            .row_locks
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = table;
    }

    /// The SSI hook (C-T3's seam, §8). Crate-private: the write path and the
    /// commit pipeline are the only callers; [`NoSsiHook`] until C-T3
    /// installs the real hook through [`Core::set_ssi_hook`].
    pub(crate) fn ssi_hook(&self) -> Arc<dyn SsiHook> {
        Arc::clone(&self.ssi_hook.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// Installs the SSI hook (C-T3).
    pub fn set_ssi_hook(&self, hook: Arc<dyn SsiHook>) {
        *self.ssi_hook.lock().unwrap_or_else(PoisonError::into_inner) = hook;
    }

    /// Installs the KV's compaction GC filter (§9.2). Crate-private: only
    /// the GC job ([`crate::gc::GcJob`]) hands the KV a filter, after
    /// `GcJob::install`'s ordering rules.
    pub(crate) fn set_kv_gc_filter(&self, filter: Box<dyn nucleus_kv::GcFilter>) {
        self.kv.set_gc_filter(filter);
    }

    /// The KV's GC watermark (§9.1): monotonic; a decrease is
    /// `KvError::WatermarkRegressed`, mapped to [`TxnError::Invariant`]
    /// (the caller treats it as fatal). Crate-private for the same reason.
    pub(crate) fn set_kv_gc_watermark(&self, watermark: u64) -> Result<(), TxnError> {
        self.kv.set_gc_watermark(watermark).map_err(|e| match e {
            nucleus_kv::KvError::WatermarkRegressed { current, requested } => {
                TxnError::Invariant(format!(
                    "gc watermark regressed: current {current}, requested {requested} (C-T0 §9.1)"
                ))
            }
            other => crate::kv_err(other),
        })
    }
}

fn read_ts<S: nucleus_kv::Snapshot>(snap: &S, key: &[u8]) -> Result<Ts, TxnError> {
    match snap.get(key).map_err(kv_err)? {
        Some(v) if v.len() == 8 => {
            let mut b = [0u8; 8];
            b.copy_from_slice(&v);
            Ok(Ts(u64::from_be_bytes(b)))
        }
        Some(v) => Err(TxnError::Corrupt(format!(
            "system key {key:?} value of {} bytes, expected 8",
            v.len()
        ))),
        None => Ok(Ts(0)),
    }
}
