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

use nucleus_kv::{Batch, Durability, Key, OrderedKv, Snapshot, Value};

use crate::commit::{AbortFailStop, CommitMsg, FailStop, ReleaseHook};
use crate::encoding::{
    decode_intent, end_key, intent_key, parse_key, parse_sys_txn_key, sys_epoch_key, sys_gc_w_key,
    sys_log_key, sys_ts_hwm_key, sys_txn_key, sys_txn_prefix, sys_txn_prefix_end, version_key,
    Entry, HEADER_LIVE,
};
use crate::latch::Latches;
use crate::registry::{Registry, SnapshotGuard};
use crate::removal::{remove_intent, RemovalMode};
use crate::status::StatusTable;
use crate::wait::Waits;
use crate::write::{NoRowLocks, NoSsiHook, RowLocks, SsiHook};
use crate::{kv_err, Ts, TxnError, TxnId, TxnStatus};

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
    /// The seq module (§10, C-T7r2): this core's sequence state, owned by
    /// construction — one core = one module for its whole lifetime, so
    /// `fail_stop` hook swaps or hooks shared across cores cannot split
    /// or alias it. See `seq.rs`.
    pub(crate) seq: Mutex<crate::seq::SeqModule>,
    /// The background-job framework's shared state (§9.3, C-B1a): one
    /// wake and one counter block per job kind, plus the job probe.
    /// Reached by the runner threads through their `Arc<Core>` and by the
    /// job bodies through `Core::job_probe_at`/`Core::wake_job`.
    pub(crate) jobs: crate::jobs::JobsShared,
    /// The latch-key source (§7.3 SD2b, C-B1b): the seam the boot sweep
    /// and retired-prefix cleanup read `latch_key(k)`'s deferrable prefix
    /// from. Default [`NoDeferrableKeys`] (owner decision D2).
    pub(crate) latch_key_source: Mutex<Arc<dyn crate::jobs::LatchKeySource>>,
}

impl<K: OrderedKv> Core<K> {
    /// Opens the store (§7.2). See the module docs for the boot order.
    pub fn open(kv: K) -> Result<Core<K>, TxnError> {
        // C-R0b: boot writes the epoch with `Durability::Yes` — Fsync.
        crate::rt::check(crate::rt::Blocking::Fsync, "Core::open");
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
            seq: Mutex::new(crate::seq::SeqModule::new()),
            jobs: crate::jobs::JobsShared::new(),
            latch_key_source: Mutex::new(Arc::new(crate::jobs::NoDeferrableKeys)),
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
        // C-R0b: a latest-state KV read — KvIo.
        crate::rt::check(crate::rt::Blocking::KvIo, "Core::latest_get");
        self.kv.get_latest(key).map_err(crate::kv_err)
    }

    /// Writes one atomic batch (§3 step 2). Ordering between concurrent
    /// writers is the caller's latch discipline, not this method.
    pub fn write(&self, batch: Batch, durability: Durability) -> Result<(), TxnError> {
        // C-R0b: Fsync when durable, otherwise a plain KV write (KvIo).
        crate::rt::check(
            if durability == Durability::Yes {
                crate::rt::Blocking::Fsync
            } else {
                crate::rt::Blocking::KvIo
            },
            "Core::write",
        );
        self.kv.write(batch, durability).map_err(crate::kv_err)
    }

    /// fsyncs the WAL through everything written so far (§3; never on an
    /// async executor thread).
    pub fn sync_wal(&self) -> Result<(), TxnError> {
        // C-R0b: the fsync funnel itself.
        crate::rt::check(crate::rt::Blocking::Fsync, "Core::sync_wal");
        self.kv.sync_wal().map_err(crate::kv_err)
    }

    /// C-T0 §10 bulk ingest (the C-T6 §7 step-6 primitive): writes every
    /// entry as a committed version at `snap.ts()` — no intents, no
    /// write-set log, no status record.
    ///
    /// Preconditions (the caller's guarantees, not checked here): `snap` is
    /// registered and stays registered until this returns (so `W <= S` and
    /// GC never removes an ingested version); the `ranges` are the storage
    /// ranges of a **non-public** index; the C-T6 §7 fence (step 3) has
    /// committed at or below `S`.
    ///
    /// Steps, in order:
    /// 1. Collect and validate every entry before any I/O: each logical key
    ///    inside one of `ranges`, logical keys strictly ascending. A
    ///    violation is [`TxnError::Invariant`] and writes nothing.
    /// 2. Through one registered view, plan the removal of every intent in
    ///    `[intent_key(first), end_key(last))` as `gc`'s `retire_locked`
    ///    plans (the owner's status looked up under the view, I-TRUNC):
    ///    `Resolve` for a visible commit at or below `S`, `Discard` for an
    ///    `Aborted` owner, and the rest left in place — §4 step 1 returns a
    ///    committed intent's data without reading versions, so an
    ///    unresolved one would shadow the ingested entry. Each removal
    ///    runs through [`remove_intent`] (§7.3, latch prefix `None`:
    ///    online indexes are never deferrable).
    /// 3. Sync the WAL, so no ingested entry is durable before the commits
    ///    and resolutions it was derived from (I-WAL-ORDER).
    /// 4. One [`OrderedKv::ingest_sorted`] of every entry as
    ///    `version_key(L, S)` with a live header and the payload.
    pub fn ingest_versions(
        &self,
        snap: &SnapshotGuard<'_>,
        ranges: &[(Key, Key)],
        entries: &mut dyn Iterator<Item = (Key, Vec<u8>)>,
    ) -> Result<(), TxnError> {
        // C-R0ba: the call's wait includes the WAL sync of step 3 — Fsync.
        crate::rt::check(crate::rt::Blocking::Fsync, "Core::ingest_versions");

        // Step 1: collect and validate everything before any I/O.
        let mut collected: Vec<(Key, Value)> = Vec::new();
        for (logical, payload) in entries {
            if !ranges.iter().any(|(lo, hi)| {
                logical.as_slice() >= lo.as_slice() && logical.as_slice() < hi.as_slice()
            }) {
                return Err(TxnError::Invariant(format!(
                    "ingest_versions: entry key {logical:?} outside every range of {ranges:?}"
                )));
            }
            if collected.last().is_some_and(|(prev, _)| logical <= *prev) {
                return Err(TxnError::Invariant(format!(
                    "ingest_versions: entry keys not strictly ascending ({logical:?} after {:?})",
                    collected.last().map(|(k, _)| k),
                )));
            }
            collected.push((logical, payload));
        }
        let (first, last) = match (collected.first(), collected.last()) {
            (Some((f, _)), Some((l, _))) => (f.clone(), l.clone()),
            // An empty chunk: nothing to ingest, no span to resolve.
            _ => return Ok(()),
        };

        // Step 2: plan the span's removals under one registered view, then
        // run them (the `retire_locked` pattern: the status is looked up
        // while the view is open, so the plan cannot hit the §7.4 window;
        // `remove_intent` re-reads under the latch and no-ops if the async
        // resolver won the race).
        struct PlannedRemoval {
            key: Key,
            owner: TxnId,
            mode: RemovalMode,
        }
        let view = self.open_view();
        let mut plan: Vec<PlannedRemoval> = Vec::new();
        let span_lo = intent_key(&first);
        let span_hi = end_key(&last);
        for entry in view.scan(
            (
                Bound::Included(span_lo.as_slice()),
                Bound::Excluded(span_hi.as_slice()),
            ),
            false,
        ) {
            let (stored, value) = entry.map_err(kv_err)?;
            let Some((logical, Entry::Intent)) = parse_key(&stored) else {
                continue; // versions of keys in the span
            };
            let intent = decode_intent(&value)?;
            let owner = intent.txn;
            let mode = match self.status.lookup_for_intent(owner)? {
                // A commit at or below S is a visible commit (S itself was
                // admitted at or below visible_ts, §3.1).
                TxnStatus::Committed(c) if c <= snap.ts() => RemovalMode::Resolve,
                TxnStatus::Aborted => RemovalMode::Discard,
                // Pending, or a commit above S: left in place (§10 resolves
                // only commits at or below S; a Pending intent does not
                // shadow — §4 step 1 falls through to versions).
                _ => continue,
            };
            plan.push(PlannedRemoval {
                key: logical.to_vec(),
                owner,
                mode,
            });
        }
        drop(view);
        for r in &plan {
            remove_intent(self, &r.key, None, r.owner, r.mode)?;
        }

        // Step 3: the WAL sync before the ingest (§10).
        self.sync_wal()?;

        // Step 4: one ingest, every entry at S with a live header.
        let ts = snap.ts();
        let mut stored: Vec<(Key, Value)> = Vec::with_capacity(collected.len());
        for (logical, payload) in collected {
            let mut value = Vec::with_capacity(1 + payload.len());
            value.push(HEADER_LIVE);
            value.extend_from_slice(&payload);
            stored.push((version_key(&logical, ts), value));
        }
        self.kv
            .ingest_sorted(&mut stored.into_iter())
            .map_err(kv_err)?;
        Ok(())
    }

    /// Gives up the KV store: the shutdown/reopen path (§7.2 boot, §3). The
    /// `Core` is consumed, so this grants no read path — no view or latch
    /// discipline can be bypassed through it.
    pub fn into_kv(self) -> K {
        self.kv
    }

    /// Truncates `id`'s status entry if and only if the §7.4 conditions
    /// hold. The eligibility check and the in-memory removal are one
    /// registry critical section; the `/sys/txn` + `/sys/log` delete batch
    /// is written **after leaving it** (draft 7.7 §7.4 SD1): it only has
    /// to follow `id`'s removal batches in the WAL, and those preceded
    /// their §7.3 step-4 bookkeeping — so holding the registry mutex over
    /// the KV write would only block every snapshot and view registration
    /// behind it. Returns whether the entry was removed.
    pub fn truncate_status(&self, id: TxnId) -> Result<bool, TxnError> {
        // C-R0b: the /sys/txn delete is a KV write — KvIo, before the
        // registry critical section.
        crate::rt::check(crate::rt::Blocking::KvIo, "Core::truncate_status");
        let removed = self.registry.with_registry(|r| {
            if self
                .status
                .truncate_eligible(id, r.min_view_counter(), r.sweep_counter())
            {
                self.status.truncate(id)
            } else {
                None
            }
        });
        match removed {
            Some(entry) => {
                // Only Committed records are ever persisted (§2.3), so only
                // a Committed entry has keys to delete. The optional
                // `/sys/log/{TxnId}` record dies with the status in the
                // same batch.
                if matches!(entry.status, TxnStatus::Committed(_)) {
                    let mut batch = Batch::default();
                    batch.delete(sys_txn_key(id));
                    batch.delete(sys_log_key(id));
                    self.write(batch, Durability::No)?;
                }
                Ok(true)
            }
            None => Ok(false),
        }
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

    /// Installs the latch-key source (§7.3 SD2b): the seam the boot sweep
    /// (§7.2) and the drop-cleanup job (§9.2) read `latch_key(k)`'s
    /// deferrable prefix from when they remove intents with only raw keys
    /// in hand. Install it **before starting either job**; the source must
    /// answer for every key that can hold an intent, including dropped
    /// indexes and older epochs (§12 Q8). The default is
    /// [`NoDeferrableKeys`] (owner decision D2: no deferrable keys yet).
    pub fn set_latch_key_source(&self, source: Arc<dyn crate::jobs::LatchKeySource>) {
        *self
            .latch_key_source
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = source;
    }

    /// The installed latch-key source (§7.3 SD2b). Crate-private: the
    /// sweep and drop-cleanup bodies are the only readers.
    pub(crate) fn latch_key_source(&self) -> Arc<dyn crate::jobs::LatchKeySource> {
        Arc::clone(
            &self
                .latch_key_source
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        )
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
