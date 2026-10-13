//! C-T0 §9.2 SD3 / §9.3 (draft 7.7, card C-B1b): the drop-cleanup job.
//! A retirement used to be remembered only in memory (`ssi.note_retired`)
//! so a crash forgot it; DROP/TRUNCATE/DROP INDEX now also write a
//! transactional `/sys/catalog/retired/{id}` row (the catalog's side,
//! `catalog.rs`), and this job retires each range once the durable `W`
//! passes the snapshot that saw the row, then deletes the row.
//!
//! Each step: (a) rescan the visible retired rows through one registered
//! view at a fresh snapshot `S` — unseen ids are remembered with their
//! first `S`, ids already seen keep their earliest `S` (the earliest
//! snapshot that saw the row is the safest gate), ids no longer visible
//! are forgotten (their marker delete committed). (b) The first id whose
//! `S < durable W` goes: `GcJob::retire_storage` per range (intents out
//! through §7.3 first, then the `DeleteRange` — the `RetireAfterRange`
//! probe fires inside `retire_storage` after each range's batch), then
//! the row is deleted in its own small RC txn (`SyncCommit::Off`: the
//! delete only has to follow the `DeleteRange` in the WAL, I-WAL-ORDER,
//! and the row is re-created never — ids come from the oid counter). (c)
//! With nothing eligible the step is `Idle`.
//!
//! The gate is why the job needs `W` to move past a snapshot, not just
//! past the DDL: a row visible at `S` committed at or below `S`, so once
//! the durable `W > S` the DDL is behind `W` for every reader the catalog
//! at `S` could name. `W` reaches past `S` only when `visible_ts` does,
//! so the last row before an idle period waits for the next commit —
//! the marker is only bookkeeping, nothing leaks.
//!
//! The job commits a txn (the marker delete), so it **needs an attached
//! commit thread** and stops before it in [`SHUTDOWN_ORDER`](crate::jobs::runner::SHUTDOWN_ORDER):
//! its in-flight delete is acked by the still-running pipeline instead of
//! failing as "commit pipeline stopped". `GcJob` wakes it whenever the
//! durable `W` rises.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use nucleus_kv::OrderedKv;

use crate::boot::Core;
use crate::catalog::{Catalog, RetiredRow};
use crate::commit::SyncCommit;
use crate::gc::GcJob;
use crate::jobs::{Job, JobCtx, JobKind, JobOutcome, JobPoint};
use crate::txn::Isolation;
use crate::visibility::ReadCtx;
use crate::write::StmtCtx;
use crate::{Ts, TxnError, TxnId};

/// The drop-cleanup job (§9.2, §9.3). Holds the [`GcJob`] it retires
/// through, plus the `seen` map: retired-row id → (the snapshot that
/// first saw it, the row). The map is memory-only — after a crash the job
/// rebuilds it from the rows, which is the point of writing them down.
pub struct DropCleanup<K: OrderedKv> {
    core: Arc<Core<K>>,
    gc: Arc<GcJob<K>>,
    seen: BTreeMap<u64, (Ts, RetiredRow)>,
}

impl<K: OrderedKv> DropCleanup<K> {
    pub fn new(core: Arc<Core<K>>, gc: Arc<GcJob<K>>) -> DropCleanup<K> {
        DropCleanup {
            core,
            gc,
            seen: BTreeMap::new(),
        }
    }

    /// How many retired rows the job has seen and not yet cleaned (the
    /// `nucleus_drop_cleanup_pending` gauge).
    pub fn pending(&self) -> usize {
        self.seen.len()
    }

    /// One step: rescan, then retire + delete the first eligible row.
    /// Drives the same body as the [`Job`] impl (recovery harnesses call
    /// this by hand).
    pub fn run_once(&mut self) -> Result<JobOutcome, TxnError> {
        // (a) Rescan at a fresh registered snapshot.
        {
            let snap = self.core.registry.take_snapshot();
            let s = snap.ts();
            {
                let view = self.core.open_view();
                // A reader id this process never allocates (status ids
                // start at 1): the job owns no intents.
                let ctx = ReadCtx {
                    txn: TxnId {
                        epoch: self.core.epoch(),
                        n: 0,
                    },
                    snapshot: s,
                    stmt_seq: 0,
                };
                let rows = Catalog::scan_retired(&self.core, &view, &ctx)?;
                drop(view);
                let visible: BTreeSet<u64> = rows.iter().map(|(id, _)| *id).collect();
                // A row whose marker delete committed reads absent: forget
                // it (its ranges were retired before that delete).
                self.seen.retain(|id, _| visible.contains(id));
                for (id, row) in rows {
                    // Keep the earliest S: the first snapshot that saw the
                    // row is the one the gate must pass.
                    self.seen.entry(id).or_insert((s, row));
                }
            }
        }
        self.core.set_drop_cleanup_pending(self.seen.len());

        // (b) The first id whose S the durable W has passed.
        let eligible = self
            .seen
            .iter()
            .find(|(_, (ts, _))| *ts < self.gc.durable_w())
            .map(|(id, (ts, row))| (*id, *ts, row.clone()));
        let Some((id, s, row)) = eligible else {
            return Ok(JobOutcome::Idle); // (c)
        };
        let source = self.core.latch_key_source();
        for (lo, hi) in &row.ranges {
            // `retire_storage` plans its §7.3 removals (mode decided
            // inside its view) and only then `DeleteRange`s the range,
            // firing `RetireAfterRange` after the batch write. The gate
            // re-checks the durable W itself.
            let retired = self
                .gc
                .retire_storage(lo, hi, s, &|k| source.deferrable_prefix(k))?;
            if !retired {
                return Err(TxnError::Invariant(format!(
                    "retire_storage of {lo:?}..{hi:?} gated at S {s:?} though the durable W \
                     already passed it (C-T0 §9.2)"
                )));
            }
        }
        self.core.job_probe_at(JobPoint::RetireBeforeMarkerDelete);
        // The marker delete: its own small RC txn, unsynced — it only has
        // to follow the `DeleteRange` batches in the WAL (I-WAL-ORDER),
        // and a crash before it simply re-runs the (idempotent) cleanup.
        let txn = self.core.begin(Isolation::ReadCommitted);
        {
            let snap = self.core.registry.take_snapshot();
            let seq = txn.next_seq()?;
            let ctx = StmtCtx::new(snap.ts(), seq, seq);
            Catalog::delete_retired(&self.core, &txn, &ctx, id)?;
            self.core.commit(txn, SyncCommit::Off)?;
        }
        self.core.job_probe_at(JobPoint::RetireAfterMarkerDelete);
        self.seen.remove(&id);
        self.core.set_drop_cleanup_pending(self.seen.len());
        self.core.job_items(JobKind::DropCleanup, 1);
        Ok(JobOutcome::Progress)
    }
}

impl<K: OrderedKv> Job for DropCleanup<K> {
    fn kind(&self) -> JobKind {
        JobKind::DropCleanup
    }

    fn step(&mut self, _cx: &JobCtx) -> Result<JobOutcome, TxnError> {
        self.run_once()
    }
}
