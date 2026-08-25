//! Per-session cross-model transaction state.
//!
//! Every specialty store (KV, graph, document, datalog, time series, blob,
//! vector, FTS) is process-global: one `RwLock`ed instance shared by every
//! session, every RESP connection, and the KV fast path. Before M8 a `BEGIN`
//! deep-cloned each of those stores and `ROLLBACK` assigned the clone back
//! **wholesale**, which meant one session's `ROLLBACK` silently destroyed every
//! other session's acknowledged, fsynced writes made since that `BEGIN`.
//!
//! This module replaces the whole-store snapshot/restore with a per-session
//! write-set:
//!
//! * The before-image of a store is captured **lazily**, the first time this
//!   session writes to it — a SQL-only transaction now pays nothing, where it
//!   previously cloned every HNSW graph in the process.
//! * Every mutation records the entities it touched. Stores that the executor
//!   owns exclusively record into an accumulator inside the store itself
//!   (so Cypher writes, which go through the same mutating methods, are covered
//!   structurally); the KV string store, which RESP and the fast path also
//!   write, records at the SQL call site instead so a concurrent RESP write is
//!   never mis-attributed to this session.
//! * `ROLLBACK` reverts **only** the recorded entities, so it can no longer
//!   touch state that belongs to anyone else.
//!
//! The same write-set makes savepoints work: a savepoint captures a second,
//! shallower before-image and its own touched set, so `ROLLBACK TO SAVEPOINT`
//! reverts exactly the cross-model writes made after the savepoint.

use std::collections::{HashMap, HashSet};

use super::Executor;
use super::enlistment::{EnlistedSet, Model, XACT_AUTOCOMMIT};
use super::types::VectorIndexEntry;
use crate::graph::GraphTouched;

/// Before-images and write-set for one nesting level (the transaction itself,
/// or one savepoint within it).
///
/// Each snapshot is `None` until this level's first write to that store, and is
/// captured immediately *before* that write — so it is always the state as of
/// the level's start, however late it is taken.
#[derive(Default)]
pub(super) struct CrossModelLevel {
    pub kv: Option<crate::kv::KvTxnSnapshot>,
    pub kv_touched: HashSet<String>,
    pub graph: Option<crate::graph::GraphTxnSnapshot>,
    pub graph_touched: GraphTouched,
    pub doc: Option<crate::document::DocTxnSnapshot>,
    pub doc_touched: HashSet<u64>,
    pub datalog: Option<crate::datalog::DatalogTxnSnapshot>,
    pub datalog_touched: HashSet<String>,
    pub datalog_rules: Vec<crate::datalog::Rule>,
    pub ts: Option<crate::timeseries::TsTxnSnapshot>,
    pub ts_touched: HashSet<String>,
    pub blob: Option<crate::blob::BlobTxnSnapshot>,
    pub blob_touched: HashSet<String>,
    pub vector: Option<HashMap<String, VectorIndexEntry>>,
    pub vector_touched: HashSet<String>,
    /// Before-image per touched stream: `None` means the stream did not exist
    /// at this level's start, so rolling back removes it. Per-stream rather
    /// than a whole-map clone because a transaction usually writes one stream
    /// and there is no reason to copy the others.
    pub streams: HashMap<String, Option<crate::pubsub::Stream>>,
}

/// Cross-model state for one session's open transaction.
pub(super) struct CrossModelTxn {
    /// This transaction's coordinator id (S63). Minted at BEGIN from the
    /// executor's `next_xact_id`, written into every specialty record the
    /// transaction tags, and vouched for by the COMMIT-record body. It is
    /// NOT the SQL engine's txn id — see `executor::enlistment` for why that
    /// one cannot serve here.
    pub xid: u64,
    /// Which specialty models this transaction has enlisted. Drives the
    /// COMMIT-record body and the S7 checkpoint gate (a SQL-only transaction
    /// never blocks a specialty checkpoint).
    pub enlisted: EnlistedSet,
    /// Before-images and write-set as of `BEGIN`.
    pub base: CrossModelLevel,
    /// Savepoint stack: `(name, level, fts_mark)`.
    pub savepoints: Vec<(String, CrossModelLevel, usize)>,
    /// FTS is already op-scoped rather than snapshot-based; one ordered log
    /// serves every level, with savepoints holding an index into it.
    pub fts_ops: Vec<crate::fts::FtsUndoOp>,
}

impl CrossModelTxn {
    pub fn new(xid: u64) -> Self {
        Self {
            xid,
            enlisted: EnlistedSet::default(),
            base: CrossModelLevel::default(),
            savepoints: Vec::new(),
            fts_ops: Vec::new(),
        }
    }
}

/// Run `$body` against every level that must record a write: the base level
/// and every open savepoint. Outer savepoints must see writes made while an
/// inner savepoint was open, otherwise a `ROLLBACK TO` the outer one would miss
/// them.
macro_rules! for_each_level {
    ($cm:expr, $lvl:ident, $body:block) => {{
        {
            let $lvl = &mut $cm.base;
            $body
        }
        for entry in $cm.savepoints.iter_mut() {
            let $lvl = &mut entry.1;
            $body
        }
    }};
}

impl Executor {
    // ── KV strings ──────────────────────────────────────────────────────────
    // Recorded at the SQL call site: `KvStore` is also written by RESP and the
    // KV fast path, which are autocommit and must never be attributed to an
    // open SQL transaction.

    /// Record that this transaction is about to write `key`, capturing the KV
    /// before-image on first use, and return the coordinating id the WAL
    /// record for that write must carry (S63): the transaction's `xid`
    /// inside an explicit transaction, `XACT_AUTOCOMMIT` outside one.
    /// Folding the enlistment into the touch means one lock acquisition
    /// covers both, and a write path cannot drift between capturing the
    /// before-image and tagging its record. Must be called *before* the
    /// mutation.
    pub(super) fn cross_model_touch_kv(&self, key: &str) -> u64 {
        let session = self.current_session();
        let mut guard = session.cross_model.lock();
        let Some(cm) = guard.as_mut() else {
            return XACT_AUTOCOMMIT;
        };
        cm.enlisted.enlist(Model::Kv);
        for_each_level!(cm, lvl, {
            if lvl.kv.is_none() {
                lvl.kv = Some(self.kv_store.txn_snapshot());
            }
            lvl.kv_touched.insert(key.to_string());
        });
        cm.xid
    }

    /// `FLUSHDB` erases the whole keyspace, so its write-set is every key the
    /// before-image holds. Enlists the KV model for the S7 checkpoint gate;
    /// the flush's own WAL effect is a snapshot (committed by construction,
    /// untaggable — see the S63 design's D3), so unlike the keyed touches
    /// there is no id to hand back.
    pub(super) fn cross_model_touch_kv_all(&self) {
        let session = self.current_session();
        let mut guard = session.cross_model.lock();
        let Some(cm) = guard.as_mut() else { return };
        cm.enlisted.enlist(Model::Kv);
        for_each_level!(cm, lvl, {
            if lvl.kv.is_none() {
                lvl.kv = Some(self.kv_store.txn_snapshot());
            }
            let keys = lvl
                .kv
                .as_ref()
                .map(crate::kv::KvStore::snapshot_keys)
                .unwrap_or_default();
            lvl.kv_touched.extend(keys);
        });
    }

    // ── Stores the executor owns exclusively ────────────────────────────────
    // `*_before` captures the lazy before-image while the caller holds the
    // store's write guard; `*_after` merges the write-set the store recorded.

    pub(super) fn cross_model_before_graph(&self, store: &crate::graph::GraphStore) {
        let session = self.current_session();
        let mut guard = session.cross_model.lock();
        let Some(cm) = guard.as_mut() else { return };
        for_each_level!(cm, lvl, {
            if lvl.graph.is_none() {
                lvl.graph = Some(store.txn_snapshot());
            }
        });
    }

    pub(super) fn cross_model_after_graph(&self, touched: GraphTouched) {
        if touched.is_empty() {
            return;
        }
        let session = self.current_session();
        let mut guard = session.cross_model.lock();
        let Some(cm) = guard.as_mut() else { return };
        for_each_level!(cm, lvl, {
            lvl.graph_touched.merge(touched.clone());
        });
    }

    pub(super) fn cross_model_before_doc(&self, store: &crate::document::DocumentStore) {
        let session = self.current_session();
        let mut guard = session.cross_model.lock();
        let Some(cm) = guard.as_mut() else { return };
        for_each_level!(cm, lvl, {
            if lvl.doc.is_none() {
                lvl.doc = Some(store.txn_snapshot());
            }
        });
    }

    pub(super) fn cross_model_after_doc(&self, touched: HashSet<u64>) {
        if touched.is_empty() {
            return;
        }
        let session = self.current_session();
        let mut guard = session.cross_model.lock();
        let Some(cm) = guard.as_mut() else { return };
        for_each_level!(cm, lvl, {
            lvl.doc_touched.extend(touched.iter().copied());
        });
    }

    pub(super) fn cross_model_before_datalog(&self, store: &crate::datalog::DatalogStore) {
        let session = self.current_session();
        let mut guard = session.cross_model.lock();
        let Some(cm) = guard.as_mut() else { return };
        for_each_level!(cm, lvl, {
            if lvl.datalog.is_none() {
                lvl.datalog = Some(store.txn_snapshot());
            }
        });
    }

    pub(super) fn cross_model_after_datalog(
        &self,
        touched: HashSet<String>,
        rules: Vec<crate::datalog::Rule>,
    ) {
        if touched.is_empty() && rules.is_empty() {
            return;
        }
        let session = self.current_session();
        let mut guard = session.cross_model.lock();
        let Some(cm) = guard.as_mut() else { return };
        for_each_level!(cm, lvl, {
            lvl.datalog_touched.extend(touched.iter().cloned());
            lvl.datalog_rules.extend(rules.iter().cloned());
        });
    }

    pub(super) fn cross_model_before_ts(&self, store: &crate::timeseries::TimeSeriesStore) {
        let session = self.current_session();
        let mut guard = session.cross_model.lock();
        let Some(cm) = guard.as_mut() else { return };
        for_each_level!(cm, lvl, {
            if lvl.ts.is_none() {
                lvl.ts = Some(store.txn_snapshot());
            }
        });
    }

    pub(super) fn cross_model_after_ts(&self, touched: HashSet<String>) {
        if touched.is_empty() {
            return;
        }
        let session = self.current_session();
        let mut guard = session.cross_model.lock();
        let Some(cm) = guard.as_mut() else { return };
        for_each_level!(cm, lvl, {
            lvl.ts_touched.extend(touched.iter().cloned());
        });
    }

    pub(super) fn cross_model_before_blob(&self, store: &crate::blob::BlobStore) {
        let session = self.current_session();
        let mut guard = session.cross_model.lock();
        let Some(cm) = guard.as_mut() else { return };
        for_each_level!(cm, lvl, {
            if lvl.blob.is_none() {
                lvl.blob = Some(store.txn_snapshot());
            }
        });
    }

    pub(super) fn cross_model_after_blob(&self, touched: HashSet<String>) {
        if touched.is_empty() {
            return;
        }
        let session = self.current_session();
        let mut guard = session.cross_model.lock();
        let Some(cm) = guard.as_mut() else { return };
        for_each_level!(cm, lvl, {
            lvl.blob_touched.extend(touched.iter().cloned());
        });
    }

    // ── Vector indexes (owned by the executor, not a store type) ────────────

    /// Record that this transaction is about to replace or drop the vector
    /// index `name`. Must be called *before* the mutation, and while not
    /// holding the `vector_indexes` write guard.
    pub(super) fn cross_model_touch_vector(&self, name: &str) {
        let session = self.current_session();
        let mut guard = session.cross_model.lock();
        let Some(cm) = guard.as_mut() else { return };
        for_each_level!(cm, lvl, {
            if lvl.vector.is_none() {
                lvl.vector = Some(self.vector_indexes.read().clone());
            }
            lvl.vector_touched.insert(name.to_string());
        });
    }

    // ── Streams ─────────────────────────────────────────────────────────────

    /// Record that this transaction is about to modify stream `name`, and
    /// return the coordinating id the WAL record for that write must carry
    /// (S63): the transaction's `xid` inside an explicit transaction,
    /// `XACT_AUTOCOMMIT` outside one. Folding the enlistment into the touch
    /// means one lock acquisition covers both, and a write path cannot drift
    /// between capturing the before-image and tagging its record.
    ///
    /// Must be called BEFORE the mutation and while NOT holding the `streams`
    /// write guard — this takes a read guard, and the reverse order would
    /// deadlock against a concurrent writer.
    ///
    /// Streams were one of the models a transaction could write and never roll
    /// back: `XADD` inside a `BEGIN` stayed in the stream after `ROLLBACK`, so
    /// an aborted transaction still published events that downstream consumers
    /// had already acted on.
    pub(super) fn cross_model_touch_stream(&self, name: &str) -> u64 {
        let session = self.current_session();
        let mut guard = session.cross_model.lock();
        let Some(cm) = guard.as_mut() else {
            return XACT_AUTOCOMMIT;
        };
        cm.enlisted.enlist(Model::Streams);
        // Read the before-image once, outside the per-level loop.
        let before = self.streams.read().get(name).cloned();
        for_each_level!(cm, lvl, {
            lvl.streams
                .entry(name.to_string())
                .or_insert_with(|| before.clone());
        });
        cm.xid
    }

    // ── FTS (already op-scoped) ─────────────────────────────────────────────

    /// Record an FTS mutation for rollback. Unlike the pre-M8 hook this uses a
    /// `parking_lot` mutex rather than `try_write` on the async transaction
    /// lock, so a contended lock can no longer silently drop the undo record
    /// and leave an unrollbackable mutation behind.
    pub(super) fn cross_model_fts_added(&self, doc_id: u64) {
        let session = self.current_session();
        let mut guard = session.cross_model.lock();
        let Some(cm) = guard.as_mut() else { return };
        cm.fts_ops.push(crate::fts::FtsUndoOp::AddedDoc { doc_id });
    }

    /// Capture a document's FTS posting state before it is removed.
    pub(super) fn cross_model_fts_removing(&self, doc_id: u64) {
        let session = self.current_session();
        let mut guard = session.cross_model.lock();
        let Some(cm) = guard.as_mut() else { return };
        let mut log = crate::fts::FtsUndoLog::default();
        self.fts_index.read().record_remove(&mut log, doc_id);
        cm.fts_ops.append(&mut log.ops);
    }

    // ── Applying a rollback ─────────────────────────────────────────────────

    /// Revert every entity recorded in `level`, using its before-images.
    ///
    /// Scoped: entities this session never wrote are untouched, so a concurrent
    /// session's committed writes survive. Durable: each store writes
    /// compensating records into its own WAL as part of the restore, so a crash
    /// after a successful `ROLLBACK` cannot resurrect the reverted writes on
    /// replay (datalog excepted — it has no live WAL to compensate).
    pub(super) fn cross_model_revert(
        &self,
        level: CrossModelLevel,
        fts_ops: Vec<crate::fts::FtsUndoOp>,
    ) {
        if let Some(ref snap) = level.kv {
            self.kv_store.txn_restore_scoped(snap, &level.kv_touched);
        }
        if let Some(ref snap) = level.graph {
            self.graph_store
                .write()
                .txn_restore_scoped(snap, &level.graph_touched);
        }
        if let Some(ref snap) = level.doc {
            self.doc_store
                .write()
                .txn_restore_scoped(snap, &level.doc_touched);
        }
        if let Some(ref snap) = level.datalog {
            let mut store = self.datalog_store.write();
            store.txn_restore_scoped(snap, &level.datalog_touched, &level.datalog_rules);
            // Compensate the WAL, or the rollback is in-memory only.
            //
            // Until 2026-08-17 the datalog WAL was never written at all, so
            // there was nothing to compensate — that was NU-013, and fixing it
            // created this gap: the WAL now holds the appends this rollback
            // just reverted, and replay would bring them back. Checkpointing
            // rewrites the log to the restored state, which is the same
            // approach FTS takes with `fts_index.json` (the file that wins on
            // reopen). Cheaper than inverse records and correct by
            // construction: the log after a rollback IS the state.
            if let Some(ref wal) = self.datalog_wal
                && let Err(e) = wal.checkpoint(&store)
            {
                tracing::error!(
                    target: "nucleus::datalog",
                    "datalog rollback could not compensate the WAL ({e}); a crash before the \
                     next checkpoint could resurrect the rolled-back facts on replay"
                );
            }
        }
        if let Some(ref snap) = level.ts {
            self.ts_store
                .write()
                .txn_restore_scoped(snap, &level.ts_touched);
        }
        if let Some(ref snap) = level.blob {
            self.blob_store
                .write()
                .txn_restore_scoped(snap, &level.blob_touched);
        }
        if let Some(ref snap) = level.vector {
            let mut live = self.vector_indexes.write();
            for name in &level.vector_touched {
                match snap.get(name) {
                    Some(entry) => {
                        live.insert(name.clone(), entry.clone());
                    }
                    None => {
                        live.remove(name);
                    }
                }
            }
        }
        if !level.streams.is_empty() {
            let mut live = self.streams.write();
            for (name, before) in &level.streams {
                match before {
                    Some(stream) => {
                        live.insert(name.clone(), stream.clone());
                    }
                    // The stream did not exist when this level started, so the
                    // transaction created it; roll that back too.
                    None => {
                        live.remove(name);
                    }
                }
            }
            // Compensate the WAL, or the rollback is in-memory only (S31-04).
            //
            // `STREAM_XADD` appends its record inside the transaction and
            // `log_xadd` ends in `write_all` + `flush`, so the record is in the
            // kernel before ROLLBACK is even parsed. Reverting only `self.streams`
            // left that record in the log: the aborted entry read back as absent
            // and then came BACK on the next restart, because replay re-applied
            // it. A graceful restart was enough — nothing on the shutdown path
            // checkpoints this log, only the 300 s timer does — so the
            // resurrection window closed only by luck. Publishing an event that
            // never happened is worse than losing a write, because consumers may
            // already have acted on it.
            //
            // Checkpointing rewrites the log from the just-restored live state,
            // which is the approach datalog and FTS take above: cheaper than
            // inverse records and correct by construction, since the log after a
            // rollback IS the state. The snapshot carries consumer groups too, so
            // a rolled-back XREADGROUP cursor advance is compensated with it.
            // Rollbacks that touch a stream are rare; stream writes are not.
            if let Some(ref wal) = self.streams_wal
                && let Err(e) = wal.checkpoint(&live)
            {
                tracing::error!(
                    target: "nucleus::streams",
                    "streams rollback could not compensate the WAL ({e}); a restart before the \
                     next checkpoint could resurrect the rolled-back stream entries on replay"
                );
            }
        }
        if !fts_ops.is_empty() {
            self.fts_index
                .write()
                .undo(crate::fts::FtsUndoLog { ops: fts_ops });
            // This checkpoint STAYS, unlike the ones on the write path. A
            // rollback is not logged to the FTS WAL — the undo is applied in
            // memory — so without rewriting the checkpoint here, a crash after
            // a successful ROLLBACK would replay the rolled-back writes from
            // the tail and resurrect them. Rollbacks are rare; writes are not.
            #[cfg(feature = "server")]
            self.save_fts_index();
        }
    }
}
