//! Shared support for the C-T3 SSI tests: a hand-driven core + SSI + commit
//! pipeline, and the read/write/commit steps of a SERIALIZABLE txn. Each
//! test binary uses a different subset.
#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use nucleus_kv::{Key, MemKv};
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{CommitConfig, CommitObserver, CommitPipeline, CommitTicket, SyncCommit};
use nucleus_txn::registry::SnapshotGuard;
use nucleus_txn::ssi::Ssi;
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::write::{
    CommittedVersion, Epq, EpqDecision, RowOp, RowOutcome, StmtCtx, UniqueRule,
};
use nucleus_txn::{Ts, TxnError, TxnId};

/// RR/SER never run EPQ; RC callers here never need it.
pub struct NoEpq;

impl Epq for NoEpq {
    fn recheck(&mut self, _newest: &CommittedVersion) -> EpqDecision {
        EpqDecision::Skip
    }
}

/// A core with SSI installed and a pipeline driven by hand.
pub struct Rig {
    pub core: Arc<Core<MemKv>>,
    pub ssi: Arc<Ssi>,
    pipeline: Mutex<CommitPipeline<MemKv>>,
}

/// A SERIALIZABLE txn with its registered snapshot.
pub struct Ser<'a> {
    pub txn: Txn,
    pub s: Ts,
    guard: SnapshotGuard<'a>,
}

impl Ser<'_> {
    pub fn id(&self) -> TxnId {
        self.txn.id
    }

    fn ctx(&self) -> ReadCtx {
        ReadCtx {
            txn: self.txn.id,
            snapshot: self.s,
            stmt_seq: self.txn.next_seq().expect("seq"),
        }
    }
}

impl Rig {
    pub fn new() -> Rig {
        Rig::with_observer(|ssi, _core| ssi.commit_observer())
    }

    /// The pipeline's observer is built from the installed SSI (seed 28
    /// wraps it).
    pub fn with_observer(
        f: impl FnOnce(&Arc<Ssi>, &Arc<Core<MemKv>>) -> Arc<dyn CommitObserver>,
    ) -> Rig {
        let core = Arc::new(Core::open(MemKv::new()).expect("open"));
        let ssi = Ssi::install(&core);
        let observer = f(&ssi, &core);
        let pipeline = CommitPipeline::with_config(
            Arc::clone(&core),
            CommitConfig::new().with_observer(observer),
        )
        .expect("pipeline");
        Rig {
            core,
            ssi,
            pipeline: Mutex::new(pipeline),
        }
    }

    /// Drains the channel into one group and runs §3 steps 1-5 on it.
    pub fn process(&self) -> usize {
        let mut p = self.pipeline.lock().expect("pipeline");
        let group = p.drain_available();
        let n = group.len();
        p.process_group(group);
        n
    }

    /// Inserts `key = value` in its own RC txn and commits it.
    pub fn preload(&self, key: &[u8], value: &[u8]) -> Ts {
        let txn = self.core.begin(Isolation::ReadCommitted);
        let seq = txn.next_seq().expect("seq");
        self.core
            .insert_key(
                &txn,
                key,
                None,
                value.to_vec(),
                StmtCtx::new(self.core.visible_ts(), seq, seq),
                UniqueRule::Unique { same_row: None },
            )
            .expect("preload insert");
        let ticket = self
            .core
            .commit_submit(txn, SyncCommit::On)
            .expect("submit");
        self.process();
        ticket.wait().expect("preload ack")
    }

    pub fn ser(&self, read_only: bool) -> Ser<'_> {
        let txn = self.core.begin(Isolation::Serializable);
        let guard = self
            .ssi
            .begin(&self.core, &txn, read_only)
            .expect("ssi begin");
        Ser {
            s: guard.ts(),
            txn,
            guard,
        }
    }

    pub fn read(&self, t: &Ser<'_>, key: &[u8]) -> Option<Vec<u8>> {
        self.ssi
            .read_key(&self.core, t.id(), key, &t.ctx())
            .expect("ssi read")
    }

    pub fn scan(&self, t: &Ser<'_>, lo: &[u8], hi: &[u8]) -> Vec<(Key, Vec<u8>)> {
        self.ssi
            .scan(&self.core, t.id(), lo, hi, &t.ctx())
            .expect("ssi scan")
    }

    pub fn try_update(
        &self,
        t: &Ser<'_>,
        key: &[u8],
        value: &[u8],
    ) -> Result<RowOutcome, TxnError> {
        let seq = t.txn.next_seq().expect("seq");
        self.core.row_op(
            &t.txn,
            key,
            None,
            RowOp::Update {
                value: value.to_vec(),
                key_cols_changed: false,
            },
            StmtCtx::new(t.s, seq, seq),
            &mut NoEpq,
        )
    }

    pub fn update(&self, t: &Ser<'_>, key: &[u8], value: &[u8]) {
        assert_eq!(
            self.try_update(t, key, value).expect("update"),
            RowOutcome::Applied
        );
    }

    pub fn insert(&self, t: &Ser<'_>, key: &[u8], value: &[u8]) {
        let seq = t.txn.next_seq().expect("seq");
        self.core
            .insert_key(
                &t.txn,
                key,
                None,
                value.to_vec(),
                StmtCtx::new(t.s, seq, seq),
                UniqueRule::Unique { same_row: None },
            )
            .expect("insert");
    }

    /// Pre-commit + enqueue only: the txn is prepared, not yet assigned.
    pub fn submit(&self, t: Ser<'_>) -> Result<CommitTicket, TxnError> {
        let Ser { txn, guard, .. } = t;
        let r = self.core.commit_submit(txn, SyncCommit::On);
        drop(guard);
        r
    }

    /// Submit, process the group, wait for the ack.
    pub fn commit(&self, t: Ser<'_>) -> Result<Ts, TxnError> {
        let ticket = self.submit(t)?;
        self.process();
        ticket.wait()
    }

    pub fn abort(&self, t: Ser<'_>) {
        let Ser { txn, guard, .. } = t;
        self.core.abort(txn).expect("abort");
        drop(guard);
    }

    pub fn has_edge(&self, from: TxnId, to: TxnId) -> bool {
        self.ssi.edges().contains(&(from, to))
    }
}

impl Default for Rig {
    fn default() -> Self {
        Rig::new()
    }
}
