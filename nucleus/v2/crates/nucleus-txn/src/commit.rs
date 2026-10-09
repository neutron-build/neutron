//! C-T0 §3: the commit pipeline and visibility, plus §7.1 abort.
//!
//! One commit thread owns the sequencer. Sessions submit a
//! [`CommitRequest`] on an unbounded `std::sync::mpsc` channel and block on a
//! per-request ack ([`Core::commit`]). [`CommitPipeline::process_group`]
//! runs §3 steps 1–5 for one drained group, so every step is drivable
//! step-by-step for the deterministic simulator:
//!
//! 1. assign `commit_ts` in channel order, reserving `/sys/ts_hwm` in blocks
//!    (`[TS_HWM_BLOCK]`, shrinkable for tests via
//!    [`CommitPipeline::set_hwm_block`]) written in the same batch as the
//!    first commit that needs it;
//! 2. write the records in `commit_ts` order, one batch per request,
//!    `Durability::No` (WAL order equals `commit_ts` order, I-WAL-ORDER);
//! 3. `CommitObserver::on_assigned` for `ssi` requests, **before any status
//!    is set** (§8.5's writer map; C-T3's hook, [`NoObserver`] default);
//! 4. the `synchronous_commit=off` prefix up to the first `on` request: set
//!    status, advance `visible_ts`, ack; then one `sync_wal` (never on a
//!    caller thread); then the rest in order (I-VIS, I-ACK);
//! 5. for each committed txn, after `visible_ts >= commit_ts`:
//!    `ReleaseHook::release_all`, bump the wake generation and wake waiters,
//!    queue for resolution, mark released.
//!
//! A KV error is fail-stop: the [`FailStop`] hook runs (default:
//! `std::process::abort`), the pipeline stops, and every request in the
//! group that has not been acked yet gets an error ack. The thread never
//! aborts a txn and never fills a ts hole by skipping. `/sys/ts_clock`
//! samples go through an injected [`Clock`], at most one per clock second,
//! in the commit batch.

use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{self, Receiver, RecvError, Sender, TryRecvError};
use std::sync::{Arc, MutexGuard, PoisonError};

use nucleus_kv::{Batch, Durability, OrderedKv};

use crate::boot::Core;
use crate::encoding::{sys_log_key, sys_ts_clock_key, sys_ts_hwm_key, sys_txn_key};
use crate::txn::{Isolation, Txn};
use crate::{Ts, TxnError, TxnId};

/// `/sys/ts_hwm` is reserved in blocks of this many timestamps (§3 step 1).
/// The production default; tests and the simulator shrink it through
/// [`CommitPipeline::set_hwm_block`].
pub const TS_HWM_BLOCK: u64 = 1024;

/// What a commit ack carries: the `commit_ts`, or the fail-stop error that
/// stopped the group before this request was ackged.
pub type CommitAck = Result<Ts, TxnError>;

/// `synchronous_commit` of one request (§3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncCommit {
    On,
    Off,
}

/// What goes down the commit channel.
pub enum CommitMsg {
    Request(CommitRequest),
    /// Drains what precedes it, then stops the commit thread.
    Stop,
}

/// One commit request (§3): the status record target, the optional `/log`
/// record, the sync mode, the SSI marker, and the write-set keys (step 5
/// queues resolution from them; the caller may drop its `Txn` as soon as
/// the ack arrives). Build it with [`CommitRequest::new`], which also
/// returns the ack receiver the caller blocks on.
pub struct CommitRequest {
    pub txn: TxnId,
    pub sync: SyncCommit,
    pub log: Option<Vec<u8>>,
    /// A SerIALIZABLE txn's request: step 3 reports it to the
    /// [`CommitObserver`] before any status is set (§8.4/§8.5).
    pub ssi: bool,
    /// The distinct keys of the txn's write set: what step 5 queues for
    /// resolution (§7.1 queues the same for abort cleanup).
    pub keys: Vec<nucleus_kv::Key>,
    ack: Sender<CommitAck>,
}

impl CommitRequest {
    /// The request and the ack receiver the caller blocks on.
    pub fn new(
        txn: TxnId,
        sync: SyncCommit,
        log: Option<Vec<u8>>,
        ssi: bool,
        keys: Vec<nucleus_kv::Key>,
    ) -> (CommitRequest, mpsc::Receiver<CommitAck>) {
        let (ack_tx, ack_rx) = mpsc::channel();
        (
            CommitRequest {
                txn,
                sync,
                log,
                ssi,
                keys,
                ack: ack_tx,
            },
            ack_rx,
        )
    }

    fn ack(&self, r: CommitAck) {
        // A dropped receiver means the caller went away; the commit
        // stands (§3: once on the channel, it commits).
        let _ = self.ack.send(r);
    }
}

/// Fail-stop hook (§3): a KV error on the commit thread (or reported by the
/// background resolver). The default aborts the process; tests install a
/// panicking or recording one.
pub trait FailStop: Send + Sync {
    fn on_kv_error(&self, err: &TxnError);
}

/// The default [`FailStop`]: `std::process::abort`.
pub struct AbortFailStop;

impl FailStop for AbortFailStop {
    fn on_kv_error(&self, _err: &TxnError) {
        std::process::abort();
    }
}

/// The C-T3 hook (§3 step 3, §8.5): `commit_ts -> TxnId` into the SSI writer
/// map, before any status is set. [`NoObserver`] is the default.
pub trait CommitObserver: Send + Sync {
    fn on_assigned(&self, txn: TxnId, ts: Ts);
}

/// The no-op [`CommitObserver`].
#[derive(Default)]
pub struct NoObserver;

impl CommitObserver for NoObserver {
    fn on_assigned(&self, _txn: TxnId, _ts: Ts) {}
}

/// Wall clock for `/sys/ts_clock` samples (§2.3): injectable so tests and
/// the simulator control "one sample per second".
pub trait Clock: Send + Sync {
    /// Wall time now, in seconds.
    fn now_secs(&self) -> u64;
}

/// The real-time [`Clock`].
#[derive(Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_secs(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }
}

/// The C-T2b hook (§3 step 5, §7.1): release a txn's in-memory locks
/// (shared row locks, relation locks, advisory locks, deferrable-unique
/// prefix waits). No-op by default; installed on the core through
/// [`Core::set_release_hook`](crate::boot::Core::set_release_hook).
pub trait ReleaseHook: Send + Sync {
    fn release_all(&self, txn: TxnId);
}

/// The no-op [`ReleaseHook`].
#[derive(Default)]
pub struct NoReleaseHook;

impl ReleaseHook for NoReleaseHook {
    fn release_all(&self, _txn: TxnId) {}
}

/// One queued resolution/cleanup entry (§3 step 5, §7.1): the txn and the
/// keys of its write set. Crate-private: only the pipeline, abort and the
/// resolver produce and consume these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolveEntry {
    pub txn: TxnId,
    pub keys: Vec<nucleus_kv::Key>,
    pub mode: crate::removal::RemovalMode,
}

/// Injection points for a spawned commit thread (and, through
/// [`Core`]'s fail-stop slot, for the background resolver): all optional,
/// defaults as documented on each hook.
#[derive(Default)]
pub struct CommitConfig {
    /// The fail-stop hook; default [`AbortFailStop`].
    pub fail_stop: Option<Arc<dyn FailStop>>,
    /// The SSI writer-map observer; default [`NoObserver`].
    pub observer: Option<Arc<dyn CommitObserver>>,
    /// The `/sys/ts_clock` clock; default [`SystemClock`].
    pub clock: Option<Arc<dyn Clock>>,
}

impl CommitConfig {
    pub fn new() -> CommitConfig {
        CommitConfig::default()
    }

    pub fn with_fail_stop(mut self, hook: Arc<dyn FailStop>) -> CommitConfig {
        self.fail_stop = Some(hook);
        self
    }

    pub fn with_observer(mut self, observer: Arc<dyn CommitObserver>) -> CommitConfig {
        self.observer = Some(observer);
        self
    }

    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> CommitConfig {
        self.clock = Some(clock);
        self
    }
}

impl<K: OrderedKv> Core<K> {
    /// The commit API (§3): enqueues the request, blocks for the ack and
    /// returns `commit_ts`. Consumes the txn: after a commit (or a
    /// fail-stop error ack) the caller has no further use for it.
    ///
    /// A txn with no writes, and not SERIALIZABLE, commits without a ts: it
    /// returns [`Ts::ZERO`], writes nothing and is released immediately, as
    /// in PostgreSQL where a read-only txn gets no xid. Its release hook
    /// still runs (a lock-only txn may hold shared locks, §6).
    pub fn commit(&self, txn: Txn, sync: SyncCommit) -> Result<Ts, TxnError> {
        if txn.write_set_keys().is_empty() && txn.isolation != Isolation::Serializable {
            self.release_hook().release_all(txn.id);
            self.bump_and_wake(txn.id)?;
            self.status.mark_released(txn.id)?;
            return Ok(Ts::ZERO);
        }
        let ssi = txn.isolation == Isolation::Serializable;
        let (req, ack) = CommitRequest::new(txn.id, sync, None, ssi, txn.write_set_keys());
        self.submit(req)?;
        match ack.recv() {
            Ok(Ok(ts)) => Ok(ts),
            Ok(Err(e)) => Err(e),
            Err(RecvError) => Err(TxnError::Invariant(format!(
                "commit of {:?} never acked: the commit pipeline stopped (fail-stop)",
                txn.id
            ))),
        }
    }

    /// §7.1: the owning session, outside any latch, sets `Aborted`, releases
    /// its in-memory locks, bumps its wake generation and wakes waiters,
    /// marks itself released, then queues an async cleanup of its intents,
    /// found through its write-set log. Nothing is persisted. Consumes the
    /// txn: only the owning session calls it, at most once.
    pub fn abort(&self, txn: Txn) -> Result<(), TxnError> {
        self.status.set_aborted(txn.id)?;
        self.release_hook().release_all(txn.id);
        self.bump_and_wake(txn.id)?;
        self.status.mark_released(txn.id)?;
        let keys = txn.write_set_keys();
        if !keys.is_empty() {
            let mut q = self.lock_resolve_queue();
            q.push_back(ResolveEntry {
                txn: txn.id,
                keys,
                mode: crate::removal::RemovalMode::Discard,
            });
        }
        Ok(())
    }
}

/// The commit pipeline: the sequencer (`next_ts`), the reserved `/sys/ts_hwm`
/// mirror, the injected hooks and the channel's receiving end.
///
/// `process_group` is the step-by-step primitive; [`spawn_commit_thread`]
/// runs the greedy drain loop in a dedicated std thread (fsync never runs on
/// a caller thread).
pub struct CommitPipeline<K: OrderedKv> {
    core: Arc<Core<K>>,
    rx: Receiver<CommitMsg>,
    next_ts: Ts,
    /// The reserved (persisted) `/sys/ts_hwm` value, mirrored from the boot
    /// read in [`Core`] (`Core::ts_hwm`): one source, no re-reads.
    hwm: Ts,
    /// The reservation block size ([`TS_HWM_BLOCK`] by default).
    hwm_block: u64,
    observer: Arc<dyn CommitObserver>,
    clock: Arc<dyn Clock>,
    /// The clock second of the last `/sys/ts_clock` sample.
    last_clock_sample: Option<u64>,
    stopped: std::sync::atomic::AtomicBool,
}

impl<K: OrderedKv> CommitPipeline<K> {
    /// Attaches a pipeline to `core` (its channel is what
    /// [`Core::commit`](crate::boot::Core::commit) sends on) without
    /// spawning a thread: the simulator drives `process_group` itself.
    /// `next_ts = ts_hwm + 1` (§3 boot). Errors if `core` already has a
    /// pipeline attached: one sequencer per core.
    pub fn new(core: Arc<Core<K>>) -> Result<CommitPipeline<K>, TxnError> {
        let (tx, rx) = mpsc::channel();
        core.try_set_commit_sender(tx)?;
        let hwm = core.ts_hwm();
        Ok(CommitPipeline {
            core,
            rx,
            next_ts: Ts(hwm.0 + 1),
            hwm,
            hwm_block: TS_HWM_BLOCK,
            observer: Arc::new(NoObserver),
            clock: Arc::new(SystemClock),
            last_clock_sample: None,
            stopped: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// [`CommitPipeline::new`] with a [`CommitConfig`]: the fail-stop hook
    /// is installed on the core (the resolver reports through it too), the
    /// observer and clock on the pipeline.
    pub fn with_config(
        core: Arc<Core<K>>,
        config: CommitConfig,
    ) -> Result<CommitPipeline<K>, TxnError> {
        if let Some(f) = &config.fail_stop {
            core.set_fail_stop(Arc::clone(f));
        }
        let mut pipeline = CommitPipeline::new(Arc::clone(&core))?;
        if let Some(o) = config.observer {
            pipeline.observer = o;
        }
        if let Some(c) = config.clock {
            pipeline.clock = c;
        }
        Ok(pipeline)
    }

    pub fn set_observer(&mut self, observer: Arc<dyn CommitObserver>) {
        self.observer = observer;
    }

    pub fn set_clock(&mut self, clock: Arc<dyn Clock>) {
        self.clock = clock;
    }

    /// Shrinks the `/sys/ts_hwm` reservation block (tests and the
    /// simulator, so a reservation boundary is reachable with few commits).
    /// The default [`TS_HWM_BLOCK`] = 1024 is the production value; values
    /// below 1 are clamped to 1.
    pub fn set_hwm_block(&mut self, block: u64) {
        self.hwm_block = block.max(1);
    }

    /// The next ts the sequencer will assign.
    pub fn next_ts(&self) -> Ts {
        self.next_ts
    }

    /// Drains every request currently on the channel into one group.
    pub fn drain_available(&mut self) -> Vec<CommitRequest> {
        let mut group = Vec::new();
        loop {
            match self.rx.try_recv() {
                Ok(CommitMsg::Request(r)) => group.push(r),
                Ok(CommitMsg::Stop) => {
                    // Process what precedes the Stop, then stop.
                    self.process_group(group);
                    self.stopped.store(true, Ordering::SeqCst);
                    return Vec::new();
                }
                Err(TryRecvError::Empty) => return group,
                Err(TryRecvError::Disconnected) => {
                    // The senders are gone; the partly built group still
                    // gets processed before the pipeline stops.
                    self.process_group(group);
                    self.stopped.store(true, Ordering::SeqCst);
                    return Vec::new();
                }
            }
        }
    }

    /// §3 steps 1–5 for one drained group, in order. On any error the group
    /// stops exactly where it failed: no later record write, no later
    /// `visible_ts` advance, no sync, no step 5, and every request that has
    /// not been acked yet gets an error ack (§3 fail-stop; the thread never
    /// aborts a txn and never fills a ts hole by skipping).
    pub fn process_group(&mut self, group: Vec<CommitRequest>) {
        if self.load_stopped() || group.is_empty() {
            return;
        }
        // Step 1: assign commit_ts in channel order (never skip, never fill
        // a hole).
        let mut ts_of = Vec::with_capacity(group.len());
        for _ in &group {
            let Some(next) = self.next_ts.0.checked_add(1) else {
                self.fail_stop(&TxnError::Invariant("commit ts space exhausted".into()));
                return;
            };
            ts_of.push(self.next_ts);
            self.next_ts = Ts(next);
        }
        // Step 2: write the records in commit_ts order, one batch per
        // request, Durability::No. The first batch whose ts exceeds the
        // reserved hwm carries `/sys/ts_hwm += block` (same batch).
        for (i, req) in group.iter().enumerate() {
            let ts = ts_of[i];
            let mut batch = Batch::default();
            if ts > self.hwm {
                let Some(hwm) = self.hwm.0.checked_add(self.hwm_block) else {
                    self.fail_stop(&TxnError::Invariant("ts_hwm space exhausted".into()));
                    return;
                };
                self.hwm = Ts(hwm);
                batch.put(sys_ts_hwm_key(), hwm.to_be_bytes().to_vec());
            }
            batch.put(sys_txn_key(req.txn), ts.0.to_be_bytes().to_vec());
            if let Some(log) = &req.log {
                batch.put(sys_log_key(req.txn), log.clone());
            }
            // /sys/ts_clock: at most one sample per clock second, in the
            // commit batch (§2.3).
            let now = self.clock.now_secs();
            if self.last_clock_sample != Some(now) {
                batch.put(sys_ts_clock_key(ts), now.to_be_bytes().to_vec());
                self.last_clock_sample = Some(now);
            }
            if let Err(e) = self.core.write(batch, Durability::No) {
                // Nothing in the group has been acked yet (acks are step
                // 4): every request gets the error ack.
                self.stop_group(&group, 0, e);
                return;
            }
        }
        // Step 3: the SSI writer map, before any status is set (§8.5).
        for (i, req) in group.iter().enumerate() {
            if req.ssi {
                self.observer.on_assigned(req.txn, ts_of[i]);
            }
        }
        // Step 4: the off prefix (status, visible_ts, ack), then one fsync,
        // then the rest in order.
        let split = group
            .iter()
            .position(|r| r.sync == SyncCommit::On)
            .unwrap_or(group.len());
        for i in 0..split {
            if let Err(e) = self.make_visible(&group[i], ts_of[i]) {
                self.stop_group(&group, i, e);
                return;
            }
        }
        if split < group.len() {
            // The one fsync of the group (§3 step 4). This thread is the
            // commit thread, never a caller thread.
            if let Err(e) = self.core.sync_wal() {
                self.stop_group(&group, split, e);
                return;
            }
            for i in split..group.len() {
                if let Err(e) = self.make_visible(&group[i], ts_of[i]) {
                    self.stop_group(&group, i, e);
                    return;
                }
            }
        }
        // Step 5: for each committed txn, after visible_ts >= commit_ts
        // (step 4 guaranteed it): release locks, bump+wake, queue for
        // resolution, mark released. Every request was ackged in step 4, so
        // a failure here is fail-stop only.
        for (i, req) in group.iter().enumerate() {
            debug_assert!(
                self.core.visible_ts() >= ts_of[i],
                "step 5 before visibility"
            );
            if let Err(e) = self.step5(req.txn, &req.keys) {
                self.fail_stop(&e);
                return;
            }
        }
    }

    /// §3 step 4 for one request: in-memory status `Committed(ts)`, then
    /// advance `visible_ts` (I-VIS), then ack (I-ACK). On error nothing has
    /// been ackged; the caller stops the group.
    fn make_visible(&self, req: &CommitRequest, ts: Ts) -> Result<(), TxnError> {
        self.core.status.set_committed(req.txn, ts)?;
        self.core.advance_visible_ts(ts);
        req.ack(Ok(ts));
        Ok(())
    }

    /// §3 step 5 for one txn. Bumps the wake generation **then** wakes the
    /// waiters (§6).
    fn step5(&self, txn: TxnId, keys: &[nucleus_kv::Key]) -> Result<(), TxnError> {
        self.core.release_hook().release_all(txn);
        self.core.bump_and_wake(txn)?;
        self.core.queue_resolution(txn, keys);
        self.core.status.mark_released(txn)
    }

    /// Fail-stop mid-group (§3): stop, run the hook, and error-ack every
    /// request from `from` (the first one not yet ackged) to the end.
    fn stop_group(&self, group: &[CommitRequest], from: usize, err: TxnError) {
        self.fail_stop(&err);
        for req in &group[from..] {
            req.ack(Err(err.clone()));
        }
    }

    fn fail_stop(&self, err: &TxnError) {
        self.stopped.store(true, Ordering::SeqCst);
        self.core.fail_stop().on_kv_error(err);
    }

    fn load_stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }
}

/// Spawns the commit thread: drains greedily (recv, then try_recv until
/// empty) and processes each group. [`CommitThreadHandle::shutdown`] stops
/// it after draining what precedes the stop. A thread-spawn failure is an
/// error, not a panic.
pub fn spawn_commit_thread<K: OrderedKv>(
    core: Arc<Core<K>>,
) -> Result<CommitThreadHandle, TxnError> {
    spawn_commit_thread_with(core, CommitConfig::default())
}

/// [`spawn_commit_thread`] with injected hooks (see [`CommitConfig`]).
pub fn spawn_commit_thread_with<K: OrderedKv>(
    core: Arc<Core<K>>,
    config: CommitConfig,
) -> Result<CommitThreadHandle, TxnError> {
    let mut pipeline = CommitPipeline::with_config(Arc::clone(&core), config)?;
    let stop_tx = core.commit_sender_clone();
    let thread = std::thread::Builder::new()
        .name("nucleus-commit".into())
        .spawn(move || pipeline.run())
        .map_err(|e| TxnError::Invariant(format!("failed to spawn the commit thread: {e}")))?;
    Ok(CommitThreadHandle {
        stop_tx,
        thread: Some(thread),
    })
}

impl<K: OrderedKv> CommitPipeline<K> {
    fn run(&mut self) {
        loop {
            match self.rx.recv() {
                Ok(CommitMsg::Request(r)) => {
                    let mut group = vec![r];
                    loop {
                        match self.rx.try_recv() {
                            Ok(CommitMsg::Request(r)) => group.push(r),
                            Ok(CommitMsg::Stop) => {
                                self.process_group(group);
                                self.stopped.store(true, Ordering::SeqCst);
                                return;
                            }
                            Err(TryRecvError::Empty) => break,
                            Err(TryRecvError::Disconnected) => {
                                // Process the partly built group before
                                // exiting.
                                self.process_group(group);
                                self.stopped.store(true, Ordering::SeqCst);
                                return;
                            }
                        }
                    }
                    self.process_group(group);
                    if self.load_stopped() {
                        return;
                    }
                }
                Ok(CommitMsg::Stop) | Err(RecvError) => {
                    self.stopped.store(true, Ordering::SeqCst);
                    return;
                }
            }
        }
    }
}

/// Handle to a spawned commit thread.
pub struct CommitThreadHandle {
    stop_tx: Option<Sender<CommitMsg>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl CommitThreadHandle {
    /// Stops the commit thread after it processes everything that precedes
    /// the stop request, then joins it. A panic inside the thread (e.g. a
    /// panicking fail-stop hook) surfaces here.
    pub fn shutdown(mut self) -> Result<(), TxnError> {
        if let Some(stop) = self.stop_tx.take() {
            let _ = stop.send(CommitMsg::Stop);
        }
        match self.thread.take() {
            Some(t) => t
                .join()
                .map_err(|_| TxnError::Invariant("commit thread panicked".into())),
            None => Ok(()),
        }
    }
}

impl Drop for CommitThreadHandle {
    fn drop(&mut self) {
        // Not shutting down explicitly would park the thread forever on
        // recv; stop it even when the handle is dropped unused.
        if let Some(stop) = self.stop_tx.take() {
            let _ = stop.send(CommitMsg::Stop);
        }
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

impl<K: OrderedKv> Core<K> {
    /// Puts one request on the commit channel without waiting: the
    /// step-by-step path for tests and the deterministic simulator
    /// (submit, then [`CommitPipeline::drain_available`] +
    /// [`CommitPipeline::process_group`]). [`Core::commit`] is the session
    /// API; this is the manual one.
    pub fn submit(&self, req: CommitRequest) -> Result<(), TxnError> {
        self.send_commit(req)
    }

    /// Sends one request on the commit channel.
    pub(crate) fn send_commit(&self, req: CommitRequest) -> Result<(), TxnError> {
        let tx = {
            let guard = self
                .commit_tx
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            guard.clone()
        };
        match tx {
            Some(tx) => tx
                .send(CommitMsg::Request(req))
                .map_err(|_| TxnError::Invariant("commit pipeline stopped".into())),
            None => Err(TxnError::Invariant("no commit pipeline attached".into())),
        }
    }

    pub(crate) fn try_set_commit_sender(&self, tx: Sender<CommitMsg>) -> Result<(), TxnError> {
        let mut guard = self
            .commit_tx
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if guard.is_some() {
            return Err(TxnError::Invariant(
                "a commit pipeline is already attached to this core".into(),
            ));
        }
        *guard = Some(tx);
        Ok(())
    }

    pub(crate) fn commit_sender_clone(&self) -> Option<Sender<CommitMsg>> {
        self.commit_tx
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn fail_stop(&self) -> Arc<dyn FailStop> {
        Arc::clone(
            &self
                .fail_stop
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }

    pub(crate) fn set_fail_stop(&self, hook: Arc<dyn FailStop>) {
        *self
            .fail_stop
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = hook;
    }

    pub(crate) fn release_hook(&self) -> Arc<dyn ReleaseHook> {
        Arc::clone(
            &self
                .release_hook
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }

    /// Installs the release hook (C-T2b's seam; no-op until then).
    pub fn set_release_hook(&self, hook: Arc<dyn ReleaseHook>) {
        *self
            .release_hook
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = hook;
    }

    /// §3 step 5: queue `txn` for async resolution with the write-set keys
    /// carried by its commit request.
    pub(crate) fn queue_resolution(&self, txn: TxnId, keys: &[nucleus_kv::Key]) {
        if !keys.is_empty() {
            self.lock_resolve_queue().push_back(ResolveEntry {
                txn,
                keys: keys.to_vec(),
                mode: crate::removal::RemovalMode::Resolve,
            });
        }
    }

    pub(crate) fn lock_resolve_queue(&self) -> MutexGuard<'_, VecDeque<ResolveEntry>> {
        self.resolve_q
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}
