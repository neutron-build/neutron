//! C-R0b (card C-R0ba): the **blocking contract** — thread roles for the
//! future async facade, classified entry points, and the executor-thread
//! check that enforces both today, in std code, with no async runtime.
//!
//! Every `nucleus-txn` entry point that can block falls into one of four
//! classes ([`Blocking`]). The check ([`check`]) sits where every blocking
//! call must pass — the KV access funnels in `Core` and the registry
//! (`Registry::open_view` for opening a view, and the `ViewGuard::get` /
//! `scan` / `as_snap` read funnel for reading through one that is already
//! open, wherever it was opened), the park funnels in `wait.rs`, and the
//! entry points whose fast path does no I/O — and a
//! thread marked as an **executor thread** that reaches one panics before
//! the call changes any state. Unmarked threads behave exactly as before:
//! that covers every existing test, the commit thread, the simulator and
//! the background jobs.
//!
//! # The contract
//!
//! - **R1**: executor threads may call only non-blocking ("N") entry
//!   points. Everything else — KvIo, Fsync, Park, Join — is a bug in the
//!   caller, and [`check`] turns it into a panic at the boundary. A
//!   refused [`Core::commit`](crate::boot::Core::commit) consumes and
//!   leaks the `Txn` (it stays `Pending`, with its intents and locks,
//!   until restart), so the facade must not rely on a refused commit
//!   being recoverable.
//! - **R2**: Park-class calls never occupy a bounded pool. The facade runs
//!   statements on a pool with at least one thread per active statement,
//!   or drives the step API with a waker-backed [`Parker`](crate::wait::Parker).
//! - **R3** (fsync isolation): no lock that another session's N- or
//!   KvIo-class operation can need is held across an fsync. C-R0bb gates
//!   this.
//! - **R4**: a backend never holds its write path across an fsync. This is
//!   a C-B0 requirement.
//! - **R5**: an Fsync-class wait is not interruptible. Cancel and timeouts
//!   are observed at the next check point, as in PostgreSQL.
//!
//! # Why a panic and not an error
//!
//! - [`Registry::open_view`](crate::registry::Registry::open_view),
//!   [`WaitHandle::park`](crate::wait::WaitHandle::park) and `Drop` have
//!   no error channel.
//! - It is the same choice tokio makes for a nested `block_on`.
//! - It fires before the call changes any state: every session-facing
//!   operation reads before it writes, and the C-R0ba tests pin this.
//! - Background jobs run on std threads and are never marked.
//!
//! # The classification table
//!
//! | Class | Entry points |
//! |---|---|
//! | N | `Core::begin`; `Txn::*` (`next_seq`, `seq`, `log_write`, `queue_event`, `take_events`, `cancel_handle`); `CancelHandle::*`; `Core::visible_ts` / `epoch` / `ts_hwm`; `Registry::take_snapshot` / `register_at` / `published_w`; `Core::commit_submit`; `CommitTicket::try_ack`; `CommitTicket::wait` on an already-answered ticket; `Core::abort`; `Core::wait_begin` / `wait_poll` / `wait_deadlock_check` / `wait_end` / `bump_and_wake`; `lock_relation` / `advisory_xact_lock` with `LockWait::NoWait` or `SkipLocked`; in-memory `Ssi` hooks (`map_storage`, `unmap_storage`, `note_retired`, `check_doomed`); `Ssi::begin`; `spawn_commit_thread`; `CommitPipeline::new`; `Ssi::install` / `LockManager::install` |
//! | KvIo | `Core::open_view` / `Registry::open_view`; `ViewGuard::get` / `scan` / `as_snap` (reading through a held view, wherever it was opened); `Core::latest_get`; `Core::write(_, No)`; `Core::truncate_status`; `read::read_key` / `scan`; `Ssi::read_key` / `scan`; `removal::*`; `rollback_to` (KvIo when the txn has layers at `seq >= s`, else N); `Resolver::run_once`; catalog reads (`read_rel`, `read_idx`, `scan_*`); `Catalog::open` (the `rebuild_storage_map` scan; it writes and syncs nothing); the step API's `RowOpTask::step` / `KeyOpTask::step` / `DeferrableCheckTask::step` (one latch section each; a step returns its waits as values, it never parks) |
//! | Fsync | `Core::open`; `Core::write(_, Yes)`; `Core::sync_wal`; `Core::commit`; `CommitTicket::wait` (unanswered); `Core::seq_next`; `Catalog::alloc_oid` / `alloc_storage_id`; `GcJob::publish` / `run_once`; `CommitPipeline::process_group` (commit thread only) |
//! | Park | `wait_on` / `wait_on_any` / `wait_on_any_deadline`; `WaitHandle::park`; `row_op`, `insert_key`, `update_pk`, `insert_on_conflict`, `fk_check_child`, `fk_check_parent`, `check_deferrable_unique`; `lock_relation` / `advisory_xact_lock` with `LockWait::Block`; `catalog_*` DDL (Park and Fsync) |
//! | Join | `CommitThreadHandle::shutdown` / `Drop`; resolver, GC and retention handle `stop` / `Drop` (wired by C-B1) |
//!
//! Rows KvIo through Join are enforced transitively, each class through
//! its own funnel. **KvIo reads:** every KV read outside a latch passes
//! two checked sites — the view open
//! ([`Registry::open_view`](crate::registry::Registry::open_view), which
//! covers opening the view on an executor thread) and then the view-read
//! funnel `ViewGuard::get` / `scan` / `as_snap`, which covers the rest of
//! the M-1 hole: a view opened off-executor or on another thread and then
//! read on an executor thread refuses at the read, because every read
//! path (`read::read_key`, `read::scan`, `Ssi::read_key` / `scan`, the
//! catalog reads) reaches the KV through the guard. The scan arm holds
//! for iteration too (C-R0bFIX-2): a scan iterator is consumed lazily,
//! so [`ViewGuard::scan`](crate::registry::ViewGuard::scan) checks at
//! creation **and on every `next()`** — an iterator created off-executor
//! (or inside `allow_blocking`) and pulled on an executor thread refuses
//! at the first pull (L-A). Writes with
//! `Durability::No` and the removals funnel through `Core::write` and
//! `Core::latest_get`. **Park** rows funnel through the park sites in
//! `wait.rs` and `locks.rs`. **Join** rows through `commit.rs`. The
//! exception is the entry points whose fast path does no I/O —
//! `Core::commit`, `CommitTicket::wait` and `Core::seq_next` — which
//! carry their own entry check so misuse is detected deterministically,
//! not only when the KV actually blocks.

use std::cell::Cell;
use std::marker::PhantomData;

/// The class of a blocking entry point (the Goal's four classes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Blocking {
    /// KV reads, and writes with `Durability::No`.
    KvIo,
    /// A durable sync, or a wait whose duration includes one.
    Fsync,
    /// Waiting on another txn, unbounded unless `lock_timeout` is set.
    Park,
    /// Joining a thread.
    Join,
}

thread_local! {
    /// Set while the current thread is an executor thread. The
    /// [`HOLDING_LATCH`](crate::latch) thread-local is the house
    /// precedent for this shape.
    static EXECUTOR: Cell<bool> = const { Cell::new(false) };
}

/// Restores the previous thread role on drop. `!Send` (a raw-pointer
/// `PhantomData`), so it is always dropped on the thread that created it —
/// a guard that crossed threads could mark one thread and unmark another.
pub struct RoleGuard {
    prev: bool,
    _not_send: PhantomData<*const ()>,
}

impl Drop for RoleGuard {
    fn drop(&mut self) {
        EXECUTOR.with(|e| e.set(self.prev));
    }
}

/// Marks the current thread as an executor thread for a scope; the guard
/// restores the previous role on drop.
pub fn enter_executor() -> RoleGuard {
    RoleGuard {
        prev: EXECUTOR.with(|e| e.replace(true)),
        _not_send: PhantomData,
    }
}

/// Unmarks the current thread for a scope: the `block_in_place` analogue,
/// for the rare intentional blocking call inside executor code. The guard
/// restores the previous role on drop.
pub fn allow_blocking() -> RoleGuard {
    RoleGuard {
        prev: EXECUTOR.with(|e| e.replace(false)),
        _not_send: PhantomData,
    }
}

/// Marks the current thread as an executor thread permanently. A runtime's
/// `on_thread_start` will call it; there is no un-marking.
pub fn mark_executor_thread() {
    std::mem::forget(enter_executor());
}

/// Whether the current thread is marked as an executor thread.
pub fn is_executor_thread() -> bool {
    EXECUTOR.with(Cell::get)
}

/// The prefix of every [`check`] panic message.
pub const VIOLATION: &str = "nucleus-txn C-R0b: blocking call on an executor thread";

/// The executor-thread check: on an executor thread it panics with
/// `{VIOLATION}: {kind:?} at {site}` **before** the call does anything
/// else; on an unmarked thread it does nothing. Always on, in release
/// builds too — this is a caller-contract violation, not a debug aid.
/// `#[track_caller]` makes the panic report point at the offending call
/// site, not at this function (L-6).
#[track_caller]
pub fn check(kind: Blocking, site: &'static str) {
    if is_executor_thread() {
        panic!("{VIOLATION}: {kind:?} at {site}");
    }
}
