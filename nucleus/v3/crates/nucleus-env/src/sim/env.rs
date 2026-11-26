#![deny(clippy::indexing_slicing)]
//! SimEnv: E4's simulator behind A-1a's `Env`/`File` traits (card A-1b
//! Work 4; PLAN-v3 §3.2's SimEnv bullet; S-STORE §15's SimEnv rows).
//!
//! [`Sim`] owns one seed's machine across lives: the [`SimDisk`], the
//! fault knobs, the crash and media streams, the virtual clock, the
//! wal-directory flag and the life count. [`Sim::run_life`] follows
//! E4's `run_world` (spike `tests/sim.rs` lines 45–84): one
//! [`SchedCore`] with the harness as task 0, one OS thread per
//! workload task under the baton, `catch_unwind`, then `finish`.
//! [`SimEnv`] is a task's view of the world; every store-side call
//! goes through it, and [`SimFile`] is its handle type.
//!
//! **S-ENV, the layer's invariants** (each pinned by a Work 6 test in
//! `tests/sim_env.rs`; E4's own pins stay in `tests/sim.rs`):
//!
//! - **Tasks.** The calling task is found in a thread-local; an Env
//!   call from a thread that is not a task is `EnvError::Halted`
//!   (`sim_halted_off_task`). E4's `EnvRef` carried the task id
//!   explicitly; A-1a's trait has `&self` methods with no task
//!   argument, and a store may share one handle across its threads,
//!   so the id lives in the thread-local instead.
//!   A task's body starts only once the baton first reaches it
//!   (`Sched::await_first_baton`; A-1b-F1 (a)), so none of its calls,
//!   `rand`, `now`, `wake` and `accepted` included, runs before its
//!   task holds the baton. The wait is a park, not a yield: it draws
//!   nothing and counts no yield.
//!   A handle used after its life ended answers at once —
//!   `Halted { "the life has ended" }` from a fallible call, 0 from `rand`,
//!   the final clock from `now`, nothing from `sleep` and `wake` — and
//!   never parks (S-LIVE; Work 10).
//! - **Yield then step** (E4 card 1.2, kept). Every `Env` and `File`
//!   method except `now`, `rand`, [`SimFile::accepted`] and `Drop`
//!   first calls `yield_baton_at(task, "env:<method>")`, where the
//!   armed crash may fire — so **a crashed operation never executes**
//!   (`env_calls_yield_and_crash`) — then acquires the world with
//!   `Sched::acquire` and acts on the `SimDisk`.
//!   `Read`-mode refusals and path errors come first, before the
//!   yield (A-1a's E-OPEN and E-PATH say "before any I/O").
//! - **Errors.** `SimFail::Crashed` maps to `EnvError::Crashed`;
//!   `Deadlock`, `Livelock`, `Baton` and `Internal` map to
//!   `EnvError::Halted`, which names them. Disk-level failures map to
//!   the A-1a variant they mean (a missing file `NotFound`, an
//!   existing name `AlreadyExists`, …); the adapter checks the case
//!   with `SimDisk::stat` before it calls a `SimDisk` method that
//!   would return `Err(String)`. E4's `step` turned every disk error
//!   into `Crashed`, and its own `exists` doc records the false
//!   "start segment missing" that caused; A-1a's contract forbids it
//!   (`sim_open_modes`). Every `Halted` the layer raises in a life (a
//!   world failure other than a crash, a refused acquisition, a call
//!   off a task thread, a panicked task) is also recorded, in order, in
//!   [`LifeOutcome::halts`], and so is a refused `rand`, which still
//!   answers 0: a body that swallows one still fails the life
//!   (A-1b-F1 (f); `sim_halted_off_task`). A crash is not a halt.
//! - **Namespace** (S-STORE §2's layout: root files, `wal`, files in
//!   `wal/`). Env path `x` maps to disk `/x`, `wal/x` to `/wal/x`;
//!   directories `""` → `/` and `wal` → `/wal/`. The E-KIND checks
//!   come first (under a file: `exists` `Ok(false)`, the rest
//!   `NotFound`; `wal` as a directory once the flag is set; a file as
//!   a directory: `NotADirectory`), then the query/creation rule: a
//!   query of a path that cannot exist here answers as missing, and
//!   only a creation the model cannot hold is `InvalidPath`
//!   (`sim_namespace_is_root_plus_wal`, `sim_kind_mismatches`,
//!   `sim_sync_dir_root_and_subdir`). `create_dir("wal")` sets the
//!   `Sim`'s wal flag — durable at once, a stated limit: a lost
//!   `mkdir` is not simulated (E4 had the same pre-existing
//!   directories), which is stronger than E-DURABLE, never weaker.
//! - **File ops.** `read_at` reads `min(buf.len(), cur_len − off)`
//!   bytes through `SimDisk::read_at` so the read is recorded for
//!   taint, and returns 0, with no disk read, at or past the end at
//!   any offset. An empty `write_at` returns `Ok` after the `Read`
//!   refusal and the leading yield **without** calling
//!   `SimDisk::write_at` (E4's would extend the file to `off`;
//!   `sim_io_edges`). The `i64::MAX` bound and the
//!   [`SIM_MAX_LEN`] cap (A-1b-0-F2) are checked in this layer,
//!   before any disk call, so the copied `disk.rs` stays exactly
//!   E4's (`sim_len_cap`): past the bound is
//!   `Io { InvalidInput }`, past the cap `Io { FileTooLarge }`.
//! - **Handles (E-HANDLE).** A [`SimFile`] holds the `FileId` its
//!   `open` resolved and never resolves its path again. `rename` and
//!   `remove` change only the disk's bindings; the inode stays, so
//!   the handle reaches its file under the new name, or unnamed after
//!   a remove, and a later `CreateNew` of the same name gets a new
//!   `FileId` (`sim_handle_survives_rename`,
//!   `sim_handle_survives_remove_and_recreate`). Errors name the
//!   open-time path.
//! - **Locks.** A per-life table keyed by `FileId`, with handle ids
//!   from a per-life counter; a lock moves with a rename and is
//!   released by `unlock` or `Drop` whatever the file's name. The
//!   same-handle rule (A-1b-0-F3): the held mode again is `Ok`, the
//!   other is `Locked` and the handle keeps its lock
//!   (`sim_lock_mode_on_same_handle`). A new life starts with no
//!   locks, so **a crash releases every lock**
//!   (`sim_power_cut_releases_locks`).
//! - **Clock and randomness.** `now` is the virtual clock, which
//!   survives lives and which only `sleep` moves; `sleep(d)` is a
//!   yield point and then adds `d`. `rand` draws from a per-life
//!   stream (`sim_clock_and_rand`, `sim_life_is_deterministic`).
//!   A task observes a crash only through a fallible call. `now` and
//!   `sleep` keep working in a failed world: each `sleep` there advances
//!   the calling task's own clock by its `d`, so a time-bounded loop ends
//!   after a crash, while the world's clock stays where the failure left
//!   it, so one task's unwinding never moves another task's time or the
//!   next life's (A-1b-F1 (b); S-DET).
//! - **Streams.** Every stream is `rng::derive(seed, TAG)` with fixed
//!   `TAG` constants, so adding or removing a stream never perturbs
//!   the others (rng.rs's rule): the schedule per life
//!   (`TAG_SCHED + life`), `rand` per life (`TAG_RAND + life`),
//!   F-CRASH once across lives (`TAG_CRASH`), media once across
//!   lives (`TAG_MEDIA`). The same seed and body give the same
//!   `LifeOutcome::trace` and the same disk (`sim_seeds_differ`).
//! - **Crash kinds.** A life that ends `crashed` with no
//!   [`Sim::power_cut`] after it is a **process crash**: the page
//!   cache survives, and the next life reads every write, synced or
//!   not (`sim_process_crash_keeps_page_cache`).
//!   [`Sim::power_cut`] is the **power cut**: F-CRASH resolves
//!   everything unsynced — drop, tear or reorder per 512-B sector,
//!   lose a suffix of each directory's unsynced ops, keep a length
//!   only where surviving data needs it. That is S-2-F2-F1's loss of
//!   unsynced creates, renames and `set_len`
//!   (`power_cut_loses_unsynced_*`).
//!   `Sim::set_dir_loss(DirLoss::Independent)` makes a power cut keep
//!   each unsynced directory op independently, applied in issue order
//!   where it still applies; the default, `DirLoss::Prefix`, keeps a
//!   prefix per directory, as E4 did (card A-6b-0, D4;
//!   `sim_dir_loss_independent`). Media faults
//!   (`media_page`, `media_wal`, `media_one`) are post-sync LOST,
//!   MISDIRECTED or TORN on durable writes, as in E4; their caller
//!   decides when they apply.

use crate::error::EnvError;
use crate::path;
use crate::rng::{self, Rng};
use crate::sim::HasDisk;
use crate::sim::disk::{DirLoss, FaultKnobs, MediaFault, SimDisk};
use crate::sim::sched::{Sched, SchedCore, SchedHost, SimFail, TaskId, WaitKey, world_held_by_me};
use crate::{Env, File, OpenMode};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

// Re-exported for the crate's public surface exactly as E4's `mod.rs`
// re-exported it from its `env.rs`.
pub use crate::sim::disk::FileId;

/// SimEnv's file-length cap (A-1b-0-F2, S-ENV "File ops"): an end or
/// length past it is `Io { kind: FileTooLarge }`. 1 GiB is 16 times
/// S-STORE §2.1's default 64 MiB segment; no test here comes near it.
/// A larger simulated file is a later card's change to this constant.
pub const SIM_MAX_LEN: u64 = 1 << 30;

/// The stream tags (S-ENV "Streams"). Every stream is
/// `rng::derive(seed, TAG…)`, so adding or removing a stream never
/// perturbs the others (rng.rs's rule). The values are fixed (this
/// card's judgment call): changing one changes every schedule.
const TAG_SCHED: u64 = 0x00A1_7B01;
const TAG_RAND: u64 = 0x00A1_7B02;
const TAG_CRASH: u64 = 0x00A1_7B03;
const TAG_MEDIA: u64 = 0x00A1_7B04;

// The calling task, recorded by `run_life` in each task thread
// before the body runs (S-ENV "Tasks"). Thread-locals are not
// inherited, so a thread the body spawns sees `None` — an Env call
// from it is `Halted`.
thread_local! {
    static TASK: std::cell::Cell<Option<TaskId>> = const { std::cell::Cell::new(None) };
    /// The calling task's own post-failure clock offset in nanoseconds
    /// (card A-6b-0 Work 2; the A-6b-0 ruling R1). It is 0 until this
    /// task's `sleep` fails in a failed world, and only this task's
    /// thread ever reads or writes it: after a failure every task
    /// unwinds on its own OS thread, so one clock shared by all of them
    /// would follow thread order (S-DET). `run_life` sets it to 0 beside
    /// `TASK`, so the next life, on fresh threads, starts without it.
    static FAILED_SLEEP_NS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// The life-ended check every `SimEnv` and `SimFile` entry point makes
/// before any yield, acquisition or other access to the world (card
/// A-6b-0 Work 10; the A-6b-0 ruling R2). `run_life` sets the flag once
/// every task thread has joined, so it is never set in a live life. A
/// handle kept past its life answers this instead of parking on a world
/// that will never hand out its baton again (S-LIVE: never a hang). The
/// answer is built here, not through [`halt`]: an ended life has no world
/// left to record in, so none of these is a halt. One atomic read: no
/// scheduler and no lock.
fn life_ended(ended: &AtomicBool) -> Result<(), EnvError> {
    if ended.load(Ordering::SeqCst) {
        Err(EnvError::Halted {
            what: "the life has ended".to_string(),
        })
    } else {
        Ok(())
    }
}

/// S-ENV "Errors" (A-1b-F1 (f)): every `Halted` this layer raises is
/// built here, which also records `what` in the life's world, so
/// [`LifeOutcome::halts`] lists it even when the body swallows the
/// error. The record takes the world with `Sched::lock`, the
/// scheduler's own blocking acquisition, which no caller holds here:
/// a halt is raised after a refused or failed scheduler call, which
/// released the world, or off a task thread, which never held it. The
/// one exception is a thread that already holds the world through an
/// `acquire` guard (a re-entrant acquisition, refused with `Baton`),
/// which cannot take the lock again; no SimEnv method holds a guard
/// across another call, so it never arises through the trait, and the
/// record is then skipped rather than deadlocking. (A stale handle's
/// "the life has ended", after `run_life` returned, has no life left to
/// record it in.)
fn halt(sched: &Sched<SimWorld>, what: String) -> EnvError {
    if !world_held_by_me() {
        sched.lock().halts.push(what.clone());
    }
    EnvError::Halted { what }
}

/// The calling task, or `Halted` off a task thread (recorded, A-1b-F1
/// (f)).
fn current_task(sched: &Sched<SimWorld>) -> Result<TaskId, EnvError> {
    match TASK.with(|t| t.get()) {
        Some(t) => Ok(t),
        None => Err(halt(
            sched,
            "an Env call from a thread that is not a life task".to_string(),
        )),
    }
}

/// S-ENV "Errors": `SimFail` onto the A-1a surface. `Crashed` keeps
/// its variant and is not a halt; every other world failure is
/// `Halted`, naming itself, and recorded (A-1b-F1 (f)).
fn fail(sched: &Sched<SimWorld>, f: SimFail) -> EnvError {
    match f {
        SimFail::Crashed => EnvError::Crashed,
        SimFail::Deadlock => halt(sched, "deadlock".to_string()),
        SimFail::Livelock(site) => halt(sched, format!("livelock at {site}")),
        SimFail::Baton(site) => halt(sched, format!("baton violation at {site}")),
        SimFail::Internal(what) => halt(sched, format!("internal: {what}")),
    }
}

/// S-ENV yield then step: the life-ended check (Work 10), then the
/// leading yield — where an armed crash fires, so a crashed operation
/// never executes — then one atomic step under the world lock (E4's card
/// 1.2 shape, kept).
fn yield_step<T>(
    ended: &AtomicBool,
    sched: &Sched<SimWorld>,
    site: &'static str,
    f: impl FnOnce(&mut SimWorld) -> Result<T, EnvError>,
) -> Result<T, EnvError> {
    life_ended(ended)?;
    let task = current_task(sched)?;
    sched
        .yield_baton_at(task, site)
        .map_err(|e| fail(sched, e))?;
    let mut guard = sched.acquire(task, site).map_err(|e| fail(sched, e))?;
    f(&mut guard)
}

// ---- S-ENV error constructors (the A-1a variants disk cases mean) ----

fn not_found(p: &str) -> EnvError {
    EnvError::NotFound {
        path: p.to_string(),
    }
}

fn already_exists(p: &str) -> EnvError {
    EnvError::AlreadyExists {
        path: p.to_string(),
    }
}

fn invalid_model(p: &str) -> EnvError {
    EnvError::InvalidPath {
        path: p.to_string(),
        why: "SimEnv models the root and wal/ only",
    }
}

fn is_a_directory(op: &'static str, p: &str) -> EnvError {
    EnvError::Io {
        op,
        path: p.to_string(),
        kind: std::io::ErrorKind::IsADirectory,
        detail: "is a directory".to_string(),
    }
}

fn not_a_directory(op: &'static str, p: &str) -> EnvError {
    EnvError::Io {
        op,
        path: p.to_string(),
        kind: std::io::ErrorKind::NotADirectory,
        detail: "not a directory".to_string(),
    }
}

fn io_invalid_input(op: &'static str, p: &str, detail: &str) -> EnvError {
    EnvError::Io {
        op,
        path: p.to_string(),
        kind: std::io::ErrorKind::InvalidInput,
        detail: detail.to_string(),
    }
}

/// A residual `SimDisk` `Err(String)`: the adapter checks every case
/// with `SimDisk::stat` first (S-ENV "Errors"), so reaching this is an
/// internal breach — reported, never a panic and never `Crashed`.
fn disk_internal(op: &'static str, p: &str, what: String) -> EnvError {
    EnvError::Io {
        op,
        path: p.to_string(),
        kind: std::io::ErrorKind::Other,
        detail: format!("sim internal: {what}"),
    }
}

/// Whether `p` is a file path in the wal directory (`wal/<name>`).
fn in_wal(p: &str) -> bool {
    p.starts_with("wal/")
}

/// The disk path `p` maps to as a **file** (S-ENV "Namespace"): `x`
/// → `/x`, `wal/x` → `/wal/x`. `None` for a path deeper than one
/// component outside `wal/` — the model holds only root files, `wal`,
/// and files in `wal/`.
fn disk_file_path(p: &str) -> Option<String> {
    match p.rsplit_once('/') {
        // A root name (`x`, or `wal` itself where no directory exists
        // there yet).
        None => Some(format!("/{p}")),
        // A name in the one modelled directory.
        Some(("wal", _)) => Some(format!("/{p}")),
        Some(_) => None,
    }
}

/// The lock mode a handle holds (S-ENV "Locks"; E-LOCK's same-handle
/// rule, A-1b-0-F3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LockMode {
    Shared,
    Exclusive,
}

/// One file's lock holders (S-ENV "Locks").
#[derive(Default)]
struct FileLocks {
    exclusive: Option<u64>,
    shared: BTreeSet<u64>,
}

/// The per-life lock table (S-ENV "Locks"): by file id and by handle
/// id. Handle ids come from `next_handle`, a per-life counter, so a
/// new life starts with no locks and **a crash releases every lock**.
#[derive(Default)]
struct LockTable {
    next_handle: u64,
    by_file: BTreeMap<FileId, FileLocks>,
    by_handle: BTreeMap<u64, LockMode>,
}

impl LockTable {
    /// The next per-life handle id (S-ENV "Locks"). Called by `open`.
    fn fresh_handle(&mut self) -> u64 {
        let h = self.next_handle;
        self.next_handle = self.next_handle.wrapping_add(1);
        h
    }

    /// Releases everything `handle` holds, whatever the file's name
    /// (E-HANDLE, E-LOCK). Used by `unlock` and by `Drop`.
    fn release(&mut self, handle: u64) {
        if self.by_handle.remove(&handle).is_none() {
            return;
        }
        for locks in self.by_file.values_mut() {
            if locks.exclusive == Some(handle) {
                locks.exclusive = None;
            }
            locks.shared.remove(&handle);
        }
    }
}

/// The scheduler host of one life (S-ENV): the scheduling core, the
/// disk — moved in from [`Sim`] for the life and back out after — the
/// virtual clock, the per-life `rand` stream, the wal-directory flag,
/// the per-life lock table and the life's halt record.
struct SimWorld {
    sched: SchedCore,
    disk: Option<SimDisk>,
    /// Keeps [`HasDisk`] total once the life has ended and the disk
    /// moved back out: unreachable while any task runs (the disk is
    /// taken only after every task joined), and empty either way.
    empty: SimDisk,
    clock_ns: u64,
    rand: Rng,
    wal_dir: bool,
    locks: LockTable,
    /// Every `Halted` the layer raised in this life, in order
    /// (A-1b-F1 (f)); `run_life` moves it into [`LifeOutcome::halts`].
    halts: Vec<String>,
}

impl SchedHost for SimWorld {
    fn sched_mut(&mut self) -> &mut SchedCore {
        &mut self.sched
    }
}

impl HasDisk for SimWorld {
    fn disk(&mut self) -> &mut SimDisk {
        let empty = &mut self.empty;
        self.disk.as_mut().unwrap_or(empty)
    }
}

impl SimWorld {
    /// The disk for a read, or `Halted` once the life has ended (a
    /// stale handle from a finished life; unreachable in a live one).
    fn disk_ref(&self) -> Result<&SimDisk, EnvError> {
        self.disk.as_ref().ok_or_else(|| EnvError::Halted {
            what: "the life has ended".to_string(),
        })
    }

    /// The disk for a mutation, as `disk_ref`.
    fn disk_mut(&mut self) -> Result<&mut SimDisk, EnvError> {
        self.disk.as_mut().ok_or_else(|| EnvError::Halted {
            what: "the life has ended".to_string(),
        })
    }

    /// `SimDisk::stat` on a disk path.
    fn dstat(&self, dpath: &str) -> Option<FileId> {
        self.disk.as_ref().and_then(|d| d.stat(dpath))
    }

    /// E-KIND's under-a-file rule: whether a proper prefix of `p`
    /// names an existing file (a root file, or `wal/<name>`).
    fn under_file(&self, p: &str) -> bool {
        let comps: Vec<&str> = p.split('/').collect();
        let mut prefix = String::new();
        for c in comps.iter().take(comps.len().saturating_sub(1)) {
            if !prefix.is_empty() {
                prefix.push('/');
            }
            prefix.push_str(c);
            if let Some(d) = disk_file_path(&prefix)
                && self.dstat(&d).is_some()
            {
                return true;
            }
        }
        false
    }

    /// Whether the wal directory exists (its flag is durable at once).
    fn wal_exists(&self) -> bool {
        self.wal_dir
    }

    // ---- the namespace, per method (S-ENV "Namespace") ----

    /// `exists`: under a file `Ok(false)`; `wal` the directory's flag
    /// (or, in the corner no test reaches, a root file of that name);
    /// `wal/…` only once the directory exists; a path the model
    /// cannot hold is missing.
    fn exists(&self, p: &str) -> bool {
        if self.under_file(p) {
            return false;
        }
        if p == "wal" {
            return self.wal_exists() || self.dstat("/wal").is_some();
        }
        match disk_file_path(p) {
            Some(d) if !in_wal(p) || self.wal_exists() => self.dstat(&d).is_some(),
            _ => false,
        }
    }

    /// `open`: resolves the file id and draws a fresh per-life handle
    /// id. E-KIND and the query/creation rule as in the module doc.
    fn open(&mut self, p: &str, mode: OpenMode) -> Result<(FileId, u64), EnvError> {
        if self.under_file(p) {
            return Err(not_found(p));
        }
        if p == "wal" && self.wal_exists() {
            // E-KIND 2: the wal directory, once it exists.
            return Err(match mode {
                OpenMode::CreateNew => already_exists(p),
                _ => is_a_directory("open", p),
            });
        }
        if in_wal(p) && !self.wal_exists() {
            // Every `wal/…` path is NotFound before the directory
            // exists (E-OPEN: the missing parent).
            return Err(not_found(p));
        }
        let Some(d) = disk_file_path(p) else {
            // A creation the model cannot hold is InvalidPath; a
            // query is missing (S-ENV's query/creation rule).
            return Err(match mode {
                OpenMode::CreateNew => invalid_model(p),
                _ => not_found(p),
            });
        };
        match mode {
            OpenMode::Read | OpenMode::ReadWrite => {
                let id = self.dstat(&d).ok_or_else(|| not_found(p))?;
                Ok((id, self.locks.fresh_handle()))
            }
            OpenMode::CreateNew => {
                if self.dstat(&d).is_some() {
                    return Err(already_exists(p));
                }
                let disk = self.disk_mut()?;
                let id = disk.create(&d).map_err(|e| disk_internal("open", p, e))?;
                Ok((id, self.locks.fresh_handle()))
            }
        }
    }

    /// `create_dir`: only `wal` can be created; an existing name —
    /// the wal directory, or a file — is `AlreadyExists`, checked
    /// first (E-KIND); anything else is a creation the model cannot
    /// hold. Setting the flag is durable at once (stated limit).
    fn create_dir(&mut self, p: &str) -> Result<(), EnvError> {
        if self.under_file(p) {
            return Err(not_found(p));
        }
        if p == "wal" {
            if self.wal_exists() || self.dstat("/wal").is_some() {
                return Err(already_exists(p));
            }
            self.wal_dir = true;
            return Ok(());
        }
        if disk_file_path(p).is_some_and(|d| self.dstat(&d).is_some()) {
            return Err(already_exists(p));
        }
        Err(invalid_model(p))
    }

    /// `sync_dir`: `""` is the root, `wal` the wal directory once it
    /// exists; a file where a directory is required is
    /// `NotADirectory` (E-KIND 3); anything else — a missing
    /// directory of any other name included — is `NotFound`, the
    /// query rule (never `InvalidPath`).
    fn sync_dir(&mut self, p: &str) -> Result<(), EnvError> {
        if self.under_file(p) {
            return Err(not_found(p));
        }
        if p == "wal" {
            if self.wal_exists() {
                let disk = self.disk_mut()?;
                return disk
                    .sync_dir(crate::sim::disk::WAL_DIR)
                    .map_err(|e| disk_internal("sync_dir", p, e));
            }
            if self.dstat("/wal").is_some() {
                return Err(not_a_directory("sync_dir", p));
            }
            return Err(not_found(p));
        }
        if p.is_empty() {
            let disk = self.disk_mut()?;
            return disk
                .sync_dir(crate::sim::disk::ROOT_DIR)
                .map_err(|e| disk_internal("sync_dir", p, e));
        }
        if disk_file_path(p).is_some_and(|d| self.dstat(&d).is_some()) {
            return Err(not_a_directory("sync_dir", p));
        }
        Err(not_found(p))
    }

    /// `rename`: E-PATH and the cross-directory rule are checked
    /// before the yield; here E-KIND's order — the source (`NotFound`
    /// under a file or missing, `IsADirectory` for `wal`), then the
    /// target (`AlreadyExists`) — then the disk's rename, which only
    /// rebinds the name (E-HANDLE).
    fn rename(&mut self, from: &str, to: &str) -> Result<(), EnvError> {
        if self.under_file(from) {
            return Err(not_found(from));
        }
        if from == "wal" && self.wal_exists() {
            return Err(is_a_directory("rename", from));
        }
        if in_wal(from) && !self.wal_exists() {
            return Err(not_found(from));
        }
        let Some(fd) = disk_file_path(from) else {
            return Err(not_found(from));
        };
        if self.dstat(&fd).is_none() {
            return Err(not_found(from));
        }
        // The target: a name that cannot exist (under a file, or in
        // the wal directory before it exists) is `NotFound` naming
        // it, as StdEnv's path-resolution refusal; an existing name —
        // a file, or `wal` the directory — is `AlreadyExists`.
        if self.under_file(to) {
            return Err(not_found(to));
        }
        if in_wal(to) && !self.wal_exists() {
            return Err(not_found(to));
        }
        if to == "wal" && self.wal_exists() {
            return Err(already_exists(to));
        }
        if let Some(td) = disk_file_path(to)
            && self.dstat(&td).is_some()
        {
            return Err(already_exists(to));
        }
        let td = disk_file_path(to).ok_or_else(|| not_found(to))?;
        let disk = self.disk_mut()?;
        disk.rename(&fd, &td)
            .map_err(|e| disk_internal("rename", from, e))
    }

    /// `remove`: a file by name; `wal` the directory is
    /// `IsADirectory` (E-KIND 2); under a file or missing `NotFound`.
    fn remove(&mut self, p: &str) -> Result<(), EnvError> {
        if self.under_file(p) {
            return Err(not_found(p));
        }
        if p == "wal" && self.wal_exists() {
            return Err(is_a_directory("remove", p));
        }
        if in_wal(p) && !self.wal_exists() {
            return Err(not_found(p));
        }
        let Some(d) = disk_file_path(p) else {
            return Err(not_found(p));
        };
        if self.dstat(&d).is_none() {
            return Err(not_found(p));
        }
        let disk = self.disk_mut()?;
        disk.unlink(&d).map_err(|e| disk_internal("remove", p, e))
    }

    /// `list_dir`: `""` merges `wal` (once it exists) into the disk's
    /// root names, sorted; `wal` lists the directory; a file is
    /// `NotADirectory` (E-KIND 3); anything else is `NotFound`.
    fn list_dir(&self, p: &str) -> Result<Vec<String>, EnvError> {
        if self.under_file(p) {
            return Err(not_found(p));
        }
        if p == "wal" {
            if self.wal_exists() {
                return Ok(self.disk_ref()?.dir_names(crate::sim::disk::WAL_DIR));
            }
            if self.dstat("/wal").is_some() {
                return Err(not_a_directory("list_dir", p));
            }
            return Err(not_found(p));
        }
        if p.is_empty() {
            let disk = self.disk_ref()?;
            let mut names = disk.dir_names(crate::sim::disk::ROOT_DIR);
            if self.wal_exists() {
                names.push("wal".to_string());
            }
            names.sort();
            return Ok(names);
        }
        if disk_file_path(p).is_some_and(|d| self.dstat(&d).is_some()) {
            return Err(not_a_directory("list_dir", p));
        }
        Err(not_found(p))
    }

    // ---- file ops on an open handle's FileId (S-ENV "File ops") ----

    /// A read of exactly `min(len, cur_len - off)` bytes, recorded
    /// for taint; 0 with no disk read at or past the end. `p` names
    /// the handle's open-time path in the residual-error case.
    fn read_at(
        &mut self,
        id: FileId,
        off: u64,
        buf: &mut [u8],
        p: &str,
    ) -> Result<usize, EnvError> {
        let cur = self.disk_ref()?.cur_len(id);
        let avail = usize::try_from(cur.saturating_sub(off)).unwrap_or(usize::MAX);
        let n = buf.len().min(avail);
        if n == 0 {
            return Ok(0);
        }
        let disk = self.disk_mut()?;
        let bytes = disk
            .read_at(id, off, n as u64)
            .map_err(|e| disk_internal("read_at", p, e))?;
        if let Some(dst) = buf.get_mut(..bytes.len()) {
            dst.copy_from_slice(&bytes);
        }
        Ok(bytes.len())
    }

    /// A write of whole `buf` (the caller has refused `Read`, the
    /// empty write, and the two length checks).
    fn write_at(&mut self, id: FileId, off: u64, buf: &[u8], p: &str) -> Result<(), EnvError> {
        let disk = self.disk_mut()?;
        disk.write_at(id, off, buf)
            .map_err(|e| disk_internal("write_at", p, e))
    }

    fn set_len(&mut self, id: FileId, n: u64, p: &str) -> Result<(), EnvError> {
        let disk = self.disk_mut()?;
        disk.set_len(id, n)
            .map_err(|e| disk_internal("set_len", p, e))
    }

    fn sync(&mut self, id: FileId, all: bool, p: &str) -> Result<(), EnvError> {
        let disk = self.disk_mut()?;
        let r = if all {
            disk.sync_all(id)
        } else {
            disk.sync_data(id)
        };
        r.map_err(|e| disk_internal(if all { "sync_all" } else { "sync_data" }, p, e))
    }

    fn cur_len(&self, id: FileId) -> Result<u64, EnvError> {
        Ok(self.disk_ref()?.cur_len(id))
    }
}

/// One seed's simulated machine, across lives (crashes): the disk,
/// the knobs, the crash and media streams, the virtual clock, the
/// wal-directory flag and the life count (S-ENV).
pub struct Sim {
    seed: u64,
    knobs: FaultKnobs,
    disk: SimDisk,
    crash_rng: Rng,
    media_rng: Rng,
    clock_ns: u64,
    wal_dir: bool,
    lives: u32,
    /// How [`Sim::power_cut`] resolves each directory's unsynced ops
    /// (D4, card A-6b-0): [`DirLoss::Prefix`], E4's rule, unless
    /// [`Sim::set_dir_loss`] chose otherwise.
    dir_loss: DirLoss,
}

impl Sim {
    /// An empty machine whose every stream derives from `seed`
    /// (S-ENV "Streams"): the same seed and the same bodies give the
    /// same schedules, the same `rand` values, the same faults and
    /// the same bytes (S-DET).
    pub fn new(seed: u64, knobs: FaultKnobs) -> Sim {
        Sim {
            seed,
            knobs,
            disk: SimDisk::new(),
            crash_rng: Rng::new(rng::derive(seed, TAG_CRASH)),
            media_rng: Rng::new(rng::derive(seed, TAG_MEDIA)),
            clock_ns: 0,
            wal_dir: false,
            lives: 0,
            dir_loss: DirLoss::Prefix,
        }
    }

    /// Run one life: `tasks` workload tasks (ids `1..=tasks`) under
    /// the baton, each on its own OS thread with its own [`SimEnv`];
    /// the crash fires at `crash_at_yield` if armed (E4's `run_world`
    /// shape). Each task's body starts only once the baton first
    /// reaches it (`Sched::await_first_baton`, A-1b-F1 (a)); a task
    /// whose world failed before that never runs its body, and its
    /// result is the failure (`Crashed` for a crash). The disk, the
    /// clock and the wal flag move into the life's world and back out
    /// when every task has ended; a fresh lock table and a fresh `rand`
    /// stream come with each life, so **a crash releases every lock**
    /// (S-ENV "Locks"). Each life begins a new disk round (the
    /// per-round read, accept and event logs clear; taint references
    /// and durable-write history survive). The outcome carries every
    /// `Halted` the layer raised in the life (A-1b-F1 (f)). Once every
    /// task thread has joined, the life is marked ended, and a handle of
    /// this life that outlives it answers at once and never parks
    /// (Work 10; the A-6b-0 ruling R2).
    pub fn run_life<F>(&mut self, tasks: usize, crash_at_yield: Option<u64>, body: F) -> LifeOutcome
    where
        F: Fn(&SimEnv, TaskId) -> Result<(), EnvError> + Send + Sync,
    {
        let life = self.lives as u64;
        let mut core = SchedCore::new(
            tasks + 1,
            rng::derive(self.seed, TAG_SCHED.wrapping_add(life)),
        );
        core.crash_at_yield = crash_at_yield;
        let mut disk = std::mem::take(&mut self.disk);
        // A new life is a new disk round: the per-round read, accept
        // and event logs clear, while taint references and the
        // durable-write history survive (E4's `new_round`).
        disk.new_round();
        let world = SimWorld {
            sched: core,
            disk: Some(disk),
            empty: SimDisk::default(),
            clock_ns: self.clock_ns,
            rand: Rng::new(rng::derive(self.seed, TAG_RAND.wrapping_add(life))),
            wal_dir: self.wal_dir,
            locks: LockTable::default(),
            halts: Vec::new(),
        };
        let sched = Arc::new(Sched::new(world));
        // The life-ended flag (Work 10): every `SimEnv` of this life, and
        // every `SimFile` opened through one, holds a clone. It is set
        // below, after the joins, and at no other time, so in a live life
        // it is never set and no live call changes.
        let ended = Arc::new(AtomicBool::new(false));
        let panicked = Arc::new(AtomicBool::new(false));
        let results_lock = Mutex::new(Vec::<(TaskId, Result<(), EnvError>)>::new());
        std::thread::scope(|scope| {
            let mut handles = Vec::new();
            let body = &body;
            for t in 1..=tasks {
                let sched = Arc::clone(&sched);
                let panicked = Arc::clone(&panicked);
                let results = &results_lock;
                let env = SimEnv {
                    sched: Arc::clone(&sched),
                    ended: Arc::clone(&ended),
                };
                handles.push(scope.spawn(move || {
                    TASK.with(|c| c.set(Some(t)));
                    // This task's own post-failure clock offset starts at 0
                    // (the A-6b-0 ruling R1): the offset belongs to one task
                    // in one life. A thread spawned for the life starts at 0
                    // anyway; setting it here makes that hold by
                    // construction, wherever the thread came from.
                    FAILED_SLEEP_NS.with(|off| off.set(0));
                    // A-1b-F1 (a): the body starts only once the baton
                    // first reaches this task — a park, not a yield, so
                    // it draws nothing and counts no yield. A world that
                    // failed first ends the task without its body, with
                    // the failure mapped as every `SimFail` is.
                    let result = match sched.await_first_baton(t) {
                        Err(f) => Err(fail(&sched, f)),
                        Ok(()) => {
                            let caught =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    body(&env, t)
                                }));
                            match caught {
                                Ok(r) => r,
                                Err(_) => {
                                    panicked.store(true, Ordering::SeqCst);
                                    Err(halt(&sched, format!("task {t} panicked")))
                                }
                            }
                        }
                    };
                    sched.finish(t);
                    if let Ok(mut rs) = results.lock() {
                        rs.push((t, result));
                    }
                }));
            }
            // The harness parks until every task has ended (crashed or
            // finished).
            let _ = sched.start();
            for h in handles {
                let _ = h.join();
            }
        });
        // Work 10: the scope has returned, so every task thread has joined,
        // and the world has not been taken back yet. From here on no call of
        // this life is live: every handle of it that survives is stale, and
        // each of its entry points answers at once instead of parking on a
        // world whose baton will never move again.
        ended.store(true, Ordering::SeqCst);
        // Take the world back out of the scheduler (it may outlive the
        // life through a leaked SimEnv clone; the disk moves home
        // regardless, and a stale handle then answers `Halted`).
        let (trace, yields, crashed, deadlock, livelock, halts) = {
            let mut guard = sched.lock();
            let w = &mut *guard;
            let core = &w.sched;
            let out = (
                core.trace.clone(),
                core.yields,
                core.crashed,
                core.deadlock,
                if core.livelock {
                    Some(core.livelock_site)
                } else {
                    None
                },
                std::mem::take(&mut w.halts),
            );
            self.disk = w.disk.take().unwrap_or_default();
            self.clock_ns = w.clock_ns;
            self.wal_dir = self.wal_dir || w.wal_dir;
            out
        };
        self.lives += 1;
        let mut results: Vec<(TaskId, Result<(), EnvError>)> =
            results_lock.into_inner().unwrap_or_default();
        results.sort_unstable_by_key(|(t, _)| *t);
        LifeOutcome {
            crashed,
            deadlock,
            livelock,
            panicked: panicked.load(Ordering::SeqCst),
            yields,
            trace,
            results: results.into_iter().map(|(_, r)| r).collect(),
            halts,
        }
    }

    /// F-CRASH, the **power cut**: resolve every volatile write, every
    /// directory op and every length change (`SimDisk::apply_crash_with`)
    /// with the seed's one crash stream. Sector images drop, tear or
    /// reorder; each directory's unsynced ops persist as a prefix
    /// (`DirLoss::Prefix`, the default) or each independently, applied
    /// in issue order where it still applies (`DirLoss::Independent`,
    /// [`Sim::set_dir_loss`]; D4) — **S-2-F2-F1's loss of unsynced
    /// creates, renames and `set_len`** — and a length survives only
    /// as far as the surviving data needs it. Returns the dropped
    /// directory ops.
    pub fn power_cut(&mut self) -> u64 {
        self.disk
            .apply_crash_with(&mut self.crash_rng, &self.knobs, self.dir_loss)
    }

    /// Choose how every later [`Sim::power_cut`] resolves each
    /// directory's unsynced ops (D4, card A-6b-0; S-STORE §12.5's
    /// `t_wal_start`). [`DirLoss::Independent`] keeps each op
    /// independently, applied in issue order where it still applies,
    /// so a later rename can survive an earlier one that is lost;
    /// [`DirLoss::Prefix`], the default, keeps a prefix per directory,
    /// as E4 did, with E4's draws. Only the directory step differs:
    /// the file data is resolved the same way, draw for draw.
    pub fn set_dir_loss(&mut self, mode: DirLoss) {
        self.dir_loss = mode;
    }

    /// F-MEDIA-PAGE on the page file named `page_file` (an Env root
    /// path, mapped to the disk's `/<name>`) with `slot`-byte slots
    /// (D1: S-STORE §2.1's configurable page size; 4096 reproduces
    /// E4's draws exactly). Post-sync LOST/MISDIRECTED/TORN on 1–3
    /// durable writes; the caller decides when they apply. A `slot`
    /// of 0 is `Io { InvalidInput }` before any disk call: the copied
    /// draw divides by it (A-1b-F1 (e)).
    pub fn media_page(
        &mut self,
        page_file: &str,
        slot: u64,
        referenced: &BTreeSet<u64>,
    ) -> Result<Vec<MediaFault>, EnvError> {
        path::check(page_file)?;
        if slot == 0 {
            return Err(EnvError::Io {
                op: "media_page",
                path: page_file.to_string(),
                kind: std::io::ErrorKind::InvalidInput,
                detail: "the slot size must be positive (it divides every offset)".to_string(),
            });
        }
        let dpath = disk_file_path(page_file).ok_or_else(|| invalid_model(page_file))?;
        Ok(self
            .disk
            .apply_media_page(&mut self.media_rng, &dpath, slot, referenced))
    }

    /// F-MEDIA-WAL: the same, on 1–2 durable WAL writes drawn
    /// uniformly over every segment's candidates (E4's class MW).
    pub fn media_wal(&mut self) -> Vec<MediaFault> {
        self.disk.apply_media_wal(&mut self.media_rng)
    }

    /// Apply one directed media fault (E4's X-5b Work 6: the caller
    /// builds it from `disk().dwrites_of(id)`; the effect is exactly
    /// a drawn fault's).
    pub fn media_one(&mut self, fault: &MediaFault) {
        self.disk.apply_one_media_fault(fault)
    }

    /// The `FileId` an Env path names on the disk, between lives
    /// (observer view). `Err` for an E-PATH violation or a path the
    /// model cannot hold.
    pub fn file_id(&self, path: &str) -> Result<Option<FileId>, EnvError> {
        path::check(path)?;
        match disk_file_path(path) {
            Some(d) => Ok(self.disk.stat(&d)),
            None => Err(invalid_model(path)),
        }
    }

    /// The disk, between lives (observer view).
    pub fn disk(&self) -> &SimDisk {
        &self.disk
    }

    /// The disk, between lives, for fixture building.
    pub fn disk_mut(&mut self) -> &mut SimDisk {
        &mut self.disk
    }

    /// Lives run through [`Sim::run_life`] so far.
    pub fn lives(&self) -> u32 {
        self.lives
    }
}

/// What one life did (S-ENV): whether it crashed or failed, its yield
/// count, its handoff trace, every task's result, and every `Halted`
/// the layer raised.
pub struct LifeOutcome {
    /// The crash point fired (or the world failed) — a process crash
    /// until [`Sim::power_cut`] resolves it.
    pub crashed: bool,
    /// All tasks were blocked (a per-life failure).
    pub deadlock: bool,
    /// The yield budget tripped, naming the site (a per-life failure).
    pub livelock: Option<&'static str>,
    /// A task unwound by panic (a per-life failure).
    pub panicked: bool,
    /// Total yield points of the life.
    pub yields: u64,
    /// `(yield_count, picked_task)` handoffs (bounded, as in E4).
    pub trace: Vec<(u64, TaskId)>,
    /// Each task's result, in task order.
    pub results: Vec<Result<(), EnvError>>,
    /// Every `Halted` the layer raised in this life, in order,
    /// including those a body swallowed; A-1b-F1 (f).
    pub halts: Vec<String>,
}

/// A task's view of the world: clone it freely inside the life's body
/// (S-ENV). Implements [`Env`]. A clone kept past the end of its life
/// answers at once and never parks (Work 10).
#[derive(Clone)]
pub struct SimEnv {
    sched: Arc<Sched<SimWorld>>,
    /// Set by [`Sim::run_life`] once the life has ended (Work 10); read
    /// by every entry point before it touches the world.
    ended: Arc<AtomicBool>,
}

impl SimEnv {
    /// The calling task (S-ENV "Tasks"): `Halted` off a task thread, and
    /// `Halted` on a handle whose life has ended (Work 10).
    pub fn task(&self) -> Result<TaskId, EnvError> {
        life_ended(&self.ended)?;
        current_task(&self.sched)
    }

    /// A plain yield point naming its site (E4's `EnvRef::yield_at`).
    pub fn yield_now(&self, site: &'static str) -> Result<(), EnvError> {
        life_ended(&self.ended)?;
        let task = current_task(&self.sched)?;
        self.sched
            .yield_baton_at(task, site)
            .map_err(|e| fail(&self.sched, e))
    }

    /// Park on a wait key (the scheduler-level condition variable;
    /// E4's `EnvRef::wait_for`).
    pub fn wait_for(&self, key: WaitKey) -> Result<(), EnvError> {
        life_ended(&self.ended)?;
        let task = current_task(&self.sched)?;
        self.sched
            .wait_for(task, key)
            .map_err(|e| fail(&self.sched, e))
    }

    /// Wake every task parked on `key` (E4's scheduler `wake`). Nothing
    /// happens on a handle whose life has ended (Work 10).
    pub fn wake(&self, key: WaitKey) {
        if life_ended(&self.ended).is_err() {
            return;
        }
        self.sched.wake(key)
    }
}

impl Env for SimEnv {
    type File = SimFile;

    fn open(&self, p: &str, mode: OpenMode) -> Result<SimFile, EnvError> {
        path::check(p)?;
        let (id, handle) = yield_step(&self.ended, &self.sched, "env:open", |w| w.open(p, mode))?;
        Ok(SimFile {
            sched: Arc::clone(&self.sched),
            ended: Arc::clone(&self.ended),
            path: p.to_string(),
            id,
            handle,
            mode,
        })
    }

    fn create_dir(&self, p: &str) -> Result<(), EnvError> {
        path::check(p)?;
        yield_step(&self.ended, &self.sched, "env:create_dir", |w| {
            w.create_dir(p)
        })
    }

    fn sync_dir(&self, p: &str) -> Result<(), EnvError> {
        path::check_dir(p)?;
        yield_step(&self.ended, &self.sched, "env:sync_dir", |w| w.sync_dir(p))
    }

    fn rename(&self, from: &str, to: &str) -> Result<(), EnvError> {
        path::check(from)?;
        path::check(to)?;
        // E-NAMESPACE: within one directory only (S-STORE §12.1's
        // recycling rename), refused before the yield like every
        // path error.
        if parent_of(from) != parent_of(to) {
            return Err(EnvError::InvalidPath {
                path: format!("{from} -> {to}"),
                why: "cross-directory rename",
            });
        }
        yield_step(&self.ended, &self.sched, "env:rename", |w| {
            w.rename(from, to)
        })
    }

    fn remove(&self, p: &str) -> Result<(), EnvError> {
        path::check(p)?;
        yield_step(&self.ended, &self.sched, "env:remove", |w| w.remove(p))
    }

    fn exists(&self, p: &str) -> Result<bool, EnvError> {
        path::check(p)?;
        yield_step(&self.ended, &self.sched, "env:exists", |w| Ok(w.exists(p)))
    }

    fn list_dir(&self, p: &str) -> Result<Vec<String>, EnvError> {
        path::check_dir(p)?;
        yield_step(&self.ended, &self.sched, "env:list_dir", |w| w.list_dir(p))
    }

    fn now(&self) -> Duration {
        // Not a yield point (S-ENV): the virtual clock, read without
        // offering the baton. `Sched::lock` is poison-tolerant and is
        // never held across a park, so this cannot wait on the scheduler.
        // On a task thread of a live life the answer is the world's clock
        // plus that task's own post-failure offset (A-1b-F1 (b); the
        // A-6b-0 ruling R1), which is 0 until the task's first `sleep` in a
        // failed world and which no other task can move. Off a task thread
        // it is the world's clock alone. So it is on a handle whose life
        // has ended: that life's final clock, whatever the calling thread's
        // own offset (Work 10).
        let clock_ns = self.sched.lock().clock_ns;
        let offset = if life_ended(&self.ended).is_ok() && TASK.with(|t| t.get()).is_some() {
            FAILED_SLEEP_NS.with(|off| off.get())
        } else {
            0
        };
        Duration::from_nanos(clock_ns.saturating_add(offset))
    }

    fn sleep(&self, d: Duration) {
        // A handle whose life has ended does nothing (Work 10).
        if life_ended(&self.ended).is_err() {
            return;
        }
        // A yield point, then the clock advances by exactly `d`. When the
        // yield fails because the world failed (a crash, a deadlock or a
        // livelock), only the calling task's own clock advances by `d`,
        // and the world's clock stays where the failure left it: a task
        // observes a crash only through a fallible call and a time-bounded
        // loop must still end (A-1b-F1 (b)), but after the failure every
        // task unwinds on its own OS thread, so a clock shared by all of
        // them would follow thread order, and the next life would inherit
        // the result (S-DET; the A-6b-0 ruling R1). The failure itself is
        // mapped as at every other yield, so a livelock tripped here is
        // recorded as a halt and a crash is not. Off a task thread nothing
        // moves and the call is a recorded halt. An S-BATON refusal (this
        // thread already holds the world) cannot take the lock for the
        // clock, so it returns with nothing advanced.
        let Ok(task) = current_task(&self.sched) else {
            return;
        };
        let add = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        if let Err(f) = self.sched.yield_baton_at(task, "env:sleep") {
            let holds_world = matches!(f, SimFail::Baton(_));
            let _ = fail(&self.sched, f);
            if holds_world {
                return;
            }
            // This task's offset alone: no lock and no world access, so
            // two tasks unwinding at once cannot interleave here.
            FAILED_SLEEP_NS.with(|off| off.set(off.get().saturating_add(add)));
            return;
        }
        let mut guard = self.sched.lock();
        guard.clock_ns = guard.clock_ns.saturating_add(add);
    }

    fn rand(&self) -> u64 {
        // A handle whose life has ended answers 0 without touching the
        // scheduler (Work 10): on a crashed life's world the acquisition
        // below would wait for an unwind turn that never comes.
        if life_ended(&self.ended).is_err() {
            return 0;
        }
        // Not a yield point (S-ENV), but still a gated host step: the
        // acquisition (X5-R14) lets only the baton holder draw, so a
        // task that has not been handed the baton yet cannot perturb
        // the stream. `rand` is infallible on every backend; a world
        // that refuses the step cannot advance it, and answers 0. A
        // refusal while the world has not failed is a halt the caller
        // cannot see, so it is recorded (A-1b-F1 (f)); since a body now
        // starts only once it holds the baton (A-1b-F1 (a)), a task's
        // own `rand` is never refused.
        let Ok(task) = current_task(&self.sched) else {
            return 0;
        };
        match self.sched.acquire(task, "env:rand") {
            Ok(mut guard) => guard.rand.next_u64(),
            Err(_) => {
                if !world_held_by_me() {
                    let mut guard = self.sched.lock();
                    let failed = guard.sched.crashed || guard.sched.deadlock;
                    if !failed {
                        guard.halts.push("rand refused: env:rand".to_string());
                    }
                }
                0
            }
        }
    }
}

/// The parent directory of a validated path: `""` for a root entry
/// (as in `mem.rs`/`std_env.rs`).
fn parent_of(path: &str) -> &str {
    match path.rsplit_once('/') {
        Some((parent, _)) => parent,
        None => "",
    }
}

/// An open simulated file (S-ENV "Handles"): the `FileId` its `open`
/// resolved — never resolved again (E-HANDLE) — its per-life lock
/// handle id, its mode, and the path it was opened by, for error
/// messages only.
pub struct SimFile {
    sched: Arc<Sched<SimWorld>>,
    /// The life-ended flag of the `SimEnv` that opened it (Work 10): set
    /// by [`Sim::run_life`] once the life has ended, and read by every
    /// entry point before it touches the world.
    ended: Arc<AtomicBool>,
    path: String,
    id: FileId,
    handle: u64,
    mode: OpenMode,
}

impl SimFile {
    /// The file this handle names (E-HANDLE): fixed at `open`.
    pub fn id(&self) -> FileId {
        self.id
    }

    /// E4's accept hook (card X-5 1.4): the store accepts the bytes
    /// it just read at this range as valid. Not an Env call, not a
    /// yield point (like E4's `E4Env::accepted`): one gated step, no
    /// baton offer. `Err` is a world failure the caller propagates —
    /// an accept that silently fails to record fails open. A range
    /// whose `off + len` overflows is `Io { InvalidInput }` before any
    /// disk call: the copied hook computes the end unchecked, with the
    /// world held (A-1b-F1 (e)).
    pub fn accepted(&self, off: u64, len: u64) -> Result<(), EnvError> {
        if off.checked_add(len).is_none() {
            return Err(EnvError::Io {
                op: "accepted",
                path: self.path.clone(),
                kind: std::io::ErrorKind::InvalidInput,
                detail: "offset plus length overflows u64".to_string(),
            });
        }
        // The life-ended check (Work 10) comes before the task and the
        // acquisition; the overflow refusal above is an argument check
        // that touches nothing.
        life_ended(&self.ended)?;
        let task = current_task(&self.sched)?;
        let mut guard = self
            .sched
            .acquire(task, "env:accepted")
            .map_err(|e| fail(&self.sched, e))?;
        let w = &mut *guard;
        let disk = w.disk_mut()?;
        disk.accepted(self.id, off, len);
        Ok(())
    }

    /// E-OPEN's read-only refusal, before the yield and everything
    /// else.
    fn refuse_read_only(&self) -> Result<(), EnvError> {
        if self.mode == OpenMode::Read {
            Err(EnvError::ReadOnly {
                path: self.path.clone(),
            })
        } else {
            Ok(())
        }
    }
}

impl File for SimFile {
    fn read_at(&self, off: u64, buf: &mut [u8]) -> Result<usize, EnvError> {
        let p = self.path.clone();
        yield_step(&self.ended, &self.sched, "env:read_at", move |w| {
            w.read_at(self.id, off, buf, &p)
        })
    }

    fn write_at(&self, off: u64, buf: &[u8]) -> Result<(), EnvError> {
        self.refuse_read_only()?;
        let p = self.path.clone();
        yield_step(&self.ended, &self.sched, "env:write_at", move |w| {
            // E-IO's zero-length write (A-1a-F3): after the `Read`
            // refusal and the leading yield, `Ok` without a disk
            // call — E4's `SimDisk::write_at` would extend the file
            // to `off`, and `disk.rs` stays as copied.
            if buf.is_empty() {
                return Ok(());
            }
            // The length bound and cap (A-1b-0-F2), checked here so
            // no huge length reaches the copied disk code (whose
            // `resize` would abort past `isize::MAX`).
            let end = off.checked_add(buf.len() as u64).ok_or_else(|| {
                io_invalid_input("write_at", &p, "offset plus length exceeds i64::MAX")
            })?;
            check_lengths("write_at", &p, end)?;
            w.write_at(self.id, off, buf, &p)
        })
    }

    fn sync_data(&self) -> Result<(), EnvError> {
        self.refuse_read_only()?;
        let p = self.path.clone();
        yield_step(&self.ended, &self.sched, "env:sync_data", move |w| {
            w.sync(self.id, false, &p)
        })
    }

    fn sync_all(&self) -> Result<(), EnvError> {
        self.refuse_read_only()?;
        let p = self.path.clone();
        yield_step(&self.ended, &self.sched, "env:sync_all", move |w| {
            w.sync(self.id, true, &p)
        })
    }

    fn len(&self) -> Result<u64, EnvError> {
        yield_step(&self.ended, &self.sched, "env:len", |w| w.cur_len(self.id))
    }

    fn set_len(&self, len: u64) -> Result<(), EnvError> {
        self.refuse_read_only()?;
        let p = self.path.clone();
        yield_step(&self.ended, &self.sched, "env:set_len", move |w| {
            // The same bound and cap as `write_at` (A-1b-0-F2).
            check_lengths("set_len", &p, len)?;
            w.set_len(self.id, len, &p)
        })
    }

    fn lock_exclusive(&self) -> Result<(), EnvError> {
        yield_step(&self.ended, &self.sched, "env:lock_exclusive", |w| {
            // E-LOCK's same-handle rule (A-1b-0-F3): the held mode
            // again is Ok; the other mode is Locked and the handle
            // keeps its lock.
            match w.locks.by_handle.get(&self.handle) {
                Some(LockMode::Exclusive) => return Ok(()),
                Some(LockMode::Shared) => {
                    return Err(EnvError::Locked {
                        path: self.path.clone(),
                    });
                }
                None => {}
            }
            let entry = w.locks.by_file.entry(self.id).or_default();
            if entry.exclusive.is_some() || !entry.shared.is_empty() {
                return Err(EnvError::Locked {
                    path: self.path.clone(),
                });
            }
            entry.exclusive = Some(self.handle);
            w.locks.by_handle.insert(self.handle, LockMode::Exclusive);
            Ok(())
        })
    }

    fn lock_shared(&self) -> Result<(), EnvError> {
        yield_step(&self.ended, &self.sched, "env:lock_shared", |w| {
            match w.locks.by_handle.get(&self.handle) {
                Some(LockMode::Shared) => return Ok(()),
                Some(LockMode::Exclusive) => {
                    return Err(EnvError::Locked {
                        path: self.path.clone(),
                    });
                }
                None => {}
            }
            let entry = w.locks.by_file.entry(self.id).or_default();
            if entry.exclusive.is_some() {
                return Err(EnvError::Locked {
                    path: self.path.clone(),
                });
            }
            entry.shared.insert(self.handle);
            w.locks.by_handle.insert(self.handle, LockMode::Shared);
            Ok(())
        })
    }

    fn unlock(&self) -> Result<(), EnvError> {
        yield_step(&self.ended, &self.sched, "env:unlock", |w| {
            w.locks.release(self.handle);
            Ok(())
        })
    }
}

impl Drop for SimFile {
    /// S-ENV "Locks": `Drop` releases the handle's lock with a
    /// non-yielding acquisition (`Sched::lock`, the scheduler's own
    /// blocking form — a dropping handle never offers the baton). It
    /// does nothing once the life has failed, because the table dies
    /// with the life, and nothing once the life has ended and the
    /// disk moved back out.
    fn drop(&mut self) {
        let mut guard = self.sched.lock();
        let w = &mut *guard;
        if w.sched.crashed || w.sched.deadlock || w.disk.is_none() {
            return;
        }
        w.locks.release(self.handle);
    }
}

/// The layer's length bound and cap (A-1b-0-F2, S-ENV "File ops"),
/// checked before any disk call: an end or length past `i64::MAX` is
/// `Io { InvalidInput }`; past [`SIM_MAX_LEN`] it is
/// `Io { FileTooLarge }` — both naming the handle's open-time path.
fn check_lengths(op: &'static str, path: &str, end: u64) -> Result<(), EnvError> {
    if end > i64::MAX as u64 {
        return Err(io_invalid_input(
            op,
            path,
            "offset plus length exceeds i64::MAX",
        ));
    }
    if end > SIM_MAX_LEN {
        return Err(EnvError::Io {
            op,
            path: path.to_string(),
            kind: std::io::ErrorKind::FileTooLarge,
            detail: format!("the simulated file cap is {SIM_MAX_LEN} bytes"),
        });
    }
    Ok(())
}
