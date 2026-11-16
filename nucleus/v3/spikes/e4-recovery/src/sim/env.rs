#![deny(clippy::indexing_slicing)]
//! `E4Env`: the narrow I/O trait every store-side call goes through (card
//! Goal, "Substrate"), plus the simulated implementation over the baton
//! scheduler.
//!
//! Every method is a **yield point** (card 1.2: "every `E4Env` call"): it
//! first offers the baton to the scheduler — where the crash point may
//! fire, so a crashed task's operation never executes — then performs the
//! operation while holding the world lock, acquired per S-BATON rule 4
//! (card X-5b Work 11): a `try_lock` whose violation reports
//! [`SimFail::Baton`] naming the call site instead of hanging.
//!
//! The two exceptions, both **not** yield points (they take the world
//! under rule 4 and return without offering the baton):
//! [`E4Env::accepted`] (card 1.4's hook, a bookkeeping record) and
//! [`E4Env::wal_names`] (E4-R2's G3 probe listing — the card's Limit 4:
//! "at start 0 the G3 probe is yield-free").
//!
//! The trait is deliberately shaped like the env surface A-1a/A-1b will
//! provide (`open/create/rename/unlink/sync_dir` for files, plus
//! `read_at/write_at/set_len/sync_data/sync_all`), so swapping in
//! `nucleus-env`'s SimEnv is mechanical if the owner pulls A-1a/A-1b
//! forward.

use crate::sim::disk::SimDisk;
use crate::sim::sched::{Sched, SchedHost, SimFail, TaskId};

/// A file handle handed out by [`E4Env::open`]/[`E4Env::create`].
pub type FileId = usize;

/// The page-file path.
pub const PAGE_FILE: &str = "/pages";
/// The WAL directory.
pub const WAL_DIR: &str = "/wal/";
/// The root directory.
pub const ROOT_DIR: &str = "/";
/// A host that carries the simulated disk (the world's disk slice).
pub trait HasDisk {
    /// The disk slice.
    fn disk(&mut self) -> &mut SimDisk;
}

/// The store-side I/O surface. `Err` is [`SimFail`] (the crash or a
/// deadlock); the simulated disk itself does not fail otherwise.
pub trait E4Env {
    /// Create a file (volatile until `sync_dir` of its directory).
    fn create(&mut self, path: &str) -> Result<FileId, SimFail>;
    /// Open an existing file.
    fn open(&mut self, path: &str) -> Result<FileId, SimFail>;
    /// Whether `path` exists in the current view (card X-5c-F3, class
    /// C-XX's G3 family). A yield point like every env call, so a
    /// crash delivered at it surfaces as `Err(SimFail)`; a missing file
    /// is `Ok(false)`, never an error. `open` cannot make this
    /// distinction: a delivered crash at open's yield and a missing
    /// file both arrive as `Err(SimFail::Crashed)`, so a caller that
    /// reads `open`'s error as "does not exist" turns a crash into a
    /// corruption report (the G3 check reported "start segment missing"
    /// for a nested recovery crash delivered at its own yield).
    fn exists(&mut self, path: &str) -> Result<bool, SimFail>;
    /// Rename a file within its directory.
    fn rename(&mut self, from: &str, to: &str) -> Result<(), SimFail>;
    /// Unlink a file.
    fn unlink(&mut self, path: &str) -> Result<(), SimFail>;
    /// Make a directory's bindings durable.
    fn sync_dir(&mut self, dir: &str) -> Result<(), SimFail>;
    /// Read `len` bytes at `off` (the current view).
    fn read_at(&mut self, f: FileId, off: u64, len: u64) -> Result<Vec<u8>, SimFail>;
    /// Write `bytes` at `off`.
    fn write_at(&mut self, f: FileId, off: u64, bytes: &[u8]) -> Result<(), SimFail>;
    /// Set the file length (metadata).
    fn set_len(&mut self, f: FileId, n: u64) -> Result<(), SimFail>;
    /// `fdatasync`: data plus the length growth needed to read it back.
    fn sync_data(&mut self, f: FileId) -> Result<(), SimFail>;
    /// `fsync`: data, length and metadata.
    fn sync_all(&mut self, f: FileId) -> Result<(), SimFail>;
    /// The accept hook (card 1.4): the store accepts the bytes it just
    /// read at this range as valid. Not an I/O op; not a yield point.
    /// `Err` is a world-acquisition failure ([`SimFail::Baton`]) — the
    /// caller must propagate it: an accept that silently fails to record
    /// fails open on an oracle input (the X-5b gate's minor finding).
    fn accepted(&mut self, f: FileId, off: u64, len: u64) -> Result<(), SimFail>;
    /// **E4-R2 (card E4-SILENT-1): the names currently bound in `/wal/`**
    /// (sorted), for the store's G3 probe at `wal_start_lsn == 0`.
    /// **Not a yield point** (unlike every I/O op above): a listing of
    /// the current directory view cannot hand the baton elsewhere — the
    /// verification's probe form (which called `exists` here and so
    /// yielded at every start-0 open) moved 19 seeds' `nested` draws
    /// alone; this form adds no yield on any path a seed reaches. It
    /// still acquires the world under S-BATON rule 4, so `Err` is a
    /// world-acquisition failure exactly as for [`Self::accepted`].
    fn wal_names(&mut self) -> Result<Vec<String>, SimFail>;
}

/// A bound view of the world for one task: store code obtains it from its
/// execution context and every I/O call goes through it.
pub struct EnvRef<'a, H: SchedHost + HasDisk> {
    /// The world's scheduler (which owns the world lock).
    pub sched: &'a Sched<H>,
    /// The calling task.
    pub task: TaskId,
}

impl<'a, H: SchedHost + HasDisk> EnvRef<'a, H> {
    /// A plain yield point (used for `write_guard`, gate attempts, WAL
    /// enqueues and commit boundaries — card 1.2).
    pub fn yield_now(&mut self) -> Result<(), SimFail> {
        self.sched.yield_baton(self.task)
    }

    /// A plain yield point naming its call site (S-BATON rule 4: a park
    /// under a held world reports [`SimFail::Baton`] instead of hanging).
    pub fn yield_at(&mut self, site: &'static str) -> Result<(), SimFail> {
        self.sched.yield_baton_at(self.task, site)
    }

    /// Park on a wait key (the scheduler-level condition variable).
    pub fn wait_for(&mut self, key: crate::sim::sched::WaitKey) -> Result<(), SimFail> {
        self.sched.wait_for(self.task, key)
    }

    /// One step body under the world lock acquired per S-BATON rule 4
    /// (every env call's host acquisition is a `try_lock` that reports
    /// violations instead of blocking). The task id feeds X5-R14's
    /// S-DET gate in [`Sched::acquire`].
    fn step<T>(
        &self,
        site: &'static str,
        f: impl FnOnce(&mut H) -> Result<T, String>,
    ) -> Result<T, SimFail> {
        let mut g = self.sched.acquire(self.task, site)?;
        f(&mut g).map_err(|_| SimFail::Crashed)
    }
}

impl<'a, H: SchedHost + HasDisk> E4Env for EnvRef<'a, H> {
    fn create(&mut self, path: &str) -> Result<FileId, SimFail> {
        self.sched.yield_baton_at(self.task, "env:create")?;
        self.step("env:create", |h| h.disk().create(path))
    }

    fn open(&mut self, path: &str) -> Result<FileId, SimFail> {
        self.sched.yield_baton_at(self.task, "env:open")?;
        self.step("env:open", |h| h.disk().open(path))
    }

    fn exists(&mut self, path: &str) -> Result<bool, SimFail> {
        self.sched.yield_baton_at(self.task, "env:exists")?;
        self.step("env:exists", |h| Ok(h.disk().stat(path).is_some()))
    }

    fn rename(&mut self, from: &str, to: &str) -> Result<(), SimFail> {
        self.sched.yield_baton_at(self.task, "env:rename")?;
        self.step("env:rename", |h| h.disk().rename(from, to))
    }

    fn unlink(&mut self, path: &str) -> Result<(), SimFail> {
        self.sched.yield_baton_at(self.task, "env:unlink")?;
        self.step("env:unlink", |h| h.disk().unlink(path))
    }

    fn sync_dir(&mut self, dir: &str) -> Result<(), SimFail> {
        self.sched.yield_baton_at(self.task, "env:sync_dir")?;
        self.step("env:sync_dir", |h| h.disk().sync_dir(dir))
    }

    fn read_at(&mut self, f: FileId, off: u64, len: u64) -> Result<Vec<u8>, SimFail> {
        self.sched.yield_baton_at(self.task, "env:read_at")?;
        self.step("env:read_at", |h| h.disk().read_at(f, off, len))
    }

    fn write_at(&mut self, f: FileId, off: u64, bytes: &[u8]) -> Result<(), SimFail> {
        self.sched.yield_baton_at(self.task, "env:write_at")?;
        self.step("env:write_at", |h| h.disk().write_at(f, off, bytes))
    }

    fn set_len(&mut self, f: FileId, n: u64) -> Result<(), SimFail> {
        self.sched.yield_baton_at(self.task, "env:set_len")?;
        self.step("env:set_len", |h| h.disk().set_len(f, n))
    }

    fn sync_data(&mut self, f: FileId) -> Result<(), SimFail> {
        self.sched.yield_baton_at(self.task, "env:sync_data")?;
        self.step("env:sync_data", |h| h.disk().sync_data(f))
    }

    fn sync_all(&mut self, f: FileId) -> Result<(), SimFail> {
        self.sched.yield_baton_at(self.task, "env:sync_all")?;
        self.step("env:sync_all", |h| h.disk().sync_all(f))
    }

    fn accepted(&mut self, f: FileId, off: u64, len: u64) -> Result<(), SimFail> {
        let mut g = self.sched.acquire(self.task, "env:accepted")?;
        g.disk().accepted(f, off, len);
        Ok(())
    }

    fn wal_names(&mut self) -> Result<Vec<String>, SimFail> {
        // E4-R2: built on `step` with no yield (see the trait's doc).
        self.step("env:wal_names", |h| Ok(h.disk().dir_names(WAL_DIR)))
    }
}
