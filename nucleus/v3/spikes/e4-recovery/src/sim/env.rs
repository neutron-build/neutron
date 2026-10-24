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
    fn accepted(&mut self, f: FileId, off: u64, len: u64);
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
    /// violations instead of blocking).
    fn step<T>(
        &self,
        site: &'static str,
        f: impl FnOnce(&mut H) -> Result<T, String>,
    ) -> Result<T, SimFail> {
        let mut g = self.sched.acquire(site)?;
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

    fn accepted(&mut self, f: FileId, off: u64, len: u64) {
        let mut g = match self.sched.acquire("env:accepted") {
            Ok(g) => g,
            Err(_) => return,
        };
        g.disk().accepted(f, off, len);
    }
}
