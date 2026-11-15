//! `StdEnv`: the real-filesystem backend, pread/pwrite, advisory locks
//! (PLAN-v3 §3.2; card A-1a Work 5).
//!
//! Invariants owned, on the real filesystem: **E-OPEN**, **E-IO**,
//! **E-DURABLE**, **E-NAMESPACE**, **E-LOCK**, **E-HANDLE**, **E-KIND**
//! and **E-CLOCK/RAND** as stated in the crate root. S-STORE §2 routes
//! every store I/O here; §12.5's writer rule (the segment `sync_all`
//! and its `wal/` directory entry synced) rests on [`StdEnv::sync_dir`]
//! and [`crate::File::sync_all`]; §14.1's "No writer" rests on the
//! flock modes of [`crate::File::lock_exclusive`] and
//! [`crate::File::lock_shared`]. **E-HANDLE** is POSIX open-file
//! semantics for free: a descriptor reaches its inode across a rename
//! and after an unlink, and flock locks the inode, so a lock moves
//! with a rename (card A-1b-0 Work 4's alignment of the backends).
//! **E-KIND**'s refusals come from explicit metadata checks on the
//! opened descriptor, never from a raw errno, so the
//! `NotADirectory`-of-path-resolution mapping in [`map_io`] cannot
//! hide them.
//!
//! **Unix only** (`#[cfg(unix)]` in `lib.rs`): the wasm32 target is not
//! unix, so this file's platform calls never reach the browser build,
//! and MemEnv stays the wasm-safe backend. Windows is an open point
//! from A-1a's report; the crate builds there without `StdEnv`.

use crate::error::EnvError;
use crate::mem::{SPLITMIX64_GAMMA, splitmix64_output};
use crate::path;
use crate::{Env, File, OpenMode};
use std::collections::hash_map::RandomState;
use std::fs::{File as StdFsFile, OpenOptions, TryLockError};
use std::hash::{BuildHasher, Hasher};
use std::io::ErrorKind;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// The real-filesystem backend: every validated path is joined to one
/// root directory, and all I/O is positioned I/O on the resulting file.
#[derive(Debug)]
pub struct StdEnv {
    root: PathBuf,
    start: Instant,
    rand: AtomicU64,
}

/// An open file on the real filesystem: the descriptor, the validated
/// path (for error context), and the open mode (E-OPEN's read-only
/// refusals).
#[derive(Debug)]
pub struct StdFile {
    file: StdFsFile,
    path: String,
    mode: OpenMode,
}

/// Maps a platform error onto the [`EnvError`] surface (card A-1a
/// Work 5: `NotFound`, `AlreadyExists`, `StorageFull` and
/// `QuotaExceeded` map to their variants, the last two to `NoSpace`;
/// everything else is `Io`). `NotADirectory` from path resolution
/// means a component of the path is a file, so the path names nothing:
/// it maps to `NotFound` (E-KIND, card A-1b-0). E-KIND's target-kind
/// refusals come from explicit metadata checks, never from this
/// mapping.
fn map_io(op: &'static str, path: &str, e: std::io::Error) -> EnvError {
    match e.kind() {
        ErrorKind::NotFound | ErrorKind::NotADirectory => EnvError::NotFound {
            path: path.to_string(),
        },
        ErrorKind::AlreadyExists => EnvError::AlreadyExists {
            path: path.to_string(),
        },
        ErrorKind::StorageFull | ErrorKind::QuotaExceeded => EnvError::NoSpace {
            op,
            path: path.to_string(),
        },
        kind => EnvError::Io {
            op,
            path: path.to_string(),
            kind,
            detail: e.to_string(),
        },
    }
}

/// The E-KIND refusal for an opened path that is a directory where a
/// file is required.
fn is_a_directory(op: &'static str, path: &str) -> EnvError {
    EnvError::Io {
        op,
        path: path.to_string(),
        kind: ErrorKind::IsADirectory,
        detail: "is a directory".to_string(),
    }
}

/// The E-KIND refusal for an opened path that is a file where a
/// directory is required.
fn not_a_directory(op: &'static str, path: &str) -> EnvError {
    EnvError::Io {
        op,
        path: path.to_string(),
        kind: ErrorKind::NotADirectory,
        detail: "not a directory".to_string(),
    }
}

/// The parent directory of a validated path: `""` for a root entry.
fn parent_of(path: &str) -> &str {
    match path.rsplit_once('/') {
        Some((parent, _)) => parent,
        None => "",
    }
}

/// Seeds `rand` per process from the OS-seeded hasher, fed the process
/// id (card A-1a Work 5: no new dependency).
fn seed_from_process() -> u64 {
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u32(std::process::id());
    hasher.finish()
}

impl StdEnv {
    /// Creates a backend rooted at `root`, which must exist and be a
    /// directory: the store's own directory (S-STORE §2: the page file
    /// plus `wal/`). A missing root is [`EnvError::NotFound`]; a root
    /// that is not a directory is [`EnvError::InvalidPath`].
    pub fn new(root: impl Into<PathBuf>) -> Result<StdEnv, EnvError> {
        let root = root.into();
        let shown = root.display().to_string();
        match std::fs::metadata(&root) {
            Ok(m) if m.is_dir() => Ok(StdEnv {
                root,
                start: Instant::now(),
                rand: AtomicU64::new(seed_from_process()),
            }),
            Ok(_) => Err(EnvError::InvalidPath {
                path: shown,
                why: "root is not a directory",
            }),
            Err(e) if e.kind() == ErrorKind::NotFound => Err(EnvError::NotFound { path: shown }),
            Err(e) => Err(map_io("stat", &shown, e)),
        }
    }

    /// A validated path joined to the root; `""` (the root itself)
    /// joins to the root.
    fn full(&self, path: &str) -> PathBuf {
        self.root.join(path)
    }
}

impl Env for StdEnv {
    type File = StdFile;

    fn open(&self, path: &str, mode: OpenMode) -> Result<StdFile, EnvError> {
        path::check(path)?;
        let full = self.full(path);
        let file = match mode {
            OpenMode::Read => StdFsFile::open(&full),
            OpenMode::ReadWrite => OpenOptions::new().read(true).write(true).open(&full),
            OpenMode::CreateNew => OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&full),
        };
        match file {
            Ok(file) => {
                // E-KIND: check the opened descriptor's metadata in
                // every mode, so a directory is refused the same way
                // everywhere (a bare `Read` open of a directory would
                // otherwise succeed on Linux).
                let meta = file.metadata().map_err(|e| map_io("open", path, e))?;
                if meta.is_dir() {
                    return Err(is_a_directory("open", path));
                }
                Ok(StdFile {
                    file,
                    path: path.to_string(),
                    mode,
                })
            }
            Err(e) => Err(map_io("open", path, e)),
        }
    }

    fn create_dir(&self, path: &str) -> Result<(), EnvError> {
        path::check(path)?;
        std::fs::create_dir(self.full(path)).map_err(|e| map_io("create_dir", path, e))
    }

    fn sync_dir(&self, path: &str) -> Result<(), EnvError> {
        path::check_dir(path)?;
        // Open the directory itself and fsync it: this makes the
        // directory's entries durable and nothing else (E-DURABLE;
        // S-STORE §12.5's "wal/ directory entry synced through Env").
        let dir = StdFsFile::open(self.full(path)).map_err(|e| map_io("open", path, e))?;
        // E-KIND: a file here would fsync the file and return Ok, so
        // the opened descriptor's metadata is checked first — a caller
        // that passes `wal/<seg>.log` for `wal` is told (A-1a-F3).
        let meta = dir.metadata().map_err(|e| map_io("metadata", path, e))?;
        if !meta.is_dir() {
            return Err(not_a_directory("sync_dir", path));
        }
        dir.sync_all().map_err(|e| map_io("sync_dir", path, e))
    }

    fn rename(&self, from: &str, to: &str) -> Result<(), EnvError> {
        path::check(from)?;
        path::check(to)?;
        if parent_of(from) != parent_of(to) {
            return Err(EnvError::InvalidPath {
                path: format!("{from} -> {to}"),
                why: "cross-directory rename",
            });
        }
        // E-KIND's order: the source first. A missing source, or one
        // under a file, is `NotFound` naming the source (map_io maps
        // path-resolution `NotADirectory` to `NotFound`); a directory
        // source is refused, because `rename` moves a file. The check
        // does not lean on a platform errno (macOS gives `EPERM` here).
        match std::fs::symlink_metadata(self.full(from)) {
            Ok(m) if m.is_dir() => return Err(is_a_directory("rename", from)),
            Ok(_) => {}
            Err(e) => return Err(map_io("stat", from, e)),
        }
        // Refuse an existing target before renaming, so a store bug
        // surfaces as AlreadyExists instead of a silent replace
        // (E-NAMESPACE). The check-then-act race is excluded by
        // N11's single owner: the renamer holds the file's exclusive
        // lock, so no other handle can create the target between the
        // checks and the rename, for `remove` as well as `rename`.
        match std::fs::symlink_metadata(self.full(to)) {
            Ok(_) => {
                return Err(EnvError::AlreadyExists {
                    path: to.to_string(),
                });
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => return Err(map_io("stat", to, e)),
        }
        // A failure of the rename itself names both ends, the form the
        // cross-directory `InvalidPath` already uses (A-1a-F3, nit 2).
        let shown = format!("{from} -> {to}");
        std::fs::rename(self.full(from), self.full(to)).map_err(|e| map_io("rename", &shown, e))
    }

    fn remove(&self, path: &str) -> Result<(), EnvError> {
        path::check(path)?;
        // E-KIND: refuse a directory from the metadata, not from the
        // platform's errno (Linux gives EISDIR but macOS gives EPERM).
        // The check-then-remove race is excluded by N11's single
        // owner, as for `rename` above.
        match std::fs::symlink_metadata(self.full(path)) {
            Ok(m) if m.is_dir() => return Err(is_a_directory("remove", path)),
            Ok(_) => {}
            Err(e) => return Err(map_io("stat", path, e)),
        }
        std::fs::remove_file(self.full(path)).map_err(|e| map_io("remove", path, e))
    }

    fn exists(&self, path: &str) -> Result<bool, EnvError> {
        path::check(path)?;
        match std::fs::symlink_metadata(self.full(path)) {
            Ok(_) => Ok(true),
            // E-NAMESPACE, E-KIND: a missing path — or a path under a
            // file, which names nothing — is Ok(false), never an
            // error.
            Err(e) if matches!(e.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => {
                Ok(false)
            }
            Err(e) => Err(map_io("stat", path, e)),
        }
    }

    fn list_dir(&self, path: &str) -> Result<Vec<String>, EnvError> {
        path::check_dir(path)?;
        // E-KIND: check the opened descriptor's metadata, so a file
        // here is `Io { NotADirectory }` rather than `Ok` with nothing
        // listed (A-1a-F3). The check-then-read race is excluded by
        // N11's single owner, as for `rename` above.
        let probe = StdFsFile::open(self.full(path)).map_err(|e| map_io("open", path, e))?;
        let meta = probe.metadata().map_err(|e| map_io("metadata", path, e))?;
        if !meta.is_dir() {
            return Err(not_a_directory("list_dir", path));
        }
        drop(probe);
        let entries = std::fs::read_dir(self.full(path)).map_err(|e| map_io("open", path, e))?;
        let mut names = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| map_io("read_dir", path, e))?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                return Err(EnvError::Io {
                    op: "list_dir",
                    path: path.to_string(),
                    kind: ErrorKind::InvalidData,
                    detail: format!("non-UTF-8 name {:?}", name),
                });
            };
            names.push(name.to_string());
        }
        names.sort();
        Ok(names)
    }

    fn now(&self) -> Duration {
        self.start.elapsed()
    }

    fn sleep(&self, d: Duration) {
        std::thread::sleep(d);
    }

    fn rand(&self) -> u64 {
        // Counter-based splitmix64: one atomic fetch-add advances the
        // state (wrapping addition is defined), the shared output
        // function mixes it. No lock, no CAS loop.
        let state = self
            .rand
            .fetch_add(SPLITMIX64_GAMMA, Ordering::Relaxed)
            .wrapping_add(SPLITMIX64_GAMMA);
        splitmix64_output(state)
    }
}

impl StdFile {
    /// The read-only refusal of E-OPEN: write-class operations fail
    /// before any I/O on a `Read` handle.
    fn refuse_read_only(&self) -> Result<(), EnvError> {
        if self.mode == OpenMode::Read {
            Err(EnvError::ReadOnly {
                path: self.path.clone(),
            })
        } else {
            Ok(())
        }
    }

    /// Maps a lock failure: `WouldBlock` is the non-blocking conflict
    /// of E-LOCK; anything else is `Io`.
    fn map_lock(op: &'static str, path: &str, e: TryLockError) -> EnvError {
        match e {
            TryLockError::WouldBlock => EnvError::Locked {
                path: path.to_string(),
            },
            TryLockError::Error(io) => map_io(op, path, io),
        }
    }
}

impl File for StdFile {
    fn read_at(&self, off: u64, buf: &mut [u8]) -> Result<usize, EnvError> {
        // E-IO: an offset at or past `i64::MAX` is past every
        // end-of-file (pread itself refuses it with InvalidInput), so
        // it reads 0 (A-1a-F3). Linux `pread` is also EINVAL whenever
        // `off + count` would overflow `loff_t`, so A-1b-0's guard
        // alone still failed just below `i64::MAX`; the count is
        // capped to keep `off + count` within it. A read capped there
        // is still past every end-of-file, so the answer is 0 either
        // way and no platform refusal can surface (A-1b-0-F1).
        let cap = (i64::MAX as u64).saturating_sub(off);
        if cap == 0 {
            return Ok(0);
        }
        let want = buf.len().min(usize::try_from(cap).unwrap_or(usize::MAX));
        // One pread can return short (or Interrupted); loop until the
        // buffer is full or end-of-file, so the count is short only at
        // the end (E-IO).
        let mut total = 0;
        while total < want {
            let Some(tail) = buf.get_mut(total..want) else {
                // total < want held on entry, so this is unreachable;
                // treated as end-of-file.
                break;
            };
            match self.file.read_at(tail, off + (total as u64)) {
                Ok(0) => break,
                Ok(n) => total += n,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(map_io("read_at", &self.path, e)),
            }
        }
        Ok(total)
    }

    fn write_at(&self, off: u64, buf: &[u8]) -> Result<(), EnvError> {
        self.refuse_read_only()?;
        self.file
            .write_all_at(buf, off)
            .map_err(|e| map_io("write_at", &self.path, e))
    }

    fn sync_data(&self) -> Result<(), EnvError> {
        self.refuse_read_only()?;
        // On macOS the platform's data sync issues `fcntl(F_FULLFSYNC)`,
        // so PLAN-v3 §3.2's macOS rule needs no code here (no libc).
        self.file
            .sync_data()
            .map_err(|e| map_io("sync_data", &self.path, e))
    }

    fn sync_all(&self) -> Result<(), EnvError> {
        self.refuse_read_only()?;
        // On macOS the platform's file sync issues `fcntl(F_FULLFSYNC)`
        // likewise.
        self.file
            .sync_all()
            .map_err(|e| map_io("sync_all", &self.path, e))
    }

    fn len(&self) -> Result<u64, EnvError> {
        self.file
            .metadata()
            .map(|m| m.len())
            .map_err(|e| map_io("metadata", &self.path, e))
    }

    fn set_len(&self, len: u64) -> Result<(), EnvError> {
        self.refuse_read_only()?;
        self.file
            .set_len(len)
            .map_err(|e| map_io("set_len", &self.path, e))
    }

    fn lock_exclusive(&self) -> Result<(), EnvError> {
        // flock semantics: works on a read-only descriptor, and two
        // descriptors of one process conflict (the dry run of card
        // A-1a's author). Non-blocking: a conflict is Locked at once.
        self.file
            .try_lock()
            .map_err(|e| Self::map_lock("lock_exclusive", &self.path, e))
    }

    fn lock_shared(&self) -> Result<(), EnvError> {
        self.file
            .try_lock_shared()
            .map_err(|e| Self::map_lock("lock_shared", &self.path, e))
    }

    fn unlock(&self) -> Result<(), EnvError> {
        // `unlock` reports a plain I/O error (no WouldBlock variant:
        // unlocking a lock the handle does not hold is a no-op on the
        // platform, as E-LOCK requires).
        self.file
            .unlock()
            .map_err(|e| map_io("unlock", &self.path, e))
    }
}
