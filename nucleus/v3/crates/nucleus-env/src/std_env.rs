//! `StdEnv`: the real-filesystem backend, pread/pwrite, advisory locks
//! (PLAN-v3 §3.2; card A-1a Work 5).
//!
//! Invariants owned, on the real filesystem: **E-OPEN**, **E-IO**,
//! **E-DURABLE**, **E-NAMESPACE**, **E-LOCK** and **E-CLOCK/RAND** as
//! stated in the crate root. S-STORE §2 routes every store I/O here;
//! §12.5's writer rule (the segment `sync_all` and its `wal/`
//! directory entry synced) rests on [`StdEnv::sync_dir`] and
//! [`crate::File::sync_all`]; §14.1's "No writer" rests on the flock
//! modes of [`crate::File::lock_exclusive`] and
//! [`crate::File::lock_shared`].
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
/// everything else is `Io`).
fn map_io(op: &'static str, path: &str, e: std::io::Error) -> EnvError {
    match e.kind() {
        ErrorKind::NotFound => EnvError::NotFound {
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
            Ok(file) => Ok(StdFile {
                file,
                path: path.to_string(),
                mode,
            }),
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
        // Refuse an existing target before renaming, so a store bug
        // surfaces as AlreadyExists instead of a silent replace
        // (E-NAMESPACE). The check-then-rename race is excluded by
        // N11's single owner: the renamer holds the file's exclusive
        // lock, so no other handle can create the target between them.
        match std::fs::symlink_metadata(self.full(to)) {
            Ok(_) => {
                return Err(EnvError::AlreadyExists {
                    path: to.to_string(),
                });
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => return Err(map_io("stat", to, e)),
        }
        std::fs::rename(self.full(from), self.full(to)).map_err(|e| map_io("rename", from, e))
    }

    fn remove(&self, path: &str) -> Result<(), EnvError> {
        path::check(path)?;
        std::fs::remove_file(self.full(path)).map_err(|e| map_io("remove", path, e))
    }

    fn exists(&self, path: &str) -> Result<bool, EnvError> {
        path::check(path)?;
        match std::fs::symlink_metadata(self.full(path)) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
            Err(e) => Err(map_io("stat", path, e)),
        }
    }

    fn list_dir(&self, path: &str) -> Result<Vec<String>, EnvError> {
        path::check_dir(path)?;
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
        // One pread can return short (or Interrupted); loop until the
        // buffer is full or end-of-file, so the count is short only at
        // the end (E-IO).
        let mut total = 0;
        while total < buf.len() {
            let Some(tail) = buf.get_mut(total..) else {
                // total < buf.len() held on entry, so this is
                // unreachable; treated as end-of-file.
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
