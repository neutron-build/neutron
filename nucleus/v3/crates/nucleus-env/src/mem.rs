//! `MemEnv`: the wasm-safe in-memory backend for unit tests (PLAN-v3
//! §3.2; card A-1a Work 4).
//!
//! Invariants owned, in memory: **E-OPEN**, **E-IO**, **E-NAMESPACE**,
//! **E-LOCK** and **E-CLOCK/RAND** as stated in the crate root.
//! **E-DURABLE** is the one this backend cannot exercise: MemEnv has no
//! crash, so `sync_data`, `sync_all` and `sync_dir` are no-ops (they
//! still refuse on read-only handles, per E-OPEN). A-1b's SimEnv wraps
//! this state with a seeded scheduler whose power cut drops, tears,
//! loses or misdirects unsynced writes, and makes E-DURABLE's losses
//! observable (S-STORE §12.5; `verdicts/S-2-F2.md` "Follow-up").
//!
//! Wasm safety: no filesystem, no platform clock, no OS randomness, no
//! thread parking — a virtual clock, a seeded splitmix64 counter and a
//! `std::thread::yield_now` in [`MemEnv::sleep`] are the whole of it.
//! (Card A-1a's rule that only `std_env.rs` may name the platform's
//! filesystem and time APIs keeps it that way.)

use crate::error::EnvError;
use crate::path;
use crate::{Env, File, OpenMode};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::Duration;

/// The whole namespace and the virtual clock and rng, shared by every
/// clone and every open handle of one [`MemEnv`] family.
struct State {
    /// File path → contents and lock state.
    files: BTreeMap<String, FileData>,
    /// Directory paths, the root `""` included.
    dirs: BTreeSet<String>,
    /// The next per-handle id (locks are keyed by handle, not by file,
    /// so two handles of one file hold distinct locks).
    next_handle: u64,
    /// The virtual clock, in nanoseconds since `MemEnv::new`.
    clock_ns: u64,
    /// The splitmix64 state behind [`MemEnv::rand`].
    rand: u64,
}

/// One in-memory file: its bytes and its lock holders (E-LOCK).
struct FileData {
    /// The file's contents.
    data: Vec<u8>,
    /// The handle id holding the exclusive lock, if any.
    exclusive: Option<u64>,
    /// The handle ids holding the shared lock.
    shared: BTreeSet<u64>,
}

/// The in-memory backend: files and directories in `BTreeMap`s behind
/// one mutex (PLAN-v3 §3.2).
///
/// `Clone` shares the namespace — every clone and every handle of one
/// family sees the same files — so a test can drop every handle and
/// reopen, as S-STORE §2's open does.
#[derive(Clone)]
pub struct MemEnv {
    state: Arc<Mutex<State>>,
}

/// An open file in a [`MemEnv`]: its mode and its per-handle lock id.
pub struct MemFile {
    state: Arc<Mutex<State>>,
    path: String,
    id: u64,
    mode: OpenMode,
}

/// One splitmix64 step: advances `state` and returns the mixed value.
/// The same function backs both backends' `rand`, so a seed means the
/// same sequence everywhere (E-CLOCK/RAND; card A-1a Works 4–5).
pub(crate) fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Locks the shared state, mapping a poisoned mutex to
/// [`EnvError::Io`] (never `unwrap`; card A-1a Work 4).
fn lock_state<'a>(
    state: &'a Mutex<State>,
    op: &'static str,
    path: &str,
) -> Result<MutexGuard<'a, State>, EnvError> {
    state.lock().map_err(|_| EnvError::Io {
        op,
        path: path.to_string(),
        kind: std::io::ErrorKind::Other,
        detail: "internal mutex poisoned".to_string(),
    })
}

/// Copies `min(tail.len(), buf.len())` bytes and returns the count.
///
/// The two `Option`s cannot be `None` (`n` is the min of both lengths);
/// the pattern only satisfies this crate's no-indexing lint.
fn copy_prefix(tail: &[u8], buf: &mut [u8]) -> usize {
    let n = tail.len().min(buf.len());
    if let (Some(src), Some(dst)) = (tail.get(..n), buf.get_mut(..n)) {
        dst.copy_from_slice(src);
    }
    n
}

/// The parent directory of a validated path: `""` for a root entry.
fn parent_of(path: &str) -> &str {
    match path.rsplit_once('/') {
        Some((parent, _)) => parent,
        None => "",
    }
}

/// The last component of a validated path.
fn basename_of(path: &str) -> &str {
    match path.rsplit_once('/') {
        Some((_, base)) => base,
        None => path,
    }
}

/// Converts a file offset to an index, refusing one a `usize` cannot
/// hold (32-bit wasm included).
fn offset_to_index(off: u64, op: &'static str, path: &str) -> Result<usize, EnvError> {
    usize::try_from(off).map_err(|_| EnvError::Io {
        op,
        path: path.to_string(),
        kind: std::io::ErrorKind::Other,
        detail: "offset out of addressable range".to_string(),
    })
}

impl State {
    /// The parent-directory check of E-NAMESPACE's `create_dir` and
    /// E-OPEN's `CreateNew`: the parent must exist.
    fn parent_exists(&self, path: &str) -> bool {
        self.dirs.contains(parent_of(path))
    }
}

impl MemEnv {
    /// Creates an empty namespace whose virtual clock starts at zero
    /// and whose `rand` is seeded with `seed` (E-CLOCK/RAND): the same
    /// seed yields the same random sequence, so tests replay.
    pub fn new(seed: u64) -> MemEnv {
        let mut dirs = BTreeSet::new();
        dirs.insert(String::new());
        MemEnv {
            state: Arc::new(Mutex::new(State {
                files: BTreeMap::new(),
                dirs,
                next_handle: 1,
                clock_ns: 0,
                rand: seed,
            })),
        }
    }

    /// An E-NAMESPACE refusal for a name that is a directory where a
    /// file is required.
    fn is_a_directory(op: &'static str, path: &str) -> EnvError {
        EnvError::Io {
            op,
            path: path.to_string(),
            kind: std::io::ErrorKind::Other,
            detail: "is a directory".to_string(),
        }
    }
}

impl Env for MemEnv {
    type File = MemFile;

    fn open(&self, path: &str, mode: OpenMode) -> Result<MemFile, EnvError> {
        path::check(path)?;
        let mut state = lock_state(&self.state, "open", path)?;
        match mode {
            OpenMode::Read | OpenMode::ReadWrite => {
                if state.dirs.contains(path) {
                    return Err(Self::is_a_directory("open", path));
                }
                if !state.files.contains_key(path) {
                    return Err(EnvError::NotFound {
                        path: path.to_string(),
                    });
                }
            }
            OpenMode::CreateNew => {
                if state.files.contains_key(path) || state.dirs.contains(path) {
                    return Err(EnvError::AlreadyExists {
                        path: path.to_string(),
                    });
                }
                if !state.parent_exists(path) {
                    return Err(EnvError::NotFound {
                        path: path.to_string(),
                    });
                }
                state.files.insert(
                    path.to_string(),
                    FileData {
                        data: Vec::new(),
                        exclusive: None,
                        shared: BTreeSet::new(),
                    },
                );
            }
        }
        let id = state.next_handle;
        state.next_handle = state.next_handle.wrapping_add(1);
        Ok(MemFile {
            state: Arc::clone(&self.state),
            path: path.to_string(),
            id,
            mode,
        })
    }

    fn create_dir(&self, path: &str) -> Result<(), EnvError> {
        path::check(path)?;
        let mut state = lock_state(&self.state, "create_dir", path)?;
        if state.files.contains_key(path) || state.dirs.contains(path) {
            return Err(EnvError::AlreadyExists {
                path: path.to_string(),
            });
        }
        if !state.parent_exists(path) {
            return Err(EnvError::NotFound {
                path: path.to_string(),
            });
        }
        state.dirs.insert(path.to_string());
        Ok(())
    }

    fn sync_dir(&self, path: &str) -> Result<(), EnvError> {
        path::check_dir(path)?;
        let state = lock_state(&self.state, "sync_dir", path)?;
        if state.dirs.contains(path) {
            Ok(())
        } else {
            Err(EnvError::NotFound {
                path: path.to_string(),
            })
        }
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
        let mut state = lock_state(&self.state, "rename", from)?;
        if state.dirs.contains(from) {
            return Err(Self::is_a_directory("rename", from));
        }
        if !state.files.contains_key(from) {
            return Err(EnvError::NotFound {
                path: from.to_string(),
            });
        }
        if state.files.contains_key(to) || state.dirs.contains(to) {
            return Err(EnvError::AlreadyExists {
                path: to.to_string(),
            });
        }
        if let Some(data) = state.files.remove(from) {
            state.files.insert(to.to_string(), data);
        }
        Ok(())
    }

    fn remove(&self, path: &str) -> Result<(), EnvError> {
        path::check(path)?;
        let mut state = lock_state(&self.state, "remove", path)?;
        if state.files.remove(path).is_some() {
            Ok(())
        } else if state.dirs.contains(path) {
            Err(Self::is_a_directory("remove", path))
        } else {
            Err(EnvError::NotFound {
                path: path.to_string(),
            })
        }
    }

    fn exists(&self, path: &str) -> Result<bool, EnvError> {
        path::check(path)?;
        let state = lock_state(&self.state, "exists", path)?;
        Ok(state.files.contains_key(path) || state.dirs.contains(path))
    }

    fn list_dir(&self, path: &str) -> Result<Vec<String>, EnvError> {
        path::check_dir(path)?;
        let state = lock_state(&self.state, "list_dir", path)?;
        if !state.dirs.contains(path) {
            return Err(EnvError::NotFound {
                path: path.to_string(),
            });
        }
        let mut names = BTreeSet::new();
        for name in state.files.keys().chain(state.dirs.iter()) {
            if parent_of(name) == path {
                names.insert(basename_of(name).to_string());
            }
        }
        Ok(names.into_iter().collect())
    }

    fn now(&self) -> Duration {
        match self.state.lock() {
            Ok(state) => Duration::from_nanos(state.clock_ns),
            // A poisoned mutex cannot advance the clock; `now` cannot
            // fail (E-CLOCK/RAND), so it reports the last value a
            // poisoned holder could have observed: zero. This cannot
            // happen outside a panic inside a lock holder.
            Err(_) => Duration::ZERO,
        }
    }

    fn sleep(&self, d: Duration) {
        if let Ok(mut state) = self.state.lock() {
            let ns = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
            state.clock_ns = state.clock_ns.saturating_add(ns);
        }
        thread::yield_now();
    }

    fn rand(&self) -> u64 {
        match self.state.lock() {
            Ok(mut state) => splitmix64(&mut state.rand),
            Err(_) => 0,
        }
    }
}

impl MemFile {
    /// The file's mutable state, or [`EnvError::NotFound`] if the file
    /// was removed or renamed away under this handle (MemEnv does not
    /// model POSIX unlink semantics for open handles).
    fn file_data<'a>(state: &'a mut State, path: &str) -> Result<&'a mut FileData, EnvError> {
        state.files.get_mut(path).ok_or_else(|| EnvError::NotFound {
            path: path.to_string(),
        })
    }

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
}

impl File for MemFile {
    fn read_at(&self, off: u64, buf: &mut [u8]) -> Result<usize, EnvError> {
        let off = offset_to_index(off, "read_at", &self.path)?;
        let state = lock_state(&self.state, "read_at", &self.path)?;
        let data = &state
            .files
            .get(&self.path)
            .ok_or_else(|| EnvError::NotFound {
                path: self.path.clone(),
            })?
            .data;
        Ok(match data.get(off..) {
            Some(tail) => copy_prefix(tail, buf),
            None => 0,
        })
    }

    fn write_at(&self, off: u64, buf: &[u8]) -> Result<(), EnvError> {
        self.refuse_read_only()?;
        let off = offset_to_index(off, "write_at", &self.path)?;
        let end = off.checked_add(buf.len()).ok_or_else(|| EnvError::Io {
            op: "write_at",
            path: self.path.clone(),
            kind: std::io::ErrorKind::Other,
            detail: "offset plus length out of addressable range".to_string(),
        })?;
        let mut state = lock_state(&self.state, "write_at", &self.path)?;
        let data = &mut Self::file_data(&mut state, &self.path)?.data;
        if data.len() < end {
            data.resize(end, 0);
        }
        if let Some(dst) = data.get_mut(off..end) {
            dst.copy_from_slice(buf);
        }
        Ok(())
    }

    fn sync_data(&self) -> Result<(), EnvError> {
        self.refuse_read_only()?;
        Ok(())
    }

    fn sync_all(&self) -> Result<(), EnvError> {
        self.refuse_read_only()?;
        Ok(())
    }

    fn len(&self) -> Result<u64, EnvError> {
        let state = lock_state(&self.state, "len", &self.path)?;
        let data = &state
            .files
            .get(&self.path)
            .ok_or_else(|| EnvError::NotFound {
                path: self.path.clone(),
            })?
            .data;
        Ok(data.len() as u64)
    }

    fn set_len(&self, len: u64) -> Result<(), EnvError> {
        self.refuse_read_only()?;
        let len = offset_to_index(len, "set_len", &self.path)?;
        let mut state = lock_state(&self.state, "set_len", &self.path)?;
        let data = &mut Self::file_data(&mut state, &self.path)?.data;
        if data.len() != len {
            data.resize(len, 0);
        }
        Ok(())
    }

    fn lock_exclusive(&self) -> Result<(), EnvError> {
        let mut state = lock_state(&self.state, "lock_exclusive", &self.path)?;
        let file = Self::file_data(&mut state, &self.path)?;
        if file.exclusive.is_some() || !file.shared.is_empty() {
            return Err(EnvError::Locked {
                path: self.path.clone(),
            });
        }
        file.exclusive = Some(self.id);
        Ok(())
    }

    fn lock_shared(&self) -> Result<(), EnvError> {
        let mut state = lock_state(&self.state, "lock_shared", &self.path)?;
        let file = Self::file_data(&mut state, &self.path)?;
        if file.exclusive.is_some() {
            return Err(EnvError::Locked {
                path: self.path.clone(),
            });
        }
        file.shared.insert(self.id);
        Ok(())
    }

    fn unlock(&self) -> Result<(), EnvError> {
        let mut state = lock_state(&self.state, "unlock", &self.path)?;
        match state.files.get_mut(&self.path) {
            Some(file) => {
                if file.exclusive == Some(self.id) {
                    file.exclusive = None;
                }
                file.shared.remove(&self.id);
                Ok(())
            }
            // The file is gone; no lock of ours exists to release.
            None => Ok(()),
        }
    }
}

impl Drop for MemFile {
    /// Dropping a handle releases its lock (E-LOCK). A poisoned mutex
    /// is skipped — `Drop` cannot report, and must not panic.
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock()
            && let Some(file) = state.files.get_mut(&self.path)
        {
            if file.exclusive == Some(self.id) {
                file.exclusive = None;
            }
            file.shared.remove(&self.id);
        }
    }
}
