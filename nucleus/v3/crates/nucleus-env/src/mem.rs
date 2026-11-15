//! `MemEnv`: the wasm-safe in-memory backend for unit tests (PLAN-v3
//! §3.2; card A-1a Work 4; file identity per card A-1b-0 Work 3).
//!
//! Invariants owned, in memory: **E-OPEN**, **E-IO**, **E-NAMESPACE**,
//! **E-LOCK**, **E-HANDLE**, **E-KIND** and **E-CLOCK/RAND** as stated
//! in the crate root. **E-HANDLE** is this backend's rename from
//! A-1a's name-keyed map: state keeps a file table keyed by **file
//! id** and a separate name table — E4's SimDisk inode model — so a
//! handle reaches its file across a rename and after a remove, and a
//! `CreateNew` of a removed name makes a different file. **E-DURABLE**
//! is the one this backend cannot exercise: MemEnv has no crash, so
//! `sync_data`, `sync_all` and `sync_dir` are no-ops (they still
//! refuse on read-only handles, per E-OPEN). A-1b's SimEnv wraps this
//! state with a seeded scheduler whose power cut drops, tears, loses
//! or misdirects unsynced writes, and makes E-DURABLE's losses
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
    /// File id → contents, lock state and open-handle count. The file
    /// table (E-HANDLE): keys are ids, never names, so a file outlives
    /// the binding it was created under.
    files: BTreeMap<u64, FileData>,
    /// File path → file id: the directory bindings (E-HANDLE, after
    /// E4's SimDisk). `rename` rebinds one id under the new name;
    /// `remove` unbinds it and the file lives on if a handle holds it.
    names: BTreeMap<String, u64>,
    /// Directory paths, the root `""` included.
    dirs: BTreeSet<String>,
    /// The next file id. Ids are never reused within a family, so a
    /// `CreateNew` of a removed name is a different file (E-HANDLE).
    next_file: u64,
    /// The next per-handle id (locks are keyed by handle, not by file,
    /// so two handles of one file hold distinct locks).
    next_handle: u64,
    /// The virtual clock, in nanoseconds since `MemEnv::new`.
    clock_ns: u64,
    /// The splitmix64 state behind [`MemEnv::rand`].
    rand: u64,
}

/// One in-memory file: its bytes, its lock holders (E-LOCK), how many
/// open handles hold it, and the name it is bound to, if any.
struct FileData {
    /// The file's contents.
    data: Vec<u8>,
    /// The handle id holding the exclusive lock, if any.
    exclusive: Option<u64>,
    /// The handle ids holding the shared lock.
    shared: BTreeSet<u64>,
    /// How many open handles hold this file. An unnamed file with no
    /// handle is freed by `remove`; one with handles lives until its
    /// last handle drops (E-HANDLE).
    handles: usize,
    /// The name this file is bound to, or `None` once `remove` has
    /// unbound it (E-HANDLE). Mirrors `State::names` under the same
    /// lock.
    name: Option<String>,
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

/// An open file in a [`MemEnv`]: the **file id** it opened, its
/// per-handle lock id, its mode, and the path it was opened by — for
/// error messages only; no method resolves that path again (E-HANDLE).
pub struct MemFile {
    state: Arc<Mutex<State>>,
    path: String,
    file_id: u64,
    id: u64,
    mode: OpenMode,
}

/// The splitmix64 gamma: the constant each `rand` state advances by.
pub(crate) const SPLITMIX64_GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

/// The splitmix64 output function over an already-advanced state. The
/// same function backs both backends' `rand`, so a seed means the same
/// sequence everywhere (E-CLOCK/RAND; card A-1a Works 4–5).
pub(crate) fn splitmix64_output(z: u64) -> u64 {
    let mut z = z;
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
                names: BTreeMap::new(),
                dirs,
                next_file: 1,
                next_handle: 1,
                clock_ns: 0,
                rand: seed,
            })),
        }
    }

    /// An E-KIND refusal for a name that is a directory where a file is
    /// required.
    fn is_a_directory(op: &'static str, path: &str) -> EnvError {
        EnvError::Io {
            op,
            path: path.to_string(),
            kind: std::io::ErrorKind::IsADirectory,
            detail: "is a directory".to_string(),
        }
    }

    /// An E-KIND refusal for a name that is a file where a directory is
    /// required.
    fn not_a_directory(op: &'static str, path: &str) -> EnvError {
        EnvError::Io {
            op,
            path: path.to_string(),
            kind: std::io::ErrorKind::NotADirectory,
            detail: "not a directory".to_string(),
        }
    }
}

impl Env for MemEnv {
    type File = MemFile;

    fn open(&self, path: &str, mode: OpenMode) -> Result<MemFile, EnvError> {
        path::check(path)?;
        let mut state = lock_state(&self.state, "open", path)?;
        let file_id = match mode {
            OpenMode::Read | OpenMode::ReadWrite => {
                if state.dirs.contains(path) {
                    return Err(Self::is_a_directory("open", path));
                }
                match state.names.get(path) {
                    Some(&file_id) => file_id,
                    None => {
                        return Err(EnvError::NotFound {
                            path: path.to_string(),
                        });
                    }
                }
            }
            OpenMode::CreateNew => {
                if state.names.contains_key(path) || state.dirs.contains(path) {
                    return Err(EnvError::AlreadyExists {
                        path: path.to_string(),
                    });
                }
                if !state.parent_exists(path) {
                    return Err(EnvError::NotFound {
                        path: path.to_string(),
                    });
                }
                let file_id = state.next_file;
                state.next_file = state.next_file.wrapping_add(1);
                state.files.insert(
                    file_id,
                    FileData {
                        data: Vec::new(),
                        exclusive: None,
                        shared: BTreeSet::new(),
                        handles: 0,
                        name: Some(path.to_string()),
                    },
                );
                state.names.insert(path.to_string(), file_id);
                file_id
            }
        };
        // The handle holds the file, not the name, and counts as a
        // holder from open until drop (E-HANDLE).
        let file = state.files.get_mut(&file_id).ok_or_else(|| EnvError::Io {
            op: "open",
            path: path.to_string(),
            kind: std::io::ErrorKind::Other,
            detail: "file id missing from the file table".to_string(),
        })?;
        file.handles += 1;
        let id = state.next_handle;
        state.next_handle = state.next_handle.wrapping_add(1);
        Ok(MemFile {
            state: Arc::clone(&self.state),
            path: path.to_string(),
            file_id,
            id,
            mode,
        })
    }

    fn create_dir(&self, path: &str) -> Result<(), EnvError> {
        path::check(path)?;
        let mut state = lock_state(&self.state, "create_dir", path)?;
        if state.names.contains_key(path) || state.dirs.contains(path) {
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
        } else if state.names.contains_key(path) {
            // E-KIND: a file where a directory is required, so a
            // caller that passes `wal/<seg>.log` for `wal` is told.
            Err(Self::not_a_directory("sync_dir", path))
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
        let file_id = match state.names.get(from) {
            Some(&file_id) => file_id,
            None => {
                return Err(EnvError::NotFound {
                    path: from.to_string(),
                });
            }
        };
        if state.names.contains_key(to) || state.dirs.contains(to) {
            return Err(EnvError::AlreadyExists {
                path: to.to_string(),
            });
        }
        // Only the binding moves: the file, its data, its lock state
        // and its handles stay with the id (E-HANDLE).
        state.names.remove(from);
        state.names.insert(to.to_string(), file_id);
        if let Some(file) = state.files.get_mut(&file_id) {
            file.name = Some(to.to_string());
        }
        Ok(())
    }

    fn remove(&self, path: &str) -> Result<(), EnvError> {
        path::check(path)?;
        let mut state = lock_state(&self.state, "remove", path)?;
        match state.names.get(path).copied() {
            Some(file_id) => {
                // E-HANDLE: unbind the name. With no handle holding the
                // file it is freed now; otherwise it lives on, unnamed,
                // until the last handle drops.
                state.names.remove(path);
                let mut free = false;
                if let Some(file) = state.files.get_mut(&file_id) {
                    file.name = None;
                    free = file.handles == 0;
                }
                if free {
                    state.files.remove(&file_id);
                }
                Ok(())
            }
            None if state.dirs.contains(path) => Err(Self::is_a_directory("remove", path)),
            None => Err(EnvError::NotFound {
                path: path.to_string(),
            }),
        }
    }

    fn exists(&self, path: &str) -> Result<bool, EnvError> {
        path::check(path)?;
        let state = lock_state(&self.state, "exists", path)?;
        Ok(state.names.contains_key(path) || state.dirs.contains(path))
    }

    fn list_dir(&self, path: &str) -> Result<Vec<String>, EnvError> {
        path::check_dir(path)?;
        let state = lock_state(&self.state, "list_dir", path)?;
        if !state.dirs.contains(path) {
            return Err(if state.names.contains_key(path) {
                // E-KIND: a file where a directory is required.
                Self::not_a_directory("list_dir", path)
            } else {
                EnvError::NotFound {
                    path: path.to_string(),
                }
            });
        }
        let mut names = BTreeSet::new();
        for name in state.names.keys().chain(state.dirs.iter()) {
            // `name != path` keeps the root from listing itself: `""` is
            // its own parent, and no other directory can be.
            if name != path && parent_of(name) == path {
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
            Ok(mut state) => {
                state.rand = state.rand.wrapping_add(SPLITMIX64_GAMMA);
                splitmix64_output(state.rand)
            }
            Err(_) => 0,
        }
    }
}

/// E-IO's length bound (A-1b-0-F2): the end a write or `set_len`
/// would reach, or `None` when `off + len` overflows u64. A `Some`
/// past `i64::MAX` is `Io { kind: InvalidInput }`; the check runs
/// before `lock_state`, so no length can panic with the state mutex
/// held and poison the env.
fn end_past_i64_max(op: &'static str, path: &str, end: Option<u64>) -> Result<u64, EnvError> {
    match end {
        Some(e) if e <= i64::MAX as u64 => Ok(e),
        _ => Err(EnvError::Io {
            op,
            path: path.to_string(),
            kind: std::io::ErrorKind::InvalidInput,
            detail: "offset plus length exceeds i64::MAX".to_string(),
        }),
    }
}

impl MemFile {
    /// The file's mutable state by **file id** (E-HANDLE). A missing id
    /// is an internal invariant breach — a file with an open handle is
    /// never freed — reported as `Io { kind: Other }`, never a panic;
    /// no correct sequence reaches it.
    fn file_data<'a>(
        state: &'a mut State,
        file_id: u64,
        op: &'static str,
        path: &str,
    ) -> Result<&'a mut FileData, EnvError> {
        state.files.get_mut(&file_id).ok_or_else(|| EnvError::Io {
            op,
            path: path.to_string(),
            kind: std::io::ErrorKind::Other,
            detail: "file id missing from the file table".to_string(),
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
        // E-IO: an offset no `usize` can hold is past every
        // end-of-file, so it reads 0 rather than failing (A-1a-F3;
        // this only matters on 32-bit targets, where `usize::MAX` is
        // still past any length).
        let off = usize::try_from(off).unwrap_or(usize::MAX);
        let mut state = lock_state(&self.state, "read_at", &self.path)?;
        let data = &Self::file_data(&mut state, self.file_id, "read_at", &self.path)?.data;
        Ok(match data.get(off..) {
            Some(tail) => copy_prefix(tail, buf),
            None => 0,
        })
    }

    fn write_at(&self, off: u64, buf: &[u8]) -> Result<(), EnvError> {
        self.refuse_read_only()?;
        // E-IO: a zero-length write changes nothing at any offset and
        // returns Ok, before any offset conversion or resize (A-1a-F3).
        if buf.is_empty() {
            return Ok(());
        }
        // E-IO's length bound (A-1b-0-F2), before `lock_state` and any
        // conversion or resize.
        let end = end_past_i64_max("write_at", &self.path, off.checked_add(buf.len() as u64))?;
        let off = offset_to_index(off, "write_at", &self.path)?;
        let end = offset_to_index(end, "write_at", &self.path)?;
        let mut state = lock_state(&self.state, "write_at", &self.path)?;
        let data = &mut Self::file_data(&mut state, self.file_id, "write_at", &self.path)?.data;
        if data.len() < end {
            // E-IO (A-1b-0-F2): reserve the growth first and map a
            // failed allocation to OutOfMemory, so the `resize` below
            // cannot reallocate — or abort the process — and no length
            // panics with the state mutex held.
            data.try_reserve(end - data.len())
                .map_err(|_| EnvError::Io {
                    op: "write_at",
                    path: self.path.clone(),
                    kind: std::io::ErrorKind::OutOfMemory,
                    detail: "allocation for the extended length failed".to_string(),
                })?;
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
        let mut state = lock_state(&self.state, "len", &self.path)?;
        let data = &Self::file_data(&mut state, self.file_id, "len", &self.path)?.data;
        Ok(data.len() as u64)
    }

    fn set_len(&self, len: u64) -> Result<(), EnvError> {
        self.refuse_read_only()?;
        // E-IO's length bound (A-1b-0-F2), before `lock_state` and any
        // conversion or resize.
        end_past_i64_max("set_len", &self.path, Some(len))?;
        let len = offset_to_index(len, "set_len", &self.path)?;
        let mut state = lock_state(&self.state, "set_len", &self.path)?;
        let data = &mut Self::file_data(&mut state, self.file_id, "set_len", &self.path)?.data;
        if data.len() != len {
            if len > data.len() {
                // E-IO (A-1b-0-F2): reserve the growth first (see
                // `write_at`), so the `resize` cannot reallocate and no
                // length panics with the state mutex held.
                data.try_reserve(len - data.len())
                    .map_err(|_| EnvError::Io {
                        op: "set_len",
                        path: self.path.clone(),
                        kind: std::io::ErrorKind::OutOfMemory,
                        detail: "allocation for the extended length failed".to_string(),
                    })?;
            }
            data.resize(len, 0);
        }
        Ok(())
    }

    fn lock_exclusive(&self) -> Result<(), EnvError> {
        let mut state = lock_state(&self.state, "lock_exclusive", &self.path)?;
        let file = Self::file_data(&mut state, self.file_id, "lock_exclusive", &self.path)?;
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
        let file = Self::file_data(&mut state, self.file_id, "lock_shared", &self.path)?;
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
        let file = Self::file_data(&mut state, self.file_id, "unlock", &self.path)?;
        if file.exclusive == Some(self.id) {
            file.exclusive = None;
        }
        file.shared.remove(&self.id);
        Ok(())
    }
}

impl Drop for MemFile {
    /// Dropping a handle releases its lock and its hold on the file
    /// (E-LOCK, E-HANDLE): the last handle of an unnamed file frees it.
    /// A poisoned mutex is skipped — `Drop` cannot report, and must not
    /// panic.
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            let mut free = false;
            if let Some(file) = state.files.get_mut(&self.file_id) {
                if file.exclusive == Some(self.id) {
                    file.exclusive = None;
                }
                file.shared.remove(&self.id);
                file.handles = file.handles.saturating_sub(1);
                free = file.handles == 0 && file.name.is_none();
            }
            if free {
                state.files.remove(&self.file_id);
            }
        }
    }
}
