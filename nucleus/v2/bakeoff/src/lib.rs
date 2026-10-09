//! C-S1 bake-off: `OrderedKv` over RocksDB (rust-rocksdb 0.25, bundled C++
//! 11.8.1) and fjall 3.1, driven through the shared C-K3 conformance suite,
//! the PLAN D2 disqualifier probes, the RocksDB UDT (C-T0 §12 Q2) study and
//! the score benchmarks. Results and citations: `DECISION.md`.
//!
//! Design choices forced by the crate APIs (evidence in DECISION.md):
//! - **RocksKv snapshots are copies.** `rocksdb::SnapshotWithThreadMode`
//!   borrows the `DB`, and `DBAccess`'s snapshot/iterator constructors are
//!   `unsafe fn`s over raw FFI pointers (`ROCKS/src/db.rs:154-163`), so no
//!   safe `'static` snapshot object exists. `snapshot()` therefore drains one
//!   pinned raw iterator into a `BTreeMap`; that has exactly the MemKv-flat
//!   semantics the conformance suite was written against.
//! - **fjall DeleteRange is emulated** with point tombstones in the same
//!   atomic write batch (fjall 3.1 has no user-visible range tombstone;
//!   `lsm-tree::AbstractTree::drop_range` is crate-internal and physically
//!   removes files). Writes are serialised behind an in-store mutex so the
//!   emulation cannot miss a concurrent insert.
//! - **fjall checkpoints are logical copies** (fresh store + sorted ingest of
//!   the latest visible state). fjall has no on-disk checkpoint API, and a
//!   filesystem copy would race its background flush/compaction threads.

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub mod scratch;

#[cfg(feature = "fjall")]
pub mod fjall_kv;
#[cfg(feature = "rocks")]
pub mod rocks;

/// Maps a backend error into `KvError`. Corruption is recognisable only by
/// the upstream error text (rocksdb statuses start with `Corruption:`,
/// fjall/lsm-tree debug-print `ChecksumMismatch`/`InvalidTrailer`/...), so
/// those markers are routed to [`KvError::Corruption`]; the trait requires
/// corruption to be reported as such, never as missing data.
pub(crate) fn io_err(e: impl std::fmt::Display) -> nucleus_kv::KvError {
    let s = e.to_string();
    let corrupt = s.starts_with("Corruption")
        || s.contains("ChecksumMismatch")
        || s.contains("checksum mismatch")
        || s.contains("InvalidTrailer")
        || s.contains("InvalidHeader");
    if corrupt {
        nucleus_kv::KvError::Corruption(s)
    } else {
        nucleus_kv::KvError::Backend(s)
    }
}

/// A fresh store directory path under `/tmp/nv2-bakeoff/<tag>-<pid>-<n>`.
pub fn scratch_path(tag: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    scratch::root().join(format!(
        "{tag}-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed),
    ))
}

/// A fresh store directory under `/tmp/nv2-bakeoff/<tag>-<pid>-<n>`.
/// Removed by the returned guard (C-S1 "Do not": no scratch left in the tree).
pub fn store_dir(tag: &str) -> (PathBuf, ScratchGuard) {
    let dir = scratch_path(tag);
    (dir.clone(), ScratchGuard(dir))
}

/// Deletes a directory tree when dropped.
pub struct ScratchGuard(PathBuf);

impl ScratchGuard {
    pub fn new(path: PathBuf) -> Self {
        Self(path)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for ScratchGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
