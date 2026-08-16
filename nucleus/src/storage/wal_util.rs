//! Shared WAL durability helpers.

use std::fs::OpenOptions;
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::storage::wal::GroupCommitter;

/// Cap for any pre-allocation sized by a count read out of a file or a socket.
///
/// Decode paths across this engine read an element count as a `u32` and hand it
/// straight to `with_capacity`. A corrupt or hostile count of `u32::MAX` asks
/// for ~4.3 billion elements — 103 GB for a 24-byte element — and **a Rust
/// allocation failure aborts**: SIGABRT, no unwind, no `Err`, no log. On a
/// recovery path that is a boot crash-loop with no diagnostic, which is exactly
/// the shape NU-385 describes.
///
/// This was found the hard way. `HnswIndex::deserialize` asked for 103 GB from
/// a corrupt layer count and aborted on Linux CI while passing on macOS, which
/// overcommits and let the reservation succeed. The count itself is not
/// trustworthy enough to size anything.
pub const MAX_PREALLOC: usize = 4096;

/// Clamp a declared element count before it sizes an allocation.
///
/// Reserving a bounded amount and letting the container grow costs at most a
/// few reallocations on the honest path, and removes the abort on the dishonest
/// one. Every caller's loop already stops when the data runs out, so this
/// changes no result — only the peak reservation.
///
/// Use this wherever a count comes from bytes you did not just write. Where a
/// cheap exact bound exists (`pos + n * elem_size > data.len()`), prefer that:
/// it rejects the corrupt input instead of merely surviving it.
pub fn bounded_capacity(declared: usize) -> usize {
    declared.min(MAX_PREALLOC)
}

/// Group-commit fsync coordinator shared by the specialty-store WALs
/// (KV, KV-collections, timeseries, vector, graph, streams, CDC).
///
/// It tracks a monotone append LSN and the highest LSN a *completed* fsync has
/// covered, and batches concurrent fsyncs through a [`GroupCommitter`] so N
/// committers racing on the same log share one `fsync`. This is the same shape
/// `columnar_wal.rs` grew inline; factoring it here keeps the seven subsystem
/// WALs from re-deriving (and mis-deriving) the ordering. The owning WAL:
///   1. calls [`WalSync::on_append`] under its writer lock after each append, and
///   2. exposes a `group_sync` that forwards to [`WalSync::group_sync`], passing a
///      closure that flushes + `sync_all`s the file and returns the LSN it covered.
///
/// The append counter MUST be bumped under the writer lock, and the covering
/// closure MUST read the mark (`current`) under that same lock, so every append
/// at or below a captured mark is guaranteed flushed before it is fsynced.
pub(crate) struct WalSync {
    /// Monotone append counter, bumped under the WAL's writer lock — the LSN.
    appends: AtomicU64,
    /// Highest append LSN covered by a COMPLETED fsync. Advanced only after the
    /// sync returns, so `synced >= mark` is a durable claim.
    synced: AtomicU64,
    /// Coordinates concurrent committers so they share fsyncs.
    committer: GroupCommitter,
}

impl WalSync {
    pub(crate) fn new() -> Self {
        Self {
            appends: AtomicU64::new(0),
            synced: AtomicU64::new(0),
            committer: GroupCommitter::new(),
        }
    }

    /// Record an append and return the new LSN. Call under the writer lock so a
    /// concurrent `group_sync`'s captured mark is exact.
    pub(crate) fn on_append(&self) -> u64 {
        self.appends.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// The current append LSN (highest appended, not necessarily synced). Read
    /// this under the writer lock inside the covering closure.
    pub(crate) fn current(&self) -> u64 {
        self.appends.load(Ordering::Acquire)
    }

    /// Mark `lsn` durably covered. Call only after a `sync_all` completes.
    pub(crate) fn mark_synced(&self, lsn: u64) {
        self.synced.fetch_max(lsn, Ordering::AcqRel);
    }

    /// Whether appends exist that no completed fsync covers yet.
    pub(crate) fn is_dirty(&self) -> bool {
        self.synced.load(Ordering::Acquire) < self.appends.load(Ordering::Acquire)
    }

    /// Durability-grade group sync: returns only once a completed fsync covers
    /// every append made before this call. `sync_covering` must flush + `fsync`
    /// the file and return the highest append LSN it durably covered (captured
    /// under the writer lock). Concurrent callers share fsyncs and each returns
    /// only when its own records are covered.
    pub(crate) fn group_sync<F: Fn() -> io::Result<u64>>(
        &self,
        sync_covering: F,
    ) -> io::Result<()> {
        let mark = self.appends.load(Ordering::Acquire);
        if self.synced.load(Ordering::Acquire) >= mark {
            return Ok(());
        }
        self.committer.sync_up_to(mark, || {
            let covered = sync_covering()?;
            self.synced.fetch_max(covered, Ordering::AcqRel);
            Ok(covered)
        })
    }
}

/// Atomically replace a log file's entire contents with `contents`.
///
/// Writes to a sibling `*.wal.tmp` file, `fsync`s it (`sync_all`), then `rename`s it
/// over `path`. The rename is atomic on POSIX, so a crash mid-checkpoint leaves
/// either the old file intact or the fully-written, fsync'd new one — never a
/// truncated or empty file.
///
/// This is the safe replacement for the `truncate(true)`-in-place checkpoint pattern
/// several subsystem WALs used (KV, KV-collections, timeseries, vector, graph, streams,
/// CDC, document, FTS), where a crash between truncating the live log and rewriting the
/// snapshot lost the entire store for that model. Callers should hold their writer lock
/// across the flush → replace → reopen so no append interleaves into the file that is
/// about to be renamed away.
#[allow(dead_code)]
pub(crate) fn atomic_replace_wal(path: &Path, contents: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("wal.tmp");
    {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        let mut w = BufWriter::new(file);
        w.write_all(contents)?;
        w.flush()?;
        w.get_ref().sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("nucleus_walutil_{name}_{}.wal", std::process::id()))
    }

    /// The helper's ONLY mutation to the live path is the final atomic rename, so a
    /// crash after the temp file is written but before the rename must leave the old
    /// content fully intact — the exact scenario the old truncate-in-place pattern got
    /// wrong (it truncated the live file first, so a crash there lost everything).
    #[test]
    fn crash_before_rename_preserves_old_content() {
        let path = tmp("before_rename");
        let _ = std::fs::remove_file(&path);
        std::fs::write(&path, b"OLD-LOG-CONTENT").unwrap();
        // Emulate the interrupted state: the temp exists, the rename has not happened.
        std::fs::write(path.with_extension("wal.tmp"), b"NEW-SNAPSHOT").unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"OLD-LOG-CONTENT",
            "before the rename, the live WAL must still hold the old content"
        );
        // Completing the replace atomically swaps in the new content.
        atomic_replace_wal(&path, b"NEW-SNAPSHOT").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"NEW-SNAPSHOT");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("wal.tmp"));
    }

    /// A concurrent reader must never observe an empty/truncated live file while
    /// repeated checkpoints run. The old `truncate(true)`-in-place pattern had an
    /// empty-file window on every checkpoint (crash there = total loss); the atomic
    /// rename never exposes one. This directly guards the fixed failure mode.
    #[test]
    fn concurrent_reader_never_sees_an_empty_live_file() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let path = tmp("concurrent");
        let _ = std::fs::remove_file(&path);
        atomic_replace_wal(&path, b"V-initial").unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let reader = {
            let path = path.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut empties = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    if let Ok(b) = std::fs::read(&path)
                        && b.is_empty()
                    {
                        empties += 1;
                    }
                }
                empties
            })
        };

        for i in 0..500u32 {
            let content = format!("V-snapshot-{i}");
            atomic_replace_wal(&path, content.as_bytes()).unwrap();
        }
        stop.store(true, Ordering::Relaxed);

        let empties = reader.join().unwrap();
        assert_eq!(
            empties, 0,
            "a concurrent reader observed an empty live WAL — replacement was not atomic"
        );
        let _ = std::fs::remove_file(&path);
    }
}
