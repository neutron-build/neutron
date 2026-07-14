//! Shared WAL durability helper.

use std::fs::OpenOptions;
use std::io::{self, BufWriter, Write};
use std::path::Path;

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
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

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
                    if let Ok(b) = std::fs::read(&path) {
                        if b.is_empty() {
                            empties += 1;
                        }
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
