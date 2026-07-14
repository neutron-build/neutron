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
