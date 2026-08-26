//! Unique temporary siblings for atomic replace-on-write.
//!
//! The fixed `.tmp` suffix this replaces was a race: two writers persisting
//! concurrently (the Observe migration runner applies schema from several
//! connections at once) both wrote `<file>.json.tmp`, and whichever renamed
//! second failed with ENOENT -- surfacing as "catalog persistence failed:
//! I/O error: No such file or directory" and killing the whole migration.
//! A unique temp name per write makes the create/write/fsync/rename sequence
//! safe under any amount of concurrency; rename-over-target stays atomic.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A temporary sibling of `path` unique to this process and this call:
/// `<dir>/<file>.<pid>.<seq>.tmp`. Callers write it, fsync it, then rename
/// it over `path`.
pub fn tmp_sibling(path: &Path) -> PathBuf {
    let seq = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut name = path
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_default();
    name.push(format!(".{}.{}.tmp", std::process::id(), seq));
    path.with_file_name(name)
}

/// Write `bytes` durably over `path` via a unique temp sibling: create,
/// write, fsync file, rename, fsync the parent directory (the rename is what
/// makes the new state visible; the directory fsync makes it durable).
pub fn atomic_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = tmp_sibling(path);
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent()
        && let Ok(dir) = std::fs::File::open(parent)
    {
        let _ = dir.sync_all();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The regression for the Observe fresh-install blocker: two threads
    /// persisting concurrently must both succeed every time. With the fixed
    /// `.tmp` name this fails within a few iterations (ENOENT on rename).
    #[test]
    fn concurrent_atomic_writes_all_succeed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catalog.json");
        std::fs::write(&path, b"{}").unwrap();
        let path = std::sync::Arc::new(path);
        let fails = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut handles = Vec::new();
        for t in 0..4u32 {
            let (path, fails) = (path.clone(), fails.clone());
            handles.push(std::thread::spawn(move || {
                for i in 0..200u32 {
                    let body = format!(r#"{{"t":{t},"i":{i}}}"#);
                    if let Err(e) = atomic_write(&path, body.as_bytes()) {
                        fails.lock().unwrap().push(e.to_string());
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let fails = fails.lock().unwrap();
        assert!(
            fails.is_empty(),
            "concurrent atomic writes failed: {:?}",
            *fails
        );
        // The final content is one writer's complete body -- never partial.
        let body = std::fs::read(&*path).unwrap();
        assert!(body.starts_with(b"{") && body.ends_with(b"}"), "{:?}", body);
        // No temp debris left behind.
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "temp debris: {:?}", leftovers);
    }
}
