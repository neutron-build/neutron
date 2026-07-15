//! Physical snapshot backup/restore (v1).
//!
//! Copies a Nucleus data directory verbatim into a portable snapshot directory
//! plus a manifest recording the Nucleus version. Because it is a byte-for-byte
//! copy, it is trivially consistent when taken against a STOPPED instance (a
//! clean shutdown has flushed all WALs), and it is format-agnostic — it needs no
//! knowledge of the on-disk layout. The tradeoff is that it is version-locked:
//! restore requires the same Nucleus version, enforced via the manifest. A
//! logical dump/restore (portable across versions) is the planned v2.

use serde::{Deserialize, Serialize};
use std::io;
use std::path::Path;

/// Manifest written alongside a backup, used to validate a restore.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupManifest {
    /// Nucleus version that produced the snapshot.
    pub nucleus_version: String,
    /// Backup format identifier.
    pub format: String,
    /// Unix seconds when the backup was taken (0 if unavailable).
    pub created_unix: u64,
    /// Human-readable source data directory.
    pub source: String,
    /// On-disk database format version (`DB_FORMAT_VERSION`) at backup time.
    /// A physical snapshot is byte-for-byte, so restore compatibility is
    /// governed by the on-disk format — not the release version. Comparing this
    /// instead of `nucleus_version` lets a patch release restore a snapshot from
    /// another patch that shares the format. `0` in legacy manifests (pre-field)
    /// → fall back to the exact-version check.
    #[serde(default)]
    pub format_version: u32,
}

const FORMAT_V1: &str = "physical-v1";
const MANIFEST_NAME: &str = "nucleus-backup.json";
const DATA_SUBDIR: &str = "data";

/// Recursively copy a directory tree (files + subdirectories only; symlinks and
/// other special files are skipped for safety).
pub fn copy_dir_recursive(src: &Path, dst: &Path) -> io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_recursive(&from, &to)?;
        } else if file_type.is_file() {
            std::fs::copy(&from, &to)?;
        }
    }
    Ok(())
}

/// Back up `data_dir` into `output_dir`. Fails if `output_dir` exists unless
/// `force`. Returns the written manifest.
pub fn backup_data_dir(
    data_dir: &Path,
    output_dir: &Path,
    force: bool,
    nucleus_version: &str,
) -> io::Result<BackupManifest> {
    if !data_dir.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("data directory does not exist: {}", data_dir.display()),
        ));
    }
    if output_dir.exists() {
        if !force {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "destination already exists: {} (use force to overwrite)",
                    output_dir.display()
                ),
            ));
        }
        std::fs::remove_dir_all(output_dir)?;
    }

    copy_dir_recursive(data_dir, &output_dir.join(DATA_SUBDIR))?;

    let manifest = BackupManifest {
        nucleus_version: nucleus_version.to_string(),
        format: FORMAT_V1.to_string(),
        created_unix: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        source: data_dir.display().to_string(),
        format_version: crate::storage::page::DB_FORMAT_VERSION,
    };
    let json = serde_json::to_string_pretty(&manifest)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    std::fs::write(output_dir.join(MANIFEST_NAME), json)?;
    Ok(manifest)
}

/// Restore a snapshot at `input_dir` into `data_dir`. Refuses to overwrite a
/// non-empty `data_dir` unless `force`, and refuses a version mismatch (physical
/// snapshots are version-locked). Returns the manifest that was restored.
pub fn restore_data_dir(
    input_dir: &Path,
    data_dir: &Path,
    force: bool,
    nucleus_version: &str,
) -> io::Result<BackupManifest> {
    let manifest_path = input_dir.join(MANIFEST_NAME);
    if !manifest_path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "not a Nucleus backup (missing {MANIFEST_NAME}): {}",
                input_dir.display()
            ),
        ));
    }
    let manifest: BackupManifest = serde_json::from_str(&std::fs::read_to_string(&manifest_path)?)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    // Compatibility is governed by the on-disk format, not the release string:
    // a physical snapshot restores into any build that reads the same
    // `DB_FORMAT_VERSION` (so patch releases interoperate). Legacy manifests
    // predate the field (format_version == 0) — fall back to the exact-version
    // check for those.
    let current_format = crate::storage::page::DB_FORMAT_VERSION;
    if manifest.format_version != 0 {
        if manifest.format_version != current_format {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "format mismatch: backup uses on-disk format v{}, this build uses v{}. \
                     Physical snapshots are format-locked — restore with a build on the same \
                     format version (backup was from Nucleus {}).",
                    manifest.format_version, current_format, manifest.nucleus_version
                ),
            ));
        }
    } else if manifest.nucleus_version != nucleus_version {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "version mismatch: backup is from Nucleus {}, this is {}. \
                 Physical snapshots are version-locked — restore with the matching version.",
                manifest.nucleus_version, nucleus_version
            ),
        ));
    }

    if data_dir.exists() {
        let non_empty = std::fs::read_dir(data_dir)?.next().is_some();
        if non_empty && !force {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "data directory is not empty: {} (use force to overwrite)",
                    data_dir.display()
                ),
            ));
        }
        std::fs::remove_dir_all(data_dir)?;
    }

    copy_dir_recursive(&input_dir.join(DATA_SUBDIR), data_dir)?;
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_tmp(tag: &str) -> std::path::PathBuf {
        // Avoid rng/time-based names (deterministic per test) — use the tag +
        // process id so parallel tests don't collide.
        std::env::temp_dir().join(format!("nucleus_bk_test_{tag}_{}", std::process::id()))
    }

    fn write(path: &Path, rel: &str, contents: &[u8]) {
        let full = path.join(rel);
        std::fs::create_dir_all(full.parent().unwrap()).unwrap();
        std::fs::write(full, contents).unwrap();
    }

    #[test]
    fn backup_then_restore_round_trips_bytes() {
        let root = unique_tmp("roundtrip");
        let _ = std::fs::remove_dir_all(&root);
        let data = root.join("data_dir");
        let snap = root.join("snap");
        let restored = root.join("restored");

        // A data dir with nested files (mirrors catalog.json + WAL/storage files).
        write(&data, "catalog.json", b"{\"tables\":1}");
        write(&data, "wal/000001.wal", &[0u8, 1, 2, 3, 255]);
        write(&data, "storage/t.dat", b"alice\x00bob");

        let m = backup_data_dir(&data, &snap, false, "0.1.1").unwrap();
        assert_eq!(m.nucleus_version, "0.1.1");
        assert_eq!(m.format, "physical-v1");
        assert!(snap.join(MANIFEST_NAME).exists());

        restore_data_dir(&snap, &restored, false, "0.1.1").unwrap();

        // Every file restored byte-for-byte.
        for rel in ["catalog.json", "wal/000001.wal", "storage/t.dat"] {
            assert_eq!(
                std::fs::read(data.join(rel)).unwrap(),
                std::fs::read(restored.join(rel)).unwrap(),
                "file diverged after round-trip: {rel}"
            );
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn restore_allows_differing_patch_on_same_format() {
        // Compatibility is by on-disk format, not release string: a snapshot
        // from 0.1.1 restores under 0.2.0 as long as the format version matches.
        let root = unique_tmp("patchinterop");
        let _ = std::fs::remove_dir_all(&root);
        let data = root.join("data_dir");
        let snap = root.join("snap");
        write(&data, "catalog.json", b"{}");
        let m = backup_data_dir(&data, &snap, false, "0.1.1").unwrap();
        assert_eq!(m.format_version, crate::storage::page::DB_FORMAT_VERSION);

        restore_data_dir(&snap, &root.join("restored"), false, "0.2.0")
            .expect("same on-disk format must restore across patch releases");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn restore_refuses_format_mismatch() {
        let root = unique_tmp("formatlock");
        let _ = std::fs::remove_dir_all(&root);
        let data = root.join("data_dir");
        let snap = root.join("snap");
        write(&data, "catalog.json", b"{}");
        backup_data_dir(&data, &snap, false, "0.1.1").unwrap();

        // Rewrite the manifest to claim a different on-disk format version.
        let mpath = snap.join(MANIFEST_NAME);
        let mut manifest: BackupManifest =
            serde_json::from_str(&std::fs::read_to_string(&mpath).unwrap()).unwrap();
        manifest.format_version = 999;
        std::fs::write(&mpath, serde_json::to_string_pretty(&manifest).unwrap()).unwrap();

        let err = restore_data_dir(&snap, &root.join("restored"), false, "0.1.1").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("format mismatch"));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn legacy_manifest_without_format_version_is_version_locked() {
        // A pre-format_version manifest (field absent → 0) falls back to the
        // exact release-string check, preserving the old lock for old backups.
        let root = unique_tmp("legacylock");
        let _ = std::fs::remove_dir_all(&root);
        let data = root.join("data_dir");
        let snap = root.join("snap");
        write(&data, "catalog.json", b"{}");
        backup_data_dir(&data, &snap, false, "0.1.1").unwrap();

        // Emulate a legacy manifest: strip the format_version field.
        let mpath = snap.join(MANIFEST_NAME);
        let legacy =
            r#"{"nucleus_version":"0.1.1","format":"physical-v1","created_unix":0,"source":"x"}"#;
        std::fs::write(&mpath, legacy).unwrap();

        let err = restore_data_dir(&snap, &root.join("restored"), false, "0.2.0").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("version mismatch"));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn restore_refuses_nonempty_dir_without_force() {
        let root = unique_tmp("nonempty");
        let _ = std::fs::remove_dir_all(&root);
        let data = root.join("data_dir");
        let snap = root.join("snap");
        write(&data, "catalog.json", b"{}");
        backup_data_dir(&data, &snap, false, "0.1.1").unwrap();

        let target = root.join("restored");
        write(&target, "existing.txt", b"do not clobber");
        let err = restore_data_dir(&snap, &target, false, "0.1.1").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        // With force it succeeds.
        restore_data_dir(&snap, &target, true, "0.1.1").unwrap();
        assert!(target.join("catalog.json").exists());
        let _ = std::fs::remove_dir_all(&root);
    }
}
