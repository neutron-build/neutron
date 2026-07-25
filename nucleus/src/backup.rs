//! Physical snapshot backup/restore.
//!
//! Two paths, with different consistency guarantees, both producing the same
//! snapshot layout (`data/` + `nucleus-backup.json`):
//!
//! * **Offline** ([`backup_data_dir`]) — a verbatim recursive copy of a data
//!   directory. Correct only when nothing is writing to it, so it *refuses to
//!   run* when a live instance holds the directory lock (see [`DataDirLock`]).
//!   The override (`allow_in_use`) exists for the deliberate case and is
//!   recorded in the manifest, so an inconsistent snapshot can never be
//!   mistaken for a consistent one after the fact.
//!
//! * **Online** ([`backup_online`]) — taken by the running engine while writes
//!   proceed. It coordinates with the WAL and the checkpointer through
//!   [`BackupCoordinator`]: pin WAL retention, checkpoint, copy the data file
//!   page-slot at a time (verifying each slot decodes to a complete page, so a
//!   page caught mid-write is re-read rather than copied torn), then copy the
//!   WAL truncated at the window's end LSN using the same byte-exact prefix cut
//!   PITR uses. Restoring and opening the snapshot replays that WAL through the
//!   engine's ordinary recovery path, landing on exactly the state a crash at
//!   `consistent_lsn` would have recovered.
//!
//! Both paths write per-file BLAKE3 checksums, the source database identity,
//! and the at-rest encryption/compression settings into the manifest. Restore
//! verifies every checksum *before* it touches the destination: a corrupted or
//! tampered snapshot is refused with the offending files named, and the
//! destination is left byte-for-byte as it was.
//!
//! Scope, stated honestly: the online path's consistency point is the SQL
//! relational substrate (the page-structured data file plus its segmented WAL),
//! which is the system of record. The specialty-model WALs and the JSON catalog
//! files in the same directory are copied after the consistency point is
//! established; each is individually crash-consistent (atomic rename, or an
//! append-only log whose torn tail recovery discards), but they are not pinned
//! to the same LSN. A DDL statement committed during the copy window can
//! therefore land in the snapshot's catalog while its pages do not. Cross-model
//! coordination is not implemented — see DATABASE_COMPLETION.md M4.

use serde::{Deserialize, Serialize};
use std::io;
use std::path::{Path, PathBuf};

/// Name of the advisory lock file an open instance holds inside its data
/// directory.
pub const LOCK_NAME: &str = "nucleus.lock";

/// Name of the file carrying the database's stable identity.
pub const DB_ID_NAME: &str = "nucleus.id";

const FORMAT_V1: &str = "physical-v1";
const MANIFEST_NAME: &str = "nucleus-backup.json";
const DATA_SUBDIR: &str = "data";

/// One file inside a snapshot, with the checksum restore validates it against.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupFile {
    /// Path relative to the snapshot's `data/` directory, `/`-separated.
    pub path: String,
    /// Byte length at backup time.
    pub len: u64,
    /// BLAKE3 hash of the contents, lowercase hex.
    pub blake3: String,
}

/// At-rest encryption/compression settings of the source database.
///
/// A physical snapshot is a byte copy, so it inherits them: restoring an
/// encrypted snapshot needs the same key, and the manifest is the only place
/// that fact is recorded. `key_id` names the key without carrying it — a
/// backup must never be a place a key leaks from.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupEncryption {
    /// Pages are encrypted at rest.
    pub encrypted: bool,
    /// Pages are compressed at rest.
    pub compressed: bool,
    /// Cipher identifier (e.g. `aes-256-gcm`). `None` when not encrypted.
    pub algorithm: Option<String>,
    /// Operator-facing key identifier, so a restore can locate the key. Never
    /// the key material itself.
    pub key_id: Option<String>,
}

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
    /// Stable identity of the source database (see [`DB_ID_NAME`]). Empty for
    /// legacy manifests and for sources that had no identity file and could
    /// not be given one (read-only media).
    #[serde(default)]
    pub database_id: String,
    /// Whether the snapshot was taken online, coordinated with the WAL.
    #[serde(default)]
    pub online: bool,
    /// LSN the snapshot's SQL substrate is consistent through. `0` when no
    /// consistency point was established (offline snapshot of a stopped
    /// instance, or a legacy manifest).
    #[serde(default)]
    pub consistent_lsn: u64,
    /// True when the snapshot was taken against a data directory a live
    /// instance held, via the explicit override. Such a snapshot may be torn;
    /// this flag is why that can never be discovered only after a restore.
    #[serde(default)]
    pub taken_while_in_use: bool,
    /// At-rest encryption/compression of the source.
    #[serde(default)]
    pub encryption: BackupEncryption,
    /// Per-file checksums covering everything under `data/`. Empty in legacy
    /// (pre-checksum) manifests, which restore accepts unverified.
    #[serde(default)]
    pub files: Vec<BackupFile>,
}

// ---------------------------------------------------------------------------
// Data-directory lock
// ---------------------------------------------------------------------------

/// Advisory exclusive lock on a data directory, held by an open instance for
/// as long as it has the directory open.
///
/// This is an OS file lock (`flock` on Unix, `LockFileEx` on Windows), not a
/// pid file: it is released by the kernel when the holding process exits,
/// however it exits, so a crashed instance never leaves a stale lock that
/// blocks a later backup. Acquisition is deliberately non-fatal — Nucleus has
/// never enforced single-instance access to a directory and this is not the
/// change that starts — but *observing* the lock is what lets a backup refuse
/// to copy a directory out from under a live writer.
#[derive(Debug)]
pub struct DataDirLock {
    file: std::fs::File,
    path: PathBuf,
}

impl DataDirLock {
    /// Try to take the lock. `Ok(None)` means another live process holds it.
    pub fn acquire(data_dir: &Path) -> io::Result<Option<Self>> {
        std::fs::create_dir_all(data_dir)?;
        let path = data_dir.join(LOCK_NAME);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        match file.try_lock() {
            Ok(()) => {
                // Best-effort provenance for an operator staring at the file.
                use std::io::Write;
                let _ = file.set_len(0);
                let _ = writeln!(
                    &file,
                    "pid {} since {}",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0)
                );
                Ok(Some(Self { file, path }))
            }
            Err(std::fs::TryLockError::WouldBlock) => Ok(None),
            Err(std::fs::TryLockError::Error(e)) => Err(e),
        }
    }

    /// Whether some *other* live process currently holds the directory lock.
    ///
    /// Implemented by trying to take the lock and immediately dropping it: a
    /// stale file from a crashed process is not locked, so this reports
    /// liveness rather than mere file existence.
    pub fn is_locked(data_dir: &Path) -> bool {
        let path = data_dir.join(LOCK_NAME);
        if !path.exists() {
            return false;
        }
        let Ok(file) = std::fs::OpenOptions::new().read(true).write(true).open(&path) else {
            // Cannot even open it — assume in use rather than assume safe.
            return true;
        };
        match file.try_lock() {
            Ok(()) => {
                let _ = file.unlock();
                false
            }
            Err(std::fs::TryLockError::WouldBlock) => true,
            Err(std::fs::TryLockError::Error(_)) => true,
        }
    }
}

impl Drop for DataDirLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
        // Only the holder removes the file, so a second (unlocked) opener
        // dropping first cannot delete the live holder's lock.
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Read the database's stable identity, creating it if absent.
///
/// The id exists so a restore can tell "this snapshot belongs to this
/// database" from "you are about to overwrite a different database with it".
/// Best-effort: a directory that cannot be written (read-only media) yields an
/// empty id rather than failing the backup.
pub fn database_id(data_dir: &Path) -> String {
    let path = data_dir.join(DB_ID_NAME);
    if let Ok(s) = std::fs::read_to_string(&path) {
        let trimmed = s.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    let seed = format!(
        "{}|{}|{}|{:?}",
        data_dir.display(),
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
        std::time::Instant::now(),
    );
    let id = blake3::hash(seed.as_bytes()).to_hex()[..32].to_string();
    // Create atomically: two racing openers must agree on one id.
    let tmp = data_dir.join(format!("{DB_ID_NAME}.{}.tmp", std::process::id()));
    if std::fs::write(&tmp, &id).is_ok() {
        if std::fs::hard_link(&tmp, &path).is_ok() {
            let _ = std::fs::remove_file(&tmp);
            return id;
        }
        let _ = std::fs::remove_file(&tmp);
        // Lost the race (or links unsupported) — re-read the winner's value.
        if let Ok(s) = std::fs::read_to_string(&path) {
            let trimmed = s.trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }
    }
    String::new()
}

// ---------------------------------------------------------------------------
// Copying
// ---------------------------------------------------------------------------

/// Recursively copy a directory tree (files + subdirectories only; symlinks and
/// other special files are skipped for safety).
pub fn copy_dir_recursive(src: &Path, dst: &Path) -> io::Result<()> {
    copy_dir_filtered(src, dst, &|_| true)
}

/// Recursive copy honoring a per-entry filter, applied to the path relative to
/// the copy root.
fn copy_dir_filtered(src: &Path, dst: &Path, keep: &dyn Fn(&Path) -> bool) -> io::Result<()> {
    copy_dir_inner(src, dst, Path::new(""), keep)
}

fn copy_dir_inner(
    src: &Path,
    dst: &Path,
    rel: &Path,
    keep: &dyn Fn(&Path) -> bool,
) -> io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let child_rel = rel.join(entry.file_name());
        if !keep(&child_rel) {
            continue;
        }
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_inner(&from, &to, &child_rel, keep)?;
        } else if file_type.is_file() {
            std::fs::copy(&from, &to)?;
        }
    }
    Ok(())
}

/// Files that exist to coordinate a *live* process and must never be captured
/// in a snapshot: restoring someone else's lock file would be meaningless at
/// best and confusing at worst.
fn is_runtime_only(rel: &Path) -> bool {
    rel.file_name().is_some_and(|n| n == LOCK_NAME)
}

/// Walk a snapshot's `data/` tree and compute the manifest's file list.
fn fingerprint_tree(root: &Path) -> io::Result<Vec<BackupFile>> {
    let mut out = Vec::new();
    let mut stack = vec![(root.to_path_buf(), PathBuf::new())];
    while let Some((dir, rel)) = stack.pop() {
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let ft = entry.file_type()?;
            let child_rel = rel.join(entry.file_name());
            if ft.is_dir() {
                stack.push((entry.path(), child_rel));
            } else if ft.is_file() {
                let bytes = std::fs::read(entry.path())?;
                out.push(BackupFile {
                    path: rel_to_string(&child_rel),
                    len: bytes.len() as u64,
                    blake3: blake3::hash(&bytes).to_hex().to_string(),
                });
            }
        }
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

fn rel_to_string(rel: &Path) -> String {
    rel.components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

// ---------------------------------------------------------------------------
// Online backup coordination
// ---------------------------------------------------------------------------

/// The engine-side hooks an online physical backup needs.
///
/// Implemented by the SQL disk engine. Everything here is about pinning a
/// window: `backup_begin` establishes an LSN below which the WAL will not be
/// reclaimed and above which every page change is recoverable from the WAL;
/// `backup_end` closes the window and names the LSN the snapshot is consistent
/// through; `backup_release` drops the pin so checkpointing resumes reclaiming.
pub trait BackupCoordinator: Send + Sync {
    /// Checkpoint, pin WAL retention, and return the window's start LSN.
    fn backup_begin(&self) -> io::Result<u64>;
    /// Make everything logged so far durable, seal the active segment, and
    /// return the LSN the snapshot is consistent through. The retention pin
    /// stays held until [`BackupCoordinator::backup_release`].
    fn backup_end(&self) -> io::Result<u64>;
    /// Release the retention pin. Safe to call without a matching begin.
    fn backup_release(&self);
    /// Path of the primary data file.
    fn data_file_path(&self) -> PathBuf;
    /// Copy the primary data file to `dst`, one page slot at a time, re-reading
    /// any slot that does not decode to a complete page so a page caught
    /// mid-write is never copied torn.
    fn snapshot_data_file(&self, dst: &Path) -> io::Result<()>;
    /// At-rest encryption/compression settings of the primary data file.
    fn encryption_info(&self) -> BackupEncryption;
}

// ---------------------------------------------------------------------------
// Backup
// ---------------------------------------------------------------------------

/// Back up `data_dir` into `output_dir` offline. Fails if `output_dir` exists
/// unless `force`, and fails if a live instance holds the directory lock.
pub fn backup_data_dir(
    data_dir: &Path,
    output_dir: &Path,
    force: bool,
    nucleus_version: &str,
) -> io::Result<BackupManifest> {
    backup_data_dir_opts(data_dir, output_dir, force, nucleus_version, false)
}

/// [`backup_data_dir`] with the in-use override.
///
/// `allow_in_use` copies a directory a live instance is writing to. The result
/// may be torn; it is recorded as `taken_while_in_use` in the manifest so the
/// caveat travels with the snapshot instead of being lost the moment the
/// command exits.
pub fn backup_data_dir_opts(
    data_dir: &Path,
    output_dir: &Path,
    force: bool,
    nucleus_version: &str,
    allow_in_use: bool,
) -> io::Result<BackupManifest> {
    if !data_dir.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("data directory does not exist: {}", data_dir.display()),
        ));
    }
    let in_use = DataDirLock::is_locked(data_dir);
    if in_use && !allow_in_use {
        return Err(io::Error::new(
            io::ErrorKind::ResourceBusy,
            format!(
                "{} is open by a running Nucleus instance. A plain directory copy of a live \
                 database is a TORN snapshot — it may not restore at all. Take the backup \
                 through the running instance (online backup), stop the instance, or pass the \
                 explicit in-use override to accept an inconsistent copy.",
                data_dir.display()
            ),
        ));
    }

    // Establish identity BEFORE the copy, so the id file is part of the
    // snapshot. Created afterwards it would be missing from every snapshot,
    // and a restore could never tell one database from another.
    let db_id = database_id(data_dir);

    reject_nested_destination(data_dir, output_dir)?;
    prepare_output_dir(output_dir, force)?;
    let snapshot_data = output_dir.join(DATA_SUBDIR);
    copy_dir_filtered(data_dir, &snapshot_data, &|rel| !is_runtime_only(rel))?;

    let manifest = BackupManifest {
        nucleus_version: nucleus_version.to_string(),
        format: FORMAT_V1.to_string(),
        created_unix: now_unix(),
        source: data_dir.display().to_string(),
        format_version: crate::storage::page::DB_FORMAT_VERSION,
        database_id: db_id,
        online: false,
        consistent_lsn: 0,
        taken_while_in_use: in_use,
        encryption: BackupEncryption::default(),
        files: fingerprint_tree(&snapshot_data)?,
    };
    write_manifest(output_dir, &manifest)?;
    Ok(manifest)
}

/// Take an online physical snapshot of a *running* database.
///
/// `coord` is the live engine owning `data_dir`'s primary data file. Writes
/// continue throughout; the snapshot restores to the state as of the returned
/// manifest's `consistent_lsn`, exactly as a crash at that LSN would have
/// recovered.
pub fn backup_online(
    data_dir: &Path,
    output_dir: &Path,
    force: bool,
    nucleus_version: &str,
    coord: &dyn BackupCoordinator,
) -> io::Result<BackupManifest> {
    if !data_dir.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("data directory does not exist: {}", data_dir.display()),
        ));
    }
    // Identity before the copy so it lands inside the snapshot (see
    // `backup_data_dir_opts`).
    let db_id = database_id(data_dir);

    reject_nested_destination(data_dir, output_dir)?;
    prepare_output_dir(output_dir, force)?;
    let snapshot_data = output_dir.join(DATA_SUBDIR);
    std::fs::create_dir_all(&snapshot_data)?;

    let result = backup_online_inner(data_dir, &snapshot_data, coord);
    coord.backup_release();
    let consistent_lsn = result?;

    let manifest = BackupManifest {
        nucleus_version: nucleus_version.to_string(),
        format: FORMAT_V1.to_string(),
        created_unix: now_unix(),
        source: data_dir.display().to_string(),
        format_version: crate::storage::page::DB_FORMAT_VERSION,
        database_id: db_id,
        online: true,
        consistent_lsn,
        taken_while_in_use: false,
        encryption: coord.encryption_info(),
        files: fingerprint_tree(&snapshot_data)?,
    };
    write_manifest(output_dir, &manifest)?;
    Ok(manifest)
}

/// The copy itself, factored out so the retention pin is released on every
/// exit path — including the error ones, where leaving the pin held would stop
/// the live database from ever reclaiming WAL again.
fn backup_online_inner(
    data_dir: &Path,
    snapshot_data: &Path,
    coord: &dyn BackupCoordinator,
) -> io::Result<u64> {
    let db_path = coord.data_file_path();
    let db_name = db_path
        .file_name()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("nucleus.db"));
    let wal_dir_name = PathBuf::from(&db_name).with_extension("wal.d");
    let single_wal_name = PathBuf::from(&db_name).with_extension("wal");

    // 1. Pin WAL retention and checkpoint. From here the WAL holds a full page
    //    image for every page that changes, and nothing prunes those images.
    coord.backup_begin()?;

    // 2. Copy the data file page-slot at a time. Concurrent writers may land
    //    inside this loop; each slot is validated as a complete page image
    //    before its bytes are written to the snapshot.
    coord.snapshot_data_file(&snapshot_data.join(&db_name))?;

    // 3. Close the window: everything at or below `end` is durable and sealed.
    let end = coord.backup_end()?;

    // 4. Copy the WAL, byte-exactly truncated at `end` — the same prefix cut
    //    PITR uses, so every CRC is preserved and the snapshot cannot replay
    //    past its own consistency point.
    copy_wal_upto(
        &data_dir.join(&wal_dir_name),
        &snapshot_data.join(&wal_dir_name),
        end,
    )?;
    let live_single = data_dir.join(&single_wal_name);
    if live_single.is_file() {
        let dst = snapshot_data.join(&single_wal_name);
        if crate::storage::wal::copy_segment_prefix_upto_lsn(&live_single, &dst, end)?.is_none() {
            let _ = std::fs::remove_file(&dst);
        }
    }

    // 5. Everything else in the directory (catalog/meta JSON, specialty-model
    //    WALs). Copied after the consistency point so it is never *older* than
    //    the SQL substrate. Individually crash-consistent, not LSN-pinned —
    //    see the module docs.
    copy_dir_filtered(data_dir, snapshot_data, &|rel| {
        if is_runtime_only(rel) {
            return false;
        }
        let first = rel.components().next();
        !matches!(first, Some(c)
            if c.as_os_str() == db_name.as_os_str()
                || c.as_os_str() == wal_dir_name.as_os_str()
                || c.as_os_str() == single_wal_name.as_os_str())
    })?;

    Ok(end)
}

/// Rebuild a WAL directory inside the snapshot holding every record at or
/// below `end_lsn`.
fn copy_wal_upto(src_dir: &Path, dst_dir: &Path, end_lsn: u64) -> io::Result<()> {
    if !src_dir.is_dir() {
        return Ok(());
    }
    std::fs::create_dir_all(dst_dir)?;
    let mut segs = crate::storage::wal::list_archive_segments(src_dir)?;
    segs.sort_unstable();
    for seg in segs {
        let src = crate::storage::wal::segment_file_path(src_dir, seg);
        let dst = crate::storage::wal::segment_file_path(dst_dir, seg);
        if crate::storage::wal::copy_segment_prefix_upto_lsn(&src, &dst, end_lsn)?.is_none() {
            let _ = std::fs::remove_file(&dst);
        }
    }
    Ok(())
}

/// Refuse a destination inside the source data directory.
///
/// The tree copy would descend into the snapshot it is writing and copy it into
/// itself until the path exceeds the OS limit, surfacing as a baffling
/// "File name too long" rather than "you asked for something impossible".
/// `BACKUP DATABASE TO '/var/lib/nucleus/data/backup'` is an easy thing to type,
/// so it must fail clearly and immediately.
fn reject_nested_destination(data_dir: &Path, output_dir: &Path) -> io::Result<()> {
    let src = data_dir.canonicalize().unwrap_or_else(|_| data_dir.to_path_buf());
    // The destination usually does not exist yet: canonicalize its nearest
    // existing ancestor, then re-attach the remainder.
    let mut probe = output_dir.to_path_buf();
    let mut tail: Vec<std::ffi::OsString> = Vec::new();
    let dst = loop {
        if let Ok(mut c) = probe.canonicalize() {
            for part in tail.iter().rev() {
                c.push(part);
            }
            break c;
        }
        match probe.file_name() {
            Some(name) => {
                tail.push(name.to_os_string());
                if !probe.pop() {
                    break output_dir.to_path_buf();
                }
            }
            None => break output_dir.to_path_buf(),
        }
    };
    if dst.starts_with(&src) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "backup destination {} is inside the data directory {} — the snapshot would \
                 copy itself recursively. Choose a destination outside the data directory.",
                dst.display(),
                src.display()
            ),
        ));
    }
    Ok(())
}

fn prepare_output_dir(output_dir: &Path, force: bool) -> io::Result<()> {
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
    std::fs::create_dir_all(output_dir)
}

fn write_manifest(output_dir: &Path, manifest: &BackupManifest) -> io::Result<()> {
    let json = serde_json::to_string_pretty(manifest)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    std::fs::write(output_dir.join(MANIFEST_NAME), json)
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Restore
// ---------------------------------------------------------------------------

/// Restore a snapshot at `input_dir` into `data_dir`. Refuses to overwrite a
/// non-empty `data_dir` unless `force`, refuses a format mismatch, refuses a
/// snapshot whose checksums do not match, and refuses to overwrite a different
/// database. Returns the manifest that was restored.
///
/// Every check runs before the destination is touched: a refused restore
/// leaves `data_dir` byte-for-byte unchanged.
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

    // Integrity before anything else destructive: a snapshot whose bytes moved
    // since it was taken must never be laid down over a working database.
    verify_snapshot(input_dir, &manifest)?;

    if DataDirLock::is_locked(data_dir) {
        return Err(io::Error::new(
            io::ErrorKind::ResourceBusy,
            format!(
                "{} is open by a running Nucleus instance — stop it before restoring over it",
                data_dir.display()
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
        // `force` says "overwrite this database", not "overwrite whichever
        // database happens to be here". A different identity is the disaster
        // case, so it takes a deliberate act (removing the directory) rather
        // than a flag that was probably already in the operator's shell
        // history.
        let existing_id = std::fs::read_to_string(data_dir.join(DB_ID_NAME))
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        if !existing_id.is_empty()
            && !manifest.database_id.is_empty()
            && existing_id != manifest.database_id
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "refusing to restore: {} holds database {} but the snapshot is of database \
                     {}. If replacing a different database is intended, remove the directory \
                     first.",
                    data_dir.display(),
                    existing_id,
                    manifest.database_id
                ),
            ));
        }
        std::fs::remove_dir_all(data_dir)?;
    }

    copy_dir_recursive(&input_dir.join(DATA_SUBDIR), data_dir)?;
    Ok(manifest)
}

/// Verify a snapshot's contents against its manifest without touching any
/// destination. Public so operators can validate an archived snapshot on its
/// own — restore verification you can only get by running a restore is not
/// restore verification.
pub fn verify_snapshot(input_dir: &Path, manifest: &BackupManifest) -> io::Result<()> {
    if manifest.files.is_empty() {
        // Legacy (pre-checksum) manifest: nothing to verify against. Say so
        // rather than implying the snapshot was checked.
        tracing::warn!(
            "backup at {} predates per-file checksums — restoring unverified",
            input_dir.display()
        );
        return Ok(());
    }
    let root = input_dir.join(DATA_SUBDIR);
    let actual = fingerprint_tree(&root)?;
    let expected: std::collections::HashMap<&str, &BackupFile> = manifest
        .files
        .iter()
        .map(|f| (f.path.as_str(), f))
        .collect();
    let mut problems: Vec<String> = Vec::new();
    for got in &actual {
        match expected.get(got.path.as_str()) {
            None => problems.push(format!("{}: present in snapshot but not in manifest", got.path)),
            Some(want) => {
                if want.len != got.len {
                    problems.push(format!(
                        "{}: length {}, manifest says {}",
                        got.path, got.len, want.len
                    ));
                } else if want.blake3 != got.blake3 {
                    problems.push(format!("{}: checksum mismatch", got.path));
                }
            }
        }
    }
    let seen: std::collections::HashSet<&str> =
        actual.iter().map(|f| f.path.as_str()).collect();
    for want in &manifest.files {
        if !seen.contains(want.path.as_str()) {
            problems.push(format!("{}: missing from snapshot", want.path));
        }
    }
    if problems.is_empty() {
        return Ok(());
    }
    problems.sort();
    problems.truncate(20);
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "backup at {} failed integrity verification and was NOT restored — \
             the destination is unchanged. Problems:\n  {}",
            input_dir.display(),
            problems.join("\n  ")
        ),
    ))
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

    /// Content fingerprint of a directory tree — used to prove a refused
    /// operation changed nothing.
    fn dir_fingerprint(dir: &Path) -> Vec<(String, u64, String)> {
        let mut out = Vec::new();
        let mut stack = vec![dir.to_path_buf()];
        while let Some(d) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&d) else {
                continue;
            };
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                } else if let Ok(bytes) = std::fs::read(&p) {
                    out.push((
                        p.strip_prefix(dir).unwrap().to_string_lossy().into_owned(),
                        bytes.len() as u64,
                        blake3::hash(&bytes).to_hex().to_string(),
                    ));
                }
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
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
        // The three data files plus the identity file the backup established.
        let checksummed: std::collections::HashSet<&str> =
            m.files.iter().map(|f| f.path.as_str()).collect();
        for rel in ["catalog.json", "wal/000001.wal", "storage/t.dat", DB_ID_NAME] {
            assert!(
                checksummed.contains(rel),
                "{rel} is missing from the manifest's checksums: {:?}",
                m.files
            );
        }
        assert_eq!(
            m.files.len(),
            4,
            "every file in the snapshot must be checksummed, and only those: {:?}",
            m.files
        );

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
    fn legacy_manifest_restores_without_checksums() {
        // A pre-checksum manifest has no `files` list. It must still restore
        // (old backups stay usable) rather than fail verification.
        let root = unique_tmp("legacynochecksum");
        let _ = std::fs::remove_dir_all(&root);
        let data = root.join("data_dir");
        let snap = root.join("snap");
        write(&data, "catalog.json", b"{}");
        backup_data_dir(&data, &snap, false, "0.1.1").unwrap();

        let mpath = snap.join(MANIFEST_NAME);
        let mut manifest: BackupManifest =
            serde_json::from_str(&std::fs::read_to_string(&mpath).unwrap()).unwrap();
        manifest.files.clear();
        manifest.database_id.clear();
        std::fs::write(&mpath, serde_json::to_string_pretty(&manifest).unwrap()).unwrap();

        restore_data_dir(&snap, &root.join("restored"), false, "0.1.1")
            .expect("legacy checksum-less manifest must remain restorable");
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

    // ── Manifest hardening ─────────────────────────────────────────────

    #[test]
    fn corrupted_snapshot_is_rejected_without_touching_the_destination() {
        // The sharp one: a snapshot whose bytes changed after it was taken
        // must be refused, and the refusal must not damage the database the
        // operator was about to restore over.
        let root = unique_tmp("corrupt");
        let _ = std::fs::remove_dir_all(&root);
        let data = root.join("data_dir");
        let snap = root.join("snap");
        write(&data, "catalog.json", b"{\"tables\":1}");
        write(&data, "storage/t.dat", b"alice\x00bob");
        backup_data_dir(&data, &snap, false, "0.1.1").unwrap();

        // Flip a byte inside the snapshot (bit rot / tampering / bad media).
        let victim = snap.join(DATA_SUBDIR).join("storage/t.dat");
        std::fs::write(&victim, b"alice\x00BOB").unwrap();

        // A live destination database the operator would lose if the restore
        // laid down a corrupt snapshot or half-deleted the directory first.
        let target = root.join("live");
        write(&target, "catalog.json", b"{\"tables\":7}");
        write(&target, "storage/t.dat", b"precious");
        let before = dir_fingerprint(&target);

        let err = restore_data_dir(&snap, &target, true, "0.1.1").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let msg = err.to_string();
        assert!(
            msg.contains("checksum mismatch") && msg.contains("storage/t.dat"),
            "error must name the corrupted file: {msg}"
        );
        assert_eq!(
            before,
            dir_fingerprint(&target),
            "a refused restore modified the destination"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn snapshot_with_an_injected_file_is_rejected() {
        let root = unique_tmp("injected");
        let _ = std::fs::remove_dir_all(&root);
        let data = root.join("data_dir");
        let snap = root.join("snap");
        write(&data, "catalog.json", b"{}");
        backup_data_dir(&data, &snap, false, "0.1.1").unwrap();
        write(&snap.join(DATA_SUBDIR), "evil.json", b"{}");

        let err = restore_data_dir(&snap, &root.join("restored"), false, "0.1.1").unwrap_err();
        assert!(
            err.to_string().contains("not in manifest"),
            "unexpected error: {err}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn snapshot_with_a_deleted_file_is_rejected() {
        let root = unique_tmp("deleted");
        let _ = std::fs::remove_dir_all(&root);
        let data = root.join("data_dir");
        let snap = root.join("snap");
        write(&data, "catalog.json", b"{}");
        write(&data, "storage/t.dat", b"rows");
        backup_data_dir(&data, &snap, false, "0.1.1").unwrap();
        std::fs::remove_file(snap.join(DATA_SUBDIR).join("storage/t.dat")).unwrap();

        let err = restore_data_dir(&snap, &root.join("restored"), false, "0.1.1").unwrap_err();
        assert!(
            err.to_string().contains("missing from snapshot"),
            "unexpected error: {err}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn manifest_carries_a_stable_database_identity() {
        let root = unique_tmp("dbid");
        let _ = std::fs::remove_dir_all(&root);
        let data = root.join("data_dir");
        write(&data, "catalog.json", b"{}");

        let a = backup_data_dir(&data, &root.join("snap_a"), false, "0.1.1").unwrap();
        let b = backup_data_dir(&data, &root.join("snap_b"), false, "0.1.1").unwrap();
        assert!(!a.database_id.is_empty(), "identity must be populated");
        assert_eq!(
            a.database_id, b.database_id,
            "identity must be stable across backups of the same database"
        );

        // A different database gets a different identity.
        let other = root.join("other_dir");
        write(&other, "catalog.json", b"{}");
        let c = backup_data_dir(&other, &root.join("snap_c"), false, "0.1.1").unwrap();
        assert_ne!(a.database_id, c.database_id);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn restore_refuses_to_overwrite_a_different_database() {
        let root = unique_tmp("wrongdb");
        let _ = std::fs::remove_dir_all(&root);
        let a = root.join("db_a");
        let b = root.join("db_b");
        write(&a, "catalog.json", b"{\"a\":1}");
        write(&b, "catalog.json", b"{\"b\":1}");
        let snap = root.join("snap_a");
        backup_data_dir(&a, &snap, false, "0.1.1").unwrap();
        // Give b an identity of its own.
        let _ = database_id(&b);

        let before = dir_fingerprint(&b);
        let err = restore_data_dir(&snap, &b, true, "0.1.1").unwrap_err();
        assert!(
            err.to_string().contains("refusing to restore"),
            "unexpected error: {err}"
        );
        assert_eq!(before, dir_fingerprint(&b), "refusal damaged the destination");
        let _ = std::fs::remove_dir_all(&root);
    }

    // ── In-use detection ───────────────────────────────────────────────

    #[test]
    fn backup_refuses_a_data_dir_a_live_instance_holds() {
        let root = unique_tmp("inuse");
        let _ = std::fs::remove_dir_all(&root);
        let data = root.join("data_dir");
        write(&data, "catalog.json", b"{}");

        let lock = DataDirLock::acquire(&data).unwrap().expect("lock acquired");
        assert!(DataDirLock::is_locked(&data));

        let err = backup_data_dir(&data, &root.join("snap"), false, "0.1.1").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ResourceBusy);
        assert!(
            err.to_string().contains("TORN"),
            "refusal must say why: {err}"
        );
        assert!(
            !root.join("snap").exists(),
            "a refused backup must not leave a snapshot directory behind"
        );

        // The explicit override still works, and records the caveat.
        let m =
            backup_data_dir_opts(&data, &root.join("snap"), false, "0.1.1", true).unwrap();
        assert!(
            m.taken_while_in_use,
            "an override backup must be marked inconsistent in the manifest"
        );
        // The lock file itself is never captured in the snapshot.
        assert!(!root.join("snap").join(DATA_SUBDIR).join(LOCK_NAME).exists());

        drop(lock);
        assert!(!DataDirLock::is_locked(&data));
        backup_data_dir(&data, &root.join("snap2"), false, "0.1.1")
            .expect("a released directory must back up normally");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_stale_lock_file_does_not_block_a_backup() {
        // A crashed instance leaves the file behind but not the OS lock.
        // Refusing on mere file existence would make every post-crash backup
        // impossible — exactly when one is most needed.
        let root = unique_tmp("stalelock");
        let _ = std::fs::remove_dir_all(&root);
        let data = root.join("data_dir");
        write(&data, "catalog.json", b"{}");
        write(&data, LOCK_NAME, b"pid 999999 since 0\n");

        assert!(!DataDirLock::is_locked(&data));
        backup_data_dir(&data, &root.join("snap"), false, "0.1.1")
            .expect("a stale lock file must not block a backup");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn restore_refuses_a_destination_a_live_instance_holds() {
        let root = unique_tmp("restorebusy");
        let _ = std::fs::remove_dir_all(&root);
        let data = root.join("data_dir");
        write(&data, "catalog.json", b"{}");
        let snap = root.join("snap");
        backup_data_dir(&data, &snap, false, "0.1.1").unwrap();

        let target = root.join("live");
        write(&target, "catalog.json", b"{}");
        let lock = DataDirLock::acquire(&target).unwrap().expect("lock acquired");
        let before = dir_fingerprint(&target);
        let err = restore_data_dir(&snap, &target, true, "0.1.1").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ResourceBusy);
        assert_eq!(before, dir_fingerprint(&target));
        drop(lock);
        let _ = std::fs::remove_dir_all(&root);
    }
}
