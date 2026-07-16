//! Point-in-time recovery (PITR) for the segmented page WAL.
//!
//! PITR builds on two primitives:
//!   1. A physical base snapshot (see [`crate::backup`]) — the data directory
//!      as of some checkpoint.
//!   2. Continuous WAL archiving (see [`crate::storage::wal::SegmentedWal`]) —
//!      every WAL segment is copied to an archive directory the moment it is
//!      sealed, and never deleted from the live WAL until it is archived.
//!
//! Restore replays the archived (and base-resident) WAL forward from the base
//! snapshot up to a target — an exact LSN, a wall-clock time (segment
//! granularity, resolved via the archive index), or the latest archived point.
//! It does so WITHOUT re-serializing records: each segment is byte-copied up to
//! the last record at or before the target (a valid replayable prefix, every
//! CRC preserved), assembled into a fresh WAL directory. The reconstructed
//! database then recovers through the engine's normal crash-recovery path when
//! it is next opened — so replay stays encryption/compression-agnostic and
//! reuses the exact, battle-tested apply logic.
//!
//! Scope (v1): the SQL disk engine's segmented page WAL — the system-of-record
//! substrate. Model-specific logical WALs (KV/columnar/FTS/streams/MVCC) archive
//! their segments through the same primitive where they use it, but forward
//! logical replay of those is future work; this module covers the page WAL.

use std::io;
use std::path::{Path, PathBuf};

use crate::storage::wal;

/// How far forward to replay from the base snapshot.
#[derive(Debug, Clone, Copy)]
pub enum PitrTarget {
    /// Replay through this exact LSN (inclusive).
    Lsn(u64),
    /// Replay through the last segment archived at or before this Unix time
    /// (seconds). Segment granularity — resolved via the archive index.
    UnixSeconds(u64),
    /// Replay everything available in the archive + base WAL.
    Latest,
}

/// Outcome of a PITR restore.
#[derive(Debug, Clone)]
pub struct PitrReport {
    /// The LSN the restore aimed for (`u64::MAX` for `Latest`).
    pub target_lsn: u64,
    /// The highest LSN actually placed into the reconstructed WAL. May be below
    /// `target_lsn` if the archive does not extend that far.
    pub restored_lsn: u64,
    /// Number of WAL segments written into the reconstructed WAL directory.
    pub segments_written: usize,
}

/// Restore a database to a point in time.
///
/// Restores the physical base snapshot at `base_snapshot` into `out_data_dir`,
/// then reconstructs the WAL directory for `db_file` from `archive_dir` (plus
/// the base's own retained WAL) truncated at `target`. Opening the resulting
/// data directory replays the reconstructed WAL up to the target.
///
/// `db_file` is the primary data file name within the data dir (e.g.
/// `"nucleus.db"`); its `.wal.d` / `.wal` siblings are the WAL locations.
pub fn restore_pitr(
    base_snapshot: &Path,
    archive_dir: &Path,
    target: PitrTarget,
    out_data_dir: &Path,
    db_file: &str,
    nucleus_version: &str,
    force: bool,
) -> io::Result<PitrReport> {
    // 1. Lay down the physical base (format-locked, refuses a dirty target).
    crate::backup::restore_data_dir(base_snapshot, out_data_dir, force, nucleus_version)?;

    // 2. Resolve the target LSN.
    let target_lsn = resolve_target_lsn(archive_dir, target)?;

    // 3. Locate the restored WAL locations for this db file.
    let db_path = Path::new(db_file);
    let wal_dir_name = db_path.with_extension("wal.d");
    let single_wal_name = db_path.with_extension("wal");
    let restored_wal_dir = out_data_dir.join(&wal_dir_name);
    let restored_single_wal = out_data_dir.join(&single_wal_name);

    // 4. Gather candidate segment sources, in a stable order. Duplicate records
    //    across sources are harmless: recovery sorts by LSN and applies
    //    last-write-wins, and identical (lsn,page) writes are idempotent.
    let mut sources: Vec<PathBuf> = Vec::new();
    if archive_dir.is_dir() {
        let mut segs = wal::list_archive_segments(archive_dir)?;
        segs.sort_unstable();
        for s in segs {
            sources.push(wal::segment_file_path(archive_dir, s));
        }
    }
    if restored_wal_dir.is_dir() {
        let mut segs = wal::list_archive_segments(&restored_wal_dir)?;
        segs.sort_unstable();
        for s in segs {
            sources.push(wal::segment_file_path(&restored_wal_dir, s));
        }
    }
    if restored_single_wal.is_file() {
        sources.push(restored_single_wal.clone());
    }

    // 5. Assemble a fresh WAL directory holding every record <= target_lsn.
    let staging = out_data_dir.join(format!(
        "{}.pitr-staging",
        wal_dir_name.to_string_lossy()
    ));
    if staging.exists() {
        std::fs::remove_dir_all(&staging)?;
    }
    std::fs::create_dir_all(&staging)?;

    let mut seq: u64 = 1;
    let mut restored_lsn: u64 = 0;
    for src in &sources {
        // Skip a segment entirely if its lowest LSN is already past the target.
        if let Some((min_lsn, _max)) = wal::segment_lsn_bounds(src)
            && min_lsn > target_lsn
        {
            continue;
        }
        let dst = wal::segment_file_path(&staging, seq);
        match wal::copy_segment_prefix_upto_lsn(src, &dst, target_lsn)? {
            Some(max_copied) => {
                restored_lsn = restored_lsn.max(max_copied);
                seq += 1;
            }
            None => {
                // Nothing at or below the target in this source — drop the empty
                // destination file if the copy created one.
                let _ = std::fs::remove_file(&dst);
            }
        }
    }

    // 6. Swap the reconstructed WAL into place and neutralize the base's WAL so
    //    a reopen never replays past the target.
    if restored_wal_dir.exists() {
        std::fs::remove_dir_all(&restored_wal_dir)?;
    }
    std::fs::rename(&staging, &restored_wal_dir)?;
    if restored_single_wal.exists() {
        std::fs::remove_file(&restored_single_wal)?;
    }

    Ok(PitrReport {
        target_lsn,
        restored_lsn,
        segments_written: (seq - 1) as usize,
    })
}

/// Resolve a [`PitrTarget`] into a concrete inclusive LSN cutoff.
fn resolve_target_lsn(archive_dir: &Path, target: PitrTarget) -> io::Result<u64> {
    match target {
        PitrTarget::Lsn(n) => Ok(n),
        PitrTarget::Latest => Ok(u64::MAX),
        PitrTarget::UnixSeconds(t) => {
            let idx = archive_dir.join(wal::ARCHIVE_INDEX_NAME);
            let contents = std::fs::read_to_string(&idx).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "time-based PITR needs the archive index {}: {e}",
                        idx.display()
                    ),
                )
            })?;
            // Each line: `<seg> <min_lsn> <max_lsn> <archived_unix>`. Take the
            // highest max_lsn among segments archived at or before `t`.
            let mut best: u64 = 0;
            let mut matched = false;
            for line in contents.lines() {
                let mut it = line.split_whitespace();
                let (_seg, _min, max, unix) =
                    match (it.next(), it.next(), it.next(), it.next()) {
                        (Some(a), Some(b), Some(c), Some(d)) => (a, b, c, d),
                        _ => continue,
                    };
                let (Ok(max_lsn), Ok(archived_unix)) = (max.parse::<u64>(), unix.parse::<u64>())
                else {
                    continue;
                };
                if archived_unix <= t {
                    best = best.max(max_lsn);
                    matched = true;
                }
            }
            if !matched {
                // No segment archived by the target time → restore to base only.
                Ok(0)
            } else {
                Ok(best)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::wal::{SegmentedWal, SyncMode};

    fn tmp(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("nucleus_pitr_{tag}_{}", std::process::id()))
    }

    // A page image whose first bytes encode a marker, so we can assert which
    // version of a page a restore landed on.
    fn page_with(marker: u8) -> Box<[u8; crate::storage::page::PAGE_SIZE]> {
        let mut p = Box::new([0u8; crate::storage::page::PAGE_SIZE]);
        p[0] = marker;
        p
    }

    #[test]
    fn archive_preserves_sealed_segments_and_index() {
        let root = tmp("archive_seal");
        let _ = std::fs::remove_dir_all(&root);
        let wal_dir = root.join("wal.d");
        let archive = root.join("archive");
        // Tiny segments so each handful of writes rotates and archives.
        let w =
            SegmentedWal::open_with_archive(&wal_dir, 12_000, SyncMode::Fsync, &archive).unwrap();
        for i in 0..40u32 {
            w.log_page_write(1, i, &page_with((i % 250) as u8 + 1)).unwrap();
        }
        w.sync().unwrap();
        // Several segments must exist in the archive, with matching index lines.
        let archived = wal::list_archive_segments(&archive).unwrap();
        assert!(
            archived.len() >= 2,
            "expected multiple archived segments, got {archived:?}"
        );
        let idx = std::fs::read_to_string(archive.join(wal::ARCHIVE_INDEX_NAME)).unwrap();
        assert_eq!(
            idx.lines().count(),
            archived.len(),
            "one index line per archived segment"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn truncate_before_never_deletes_unarchived_segment() {
        // With archiving on, truncate_before must archive a segment before
        // removing it. After truncation, every removed segment is in the archive.
        let root = tmp("truncate_guard");
        let _ = std::fs::remove_dir_all(&root);
        let wal_dir = root.join("wal.d");
        let archive = root.join("archive");
        let w =
            SegmentedWal::open_with_archive(&wal_dir, 12_000, SyncMode::Fsync, &archive).unwrap();
        let mut last = 0;
        for i in 0..60u32 {
            last = w.log_page_write(1, i, &page_with(7)).unwrap();
        }
        w.sync().unwrap();
        // Truncate everything below the final LSN — reclaims sealed segments.
        w.truncate_before(last).unwrap();
        // Any segment number that existed and is now gone from the live dir must
        // be present in the archive.
        let live: std::collections::HashSet<u64> =
            wal::list_archive_segments(&wal_dir).unwrap().into_iter().collect();
        let archived: std::collections::HashSet<u64> =
            wal::list_archive_segments(&archive).unwrap().into_iter().collect();
        assert!(!archived.is_empty(), "truncate should have archived segments");
        // Every archived segment's records are recoverable: read them back.
        for s in &archived {
            if !live.contains(s) {
                let recs =
                    wal::read_wal_records(&wal::segment_file_path(&archive, *s)).unwrap();
                assert!(!recs.is_empty(), "archived segment {s} must be replayable");
            }
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn prefix_copy_cuts_at_target_lsn() {
        let root = tmp("prefix_cut");
        let _ = std::fs::remove_dir_all(&root);
        let wal_dir = root.join("wal.d");
        std::fs::create_dir_all(&wal_dir).unwrap();
        // One big segment holding LSNs 1..=20.
        let w = SegmentedWal::open(&wal_dir, 10 * 1024 * 1024).unwrap();
        let mut lsns = Vec::new();
        for i in 0..20u32 {
            lsns.push(w.log_page_write(1, i, &page_with(1)).unwrap());
        }
        w.sync().unwrap();
        let src = wal::segment_file_path(&wal_dir, 1);
        let dst = root.join("cut.log");
        let target = lsns[9]; // keep the first 10 records
        let max_copied = wal::copy_segment_prefix_upto_lsn(&src, &dst, target)
            .unwrap()
            .unwrap();
        assert_eq!(max_copied, target);
        let recs = wal::read_wal_records(&dst).unwrap();
        assert_eq!(recs.len(), 10);
        assert!(recs.iter().all(|r| r.lsn <= target));
        let _ = std::fs::remove_dir_all(&root);
    }
}
