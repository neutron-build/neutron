//! Write-ahead log for the Geo R-tree spatial index.
//!
//! Provides crash-recovery by recording all point mutations to an append-only
//! log file (`geo.wal`). On restart the log is replayed to reconstruct the
//! R-tree from scratch (R-trees don't support incremental WAL replay -- the
//! tree must be rebuilt from the full point list).
//!
//! ## Log entry binary format
//! ```text
//! INSERT:   [0x01] [doc_id: u64 LE] [x: f64 LE] [y: f64 LE]
//!           [name_len: u32 LE] [name: bytes]
//!           [n_props: u32 LE] [per prop: key_len(u32) + key + val_len(u32) + val]
//! DELETE:   [0x02] [doc_id: u64 LE]
//! SNAPSHOT: [0x03] [n_points: u32 LE]
//!           [per point: doc_id(u64) + x(f64) + y(f64) + name_len(u32) + name
//!            + n_props(u32) + per prop: key_len(u32) + key + val_len(u32) + val]
//! ```
//!
//! A SNAPSHOT resets all state. After `checkpoint()` the file is truncated to
//! a single SNAPSHOT entry so the log stays small.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

use crate::storage::wal_util::WalSync;

use super::{Point, RTree};

// ---- Entry type tags --------------------------------------------------------

const ENTRY_INSERT: u8 = 0x01;
const ENTRY_DELETE: u8 = 0x02;
const ENTRY_SNAPSHOT: u8 = 0x03;

// ---- Public types -----------------------------------------------------------

/// A recovered geo point from WAL replay.
#[derive(Debug, Clone)]
pub struct GeoWalPoint {
    pub doc_id: u64,
    pub x: f64,
    pub y: f64,
    pub name: String,
    pub properties: HashMap<String, String>,
}

/// Recovered geo state from WAL replay.
pub struct GeoWalState {
    /// `doc_id -> GeoWalPoint` for all live points.
    pub points: HashMap<u64, GeoWalPoint>,
}

/// Append-only Geo WAL.
pub struct GeoWal {
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
    /// Append/sync bookkeeping for group commit. These appends previously
    /// ended at `BufWriter::flush`, which only moves bytes into the kernel, so
    /// an acknowledged point mutation survived `kill -9` but not power loss.
    /// NU-006.
    syncer: WalSync,
    /// The writer holds an inode a checkpoint's rename displaced: it is
    /// unlinked, so appends to it "succeed" into a file no future recovery
    /// reads while `group_sync`/`is_dirty` report healthy. Set when a
    /// checkpoint replaced the log but its reopen failed; cleared by the next
    /// successful reattach (or checkpoint reopen). See `reattach_if_stranded`.
    stranded: std::sync::atomic::AtomicBool,
    /// Test-only one-shot checkpoint-reopen fault; see `checkpoint`.
    #[cfg(test)]
    fail_reopen_once: std::sync::atomic::AtomicBool,
}

impl GeoWal {
    /// Open or create the WAL file in `dir`.
    ///
    /// Returns `(wal, recovered_state)`. If no WAL file exists the recovered
    /// state is empty. Corrupt trailing bytes are silently ignored (best-effort
    /// recovery).
    pub fn open(dir: &Path) -> io::Result<(Self, GeoWalState)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("geo.wal");
        let state = if path.exists() {
            let data = std::fs::read(&path)?;
            replay(&data)
        } else {
            GeoWalState {
                points: HashMap::new(),
            }
        };
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok((
            Self {
                path,
                writer: Mutex::new(BufWriter::new(file)),
                syncer: WalSync::new(),
                stranded: std::sync::atomic::AtomicBool::new(false),
                #[cfg(test)]
                fail_reopen_once: std::sync::atomic::AtomicBool::new(false),
            },
            state,
        ))
    }

    /// Re-point the writer at the live log file after a checkpoint replaced
    /// the file but could not reopen it. While stranded, `writer` holds an
    /// UNLINKED inode — appends to it succeed into a file no future recovery
    /// reads — so this runs before every append: a successful reopen recovers
    /// the writer, and a failed one fails the append loudly instead of
    /// letting it acknowledge a write to a dead inode.
    fn reattach_if_stranded(&self, w: &mut BufWriter<File>) -> io::Result<()> {
        if !self.stranded.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }
        if let Some(e) = crate::storage::crashpoint::io_fault("geo.wal_reopen") {
            return Err(e);
        }
        let file = OpenOptions::new()
            .append(true)
            .open(&self.path)
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!(
                        "geo WAL writer is stranded: a checkpoint replaced {} but its \
                         reopen failed; refusing to append to the unlinked old file ({e})",
                        self.path.display()
                    ),
                )
            })?;
        *w = BufWriter::new(file);
        self.stranded
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Flush + `fsync` the log, capturing (under the writer lock) the highest
    /// append LSN the fsync covers.
    fn sync_covering(&self) -> io::Result<u64> {
        let mut w = self.writer.lock();
        let covered = self.syncer.current();
        w.flush()?;
        w.get_ref().sync_all()?;
        Ok(covered)
    }

    /// Group-commit sync: durable coverage of every append made before this
    /// call; concurrent committers share fsyncs.
    pub fn group_sync(&self) -> io::Result<()> {
        self.syncer.group_sync(|| self.sync_covering())
    }

    /// Whether appends exist that no completed fsync covers yet.
    pub fn is_dirty(&self) -> bool {
        self.syncer.is_dirty()
    }

    /// Log an INSERT operation (point insert).
    pub fn log_insert(
        &self,
        doc_id: u64,
        point: &Point,
        name: &str,
        properties: &HashMap<String, String>,
    ) -> io::Result<()> {
        let mut buf = Vec::new();
        buf.push(ENTRY_INSERT);

        // doc_id
        buf.extend_from_slice(&doc_id.to_le_bytes());

        // x, y
        buf.extend_from_slice(&point.x.to_le_bytes());
        buf.extend_from_slice(&point.y.to_le_bytes());

        // name
        write_str(&mut buf, name);

        // properties
        buf.extend_from_slice(&(properties.len() as u32).to_le_bytes());
        for (k, v) in properties {
            write_str(&mut buf, k);
            write_str(&mut buf, v);
        }

        let mut w = self.writer.lock();
        self.reattach_if_stranded(&mut w)?;
        w.write_all(&buf)?;
        w.flush()?;
        self.syncer.on_append();
        Ok(())
    }

    /// Log a DELETE operation.
    pub fn log_delete(&self, doc_id: u64) -> io::Result<()> {
        let mut buf = Vec::new();
        buf.push(ENTRY_DELETE);
        buf.extend_from_slice(&doc_id.to_le_bytes());

        let mut w = self.writer.lock();
        self.reattach_if_stranded(&mut w)?;
        w.write_all(&buf)?;
        w.flush()?;
        self.syncer.on_append();
        Ok(())
    }

    /// Write a full snapshot and truncate the log to just that snapshot.
    ///
    /// `points` is the current set of all live points.
    pub fn checkpoint(&self, points: &HashMap<u64, GeoWalPoint>) -> io::Result<()> {
        let mut payload = Vec::new();

        // n_points
        payload.extend_from_slice(&(points.len() as u32).to_le_bytes());

        for point in points.values() {
            // doc_id
            payload.extend_from_slice(&point.doc_id.to_le_bytes());

            // x, y
            payload.extend_from_slice(&point.x.to_le_bytes());
            payload.extend_from_slice(&point.y.to_le_bytes());

            // name
            write_str(&mut payload, &point.name);

            // properties
            payload.extend_from_slice(&(point.properties.len() as u32).to_le_bytes());
            for (k, v) in &point.properties {
                write_str(&mut payload, k);
                write_str(&mut payload, v);
            }
        }

        // Flush existing writer
        {
            self.writer.lock().flush()?;
        }

        // CRASH SAFETY: stage the snapshot, fsync it, then swap it in with an
        // atomic rename. Truncating the live log first destroyed the only
        // durable copy — a crash before the rewrite finished lost every point.
        // (Same defect the crash matrix found in the MVCC WAL compaction; the
        // graph and blob WALs already use this pattern.)
        let tmp_path = self.path.with_extension("wal.compacting");
        {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;
            let mut w = BufWriter::new(file);
            w.write_all(&[ENTRY_SNAPSHOT])?;
            w.write_all(&payload)?;
            w.flush()?;
            w.get_ref().sync_all()?;
        }
        let mut guard = self.writer.lock();
        std::fs::rename(&tmp_path, &self.path)?;
        if let Some(dir) = self.path.parent()
            && let Ok(d) = std::fs::File::open(dir)
        {
            let _ = d.sync_all();
        }
        // The reopen is the hazardous half: the rename above already unlinked
        // the inode `guard` holds, so a failure here leaves the writer
        // pointing at a file no future recovery reads.
        #[cfg(test)]
        let injected: Option<io::Error> = self
            .fail_reopen_once
            .swap(false, std::sync::atomic::Ordering::AcqRel)
            .then(|| io::Error::other("injected geo WAL reopen failure"));
        #[cfg(not(test))]
        let injected: Option<io::Error> = None;
        let file = if let Some(e) = injected {
            Err(e)
        } else if let Some(e) = crate::storage::crashpoint::io_fault("geo.wal_reopen") {
            Err(e)
        } else {
            OpenOptions::new().append(true).open(&self.path)
        };
        let file = match file {
            Ok(f) => f,
            Err(e) => {
                // The rename already happened, so the handle in `guard` is
                // now an unlinked inode. Mark the writer stranded: appends
                // must reattach (or fail loudly), never write through it.
                self.stranded
                    .store(true, std::sync::atomic::Ordering::Release);
                return Err(e);
            }
        };
        // Re-open in append mode for future writes
        *guard = BufWriter::new(file);
        self.stranded
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    }
}

/// Reconstruct an R-tree from recovered WAL state.
///
/// R-trees don't support incremental replay, so we rebuild from the full
/// point list by inserting each point into a fresh tree.
pub fn rebuild_rtree(state: &GeoWalState) -> RTree {
    let mut tree = RTree::new();
    for point in state.points.values() {
        tree.insert(&Point::new(point.x, point.y), point.doc_id);
    }
    tree
}

// ---- Binary encoding helpers ------------------------------------------------

fn write_str(buf: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
    buf.extend_from_slice(b);
}

// ---- Replay -----------------------------------------------------------------

fn replay(data: &[u8]) -> GeoWalState {
    let mut points: HashMap<u64, GeoWalPoint> = HashMap::new();
    let mut pos = 0usize;

    while pos < data.len() {
        let Some(&entry_type) = data.get(pos) else {
            break;
        };
        pos += 1;

        match entry_type {
            ENTRY_INSERT => {
                let Some(point) = replay_insert(data, &mut pos) else {
                    break;
                };
                points.insert(point.doc_id, point);
            }
            ENTRY_DELETE => {
                let Some(doc_id) = read_u64(data, &mut pos) else {
                    break;
                };
                points.remove(&doc_id);
            }
            ENTRY_SNAPSHOT => {
                points.clear();
                if !replay_snapshot(data, &mut pos, &mut points) {
                    break;
                }
            }
            _ => {
                break;
            }
        }
    }

    GeoWalState { points }
}

fn replay_insert(data: &[u8], pos: &mut usize) -> Option<GeoWalPoint> {
    let doc_id = read_u64(data, pos)?;
    let x = read_f64(data, pos)?;
    let y = read_f64(data, pos)?;
    let name = read_string(data, pos)?;
    let n_props = read_u32(data, pos)? as usize;
    let mut properties = HashMap::new();
    for _ in 0..n_props {
        let k = read_string(data, pos)?;
        let v = read_string(data, pos)?;
        properties.insert(k, v);
    }
    Some(GeoWalPoint {
        doc_id,
        x,
        y,
        name,
        properties,
    })
}

fn replay_snapshot(data: &[u8], pos: &mut usize, points: &mut HashMap<u64, GeoWalPoint>) -> bool {
    let Some(n_points) = read_u32(data, pos) else {
        return false;
    };
    for _ in 0..n_points as usize {
        let Some(point) = replay_insert(data, pos) else {
            return false;
        };
        points.insert(point.doc_id, point);
    }
    true
}

// ---- Primitive readers ------------------------------------------------------

fn read_u32(data: &[u8], pos: &mut usize) -> Option<u32> {
    let b = data.get(*pos..*pos + 4)?;
    *pos += 4;
    Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

fn read_u64(data: &[u8], pos: &mut usize) -> Option<u64> {
    let b = data.get(*pos..*pos + 8)?;
    *pos += 8;
    Some(u64::from_le_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
    ]))
}

fn read_f64(data: &[u8], pos: &mut usize) -> Option<f64> {
    read_u64(data, pos).map(f64::from_bits)
}

fn read_string(data: &[u8], pos: &mut usize) -> Option<String> {
    let len = read_u32(data, pos)? as usize;
    if *pos + len > data.len() {
        return None;
    }
    let s = std::str::from_utf8(&data[*pos..*pos + len])
        .ok()?
        .to_string();
    *pos += len;
    Some(s)
}

// ---- Tests ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// An append is un-fsynced until `group_sync` covers it. NU-006.
    #[test]
    fn group_sync_marks_clean() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = GeoWal::open(dir.path()).unwrap();
        assert!(!wal.is_dirty(), "a fresh WAL has no un-fsynced appends");
        wal.log_insert(1, &Point::new(1.0, 2.0), "a", &HashMap::new())
            .unwrap();
        assert!(wal.is_dirty(), "an append is uncovered until fsync");
        wal.group_sync().unwrap();
        assert!(!wal.is_dirty(), "group_sync fsyncs the tail");
    }

    #[test]
    fn test_insert_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, state) = GeoWal::open(dir.path()).unwrap();
        assert!(state.points.is_empty());

        let mut props = HashMap::new();
        props.insert("city".to_string(), "New York".to_string());

        wal.log_insert(1, &Point::new(-74.006, 40.7128), "nyc", &props)
            .unwrap();
        wal.log_insert(2, &Point::new(-0.1278, 51.5074), "london", &HashMap::new())
            .unwrap();
        drop(wal);

        let (_wal2, state2) = GeoWal::open(dir.path()).unwrap();
        assert_eq!(state2.points.len(), 2);

        let nyc = &state2.points[&1];
        assert_eq!(nyc.name, "nyc");
        assert!((nyc.x - (-74.006)).abs() < 1e-10);
        assert!((nyc.y - 40.7128).abs() < 1e-10);
        assert_eq!(nyc.properties["city"], "New York");

        let london = &state2.points[&2];
        assert_eq!(london.name, "london");
        assert!(london.properties.is_empty());
    }

    #[test]
    fn test_delete_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = GeoWal::open(dir.path()).unwrap();

        wal.log_insert(1, &Point::new(0.0, 0.0), "origin", &HashMap::new())
            .unwrap();
        wal.log_insert(2, &Point::new(1.0, 1.0), "point2", &HashMap::new())
            .unwrap();
        wal.log_delete(1).unwrap();
        drop(wal);

        let (_wal2, state) = GeoWal::open(dir.path()).unwrap();
        assert_eq!(state.points.len(), 1);
        assert!(!state.points.contains_key(&1));
        assert!(state.points.contains_key(&2));
    }

    #[test]
    fn test_rebuild_rtree() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = GeoWal::open(dir.path()).unwrap();

        for i in 0..10 {
            let x = (i as f64) * 0.01;
            let y = (i as f64) * 0.01;
            wal.log_insert(i, &Point::new(x, y), &format!("p{}", i), &HashMap::new())
                .unwrap();
        }
        drop(wal);

        let (_wal2, state) = GeoWal::open(dir.path()).unwrap();
        let tree = rebuild_rtree(&state);
        assert_eq!(tree.len(), 10);

        // All points should be findable
        let bbox = super::super::BBox::new(-1.0, -1.0, 1.0, 1.0);
        let results = tree.search_bbox(&bbox);
        assert_eq!(results.len(), 10);
    }

    #[test]
    fn test_checkpoint_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = GeoWal::open(dir.path()).unwrap();

        wal.log_insert(1, &Point::new(0.0, 0.0), "a", &HashMap::new())
            .unwrap();
        wal.log_insert(2, &Point::new(1.0, 1.0), "b", &HashMap::new())
            .unwrap();

        // Checkpoint with only point 1
        let mut checkpoint_points = HashMap::new();
        checkpoint_points.insert(
            1,
            GeoWalPoint {
                doc_id: 1,
                x: 0.0,
                y: 0.0,
                name: "a".to_string(),
                properties: HashMap::new(),
            },
        );
        wal.checkpoint(&checkpoint_points).unwrap();

        // Add new point after checkpoint
        wal.log_insert(3, &Point::new(2.0, 2.0), "c", &HashMap::new())
            .unwrap();
        drop(wal);

        let (_wal2, state) = GeoWal::open(dir.path()).unwrap();
        // Point 2 was dropped by checkpoint, points 1 and 3 survive
        assert_eq!(state.points.len(), 2);
        assert!(state.points.contains_key(&1));
        assert!(state.points.contains_key(&3));
        assert!(!state.points.contains_key(&2));
    }

    /// S31-14: a checkpoint whose reopen fails must not leave the writer
    /// appending into the unlinked inode the rename displaced. Those appends
    /// report success while no future recovery can ever read them, so an
    /// acknowledged point silently vanishes at restart. The discriminator is
    /// durability: the post-failure insert must land in the replaced file.
    #[test]
    fn a_failed_checkpoint_reopen_does_not_strand_the_writer() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = GeoWal::open(dir.path()).unwrap();
            wal.log_insert(1, &Point::new(0.0, 0.0), "a", &HashMap::new())
                .unwrap();
            let mut points = HashMap::new();
            points.insert(
                1,
                GeoWalPoint {
                    doc_id: 1,
                    x: 0.0,
                    y: 0.0,
                    name: "a".to_string(),
                    properties: HashMap::new(),
                },
            );
            wal.fail_reopen_once
                .store(true, std::sync::atomic::Ordering::SeqCst);
            wal.checkpoint(&points)
                .expect_err("the injected reopen failure must fail the checkpoint");
            wal.log_insert(2, &Point::new(1.0, 1.0), "b", &HashMap::new())
                .expect("a later append must reattach, not strand");
        }
        let (_wal2, state) = GeoWal::open(dir.path()).unwrap();
        assert_eq!(
            state.points.len(),
            2,
            "the post-checkpoint-failure insert went to the unlinked inode: it \
             returned Ok and no recovery can ever read it"
        );
        assert!(state.points.contains_key(&2));
    }

    #[test]
    fn test_empty_open() {
        let dir = tempfile::tempdir().unwrap();
        let (_wal, state) = GeoWal::open(dir.path()).unwrap();
        assert!(state.points.is_empty());
    }

    #[test]
    fn test_corrupt_wal_graceful_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("geo.wal");

        {
            let (wal, _) = GeoWal::open(dir.path()).unwrap();
            wal.log_insert(42, &Point::new(10.0, 20.0), "good_point", &HashMap::new())
                .unwrap();
            drop(wal);
        }

        // Append garbage
        {
            let mut f = OpenOptions::new().append(true).open(&wal_path).unwrap();
            f.write_all(&[0xFF, 0xFE, 0xFD]).unwrap();
            f.flush().unwrap();
        }

        let (_wal, state) = GeoWal::open(dir.path()).unwrap();
        assert_eq!(state.points.len(), 1);
        assert!(state.points.contains_key(&42));
        assert_eq!(state.points[&42].name, "good_point");
    }

    #[test]
    fn test_overwrite_point() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = GeoWal::open(dir.path()).unwrap();

        let mut props1 = HashMap::new();
        props1.insert("version".to_string(), "1".to_string());
        wal.log_insert(1, &Point::new(0.0, 0.0), "p", &props1)
            .unwrap();

        let mut props2 = HashMap::new();
        props2.insert("version".to_string(), "2".to_string());
        wal.log_insert(1, &Point::new(5.0, 5.0), "p_moved", &props2)
            .unwrap();
        drop(wal);

        let (_wal2, state) = GeoWal::open(dir.path()).unwrap();
        assert_eq!(state.points.len(), 1);
        let p = &state.points[&1];
        assert_eq!(p.name, "p_moved");
        assert!((p.x - 5.0).abs() < 1e-10);
        assert_eq!(p.properties["version"], "2");
    }

    #[test]
    fn test_insert_with_properties() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = GeoWal::open(dir.path()).unwrap();

        let mut props = HashMap::new();
        props.insert("type".to_string(), "restaurant".to_string());
        props.insert("cuisine".to_string(), "italian".to_string());
        props.insert("rating".to_string(), "4.5".to_string());

        wal.log_insert(100, &Point::new(-73.99, 40.73), "joes_pizza", &props)
            .unwrap();
        drop(wal);

        let (_wal2, state) = GeoWal::open(dir.path()).unwrap();
        let p = &state.points[&100];
        assert_eq!(p.properties.len(), 3);
        assert_eq!(p.properties["type"], "restaurant");
        assert_eq!(p.properties["cuisine"], "italian");
        assert_eq!(p.properties["rating"], "4.5");
    }
}
