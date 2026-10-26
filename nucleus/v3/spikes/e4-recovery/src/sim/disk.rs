#![deny(clippy::indexing_slicing)]
//! The simulated disk (card X-5 Work 1.1) and the pre-registered fault
//! model (1.3) with taint tracking and the accept hook (1.4).
//!
//! What makes data durable (1.1), modelled the way the card fixes it:
//! - every file keeps its **durable bytes** and durable length, the list of
//!   **volatile writes** since the last covering sync in issue order, and a
//!   per-byte-range taint reference of the **last bytes the store issued**
//!   (kept across lives, 1.4);
//! - `sync_data` makes the file's data durable plus the length growth
//!   needed to read it back (POSIX `fdatasync`); `sync_all` also makes the
//!   length and metadata durable; a sync covers only its own file;
//! - directory operations (`create`, `rename`, `unlink`) are volatile until
//!   `sync_dir` of their directory;
//! - reads return the current (volatile-inclusive) view; bytes beyond the
//!   length read as zeros;
//! - the sector size is 512 B for tearing (pessimistic for NVMe's usual
//!   4 KiB atomic unit; stated in the README).
//!
//! F-CRASH (1.3), applied at every crash: each volatile write since its
//! file's last covering sync is resolved **per 512-B sector**, choosing
//! among that sector's own history — the durable value, or the value of a
//! per-sector *prefix* of the write history — so dropped writes, torn
//! writes and arbitrary reordering of unsynced writes are all covered.
//! Directory ops persist as a prefix of issue order per directory (this
//! models ext4's ordered metadata journal on n5000; assumption stated in
//! the README). A volatile length change persists only as far as the
//! surviving data requires.
//!
//! F-MEDIA-PAGE / F-MEDIA-WAL (class MP/MW only, at the round's crash,
//! after F-CRASH, never on a recovery crash): 1–3 (MP) or 1–2 (MW) durable
//! writes whose bytes are still current get LOST / MISDIRECTED / TORN.
//!
//! Taint (1.4): bytes returned by a read are **tainted** if they differ
//! from the last bytes the store issued to that range in any life of this
//! seed — including writes dropped by F-CRASH and bytes damaged by media
//! faults. Taint is computed per sub-range, and an issued interval that
//! starts before the read's offset still counts (a read that begins
//! mid-batch is judged over its whole extent; gate REWORK item 1). The
//! store calls [`SimDisk::accepted`] exactly where it accepts bytes as
//! valid; an accepted range that was tainted at read time — judged per
//! accepted byte against the most recent read that covers that byte, at
//! sub-range granularity (gate REWORK item 2 and round 2, item B) — is a
//! served corruption (O-3 failure), and accepted bytes no read ever
//! covered fail closed the same way (accepted unread: a harness or store
//! defect, never an open pass). Bytes never issued to cannot be tainted
//! (nothing to differ from); §3.3 still rejects stray bytes there
//! because they cannot match a reference.
//!
//! Determinism: iteration is over `BTreeMap`s only; every random draw
//! comes from the caller's [`Rng`] streams.

use std::collections::{BTreeMap, BTreeSet};

use crate::rng::Rng;

/// The simulated sector size for tearing (card 1.1).
pub const SECTOR: u64 = 512;

/// The page-file path (in `/`).
pub const PAGE_FILE: &str = "/pages";
/// The WAL directory.
pub const WAL_DIR: &str = "/wal/";
/// The root directory.
pub const ROOT_DIR: &str = "/";

/// A file handle (index into the inode table).
pub type FileId = usize;

/// One volatile write since the last covering sync, with the pre-image
/// captured at issue time (the LOST fault's revert target).
#[derive(Debug, Clone)]
pub struct VWrite {
    /// File offset.
    pub off: u64,
    /// The bytes written.
    pub bytes: Vec<u8>,
    /// The content of the range before this write was issued.
    pub pre: Vec<u8>,
}

/// A write that has become durable (its covering sync completed). Media
/// faults target these.
#[derive(Debug, Clone)]
pub struct DWrite {
    /// File offset.
    pub off: u64,
    /// The bytes that became durable.
    pub bytes: Vec<u8>,
    /// The content the range had before the write was issued (LOST target).
    pub pre: Vec<u8>,
    /// Which inode (file) the write belongs to.
    pub inode: FileId,
    /// Whether the graduating sync was a `sync_all` (card X-5b Work 13:
    /// X5-R5's `meta_rollback` and the set `R` key on "a completed
    /// `sync_all` covering a meta write"; the durable-write list itself
    /// already restricts membership to writes a completed sync covered).
    pub all: bool,
}

/// A recorded read (for the accept hook and the twin's read sets).
#[derive(Debug, Clone)]
pub struct ReadRec {
    /// File read.
    pub inode: FileId,
    /// Offset read.
    pub off: u64,
    /// Length read.
    pub len: u64,
    /// Bytes returned differ from the last issued bytes somewhere in the
    /// range (the taint flag, 1.4).
    pub tainted: bool,
    /// The tainted sub-ranges of this read, as absolute file offsets
    /// (1.4). The accept hook judges the **accepted sub-range** against
    /// the most recent covering read, so the whole-read flag alone is
    /// not enough (gate REWORK item 2).
    pub tainted_ranges: Vec<(u64, u64)>,
}

/// A volatile directory operation in issue order.
#[derive(Debug, Clone)]
pub enum DirOp {
    /// Create a file by name in the directory.
    Create(String, FileId),
    /// Rename within the directory.
    Rename(String, String, FileId),
    /// Unlink by name.
    Unlink(String, FileId),
}

/// The media fault kinds (card 1.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MediaKind {
    /// The range reverts to its content before that write.
    Lost,
    /// As LOST, and the written bytes land on another slot (page file) or
    /// another 512-aligned offset of the same `/wal/` segment (WAL).
    Misdirected {
        /// Target offset of the stray bytes.
        to_off: u64,
    },
    /// A random non-empty strict subset of the write's sectors reverts.
    Torn {
        /// The sectors (start offsets) that revert.
        sectors: Vec<u64>,
    },
}

/// One applied media fault, with its target (for O-3 (c)/(e) and quotas).
#[derive(Debug, Clone)]
pub struct MediaFault {
    /// File the faulted write belongs to.
    pub inode: FileId,
    /// Offset of the faulted write.
    pub off: u64,
    /// Length of the faulted write.
    pub len: u64,
    /// The bytes of the faulted write (the MISDIRECTED stray payload).
    pub bytes: Vec<u8>,
    /// The pre-image (the LOST/TORN revert content).
    pub pre: Vec<u8>,
    /// The fault.
    pub kind: MediaKind,
}

/// The per-seed fault knobs (1.3): values drawn from {0.1, 0.3, 0.5, 0.7,
/// 0.9}, stored in permille.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FaultKnobs {
    /// Probability a touched sector takes its latest volatile write's
    /// value (and that a directory's volatile op prefix is complete).
    pub keep_latest_pm: u32,
    /// Probability, given not-latest, of a torn choice (some earlier
    /// prefix) rather than the durable value.
    pub tear_pm: u32,
}

/// The fault class of a seed (card 5.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultClass {
    /// Crash faults only.
    C,
    /// F-CRASH plus F-MEDIA-PAGE.
    Mp,
    /// F-CRASH plus F-MEDIA-WAL.
    Mw,
}

/// Disk events, observed by the harness-side model and monitors (card
/// 3.10: they read only SimDisk state and parsed WAL bytes, never store
/// memory).
#[derive(Debug, Clone)]
pub enum DiskEvent {
    /// A write was issued (bytes copied for the observers).
    WriteIssued {
        /// File.
        inode: FileId,
        /// Offset.
        off: u64,
        /// The bytes.
        bytes: Vec<u8>,
    },
    /// A write call returned.
    WriteReturned {
        /// File.
        inode: FileId,
        /// Offset.
        off: u64,
        /// Length.
        len: u64,
    },
    /// A sync was issued.
    SyncIssued {
        /// File.
        inode: FileId,
        /// Whether it covers metadata (`sync_all`).
        all: bool,
    },
    /// A sync returned.
    SyncReturned {
        /// File.
        inode: FileId,
        /// Whether it covers metadata (`sync_all`).
        all: bool,
    },
    /// A directory sync returned.
    SyncDirReturned {
        /// The directory.
        dir: String,
    },
    /// A directory operation was issued.
    DirOpIssued {
        /// The directory.
        dir: String,
        /// A stable description (for observers and diagnosis).
        what: String,
    },
    /// An accept hook fired (card X-5b Work 13, the R-taint's history
    /// query: the ruling's window — "from open until the store next
    /// writes it" — needs accepts **interleaved with writes in order**,
    /// which `SimDisk::accepts` alone cannot give).
    Accepted {
        /// File.
        inode: FileId,
        /// Offset.
        off: u64,
        /// Length.
        len: u64,
    },
    /// A crash was resolved (F-CRASH): a life boundary. The R-taint's
    /// window restarts at each new life's open — a slot a crashed life
    /// had written can be re-accepted stale by the next life.
    Crash,
}

/// One file's state.
#[derive(Debug, Default, Clone)]
struct Inode {
    /// Current bytes (the volatile-inclusive view). Length tracks the
    /// current length; writes beyond it extend it.
    cur: Vec<u8>,
    /// Durable bytes (the durable data extent).
    durable: Vec<u8>,
    /// Durable length: reading at `[durable.len(), durable_len)` yields
    /// zeros (preallocated-but-unwritten tails).
    durable_len: u64,
    /// Volatile writes since the last covering sync, issue order.
    volatile: Vec<VWrite>,
    /// Durable writes (media-fault candidates), issue order, kept while
    /// the file exists.
    dwrites: Vec<DWrite>,
    /// The taint reference: last issued bytes per range, as an interval
    /// map (start → bytes). Kept across lives; never trimmed by crashes.
    issued: BTreeMap<u64, Vec<u8>>,
}

impl Inode {
    fn read_cur(&self, off: u64, len: u64) -> Vec<u8> {
        let mut out = vec![0u8; len as usize];
        let start = (off as usize).min(self.cur.len());
        let end = ((off + len) as usize).min(self.cur.len());
        if start < end {
            let src = self.cur.get(start..end).unwrap_or(&[]);
            let dst = out.get_mut(0..end - start).unwrap_or(&mut []);
            dst.copy_from_slice(src);
        }
        out
    }

    /// The tainted sub-ranges of `[off, off+len)` against the bytes the
    /// read actually returned (`view`): the intervals where the two
    /// differ (1.4). An issued interval that starts **before** `off` and
    /// extends into the range is included — iterating `issued` from
    /// `off` skips it, which is the gate REWORK item 1 bug: every read
    /// that begins mid-batch (a WAL read starting inside a previously
    /// issued 16 KiB batch) was blind to taint, and so was
    /// [`SimDisk::range_tainted`].
    fn taint_ranges(&self, off: u64, len: u64, view: &[u8]) -> Vec<(u64, u64)> {
        let end = off + len;
        let mut out = Vec::new();
        // Issued intervals are disjoint (`record_issued` splits the
        // neighbours of every insert), so the only interval that can
        // straddle `off` is the last one starting at or before it.
        let mut first = off;
        if let Some((&kstart, kbytes)) = self.issued.range(..=off).next_back()
            && kstart + kbytes.len() as u64 > off
        {
            first = kstart;
        }
        for (&fstart, fbytes) in self.issued.range(first..end) {
            let fend = fstart + fbytes.len() as u64;
            let s = off.max(fstart);
            let e = end.min(fend);
            if s >= e {
                continue;
            }
            let fseg = fbytes.get((s - fstart) as usize..(e - fstart) as usize);
            let vseg = view.get((s - off) as usize..(e - off) as usize);
            if let (Some(f), Some(v)) = (fseg, vseg)
                && f != v
            {
                out.push((s, e));
            }
        }
        out
    }

    /// Whether any part of the range is tainted (1.4).
    fn taint_of(&self, off: u64, len: u64, view: &[u8]) -> bool {
        !self.taint_ranges(off, len, view).is_empty()
    }

    fn record_issued(&mut self, off: u64, bytes: &[u8]) {
        let end = off + bytes.len() as u64;
        let mut to_remove: Vec<u64> = Vec::new();
        let mut to_add: Vec<(u64, Vec<u8>)> = Vec::new();
        for (&fstart, fbytes) in self.issued.range(..end) {
            let fend = fstart + fbytes.len() as u64;
            if fend <= off {
                continue;
            }
            if fstart < off {
                let cut = (off - fstart) as usize;
                if let Some(pre) = fbytes.get(..cut) {
                    to_add.push((fstart, pre.to_vec()));
                }
            }
            if fend > end {
                let cut = (end - fstart) as usize;
                if let Some(post) = fbytes.get(cut..) {
                    to_add.push((end, post.to_vec()));
                }
            }
            to_remove.push(fstart);
        }
        for k in to_remove {
            self.issued.remove(&k);
        }
        for (k, v) in to_add {
            self.issued.insert(k, v);
        }
        self.issued.insert(off, bytes.to_vec());
    }

    /// The durable bytes of a range (the observer's taint view).
    fn durable_view(&self, off: u64, len: u64) -> Vec<u8> {
        let mut out = vec![0u8; len as usize];
        let start = (off as usize).min(self.durable.len());
        let end = ((off + len) as usize).min(self.durable.len());
        if start < end {
            let src = self.durable.get(start..end).unwrap_or(&[]);
            let dst = out.get_mut(0..end - start).unwrap_or(&mut []);
            dst.copy_from_slice(src);
        }
        out
    }
}

/// The simulated disk. All namespace iteration is over `BTreeMap`s.
/// `Clone` snapshots the whole disk for the harness's pass re-runs, twin
/// runs and deterministic fault-plan retries (card X-5b Work 13).
#[derive(Debug, Default, Clone)]
pub struct SimDisk {
    inodes: Vec<Inode>,
    cur_bindings: BTreeMap<String, BTreeMap<String, FileId>>,
    durable_bindings: BTreeMap<String, BTreeMap<String, FileId>>,
    volatile_ops: BTreeMap<String, Vec<DirOp>>,
    /// Reads this round (for the accept hook and the twin read sets).
    pub reads: Vec<ReadRec>,
    /// `accepted` calls this round, for the gate review of accept sites.
    pub accepts: Vec<(FileId, u64, u64)>,
    /// Served corruptions this round: accepted ranges tainted at read time
    /// (O-3 (a)).
    pub served: Vec<(FileId, u64, u64)>,
    /// Accepts whose range (partly) no read of this round covered (gate
    /// REWORK round 2, item B): the store accepted bytes it never read —
    /// a harness or store defect. Each is also recorded in `served`, so
    /// O-3 (a) fails the round; this list exists for diagnosis.
    pub uncovered_accepts: Vec<(FileId, u64, u64)>,
    /// Media faults applied so far (all rounds).
    pub media: Vec<MediaFault>,
    /// Disk events this round (for the model and the monitors).
    pub events: Vec<DiskEvent>,
    /// Crashes resolved so far.
    pub lives: u32,
}

impl SimDisk {
    /// An empty disk with the two directories present.
    pub fn new() -> Self {
        let mut d = Self::default();
        d.cur_bindings.insert(ROOT_DIR.to_owned(), BTreeMap::new());
        d.cur_bindings.insert(WAL_DIR.to_owned(), BTreeMap::new());
        d.durable_bindings = d.cur_bindings.clone();
        d
    }

    /// Create a file (volatile until `sync_dir` of its directory). Fails if
    /// the name exists in the current view.
    pub fn create(&mut self, path: &str) -> Result<FileId, String> {
        let (dir, name) = split_path(path)?;
        if self.lookup(path).is_some() {
            return Err(format!("create: {path} exists"));
        }
        let id = self.inodes.len();
        self.inodes.push(Inode::default());
        self.volatile_ops
            .entry(dir.clone())
            .or_default()
            .push(DirOp::Create(name.clone(), id));
        self.bind(&dir, &name, id);
        self.events.push(DiskEvent::DirOpIssued {
            dir,
            what: format!("create {path}"),
        });
        Ok(id)
    }

    /// Open (look up) a file in the current view.
    pub fn open(&mut self, path: &str) -> Result<FileId, String> {
        self.lookup(path)
            .ok_or_else(|| format!("open: {path} does not exist"))
    }

    /// Rename a file (volatile until the directory's `sync_dir`).
    pub fn rename(&mut self, from: &str, to: &str) -> Result<(), String> {
        let (fdir, fname) = split_path(from)?;
        let (tdir, tname) = split_path(to)?;
        if fdir != tdir {
            return Err("rename across directories".to_owned());
        }
        let id = self
            .lookup(from)
            .ok_or_else(|| format!("rename: {from} missing"))?;
        if self.lookup(to).is_some() {
            return Err(format!("rename: {to} exists"));
        }
        self.volatile_ops
            .entry(fdir.clone())
            .or_default()
            .push(DirOp::Rename(fname.clone(), tname.clone(), id));
        self.unbind(&fdir, &fname);
        self.bind(&tdir, &tname, id);
        self.events.push(DiskEvent::DirOpIssued {
            dir: fdir,
            what: format!("rename {from} -> {to}"),
        });
        Ok(())
    }

    /// Unlink a file (volatile until the directory's `sync_dir`).
    pub fn unlink(&mut self, path: &str) -> Result<(), String> {
        let (dir, name) = split_path(path)?;
        let id = self
            .lookup(path)
            .ok_or_else(|| format!("unlink: {path} missing"))?;
        self.volatile_ops
            .entry(dir.clone())
            .or_default()
            .push(DirOp::Unlink(name.clone(), id));
        self.unbind(&dir, &name);
        self.events.push(DiskEvent::DirOpIssued {
            dir,
            what: format!("unlink {path}"),
        });
        Ok(())
    }

    /// `sync_dir`: the directory's current bindings become durable and its
    /// volatile op list clears.
    pub fn sync_dir(&mut self, dir: &str) -> Result<(), String> {
        let cur = self
            .cur_bindings
            .get(dir)
            .ok_or_else(|| format!("sync_dir: no dir {dir}"))?
            .clone();
        self.durable_bindings.insert(dir.to_owned(), cur);
        self.volatile_ops.insert(dir.to_owned(), Vec::new());
        self.events.push(DiskEvent::SyncDirReturned {
            dir: dir.to_owned(),
        });
        Ok(())
    }

    fn lookup(&self, path: &str) -> Option<FileId> {
        let (dir, name) = split_path(path).ok()?;
        self.cur_bindings.get(&dir)?.get(&name).copied()
    }

    fn bind(&mut self, dir: &str, name: &str, id: FileId) {
        self.cur_bindings
            .entry(dir.to_owned())
            .or_default()
            .insert(name.to_owned(), id);
    }

    fn unbind(&mut self, dir: &str, name: &str) {
        if let Some(m) = self.cur_bindings.get_mut(dir) {
            m.remove(name);
        }
    }

    /// Read the current view. Records the read with its taint flag and
    /// its tainted sub-ranges.
    pub fn read_at(&mut self, id: FileId, off: u64, len: u64) -> Result<Vec<u8>, String> {
        let inode = self.inodes.get_mut(id).ok_or("bad file id")?;
        let view = inode.read_cur(off, len);
        let tainted_ranges = inode.taint_ranges(off, len, &view);
        let tainted = !tainted_ranges.is_empty();
        self.reads.push(ReadRec {
            inode: id,
            off,
            len,
            tainted,
            tainted_ranges,
        });
        Ok(view)
    }

    /// Write bytes at an offset (a volatile write). Beyond-length writes
    /// extend the current length.
    pub fn write_at(&mut self, id: FileId, off: u64, bytes: &[u8]) -> Result<(), String> {
        let inode = self.inodes.get_mut(id).ok_or("bad file id")?;
        let end = off + bytes.len() as u64;
        if end > inode.cur.len() as u64 {
            inode.cur.resize(end as usize, 0);
        }
        let pre = inode.read_cur(off, bytes.len() as u64);
        if let Some(dst) = inode.cur.get_mut(off as usize..end as usize) {
            dst.copy_from_slice(bytes);
        }
        inode.record_issued(off, bytes);
        inode.volatile.push(VWrite {
            off,
            bytes: bytes.to_vec(),
            pre,
        });
        self.events.push(DiskEvent::WriteIssued {
            inode: id,
            off,
            bytes: bytes.to_vec(),
        });
        self.events.push(DiskEvent::WriteReturned {
            inode: id,
            off,
            len: bytes.len() as u64,
        });
        Ok(())
    }

    /// Change the current length (metadata; volatile until a sync).
    pub fn set_len(&mut self, id: FileId, n: u64) -> Result<(), String> {
        let inode = self.inodes.get_mut(id).ok_or("bad file id")?;
        if (n as usize) <= inode.cur.len() {
            inode.cur.truncate(n as usize);
        } else {
            inode.cur.resize(n as usize, 0);
        }
        Ok(())
    }

    /// `sync_data`: the file's data becomes durable, plus the length growth
    /// needed to read it back. Volatile writes graduate to durable writes
    /// (media-fault candidates).
    pub fn sync_data(&mut self, id: FileId) -> Result<(), String> {
        self.events.push(DiskEvent::SyncIssued {
            inode: id,
            all: false,
        });
        let inode = self.inodes.get_mut(id).ok_or("bad file id")?;
        let maxend = inode
            .volatile
            .iter()
            .map(|w| w.off + w.bytes.len() as u64)
            .max();
        let ws = std::mem::take(&mut inode.volatile);
        for w in ws {
            graduate(inode, &w, id, false);
        }
        if let Some(e) = maxend {
            let needed = e.min(inode.cur.len() as u64);
            if needed > inode.durable_len {
                inode.durable_len = needed;
            }
        }
        self.events.push(DiskEvent::SyncReturned {
            inode: id,
            all: false,
        });
        Ok(())
    }

    /// `sync_all`: data, length and metadata become durable.
    pub fn sync_all(&mut self, id: FileId) -> Result<(), String> {
        self.events.push(DiskEvent::SyncIssued {
            inode: id,
            all: true,
        });
        let inode = self.inodes.get_mut(id).ok_or("bad file id")?;
        let cur_len = inode.cur.len() as u64;
        let ws = std::mem::take(&mut inode.volatile);
        for w in ws {
            graduate(inode, &w, id, true);
        }
        if cur_len > inode.durable.len() as u64 {
            inode.durable.resize(cur_len as usize, 0);
        }
        inode.durable_len = cur_len;
        self.events.push(DiskEvent::SyncReturned {
            inode: id,
            all: true,
        });
        Ok(())
    }

    /// The accept hook (card 1.4). The store calls this exactly where it
    /// accepts bytes as valid — a page passing §3.3, the meta chosen at
    /// open, page 0, a WAL record the reader yields. A served corruption
    /// is recorded when the accepted range was tainted at read time,
    /// judged **per accepted byte, against the most recent read that
    /// covers that byte** (gate REWORK round 2, item B): the store reads
    /// in batches (e.g. 16 KiB of WAL) and accepts one record inside, so
    /// matching only a read of exactly `(off, len)` could never fire,
    /// scanning every earlier read would judge stale reads (gate REWORK
    /// item 2), and a single covering read is blind to a record
    /// reassembled across two batch reads (the round-2 gate probe: two
    /// tainted reads `[0,4096)` and `[4096,8192)`, `accepted(f, 4000,
    /// 200)`). Accepted bytes **no read of this round covered** were
    /// accepted unread — a harness or store defect — and fail closed:
    /// recorded in `served` (so O-3 (a) fails the round whatever the
    /// harness does) and in `uncovered_accepts` (for diagnosis).
    pub fn accepted(&mut self, id: FileId, off: u64, len: u64) {
        self.accepts.push((id, off, len));
        self.events.push(DiskEvent::Accepted {
            inode: id,
            off,
            len,
        });
        if len == 0 {
            // Zero bytes accepted: nothing to judge, nothing uncovered.
            return;
        }
        let end = off + len;
        // Walk the reads most-recent first: the first read to cover a
        // byte is the most recent one that does, so each accepted byte is
        // judged exactly once, by its own covering read. `uncovered`
        // holds the sub-ranges not yet judged; what survives every read
        // was never read.
        let mut uncovered: Vec<(u64, u64)> = vec![(off, end)];
        let mut tainted = false;
        for r in self.reads.iter().rev() {
            if uncovered.is_empty() {
                break;
            }
            if r.inode != id {
                continue;
            }
            let rend = r.off + r.len;
            let mut next: Vec<(u64, u64)> = Vec::new();
            for &(s, e) in uncovered.iter() {
                let lo = s.max(r.off);
                let hi = e.min(rend);
                if lo < hi {
                    // This read is the most recent one covering
                    // `[lo, hi)`: judge it, at sub-range granularity.
                    if r.tainted_ranges.iter().any(|&(ts, te)| ts < hi && te > lo) {
                        tainted = true;
                    }
                    // Parts of the interval outside this read stay for
                    // earlier reads.
                    if s < r.off {
                        next.push((s, r.off));
                    }
                    if e > rend {
                        next.push((rend, e));
                    }
                } else {
                    next.push((s, e));
                }
            }
            uncovered = next;
        }
        if !uncovered.is_empty() {
            self.uncovered_accepts.push((id, off, len));
            tainted = true;
        }
        if tainted {
            self.served.push((id, off, len));
        }
    }

    /// Apply F-CRASH: resolve every file's volatile writes per sector and
    /// every directory's volatile ops as a prefix (card 1.3). Clears
    /// volatile state; keeps taint references, durable-write history, read
    /// and accept logs (1.4: "in any life of this seed"). Returns the
    /// number of dropped directory ops.
    pub fn apply_crash(&mut self, rng: &mut Rng, knobs: &FaultKnobs) -> u64 {
        self.events.push(DiskEvent::Crash);
        let mut dropped_ops = 0u64;
        for inode in self.inodes.iter_mut() {
            if !inode.volatile.is_empty() {
                let mut sectors: Vec<u64> = Vec::new();
                for w in inode.volatile.iter() {
                    let s0 = w.off / SECTOR;
                    let s1 = (w.off + w.bytes.len() as u64).div_ceil(SECTOR);
                    for s in s0..s1 {
                        sectors.push(s * SECTOR);
                    }
                }
                sectors.sort_unstable();
                sectors.dedup();
                let mut surviving_end = inode.durable_len;
                for &sec in sectors.iter() {
                    // History: [durable image, image after each write].
                    let mut images: Vec<Vec<u8>> = Vec::new();
                    let mut img = sector_image(&inode.durable, sec);
                    images.push(img.clone());
                    for w in inode.volatile.iter() {
                        apply_write_to(&mut img, sec, w);
                        images.push(img.clone());
                    }
                    let n = images.len() - 1;
                    let keep = rng.below(1000) < knobs.keep_latest_pm as u64;
                    let idx = if keep {
                        n
                    } else if rng.below(1000) < knobs.tear_pm as u64 {
                        rng.below(n as u64) as usize
                    } else {
                        0
                    };
                    let chosen = images.get(idx).cloned().unwrap_or_default();
                    write_durable(inode, sec, &chosen);
                    if idx > 0 {
                        surviving_end = surviving_end.max(sec + SECTOR);
                    }
                }
                if surviving_end > inode.durable_len {
                    inode.durable_len = surviving_end;
                }
                inode.volatile.clear();
            }
            // The current view collapses to the durable state; a volatile
            // length change persists only as far as the surviving data.
            // Gate REWORK item 6a: a **synced** shrink must survive the
            // crash too — `durable` is truncated to `durable_len`, so
            // the stale bytes behind the smaller length are not readable
            // afterwards (1.1: the file's durable length is the extent).
            let dlen = inode.durable_len.min(inode.durable.len() as u64) as usize;
            inode.durable.truncate(dlen);
            inode.cur = inode.durable.clone();
            if (inode.durable_len as usize) > inode.cur.len() {
                inode.cur.resize(inode.durable_len as usize, 0);
            }
        }
        // Directory prefixes.
        let dirs: Vec<String> = self.volatile_ops.keys().cloned().collect();
        for dir in dirs {
            let ops = self.volatile_ops.get(&dir).cloned().unwrap_or_default();
            if ops.is_empty() {
                continue;
            }
            let keep = rng.below(1000) < knobs.keep_latest_pm as u64;
            let nkeep = if keep {
                ops.len()
            } else {
                rng.below(ops.len() as u64 + 1) as usize
            };
            dropped_ops += (ops.len() - nkeep) as u64;
            let durable = self.durable_bindings.entry(dir.clone()).or_default();
            for op in ops.into_iter().take(nkeep) {
                match op {
                    DirOp::Create(name, id) => {
                        durable.insert(name, id);
                    }
                    DirOp::Rename(from, to, id) => {
                        durable.remove(&from);
                        durable.insert(to, id);
                    }
                    DirOp::Unlink(name, _) => {
                        durable.remove(&name);
                    }
                }
            }
            self.volatile_ops.insert(dir, Vec::new());
        }
        // Rebuild the current namespace from the durable bindings.
        self.cur_bindings = self.durable_bindings.clone();
        // Files no longer bound are gone; their state is discarded (their
        // taint references die with them: nothing can read them again).
        let bound: BTreeSet<FileId> = self
            .cur_bindings
            .values()
            .flat_map(|m| m.values().copied())
            .collect();
        for (i, inode) in self.inodes.iter_mut().enumerate() {
            if !bound.contains(&i) {
                *inode = Inode::default();
            }
        }
        self.lives += 1;
        dropped_ops
    }

    /// Apply F-MEDIA-PAGE (class MP) after F-CRASH at the round's crash:
    /// 1–3 durable page-file writes whose bytes are still current get
    /// LOST / MISDIRECTED / TORN. `referenced` biases ≥ 60% of faults to
    /// slots the durable image references (or meta slots — the harness
    /// includes them in the set); it is supplied by the harness walk and
    /// never feeds a check.
    pub fn apply_media_page(
        &mut self,
        rng: &mut Rng,
        referenced: &BTreeSet<u64>,
    ) -> Vec<MediaFault> {
        let page_id = match self.lookup(PAGE_FILE) {
            Some(id) => id,
            None => return Vec::new(),
        };
        let faults = self.media_faults_common(rng, page_id, true, referenced, 1, 3);
        self.record_media(faults)
    }

    /// Apply F-MEDIA-WAL (class MW): the same, on 1–2 durable WAL writes
    /// drawn **uniformly over every segment's candidates** — the old
    /// loop faulted only the lowest-id `/wal/` file that had candidates,
    /// so in multi-segment seeds the active segment was never hit (gate
    /// REWORK item 6c). MISDIRECTED stays within `/wal/` files (the same
    /// segment; declared simplification, see README).
    pub fn apply_media_wal(&mut self, rng: &mut Rng) -> Vec<MediaFault> {
        let mut wal_ids: Vec<FileId> = self
            .cur_bindings
            .get(WAL_DIR)
            .map(|m| m.values().copied().collect())
            .unwrap_or_default();
        wal_ids.sort_unstable();
        let mut cands: Vec<(FileId, DWrite)> = Vec::new();
        for id in wal_ids {
            cands.extend(
                self.durable_candidates(id, false)
                    .into_iter()
                    .map(|w| (id, w)),
            );
        }
        if cands.is_empty() {
            return Vec::new();
        }
        // 1–2 distinct durable WAL writes, uniform over the pool.
        let want = 1 + rng.below(2);
        let mut faults: Vec<MediaFault> = Vec::new();
        for _ in 0..want {
            if cands.is_empty() {
                break;
            }
            let idx = rng.below(cands.len() as u64) as usize;
            let (id, w) = cands.remove(idx);
            let kind = self.draw_media_kind(rng, id, &w, false);
            faults.push(MediaFault {
                inode: id,
                off: w.off,
                len: w.bytes.len() as u64,
                bytes: w.bytes.clone(),
                pre: w.pre.clone(),
                kind,
            });
        }
        self.record_media(faults)
    }

    /// The durable writes of `inode` whose bytes are still current
    /// (media-fault candidates), in issue order. Page faults target only
    /// slot-aligned whole-page writes.
    fn durable_candidates(&self, inode: FileId, page_file: bool) -> Vec<DWrite> {
        let mut cands = Vec::new();
        let Some(i) = self.inodes.get(inode) else {
            return cands;
        };
        for w in i.dwrites.iter() {
            if page_file && (w.off % 4096 != 0 || w.bytes.len() != 4096) {
                continue;
            }
            if i.durable
                .get(w.off as usize..w.off as usize + w.bytes.len())
                == Some(w.bytes.as_slice())
            {
                cands.push(w.clone());
            }
        }
        cands
    }

    /// Draw a fault kind for one candidate write (card 1.3): LOST, or
    /// MISDIRECTED to another page slot (page file) or another
    /// 512-aligned offset of the same segment (WAL), or TORN over a
    /// random non-empty strict subset of the write's sectors.
    fn draw_media_kind(
        &self,
        rng: &mut Rng,
        inode: FileId,
        w: &DWrite,
        page_file: bool,
    ) -> MediaKind {
        match rng.below(3) {
            0 => MediaKind::Lost,
            1 => {
                let target_off = if page_file {
                    let slots = self
                        .inodes
                        .get(inode)
                        .map(|i| i.durable_len / 4096)
                        .unwrap_or(3);
                    let t = if rng.below(10) == 0 {
                        // A meta slot (1–2), with probability 0.1.
                        1 + rng.below(2)
                    } else {
                        let hi = slots.max(3);
                        1 + rng.below(hi - 1)
                    };
                    let mut t = t;
                    if (t * 4096) == w.off {
                        t += 1;
                        if t >= slots.max(3) {
                            t = 1;
                        }
                    }
                    t * 4096
                } else {
                    // WAL: a 512-aligned offset inside this segment.
                    let len = w.bytes.len() as u64;
                    let dlen = self.inodes.get(inode).map(|i| i.durable_len).unwrap_or(0);
                    if dlen > len {
                        rng.below((dlen - len) / SECTOR + 1) * SECTOR
                    } else {
                        0
                    }
                };
                MediaKind::Misdirected { to_off: target_off }
            }
            _ => {
                let s0 = w.off / SECTOR;
                let s1 = (w.off + w.bytes.len() as u64).div_ceil(SECTOR);
                let total = (s1 - s0) as usize;
                if total < 2 {
                    MediaKind::Lost
                } else {
                    let mut set: Vec<u64> = Vec::new();
                    for _tries in 0..64 {
                        set.clear();
                        for s in s0..s1 {
                            if rng.coin() {
                                set.push(s * SECTOR);
                            }
                        }
                        if !set.is_empty() && set.len() < total {
                            break;
                        }
                    }
                    if set.is_empty() || set.len() >= total {
                        set = vec![s0 * SECTOR];
                    }
                    MediaKind::Torn { sectors: set }
                }
            }
        }
    }

    fn media_faults_common(
        &mut self,
        rng: &mut Rng,
        inode: FileId,
        page_file: bool,
        referenced: &BTreeSet<u64>,
        nmin: u64,
        nmax: u64,
    ) -> Vec<MediaFault> {
        let want = nmin + rng.below(nmax - nmin + 1);
        let cands = self.durable_candidates(inode, page_file);
        if cands.is_empty() {
            return Vec::new();
        }
        let mut faults = Vec::new();
        for _ in 0..want {
            let idx = if page_file && rng.below(10) < 6 {
                let pref: Vec<usize> = cands
                    .iter()
                    .enumerate()
                    .filter(|(_, w)| referenced.contains(&(w.off / 4096)))
                    .map(|(i, _)| i)
                    .collect();
                if pref.is_empty() {
                    rng.below(cands.len() as u64) as usize
                } else {
                    // `pref` is non-empty here; the fallback is unreachable
                    // and deterministic anyway.
                    let i = rng.below(pref.len() as u64) as usize;
                    pref.get(i).copied().unwrap_or(0)
                }
            } else {
                rng.below(cands.len() as u64) as usize
            };
            let Some(w) = cands.get(idx).cloned() else {
                break;
            };
            let kind = self.draw_media_kind(rng, inode, &w, page_file);
            faults.push(MediaFault {
                inode,
                off: w.off,
                len: w.bytes.len() as u64,
                bytes: w.bytes.clone(),
                pre: w.pre.clone(),
                kind,
            });
        }
        faults
    }

    fn record_media(&mut self, faults: Vec<MediaFault>) -> Vec<MediaFault> {
        for f in faults.iter() {
            let Some(inode) = self.inodes.get_mut(f.inode) else {
                continue;
            };
            let end = f.off + f.len;
            match &f.kind {
                MediaKind::Lost => {
                    write_both(inode, f.off, &f.pre);
                }
                MediaKind::Misdirected { to_off } => {
                    write_both(inode, f.off, &f.pre);
                    write_both(inode, *to_off, &f.bytes);
                }
                MediaKind::Torn { sectors } => {
                    for &sec in sectors {
                        let s0 = sec.max(f.off);
                        let s1 = (sec + SECTOR).min(end);
                        if s1 > s0 {
                            let seg = f
                                .pre
                                .get((s0 - f.off) as usize..(s1 - f.off) as usize)
                                .map(|s| s.to_vec())
                                .unwrap_or_default();
                            write_both(inode, s0, &seg);
                        }
                    }
                }
            }
        }
        let out = faults.clone();
        self.media.extend(faults);
        out
    }

    // ---- observer accessors (harness and monitors only) ----

    /// The durable writes of a file, in issue order, across all lives of
    /// the seed (harness/monitor history query, card X-5b Work 13: X5-R5's
    /// `meta_rollback`, the set `R` and M-NOINPLACE read this; the store
    /// never may).
    pub fn dwrites_of(&self, id: FileId) -> &[DWrite] {
        match self.inodes.get(id) {
            Some(i) => i.dwrites.as_slice(),
            None => &[],
        }
    }

    /// Apply one specific media fault (card X-5b Work 6/D17, D10b: the
    /// directed tests fault named targets deterministically). The fault
    /// must describe a durable write (the caller builds it from
    /// [`SimDisk::dwrites_of`]); the effect is exactly what the drawn
    /// faults of `apply_media_page`/`apply_media_wal` do.
    pub fn apply_one_media_fault(&mut self, f: &MediaFault) {
        self.record_media(vec![f.clone()]);
    }

    /// The durable bytes of a range (observer view; no taint recording).
    pub fn durable_read(&self, id: FileId, off: u64, len: u64) -> Vec<u8> {
        let mut out = vec![0u8; len as usize];
        if let Some(i) = self.inodes.get(id) {
            let start = (off as usize).min(i.durable.len());
            let end = ((off + len) as usize).min(i.durable.len());
            if start < end {
                let src = i.durable.get(start..end).unwrap_or(&[]);
                let dst = out.get_mut(0..end - start).unwrap_or(&mut []);
                dst.copy_from_slice(src);
            }
        }
        out
    }

    /// The durable length of a file.
    pub fn durable_len(&self, id: FileId) -> u64 {
        self.inodes.get(id).map(|i| i.durable_len).unwrap_or(0)
    }

    /// Current bindings of a directory (observer view), sorted by name.
    pub fn list_dir(&self, dir: &str) -> Vec<(String, FileId)> {
        let mut out: Vec<(String, FileId)> = self
            .cur_bindings
            .get(dir)
            .map(|m| m.iter().map(|(k, v)| (k.clone(), *v)).collect())
            .unwrap_or_default();
        out.sort_unstable();
        out
    }

    /// Look up a file id by path (observer view).
    pub fn stat(&self, path: &str) -> Option<FileId> {
        self.lookup(path)
    }

    /// Begin a new round: clear the per-round logs (reads, accepts,
    /// served-corruption findings, uncovered accepts, events). Taint
    /// references and durable-write history survive, per 1.4 ("in any
    /// life of this seed").
    pub fn new_round(&mut self) {
        self.reads.clear();
        self.accepts.clear();
        self.served.clear();
        self.uncovered_accepts.clear();
        self.events.clear();
    }

    /// Observer (harness/monitors only): is the durable range tainted
    /// (different from the last issued bytes)? Used for O-3 (e)'s tainted
    /// referenced slots.
    pub fn range_tainted(&self, id: FileId, off: u64, len: u64) -> bool {
        match self.inodes.get(id) {
            Some(i) => i.taint_of(off, len, &i.durable_view(off, len)),
            None => false,
        }
    }
}

/// Move a volatile write into the durable bytes and the durable-write log.
fn graduate(inode: &mut Inode, w: &VWrite, id: FileId, all: bool) {
    let end = w.off + w.bytes.len() as u64;
    if end > inode.durable.len() as u64 {
        inode.durable.resize(end as usize, 0);
    }
    if let Some(dst) = inode.durable.get_mut(w.off as usize..end as usize) {
        dst.copy_from_slice(&w.bytes);
    }
    inode.dwrites.push(DWrite {
        off: w.off,
        bytes: w.bytes.clone(),
        pre: w.pre.clone(),
        inode: id,
        all,
    });
}

fn write_both(inode: &mut Inode, off: u64, bytes: &[u8]) {
    let end = off + bytes.len() as u64;
    if end > inode.durable.len() as u64 {
        inode.durable.resize(end as usize, 0);
    }
    if let Some(dst) = inode.durable.get_mut(off as usize..end as usize) {
        dst.copy_from_slice(bytes);
    }
    if end > inode.cur.len() as u64 {
        inode.cur.resize(end as usize, 0);
    }
    if let Some(dst) = inode.cur.get_mut(off as usize..end as usize) {
        dst.copy_from_slice(bytes);
    }
    if end > inode.durable_len {
        inode.durable_len = end;
    }
}

fn write_durable(inode: &mut Inode, sec: u64, img: &[u8]) {
    let end = sec + img.len() as u64;
    if end > inode.durable.len() as u64 {
        inode.durable.resize(end as usize, 0);
    }
    if let Some(dst) = inode.durable.get_mut(sec as usize..end as usize) {
        dst.copy_from_slice(img);
    }
}

fn sector_image(durable: &[u8], sec: u64) -> Vec<u8> {
    let mut img = vec![0u8; SECTOR as usize];
    let start = sec as usize;
    let end = start + SECTOR as usize;
    if start < durable.len() {
        let e = end.min(durable.len());
        let src = durable.get(start..e).unwrap_or(&[]);
        let dst = img.get_mut(0..e - start).unwrap_or(&mut []);
        dst.copy_from_slice(src);
    }
    img
}

fn apply_write_to(img: &mut [u8], sec: u64, w: &VWrite) {
    let wend = w.off + w.bytes.len() as u64;
    let s = w.off.max(sec);
    let e = wend.min(sec + SECTOR);
    if e <= s {
        return;
    }
    let src = w.bytes.get((s - w.off) as usize..(e - w.off) as usize);
    let dst = img.get_mut((s - sec) as usize..(e - sec) as usize);
    if let (Some(s), Some(d)) = (src, dst) {
        d.copy_from_slice(s);
    }
}

fn split_path(path: &str) -> Result<(String, String), String> {
    if let Some(name) = path.strip_prefix(WAL_DIR) {
        Ok((WAL_DIR.to_owned(), name.to_owned()))
    } else if let Some(name) = path.strip_prefix('/') {
        Ok((ROOT_DIR.to_owned(), name.to_owned()))
    } else {
        Err(format!("bad path {path}"))
    }
}
