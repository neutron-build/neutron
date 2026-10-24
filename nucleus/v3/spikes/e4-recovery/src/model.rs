//! The reference model (card X-5 Work 2.1) and the oracle's sampling and
//! bound rules (Work 2.2).
//!
//! The workload records every committed transaction of the seed's life in
//! ts order — `(ts, mode, ops, written, acked)` — plus every bulk load
//! (unlogged entries of an unpublished tree at its `S_L`).
//!
//! - `written`: the `E4Env` write carrying the **last byte** of the
//!   transaction's `Commit` or `CommitMarker` record returned before the
//!   crash. Set by the harness from the disk's write events.
//! - `acked`: for SyncOn, a WAL sync covering that byte returned; for
//!   SyncOff, the write returned. Acked is the durability point; it does
//!   not wait for the visibility wait (§17), so the oracle's lower bound is
//!   the strongest possible.
//!
//! The catalog is tree id 1, an ordinary versioned tree. A user tree is
//! **published at c** iff the model's catalog read at `c` holds
//! `Put(1, be64(id), "public")`. A drop is `Delete(1, be64(id))` plus
//! `DropTree{id}` in one commit.
//!
//! Rounds: after round r recovers at `c_r`, the model is truncated to
//! `c_r` — commits above it were never durable, and their ops disappear.
//!
//! `model.scan(t, S)`: for each user key, the newest op with `ts ≤ S` (a
//! Delete reads as absent), plus load entries at `S_L ≤ S`.

use std::collections::{BTreeMap, BTreeSet};

/// The publish marker value in the catalog tree (`"public"`).
pub const PUBLIC: &[u8] = b"public";
/// The catalog tree id (card 2.1: "The catalog is tree id 1").
pub const CATALOG: u64 = 1;

/// One logged operation of a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// Create a tree.
    CreateTree {
        /// The tree id.
        tree: u64,
    },
    /// Drop a tree (always in the same commit as its catalog Delete).
    DropTree {
        /// The tree id.
        tree: u64,
    },
    /// Put a value under a user key.
    Put {
        /// The tree id.
        tree: u64,
        /// The user key.
        key: Vec<u8>,
        /// The value.
        value: Vec<u8>,
    },
    /// Delete a user key (a tombstone at apply time).
    Delete {
        /// The tree id.
        tree: u64,
        /// The user key.
        key: Vec<u8>,
    },
}

/// The commit mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// `synchronous_commit=on`: acked at a covering sync.
    SyncOn,
    /// `synchronous_commit=off`: acked at the write.
    SyncOff,
}

/// One committed transaction of the model.
#[derive(Debug, Clone)]
pub struct TxnRec {
    /// Its commit timestamp.
    pub ts: u64,
    /// Its durability mode.
    pub mode: Mode,
    /// Its operations.
    pub ops: Vec<Op>,
    /// The last byte of its commit record was carried by a write that
    /// returned before the crash.
    pub written: bool,
    /// Its durability point returned (see above).
    pub acked: bool,
}

/// One bulk load: unlogged entries of a tree at snapshot `S_L`.
#[derive(Debug, Clone)]
pub struct LoadRec {
    /// The tree id (unpublished until the publish commit).
    pub tree: u64,
    /// The load snapshot timestamp.
    pub s_l: u64,
    /// The loaded entries, `(user key, value)`.
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
}

/// The reference model.
#[derive(Debug, Default)]
pub struct Model {
    /// Commits by ts (ts order).
    pub commits: BTreeMap<u64, TxnRec>,
    /// Bulk loads.
    pub loads: Vec<LoadRec>,
}

/// The result of a scan: per user key, `None` = absent (tombstone or
/// never written), `Some(value)` = live.
pub type Scan = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

impl Model {
    /// An empty model.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a commit (durability flags start false; the harness sets
    /// them from disk events).
    pub fn commit(&mut self, ts: u64, mode: Mode, ops: Vec<Op>) {
        self.commits.insert(
            ts,
            TxnRec {
                ts,
                mode,
                ops,
                written: false,
                acked: false,
            },
        );
    }

    /// Mark `ts` as written (its commit record's last byte returned).
    pub fn mark_written(&mut self, ts: u64) {
        if let Some(r) = self.commits.get_mut(&ts) {
            r.written = true;
        }
    }

    /// Mark `ts` as acked (its durability point returned).
    pub fn mark_acked(&mut self, ts: u64) {
        if let Some(r) = self.commits.get_mut(&ts) {
            r.acked = true;
        }
    }

    /// Record a bulk load at `S_L`.
    pub fn load(&mut self, tree: u64, s_l: u64, entries: Vec<(Vec<u8>, Vec<u8>)>) {
        self.loads.push(LoadRec { tree, s_l, entries });
    }

    /// Truncate to cut `c` (round r recovered at `c`): commits above it
    /// were never durable; loads above it vanish with their unpublished
    /// trees.
    pub fn truncate(&mut self, c: u64) {
        self.commits.retain(|ts, _| *ts <= c);
        self.loads.retain(|l| l.s_l <= c);
    }

    /// The scan of tree `t` at snapshot `S`: per user key the newest op
    /// with `ts ≤ S` (a Delete reads as absent), plus load entries at
    /// `S_L ≤ S`.
    pub fn scan(&self, t: u64, s: u64) -> Scan {
        let mut out: Scan = BTreeMap::new();
        for rec in self.commits.values() {
            if rec.ts > s {
                continue;
            }
            for op in &rec.ops {
                match op {
                    Op::Put { tree, key, value } if *tree == t => {
                        out.insert(key.clone(), Some(value.clone()));
                    }
                    Op::Delete { tree, key } if *tree == t => {
                        out.insert(key.clone(), None);
                    }
                    _ => {}
                }
            }
        }
        for l in &self.loads {
            if l.tree == t && l.s_l <= s {
                for (k, v) in &l.entries {
                    out.insert(k.clone(), Some(v.clone()));
                }
            }
        }
        out
    }

    /// The published tree set at commit cut `c` (catalog rows holding
    /// `"public"` at `c`).
    pub fn published_at(&self, c: u64) -> BTreeSet<u64> {
        let mut out = BTreeSet::new();
        for (k, v) in self.scan(CATALOG, c) {
            if v.as_deref() == Some(PUBLIC)
                && let Some(id) = be64(&k)
            {
                out.insert(id);
            }
        }
        out
    }

    /// All commit timestamps (the oracle's candidate set `H`).
    pub fn commit_ts(&self) -> BTreeSet<u64> {
        self.commits.keys().copied().collect()
    }

    /// `lo` for a round (Work 2.2): `max(c_{r−1}, max{ts : SyncOn ∧
    /// acked})`, with `c_0 = 0`.
    pub fn lower_bound(&self, c_prev: u64) -> u64 {
        let mut lo = c_prev;
        for rec in self.commits.values() {
            if rec.mode == Mode::SyncOn && rec.acked && rec.ts > lo {
                lo = rec.ts;
            }
        }
        lo
    }

    /// `hi` for a round: `max{ts : written}`, defaulting to `lo` when
    /// nothing was written this life.
    pub fn upper_bound(&self, lo: u64) -> u64 {
        let mut hi = lo;
        for rec in self.commits.values() {
            if rec.written && rec.ts > hi {
                hi = rec.ts;
            }
        }
        hi
    }

    /// The sampling set `samp([gc_w, c])` (Work 2.4 O-1 (d)): every commit
    /// ts in `[gc_w, c]` plus `gc_w` itself; if that set has more than 64
    /// members, keep both endpoints and 62 seeded picks. `rng` is the
    /// round's sampling stream (seeded per seed and round by the harness).
    pub fn sample_points(&self, gc_w: u64, c: u64, rng: &mut crate::rng::Rng) -> Vec<u64> {
        if gc_w > c {
            return vec![gc_w];
        }
        let mut set: Vec<u64> = vec![gc_w];
        for ts in self.commits.range(gc_w..=c) {
            set.push(*ts.0);
        }
        set.sort_unstable();
        set.dedup();
        if set.len() <= 64 {
            return set;
        }
        // Keep the endpoints and 62 seeded picks from the middle.
        let mut picks: Vec<u64> = set
            .iter()
            .copied()
            .filter(|ts| *ts != gc_w && *ts != c)
            .collect();
        let mut chosen: Vec<u64> = vec![gc_w, c];
        let n = 62.min(picks.len());
        for _ in 0..n {
            let i = rng.below(picks.len() as u64) as usize;
            if let Some(v) = picks.get(i) {
                chosen.push(*v);
            }
            picks.remove(i);
        }
        chosen.sort_unstable();
        chosen.dedup();
        chosen
    }
}

/// Decode a big-endian u64 (catalog keys are `be64(id)`).
pub fn be64(k: &[u8]) -> Option<u64> {
    if k.len() != 8 {
        return None;
    }
    let mut a = [0u8; 8];
    a.copy_from_slice(k);
    Some(u64::from_be_bytes(a))
}

/// Encode a big-endian u64.
pub fn to_be64(v: u64) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}
