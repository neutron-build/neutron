//! G0-ssi (C-T0 §11): SSI (§8) over the §3 commit pipeline and the §3.1 registry, at
//! the G0-ssi scope of the §11 table: 3 SERIALIZABLE txns (one may be declared READ
//! ONLY; one may instead run a TRUNCATE), 2 preloaded heap keys plus 1 index range
//! and 1 index→row fetch, 2 statements per txn, relation locks with the §4
//! storage-id rule, pre-commit with the channel send as a separate step, a commit
//! thread draining groups of up to 2 requests (status-set and visible-advance as
//! separate steps per request), an async resolver, the SSI writer map with §8.6
//! retention, and a registry of (live) snapshots.
//!
//! Checked invariants: I-SSI-ORDER, I-SER (cycle in the dependency graph of the
//! committed history, built from ghost state: ww, wr, rw by commit/snapshot order,
//! with TRUNCATE's wipe as a write of every relation key), I-SSI-EDGES (the recorded
//! rw-edge set equals the SIREAD-footprint oracle computed from ghost state, both
//! directions) and I-SSI-PRECISION (every 40001 raised by the dangerous-structure
//! check has a valid recorded structure), plus §4 read correctness of every
//! performed read (a read at snapshot S returns the newest committed state <= S),
//! which carries seed 62.
//!
//! A SER read records reader-side edges (§4) and feeds the ghost read log, so reads
//! are steps here, unlike G0-commit's pure readers (see C-T0 §11 "Model
//! discipline": the exemption covers SIREAD-free reads only).
//!
//! ## Workloads (fixed, chosen by the initial `Choose`)
//!
//! Keys: heap keys `h0..h2` (`h0`, `h1` preloaded; `h2` exists only after the
//! phantom insert) and index entries `i0..i2` (`i0`, `i1` preloaded). "read hK" is
//! a point read (SIREAD, view, read); "scan [i0..i2]" an index range scan (SIREAD
//! on the bound, view, read of every entry in range); "fetch h0" the index→row
//! fetch (SIREAD on the heap key, then a **fresh** view); "write" places intents on
//! the listed keys; "insert" additionally creates `h2`/`i2` inside the scanned gap.
//!
//! | Work   | T0                                                  | T1                      | T2          |
//! |--------|-----------------------------------------------------|-------------------------|-------------|
//! | Skew   | read h0; write h1                                   | read h1; write h0       | —           |
//! | Ro     | READ ONLY: read h0; read h1                         | read h0; write h1       | write h0    |
//! | Eo     | read h0; write h1                                   | scan [i0..i2]; write h0 | insert h2+i2 |
//! | Scan   | scan [i0..i1]; fetch h0                             | write h0+i0             | —           |
//! | Trunc  | read h0                                             | —                       | TRUNCATE    |
//! | Hold   | WITH HOLD read h0 (materialise at commit, fetch after) | write h0             | —           |
//!
//! Workload write sets are pairwise disjoint (the TRUNCATE is a DDL and places no
//! intents), so no §5.1 wait ever arises; the model stays inside G0-write's scope.
//!
//! ## Seed → workload
//!
//! | seed | workload | deviation (one local change in the named protocol step) |
//! |------|----------|-------------------------------------------------------|
//! | 5  | Scan  | the scan registers its SIREAD after iterating |
//! | 6  | Scan  | the scan registers its SIREAD after its view opened |
//! | 20 | Skew  | the pre-commit critical section is split: check, then prepare+enqueue with no doomed re-read |
//! | 21 | Ro    | taking S and registering it are two steps (retirement runs inside the window) |
//! | 28 | Ro    | writer-map entry inserted after status is set (a skip before the insert loses the edge) |
//! | 29 | Ro    | SSI retention ignores `visible_ts` (retires before advance, dropping the writer-map entry) |
//! | 30 | Scan  | the index→row fetch reuses the scan's view |
//! | 32 | Trunc | TRUNCATE runs no relation-level SIREAD check |
//! | 36 | Scan  | the index→row fetch reads the latest state with no registered view |
//! | 39 | Skew  | writer-side edge added from a SIREAD holder that committed at or before S(W) |
//! | 40 | Ro    | retirement drops edges pointing at the retired txn, and the committed-T2 test walks the live edge list |
//! | 43 | Ro    | pre-commit checks only structures with the committer as pivot |
//! | 60 | Trunc | the DDL-side SIREAD check runs at pre-commit, after PREPARED (missing during the window) |
//! | 61 | Eo    | `earliest_out_conflict_commit` set by an X that commits after T (spurious 40001) |
//! | 62 | Hold  | the WITH HOLD cursor is not materialised; the fetch reads the live state after commit |
//! | 63 | Ro    | `earliest_out_conflict_commit` update skipped because T already has a group-assigned ts |
//!
//! ## Scope cuts (vs the full protocol; none observable by the four invariants)
//!
//! - No crash, fsync, `synchronous_commit` modes, ack step or `/sys` records:
//!   G0-commit's invariants. The pipeline keeps group step 1, step 3 (writer map),
//!   step 4 with status-set and visible-advance separate, and step 5.
//! - No status truncation, view counters or GC watermark in the registry: only
//!   snapshot registration, which is what §8.6 retention reads. View counters and
//!   `W` belong to G0-commit/G0-gc.
//! - No row-lock waits, shared locks, savepoints, EPQ, unique/FK checks or seq
//!   layers: G0-write's scope; the workloads never conflict (disjoint write sets).
//! - No GC job: TRUNCATE's retired prefix is never physically DeleteRanged; the
//!   wipe exists in the ghost write log and the catalog, and post-truncate access
//!   follows the modeled §4 storage-id rule (40001 when the current storage id was
//!   created after the accessor's snapshot; reads at a snapshot at or after the
//!   truncate see the relation empty).
//! - §8.4's "commit order equals prepare order" is enforced by gating `Enqueue` on
//!   lower-prepare txns having sent, restoring what the single critical section
//!   guarantees in the real system while keeping the channel send a separate step.
//! - The DDL-side check of §8.2 adds edges (at execution); the dangerous-structure
//!   evaluation itself runs at §8.4 pre-commit, which walks every position, so no
//!   separate evaluation at DDL time is modeled.
//! - §8.6's retired-id SIREAD promotion is vacuous here: the model has a single
//!   storage generation, so every SIREAD is on the relation the DDL retires.

use std::collections::{BTreeMap, BTreeSet};

use crate::Model;

pub type Ts = u8;

/// Seeded bugs of C-T0 §11 owned by this model.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bug {
    /// Seed 5: SIREAD registered after iterating.
    SireadAfterIterating,
    /// Seed 6: SIREAD registered after the scan's view opened.
    SireadAfterViewOpen,
    /// Seed 20: SSI pre-commit check not atomic with enqueue.
    PreCommitNotAtomic,
    /// Seed 21: taking `S` and registering it are separate steps.
    SnapshotSplit,
    /// Seed 28: writer-map entry inserted after status is set.
    WriterMapAfterStatus,
    /// Seed 29: SSI retention ignores `visible_ts`.
    RetireIgnoresVisible,
    /// Seed 30: index→row fetch reuses the scan's view.
    FetchReusesScanView,
    /// Seed 32: TRUNCATE without the relation-level SIREAD check.
    NoTruncateSireadCheck,
    /// Seed 36: index→row fetch reads the latest state without a registered view.
    FetchReadsLatest,
    /// Seed 39: writer-side edge from a SIREAD holder that committed at or before `S(W)`.
    EdgeFromNonconcurrentHolder,
    /// Seed 40: edge `T2 -> T3` dropped when T3's SSI state is retired; committed T2
    /// tested through its edge list instead of the frozen `earliest_out_conflict_commit`.
    DropEdgeOnRetire,
    /// Seed 43: pre-commit checks only structures with the committer as pivot.
    PivotOnlyCheck,
    /// Seed 60: DDL-side SIREAD check run at pre-commit after `PREPARED` instead of
    /// at DDL execution.
    LateDdlCheck,
    /// Seed 61: `earliest_out_conflict_commit` set by an X that commits after T.
    EarliestFromLaterCommit,
    /// Seed 62: SER `WITH HOLD` cursor read lazily after commit.
    LazyHoldCursor,
    /// Seed 63: `earliest_out_conflict_commit` update skipped because T already has
    /// a group-assigned commit ts.
    SkipAssignedTsUpdate,
}

impl Bug {
    pub const ALL: [Bug; 16] = [
        Bug::SireadAfterIterating,
        Bug::SireadAfterViewOpen,
        Bug::PreCommitNotAtomic,
        Bug::SnapshotSplit,
        Bug::WriterMapAfterStatus,
        Bug::RetireIgnoresVisible,
        Bug::FetchReusesScanView,
        Bug::NoTruncateSireadCheck,
        Bug::FetchReadsLatest,
        Bug::EdgeFromNonconcurrentHolder,
        Bug::DropEdgeOnRetire,
        Bug::PivotOnlyCheck,
        Bug::LateDdlCheck,
        Bug::EarliestFromLaterCommit,
        Bug::LazyHoldCursor,
        Bug::SkipAssignedTsUpdate,
    ];

    pub fn seed(self) -> u8 {
        match self {
            Bug::SireadAfterIterating => 5,
            Bug::SireadAfterViewOpen => 6,
            Bug::PreCommitNotAtomic => 20,
            Bug::SnapshotSplit => 21,
            Bug::WriterMapAfterStatus => 28,
            Bug::RetireIgnoresVisible => 29,
            Bug::FetchReusesScanView => 30,
            Bug::NoTruncateSireadCheck => 32,
            Bug::FetchReadsLatest => 36,
            Bug::EdgeFromNonconcurrentHolder => 39,
            Bug::DropEdgeOnRetire => 40,
            Bug::PivotOnlyCheck => 43,
            Bug::LateDdlCheck => 60,
            Bug::EarliestFromLaterCommit => 61,
            Bug::LazyHoldCursor => 62,
            Bug::SkipAssignedTsUpdate => 63,
        }
    }
}

/// Fixed workloads (see the module doc).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Work {
    Skew,
    Ro,
    Eo,
    Scan,
    Trunc,
    Hold,
}

const WORKS: [Work; 6] = [
    Work::Skew,
    Work::Ro,
    Work::Eo,
    Work::Scan,
    Work::Trunc,
    Work::Hold,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Key {
    Heap(u8),
    Idx(u8),
}

const H0: Key = Key::Heap(0);
const H1: Key = Key::Heap(1);
const H2: Key = Key::Heap(2);
const I0: Key = Key::Idx(0);
const I2: Key = Key::Idx(2);

/// Every key of the relation (heap and index entries): the TRUNCATE wipe and the
/// DDL-side SIREAD check.
const RELATION: [Key; 6] = [H0, H1, H2, Key::Idx(0), Key::Idx(1), I2];

/// A SIREAD bound. A range bound covers index entries `lo..=hi`, gaps included
/// (§8.1); a point bound covers one key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Bound {
    Point(Key),
    Range(u8, u8),
}

fn covers(b: &Bound, k: Key) -> bool {
    match (*b, k) {
        (Bound::Point(a), b) => a == b,
        (Bound::Range(lo, hi), Key::Idx(i)) => lo <= i && i <= hi,
        (Bound::Range(..), Key::Heap(..)) => false,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Kvk {
    Intent(Key),
    Version(Key, Ts),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Val {
    Intent { owner: u8, data: u8 },
    Data(u8),
}

type Kv = BTreeMap<Kvk, Val>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum WOp {
    Put(u8),
    Del,
}

/// One performed read, with what the protocol cannot see but the checks need: the
/// snapshot, the ts actually returned, and which view / SIREAD served it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct GRead {
    t: u8,
    key: Key,
    snap: Ts,
    ts: Ts,
    view: bool,
    vver: u32,
    sver: Option<u32>,
}

/// A dangerous structure `t1 -> t2 -> t3` (t1 == t3 allowed), frozen when a 40001
/// was raised, for I-SSI-PRECISION.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Fired {
    t1: u8,
    t2: u8,
    t3: u8,
    /// The `earliest_out_conflict_commit` value that fired (committed-T2 rule).
    fired: Option<Ts>,
    c3: Ts,
    t1_ro: bool,
    s1: Ts,
    o1: u32,
    o2: u32,
    o3: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct View {
    kv: Kv,
    ver: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum St {
    Active,
    /// Bug 20 only: the pre-commit check ran, prepare has not.
    Checked,
    Prepared,
    Committed(Ts),
    Aborted,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Txn {
    pc: u8,
    st: St,
    /// Registered snapshot (None = unregistered); the registration is what §8.6
    /// retention reads.
    snap: Option<Ts>,
    /// The snapshot value itself; it outlives the registration (§3.1: the
    /// registration ends with the txn, the value is what visibility and the §8.3
    /// read-only exception compare).
    snap_at: Ts,
    /// Bug 21: S read but not yet registered.
    snap_pending: Option<Ts>,
    sireads: BTreeMap<Bound, u32>,
    views: [Option<View>; 3],
    released: bool,
    /// Ts assigned by the commit thread (group step 1), before status.
    assigned: Option<Ts>,
    pseq: Option<u32>,
    enq: bool,
    /// Holds AccessShare on the relation (from first access to release).
    touched: bool,
    intents: BTreeSet<Key>,
    /// Materialised WITH HOLD cursor: (ts, view ver, SIREAD ver) of the frozen read.
    hold: Option<(Ts, u32, Option<u32>)>,
}

impl Txn {
    fn fresh() -> Txn {
        Txn {
            pc: 0,
            st: St::Active,
            snap: None,
            snap_at: 0,
            snap_pending: None,
            sireads: BTreeMap::new(),
            views: [None, None, None],
            released: false,
            assigned: None,
            pseq: None,
            enq: false,
            touched: false,
            intents: BTreeSet::new(),
            hold: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct GrpItem {
    t: u8,
    ts: Ts,
    insmap: bool,
    status: bool,
    advanced: bool,
    step5: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    w: Option<Work>,
    /// Global step version; only compared, renumbered by `normalize`.
    ver: u32,
    kv: Kv,
    txns: [Txn; 3],
    channel: Vec<u8>,
    group: Vec<GrpItem>,
    next_ts: Ts,
    visible_ts: Ts,
    prepare_seq: u32,
    resolve_q: Vec<(u8, Key)>,
    cleanup_q: Vec<(u8, Key)>,
    aexcl: Option<u8>,
    /// Ts at which the relation's current storage id was created (0 = original).
    catalog_created: Ts,
    /// Recorded SSI state (what §8 owns).
    edges: BTreeSet<(u8, u8)>,
    wmap: BTreeMap<Ts, u8>,
    earliest: BTreeMap<u8, Ts>,
    doomed: BTreeMap<u8, Fired>,
    retired: [bool; 3],
    /// Ghost state.
    g_commits: Vec<(u8, Ts)>,
    g_writes: [BTreeMap<Key, WOp>; 3],
    g_reads: Vec<GRead>,
    g_edges: BTreeSet<(u8, u8)>,
    g_abort: Vec<Fired>,
    bad: Option<String>,
}

#[derive(Clone, Debug)]
pub enum Action {
    Choose(Work),
    Step(u8),
    Drain(u8),
    InsMap(u8),
    SetStatus(u8),
    Advance(u8),
    Step5(u8),
    Resolve(u8, Key),
    Cleanup(u8, Key),
    Retire(u8),
}

pub struct SsiModel {
    pub bug: Option<Bug>,
}

// -- fixed programs ---------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    TakeSnapshot,
    Siread(Bound),
    OpenView(u8),
    Read { slot: u8, key: Key },
    Scan { slot: u8, lo: u8, hi: u8 },
    ReadLatest { key: Key },
    Place { keys: &'static [Key] },
    AcquireAExcl,
    DdlExec,
    Materialise { key: Key },
    PreCommit,
    Enqueue,
    HoldFetch { key: Key },
}

use Step as S;

fn program(bug: Option<Bug>, w: Work, t: u8) -> &'static [Step] {
    match (w, t) {
        (Work::Skew, 0) => &[
            S::TakeSnapshot,
            S::Siread(Bound::Point(H0)),
            S::OpenView(0),
            S::Read { slot: 0, key: H0 },
            S::Place { keys: &[H1] },
            S::PreCommit,
            S::Enqueue,
        ],
        (Work::Skew, 1) => &[
            S::TakeSnapshot,
            S::Siread(Bound::Point(H1)),
            S::OpenView(0),
            S::Read { slot: 0, key: H1 },
            S::Place { keys: &[H0] },
            S::PreCommit,
            S::Enqueue,
        ],
        (Work::Skew, 2) => &[],
        (Work::Ro, 0) => &[
            S::TakeSnapshot,
            S::Siread(Bound::Point(H0)),
            S::OpenView(0),
            S::Read { slot: 0, key: H0 },
            S::Siread(Bound::Point(H1)),
            S::OpenView(1),
            S::Read { slot: 1, key: H1 },
            S::PreCommit,
            S::Enqueue,
        ],
        (Work::Ro, 1) => &[
            S::TakeSnapshot,
            S::Siread(Bound::Point(H0)),
            S::OpenView(0),
            S::Read { slot: 0, key: H0 },
            S::Place { keys: &[H1] },
            S::PreCommit,
            S::Enqueue,
        ],
        (Work::Ro, 2) => &[
            S::TakeSnapshot,
            S::Place { keys: &[H0] },
            S::PreCommit,
            S::Enqueue,
        ],
        (Work::Eo, 0) => &[
            S::TakeSnapshot,
            S::Siread(Bound::Point(H0)),
            S::OpenView(0),
            S::Read { slot: 0, key: H0 },
            S::Place { keys: &[H1] },
            S::PreCommit,
            S::Enqueue,
        ],
        (Work::Eo, 1) => &[
            S::TakeSnapshot,
            S::Siread(Bound::Range(0, 2)),
            S::OpenView(0),
            S::Scan {
                slot: 0,
                lo: 0,
                hi: 2,
            },
            S::Place { keys: &[H0] },
            S::PreCommit,
            S::Enqueue,
        ],
        (Work::Eo, 2) => &[
            S::TakeSnapshot,
            S::Place { keys: &[H2, I2] },
            S::PreCommit,
            S::Enqueue,
        ],
        // The scan workload's T0 is the one whose statement order the SIREAD-order
        // seeds reorder; the four variants are those local deviations.
        (Work::Scan, 0) => match bug {
            Some(Bug::SireadAfterIterating) => &[
                S::TakeSnapshot,
                S::OpenView(0),
                S::Scan {
                    slot: 0,
                    lo: 0,
                    hi: 1,
                },
                S::Siread(Bound::Range(0, 1)),
                S::Siread(Bound::Point(H0)),
                S::OpenView(1),
                S::Read { slot: 1, key: H0 },
                S::PreCommit,
                S::Enqueue,
            ],
            Some(Bug::SireadAfterViewOpen) => &[
                S::TakeSnapshot,
                S::OpenView(0),
                S::Siread(Bound::Range(0, 1)),
                S::Scan {
                    slot: 0,
                    lo: 0,
                    hi: 1,
                },
                S::Siread(Bound::Point(H0)),
                S::OpenView(1),
                S::Read { slot: 1, key: H0 },
                S::PreCommit,
                S::Enqueue,
            ],
            Some(Bug::FetchReusesScanView) => &[
                S::TakeSnapshot,
                S::Siread(Bound::Range(0, 1)),
                S::OpenView(0),
                S::Scan {
                    slot: 0,
                    lo: 0,
                    hi: 1,
                },
                S::Siread(Bound::Point(H0)),
                S::Read { slot: 0, key: H0 },
                S::PreCommit,
                S::Enqueue,
            ],
            Some(Bug::FetchReadsLatest) => &[
                S::TakeSnapshot,
                S::Siread(Bound::Range(0, 1)),
                S::OpenView(0),
                S::Scan {
                    slot: 0,
                    lo: 0,
                    hi: 1,
                },
                S::Siread(Bound::Point(H0)),
                S::ReadLatest { key: H0 },
                S::PreCommit,
                S::Enqueue,
            ],
            _ => &[
                S::TakeSnapshot,
                S::Siread(Bound::Range(0, 1)),
                S::OpenView(0),
                S::Scan {
                    slot: 0,
                    lo: 0,
                    hi: 1,
                },
                S::Siread(Bound::Point(H0)),
                S::OpenView(1),
                S::Read { slot: 1, key: H0 },
                S::PreCommit,
                S::Enqueue,
            ],
        },
        (Work::Scan, 1) => &[
            S::TakeSnapshot,
            S::Place { keys: &[H0, I0] },
            S::PreCommit,
            S::Enqueue,
        ],
        (Work::Scan, 2) => &[],
        (Work::Trunc, 0) => &[
            S::TakeSnapshot,
            S::Siread(Bound::Point(H0)),
            S::OpenView(0),
            S::Read { slot: 0, key: H0 },
            S::PreCommit,
            S::Enqueue,
        ],
        (Work::Trunc, 1) => &[],
        (Work::Trunc, 2) => &[
            S::TakeSnapshot,
            S::AcquireAExcl,
            S::DdlExec,
            S::PreCommit,
            S::Enqueue,
        ],
        (Work::Hold, 0) => match bug {
            Some(Bug::LazyHoldCursor) => &[
                S::TakeSnapshot,
                S::Siread(Bound::Point(H0)),
                S::OpenView(0),
                S::Read { slot: 0, key: H0 },
                S::PreCommit,
                S::Enqueue,
                S::HoldFetch { key: H0 },
            ],
            _ => &[
                S::TakeSnapshot,
                S::Siread(Bound::Point(H0)),
                S::OpenView(0),
                S::Read { slot: 0, key: H0 },
                S::Materialise { key: H0 },
                S::PreCommit,
                S::Enqueue,
                S::HoldFetch { key: H0 },
            ],
        },
        (Work::Hold, 1) => &[
            S::TakeSnapshot,
            S::Place { keys: &[H0] },
            S::PreCommit,
            S::Enqueue,
        ],
        (Work::Hold, _) => &[],
        _ => &[],
    }
}

fn declared_ro(w: Work, t: u8) -> bool {
    w == Work::Ro && t == 0
}

impl State {
    fn latest(&self) -> Kv {
        self.kv.clone()
    }

    fn ts_of(&self, t: u8) -> Ts {
        // An unassigned commit ts counts as infinity (§8.5).
        self.txns[t as usize].assigned.unwrap_or(Ts::MAX)
    }

    fn ord(&self, t: u8) -> u32 {
        // Commit order equals prepare order (§8.4), so prepare_seq orders members.
        self.txns[t as usize].pseq.unwrap_or(u32::MAX)
    }

    fn is_committed(&self, t: u8) -> bool {
        matches!(self.txns[t as usize].st, St::Committed(_))
    }

    fn is_aborted(&self, t: u8) -> bool {
        self.txns[t as usize].st == St::Aborted
    }

    fn ghost_writer(&self, ts: Ts) -> Option<u8> {
        self.g_commits.iter().find(|&&(_, c)| c == ts).map(|p| p.0)
    }

    fn snap_of(&self, t: u8) -> Ts {
        self.txns[t as usize].snap_at
    }

    fn ro_at(&self, t: u8) -> bool {
        // §8.3: declared READ ONLY, or no writes and committing.
        declared_ro(self.w.unwrap_or(Work::Skew), t) || self.g_writes[t as usize].is_empty()
    }

    /// §4 read of `key` at snapshot `snap` through `view`: the ts of the version
    /// read (0 = absent), the owners of skipped foreign intents, and the ts of
    /// every skipped version.
    fn read_at(&self, view: &Kv, key: Key, snap: Ts) -> (Ts, Vec<u8>, Vec<Ts>) {
        // §4 storage-id rule: for snapshots at or after the truncate the old
        // storage is gone; such reads see the relation empty (ts = the wipe's ts).
        if self.catalog_created > 0 && snap >= self.catalog_created {
            return (self.catalog_created, Vec::new(), Vec::new());
        }
        let mut owners = Vec::new();
        let mut vers = Vec::new();
        if let Some(Val::Intent { owner, .. }) = view.get(&Kvk::Intent(key)) {
            match self.txns[*owner as usize].st {
                St::Committed(c) if c <= snap => return (c, owners, vers),
                St::Aborted => {}
                _ => owners.push(*owner),
            }
        }
        let mut got = 0;
        for (k, _) in view.range(Kvk::Version(key, 0)..=Kvk::Version(key, Ts::MAX)) {
            if let Kvk::Version(_, ts) = k {
                if *ts <= snap {
                    got = got.max(*ts);
                } else {
                    vers.push(*ts);
                }
            }
        }
        (got, owners, vers)
    }

    /// Reader-side rw edges of a performed read (§4, §8.2): recorded from intent
    /// owners and the writer map; the oracle from intent owners and ghost commits.
    fn record_read(&mut self, t: u8, owners: &[u8], vers: &[Ts]) {
        for &o in owners {
            if o != t && !self.is_aborted(o) {
                self.edges.insert((t, o));
                self.g_edges.insert((t, o));
                // §8.5: an edge recorded to an already-committed X.
                let c = self.ts_of(o);
                if c != Ts::MAX && c < self.ts_of(t) {
                    let e = self.earliest.entry(t).or_insert(Ts::MAX);
                    *e = (*e).min(c);
                }
            }
        }
        for &c in vers {
            let mut target: Option<u8> = None;
            if let Some(&x) = self.wmap.get(&c) {
                if x != t {
                    self.edges.insert((t, x));
                    target = Some(x);
                }
            }
            if let Some(x) = self.ghost_writer(c) {
                if x != t {
                    self.g_edges.insert((t, x));
                }
            }
            if target.is_some() && c < self.ts_of(t) {
                let e = self.earliest.entry(t).or_insert(Ts::MAX);
                *e = (*e).min(c);
            }
        }
    }

    /// Writer-side SIREAD check after a data-changing placement (§5.1, §8.2). The
    /// oracle always applies the concurrency filter; seed 39 records the edge
    /// without it.
    fn writer_siread_check(&mut self, w: u8, keys: &[Key], bug: Option<Bug>) {
        let snap = self.snap_of(w);
        for r in 0..3u8 {
            if r == w || self.is_aborted(r) {
                continue;
            }
            if !keys
                .iter()
                .any(|k| self.txns[r as usize].sireads.keys().any(|b| covers(b, *k)))
            {
                continue;
            }
            let concurrent = !matches!(self.txns[r as usize].st, St::Committed(c) if c <= snap);
            if concurrent {
                self.g_edges.insert((r, w));
                self.edges.insert((r, w));
            } else if bug == Some(Bug::EdgeFromNonconcurrentHolder) {
                self.edges.insert((r, w));
            }
        }
    }

    /// DDL-side SIREAD check (§8.2). The oracle edge is derived here regardless of
    /// the bug; seeds 32 and 60 only change when (or whether) the recorded edge
    /// appears.
    fn ddl_siread_check(&mut self, w: u8, record: bool) {
        let snap = self.snap_of(w);
        for r in 0..3u8 {
            if r == w || self.is_aborted(r) {
                continue;
            }
            if !self.txns[r as usize]
                .sireads
                .keys()
                .any(|b| RELATION.iter().any(|k| covers(b, *k)))
            {
                continue;
            }
            let concurrent = !matches!(self.txns[r as usize].st, St::Committed(c) if c <= snap);
            if concurrent {
                self.g_edges.insert((r, w));
                if record {
                    self.edges.insert((r, w));
                }
            }
        }
    }

    /// Ts of the newest committed write to `key` at or before `snap` (ghost).
    fn expected_ts(&self, key: Key, snap: Ts) -> Ts {
        let mut best = 0;
        for &(x, c) in &self.g_commits {
            if c <= snap && c > best && self.g_writes[x as usize].contains_key(&key) {
                best = c;
            }
        }
        best
    }

    fn live_earliest(&self, y: u8) -> Option<Ts> {
        // Seed 40's path: derive the value from the live edge list.
        let mut best = None;
        for &(a, b) in &self.edges {
            if a == y {
                let c = self.ts_of(b);
                if c != Ts::MAX {
                    best = Some(best.map_or(c, |x: Ts| x.min(c)));
                }
            }
        }
        best
    }

    fn first_among(&self, t3: u8, members: [u8; 3]) -> bool {
        let o3 = self.ord(t3);
        members.iter().any(|&m| m != t3) && members.iter().all(|&m| m == t3 || self.ord(m) > o3)
    }

    fn ro_ok(&self, t1: u8, t3: u8) -> bool {
        !self.ro_at(t1)
            || matches!(self.txns[t3 as usize].assigned, Some(c) if c <= self.snap_of(t1))
    }

    fn fired(&self, t1: u8, t2: u8, t3: u8, value: Option<Ts>) -> Fired {
        let c3 = value.or(self.txns[t3 as usize].assigned).unwrap_or(0);
        Fired {
            t1,
            t2,
            t3,
            fired: value,
            c3,
            t1_ro: self.ro_at(t1),
            s1: self.snap_of(t1),
            o1: self.ord(t1),
            o2: self.ord(t2),
            o3: self.ord(t3),
        }
    }

    /// §8.3/§8.4 dangerous-structure check for the txn running it: every structure
    /// containing it two hops in both directions. Returns the structure and the
    /// victim (the checker itself means self-abort). The `X -> Y -> T` walk is
    /// omitted: the checker is never prepared, so it can never be the member that
    /// committed first.
    fn dangerous(&self, t: u8, bug: Option<Bug>) -> Option<(Fired, u8)> {
        // X -> t -> Y (t as pivot; the checker is the victim, it is not prepared).
        for x in 0..3u8 {
            if x == t || !self.edges.contains(&(x, t)) {
                continue;
            }
            for y in 0..3u8 {
                if y == t || !self.edges.contains(&(t, y)) {
                    continue;
                }
                if self.first_among(y, [x, t, y]) && self.ro_ok(x, y) {
                    return Some((self.fired(x, t, y, None), t));
                }
            }
        }
        if bug == Some(Bug::PivotOnlyCheck) {
            return None;
        }
        // t -> Y -> Z (t as T1). A committed Y is tested through
        // earliest_out_conflict_commit (§8.3); an active Y through its out-edges.
        for y in 0..3u8 {
            if y == t || !self.edges.contains(&(t, y)) {
                continue;
            }
            if self.is_committed(y) {
                let e = if bug == Some(Bug::DropEdgeOnRetire) {
                    self.live_earliest(y)
                } else {
                    self.earliest.get(&y).copied()
                };
                if let Some(e) = e {
                    if !self.ro_at(t) || e <= self.snap_of(t) {
                        let z = self.ghost_writer(e).unwrap_or(y);
                        return Some((self.fired(t, y, z, Some(e)), t));
                    }
                }
            } else {
                for z in 0..3u8 {
                    if z == t || z == y || !self.edges.contains(&(y, z)) {
                        continue;
                    }
                    if self.first_among(z, [t, y, z]) && self.ro_ok(t, z) {
                        let victim = if self.txns[y as usize].pseq.is_none() {
                            y
                        } else {
                            t
                        };
                        return Some((self.fired(t, y, z, None), victim));
                    }
                }
            }
        }
        None
    }

    fn prepare(&mut self, t: u8) {
        self.prepare_seq += 1;
        self.txns[t as usize].pseq = Some(self.prepare_seq);
        self.txns[t as usize].st = St::Prepared;
        self.txns[t as usize].pc += 1;
    }

    /// End a txn (40001 with its structure, or a §4 storage-id abort): SSI state
    /// removed (§8.6), locks released, intents queued for cleanup (§7.1).
    fn abort(&mut self, t: u8, raised: Option<Fired>) {
        if let Some(f) = raised {
            self.g_abort.push(f);
        }
        self.doomed.remove(&t);
        self.txns[t as usize].st = St::Aborted;
        self.txns[t as usize].released = true;
        self.txns[t as usize].snap = None;
        self.txns[t as usize].snap_pending = None;
        self.txns[t as usize].sireads.clear();
        let keys: Vec<Key> = self.txns[t as usize].intents.iter().copied().collect();
        for k in keys {
            self.cleanup_q.push((t, k));
        }
        self.txns[t as usize].intents.clear();
        self.edges.retain(|(a, b)| *a != t && *b != t);
        self.g_edges.retain(|(a, b)| *a != t && *b != t);
        if self.aexcl == Some(t) {
            self.aexcl = None;
        }
    }

    fn covering_sver(&self, t: u8, key: Key) -> Option<u32> {
        self.txns[t as usize]
            .sireads
            .iter()
            .filter(|(b, _)| covers(b, key))
            .map(|(_, v)| *v)
            .min()
    }

    /// The §4 read path shared by point reads, scans and the materialising read:
    /// read through `view`, record reader-side edges and the ghost read.
    fn do_read(&mut self, t: u8, view: &Kv, vver: u32, key: Key) {
        let snap = self.snap_of(t);
        let (ts, owners, vers) = self.read_at(view, key, snap);
        self.record_read(t, &owners, &vers);
        let sver = self.covering_sver(t, key);
        self.g_reads.push(GRead {
            t,
            key,
            snap,
            ts,
            view: true,
            vver,
            sver,
        });
    }

    fn storage_blocked(&self, t: u8) -> bool {
        self.catalog_created > 0 && self.catalog_created > self.snap_of(t)
    }

    fn is_ddl(&self, t: u8) -> bool {
        self.g_writes[t as usize]
            .iter()
            .any(|(_, op)| matches!(op, WOp::Del))
    }

    fn run_step(&mut self, bug: Option<Bug>, t: u8) {
        let w = match self.w {
            Some(w) => w,
            None => return,
        };
        let prog = program(bug, w, t);
        let pc = self.txns[t as usize].pc as usize;
        if pc >= prog.len() {
            return;
        }
        match prog[pc] {
            S::TakeSnapshot => {
                if bug == Some(Bug::SnapshotSplit) && self.txns[t as usize].snap_pending.is_none() {
                    // First half: read S without registering it.
                    self.txns[t as usize].snap_pending = Some(self.visible_ts);
                } else {
                    let v = self.txns[t as usize].snap_pending.take();
                    let v = v.unwrap_or(self.visible_ts);
                    self.txns[t as usize].snap = Some(v);
                    self.txns[t as usize].snap_at = v;
                    self.txns[t as usize].pc += 1;
                }
            }
            S::Siread(b) => {
                self.txns[t as usize].sireads.insert(b, self.ver);
                self.txns[t as usize].touched = true;
                self.txns[t as usize].pc += 1;
            }
            S::OpenView(slot) => {
                let kv = self.latest();
                let ver = self.ver;
                self.txns[t as usize].views[slot as usize] = Some(View { kv, ver });
                self.txns[t as usize].pc += 1;
            }
            S::Read { slot, key } => {
                if self.storage_blocked(t) {
                    self.abort(t, None);
                    return;
                }
                if let Some(v) = self.txns[t as usize].views[slot as usize].clone() {
                    let vver = v.ver;
                    self.do_read(t, &v.kv, vver, key);
                    self.txns[t as usize].pc += 1;
                }
            }
            S::Scan { slot, lo, hi } => {
                if self.storage_blocked(t) {
                    self.abort(t, None);
                    return;
                }
                if let Some(v) = self.txns[t as usize].views[slot as usize].clone() {
                    let vver = v.ver;
                    for i in lo..=hi {
                        self.do_read(t, &v.kv, vver, Key::Idx(i));
                    }
                    self.txns[t as usize].pc += 1;
                }
            }
            S::ReadLatest { key } => {
                // Seed 36: the fetch reads the latest state with no view at all.
                if self.storage_blocked(t) {
                    self.abort(t, None);
                    return;
                }
                let kv = self.latest();
                let snap = self.snap_of(t);
                let (ts, owners, vers) = self.read_at(&kv, key, snap);
                self.record_read(t, &owners, &vers);
                let sver = self.covering_sver(t, key);
                let vver = self.ver;
                self.g_reads.push(GRead {
                    t,
                    key,
                    snap,
                    ts,
                    view: false,
                    vver,
                    sver,
                });
                self.txns[t as usize].pc += 1;
            }
            S::Place { keys } => {
                if self.storage_blocked(t) {
                    self.abort(t, None);
                    return;
                }
                let data = t + 1;
                for k in keys {
                    self.kv
                        .insert(Kvk::Intent(*k), Val::Intent { owner: t, data });
                    self.txns[t as usize].intents.insert(*k);
                    self.g_writes[t as usize].insert(*k, WOp::Put(data));
                }
                self.txns[t as usize].touched = true;
                let owned: Vec<Key> = keys.to_vec();
                self.writer_siread_check(t, &owned, bug);
                self.txns[t as usize].pc += 1;
            }
            S::AcquireAExcl => {
                self.aexcl = Some(t);
                self.txns[t as usize].pc += 1;
            }
            S::DdlExec => {
                if self.aexcl != Some(t) {
                    return;
                }
                // The wipe is a write of every relation key; it becomes real for
                // the ghost when the txn commits.
                for k in RELATION {
                    self.g_writes[t as usize].insert(k, WOp::Del);
                }
                let record =
                    bug != Some(Bug::NoTruncateSireadCheck) && bug != Some(Bug::LateDdlCheck);
                self.ddl_siread_check(t, record);
                self.txns[t as usize].pc += 1;
            }
            S::Materialise { key } => {
                // §3.1: a holdable cursor is materialised at commit, before the
                // SSI pre-commit, by re-reading at S through a fresh view.
                if self.storage_blocked(t) {
                    self.abort(t, None);
                    return;
                }
                let kv = self.latest();
                let ver = self.ver;
                self.do_read(t, &kv, ver, key);
                if let Some(r) = self.g_reads.last() {
                    self.txns[t as usize].hold = Some((r.ts, r.vver, r.sver));
                }
                self.txns[t as usize].pc += 1;
            }
            S::PreCommit => match self.txns[t as usize].st {
                St::Active => {
                    if bug == Some(Bug::PreCommitNotAtomic) {
                        // Check, then leave prepare+enqueue for later steps.
                        let mut aborted = false;
                        match self.dangerous(t, bug) {
                            Some((f, victim)) if victim == t => {
                                self.abort(t, Some(f));
                                aborted = true;
                            }
                            Some((f, victim)) => {
                                self.doomed.insert(victim, f);
                            }
                            None => {}
                        }
                        if !aborted {
                            self.txns[t as usize].st = St::Checked;
                        }
                    } else {
                        match self.dangerous(t, bug) {
                            Some((f, victim)) if victim == t => self.abort(t, Some(f)),
                            Some((f, victim)) => {
                                self.doomed.insert(victim, f);
                                self.prepare(t);
                            }
                            None => self.prepare(t),
                        }
                        // Seed 60: the DDL-side check runs here, after PREPARED.
                        if bug == Some(Bug::LateDdlCheck) && self.is_ddl(t) {
                            self.ddl_siread_check(t, true);
                        }
                    }
                }
                // Bug 20 second half: no doomed re-read inside the broken window.
                St::Checked => self.prepare(t),
                _ => {}
            },
            S::Enqueue => {
                self.channel.push(t);
                self.txns[t as usize].enq = true;
                self.txns[t as usize].pc += 1;
            }
            S::HoldFetch { key } => {
                let snap = self.snap_of(t);
                match self.txns[t as usize].hold {
                    Some((ts, vver, sver)) => {
                        // The cursor reads the materialised copy; nothing touches
                        // the KV after the txn ended.
                        self.g_reads.push(GRead {
                            t,
                            key,
                            snap,
                            ts,
                            view: true,
                            vver,
                            sver,
                        });
                    }
                    None => {
                        // Seed 62: nothing was materialised; the fetch re-reads
                        // the latest committed state, ignoring the cursor's
                        // snapshot.
                        let kv = self.latest();
                        let mut ts = 0;
                        for (k, _) in kv.range(Kvk::Version(key, 0)..=Kvk::Version(key, Ts::MAX)) {
                            if let Kvk::Version(_, c) = k {
                                ts = ts.max(*c);
                            }
                        }
                        if let Some(Val::Intent { owner, .. }) = kv.get(&Kvk::Intent(key)) {
                            if let St::Committed(c) = self.txns[*owner as usize].st {
                                ts = ts.max(c);
                            }
                        }
                        let vver = self.ver;
                        self.g_reads.push(GRead {
                            t,
                            key,
                            snap,
                            ts,
                            view: false,
                            vver,
                            sver: None,
                        });
                    }
                }
                self.txns[t as usize].hold = None;
                // The cursor closes; its snapshot is unregistered.
                self.txns[t as usize].snap = None;
                self.txns[t as usize].pc += 1;
            }
        }
    }

    fn clear_group_if_done(&mut self) {
        if !self.group.is_empty()
            && self
                .group
                .iter()
                .all(|p| p.insmap && p.status && p.advanced && p.step5)
        {
            self.group.clear();
        }
    }
}

impl State {
    /// Canonical form for deduplication: the version counter and the prepare
    /// counter are only compared, so both are renumbered by rank.
    fn normalize(&mut self) {
        let mut vals: Vec<u32> = Vec::with_capacity(32);
        vals.push(self.ver);
        for x in &self.txns {
            vals.extend(x.sireads.values().copied());
            for v in x.views.iter().flatten() {
                vals.push(v.ver);
            }
            if let Some((_, v, _)) = x.hold {
                vals.push(v);
            }
        }
        for r in &self.g_reads {
            vals.push(r.vver);
            if let Some(sv) = r.sver {
                vals.push(sv);
            }
        }
        vals.sort_unstable();
        vals.dedup();
        let rank = |v: u32| vals.binary_search(&v).map_or(0, |i| i as u32);
        self.ver = rank(self.ver);
        for x in &mut self.txns {
            for v in x.sireads.values_mut() {
                *v = rank(*v);
            }
            for v in x.views.iter_mut().flatten() {
                v.ver = rank(v.ver);
            }
            if let Some((_, v, _)) = &mut x.hold {
                *v = rank(*v);
            }
        }
        for r in &mut self.g_reads {
            r.vver = rank(r.vver);
            if let Some(sv) = &mut r.sver {
                *sv = rank(*sv);
            }
        }

        let mut ps: Vec<u32> = Vec::with_capacity(16);
        ps.push(0);
        ps.push(self.prepare_seq);
        ps.push(u32::MAX);
        for x in &self.txns {
            if let Some(p) = x.pseq {
                ps.push(p);
            }
        }
        for f in self.doomed.values().chain(self.g_abort.iter()) {
            ps.extend([f.o1, f.o2, f.o3]);
        }
        ps.sort_unstable();
        ps.dedup();
        let prank = |v: u32| ps.binary_search(&v).map_or(0, |i| i as u32);
        self.prepare_seq = prank(self.prepare_seq);
        for x in &mut self.txns {
            if let Some(p) = x.pseq {
                x.pseq = Some(prank(p));
            }
        }
        for f in self.doomed.values_mut().chain(self.g_abort.iter_mut()) {
            f.o1 = prank(f.o1);
            f.o2 = prank(f.o2);
            f.o3 = prank(f.o3);
        }
    }
}

impl Model for SsiModel {
    type State = State;
    type Action = Action;

    fn init(&self) -> State {
        let mut kv = Kv::new();
        kv.insert(Kvk::Version(H0, 0), Val::Data(1));
        kv.insert(Kvk::Version(H1, 0), Val::Data(1));
        kv.insert(Kvk::Version(Key::Idx(0), 0), Val::Data(1));
        kv.insert(Kvk::Version(Key::Idx(1), 0), Val::Data(1));
        State {
            w: None,
            ver: 1,
            kv,
            txns: [Txn::fresh(), Txn::fresh(), Txn::fresh()],
            channel: Vec::new(),
            group: Vec::new(),
            next_ts: 1,
            visible_ts: 0,
            prepare_seq: 0,
            resolve_q: Vec::new(),
            cleanup_q: Vec::new(),
            aexcl: None,
            catalog_created: 0,
            edges: BTreeSet::new(),
            wmap: BTreeMap::new(),
            earliest: BTreeMap::new(),
            doomed: BTreeMap::new(),
            retired: [false, false, false],
            g_commits: Vec::new(),
            g_writes: [BTreeMap::new(), BTreeMap::new(), BTreeMap::new()],
            g_reads: Vec::new(),
            g_edges: BTreeSet::new(),
            g_abort: Vec::new(),
            bad: None,
        }
    }

    fn actions(&self, s: &State, out: &mut Vec<Action>) {
        if s.bad.is_some() {
            return;
        }
        let Some(w) = s.w else {
            out.extend(WORKS.iter().copied().map(Action::Choose));
            return;
        };
        for t in 0..3u8 {
            let txn = &s.txns[t as usize];
            let prog = program(self.bug, w, t);
            let Some(step) = prog.get(txn.pc as usize).copied() else {
                continue;
            };
            let ok = match step {
                S::Enqueue => {
                    if txn.st != St::Prepared {
                        false
                    } else {
                        match txn.pseq {
                            None => false,
                            Some(myp) => (0..3u8).all(|x| {
                                if x == t {
                                    return true;
                                }
                                let o = &s.txns[x as usize];
                                // §8.4: commit order equals prepare order, so a
                                // lower-prepare txn must have sent first.
                                !matches!((o.pseq, o.enq), (Some(px), false) if px < myp)
                            }),
                        }
                    }
                }
                S::HoldFetch { .. } => txn.released && matches!(txn.st, St::Committed(_)),
                S::AcquireAExcl => {
                    s.aexcl.is_none()
                        && (0..3u8).all(|x| {
                            x == t
                                || !s.txns[x as usize].touched
                                || s.txns[x as usize].released
                                || s.txns[x as usize].st == St::Aborted
                        })
                }
                S::DdlExec => s.aexcl == Some(t),
                _ => matches!(txn.st, St::Active | St::Checked),
            };
            if ok {
                out.push(Action::Step(t));
            }
        }
        if s.group.is_empty() && !s.channel.is_empty() {
            out.push(Action::Drain(1));
            if s.channel.len() >= 2 {
                out.push(Action::Drain(2));
            }
        }
        for (i, it) in s.group.iter().enumerate() {
            if !it.insmap {
                out.push(Action::InsMap(i as u8));
            }
            // §3 step 3 precedes step 4; seed 28 inverts the requirement.
            let insmap_ok = it.insmap || self.bug == Some(Bug::WriterMapAfterStatus);
            if !it.status && insmap_ok && s.group[..i].iter().all(|p| p.status) {
                out.push(Action::SetStatus(i as u8));
            }
            if it.status && !it.advanced && s.group[..i].iter().all(|p| p.advanced) {
                out.push(Action::Advance(i as u8));
            }
            if it.advanced && !it.step5 {
                out.push(Action::Step5(i as u8));
            }
        }
        for &(t, k) in &s.resolve_q {
            out.push(Action::Resolve(t, k));
        }
        for &(t, k) in &s.cleanup_q {
            out.push(Action::Cleanup(t, k));
        }
        for t in 0..3u8 {
            if !s.retired[t as usize] {
                if let St::Committed(c) = s.txns[t as usize].st {
                    // §8.6: both visible_ts >= c and every SER snapshot below c ended.
                    let vis_ok = self.bug == Some(Bug::RetireIgnoresVisible) || s.visible_ts >= c;
                    let snap_ok = (0..3u8)
                        .all(|x| x == t || !matches!(s.txns[x as usize].snap, Some(sv) if sv < c));
                    if vis_ok && snap_ok {
                        out.push(Action::Retire(t));
                    }
                }
            }
        }
    }

    fn next(&self, s: &State, a: &Action) -> State {
        let mut s = s.clone();
        match a {
            Action::Choose(w) => {
                s.w = Some(*w);
            }
            Action::Step(t) => {
                if matches!(s.txns[*t as usize].st, St::Active | St::Checked) {
                    if let Some(f) = s.doomed.get(t).copied() {
                        s.abort(*t, Some(f));
                    } else {
                        s.run_step(self.bug, *t);
                    }
                } else {
                    s.run_step(self.bug, *t);
                }
            }
            Action::Drain(n) => {
                for _ in 0..*n {
                    if let Some(t) = s.channel.first().copied() {
                        s.channel.remove(0);
                        let ts = s.next_ts;
                        s.next_ts += 1;
                        s.txns[t as usize].assigned = Some(ts);
                        s.group.push(GrpItem {
                            t,
                            ts,
                            insmap: false,
                            status: false,
                            advanced: false,
                            step5: false,
                        });
                    }
                }
            }
            Action::InsMap(i) => {
                let it = s.group[*i as usize];
                s.wmap.insert(it.ts, it.t);
                // §3 step 3 / §8.5: for every txn with an edge to it, update
                // earliest_out_conflict_commit.
                for x in 0..3u8 {
                    if s.edges.contains(&(x, it.t)) {
                        let tx = s.ts_of(x);
                        let upd = match self.bug {
                            // Seed 63: skipped because T already has a ts.
                            Some(Bug::SkipAssignedTsUpdate) => tx == Ts::MAX,
                            // Seed 61: set by an X that commits after T.
                            Some(Bug::EarliestFromLaterCommit) => it.ts >= tx,
                            _ => it.ts < tx,
                        };
                        if upd {
                            let e = s.earliest.entry(x).or_insert(Ts::MAX);
                            *e = (*e).min(it.ts);
                        }
                    }
                }
                s.group[*i as usize].insmap = true;
                s.clear_group_if_done();
            }
            Action::SetStatus(i) => {
                let it = s.group[*i as usize];
                s.txns[it.t as usize].st = St::Committed(it.ts);
                s.g_commits.push((it.t, it.ts));
                s.group[*i as usize].status = true;
                s.clear_group_if_done();
            }
            Action::Advance(i) => {
                let it = s.group[*i as usize];
                s.visible_ts = it.ts;
                if s.is_ddl(it.t) {
                    s.catalog_created = it.ts;
                }
                s.group[*i as usize].advanced = true;
                s.clear_group_if_done();
            }
            Action::Step5(i) => {
                let it = s.group[*i as usize];
                let keys: Vec<Key> = s.txns[it.t as usize].intents.iter().copied().collect();
                for k in keys {
                    s.resolve_q.push((it.t, k));
                }
                if s.aexcl == Some(it.t) {
                    s.aexcl = None;
                }
                s.txns[it.t as usize].released = true;
                // The txn's snapshot stays registered until it ends, unless a WITH
                // HOLD cursor keeps it registered until it closes (§3.1).
                let hold_pending = match s.w {
                    Some(w) => program(self.bug, w, it.t)
                        .iter()
                        .skip(s.txns[it.t as usize].pc as usize)
                        .any(|st| matches!(st, S::HoldFetch { .. })),
                    None => false,
                };
                if !hold_pending {
                    s.txns[it.t as usize].snap = None;
                }
                s.group[*i as usize].step5 = true;
                s.clear_group_if_done();
            }
            Action::Resolve(t, k) => {
                let c = match s.txns[*t as usize].st {
                    St::Committed(c) => c,
                    _ => 0,
                };
                let data = match s.g_writes[*t as usize].get(k) {
                    Some(WOp::Put(v)) => *v,
                    _ => 0,
                };
                if matches!(s.kv.get(&Kvk::Intent(*k)), Some(Val::Intent { owner, .. }) if *owner == *t)
                {
                    s.kv.remove(&Kvk::Intent(*k));
                    s.kv.insert(Kvk::Version(*k, c), Val::Data(data));
                }
                s.txns[*t as usize].intents.remove(k);
                s.resolve_q.retain(|&(x, y)| (x, y) != (*t, *k));
            }
            Action::Cleanup(t, k) => {
                if matches!(s.kv.get(&Kvk::Intent(*k)), Some(Val::Intent { owner, .. }) if *owner == *t)
                {
                    s.kv.remove(&Kvk::Intent(*k));
                }
                s.txns[*t as usize].intents.remove(k);
                s.cleanup_q.retain(|&(x, y)| (x, y) != (*t, *k));
            }
            Action::Retire(t) => {
                let t = *t;
                s.retired[t as usize] = true;
                s.txns[t as usize].sireads.clear();
                // §8.2: retiring Y never removes an edge pointing at Y.
                if self.bug == Some(Bug::DropEdgeOnRetire) {
                    s.edges.retain(|(a, b)| *a != t && *b != t);
                } else {
                    s.edges.retain(|(a, _)| *a != t);
                }
                s.g_edges.retain(|(a, _)| *a != t);
                if let St::Committed(c) = s.txns[t as usize].st {
                    s.wmap.remove(&c);
                }
            }
        }
        s.ver += 1;
        s.normalize();
        s
    }

    fn check(&self, s: &State) -> Result<(), String> {
        if let Some(b) = &s.bad {
            return Err(b.clone());
        }
        // §4 read correctness of every performed read.
        for r in &s.g_reads {
            let want = s.expected_ts(r.key, r.snap);
            if r.ts != want {
                return Err(format!(
                    "§4/I-SER: T{} read {:?} at S={} and got ts {}, but the committed state at S is ts {}",
                    r.t, r.key, r.snap, r.ts, want
                ));
            }
        }
        // I-SSI-ORDER.
        for r in &s.g_reads {
            if !r.view {
                return Err(format!(
                    "I-SSI-ORDER: T{} read {:?} from the latest state without a registered view",
                    r.t, r.key
                ));
            }
            match r.sver {
                None => {
                    return Err(format!(
                        "I-SSI-ORDER: T{} read {:?} with no covering SIREAD registered",
                        r.t, r.key
                    ))
                }
                Some(sv) if sv >= r.vver => {
                    return Err(format!(
                        "I-SSI-ORDER: T{}'s SIREAD covering {:?} was registered after the view it read from was opened",
                        r.t, r.key
                    ))
                }
                _ => {}
            }
        }
        // I-SSI-EDGES, both directions.
        for e in &s.g_edges {
            if !s.edges.contains(e) {
                return Err(format!(
                    "I-SSI-EDGES: missing recorded rw-edge T{} -> T{} (the SIREAD footprint derives it)",
                    e.0, e.1
                ));
            }
        }
        for e in &s.edges {
            if !s.g_edges.contains(e) {
                return Err(format!(
                    "I-SSI-EDGES: recorded rw-edge T{} -> T{} is not derived from the SIREAD footprint",
                    e.0, e.1
                ));
            }
        }
        // I-SER: dependency graph over committed txns (ww, wr, rw).
        let mut adj: BTreeMap<u8, BTreeSet<u8>> = BTreeMap::new();
        let committed = |t: u8| s.g_commits.iter().any(|&(x, _)| x == t);
        for r in &s.g_reads {
            if !committed(r.t) {
                continue;
            }
            if r.ts > 0 {
                if let Some(wx) = s.ghost_writer(r.ts) {
                    if wx != r.t {
                        adj.entry(wx).or_default().insert(r.t);
                    }
                }
            }
            for &(x, c) in &s.g_commits {
                if x != r.t && c > r.ts && s.g_writes[x as usize].contains_key(&r.key) {
                    adj.entry(r.t).or_default().insert(x);
                }
            }
        }
        for &(x, c) in &s.g_commits {
            for &(y, c2) in &s.g_commits {
                if x != y
                    && c < c2
                    && s.g_writes[x as usize]
                        .keys()
                        .any(|k| s.g_writes[y as usize].contains_key(k))
                {
                    adj.entry(x).or_default().insert(y);
                }
            }
        }
        for &n in adj.keys() {
            let mut stack = vec![n];
            let mut seen = BTreeSet::new();
            while let Some(x) = stack.pop() {
                if let Some(ns) = adj.get(&x) {
                    for &m in ns {
                        if m == n {
                            return Err(format!(
                                "I-SER: non-serializable committed history: dependency cycle through T{}",
                                n
                            ));
                        }
                        if seen.insert(m) {
                            stack.push(m);
                        }
                    }
                }
            }
        }
        // I-SSI-PRECISION.
        for f in &s.g_abort {
            if f.t1 != f.t3 && f.o3 >= f.o1 {
                return Err(format!(
                    "I-SSI-PRECISION: 40001 of T{} raised on structure T{}->T{}->T{} but T{} did not commit first",
                    f.t1, f.t1, f.t2, f.t3, f.t3
                ));
            }
            if f.t2 != f.t3 && f.o3 >= f.o2 {
                return Err(format!(
                    "I-SSI-PRECISION: 40001 of T{} raised on structure T{}->T{}->T{} but T{} did not commit first",
                    f.t1, f.t1, f.t2, f.t3, f.t3
                ));
            }
            if f.t1_ro && f.c3 > f.s1 {
                return Err(format!(
                    "I-SSI-PRECISION: 40001 raised on read-only T{} with commit_ts(T3)={} > S(T1)={}",
                    f.t1, f.c3, f.s1
                ));
            }
            if let Some(v) = f.fired {
                if v != f.c3 {
                    return Err(format!(
                        "I-SSI-PRECISION: fired earliest_out_conflict_commit {} is not the commit of T{} (ts {})",
                        v, f.t3, f.c3
                    ));
                }
            }
        }
        Ok(())
    }

    fn is_final(&self, s: &State) -> bool {
        s.bad.is_none()
    }
}
