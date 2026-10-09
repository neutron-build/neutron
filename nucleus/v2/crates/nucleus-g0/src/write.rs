//! G0-write (C-T0 §11): the write path — §5.0-§5.5 (without §5.3.1 ON CONFLICT, which is
//! card C-G0wb), §6 (locks, waits, wake generations, deadlock DFS, cancel), §7.1 (abort),
//! §7.3 (intent removal) and §7.4 (status truncation with remembered-TxnId lookups), plus
//! the commit thread of §3 with status-set, visible-advance, the shared-lock release and
//! the step-5 wake as separate steps, and the async resolver. Checked: I-ONE-INTENT,
//! I-WW, I-LOCK, I-UNIQUE, I-FK, I-HALLOWEEN, I-ATOMIC, I-LIVE (a/b/c), I-PROGRESS
//! (retry bound), I-RC-MONO, I-TRUNC, I-COUNT, and no lost update under RC.
//!
//! ## Latch ownership
//!
//! `latch` names the holder of `latch_key(k)` (§5.0: the `/i/` prefix for deferrable
//! entries, the key itself otherwise). A §5.1 latch section spans several model steps and
//! the latch stays held across them, so another actor's step that skips the latch (seeds
//! 4, 46) or latches a different key (seed 25) can land inside it — that window is what
//! those seeds' latch sections exist to close:
//!
//! - `SecRead` takes the latch, runs the foreign-intent block and reads the shared-lock
//!   table (remembering the conflicting holders). `SecGen` then reads their wake
//!   generations **still under the latch** (§5.1: "g' = their wake generations (read under
//!   this latch)") before unlatching and parking. A shared lock released without the key's
//!   latch (seed 46) can slip between the two reads; the waiter then records the
//!   post-bump generation and parks on a holder that no longer conflicts (I-LIVE a/c).
//! - `SecPlace`/`SecGrant` write the intent / record the shared lock under the same
//!   latch. A placement without the latch (seed 4) releases it after `SecRead`, so a
//!   second txn's `SecRead` can find no intent and both place, clobbering one intent
//!   (I-COUNT/I-ONE-INTENT).
//! - The deferrable commit check (§5.3 timing 3) is `DefScan` (take the prefix latch,
//!   read the `/i/` entries) then `DefLook` (status lookups and verdict, still latched).
//!   A `/i/` removal latched on the entry key (seed 25) plus a truncation can slip
//!   between them, so `DefLook` finds a missing status for an intent it read under the
//!   latch — the exact §3.1/§4 I-TRUNC fatality.
//! - Every removal (`Resolve`, `Cleanup`, `RollbackTo`, the shared releases of step 5,
//!   abort) takes the latch its spec section names, as an action guard.
//!
//! ## Workloads (fixed, chosen by the initial `Choose`)
//!
//! Every workload is concurrent (no single-transaction workloads). Writes are
//! read-modify-write (`v = v + 1`) under RC, so a skipped or stale EPQ re-check is a
//! visible lost update; `W_epq` adds a qual (`UPDATE ... WHERE v = 0`) that a concurrent
//! update makes fail, so the EPQ-fail skip path is reachable.
//!
//! | # | name | program (RC unless noted) |
//! |---|------|----------------------------|
//! | 0 | `main` | W0 `UPDATE k0; SAVEPOINT; UPDATE k1; SELECT k0 FOR UPDATE; ROLLBACK TO`; W1 `SELECT k1 FOR KEY SHARE; DELETE k0`; W2 `UPDATE k1; UPDATE k0`; cancel enabled |
//! | 1 | `lockdata` | W0 `UPDATE k0; FOR NO KEY UPDATE k0; SAVEPOINT; UPDATE k1; ROLLBACK TO`; W1 `UPDATE k0` |
//! | 2 | `uniq` | W0 `INSERT (t0,u0); INSERT (t1,u0)`; W1 `INSERT (t1,u0)` |
//! | 3 | `uniqdup` | W0 `SAVEPOINT; INSERT (t0,u0); ROLLBACK TO; INSERT (t0,u0)`; W1 `INSERT (t1,u0)` |
//! | 4 | `ownabsent` | preload row t1 + entry u0→t1; W0 `FOR NO KEY UPDATE t1; INSERT (t1,u1)`; W1 `UPDATE t1` |
//! | 5 | `deferrable` | W0 `INSERT (t0,d0)`; W1 `INSERT (t1,d1)` (deferrable unique value, prefix `/i/`, check at commit); cancel enabled |
//! | 6 | `fk` | preload parent t0, no child; W0 `DELETE parent t0` (end-of-stmt check); W1 `read parent; FOR KEY SHARE t0 (FK); INSERT child c1` |
//! | 7 | `keyshare` | preload t0; Ta `DELETE t0`; Tb `INSERT t0` (over the tombstone); R (RR) `FOR KEY SHARE t0` below both |
//! | 8 | `share` | preload t0; W0 `SAVEPOINT; FOR KEY SHARE t0; ROLLBACK TO`; W1 `DELETE t0` |
//! | 9 | `epq` | preload t0=0; W0 `UPDATE t0`; W2 `UPDATE t0`; W1 `UPDATE t0 WHERE v = 0` |
//! | 10 | `epqshare` | preload t0; W0 `UPDATE t0`; W2 `UPDATE t0`; W1 `FOR KEY SHARE t0` |
//! | 11 | `dlk3` | preload t0,t1,c0; W0 `UPDATE t0; UPDATE t1`; W1 `UPDATE t1; UPDATE c0`; W2 `UPDATE c0; UPDATE t0` (3-txn cycle) |
//! | 12 | `epqtomb` | preload t0; W0 `DELETE t0`; W1 `UPDATE t0` |
//! | 13 | `rellock` | preload t0; W0 `LOCK TABLE R EXCLUSIVE`; W1 relation lock R, `UPDATE t0` |
//!
//! ## Seed -> workload
//!
//! | seed | workload | | seed | workload |
//! |---|---|---|---|---|
//! | 4 | main | | 27 | main |
//! | 11 | main | | 37 | main |
//! | 12 | main | | 38 | main |
//! | 15 | lockdata | | 45 | keyshare |
//! | 16 | main | | 46 | share |
//! | 17 | main | | 47 | epq |
//! | 18 | main | | 48 | main |
//! | 19 | uniq | | 50 | main |
//! | 24 | ownabsent | | 52 | epqshare |
//! | 25 | deferrable | | 59 | dlk3 |
//! | 26 | main | | | |
//!
//! ## Mutation checks
//!
//! Beyond the seeds, [`Mutant`] carries five protocol deviations the clean model must
//! catch (checked by unit tests in this file): an RC UPDATE that skips EPQ (lost update,
//! caught by the increment oracle), both members of a deadlock cycle raising 40P01
//! (I-LIVE b), an UPDATE whose EPQ passes over a tombstone (increment on a dead row), a
//! 40P01 victim that keeps its wait-for edges (I-LIVE c), and an abort that does not
//! queue intent cleanup (stuck state).
//!
//! ## Scope cuts
//!
//! - No crash, fsync, epoch or `/sys` records (G0-commit owns them); the KV is one map,
//!   a batch is one atomic write, a view is a copy. Views open and close inside one step
//!   (the FK read, the parent-side scan), so `min(registered view counters)` is always
//!   `u32::MAX`; G0-commit owns the non-trivial truncation view-counter conditions.
//! - I-PROGRESS is checked by the §11 retry bound (a per-actor spin counter, bumped by
//!   each non-parking retry and reset by any other actor's step), not by lasso detection
//!   over the counter-dropping abstraction; on these fixed workloads the bound subsumes
//!   the lasso (every spin loop here is unbounded rather than cyclic).
//! - The write-set log is the in-memory list §5.5 describes (one entry per layer change);
//!   its spill to disk and layer compaction are not modelled. Layer existence (the seqs
//!   of an active txn's layers, from a bug-free ghost log) is checked; layer *contents*
//!   are checked only through the commit oracle (data) and I-LOCK interactions (locks).
//! - Seed 16 (rollback drops the restored top layer's lock) is caught when a second txn
//!   places over the lock-less pending intent — an I-ONE-INTENT/I-COUNT breach between
//!   two txns. A standing dual-lock state is unreachable from it because §5.1's
//!   holders-check precedes every lock-raising step, so no structural layer-shape rule is
//!   used (and none is needed).
//! - Relation locks have two modes (RowExclusive, Exclusive) exercised by `rellock` only;
//!   their release is fused into the step-5 wake (no per-key latch). No NOWAIT / SKIP
//!   LOCKED / `lock_timeout`, no advisory locks, no moved-tombstones or PK-changing
//!   UPDATEs (`key_changed` is still modelled and exercised by `keyshare`), no SIREAD or
//!   SSI state (G0-ssi), no `WITH HOLD` cursors, no deferred-trigger fixpoint, no NULLS
//!   DISTINCT, no ON CONFLICT (C-G0wb), and the deadlock DFS runs whenever a cycle exists
//!   (`deadlock_timeout` is not modelled).

use std::collections::BTreeMap;

use crate::Model;

pub type Ts = u8;

/// Logical keys in scope: two row keys, two unique entries, two deferrable `/i/`
/// entries (one prefix), two child keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum LKey {
    T0,
    T1,
    U0,
    U1,
    D0,
    D1,
    C0,
    C1,
}

const KEYS: [LKey; 8] = [
    LKey::T0,
    LKey::T1,
    LKey::U0,
    LKey::U1,
    LKey::D0,
    LKey::D1,
    LKey::C0,
    LKey::C1,
];
const DEF_KEYS: [LKey; 2] = [LKey::D0, LKey::D1];
const CHILD_KEYS: [LKey; 2] = [LKey::C0, LKey::C1];
const NKEYS: usize = 8;
const NO_WL: u8 = u8::MAX;
const NTXNS: usize = 3;

/// Lock modes. `KeyShare` is the one shared mode in scope (§11); it never appears in an
/// intent layer (§2.1), only in the shared lock table and as a requested mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Lock {
    None,
    NoKeyUpd,
    Update,
    KeyShare,
}

/// The §6 conflict matrix, applied to (requested, held).
fn conflicts(req: Lock, held: Lock) -> bool {
    use Lock::*;
    matches!(
        (req, held),
        (NoKeyUpd | Update, NoKeyUpd | Update) | (Update, KeyShare) | (KeyShare, Update)
    )
}

/// §2.1: a data write implies a lock.
fn implied(data: Data) -> Lock {
    match data {
        Data::Absent => Lock::None,
        Data::Write { kc, .. } => {
            if kc {
                Lock::Update
            } else {
                Lock::NoKeyUpd
            }
        }
        Data::Delete => Lock::Update,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Data {
    Absent,
    Write { val: u8, kc: bool },
    Delete,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum VerData {
    Live { val: u8, kc: bool },
    Tomb,
}

/// Ghost net effect of a txn's surviving statements on one key. Updates are increments
/// (`v = v + 1`), so the oracle can detect a stale base: the committed value must equal
/// the reset value plus the committed increments since it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ED {
    Dead,
    Inc {
        d: u8,
    },
    Row {
        val: u8,
        uval: Option<LKey>,
        child: bool,
    },
    Entry {
        row: LKey,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Layer {
    pub seq: u8,
    pub dseq: u8,
    pub data: Data,
    pub lock: Lock,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Intent {
    pub owner: u8,
    pub layers: Vec<Layer>,
}

/// One logical key's slot: at most one intent plus committed versions, newest first.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Slot {
    pub intent: Option<Intent>,
    pub vers: Vec<(Ts, VerData)>,
}

type Kv = BTreeMap<LKey, Slot>;

/// One frozen net-write set of a commit (or of a preload step).
type ExpWrites = Vec<(LKey, ED)>;
/// Ghost commits in ts order: (commit ts, txn or 255 for preload, writes).
type Commits = Vec<(Ts, u8, ExpWrites)>;

fn ver_of(d: Data) -> Option<VerData> {
    match d {
        Data::Write { val, kc } => Some(VerData::Live { val, kc }),
        Data::Delete => Some(VerData::Tomb),
        Data::Absent => None,
    }
}

fn top_layer(i: &Intent) -> Layer {
    i.layers.last().copied().unwrap_or(Layer {
        seq: 0,
        dseq: 0,
        data: Data::Absent,
        lock: Lock::None,
    })
}

/// §5.0 latch keys: the `/i/` prefix for deferrable entries, the key itself otherwise.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum LatchKey {
    Key(LKey),
    Prefix,
}

fn lk_of(key: LKey) -> LatchKey {
    if matches!(key, LKey::D0 | LKey::D1) {
        LatchKey::Prefix
    } else {
        LatchKey::Key(key)
    }
}

/// Seeded bugs of C-T0 §11 owned by this model.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bug {
    /// Seed 4: intent placed without the latch.
    PlaceWithoutLatch,
    /// Seed 11: async resolution or abort cleanup without the latch/owner check.
    NoOwnerRecheck,
    /// Seed 12: a foreign visible-committed intent overwritten without first removing it.
    OverwriteForeignEnded,
    /// Seed 15: lock-only request replaces own data.
    LockOnlyReplacesData,
    /// Seed 16: savepoint rollback drops the lock with the data.
    RollbackDropsLock,
    /// Seed 17: `wait_for` without the generation re-check.
    NoGenRecheck,
    /// Seed 18: writer proceeds on `Committed(c)` with `c > visible_ts`.
    ProceedBeforeVisible,
    /// Seed 19: unique check ignores own intents.
    UniqueIgnoresOwn,
    /// Seed 24: unique check treats an own `Absent` layer as not live.
    OwnAbsentNotLive,
    /// Seed 25: deferrable `/i/` removal latched on the entry key instead of the prefix.
    DeferrableEntryLatch,
    /// Seed 26: wake lost across `ROLLBACK TO` (status-only re-check).
    RollbackNoWake,
    /// Seed 27: async resolution queued before `visible_ts >= commit_ts`.
    EarlyResolveQueue,
    /// Seed 37: `wait_for` treats a missing status as Pending.
    MissingStatusPending,
    /// Seed 38: status truncated before the txn is `released`.
    TruncateBeforeRelease,
    /// Seed 45: KEY SHARE checks only the newest version above `S`.
    KeyShareNewestOnly,
    /// Seed 46: shared row lock released without the key's latch.
    SharedReleaseNoLatch,
    /// Seed 47: RC EPQ places its intent without re-verifying under the latch.
    EpqPlaceNoReverify,
    /// Seed 48: cancel does not wake a parked waiter.
    CancelNoWake,
    /// Seed 50: write-set log records only new intents, so `ROLLBACK TO` misses a later
    /// layer on an existing intent.
    LogOnlyNewIntents,
    /// Seed 52: EPQ repeats on a non-conflicting foreign intent instead of only on a
    /// changed version.
    EpqRepeatOnIntent,
    /// Seed 59: waker leaves woken waiters' edges in the graph.
    WakerLeavesEdges,
}

impl Bug {
    pub const ALL: [Bug; 21] = [
        Bug::PlaceWithoutLatch,
        Bug::NoOwnerRecheck,
        Bug::OverwriteForeignEnded,
        Bug::LockOnlyReplacesData,
        Bug::RollbackDropsLock,
        Bug::NoGenRecheck,
        Bug::ProceedBeforeVisible,
        Bug::UniqueIgnoresOwn,
        Bug::OwnAbsentNotLive,
        Bug::DeferrableEntryLatch,
        Bug::RollbackNoWake,
        Bug::EarlyResolveQueue,
        Bug::MissingStatusPending,
        Bug::TruncateBeforeRelease,
        Bug::KeyShareNewestOnly,
        Bug::SharedReleaseNoLatch,
        Bug::EpqPlaceNoReverify,
        Bug::CancelNoWake,
        Bug::LogOnlyNewIntents,
        Bug::EpqRepeatOnIntent,
        Bug::WakerLeavesEdges,
    ];

    pub fn seed(self) -> u8 {
        match self {
            Bug::PlaceWithoutLatch => 4,
            Bug::NoOwnerRecheck => 11,
            Bug::OverwriteForeignEnded => 12,
            Bug::LockOnlyReplacesData => 15,
            Bug::RollbackDropsLock => 16,
            Bug::NoGenRecheck => 17,
            Bug::ProceedBeforeVisible => 18,
            Bug::UniqueIgnoresOwn => 19,
            Bug::OwnAbsentNotLive => 24,
            Bug::DeferrableEntryLatch => 25,
            Bug::RollbackNoWake => 26,
            Bug::EarlyResolveQueue => 27,
            Bug::MissingStatusPending => 37,
            Bug::TruncateBeforeRelease => 38,
            Bug::KeyShareNewestOnly => 45,
            Bug::SharedReleaseNoLatch => 46,
            Bug::EpqPlaceNoReverify => 47,
            Bug::CancelNoWake => 48,
            Bug::LogOnlyNewIntents => 50,
            Bug::EpqRepeatOnIntent => 52,
            Bug::WakerLeavesEdges => 59,
        }
    }
}

/// Extra mutation checks (review rework): deviations that are not §11 seeds but that the
/// clean model must still catch. Run by the unit tests at the bottom of this file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mutant {
    /// RC UPDATE places from the stale snapshot instead of running EPQ.
    RcUpdateSkipsEpq,
    /// Every member of a deadlock cycle raises 40P01 (I-LIVE b).
    BothCycleMembers40P01,
    /// An UPDATE's EPQ quals pass over a tombstone (I-FK/lost-update class).
    EpqPassesTombstone,
    /// The 40P01 victim keeps its wait-for edges in the graph (I-LIVE c).
    VictimKeepsEdges,
    /// Abort does not queue the async intent cleanup.
    AbortCleanupNotQueued,
}

/// One operation of a statement. Ops of one statement share `seq0`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Op {
    /// Read-modify-write `v = v + 1`, with an optional qual `WHERE v = qual`.
    Write {
        key: LKey,
        kc: bool,
        qual: Option<u8>,
    },
    Delete {
        key: LKey,
    },
    /// Key-existence op: INSERT of the `/t/` row key (Pk), a `/u/` unique entry
    /// (Unique) or an `/i/` deferrable entry (Def, checked at commit, not here).
    KeyExist {
        key: LKey,
        kind: KeyKind,
    },
    LockOnly {
        key: LKey,
        upd: bool,
    },
    KeyShare {
        key: LKey,
        fk: bool,
    },
    /// FK child-side read of the parent (registered view, §3.1/§5.3).
    FkRead {
        key: LKey,
    },
    RelLock {
        excl: bool,
    },
    Savepoint,
    RollbackTo,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum KeyKind {
    Pk { uval: Option<LKey>, child: bool },
    Unique { row: LKey },
    Def { row: LKey },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StmtSpec {
    pub ops: &'static [Op],
    /// End-of-statement FK parent-side check (§5.3).
    pub parent_check: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TxnSpec {
    pub iso: Iso,
    pub stmts: &'static [StmtSpec],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Iso {
    Rc,
    Rr,
}

pub struct Workload {
    pub txns: &'static [TxnSpec],
    /// Preloaded committed versions with the ghost expected state they produce.
    pub preload: &'static [(LKey, Ts, VerData, ED)],
    pub cancel: bool,
    pub deferrable: bool,
}

macro_rules! upd {
    ($key:expr) => {
        Op::Write {
            key: $key,
            kc: false,
            qual: None,
        }
    };
}

macro_rules! stmt {
    ($($op:expr),* $(,)?) => {
        StmtSpec { ops: &[$($op),*], parent_check: false }
    };
}

macro_rules! pcheck {
    ($($op:expr),* $(,)?) => {
        StmtSpec { ops: &[$($op),*], parent_check: true }
    };
}

macro_rules! rc {
    ($($stmt:expr),* $(,)?) => {
        TxnSpec { iso: Iso::Rc, stmts: &[$($stmt),*] }
    };
}

macro_rules! pre_row {
    ($key:expr, $val:expr, $uval:expr, $child:expr) => {
        (
            $key,
            1,
            VerData::Live {
                val: $val,
                kc: false,
            },
            ED::Row {
                val: $val,
                uval: $uval,
                child: $child,
            },
        )
    };
}

/// W_main: locks, savepoints, waits and cancel in one 3-txn workload (reviewer's
/// measured shape). W0's `SELECT k0 FOR UPDATE` after the savepoint modifies the
/// existing k0 intent — the layer change whose write-set-log entry seed 50 misses.
const W_MAIN: Workload = Workload {
    txns: &[
        rc![
            stmt![upd!(LKey::T0)],
            stmt![Op::Savepoint],
            stmt![upd!(LKey::T1)],
            stmt![Op::LockOnly {
                key: LKey::T0,
                upd: true,
            }],
            stmt![Op::RollbackTo],
        ],
        rc![stmt![
            Op::KeyShare {
                key: LKey::T1,
                fk: false,
            },
            Op::Delete { key: LKey::T0 },
        ]],
        rc![stmt![upd!(LKey::T1), upd!(LKey::T0)]],
    ],
    preload: &[
        pre_row!(LKey::T0, 0, None, false),
        pre_row!(LKey::T1, 0, None, false),
    ],
    cancel: true,
    deferrable: false,
};

/// W_lockdata: the lock-only layer before the savepoint survives `ROLLBACK TO`, so a
/// lock-only layer that replaced own data (seed 15) commits `Absent` and loses the row.
const W_LOCKDATA: Workload = Workload {
    txns: &[
        rc![
            stmt![upd!(LKey::T0)],
            stmt![Op::LockOnly {
                key: LKey::T0,
                upd: false,
            }],
            stmt![Op::Savepoint],
            stmt![upd!(LKey::T1)],
            stmt![Op::RollbackTo],
        ],
        rc![stmt![upd!(LKey::T0)]],
    ],
    preload: &[
        pre_row!(LKey::T0, 0, None, false),
        pre_row!(LKey::T1, 0, None, false),
    ],
    cancel: false,
    deferrable: false,
};

/// W_uniq: seed 19 — the second insert's unique check must see W0's own live entry.
const W_UNIQ: Workload = Workload {
    txns: &[
        rc![
            stmt![
                Op::KeyExist {
                    key: LKey::T0,
                    kind: KeyKind::Pk {
                        uval: Some(LKey::U0),
                        child: false,
                    },
                },
                Op::KeyExist {
                    key: LKey::U0,
                    kind: KeyKind::Unique { row: LKey::T0 },
                },
            ],
            stmt![
                Op::KeyExist {
                    key: LKey::T1,
                    kind: KeyKind::Pk {
                        uval: Some(LKey::U0),
                        child: false,
                    },
                },
                Op::KeyExist {
                    key: LKey::U0,
                    kind: KeyKind::Unique { row: LKey::T1 },
                },
            ],
        ],
        rc![stmt![
            Op::KeyExist {
                key: LKey::T1,
                kind: KeyKind::Pk {
                    uval: Some(LKey::U0),
                    child: false,
                },
            },
            Op::KeyExist {
                key: LKey::U0,
                kind: KeyKind::Unique { row: LKey::T1 },
            },
        ]],
    ],
    preload: &[],
    cancel: false,
    deferrable: false,
};

/// W_uniqdup: the reviewer's concurrent duplicate unique insert (savepoint, rollback,
/// re-insert against a concurrent insert of the same value).
const W_UNIQDUP: Workload = Workload {
    txns: &[
        rc![
            stmt![Op::Savepoint],
            stmt![
                Op::KeyExist {
                    key: LKey::T0,
                    kind: KeyKind::Pk {
                        uval: Some(LKey::U0),
                        child: false,
                    },
                },
                Op::KeyExist {
                    key: LKey::U0,
                    kind: KeyKind::Unique { row: LKey::T0 },
                },
            ],
            stmt![Op::RollbackTo],
            stmt![
                Op::KeyExist {
                    key: LKey::T0,
                    kind: KeyKind::Pk {
                        uval: Some(LKey::U0),
                        child: false,
                    },
                },
                Op::KeyExist {
                    key: LKey::U0,
                    kind: KeyKind::Unique { row: LKey::T0 },
                },
            ],
        ],
        rc![stmt![
            Op::KeyExist {
                key: LKey::T1,
                kind: KeyKind::Pk {
                    uval: Some(LKey::U0),
                    child: false,
                },
            },
            Op::KeyExist {
                key: LKey::U0,
                kind: KeyKind::Unique { row: LKey::T1 },
            },
        ]],
    ],
    preload: &[],
    cancel: false,
    deferrable: false,
};

/// W_ownabsent: seed 24 — the insert's unique check on t1 must use the committed state
/// (live) even though W0's own top layer on t1 is `Absent` (lock-only).
const W_OWNABSENT: Workload = Workload {
    txns: &[
        rc![
            stmt![Op::LockOnly {
                key: LKey::T1,
                upd: false,
            }],
            stmt![
                Op::KeyExist {
                    key: LKey::T1,
                    kind: KeyKind::Pk {
                        uval: Some(LKey::U1),
                        child: false,
                    },
                },
                Op::KeyExist {
                    key: LKey::U1,
                    kind: KeyKind::Unique { row: LKey::T1 },
                },
            ],
        ],
        rc![stmt![upd!(LKey::T1)]],
    ],
    preload: &[
        pre_row!(LKey::T1, 0, Some(LKey::U0), false),
        (
            LKey::U0,
            1,
            VerData::Live { val: 0, kc: false },
            ED::Entry { row: LKey::T1 },
        ),
    ],
    cancel: false,
    deferrable: false,
};

/// W_deferrable: seed 25 — two rows under one deferrable prefix, checked at commit
/// under the prefix latch; cancel enabled so a `/i/` abort cleanup can race the check.
const W_DEFERRABLE: Workload = Workload {
    txns: &[
        rc![stmt![
            Op::KeyExist {
                key: LKey::T0,
                kind: KeyKind::Pk {
                    uval: None,
                    child: false,
                },
            },
            Op::KeyExist {
                key: LKey::D0,
                kind: KeyKind::Def { row: LKey::T0 },
            },
        ]],
        rc![stmt![
            Op::KeyExist {
                key: LKey::T1,
                kind: KeyKind::Pk {
                    uval: None,
                    child: false,
                },
            },
            Op::KeyExist {
                key: LKey::D1,
                kind: KeyKind::Def { row: LKey::T1 },
            },
        ]],
    ],
    preload: &[],
    cancel: true,
    deferrable: true,
};

/// W_fk: the live FK race — W1's FK KEY SHARE on the parent must wait on or conflict
/// with W0's pending delete; the parent-side check sees a live child or the child-side
/// EPQ sees the tombstone (23503, never a skip).
const W_FK: Workload = Workload {
    txns: &[
        rc![pcheck![Op::Delete { key: LKey::T0 }]],
        rc![stmt![
            Op::FkRead { key: LKey::T0 },
            Op::KeyShare {
                key: LKey::T0,
                fk: true,
            },
            Op::KeyExist {
                key: LKey::C1,
                kind: KeyKind::Pk {
                    uval: None,
                    child: true,
                },
            },
        ]],
    ],
    preload: &[pre_row!(LKey::T0, 0, None, false)],
    cancel: false,
    deferrable: false,
};

/// W_keyshare: the §11 KEY SHARE workload — a key-changing commit (the delete) then a
/// non-key commit (the re-insert) above the RR reader's snapshot.
const W_KEYSHARE: Workload = Workload {
    txns: &[
        rc![stmt![Op::Delete { key: LKey::T0 }]],
        rc![stmt![Op::KeyExist {
            key: LKey::T0,
            kind: KeyKind::Pk {
                uval: None,
                child: false,
            },
        }]],
        TxnSpec {
            iso: Iso::Rr,
            stmts: &[stmt![Op::KeyShare {
                key: LKey::T0,
                fk: false,
            }]],
        },
    ],
    preload: &[pre_row!(LKey::T0, 0, None, false)],
    cancel: false,
    deferrable: false,
};

/// W_share: seed 46 — the savepoint rollback releases W0's post-savepoint KEY SHARE;
/// the release must take the key's latch so it cannot slip between W1's holders-read
/// and its generations-read (both under the latch, §5.1/§6).
const W_SHARE: Workload = Workload {
    txns: &[
        rc![
            stmt![Op::Savepoint],
            stmt![Op::KeyShare {
                key: LKey::T0,
                fk: false,
            }],
            stmt![Op::RollbackTo],
        ],
        rc![stmt![Op::Delete { key: LKey::T0 }]],
    ],
    preload: &[pre_row!(LKey::T0, 0, None, false)],
    cancel: false,
    deferrable: false,
};

/// W_epq: observable lost updates — three RC read-modify-writes on one row, one with a
/// qual a concurrent update makes fail, so both the EPQ-pass and EPQ-skip paths run.
const W_EPQ: Workload = Workload {
    txns: &[
        rc![stmt![upd!(LKey::T0)]],
        rc![stmt![Op::Write {
            key: LKey::T0,
            kc: false,
            qual: Some(0),
        }]],
        rc![stmt![upd!(LKey::T0)]],
    ],
    preload: &[pre_row!(LKey::T0, 0, None, false)],
    cancel: false,
    deferrable: false,
};

/// W_epqshare: seed 52 — a KEY SHARE requester that EPQ'd once meets a non-conflicting
/// pending foreign intent and must proceed, not repeat EPQ.
const W_EPQSHARE: Workload = Workload {
    txns: &[
        rc![stmt![upd!(LKey::T0)]],
        rc![stmt![Op::KeyShare {
            key: LKey::T0,
            fk: false,
        }]],
        rc![stmt![upd!(LKey::T0)]],
    ],
    preload: &[pre_row!(LKey::T0, 0, None, false)],
    cancel: false,
    deferrable: false,
};

/// W_dlk3: a 3-txn deadlock cycle over t0, t1, c0.
const W_DLK3: Workload = Workload {
    txns: &[
        rc![stmt![upd!(LKey::T0), upd!(LKey::T1)]],
        rc![stmt![upd!(LKey::T1), upd!(LKey::C0)]],
        rc![stmt![upd!(LKey::C0), upd!(LKey::T0)]],
    ],
    preload: &[
        pre_row!(LKey::T0, 0, None, false),
        pre_row!(LKey::T1, 0, None, false),
        pre_row!(LKey::C0, 0, None, false),
    ],
    cancel: false,
    deferrable: false,
};

/// W_epqtomb: an UPDATE whose snapshot is below a concurrent DELETE — EPQ over a
/// tombstone must skip the row (the increment-on-dead-row oracle).
const W_EPQTOMB: Workload = Workload {
    txns: &[
        rc![stmt![Op::Delete { key: LKey::T0 }]],
        rc![stmt![upd!(LKey::T0)]],
    ],
    preload: &[pre_row!(LKey::T0, 0, None, false)],
    cancel: false,
    deferrable: false,
};

/// W_rellock: relation locks (AccessExclusive vs RowShare-equivalent) plus a row write.
const W_RELLOCK: Workload = Workload {
    txns: &[
        rc![stmt![Op::RelLock { excl: true }]],
        rc![stmt![
            Op::RelLock { excl: false },
            Op::Write {
                key: LKey::T0,
                kc: false,
                qual: None,
            },
        ]],
    ],
    preload: &[pre_row!(LKey::T0, 0, None, false)],
    cancel: false,
    deferrable: false,
};

const WORKLOADS: &[Workload] = &[
    W_MAIN,
    W_LOCKDATA,
    W_UNIQ,
    W_UNIQDUP,
    W_OWNABSENT,
    W_DEFERRABLE,
    W_FK,
    W_KEYSHARE,
    W_SHARE,
    W_EPQ,
    W_EPQSHARE,
    W_DLK3,
    W_EPQTOMB,
    W_RELLOCK,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Phase {
    /// Statement start: take the snapshot (RC per statement, RR once).
    Snap,
    /// Run one §5.1 latch section: `SecRead` (and the op's own actions).
    Op,
    /// Inside a section: the conflicting shared holders were read; their wake
    /// generations are read next, still under the latch.
    SecGen,
    /// Inside a section: the write (`SecPlace`) or the shared-lock grant (`SecGrant`).
    SecPlace,
    SecGrant,
    /// Unlatched, between the latch section and `wait_for` (`WaitEnter`).
    WaitReg,
    /// Unlatched, EPQ quals re-evaluation pending (`EpqStep`).
    Epq,
    /// Parked on the wait-for graph (`cc`: parked from the deferrable commit check).
    Parked {
        cc: bool,
    },
    /// End-of-statement FK parent-side check pending.
    EndS,
    /// Deferrable commit-check loop (§5.3 timing 3) pending.
    CommitCheck,
    /// Inside the deferrable check: entries read under the prefix latch, lookups next.
    DefLook,
    /// Commit request pending enqueue.
    Chan,
    /// Enqueued; the commit thread owns this txn now.
    Queued,
    Done {
        aborted: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Txn {
    stmt: u8,
    op: u8,
    phase: Phase,
    /// Row-op base: the snapshot `S`, or the EPQ-evaluated version's ts (§5.1/§5.2).
    base: Ts,
    epq_v: Option<(Ts, VerData)>,
    /// All versions the KEY SHARE EPQ must examine (§5.2); the newest first.
    epq_n: Vec<(Ts, VerData)>,
    epq_count: u8,
    /// `wait_for` context: (target, gen) pairs.
    wait: Option<Vec<(u8, u8)>>,
    wait_kind: EdgeKind,
    wait_cc: bool,
    /// Conflicting shared holders read under the latch (SecRead -> SecGen).
    holders: Vec<u8>,
    seq: u8,
    seq0: u8,
    /// Ghost: net effect of the surviving statements (frozen at commit).
    expected: [Option<ED>; NKEYS],
    sp: Vec<(u8, [Option<ED>; NKEYS])>,
    /// Ghost layer log: every layer change, bug-free (§5.5's log done right).
    glog: Vec<(u8, LKey)>,
    /// The modelled write-set log (§5.5); seed 50 records only new intents here.
    wlog: Vec<(u8, LKey)>,
    /// Ghost: txns whose committed effects this txn observed (I-RC-MONO).
    observing: u8,
    spin: u8,
    snap: Option<Ts>,
    cancel: bool,
    /// Ops of the current statement whose qual did not match at the statement's scan
    /// (the row is invisible to the statement; the op is skipped).
    skip: u8,
}

impl Txn {
    fn fresh() -> Txn {
        Txn {
            stmt: 0,
            op: 0,
            phase: Phase::Snap,
            base: 0,
            epq_v: None,
            epq_n: Vec::new(),
            epq_count: 0,
            wait: None,
            wait_kind: EdgeKind::Key(LKey::T0, Lock::None),
            wait_cc: false,
            holders: Vec::new(),
            seq: 1,
            seq0: 0,
            expected: [None; NKEYS],
            sp: Vec::new(),
            glog: Vec::new(),
            wlog: Vec::new(),
            observing: 0,
            spin: 0,
            snap: None,
            cancel: false,
            skip: 0,
        }
    }

    fn done() -> Txn {
        Txn {
            phase: Phase::Done { aborted: false },
            ..Txn::fresh()
        }
    }

    fn in_section(&self) -> bool {
        matches!(
            self.phase,
            Phase::SecGen | Phase::SecPlace | Phase::SecGrant | Phase::DefLook
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum St {
    Pending,
    Committed(Ts),
    Aborted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct TxnEntry {
    st: St,
    released: bool,
    count: i8,
    last_removal: u32,
    gen: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Req {
    w: u8,
    ts: Ts,
    status: bool,
    visible: bool,
    done: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct SharedLock {
    key: LKey,
    txn: u8,
    seq: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum EdgeKind {
    Key(LKey, Lock),
    /// Relation wait; the flag is the waiter's exclusive request.
    Rel(bool),
}

/// I-PROGRESS accounting for one actor step: the step completed work outside the
/// retry loop (reset), continued the loop without retrying (keep), or retried
/// without parking (bump).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Spin {
    Done,
    Mid,
    Retry,
}

/// A deferrable `/i/` entry as read by `DefScan` under the prefix latch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum EntryObs {
    /// No intent; liveness is re-derived from committed state at `DefLook`.
    None,
    Intent {
        owner: u8,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    wl: u8,
    kv: Kv,
    status: BTreeMap<u8, TxnEntry>,
    txns: [Txn; NTXNS],
    channel: Vec<u8>,
    group: Vec<Req>,
    next_ts: Ts,
    visible_ts: Ts,
    resolve_q: Vec<(u8, LKey)>,
    cleanup_q: Vec<(u8, LKey)>,
    shared: Vec<SharedLock>,
    /// Relation locks: (relation, txn, exclusive). One relation in scope.
    rels: Vec<(u8, u8, bool)>,
    edges: Vec<(u8, u8, EdgeKind)>,
    /// The latches held, and by which txn each (§5.0: striped over keys; a thread
    /// never holds two, but different txns hold different keys' latches).
    latch: Vec<(LatchKey, u8)>,
    /// Entries read by `DefScan`, indexed over `DEF_KEYS`.
    def_scan: [EntryObs; 2],
    view_counter: u32,
    /// Ghost: (commit ts, txn, net writes) in ts order; txn 255 = preload.
    commits: Commits,
    /// Ghost: (cycle members, mask of members that raised 40P01 at that
    /// detection) — I-LIVE(b): exactly one per cycle (§6).
    dlk_cycles: Vec<(Vec<u8>, u8)>,
    bad: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    Choose(u8),
    Snap(u8),
    SecRead(u8),
    SecGen(u8),
    SecPlace(u8),
    SecGrant(u8),
    WaitEnter(u8),
    EpqStep(u8),
    FkRead(u8),
    EndStmt(u8),
    Savepoint(u8),
    RollbackTo(u8),
    Request(u8),
    Drain(u8),
    SetStatus(u8),
    Advance(u8),
    RelShared(u8, LKey),
    Wake5(u8),
    Resolve(u8, LKey),
    Cleanup(u8, LKey),
    Truncate(u8),
    Cancel(u8),
    SelfAbort(u8),
    DeadlockCheck(u8),
    DefScan(u8),
    DefLook(u8),
}

pub struct WriteModel {
    pub bug: Option<Bug>,
    pub mutant: Option<Mutant>,
}

fn req_mode(op: Op) -> Lock {
    match op {
        Op::Write { kc, .. } => {
            if kc {
                Lock::Update
            } else {
                Lock::NoKeyUpd
            }
        }
        Op::Delete { .. } => Lock::Update,
        Op::KeyExist { .. } => Lock::NoKeyUpd,
        Op::LockOnly { upd, .. } => {
            if upd {
                Lock::Update
            } else {
                Lock::NoKeyUpd
            }
        }
        Op::KeyShare { .. } => Lock::KeyShare,
        _ => Lock::None,
    }
}

fn op_key(op: Op) -> Option<LKey> {
    match op {
        Op::Write { key, .. }
        | Op::Delete { key }
        | Op::KeyExist { key, .. }
        | Op::LockOnly { key, .. }
        | Op::KeyShare { key, .. }
        | Op::FkRead { key } => Some(key),
        _ => None,
    }
}

fn is_key_exist(op: Op) -> bool {
    matches!(op, Op::KeyExist { .. })
}

fn is_data_row_op(op: Op) -> bool {
    matches!(op, Op::Write { .. } | Op::Delete { .. })
}

fn is_defer_op(op: Op) -> bool {
    matches!(
        op,
        Op::KeyExist {
            kind: KeyKind::Def { .. },
            ..
        }
    )
}

impl State {
    fn ntxns(&self) -> usize {
        WORKLOADS[self.wl as usize].txns.len()
    }

    fn cur_op(&self, w: u8) -> Op {
        let t = &self.txns[w as usize];
        WORKLOADS[self.wl as usize].txns[w as usize].stmts[t.stmt as usize].ops[t.op as usize]
    }

    fn iso(&self, w: u8) -> Iso {
        WORKLOADS[self.wl as usize].txns[w as usize].iso
    }

    fn slot(&self, k: LKey) -> &Slot {
        static EMPTY: Slot = Slot {
            intent: None,
            vers: Vec::new(),
        };
        self.kv.get(&k).unwrap_or(&EMPTY)
    }

    /// The key's current "committed" state: the newest version, or a committed-visible
    /// intent's top data as a pseudo-version (it committed; resolution is pending).
    fn committed_state(&self, k: LKey) -> Option<(Ts, VerData)> {
        if let Some(i) = &self.slot(k).intent {
            if let Some(e) = self.status.get(&i.owner) {
                if let St::Committed(c) = e.st {
                    if c <= self.visible_ts {
                        if let Some(v) = ver_of(top_layer(i).data) {
                            return Some((c, v));
                        }
                    }
                }
            }
        }
        self.slot(k).vers.first().copied()
    }

    /// A txn still blocks others while Pending or a not-yet-visible commit (§3.2);
    /// ended holders never conflict (§6).
    fn holds_locks(&self, t: u8) -> bool {
        match self.status.get(&t) {
            None => false,
            Some(e) => match e.st {
                St::Pending => true,
                St::Committed(c) => c > self.visible_ts,
                St::Aborted => false,
            },
        }
    }

    /// States inside the commit thread's step-4/5 sequence for `t` are exempt from
    /// I-LIVE (§11: "states inside that sequence are exempt").
    fn in_step5_window(&self, t: u8) -> bool {
        self.group.iter().any(|r| r.w == t && r.visible && !r.done)
    }

    fn min_view(&self) -> u32 {
        // Views open and close inside one step here (see module doc), so no view
        // counter is registered across states.
        u32::MAX
    }

    /// §4 read as an external observer at snapshot `ts`.
    fn read_at(&self, k: LKey, ts: Ts) -> Option<u8> {
        let slot = self.slot(k);
        if let Some(i) = &slot.intent {
            if let Some(e) = self.status.get(&i.owner) {
                if let St::Committed(c) = e.st {
                    if c <= ts {
                        return match top_layer(i).data {
                            Data::Write { val, .. } => Some(val),
                            Data::Delete => None,
                            Data::Absent => match self.vers_read(slot, ts) {
                                Some(VerData::Live { val, .. }) => Some(val),
                                _ => None,
                            },
                        };
                    }
                }
            }
        }
        match self.vers_read(slot, ts) {
            Some(VerData::Live { val, .. }) => Some(val),
            _ => None,
        }
    }

    /// §4 read inside txn `w`: the newest own layer with `seq < seq0`, else the
    /// committed state at `ts` (I-HALLOWEEN: a statement never sees `seq >= seq0`).
    fn read_own(&self, k: LKey, ts: Ts, w: u8, seq0: u8) -> Option<u8> {
        if let Some(i) = &self.slot(k).intent {
            if i.owner == w {
                if let Some(l) = i.layers.iter().rev().find(|l| l.seq < seq0) {
                    match l.data {
                        // §4: an own `Absent` layer is lock-only; reads fall through.
                        Data::Write { val, .. } => return Some(val),
                        Data::Delete => return None,
                        Data::Absent => {}
                    }
                }
            }
        }
        self.read_at(k, ts)
    }

    fn vers_read(&self, slot: &Slot, ts: Ts) -> Option<VerData> {
        slot.vers.iter().find(|(t, _)| *t <= ts).map(|(_, v)| *v)
    }

    fn intents_of(&self, t: u8) -> Vec<LKey> {
        let mut v = Vec::new();
        for k in KEYS {
            if matches!(&self.slot(k).intent, Some(i) if i.owner == t) {
                v.push(k);
            }
        }
        v
    }

    /// Find a wait-for cycle through `w` (up to 3 txns), returning its edges.
    fn cycle_through(&self, w: u8) -> Option<Vec<(u8, u8, EdgeKind)>> {
        for e1 in &self.edges {
            if e1.0 != w {
                continue;
            }
            if e1.1 == w {
                return Some(vec![*e1]);
            }
            for e2 in &self.edges {
                if e2.0 != e1.1 {
                    continue;
                }
                if e2.1 == w {
                    return Some(vec![*e1, *e2]);
                }
                for e3 in &self.edges {
                    if e3.0 == e2.1 && e3.1 == w {
                        return Some(vec![*e1, *e2, *e3]);
                    }
                }
            }
        }
        None
    }

    /// Is the edge (x -> t, kind) a current conflict (I-LIVE)?
    fn edge_current(&self, x: u8, t: u8, kind: EdgeKind) -> bool {
        if matches!(self.txns[x as usize].phase, Phase::Done { .. }) {
            return false;
        }
        let st = match self.status.get(&t) {
            None => return false,
            Some(e) => e.st,
        };
        match st {
            St::Aborted => return false,
            St::Committed(c) if c <= self.visible_ts => return false,
            _ => {}
        }
        match kind {
            EdgeKind::Key(k, m) => {
                if let Some(i) = &self.slot(k).intent {
                    if i.owner == t && conflicts(m, top_layer(i).lock) {
                        return true;
                    }
                }
                !self.shared.is_empty()
                    && self
                        .shared
                        .iter()
                        .any(|s| s.key == k && s.txn == t && conflicts(m, Lock::KeyShare))
            }
            EdgeKind::Rel(req_excl) => self
                .rels
                .iter()
                .any(|(_, t2, ex)| *t2 == t && (req_excl || *ex)),
        }
    }

    /// Canonical form for fingerprint deduplication.
    fn normalize(&mut self) {
        let mut vals: Vec<u32> = vec![0, self.view_counter];
        vals.extend(self.status.values().map(|t| t.last_removal));
        vals.sort_unstable();
        vals.dedup();
        let rank = |v: u32| match vals.binary_search(&v) {
            Ok(i) => i as u32,
            Err(_) => 0,
        };
        self.view_counter = rank(self.view_counter);
        for t in self.status.values_mut() {
            t.last_removal = rank(t.last_removal);
        }
        self.edges.sort_unstable();
        self.edges.dedup();
        self.shared.sort_unstable();
        self.shared.dedup();
        self.rels.sort_unstable();
        self.rels.dedup();
        self.resolve_q.sort_unstable();
        self.resolve_q.dedup();
        self.cleanup_q.sort_unstable();
        self.cleanup_q.dedup();
        self.latch.sort_unstable();
        self.latch.dedup();
    }
}

impl WriteModel {
    // ---- shared protocol helpers. The bug changes guards and inputs inside the
    // step it names; every check and oracle below runs the same in clean and buggy
    // runs and never reads `self.bug`. ----

    /// The latch a removal of `k` must take (§5.0); seed 25 latches the entry key of
    /// a deferrable `/i/` entry instead of the prefix.
    fn removal_lk(&self, k: LKey) -> LatchKey {
        if self.bug == Some(Bug::DeferrableEntryLatch) {
            LatchKey::Key(k)
        } else {
            lk_of(k)
        }
    }

    /// §7.3 for the intent on `k`, expected to be owned by `t`. `check_owner` false is
    /// seed 11: act without re-reading the owner under the latch.
    fn remove_intent(&self, s: &mut State, t: u8, k: LKey, check_owner: bool) {
        let cur = s.kv.get(&k).and_then(|slot| slot.intent.clone());
        let acts = match &cur {
            Some(i) => i.owner == t || !check_owner,
            None => false,
        };
        if !acts {
            // Gone or re-owned: writes nothing, changes no count (§7.3 step 2).
            return;
        }
        let owner = cur.as_ref().map(|i| i.owner).unwrap_or(t);
        let data = cur
            .as_ref()
            .map(|i| top_layer(i).data)
            .unwrap_or(Data::Absent);
        let st = s.status.get(&t).map(|e| e.st).unwrap_or(St::Pending);
        let slot = s.kv.entry(k).or_default();
        slot.intent = None;
        if owner == t {
            if let St::Committed(c) = st {
                if let Some(v) = ver_of(data) {
                    slot.vers.push((c, v));
                    slot.vers.sort_by_key(|(t, _)| std::cmp::Reverse(*t));
                }
            }
        }
        let vc = s.view_counter;
        if let Some(e) = s.status.get_mut(&t) {
            e.last_removal = vc;
            e.count -= 1;
        }
    }

    /// Wake every waiter on `t`: bump the generation, remove their edges (unless seed
    /// 59 left them), resume parked ones.
    fn wake(&self, s: &mut State, t: u8) {
        if let Some(e) = s.status.get_mut(&t) {
            e.gen += 1;
        }
        let committed = matches!(s.status.get(&t).map(|e| e.st), Some(St::Committed(_)));
        let mut resume: Vec<u8> = Vec::new();
        if self.bug == Some(Bug::WakerLeavesEdges) {
            for (x, t2, _) in &s.edges {
                if *t2 == t {
                    resume.push(*x);
                }
            }
        } else {
            let mut i = 0;
            while i < s.edges.len() {
                if s.edges[i].1 == t {
                    let e = s.edges.remove(i);
                    resume.push(e.0);
                } else {
                    i += 1;
                }
            }
        }
        resume.sort_unstable();
        resume.dedup();
        for x in resume {
            if committed {
                s.txns[x as usize].observing |= 1 << t;
            }
            let cc = matches!(s.txns[x as usize].phase, Phase::Parked { cc: true });
            if matches!(s.txns[x as usize].phase, Phase::Parked { .. }) {
                s.txns[x as usize].phase = if cc { Phase::CommitCheck } else { Phase::Op };
            }
        }
    }

    /// Release `t`'s shared row locks with seq >= `min_seq` (§6), waking waiters.
    /// Whether the release must hold the key's latch is decided by the action guard;
    /// by the time this runs the release is happening.
    fn release_shared(&self, s: &mut State, t: u8, min_seq: u8) -> bool {
        let before = s.shared.len();
        s.shared.retain(|sl| !(sl.txn == t && sl.seq >= min_seq));
        let released = before != s.shared.len();
        if released {
            self.wake(s, t);
        }
        released
    }

    /// §7.1 fused abort: status, release, wake, queue cleanup.
    fn do_fail(&self, s: &mut State, w: u8) {
        if let Some(e) = s.status.get_mut(&w) {
            e.st = St::Aborted;
            e.released = true;
        }
        self.release_shared(s, w, 0);
        s.rels.retain(|(_, t, _)| *t != w);
        if self.mutant != Some(Mutant::AbortCleanupNotQueued) {
            for k in s.intents_of(w) {
                s.cleanup_q.push((w, k));
            }
        }
        self.wake(s, w);
        s.txns[w as usize].phase = Phase::Done { aborted: true };
    }

    /// The RMW value `v = v + 1` computed from the row the protocol would use: the
    /// own layer below `seq0`, else the committed state at `base` (§5.1/§5.2).
    fn rmw_val(&self, s: &State, w: u8, key: LKey) -> u8 {
        let txn = &s.txns[w as usize];
        s.read_own(key, txn.base, w, txn.seq0)
            .map_or(1, |v| v.saturating_add(1))
    }

    /// Build and write the new top layer (§2.1), update the logs and the count, and
    /// record the ghost net effect. `SecPlace` runs this under the held latch (seed 4:
    /// after releasing it); the seed-12 and seed-47 paths run it fused.
    fn do_place(&self, s: &mut State, w: u8) {
        let op = s.cur_op(w);
        let key = op_key(op).unwrap_or(LKey::T0);
        let seq0 = s.txns[w as usize].seq0;
        let prev = s.slot(key).intent.clone();
        let own = matches!(&prev, Some(i) if i.owner == w);
        let mut layers = match &prev {
            Some(i) if i.owner == w => i.layers.clone(),
            _ => Vec::new(),
        };
        let kc_sticky = layers
            .iter()
            .any(|l| matches!(l.data, Data::Write { kc: true, .. }));
        let (data, dseq) = match op {
            Op::Write { kc, .. } => {
                let val = self.rmw_val(s, w, key);
                (
                    Data::Write {
                        val,
                        kc: kc || kc_sticky,
                    },
                    seq0,
                )
            }
            Op::Delete { .. } => (Data::Delete, seq0),
            Op::KeyExist { .. } => (Data::Write { val: 1, kc: false }, seq0),
            Op::LockOnly { .. } => {
                if let Some(top) = layers.last().copied() {
                    if self.bug != Some(Bug::LockOnlyReplacesData) {
                        (top.data, top.dseq)
                    } else {
                        // Seed 15: the lock-only layer replaces own data.
                        (Data::Absent, 0)
                    }
                } else {
                    // First lock-only layer on the key.
                    (Data::Absent, 0)
                }
            }
            _ => (Data::Absent, 0),
        };
        let prev_lock = layers.last().map(|l| l.lock).unwrap_or(Lock::None);
        let mut lock = implied(data).max(prev_lock);
        let req = req_mode(op);
        if !matches!(req, Lock::None | Lock::KeyShare) {
            lock = lock.max(req);
        }
        let top_seq = layers.last().map(|l| l.seq).unwrap_or(0);
        let seq = seq0.max(top_seq);
        let layer = Layer {
            seq,
            dseq,
            data,
            lock,
        };
        if layers.last().map(|l| l.seq) == Some(seq) {
            let n = layers.len();
            layers[n - 1] = layer;
        } else {
            layers.push(layer);
        }
        let fresh = !own;
        s.kv.entry(key).or_default().intent = Some(Intent { owner: w, layers });
        // §5.1: log and count before the write; once per (seq, key).
        let glog = &mut s.txns[w as usize].glog;
        if !glog.contains(&(seq, key)) {
            glog.push((seq, key));
        }
        if fresh || self.bug != Some(Bug::LogOnlyNewIntents) {
            let wlog = &mut s.txns[w as usize].wlog;
            if !wlog.contains(&(seq, key)) {
                wlog.push((seq, key));
            }
        }
        if fresh {
            if let Some(e) = s.status.get_mut(&w) {
                e.count += 1;
            }
        }
        // Ghost net effect of the surviving statements.
        let exp = &mut s.txns[w as usize].expected;
        match op {
            Op::Write { .. } => {
                exp[key as usize] = match exp[key as usize] {
                    Some(ED::Inc { d }) => Some(ED::Inc {
                        d: d.saturating_add(1),
                    }),
                    Some(ED::Row { val, uval, child }) => Some(ED::Row {
                        val: val.saturating_add(1),
                        uval,
                        child,
                    }),
                    _ => Some(ED::Inc { d: 1 }),
                };
            }
            Op::Delete { .. } => exp[key as usize] = Some(ED::Dead),
            Op::KeyExist { kind, .. } => match kind {
                KeyKind::Pk { uval, child } => {
                    exp[key as usize] = Some(ED::Row {
                        val: 1,
                        uval,
                        child,
                    })
                }
                KeyKind::Unique { row } | KeyKind::Def { row } => {
                    exp[key as usize] = Some(ED::Entry { row })
                }
            },
            _ => {}
        }
        self.advance_op(s, w);
    }

    /// Record the shared lock (§5.1). The I-WW grant rule of §5.1 is checked here from
    /// the KV, independently of the branch that decided the grant.
    fn do_grant(&self, s: &mut State, w: u8) {
        let op = s.cur_op(w);
        let key = op_key(op).unwrap_or(LKey::T0);
        let seq0 = s.txns[w as usize].seq0;
        let snap = s.txns[w as usize].snap.unwrap_or(s.visible_ts);
        if matches!(op, Op::KeyShare { .. }) {
            for (ts, v) in s.slot(key).vers.clone() {
                if ts > snap {
                    let bad = match v {
                        VerData::Tomb => true,
                        VerData::Live { kc, .. } => kc,
                    };
                    if bad {
                        s.bad = Some(format!(
                            "I-WW: KEY SHARE on {key:?} granted over version @{ts} above S={snap}"
                        ));
                    }
                }
            }
        }
        s.shared.push(SharedLock {
            key,
            txn: w,
            seq: seq0,
        });
        self.advance_op(s, w);
    }

    /// Skip ops of statement `stmt` whose qual did not match at the statement's scan
    /// (§4: the row is invisible, so the statement never targets it).
    fn compute_skip(&self, s: &State, w: u8, stmt: u8) -> u8 {
        let t = &s.txns[w as usize];
        let stmt = &WORKLOADS[s.wl as usize].txns[w as usize].stmts[stmt as usize];
        let snap = t.snap.unwrap_or(s.visible_ts);
        let mut skip = 0u8;
        for (i, op) in stmt.ops.iter().enumerate() {
            if let Some(key) = op_key(*op) {
                let live = s.read_own(key, snap, w, t.seq0);
                let matched = match *op {
                    Op::Write { qual, .. } => {
                        live.is_some() && qual.is_none_or(|q| live == Some(q))
                    }
                    Op::Delete { .. } | Op::LockOnly { .. } | Op::KeyShare { .. } => live.is_some(),
                    _ => true,
                };
                if !matched {
                    skip |= 1 << i;
                }
            }
        }
        skip
    }

    fn advance_op(&self, s: &mut State, w: u8) {
        {
            let txn = &mut s.txns[w as usize];
            txn.op += 1;
            txn.base = txn.snap.unwrap_or(0);
            txn.epq_count = 0;
            txn.epq_v = None;
            txn.epq_n.clear();
            txn.wait = None;
            txn.wait_cc = false;
            txn.holders.clear();
        }
        self.scan_ready(s, w);
    }

    /// Position `op` on the first op of the current statement the scan targets
    /// (§4: a row invisible at S is not targeted, so the op is skipped at the
    /// statement scan, not at latch time), then dispatch to Op / EndS / the next
    /// statement.
    fn scan_ready(&self, s: &mut State, w: u8) {
        let stmts = WORKLOADS[s.wl as usize].txns[w as usize].stmts;
        let txn = &mut s.txns[w as usize];
        let nops = stmts[txn.stmt as usize].ops.len();
        while (txn.op as usize) < nops && txn.skip & (1 << txn.op) != 0 {
            txn.op += 1;
        }
        if (txn.op as usize) < nops {
            txn.phase = Phase::Op;
            return;
        }
        if stmts[txn.stmt as usize].parent_check {
            txn.phase = Phase::EndS;
            return;
        }
        self.advance_stmt(s, w);
    }

    fn advance_stmt(&self, s: &mut State, w: u8) {
        let iso = WORKLOADS[s.wl as usize].txns[w as usize].iso;
        let nstmts = WORKLOADS[s.wl as usize].txns[w as usize].stmts.len();
        let deferrable = WORKLOADS[s.wl as usize].deferrable;
        let next = s.txns[w as usize].stmt + 1;
        if (next as usize) < nstmts {
            if iso == Iso::Rc {
                let txn = &mut s.txns[w as usize];
                txn.stmt += 1;
                txn.op = 0;
                txn.skip = 0;
                txn.phase = Phase::Snap;
            } else {
                {
                    let txn = &mut s.txns[w as usize];
                    txn.stmt += 1;
                    txn.op = 0;
                    txn.seq0 = txn.seq;
                    txn.seq += 1;
                }
                // One snapshot per txn (§4); later statements re-derive their skip set.
                s.txns[w as usize].skip = self.compute_skip(s, w, next);
                self.scan_ready(s, w);
            }
            return;
        }
        let txn = &mut s.txns[w as usize];
        txn.stmt += 1;
        txn.op = 0;
        txn.skip = 0;
        txn.phase = if deferrable {
            Phase::CommitCheck
        } else {
            Phase::Chan
        };
    }

    /// A foreign intent that is ended (Aborted, or a visible commit, or a truncated
    /// owner): true when §5.1 must remove it and continue.
    fn foreign_ended(&self, s: &State, owner: u8) -> bool {
        match s.status.get(&owner).map(|e| e.st) {
            None => true,
            Some(St::Aborted) => true,
            Some(St::Pending) => false,
            Some(St::Committed(c)) => {
                self.bug == Some(Bug::ProceedBeforeVisible) || c <= s.visible_ts
            }
        }
    }

    // ---- the §5.1 latch section ----

    /// One §5.1 iteration for `w`'s current op: take `latch_key(k)`,
    /// run the foreign-intent block, read the shared-lock table, and decide.
    fn do_sec_read(&self, s: &mut State, w: u8) -> Spin {
        let op = s.cur_op(w);
        if let Op::RelLock { excl } = op {
            return self.do_rel_lock(s, w, excl);
        }
        let key = op_key(op).unwrap_or(LKey::T0);
        let lk = lk_of(key);
        s.latch.push((lk, w));
        let m = req_mode(op);
        // 1. Foreign intent (§5.1).
        let foreign = s.slot(key).intent.clone();
        if let Some(i) = &foreign {
            if i.owner != w {
                let owner = i.owner;
                let top = top_layer(i);
                if self.foreign_ended(s, owner) {
                    if self.bug == Some(Bug::OverwriteForeignEnded) {
                        // Seed 12: place without removing first.
                        unlatch(s, w);
                        self.do_place(s, w);
                        return Spin::Done;
                    }
                    let committed_visible = matches!(
                        s.status.get(&owner).map(|e| e.st),
                        Some(St::Committed(c)) if c <= s.visible_ts
                    );
                    self.remove_intent(s, owner, key, true);
                    if committed_visible {
                        s.txns[w as usize].observing |= 1 << owner;
                    }
                    unlatch(s, w);
                    return Spin::Retry; // `continue`
                }
                // R3W-8: no wait on a lock-only intent over a live row.
                if is_key_exist(op)
                    && top.data == Data::Absent
                    && matches!(s.committed_state(key), Some((_, VerData::Live { .. })))
                {
                    unlatch(s, w);
                    self.do_fail(s, w); // 23505
                    return Spin::Done;
                }
                if conflicts(m, top.lock) {
                    let g = s.status.get(&owner).map(|e| e.gen).unwrap_or(0);
                    s.txns[w as usize].wait = Some(vec![(owner, g)]);
                    s.txns[w as usize].wait_kind = EdgeKind::Key(key, m);
                    s.txns[w as usize].wait_cc = false;
                    s.txns[w as usize].phase = Phase::WaitReg;
                    unlatch(s, w);
                    return Spin::Mid;
                }
                // Non-conflicting foreign intent (e.g. KEY SHARE vs NO KEY UPDATE).
                if self.bug == Some(Bug::EpqRepeatOnIntent)
                    && s.txns[w as usize].epq_count >= 1
                    && matches!(op, Op::KeyShare { .. })
                {
                    // Seed 52: repeat EPQ instead of proceeding.
                    let snap = s.txns[w as usize].snap.unwrap_or(s.visible_ts);
                    let vers: Vec<(Ts, VerData)> = s
                        .slot(key)
                        .vers
                        .iter()
                        .filter(|(t, _)| *t > snap)
                        .copied()
                        .collect();
                    s.txns[w as usize].epq_v = vers.first().copied();
                    s.txns[w as usize].epq_n = vers;
                    s.txns[w as usize].epq_count += 1;
                    s.txns[w as usize].phase = Phase::Epq;
                    unlatch(s, w);
                    return Spin::Retry;
                }
            }
        }
        // 2. Conflicting shared holders (§5.1): read the table here, the generations
        // in `SecGen`, both under this latch.
        let hs: Vec<u8> = s
            .shared
            .iter()
            .filter(|sl| sl.key == key && sl.txn != w && conflicts(m, Lock::KeyShare))
            .filter(|sl| s.holds_locks(sl.txn))
            .map(|sl| sl.txn)
            .collect();
        if !hs.is_empty() {
            s.txns[w as usize].holders = hs;
            s.txns[w as usize].phase = Phase::SecGen;
            return Spin::Mid; // the latch stays held
        }
        // 3. Decide (unique check / §5.4 / newer-version rule) and dispatch.
        self.sec_decide(s, w)
    }

    fn sec_decide(&self, s: &mut State, w: u8) -> Spin {
        let op = s.cur_op(w);
        let key = op_key(op).unwrap_or(LKey::T0);
        let seq0 = s.txns[w as usize].seq0;
        let base = s.txns[w as usize].base;
        let iso = s.iso(w);
        let own = match &s.slot(key).intent {
            Some(i) if i.owner == w => Some(i.layers.clone()),
            _ => None,
        };
        // §5.4 own-row rules.
        if let Some(layers) = &own {
            let top = top_layer(&Intent {
                owner: w,
                layers: layers.clone(),
            });
            if is_data_row_op(op) {
                if top.dseq == seq0 {
                    // Revisit in the same statement: skip the row.
                    unlatch(s, w);
                    self.advance_op(s, w);
                    return Spin::Done;
                }
                if top.dseq > seq0 {
                    unlatch(s, w);
                    self.do_fail(s, w); // 27000
                    return Spin::Done;
                }
                return self.dispatch_place(s, w);
            }
            if !is_key_exist(op) {
                // A lock-only request on the own intent builds a new layer.
                return self.dispatch_place(s, w);
            }
            // A key-existence op always runs the unique check, own intent or not.
        }
        if !is_key_exist(op) {
            // 4. Newer-version rule (row ops and KEY SHARE; key-existence ops are
            // governed by §5.3).
            let n: Vec<(Ts, VerData)> = s
                .slot(key)
                .vers
                .iter()
                .filter(|(t, _)| *t > base)
                .copied()
                .collect();
            if !n.is_empty() {
                if iso == Iso::Rr {
                    let examined: Vec<(Ts, VerData)> = if self.bug == Some(Bug::KeyShareNewestOnly)
                        && matches!(op, Op::KeyShare { .. })
                    {
                        vec![n[0]]
                    } else {
                        n.clone()
                    };
                    let bad = examined.iter().any(|(_, v)| match v {
                        VerData::Tomb => true,
                        VerData::Live { kc, .. } => *kc,
                    });
                    if bad {
                        unlatch(s, w);
                        self.do_fail(s, w); // 40001
                        return Spin::Done;
                    }
                    // KEY SHARE may proceed over plain newer writes.
                } else {
                    // RC: EPQ (§5.2), unlatched. The mutant skips it for updates.
                    if self.mutant == Some(Mutant::RcUpdateSkipsEpq) && is_data_row_op(op) {
                        return self.dispatch_place(s, w);
                    }
                    let snap = s.txns[w as usize].snap.unwrap_or(s.visible_ts);
                    let vers: Vec<(Ts, VerData)> = if matches!(op, Op::KeyShare { .. }) {
                        // §5.2: KEY SHARE examines every version above S.
                        if self.bug == Some(Bug::KeyShareNewestOnly) {
                            vec![n[0]]
                        } else {
                            s.slot(key)
                                .vers
                                .iter()
                                .filter(|(t, _)| *t > snap)
                                .copied()
                                .collect()
                        }
                    } else {
                        vec![n[0]]
                    };
                    let repeat = s.txns[w as usize].epq_count >= 1;
                    s.txns[w as usize].epq_v = vers.first().copied();
                    s.txns[w as usize].epq_n = vers;
                    s.txns[w as usize].epq_count += 1;
                    s.txns[w as usize].phase = Phase::Epq;
                    unlatch(s, w);
                    return if repeat { Spin::Retry } else { Spin::Mid };
                }
            }
        } else if !is_defer_op(op) {
            // 4b. Unique check (§5.3): the key's current state.
            let own_top: Option<Data> = own.as_ref().map(|ls| {
                top_layer(&Intent {
                    owner: w,
                    layers: ls.clone(),
                })
                .data
            });
            let committed_live = matches!(s.committed_state(key), Some((_, VerData::Live { .. })));
            let live = if self.bug == Some(Bug::UniqueIgnoresOwn) {
                committed_live
            } else if own_top == Some(Data::Absent) && self.bug == Some(Bug::OwnAbsentNotLive) {
                false
            } else {
                match own_top {
                    Some(Data::Write { .. }) => true,
                    Some(Data::Delete) | Some(Data::Absent) | None => committed_live,
                }
            };
            if live {
                unlatch(s, w);
                self.do_fail(s, w); // 23505
                return Spin::Done;
            }
        }
        if matches!(op, Op::KeyShare { .. }) {
            self.dispatch_grant(s, w)
        } else {
            self.dispatch_place(s, w)
        }
    }

    /// Place under the held latch (`SecPlace`). Seed 4 releases the latch before the
    /// placement, so another txn's latch section can run between this section's read
    /// and its write.
    fn dispatch_place(&self, s: &mut State, w: u8) -> Spin {
        if self.bug == Some(Bug::PlaceWithoutLatch) {
            unlatch(s, w);
        }
        s.txns[w as usize].phase = Phase::SecPlace;
        Spin::Mid
    }

    fn dispatch_grant(&self, s: &mut State, w: u8) -> Spin {
        s.txns[w as usize].phase = Phase::SecGrant;
        Spin::Mid
    }

    fn do_rel_lock(&self, s: &mut State, w: u8, excl: bool) -> Spin {
        let hs: Vec<u8> = s
            .rels
            .iter()
            .filter(|(_, t, ex)| *t != w && (excl || *ex) && s.holds_locks(*t))
            .map(|(_, t, _)| *t)
            .collect();
        if !hs.is_empty() {
            let targets: Vec<(u8, u8)> = hs
                .iter()
                .map(|t| (*t, s.status.get(t).map(|e| e.gen).unwrap_or(0)))
                .collect();
            s.txns[w as usize].wait = Some(targets);
            s.txns[w as usize].wait_kind = EdgeKind::Rel(excl);
            s.txns[w as usize].wait_cc = false;
            s.txns[w as usize].phase = Phase::WaitReg;
            return Spin::Mid;
        }
        s.rels.push((0, w, excl));
        self.advance_op(s, w);
        Spin::Done
    }

    /// §5.1: read the remembered holders' wake generations under this latch, then
    /// unlatch and wait.
    fn do_sec_gen(&self, s: &mut State, w: u8) -> Spin {
        let op = s.cur_op(w);
        let key = op_key(op).unwrap_or(LKey::T0);
        let m = req_mode(op);
        let targets: Vec<(u8, u8)> = s.txns[w as usize]
            .holders
            .iter()
            .map(|t| (*t, s.status.get(t).map(|e| e.gen).unwrap_or(0)))
            .collect();
        s.txns[w as usize].wait = Some(targets);
        s.txns[w as usize].wait_kind = EdgeKind::Key(key, m);
        s.txns[w as usize].wait_cc = false;
        s.txns[w as usize].holders.clear();
        s.txns[w as usize].phase = Phase::WaitReg;
        unlatch(s, w);
        Spin::Mid
    }

    /// §5.2 EPQ: re-evaluate the quals against the remembered version(s), unlatched.
    fn do_epq_step(&self, s: &mut State, w: u8) -> Spin {
        let v = s.txns[w as usize].epq_v.unwrap_or((0, VerData::Tomb));
        let n = s.txns[w as usize].epq_n.clone();
        let op = s.cur_op(w);
        let pass = match op {
            Op::KeyShare { .. } => {
                // §5.1/§5.2: KEY SHARE examines every version above S.
                n.iter().all(|(_, vd)| match vd {
                    VerData::Tomb => false,
                    VerData::Live { kc, .. } => !*kc,
                })
            }
            Op::Write { qual, .. } => match v.1 {
                VerData::Live { val, .. } => qual.is_none_or(|q| q == val),
                VerData::Tomb => self.mutant == Some(Mutant::EpqPassesTombstone),
            },
            Op::Delete { .. } => matches!(v.1, VerData::Live { .. }),
            _ => true,
        };
        if !pass {
            if matches!(op, Op::KeyShare { fk: true, .. }) {
                // §5.3: an FK EPQ failure raises 23503, never skips.
                self.do_fail(s, w);
            } else {
                self.advance_op(s, w); // skip the row
            }
            return Spin::Done;
        }
        s.txns[w as usize].base = v.0;
        if self.bug == Some(Bug::EpqPlaceNoReverify) && is_data_row_op(op) {
            // Seed 47: place without re-verifying under the latch.
            self.do_place(s, w);
            return Spin::Done;
        }
        s.txns[w as usize].phase = Phase::Op; // re-enter §5.1 (a new latch section)
        Spin::Mid
    }

    /// FK child-side read of the parent (§5.3) from a registered view.
    fn do_fk_read(&self, s: &mut State, w: u8) {
        let op = s.cur_op(w);
        let key = op_key(op).unwrap_or(LKey::T0);
        s.view_counter += 1;
        let snap = s.txns[w as usize].snap.unwrap_or(s.visible_ts);
        let seq0 = s.txns[w as usize].seq0;
        let val = s.read_own(key, snap, w, seq0);
        let want = self.oracle_val(s, key, snap);
        if val != want {
            s.bad = Some(format!(
                "I-HALLOWEEN/I-ATOMIC: W{w} FK read {key:?} at S={snap}: got {val:?}, committed state says {want:?}"
            ));
        }
        if val.is_none() {
            self.do_fail(s, w); // 23503
            return;
        }
        self.advance_op(s, w);
    }

    /// The oracle value of `k` at `ts` (committed net effects only).
    fn oracle_val(&self, s: &State, k: LKey, ts: Ts) -> Option<u8> {
        let mut live: Option<u8> = None;
        for (t, _, map) in &s.commits {
            if *t > ts {
                continue;
            }
            if let Some((_, ed)) = map.iter().find(|(kk, _)| *kk == k) {
                match ed {
                    ED::Row { val, .. } => live = Some(*val),
                    ED::Inc { d } => {
                        live = live.map(|v| v.saturating_add(*d));
                    }
                    ED::Dead => live = None,
                    ED::Entry { .. } => {}
                }
            }
        }
        live
    }

    /// End-of-statement FK parent-side check (§5.3): scan the child index in the
    /// latest committed state plus own writes.
    fn do_end_stmt(&self, s: &mut State, w: u8) {
        s.view_counter += 1;
        let child_live = CHILD_KEYS.iter().any(|c| {
            if let Some(i) = &s.slot(*c).intent {
                if i.owner == w {
                    return matches!(top_layer(i).data, Data::Write { .. });
                }
                return match s.status.get(&i.owner).map(|e| e.st) {
                    Some(St::Committed(c)) => {
                        c <= s.visible_ts && matches!(top_layer(i).data, Data::Write { .. })
                    }
                    _ => false,
                };
            }
            matches!(s.committed_state(*c), Some((_, VerData::Live { .. })))
        });
        if child_live {
            // NO ACTION: look for a live parent with the old key in the same state.
            let parent_live = match &s.slot(LKey::T0).intent {
                Some(i) if i.owner == w => matches!(top_layer(i).data, Data::Write { .. }),
                _ => matches!(s.committed_state(LKey::T0), Some((_, VerData::Live { .. }))),
            };
            if !parent_live {
                self.do_fail(s, w); // 23503
                return;
            }
        }
        self.advance_stmt(s, w);
    }

    /// §5.5 `ROLLBACK TO SAVEPOINT`: drop layers with seq >= s on every key the
    /// write-set log names, release post-savepoint shared locks, restore the ghost
    /// expected state, and wake all waiters.
    fn do_rollback(&self, s: &mut State, w: u8) {
        let sp = s.txns[w as usize].sp.pop();
        if let Some((sp_seq, exp)) = sp {
            // Keys written at seq >= sp_seq, per the write-set log (seed 50's bug:
            // layer changes on existing intents are missing from it).
            let keys: Vec<LKey> = s.txns[w as usize]
                .wlog
                .iter()
                .filter(|(q, _)| *q >= sp_seq)
                .map(|(_, k)| *k)
                .collect();
            for k in keys {
                let layers = s
                    .slot(k)
                    .intent
                    .clone()
                    .map(|i| i.layers)
                    .unwrap_or_default();
                let kept: Vec<Layer> = layers.iter().copied().filter(|l| l.seq < sp_seq).collect();
                if kept.is_empty() {
                    self.remove_intent(s, w, k, true);
                } else {
                    let mut kept = kept;
                    if self.bug == Some(Bug::RollbackDropsLock) {
                        // Seed 16: the restored top layer loses its lock.
                        let n = kept.len();
                        kept[n - 1].lock = Lock::None;
                    }
                    s.kv.entry(k).or_default().intent = Some(Intent {
                        owner: w,
                        layers: kept,
                    });
                }
            }
            s.txns[w as usize].wlog.retain(|(q, _)| *q < sp_seq);
            s.txns[w as usize].glog.retain(|(q, _)| *q < sp_seq);
            self.release_shared(s, w, sp_seq);
            s.txns[w as usize].expected = exp;
            if self.bug != Some(Bug::RollbackNoWake) {
                self.wake(s, w);
            }
        }
        self.advance_op(s, w);
    }

    /// §6 `wait_for`: register the edges, re-check, park or return.
    fn do_wait_enter(&self, s: &mut State, w: u8) -> Spin {
        let targets = s.txns[w as usize].wait.clone().unwrap_or_default();
        let cc = s.txns[w as usize].wait_cc;
        let kind = s.txns[w as usize].wait_kind;
        for (t, _) in &targets {
            let e = (w, *t, kind);
            if !s.edges.contains(&e) {
                s.edges.push(e);
            }
        }
        let returns = |s: &State| -> bool {
            if s.txns[w as usize].cancel {
                return true;
            }
            targets.iter().any(|(t, g)| {
                match s.status.get(t).map(|e| (e.st, e.gen)) {
                    // §4/§6: a missing status means ended and released.
                    None => self.bug != Some(Bug::MissingStatusPending),
                    Some((st, g2)) => {
                        let ended = match st {
                            St::Aborted => true,
                            St::Committed(c) => c <= s.visible_ts,
                            St::Pending => false,
                        };
                        ended || g2 != *g
                    }
                }
            })
        };
        let ret = if self.bug == Some(Bug::NoGenRecheck) {
            s.txns[w as usize].cancel
        } else {
            returns(s)
        };
        if ret {
            // I-RC-MONO: a wait that observed a visible commit saw its effects.
            for (t, _) in &targets {
                if let Some(e) = s.status.get(t) {
                    if let St::Committed(c) = e.st {
                        if c <= s.visible_ts {
                            s.txns[w as usize].observing |= 1 << t;
                        }
                    }
                }
            }
            s.edges
                .retain(|(x, t2, _)| !(*x == w && targets.iter().any(|(t, _)| *t == *t2)));
            s.txns[w as usize].phase = if cc { Phase::CommitCheck } else { Phase::Op };
            s.txns[w as usize].wait = None;
            s.txns[w as usize].wait_cc = false;
            return Spin::Retry; // returned without parking
        }
        s.txns[w as usize].phase = Phase::Parked { cc };
        s.txns[w as usize].wait = None;
        s.txns[w as usize].wait_cc = false;
        Spin::Mid
    }

    /// The deferrable commit check's scan (§5.3 timing 3): take the prefix latch and
    /// read every `/i/` entry of the prefix.
    fn do_def_scan(&self, s: &mut State, w: u8) -> Spin {
        s.latch.push((LatchKey::Prefix, w));
        let mut obs = [EntryObs::None, EntryObs::None];
        for (i, d) in DEF_KEYS.iter().enumerate() {
            if let Some(i2) = &s.slot(*d).intent {
                if i2.owner != w {
                    obs[i] = EntryObs::Intent { owner: i2.owner };
                }
            }
        }
        s.def_scan = obs;
        s.txns[w as usize].phase = Phase::DefLook;
        Spin::Mid
    }

    /// The deferrable check's verdict, still under the prefix latch: status lookups on
    /// the intents read by `DefScan` (§4: a missing current-epoch status found through
    /// an intent read under the latch is fatal), then removal / wait / 23505.
    fn do_def_look(&self, s: &mut State, w: u8) -> Spin {
        let obs = s.def_scan;
        for (i, d) in DEF_KEYS.iter().enumerate() {
            if let EntryObs::Intent { owner } = obs[i] {
                match s.status.get(&owner).map(|e| e.st) {
                    None => {
                        // §3.1/§4: the prefix latch guarantees no removal (hence no
                        // truncation) of an entry read under it can intervene.
                        s.bad = Some(format!(
                            "I-TRUNC: deferrable entry {d:?} of W{owner} read under the prefix latch lost its status entry"
                        ));
                        unlatch(s, w);
                        return Spin::Done;
                    }
                    Some(st) => {
                        let ended = match st {
                            St::Aborted => true,
                            St::Committed(c) => c <= s.visible_ts,
                            St::Pending => false,
                        };
                        if ended {
                            // Remove it per §7.3 steps 2-4 and re-scan.
                            self.remove_intent(s, owner, *d, true);
                            unlatch(s, w);
                            s.txns[w as usize].phase = Phase::CommitCheck;
                            return Spin::Retry;
                        }
                        // Pending or committed-not-visible: wait on its owner.
                        let g = s.status.get(&owner).map(|e| e.gen).unwrap_or(0);
                        s.txns[w as usize].wait = Some(vec![(owner, g)]);
                        s.txns[w as usize].wait_kind = EdgeKind::Key(*d, Lock::NoKeyUpd);
                        s.txns[w as usize].wait_cc = true;
                        s.txns[w as usize].phase = Phase::WaitReg;
                        unlatch(s, w);
                        return Spin::Mid;
                    }
                }
            }
        }
        // Two live entries for different rows -> 23505.
        let mut live_rows: Vec<LKey> = Vec::new();
        for d in DEF_KEYS {
            if let Some(i) = &s.slot(d).intent {
                if i.owner == w && matches!(top_layer(i).data, Data::Write { .. }) {
                    live_rows.push(d);
                }
            }
        }
        let committed_live: Vec<LKey> = DEF_KEYS
            .iter()
            .filter(|d| matches!(s.committed_state(**d), Some((_, VerData::Live { .. }))))
            .copied()
            .collect();
        if live_rows.len() + committed_live.len() >= 2 {
            unlatch(s, w);
            self.do_fail(s, w); // 23505
            return Spin::Done;
        }
        // Verdict clean; the check and the enqueue are one latch section.
        s.txns[w as usize].phase = Phase::Chan;
        unlatch(s, w);
        Spin::Done
    }
}

fn latch_free(s: &State, lk: LatchKey) -> bool {
    s.latch.iter().all(|(k, _)| *k != lk)
}

/// Release every latch `w` holds (a thread holds at most one).
fn unlatch(s: &mut State, w: u8) {
    s.latch.retain(|(_, h)| *h != w);
}

impl WriteModel {
    /// Latches `RollbackTo(w)` needs: one section per visited key (§5.5) plus one per
    /// released shared lock (§6). Seed 46 skips the shared-lock sections.
    fn rollback_latches_free(&self, s: &State, w: u8) -> bool {
        let sp_seq = match s.txns[w as usize].sp.last() {
            Some(&(q, _)) => q,
            None => return true,
        };
        let mut ok = true;
        for (q, k) in &s.txns[w as usize].wlog {
            if *q >= sp_seq && !latch_free(s, self.removal_lk(*k)) {
                ok = false;
            }
        }
        if self.bug != Some(Bug::SharedReleaseNoLatch) {
            for sl in &s.shared {
                if sl.txn == w && sl.seq >= sp_seq && !latch_free(s, lk_of(sl.key)) {
                    ok = false;
                }
            }
        }
        ok
    }

    /// Latches an abort of `w` needs for its shared-lock release (§6/§7.1).
    fn abort_latches_free(&self, s: &State, w: u8) -> bool {
        if self.bug == Some(Bug::SharedReleaseNoLatch) {
            return true;
        }
        s.shared
            .iter()
            .filter(|sl| sl.txn == w)
            .all(|sl| latch_free(s, lk_of(sl.key)))
    }

    /// Step-5 preconditions (§3): visible (seed 27: already at status-set), and the
    /// commit thread processes the group in order.
    fn step5_ready(&self, s: &State, i: usize) -> bool {
        let q = s.group[i];
        let prev = |f: fn(&Req) -> bool| s.group[..i].iter().all(f);
        let visible = q.visible || (self.bug == Some(Bug::EarlyResolveQueue) && q.status);
        visible && !q.done && prev(|p| p.done)
    }
}

impl Model for WriteModel {
    type State = State;
    type Action = Action;

    fn init(&self) -> State {
        State {
            wl: NO_WL,
            kv: Kv::new(),
            status: BTreeMap::new(),
            txns: [Txn::done(), Txn::done(), Txn::done()],
            channel: Vec::new(),
            group: Vec::new(),
            next_ts: 1,
            visible_ts: 0,
            resolve_q: Vec::new(),
            cleanup_q: Vec::new(),
            shared: Vec::new(),
            rels: Vec::new(),
            edges: Vec::new(),
            latch: Vec::new(),
            def_scan: [EntryObs::None, EntryObs::None],
            view_counter: 0,
            commits: Vec::new(),
            dlk_cycles: Vec::new(),
            bad: None,
        }
    }

    fn actions(&self, s: &State, out: &mut Vec<Action>) {
        if s.bad.is_some() {
            return;
        }
        if s.wl == NO_WL {
            for i in 0..WORKLOADS.len() {
                out.push(Action::Choose(i as u8));
            }
            return;
        }
        let n = s.ntxns();
        for w in 0..n as u8 {
            let t = &s.txns[w as usize];
            // A cancelled session acts on the flag at its next checkpoint (§6): the
            // self-abort, outside any latch section.
            let can_abort = t.cancel
                && !t.in_section()
                && !matches!(
                    t.phase,
                    Phase::Done { .. } | Phase::Parked { .. } | Phase::Queued
                )
                && matches!(s.status.get(&w).map(|e| e.st), Some(St::Pending) | None);
            if can_abort {
                out.push(Action::SelfAbort(w));
                continue;
            }
            match t.phase {
                Phase::Snap => out.push(Action::Snap(w)),
                Phase::Op => match s.cur_op(w) {
                    Op::FkRead { .. } => out.push(Action::FkRead(w)),
                    Op::Savepoint => out.push(Action::Savepoint(w)),
                    Op::RollbackTo => {
                        if self.rollback_latches_free(s, w) {
                            out.push(Action::RollbackTo(w));
                        }
                    }
                    op => {
                        let key = op_key(op).unwrap_or(LKey::T0);
                        if matches!(op, Op::RelLock { .. }) || latch_free(s, lk_of(key)) {
                            out.push(Action::SecRead(w));
                        }
                    }
                },
                Phase::SecGen => out.push(Action::SecGen(w)),
                Phase::SecPlace => out.push(Action::SecPlace(w)),
                Phase::SecGrant => out.push(Action::SecGrant(w)),
                Phase::WaitReg => out.push(Action::WaitEnter(w)),
                Phase::Epq => out.push(Action::EpqStep(w)),
                Phase::Parked { .. } => {
                    if s.cycle_through(w).is_some() && self.abort_latches_free(s, w) {
                        out.push(Action::DeadlockCheck(w));
                    }
                }
                Phase::EndS => out.push(Action::EndStmt(w)),
                Phase::CommitCheck => {
                    if latch_free(s, LatchKey::Prefix) {
                        out.push(Action::DefScan(w));
                    }
                }
                Phase::DefLook => out.push(Action::DefLook(w)),
                Phase::Chan => out.push(Action::Request(w)),
                Phase::Queued | Phase::Done { .. } => {}
            }
        }
        if s.group.is_empty() && !s.channel.is_empty() {
            out.push(Action::Drain(1));
            if s.channel.len() >= 2 {
                out.push(Action::Drain(2));
            }
        }
        for i in 0..s.group.len() {
            let q = s.group[i];
            let prev = |f: fn(&Req) -> bool| s.group[..i].iter().all(f);
            if !q.status && prev(|p| p.status) {
                out.push(Action::SetStatus(i as u8));
            }
            if q.status && !q.visible && prev(|p| p.visible) {
                out.push(Action::Advance(i as u8));
            }
            if self.step5_ready(s, i) {
                // §3 step 5: release each shared row lock under its key's latch
                // (seed 46: without it), then bump and wake.
                let remaining: Vec<LKey> = s
                    .shared
                    .iter()
                    .filter(|sl| sl.txn == q.w)
                    .map(|sl| sl.key)
                    .collect();
                if remaining.is_empty() {
                    out.push(Action::Wake5(i as u8));
                } else {
                    for k in remaining {
                        let lk = lk_of(k);
                        if self.bug == Some(Bug::SharedReleaseNoLatch) || latch_free(s, lk) {
                            out.push(Action::RelShared(q.w, k));
                        }
                    }
                }
            }
        }
        for (t, k) in &s.resolve_q {
            if self.bug == Some(Bug::NoOwnerRecheck) || latch_free(s, self.removal_lk(*k)) {
                out.push(Action::Resolve(*t, *k));
            }
        }
        for (t, k) in &s.cleanup_q {
            if self.bug == Some(Bug::NoOwnerRecheck) || latch_free(s, self.removal_lk(*k)) {
                out.push(Action::Cleanup(*t, *k));
            }
        }
        for (t, e) in &s.status {
            let released_ok = e.released || self.bug == Some(Bug::TruncateBeforeRelease);
            if released_ok && e.count == 0 && s.min_view() > e.last_removal {
                out.push(Action::Truncate(*t));
            }
        }
        if WORKLOADS[s.wl as usize].cancel {
            for w in 0..n as u8 {
                let t = &s.txns[w as usize];
                if !t.cancel && !matches!(t.phase, Phase::Done { .. } | Phase::Queued) {
                    out.push(Action::Cancel(w));
                }
            }
        }
    }

    fn next(&self, s: &State, a: &Action) -> State {
        let mut s = s.clone();
        let mut spin_code = Spin::Done;
        let mut subject: Option<u8> = None;
        match a {
            Action::Choose(i) => {
                let wl = &WORKLOADS[*i as usize];
                s.wl = *i;
                let mut vis = 0;
                for (k, ts, v, _) in wl.preload {
                    s.kv.entry(*k).or_default().vers.push((*ts, *v));
                    vis = vis.max(*ts);
                }
                for slot in s.kv.values_mut() {
                    slot.vers.sort_by_key(|(t, _)| std::cmp::Reverse(*t));
                }
                let mut pre: Vec<(LKey, Ts, ED)> = wl
                    .preload
                    .iter()
                    .map(|(k, t, _, ed)| (*k, *t, *ed))
                    .collect();
                pre.sort_by_key(|p| p.1);
                let mut commits: Commits = Vec::new();
                for (k, t, ed) in pre {
                    match commits.last_mut() {
                        Some(last) if last.0 == t => last.2.push((k, ed)),
                        _ => commits.push((t, 255, vec![(k, ed)])),
                    }
                }
                s.commits = commits;
                s.visible_ts = vis;
                s.next_ts = s.visible_ts + 1;
                for w in 0..wl.txns.len() {
                    s.txns[w] = Txn::fresh();
                    s.status.insert(
                        w as u8,
                        TxnEntry {
                            st: St::Pending,
                            released: false,
                            count: 0,
                            last_removal: 0,
                            gen: 0,
                        },
                    );
                }
            }
            Action::Snap(w) => {
                subject = Some(*w);
                // I-RC-MONO: the new statement must see everything observed before.
                let obs = s.txns[*w as usize].observing;
                for t in 0..NTXNS as u8 {
                    if obs & (1 << t) == 0 {
                        continue;
                    }
                    if let Some(St::Committed(c)) = s.status.get(&t).map(|e| e.st) {
                        if c > s.visible_ts {
                            s.bad = Some(format!(
                                "I-RC-MONO: W{w} observed W{t}'s commit @{c} but S={}",
                                s.visible_ts
                            ));
                        }
                    }
                }
                let visible = s.visible_ts;
                {
                    let txn = &mut s.txns[*w as usize];
                    txn.observing = 0;
                    txn.snap = Some(visible);
                    txn.base = visible;
                    txn.seq0 = txn.seq;
                    txn.seq += 1;
                }
                s.txns[*w as usize].skip = self.compute_skip(&s, *w, s.txns[*w as usize].stmt);
                self.scan_ready(&mut s, *w);
            }
            Action::SecRead(w) => {
                subject = Some(*w);
                spin_code = self.do_sec_read(&mut s, *w);
            }
            Action::SecGen(w) => {
                subject = Some(*w);
                spin_code = self.do_sec_gen(&mut s, *w);
            }
            Action::SecPlace(w) => {
                subject = Some(*w);
                unlatch(&mut s, *w);
                self.do_place(&mut s, *w);
            }
            Action::SecGrant(w) => {
                subject = Some(*w);
                unlatch(&mut s, *w);
                self.do_grant(&mut s, *w);
            }
            Action::WaitEnter(w) => {
                subject = Some(*w);
                spin_code = self.do_wait_enter(&mut s, *w);
            }
            Action::EpqStep(w) => {
                subject = Some(*w);
                spin_code = self.do_epq_step(&mut s, *w);
            }
            Action::FkRead(w) => {
                subject = Some(*w);
                self.do_fk_read(&mut s, *w);
            }
            Action::EndStmt(w) => {
                subject = Some(*w);
                self.do_end_stmt(&mut s, *w);
            }
            Action::Savepoint(w) => {
                subject = Some(*w);
                let (seq, exp) = {
                    let txn = &mut s.txns[*w as usize];
                    let seq = txn.seq;
                    txn.seq += 1;
                    (seq, txn.expected)
                };
                s.txns[*w as usize].sp.push((seq, exp));
                self.advance_op(&mut s, *w);
            }
            Action::RollbackTo(w) => {
                subject = Some(*w);
                self.do_rollback(&mut s, *w);
            }
            Action::Request(w) => {
                subject = Some(*w);
                s.txns[*w as usize].phase = Phase::Queued;
                s.channel.push(*w);
            }
            Action::Drain(n) => {
                for _ in 0..*n {
                    if let Some(w) = s.channel.first().copied() {
                        s.channel.remove(0);
                        let ts = s.next_ts;
                        s.next_ts += 1;
                        // Freeze the ghost: the net effect of the surviving statements.
                        let exp: ExpWrites = s.txns[w as usize]
                            .expected
                            .iter()
                            .enumerate()
                            .filter_map(|(i, ed)| ed.map(|e| (KEYS[i], e)))
                            .collect();
                        s.txns[w as usize].expected = [None; NKEYS];
                        s.commits.push((ts, w, exp));
                        s.group.push(Req {
                            w,
                            ts,
                            status: false,
                            visible: false,
                            done: false,
                        });
                    }
                }
            }
            Action::SetStatus(i) => {
                let q = s.group[*i as usize];
                if let Some(e) = s.status.get_mut(&q.w) {
                    e.st = St::Committed(q.ts);
                }
                s.group[*i as usize].status = true;
            }
            Action::Advance(i) => {
                let q = s.group[*i as usize];
                s.visible_ts = s.visible_ts.max(q.ts);
                s.group[*i as usize].visible = true;
            }
            Action::RelShared(w, k) => {
                // §3 step 5 / §6: remove the shared lock under the key's latch, in
                // its own section. The wake (generation bump) comes after all
                // releases, in Wake5.
                s.shared.retain(|sl| !(sl.txn == *w && sl.key == *k));
            }
            Action::Wake5(i) => {
                let q = s.group[*i as usize];
                let w = q.w;
                s.rels.retain(|(_, t, _)| *t != w);
                for k in s.intents_of(w) {
                    s.resolve_q.push((w, k));
                }
                if let Some(e) = s.status.get_mut(&w) {
                    e.released = true;
                }
                self.wake(&mut s, w);
                s.group[*i as usize].done = true;
                s.txns[w as usize].phase = Phase::Done { aborted: false };
                if s.group.iter().all(|r| r.done) {
                    s.group.clear();
                }
            }
            Action::Resolve(t, k) | Action::Cleanup(t, k) => {
                s.resolve_q.retain(|e| e != &(*t, *k));
                s.cleanup_q.retain(|e| e != &(*t, *k));
                self.remove_intent(&mut s, *t, *k, self.bug != Some(Bug::NoOwnerRecheck));
            }
            Action::Truncate(t) => {
                s.status.remove(t);
            }
            Action::Cancel(w) => {
                subject = Some(*w);
                s.txns[*w as usize].cancel = true;
                if matches!(s.txns[*w as usize].phase, Phase::Parked { .. })
                    && self.bug != Some(Bug::CancelNoWake)
                {
                    // §6: setting the flag also wakes a parked session.
                    let cc = matches!(s.txns[*w as usize].phase, Phase::Parked { cc: true });
                    s.txns[*w as usize].phase = if cc { Phase::CommitCheck } else { Phase::Op };
                    // A woken waiter removes any remaining edges.
                    s.edges.retain(|(x, _, _)| x != w);
                }
            }
            Action::SelfAbort(w) => {
                subject = Some(*w);
                self.do_fail(&mut s, *w);
            }
            Action::DeadlockCheck(w) => {
                subject = Some(*w);
                if let Some(cycle) = s.cycle_through(*w) {
                    for (_, t, kind) in &cycle {
                        if !s.edge_current(*w, *t, *kind) {
                            s.bad = Some(format!(
                                "I-LIVE(c): 40P01 of W{w} raised on a stale edge to W{t}"
                            ));
                        }
                    }
                    let mut members: Vec<u8> = vec![*w];
                    for (_, t, _) in &cycle {
                        if !members.contains(t) {
                            members.push(*t);
                        }
                    }
                    // Ghost: which members raise 40P01 at this detection. §6: the
                    // DFS runner is the one victim; the mutant gives every member
                    // the error.
                    let mut marked = 1u8 << *w;
                    if self.mutant == Some(Mutant::BothCycleMembers40P01) {
                        for m in &members {
                            marked |= 1 << *m;
                        }
                    }
                    s.dlk_cycles.push((members.clone(), marked));
                    // §6: the victim removes its own edges before raising 40P01.
                    if self.mutant != Some(Mutant::VictimKeepsEdges) {
                        s.edges.retain(|(x, _, _)| x != w);
                    }
                    // §6: exactly one member of the cycle is aborted — the victim
                    // raising 40P01. The other members are woken by its abort.
                    if self.mutant == Some(Mutant::BothCycleMembers40P01) {
                        let others: Vec<u8> = members.iter().copied().filter(|m| m != w).collect();
                        for m in others {
                            self.do_fail(&mut s, m);
                        }
                    }
                    self.do_fail(&mut s, *w);
                }
            }
            Action::DefScan(w) => {
                subject = Some(*w);
                spin_code = self.do_def_scan(&mut s, *w);
            }
            Action::DefLook(w) => {
                subject = Some(*w);
                spin_code = self.do_def_look(&mut s, *w);
            }
        }
        // I-PROGRESS accounting: any other actor's step resets the counter; the
        // actor's own retry bumps it, a mid-loop step keeps it, anything else clears.
        for j in 0..NTXNS {
            if subject != Some(j as u8) {
                s.txns[j].spin = 0;
            }
        }
        if let Some(i) = subject {
            let t = &mut s.txns[i as usize];
            t.spin = match spin_code {
                Spin::Retry => t.spin.saturating_add(1).min(4),
                Spin::Mid => t.spin,
                Spin::Done => 0,
            };
        }
        s.normalize();
        s
    }

    fn check(&self, s: &State) -> Result<(), String> {
        if let Some(b) = &s.bad {
            return Err(b.clone());
        }
        if s.wl == NO_WL {
            return Ok(());
        }
        // A latch is held only across an open latch section.
        for (_, h) in &s.latch {
            if (*h as usize) < NTXNS && !s.txns[*h as usize].in_section() {
                return Err(format!(
                    "I-ONE-INTENT: W{h} holds a latch outside its section ({:?})",
                    s.txns[*h as usize].phase
                ));
            }
        }
        for k in KEYS {
            let slot = s.slot(k);
            if let Some(i) = &slot.intent {
                if !s.status.contains_key(&i.owner) {
                    return Err(format!(
                        "I-TRUNC: intent on {k:?} of W{} has no status entry",
                        i.owner
                    ));
                }
                if i.layers.is_empty() {
                    return Err(format!("I-LOCK: intent on {k:?} has no layer"));
                }
                let mut prev_seq = 0;
                for l in &i.layers {
                    if l.seq < prev_seq {
                        return Err(format!("I-LOCK: layers of {k:?} out of seq order (§2.1)"));
                    }
                    if l.dseq > l.seq {
                        return Err(format!(
                            "I-LOCK: layer on {k:?} has data_seq {} > seq {} (§2.1)",
                            l.dseq, l.seq
                        ));
                    }
                    prev_seq = l.seq;
                }
            }
            for (ts, _) in &slot.vers {
                if *ts > s.visible_ts {
                    return Err(format!(
                        "I-RC-MONO: version {k:?}@{ts} exists above visible_ts {} (§3.2)",
                        s.visible_ts
                    ));
                }
            }
        }
        // In-memory locks must not outlive their status entry (§7.4 `released`).
        for sl in &s.shared {
            if !s.status.contains_key(&sl.txn) {
                return Err(format!(
                    "I-LOCK: shared lock on {:?} of W{} has no status entry",
                    sl.key, sl.txn
                ));
            }
        }
        for (_, t, _) in &s.rels {
            if !s.status.contains_key(t) {
                return Err(format!("I-LOCK: relation lock of W{t} has no status entry"));
            }
        }
        // I-COUNT, and the write-set log (against a bug-free ghost log) must name
        // every layer an active txn's intents hold (§5.1, §5.5).
        for (t, e) in &s.status {
            let n = s.intents_of(*t).len() as i8;
            if e.count != n {
                return Err(format!(
                    "I-COUNT: intent_count(W{t}) = {}, KV holds {n}",
                    e.count
                ));
            }
            if e.st == St::Pending {
                for k in KEYS {
                    let gseqs = s.txns[*t as usize]
                        .glog
                        .iter()
                        .filter(|(_, kk)| *kk == k)
                        .map(|(q, _)| *q)
                        .collect::<Vec<u8>>();
                    let rseqs = match &s.slot(k).intent {
                        Some(ii) if ii.owner == *t => {
                            ii.layers.iter().map(|l| l.seq).collect::<Vec<u8>>()
                        }
                        _ => Vec::new(),
                    };
                    if gseqs != rseqs {
                        return Err(format!(
                            "I-COUNT: W{t}'s write-set log names layers {gseqs:?} on {k:?}, the intent holds {rseqs:?}"
                        ));
                    }
                }
            }
        }
        // I-LOCK pairwise, per key.
        for k in KEYS {
            let mut held: Vec<(u8, Lock)> = Vec::new();
            if let Some(i) = &s.slot(k).intent {
                if s.holds_locks(i.owner) {
                    held.push((i.owner, top_layer(i).lock));
                }
            }
            for sl in s.shared.iter().filter(|sl| sl.key == k) {
                if s.holds_locks(sl.txn) {
                    held.push((sl.txn, Lock::KeyShare));
                }
            }
            for x in 0..held.len() {
                for y in x + 1..held.len() {
                    if conflicts(held[x].1, held[y].1) {
                        return Err(format!(
                            "I-LOCK: W{} ({:?}) and W{} ({:?}) hold conflicting locks on {k:?}",
                            held[x].0, held[x].1, held[y].0, held[y].1
                        ));
                    }
                }
            }
        }
        {
            let excl: Vec<u8> = s
                .rels
                .iter()
                .filter(|(_, t, ex)| *ex && s.holds_locks(*t))
                .map(|(_, t, _)| *t)
                .collect();
            let other = s
                .rels
                .iter()
                .any(|(_, t, _)| s.holds_locks(*t) && !excl.contains(t));
            if !excl.is_empty() && other {
                return Err("I-LOCK: conflicting relation locks held".into());
            }
        }
        // Ghost oracles: values (with the increment/lost-update rule) on row keys,
        // unique and FK consistency over the ghost identities, for every S <= visible_ts.
        for target in 0..=s.visible_ts {
            let mut rows: Vec<(LKey, Option<LKey>, bool)> = Vec::new();
            let mut entries: Vec<(LKey, LKey)> = Vec::new();
            for k in KEYS {
                let is_row = matches!(k, LKey::T0 | LKey::T1 | LKey::C0 | LKey::C1);
                let mut live: Option<u8> = None;
                let mut reset = 0u8;
                let mut since = 0u8;
                let mut ident: Option<(Option<LKey>, bool)> = None;
                let mut bad_inc = false;
                for (ts, _, map) in &s.commits {
                    if *ts > target {
                        continue;
                    }
                    if let Some((_, ed)) = map.iter().find(|(kk, _)| *kk == k) {
                        match ed {
                            ED::Row {
                                val, uval, child, ..
                            } => {
                                live = Some(*val);
                                reset = *val;
                                since = 0;
                                ident = Some((*uval, *child));
                            }
                            ED::Inc { d } => match live {
                                None => bad_inc = true,
                                Some(_) => {
                                    live = live.map(|v| v.saturating_add(*d));
                                    since = since.saturating_add(*d);
                                }
                            },
                            ED::Dead => {
                                live = None;
                                since = 0;
                                ident = None;
                            }
                            ED::Entry { row } => entries.push((k, *row)),
                        }
                    }
                }
                if bad_inc {
                    return Err(format!(
                        "I-RC-MONO/lost update: an increment of {k:?} landed on a dead row at S={target}"
                    ));
                }
                if is_row {
                    if let Some(v) = live {
                        if v != reset + since {
                            return Err(format!(
                            "I-RC-MONO/lost update: {k:?} at S={target} is {v}, its commits serialise to {} + {} increments",
                            reset, since
                        ));
                        }
                    }
                    let got = s.read_at(k, target);
                    if got != live {
                        return Err(format!(
                            "I-ATOMIC: {k:?} at S={target} reads {got:?}, committed state says {live:?}"
                        ));
                    }
                }
                if let Some((uval, child)) = ident {
                    rows.push((k, uval, child));
                }
            }
            for (e, row) in &entries {
                if matches!(e, LKey::U0 | LKey::U1) {
                    let holders: Vec<LKey> = rows
                        .iter()
                        .filter(|r| r.1 == Some(*e))
                        .map(|r| r.0)
                        .collect();
                    if holders.len() > 1 {
                        return Err(format!(
                            "I-UNIQUE: {} live rows share unique value {e:?} at S={target}",
                            holders.len()
                        ));
                    }
                    if holders.len() == 1 && holders[0] != *row {
                        return Err(format!(
                            "I-UNIQUE: live entry {e:?} at S={target} points at {row:?}, the value's row is {:?}",
                            holders[0]
                        ));
                    }
                    if holders.is_empty() {
                        return Err(format!(
                            "I-UNIQUE: live entry {e:?} at S={target} belongs to no live row"
                        ));
                    }
                }
            }
            for (k, uval, _) in &rows {
                if let Some(x) = uval {
                    if !entries.iter().any(|(e, _)| e == x) {
                        return Err(format!(
                            "I-UNIQUE: live row {k:?} has no live entry at S={target}"
                        ));
                    }
                }
            }
            let live_d = entries
                .iter()
                .filter(|(e, _)| matches!(e, LKey::D0 | LKey::D1))
                .count();
            if live_d > 1 {
                return Err(format!(
                    "I-UNIQUE: {live_d} live deferrable entries under one prefix at S={target}"
                ));
            }
            for (k, _, child) in &rows {
                if *child && !rows.iter().any(|r| r.0 == LKey::T0) {
                    return Err(format!(
                        "I-FK: live child {k:?} at S={target} has no live parent"
                    ));
                }
            }
        }
        // I-LIVE: every wait edge is a current conflict, no cancelled txn stays
        // parked, and a parked waiter has at least one current blocker.
        for (x, t, kind) in &s.edges {
            if !s.in_step5_window(*t) && !s.edge_current(*x, *t, *kind) {
                return Err(format!("I-LIVE: stale wait edge W{x} -> W{t} ({kind:?})"));
            }
        }
        // I-LIVE(b): every wait-for cycle is broken with exactly one 40P01 (§6).
        for (members, marked) in &s.dlk_cycles {
            let n = marked.count_ones();
            if n > 1 {
                return Err(format!(
                    "I-LIVE(b): one deadlock cycle {members:?} raised 40P01 for {n} members"
                ));
            }
            for m in 0..NTXNS as u8 {
                if marked & (1 << m) != 0 && !members.contains(&m) {
                    return Err(format!(
                        "I-LIVE(b): W{m} raised 40P01 outside its cycle {members:?}"
                    ));
                }
            }
        }
        for w in 0..s.ntxns() {
            let t = &s.txns[w];
            if matches!(t.phase, Phase::Parked { .. }) {
                if t.cancel {
                    return Err(format!("I-LIVE: cancelled W{w} stays parked"));
                }
                let blocked = s.edges.iter().any(|(x, t2, kind)| {
                    *x == w as u8 && (s.in_step5_window(*t2) || s.edge_current(*x, *t2, *kind))
                });
                if !blocked {
                    return Err(format!(
                        "I-LIVE: parked W{w} has no current conflicting blocker"
                    ));
                }
            }
            if t.spin > 3 {
                return Err(format!(
                    "I-PROGRESS: W{w} retried {} times without a foreign step",
                    t.spin
                ));
            }
        }
        Ok(())
    }

    fn is_final(&self, s: &State) -> bool {
        if s.wl == NO_WL {
            return false;
        }
        s.bad.is_none()
            && s.latch.is_empty()
            && s.channel.is_empty()
            && s.group.is_empty()
            && s.resolve_q.is_empty()
            && s.cleanup_q.is_empty()
            && s.edges.is_empty()
            && s.shared.is_empty()
            && s.rels.is_empty()
            && KEYS.iter().all(|k| s.slot(*k).intent.is_none())
            && (0..s.ntxns()).all(|w| matches!(s.txns[w].phase, Phase::Done { .. }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::check;

    fn max_states() -> usize {
        std::env::var("G0_MAX_STATES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20_000_000)
    }

    /// The review rework's mutation evidence: each of these deviations must be caught
    /// by the clean workload set.
    #[test]
    fn g0_write_mutants_caught() {
        for m in [
            Mutant::RcUpdateSkipsEpq,
            Mutant::BothCycleMembers40P01,
            Mutant::EpqPassesTombstone,
            Mutant::VictimKeepsEdges,
            Mutant::AbortCleanupNotQueued,
        ] {
            let r = check(
                &WriteModel {
                    bug: None,
                    mutant: Some(m),
                },
                max_states(),
            );
            let v = r
                .violation
                .unwrap_or_else(|| panic!("mutant {m:?} not caught in {} states", r.states));
            eprintln!(
                "mutant {m:?}: {} (trace {} steps)",
                v.message,
                v.trace.len()
            );
        }
    }
}
