//! G0-write (C-T0 §11): the write path — §5.0-§5.5 (without §5.3.1 ON CONFLICT, which is
//! card C-G0wb), §6 (locks, waiting, wake generations, deadlock DFS, cancel), §7.1 (abort),
//! §7.3 (intent removal) and §7.4 (status truncation with remembered-TxnId lookups), plus
//! the commit thread of §3 with status-set, visible-advance and step-5 as separate steps,
//! and the async resolver. Checked: I-ONE-INTENT, I-WW, I-LOCK, I-UNIQUE, I-FK,
//! I-HALLOWEEN, I-ATOMIC, I-LIVE, I-PROGRESS, I-RC-MONO, I-TRUNC, I-COUNT.
//!
//! One latch section or one KV write is one atomic step (`Latch` runs a whole §5.1
//! iteration, including any inline §7.3 removal, under the latch it took; `wait_for` runs
//! unlatched as `WaitEnter`; the EPQ quals re-evaluation runs unlatched as `EpqStep`).
//! Waits register an edge and park; every waker (commit step 5, abort, `ROLLBACK TO`,
//! lock release, cancel) removes edges and resumes waiters in its own step, so I-LIVE is
//! checkable per state: every graph edge must be current, and no cancelled txn may stay
//! parked. I-PROGRESS is the §11 backstop: a per-txn spin counter bumped by each
//! non-parking retry (a §5.1 `continue` after a removal, a repeated EPQ pass, a
//! `wait_for` that returns without parking), kept by the actor's own mid-loop steps
//! (entering EPQ, the quals re-evaluation) and reset by any other actor's step or by
//! completing the op; more than 3 retries without a foreign step is a violation. With
//! the 3-txn scope a correct run never exceeds 3: the longest chain is wake-return,
//! inline removal, repeated EPQ (each consuming one foreign change), and every further
//! retry needs a foreign intent or version that only another actor can produce.
//!
//! ## Workloads (fixed, chosen by the initial `Choose`)
//!
//! | # | name | program |
//! |---|------|---------|
//! | 0 | `lockdata` | W0 (RC): `UPDATE k0`; `FOR NO KEY UPDATE k0`. Preload k0 live. |
//! | 1 | `sp` | W0 (RC): `FOR UPDATE k0`; `SAVEPOINT s; UPDATE k0`; `ROLLBACK TO s`. Preload k0 live. |
//! | 2 | `unique` | W0 (RC): `INSERT (k0,u0)`; `INSERT (k1,u0)` (same unique value). |
//! | 3 | `ownabsent` | W0 (RC): `FOR NO KEY UPDATE k0`; `INSERT (k0,u1)`. Preload row k0 + entry u0 live. |
//! | 4 | `deferrable` | W0, W1 (RC): `INSERT (k0,d0)` / `INSERT (k1,d1)`, deferrable unique value v (prefix `/i/`), check at commit. |
//! | 5 | `fk` | W0 (RC): `DELETE parent k0` (end-of-stmt parent check). W1 (RC): `UPDATE k0; read parent`; `read parent; FOR KEY SHARE k0; INSERT child c1`. Preload parent k0, child c0. |
//! | 6 | `keyshare` | T_a: `DELETE k0` (key-changing, commits above S). T_b: `INSERT k0` (non-key, over the tombstone). R (RR): `FOR KEY SHARE k0` at S below both. Preload k0 live. |
//! | 7 | `share` | W0 (RC): `FOR KEY SHARE k0`; commit. W1 (RC): `DELETE k0`. Preload k0 live. |
//! | 8 | `epq_excl` | T0: `UPDATE k0`, commit, resolved; T2: `UPDATE k0`; W1 (RC): `UPDATE k0` (EPQ over T0's version). Preload k0 live. |
//! | 9 | `epq_share` | T0: `UPDATE k0`, commit, resolved; T2: `UPDATE k0`; W1 (RC): `FOR KEY SHARE k0` (EPQ over T0's version). Preload k0 live. |
//! | 10 | `wait_commit` | W0: `UPDATE k0`, commit. W1: `UPDATE k0`. Cancel enabled. Preload k0 live. |
//! | 11 | `wait_rollback` | W0: `SAVEPOINT s; UPDATE k0; ROLLBACK TO s`, commit. W1: `UPDATE k0`. Preload k0 live. |
//! | 12 | `rellock` | W0: `LOCK TABLE R EXCLUSIVE`, commit. W1: relation lock R (RowExclusive), `UPDATE k0`. Preload k0 live. |
//! | 13 | `deadlock` | W0: `UPDATE k0; UPDATE k1`. W1: `UPDATE k1; UPDATE k0`. Preload k0, k1 live. |
//!
//! ## Seed -> workload
//!
//! | seed | workload | | seed | workload |
//! |---|---|---|---|---|
//! | 4 | wait_commit | | 37 | wait_commit |
//! | 11 | wait_commit | | 38 | share |
//! | 12 | wait_commit | | 45 | keyshare |
//! | 15 | lockdata | | 46 | keyshare, share |
//! | 16 | sp | | 47 | epq_excl |
//! | 17 | wait_rollback | | 48 | wait_commit |
//! | 18 | wait_commit | | 50 | sp |
//! | 19 | unique | | 52 | epq_share |
//! | 24 | ownabsent | | 59 | wait_commit |
//! | 25 | deferrable | | 26 | wait_rollback |
//! | 27 | wait_commit | | | |
//!
//! ## Reductions and renderings (state-space and scope)
//!
//! - No crash, no fsync, no epoch (G0-commit owns those): the KV is one map, a batch is
//!   one atomic write, a view is a copy. `Ts` values are not rank-renumbered (the space
//!   is small); `view_counter` and `last_removal` are, as in `commit.rs`.
//! - Views: every unlatched read (§3.1) opens a registered view; here the open, the read
//!   and the close are one step (the FK reads and the parent-side scan), so no view stays
//!   open across states and `min(registered view counters)` is always `u32::MAX`. The
//!   truncation view-counter machinery is therefore trivially satisfied; G0-commit owns
//!   the non-trivial cases (seeds 2, 51). `/sys/txn` records are not modelled (no crash).
//! - Seed 46 ("released without the key's latch"): the coordination the latch orders —
//!   the removal being visible to the wake protocol of the same section — is rendered as
//!   the release dropping the wake of the waiters parked on that lock; with the bug
//!   active, step 5's generic wake happens in a separate `Wake` step, which is the
//!   observable window. Seed 25 ("removal latched on the entry key instead of the
//!   prefix"): the deferrable check's scan scope is the observable of the latch scope —
//!   the buggy check scans only its own entry's key instead of the whole prefix. Seed 50:
//!   the write-set log's "one entry per layer change" is rendered by the rollback
//!   dropping whole intents by placement seq instead of per-layer seqs when the bug is
//!   active (a later layer on an existing intent survives).
//! - Scope cuts (documented, none silent): no ON CONFLICT (C-G0wb), no moved-tombstones
//!   or PK-changing UPDATEs (no workload moves a key; `key_changed` is still modelled),
//!   no SIREAD/SSI state (G0-ssi), no `WITH HOLD` cursors, no advisory locks, relation
//!   locks have two modes (RowExclusive, Exclusive) exercised by `rellock` only, deadlock
//!   detection runs when a cycle exists (`deadlock_timeout` is not modelled), cancel is
//!   modelled for the sessions of `wait_commit` only, the FK parent-side detectNewRows
//!   rule is RR-only and not exercised (all FK workloads are RC), and EPQ quals are
//!   existence checks (a live version passes, a tombstone fails; the FK-EPQ 23503 path
//!   exists for C-G0wb but no 2.0 workload here needs a non-existence qual).

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

/// Lock modes. `KeyShare` is the one shared mode (§11); it never appears in an intent
/// layer (§2.1), only in the shared lock table and as a requested mode.
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

/// Ghost expected state of one key after a txn's surviving statements.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ED {
    Dead,
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

/// One frozen expected-write set of a commit (or of a preload step).
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
    SharedReleaseNoWake,
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
        Bug::SharedReleaseNoWake,
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
            Bug::SharedReleaseNoWake => 46,
            Bug::EpqPlaceNoReverify => 47,
            Bug::CancelNoWake => 48,
            Bug::LogOnlyNewIntents => 50,
            Bug::EpqRepeatOnIntent => 52,
            Bug::WakerLeavesEdges => 59,
        }
    }
}

/// One operation of a statement. Ops of one statement share `seq0`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Op {
    Write {
        key: LKey,
        val: u8,
        kc: bool,
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

macro_rules! w_update {
    ($key:expr, $val:expr) => {
        Op::Write {
            key: $key,
            val: $val,
            kc: false,
        }
    };
}

const W_LOCKDATA: Workload = Workload {
    txns: &[TxnSpec {
        iso: Iso::Rc,
        stmts: &[
            StmtSpec {
                ops: &[w_update!(LKey::T0, 2)],
                parent_check: false,
            },
            StmtSpec {
                ops: &[Op::LockOnly {
                    key: LKey::T0,
                    upd: false,
                }],
                parent_check: false,
            },
        ],
    }],
    preload: &[(
        LKey::T0,
        1,
        VerData::Live { val: 1, kc: false },
        ED::Row {
            val: 1,
            uval: None,
            child: false,
        },
    )],
    cancel: false,
    deferrable: false,
};

const W_SP: Workload = Workload {
    txns: &[TxnSpec {
        iso: Iso::Rc,
        stmts: &[
            StmtSpec {
                ops: &[Op::LockOnly {
                    key: LKey::T0,
                    upd: true,
                }],
                parent_check: false,
            },
            StmtSpec {
                ops: &[Op::Savepoint],
                parent_check: false,
            },
            StmtSpec {
                ops: &[w_update!(LKey::T0, 2)],
                parent_check: false,
            },
            StmtSpec {
                ops: &[Op::RollbackTo],
                parent_check: false,
            },
        ],
    }],
    preload: &[(
        LKey::T0,
        1,
        VerData::Live { val: 1, kc: false },
        ED::Row {
            val: 1,
            uval: None,
            child: false,
        },
    )],
    cancel: false,
    deferrable: false,
};

const W_UNIQUE: Workload = Workload {
    txns: &[TxnSpec {
        iso: Iso::Rc,
        stmts: &[
            StmtSpec {
                ops: &[
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
                parent_check: false,
            },
            StmtSpec {
                ops: &[
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
                parent_check: false,
            },
        ],
    }],
    preload: &[],
    cancel: false,
    deferrable: false,
};

const W_OWNABSENT: Workload = Workload {
    txns: &[TxnSpec {
        iso: Iso::Rc,
        stmts: &[
            StmtSpec {
                ops: &[Op::LockOnly {
                    key: LKey::T0,
                    upd: false,
                }],
                parent_check: false,
            },
            StmtSpec {
                ops: &[
                    Op::KeyExist {
                        key: LKey::T0,
                        kind: KeyKind::Pk {
                            uval: Some(LKey::U1),
                            child: false,
                        },
                    },
                    Op::KeyExist {
                        key: LKey::U1,
                        kind: KeyKind::Unique { row: LKey::T0 },
                    },
                ],
                parent_check: false,
            },
        ],
    }],
    preload: &[
        (
            LKey::T0,
            1,
            VerData::Live { val: 1, kc: false },
            ED::Row {
                val: 1,
                uval: Some(LKey::U0),
                child: false,
            },
        ),
        (
            LKey::U0,
            1,
            VerData::Live { val: 1, kc: false },
            ED::Entry { row: LKey::T0 },
        ),
    ],
    cancel: false,
    deferrable: false,
};

const W_DEFERRABLE: Workload = Workload {
    txns: &[
        TxnSpec {
            iso: Iso::Rc,
            stmts: &[StmtSpec {
                ops: &[
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
                ],
                parent_check: false,
            }],
        },
        TxnSpec {
            iso: Iso::Rc,
            stmts: &[StmtSpec {
                ops: &[
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
                ],
                parent_check: false,
            }],
        },
    ],
    preload: &[],
    cancel: false,
    deferrable: true,
};

const W_FK: Workload = Workload {
    txns: &[
        TxnSpec {
            iso: Iso::Rc,
            stmts: &[StmtSpec {
                ops: &[Op::Delete { key: LKey::T0 }],
                parent_check: true,
            }],
        },
        TxnSpec {
            iso: Iso::Rc,
            stmts: &[
                StmtSpec {
                    ops: &[w_update!(LKey::T0, 2), Op::FkRead { key: LKey::T0 }],
                    parent_check: false,
                },
                StmtSpec {
                    ops: &[
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
                    ],
                    parent_check: false,
                },
            ],
        },
    ],
    preload: &[
        (
            LKey::T0,
            1,
            VerData::Live { val: 1, kc: false },
            ED::Row {
                val: 1,
                uval: None,
                child: false,
            },
        ),
        (
            LKey::C0,
            1,
            VerData::Live { val: 1, kc: false },
            ED::Row {
                val: 1,
                uval: None,
                child: true,
            },
        ),
    ],
    cancel: false,
    deferrable: false,
};

const W_KEYSHARE: Workload = Workload {
    txns: &[
        // T_a: key-changing commit above S (the delete).
        TxnSpec {
            iso: Iso::Rc,
            stmts: &[StmtSpec {
                ops: &[Op::Delete { key: LKey::T0 }],
                parent_check: false,
            }],
        },
        // T_b: non-key commit above S (the re-insert; kc = false over the tombstone).
        TxnSpec {
            iso: Iso::Rc,
            stmts: &[StmtSpec {
                ops: &[Op::KeyExist {
                    key: LKey::T0,
                    kind: KeyKind::Pk {
                        uval: None,
                        child: false,
                    },
                }],
                parent_check: false,
            }],
        },
        // R: RR reader below both commits.
        TxnSpec {
            iso: Iso::Rr,
            stmts: &[StmtSpec {
                ops: &[Op::KeyShare {
                    key: LKey::T0,
                    fk: false,
                }],
                parent_check: false,
            }],
        },
    ],
    preload: &[(
        LKey::T0,
        1,
        VerData::Live { val: 1, kc: false },
        ED::Row {
            val: 1,
            uval: None,
            child: false,
        },
    )],
    cancel: false,
    deferrable: false,
};

const W_SHARE: Workload = Workload {
    txns: &[
        TxnSpec {
            iso: Iso::Rc,
            stmts: &[StmtSpec {
                ops: &[Op::KeyShare {
                    key: LKey::T0,
                    fk: false,
                }],
                parent_check: false,
            }],
        },
        TxnSpec {
            iso: Iso::Rc,
            stmts: &[StmtSpec {
                ops: &[Op::Delete { key: LKey::T0 }],
                parent_check: false,
            }],
        },
    ],
    preload: &[(
        LKey::T0,
        1,
        VerData::Live { val: 1, kc: false },
        ED::Row {
            val: 1,
            uval: None,
            child: false,
        },
    )],
    cancel: false,
    deferrable: false,
};

const W_EPQ_EXCL: Workload = Workload {
    txns: &[
        TxnSpec {
            iso: Iso::Rc,
            stmts: &[StmtSpec {
                ops: &[w_update!(LKey::T0, 2)],
                parent_check: false,
            }],
        },
        TxnSpec {
            iso: Iso::Rc,
            stmts: &[StmtSpec {
                ops: &[w_update!(LKey::T0, 4)],
                parent_check: false,
            }],
        },
        TxnSpec {
            iso: Iso::Rc,
            stmts: &[StmtSpec {
                ops: &[w_update!(LKey::T0, 3)],
                parent_check: false,
            }],
        },
    ],
    preload: &[(
        LKey::T0,
        1,
        VerData::Live { val: 1, kc: false },
        ED::Row {
            val: 1,
            uval: None,
            child: false,
        },
    )],
    cancel: false,
    deferrable: false,
};

const W_EPQ_SHARE: Workload = Workload {
    txns: &[
        TxnSpec {
            iso: Iso::Rc,
            stmts: &[StmtSpec {
                ops: &[w_update!(LKey::T0, 2)],
                parent_check: false,
            }],
        },
        TxnSpec {
            iso: Iso::Rc,
            stmts: &[StmtSpec {
                ops: &[Op::KeyShare {
                    key: LKey::T0,
                    fk: false,
                }],
                parent_check: false,
            }],
        },
        TxnSpec {
            iso: Iso::Rc,
            stmts: &[StmtSpec {
                ops: &[w_update!(LKey::T0, 3)],
                parent_check: false,
            }],
        },
    ],
    preload: &[(
        LKey::T0,
        1,
        VerData::Live { val: 1, kc: false },
        ED::Row {
            val: 1,
            uval: None,
            child: false,
        },
    )],
    cancel: false,
    deferrable: false,
};

const W_WAIT_COMMIT: Workload = Workload {
    txns: &[
        TxnSpec {
            iso: Iso::Rc,
            stmts: &[StmtSpec {
                ops: &[w_update!(LKey::T0, 2)],
                parent_check: false,
            }],
        },
        TxnSpec {
            iso: Iso::Rc,
            stmts: &[StmtSpec {
                ops: &[w_update!(LKey::T0, 3)],
                parent_check: false,
            }],
        },
    ],
    preload: &[(
        LKey::T0,
        1,
        VerData::Live { val: 1, kc: false },
        ED::Row {
            val: 1,
            uval: None,
            child: false,
        },
    )],
    cancel: true,
    deferrable: false,
};

const W_WAIT_ROLLBACK: Workload = Workload {
    txns: &[
        TxnSpec {
            iso: Iso::Rc,
            stmts: &[
                StmtSpec {
                    ops: &[Op::Savepoint],
                    parent_check: false,
                },
                StmtSpec {
                    ops: &[w_update!(LKey::T0, 2)],
                    parent_check: false,
                },
                StmtSpec {
                    ops: &[Op::RollbackTo],
                    parent_check: false,
                },
            ],
        },
        TxnSpec {
            iso: Iso::Rc,
            stmts: &[StmtSpec {
                ops: &[w_update!(LKey::T0, 3)],
                parent_check: false,
            }],
        },
    ],
    preload: &[(
        LKey::T0,
        1,
        VerData::Live { val: 1, kc: false },
        ED::Row {
            val: 1,
            uval: None,
            child: false,
        },
    )],
    cancel: false,
    deferrable: false,
};

const W_RELLOCK: Workload = Workload {
    txns: &[
        TxnSpec {
            iso: Iso::Rc,
            stmts: &[StmtSpec {
                ops: &[Op::RelLock { excl: true }],
                parent_check: false,
            }],
        },
        TxnSpec {
            iso: Iso::Rc,
            stmts: &[StmtSpec {
                ops: &[Op::RelLock { excl: false }, w_update!(LKey::T0, 2)],
                parent_check: false,
            }],
        },
    ],
    preload: &[(
        LKey::T0,
        1,
        VerData::Live { val: 1, kc: false },
        ED::Row {
            val: 1,
            uval: None,
            child: false,
        },
    )],
    cancel: false,
    deferrable: false,
};

const W_DEADLOCK: Workload = Workload {
    txns: &[
        TxnSpec {
            iso: Iso::Rc,
            stmts: &[StmtSpec {
                ops: &[w_update!(LKey::T0, 2), w_update!(LKey::T1, 2)],
                parent_check: false,
            }],
        },
        TxnSpec {
            iso: Iso::Rc,
            stmts: &[StmtSpec {
                ops: &[w_update!(LKey::T1, 3), w_update!(LKey::T0, 3)],
                parent_check: false,
            }],
        },
    ],
    preload: &[
        (
            LKey::T0,
            1,
            VerData::Live { val: 1, kc: false },
            ED::Row {
                val: 1,
                uval: None,
                child: false,
            },
        ),
        (
            LKey::T1,
            1,
            VerData::Live { val: 1, kc: false },
            ED::Row {
                val: 1,
                uval: None,
                child: false,
            },
        ),
    ],
    cancel: false,
    deferrable: false,
};

const WORKLOADS: &[Workload] = &[
    W_LOCKDATA,
    W_SP,
    W_UNIQUE,
    W_OWNABSENT,
    W_DEFERRABLE,
    W_FK,
    W_KEYSHARE,
    W_SHARE,
    W_EPQ_EXCL,
    W_EPQ_SHARE,
    W_WAIT_COMMIT,
    W_WAIT_ROLLBACK,
    W_RELLOCK,
    W_DEADLOCK,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Phase {
    /// Statement start: take the snapshot (RC per statement, RR once).
    Snap,
    /// Mid-op: run one §5.1 latch iteration (`Latch`), or the op's own action.
    Op,
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
    /// Row-op base: the snapshot `S`, or the EPQ-evaluated version's ts.
    base: Ts,
    epq_v: Option<(Ts, VerData)>,
    epq_count: u8,
    /// `wait_for` context read under the latch: (target, gen) pairs.
    wait: Option<Vec<(u8, u8)>>,
    /// The kind of the pending wait, read under the same latch.
    wait_kind: EdgeKind,
    /// The wait context came from the deferrable commit check.
    wait_cc: bool,
    seq: u8,
    seq0: u8,
    /// Ghost: what a correct execution of the surviving statements has written.
    expected: [Option<ED>; NKEYS],
    /// Ghost: `expected` as of the current statement's start (§4's own-write masking:
    /// layers with seq >= seq0 are invisible to the statement's reads).
    stmt_expected: [Option<ED>; NKEYS],
    sp: Vec<(u8, [Option<ED>; NKEYS])>,
    /// Ghost: txns whose committed effects this txn observed (I-RC-MONO).
    observing: u8,
    spin: u8,
    snap: Option<Ts>,
    cancel: bool,
}

impl Txn {
    fn fresh() -> Txn {
        Txn {
            stmt: 0,
            op: 0,
            phase: Phase::Snap,
            base: 0,
            epq_v: None,
            epq_count: 0,
            wait: None,
            wait_kind: EdgeKind::Key(LKey::T0, Lock::None),
            wait_cc: false,
            seq: 1,
            seq0: 0,
            expected: [None; NKEYS],
            stmt_expected: [None; NKEYS],
            sp: Vec::new(),
            observing: 0,
            spin: 0,
            snap: None,
            cancel: false,
        }
    }

    fn done() -> Txn {
        Txn {
            phase: Phase::Done { aborted: false },
            ..Txn::fresh()
        }
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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    wl: u8,
    kv: Kv,
    status: BTreeMap<u8, TxnEntry>,
    txns: [Txn; 3],
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
    view_counter: u32,
    /// Ghost: (commit ts, txn, expected writes) in ts order; txn 255 = preload.
    commits: Commits,
    /// Seed 46's split: step 5 released a shared lock but the wake is pending.
    pending_wake: Option<u8>,
    bad: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    Choose(u8),
    Snap(u8),
    Latch(u8),
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
    Step5(u8),
    Wake(u8),
    Resolve(u8, LKey),
    Cleanup(u8, LKey),
    Truncate(u8),
    Cancel(u8),
    SelfAbort(u8),
    DeadlockCheck(u8),
}

pub struct WriteModel {
    pub bug: Option<Bug>,
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

/// The deferrable entry key of txn `w`'s program (each deferrable workload has one
/// `Def` op per txn).
fn def_key_of(wl: u8, w: u8) -> LKey {
    let mut r = LKey::D0;
    for stmt in WORKLOADS[wl as usize].txns[w as usize].stmts {
        for op in stmt.ops {
            if let Op::KeyExist {
                key,
                kind: KeyKind::Def { .. },
            } = op
            {
                r = *key;
            }
        }
    }
    r
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

    fn vers_read(&self, slot: &Slot, ts: Ts) -> Option<VerData> {
        slot.vers.iter().find(|(t, _)| *t <= ts).map(|(_, v)| *v)
    }

    fn ed_at(&self, k: LKey, ts: Ts) -> Option<ED> {
        let mut r = None;
        for (t, _, map) in &self.commits {
            if *t <= ts {
                if let Some((_, ed)) = map.iter().find(|(kk, _)| *kk == k) {
                    r = Some(*ed);
                }
            }
        }
        r
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

    /// Find a wait-for cycle through `w`, returning its edges.
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
    fn edge_current(&self, _x: u8, t: u8, kind: EdgeKind) -> bool {
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
    }
}

impl WriteModel {
    // ---- shared protocol helpers. The bug changes guards and inputs inside the
    // step it names; every check and oracle below runs the same in clean and buggy
    // runs. ----

    /// §7.3 for the intent on `k`, expected to be owned by `t`. `check_owner` false is
    /// seed 11: act without re-reading the owner under the latch.
    fn remove_intent(&self, s: &mut State, t: u8, k: LKey, check_owner: bool) {
        let cur = s.kv.get(&k).and_then(|slot| slot.intent.clone());
        let acts = match &cur {
            Some(i) => i.owner == t || !check_owner,
            None => false,
        };
        if !acts {
            // Gone or re-owned: writes nothing, changes no count.
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

    /// Release `t`'s shared row locks with seq >= `min_seq` (§6). Returns whether
    /// anything was released. Without the key's latch (seed 46) the release drops the
    /// wake of the waiters parked on it.
    fn release_shared(&self, s: &mut State, t: u8, min_seq: u8) -> bool {
        let before = s.shared.len();
        s.shared.retain(|sl| !(sl.txn == t && sl.seq >= min_seq));
        let released = before != s.shared.len();
        if released && self.bug != Some(Bug::SharedReleaseNoWake) {
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
        let keys = s.intents_of(w);
        for k in keys {
            s.cleanup_q.push((w, k));
        }
        self.wake(s, w);
        s.txns[w as usize].phase = Phase::Done { aborted: true };
    }

    /// Build and write the new top layer (§2.1), update the intent and the count.
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
            Op::Write { val, kc, .. } => (
                Data::Write {
                    val,
                    kc: kc || kc_sticky,
                },
                seq0,
            ),
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
        if fresh {
            if let Some(e) = s.status.get_mut(&w) {
                e.count += 1;
            }
        }
        // Ghost expected state of the surviving statements.
        let ed = match op {
            Op::Write { val, .. } => Some(ED::Row {
                val,
                uval: None,
                child: false,
            }),
            Op::Delete { .. } => Some(ED::Dead),
            Op::KeyExist { kind, .. } => match kind {
                KeyKind::Pk { uval, child } => Some(ED::Row {
                    val: 1,
                    uval,
                    child,
                }),
                KeyKind::Unique { row } | KeyKind::Def { row } => Some(ED::Entry { row }),
            },
            _ => None,
        };
        if let Some(ed) = ed {
            s.txns[w as usize].expected[key as usize] = Some(ed);
        }
        self.advance_op(s, w);
    }

    fn do_grant(&self, s: &mut State, w: u8) {
        let op = s.cur_op(w);
        let key = op_key(op).unwrap_or(LKey::T0);
        let seq0 = s.txns[w as usize].seq0;
        let snap = s.txns[w as usize].snap.unwrap_or(s.visible_ts);
        // I-WW (the KEY SHARE version rule of §5.1): a grant must not cover a
        // tombstone or key-changing write above S.
        if matches!(op, Op::KeyShare { .. }) {
            let vers: Vec<(Ts, VerData)> = s.slot(key).vers.clone();
            for (ts, v) in &vers {
                if *ts > snap {
                    let bad = match v {
                        VerData::Tomb => true,
                        VerData::Live { kc, .. } => *kc,
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

    fn advance_op(&self, s: &mut State, w: u8) {
        let txn = &mut s.txns[w as usize];
        txn.op += 1;
        txn.base = txn.snap.unwrap_or(0);
        txn.epq_count = 0;
        txn.epq_v = None;
        txn.wait = None;
        txn.wait_cc = false;
        let stmts = WORKLOADS[s.wl as usize].txns[w as usize].stmts;
        let nops = stmts[txn.stmt as usize].ops.len();
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
        let txn = &mut s.txns[w as usize];
        txn.stmt += 1;
        txn.op = 0;
        if (txn.stmt as usize) < nstmts {
            if iso == Iso::Rc {
                txn.phase = Phase::Snap;
            } else {
                txn.seq0 = txn.seq;
                txn.seq += 1;
                txn.stmt_expected = txn.expected;
                txn.phase = Phase::Op;
            }
            return;
        }
        txn.phase = if deferrable {
            Phase::CommitCheck
        } else {
            Phase::Chan
        };
    }

    // ---- the §5.1 latch section ----

    /// One §5.1 iteration for `w`'s current op (or one deferrable commit-check scan).
    fn do_latch(&self, s: &mut State, w: u8) -> Spin {
        if matches!(s.txns[w as usize].phase, Phase::CommitCheck) {
            return self.do_def_latch(s, w);
        }
        let op = s.cur_op(w);
        if let Op::RelLock { excl } = op {
            return self.do_rel_latch(s, w, excl);
        }
        let key = op_key(op).unwrap_or(LKey::T0);
        let m = req_mode(op);
        let seq0 = s.txns[w as usize].seq0;
        // 1. Foreign intent.
        let foreign = s.slot(key).intent.clone();
        if let Some(i) = &foreign {
            if i.owner != w {
                let owner = i.owner;
                let ended = match s.status.get(&owner).map(|e| e.st) {
                    None => true,
                    Some(St::Aborted) => true,
                    Some(St::Pending) => false,
                    Some(St::Committed(c)) => {
                        self.bug == Some(Bug::ProceedBeforeVisible) || c <= s.visible_ts
                    }
                };
                if ended {
                    if self.bug == Some(Bug::OverwriteForeignEnded) {
                        // Seed 12: place without removing first.
                        self.do_place(s, w);
                        return Spin::Done;
                    }
                    let committed =
                        matches!(s.status.get(&owner).map(|e| e.st), Some(St::Committed(_)));
                    self.remove_intent(s, owner, key, true);
                    if committed {
                        s.txns[w as usize].observing |= 1 << owner;
                    }
                    return Spin::Retry; // `continue`
                }
                let top = top_layer(i);
                // R3W-8: no wait on a lock-only intent over a live row.
                if is_key_exist(op)
                    && top.data == Data::Absent
                    && matches!(s.committed_state(key), Some((_, VerData::Live { .. })))
                {
                    self.do_fail(s, w);
                    return Spin::Done;
                }
                if conflicts(m, top.lock) {
                    if self.bug == Some(Bug::PlaceWithoutLatch) {
                        // Seed 4: place without the latch.
                        self.do_place(s, w);
                        return Spin::Done;
                    }
                    let g = s.status.get(&owner).map(|e| e.gen).unwrap_or(0);
                    s.txns[w as usize].wait = Some(vec![(owner, g)]);
                    s.txns[w as usize].wait_kind = EdgeKind::Key(key, m);
                    s.txns[w as usize].wait_cc = false;
                    s.txns[w as usize].phase = Phase::WaitReg;
                    return Spin::Mid;
                }
                // Non-conflicting foreign intent (KEY SHARE vs NO KEY UPDATE).
                if self.bug == Some(Bug::EpqRepeatOnIntent)
                    && s.txns[w as usize].epq_count >= 1
                    && matches!(op, Op::KeyShare { .. })
                {
                    // Seed 52: repeat EPQ instead of proceeding.
                    s.txns[w as usize].epq_v = s.slot(key).vers.first().copied();
                    s.txns[w as usize].epq_count += 1;
                    s.txns[w as usize].phase = Phase::Epq;
                    return Spin::Retry;
                }
            }
        }
        // 2. Conflicting shared holders.
        let hs: Vec<(u8, u8)> = s
            .shared
            .iter()
            .filter(|sl| sl.key == key && sl.txn != w && conflicts(m, Lock::KeyShare))
            .filter(|sl| s.holds_locks(sl.txn))
            .map(|sl| (sl.txn, s.status.get(&sl.txn).map(|e| e.gen).unwrap_or(0)))
            .collect();
        if !hs.is_empty() {
            s.txns[w as usize].wait = Some(hs);
            s.txns[w as usize].wait_kind = EdgeKind::Key(key, m);
            s.txns[w as usize].wait_cc = false;
            s.txns[w as usize].phase = Phase::WaitReg;
            return Spin::Mid;
        }
        // 3. Own intent: §5.4.
        if let Some(i) = &foreign {
            if i.owner == w {
                let top = top_layer(i);
                if is_data_row_op(op) {
                    if top.dseq == seq0 {
                        // Revisit in the same statement: skip the row.
                        self.advance_op(s, w);
                        return Spin::Done;
                    }
                    if top.dseq > seq0 {
                        self.do_fail(s, w); // 27000
                        return Spin::Done;
                    }
                    self.do_place(s, w);
                    return Spin::Done;
                }
                if matches!(op, Op::KeyShare { .. }) {
                    self.do_grant(s, w);
                    return Spin::Done;
                }
                if !is_key_exist(op) {
                    self.do_place(s, w);
                    return Spin::Done;
                }
                // A key-existence op always runs the unique check, own intent or not.
            }
        }
        // 4. Newer-version rule (row ops; key-existence ops are governed by §5.3).
        if !is_key_exist(op) {
            let base = s.txns[w as usize].base;
            let n: Vec<(Ts, VerData)> = s
                .slot(key)
                .vers
                .iter()
                .filter(|(t, _)| *t > base)
                .copied()
                .collect();
            if !n.is_empty() {
                if s.iso(w) == Iso::Rr {
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
                        self.do_fail(s, w); // 40001
                        return Spin::Done;
                    }
                    // KEY SHARE may proceed over plain newer writes.
                } else {
                    // RC: EPQ (§5.2), unlatched.
                    let repeat = s.txns[w as usize].epq_count >= 1;
                    s.txns[w as usize].epq_v = n.first().copied();
                    s.txns[w as usize].epq_count += 1;
                    s.txns[w as usize].phase = Phase::Epq;
                    return if repeat { Spin::Retry } else { Spin::Mid };
                }
            }
        } else if !is_defer_op(op) {
            // 4b. Unique check (§5.3): the key's current state.
            let own_top: Option<Data> = match &foreign {
                Some(i) if i.owner == w => Some(top_layer(i).data),
                _ => None,
            };
            let committed_live = matches!(s.committed_state(key), Some((_, VerData::Live { .. })));
            let live = if self.bug == Some(Bug::UniqueIgnoresOwn) {
                committed_live
            } else if own_top == Some(Data::Absent) && self.bug == Some(Bug::OwnAbsentNotLive) {
                false
            } else {
                match own_top {
                    Some(Data::Write { .. }) => true,
                    Some(Data::Delete) | Some(Data::Absent) => committed_live,
                    None => committed_live,
                }
            };
            if live {
                self.do_fail(s, w); // 23505
                return Spin::Done;
            }
        }
        if matches!(op, Op::KeyShare { .. }) {
            self.do_grant(s, w);
        } else {
            self.do_place(s, w);
        }
        Spin::Done
    }

    fn do_rel_latch(&self, s: &mut State, w: u8, excl: bool) -> Spin {
        let hs: Vec<(u8, u8)> = s
            .rels
            .iter()
            .filter(|(_, t, ex)| *t != w && (excl || *ex) && s.holds_locks(*t))
            .map(|(_, t, _)| (*t, s.status.get(t).map(|e| e.gen).unwrap_or(0)))
            .collect();
        if !hs.is_empty() {
            s.txns[w as usize].wait = Some(hs);
            s.txns[w as usize].wait_kind = EdgeKind::Rel(excl);
            s.txns[w as usize].wait_cc = false;
            s.txns[w as usize].phase = Phase::WaitReg;
            return Spin::Mid;
        }
        s.rels.push((0, w, excl));
        self.advance_op(s, w);
        Spin::Done
    }

    /// The deferrable commit-check (§5.3 timing 3) under the prefix latch.
    fn do_def_latch(&self, s: &mut State, w: u8) -> Spin {
        let own_d = def_key_of(s.wl, w);
        let scan: [LKey; 2] = if self.bug == Some(Bug::DeferrableEntryLatch) {
            [own_d, own_d]
        } else {
            DEF_KEYS
        };
        for d in scan {
            if let Some(i) = s.slot(d).intent.clone() {
                if i.owner != w {
                    let owner = i.owner;
                    let ended = match s.status.get(&owner).map(|e| e.st) {
                        None => true,
                        Some(St::Aborted) => true,
                        Some(St::Pending) => false,
                        Some(St::Committed(c)) => c <= s.visible_ts,
                    };
                    if ended {
                        self.remove_intent(s, owner, d, true);
                        return Spin::Retry; // remove and re-scan
                    }
                    // Pending or committed-not-visible: wait on its owner.
                    let g = s.status.get(&owner).map(|e| e.gen).unwrap_or(0);
                    s.txns[w as usize].wait = Some(vec![(owner, g)]);
                    s.txns[w as usize].wait_kind = EdgeKind::Key(d, Lock::NoKeyUpd);
                    s.txns[w as usize].wait_cc = true;
                    s.txns[w as usize].phase = Phase::WaitReg;
                    return Spin::Mid;
                }
            }
            // A live committed entry of a different row -> 23505.
            if let Some((_, VerData::Live { .. })) = s.committed_state(d) {
                self.do_fail(s, w);
                return Spin::Done;
            }
        }
        // Verdict clean; the check and the enqueue are one latch section.
        s.txns[w as usize].phase = Phase::Chan;
        Spin::Done
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
            targets
                .iter()
                .any(|(t, g)| match s.status.get(t).map(|e| (e.st, e.gen)) {
                    None => self.bug != Some(Bug::MissingStatusPending),
                    Some((st, g2)) => {
                        let ended = match st {
                            St::Aborted => true,
                            St::Committed(c) => c <= s.visible_ts,
                            St::Pending => false,
                        };
                        ended || g2 != *g
                    }
                })
        };
        let ret = if self.bug == Some(Bug::NoGenRecheck) {
            s.txns[w as usize].cancel
        } else {
            returns(s)
        };
        if ret {
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

    /// §5.2 EPQ: re-evaluate the quals (existence) against `v`, unlatched.
    fn do_epq_step(&self, s: &mut State, w: u8) -> Spin {
        let v = s.txns[w as usize]
            .epq_v
            .take()
            .unwrap_or((0, VerData::Tomb));
        let op = s.cur_op(w);
        let pass = matches!(v.1, VerData::Live { .. });
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
        s.txns[w as usize].phase = Phase::Op;
        Spin::Mid
    }

    /// FK child-side read of the parent (§5.3) from a registered view.
    fn do_fk_read(&self, s: &mut State, w: u8) {
        let op = s.cur_op(w);
        let key = op_key(op).unwrap_or(LKey::T0);
        s.view_counter += 1;
        let snap = s.txns[w as usize].snap.unwrap_or(s.visible_ts);
        let seq0 = s.txns[w as usize].seq0;
        // §4 read with own layers masked at seq >= seq0 (I-HALLOWEEN).
        let val = {
            let slot = s.slot(key);
            let mut r: Option<Option<u8>> = None;
            if let Some(i) = &slot.intent {
                if i.owner == w {
                    if let Some(l) = i.layers.iter().rev().find(|l| l.seq < seq0) {
                        match l.data {
                            Data::Write { val, .. } => r = Some(Some(val)),
                            Data::Delete => r = Some(None),
                            Data::Absent => {}
                        }
                    }
                } else if let Some(St::Committed(c)) = s.status.get(&i.owner).map(|e| e.st) {
                    if c <= snap {
                        match top_layer(i).data {
                            Data::Write { val, .. } => r = Some(Some(val)),
                            Data::Delete => r = Some(None),
                            Data::Absent => {}
                        }
                    }
                }
            }
            match r {
                Some(v) => v,
                None => match s.vers_read(slot, snap) {
                    Some(VerData::Live { val, .. }) => Some(val),
                    _ => None,
                },
            }
        };
        let want = match s.txns[w as usize].stmt_expected[key as usize] {
            Some(ED::Dead) => None,
            Some(ED::Row { val, .. }) => Some(val),
            Some(ED::Entry { .. }) => Some(1),
            None => match s.ed_at(key, snap) {
                Some(ED::Dead) => None,
                Some(ED::Row { val, .. }) => Some(val),
                Some(ED::Entry { .. }) => Some(1),
                None => None,
            },
        };
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

    /// End-of-statement FK parent-side check (§5.3).
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

    /// §5.5 `ROLLBACK TO SAVEPOINT`.
    fn do_rollback(&self, s: &mut State, w: u8) {
        let sp = s.txns[w as usize].sp.pop();
        if let Some((s_seq, exp)) = sp {
            for k in s.intents_of(w) {
                let layers = s
                    .slot(k)
                    .intent
                    .clone()
                    .map(|i| i.layers)
                    .unwrap_or_default();
                // Correct: drop layers with seq >= s. Seed 50: the write-set log has
                // only the placement, so whole intents whose first layer is >= s are
                // handled and later layers on older intents survive.
                let kept: Vec<Layer> = if self.bug == Some(Bug::LogOnlyNewIntents) {
                    let first = layers.first().map(|l| l.seq).unwrap_or(0);
                    if first >= s_seq {
                        Vec::new()
                    } else {
                        layers
                    }
                } else {
                    layers.iter().copied().filter(|l| l.seq < s_seq).collect()
                };
                if kept.is_empty() {
                    self.remove_intent(s, w, k, true);
                } else {
                    let mut kept = kept;
                    if self.bug == Some(Bug::RollbackDropsLock) {
                        let n = kept.len();
                        kept[n - 1].lock = Lock::None;
                    }
                    s.kv.entry(k).or_default().intent = Some(Intent {
                        owner: w,
                        layers: kept,
                    });
                }
            }
            self.release_shared(s, w, s_seq);
            s.txns[w as usize].expected = exp;
            if self.bug != Some(Bug::RollbackNoWake) {
                self.wake(s, w);
            }
        }
        self.advance_op(s, w);
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
            view_counter: 0,
            commits: Vec::new(),
            pending_wake: None,
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
            // A cancelled session acts on the flag at its next checkpoint: its only
            // action is the self-abort (§6).
            let can_abort = t.cancel
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
                    Op::RollbackTo => out.push(Action::RollbackTo(w)),
                    _ => out.push(Action::Latch(w)),
                },
                Phase::WaitReg => out.push(Action::WaitEnter(w)),
                Phase::Epq => out.push(Action::EpqStep(w)),
                Phase::Parked { .. } => {
                    if s.cycle_through(w).is_some() {
                        out.push(Action::DeadlockCheck(w));
                    }
                }
                Phase::EndS => out.push(Action::EndStmt(w)),
                Phase::CommitCheck => out.push(Action::Latch(w)),
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
            let step5_ok = q.visible || (self.bug == Some(Bug::EarlyResolveQueue) && q.status);
            if step5_ok && !q.done && prev(|p| p.done) {
                out.push(Action::Step5(i as u8));
            }
        }
        for (t, k) in &s.resolve_q {
            out.push(Action::Resolve(*t, *k));
        }
        for (t, k) in &s.cleanup_q {
            out.push(Action::Cleanup(*t, *k));
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
        if let Some(w) = s.pending_wake {
            out.push(Action::Wake(w));
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
                for t in 0..3u8 {
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
                let txn = &mut s.txns[*w as usize];
                txn.observing = 0;
                txn.snap = Some(visible);
                txn.base = visible;
                txn.seq0 = txn.seq;
                txn.seq += 1;
                txn.stmt_expected = txn.expected;
                txn.phase = Phase::Op;
            }
            Action::Latch(w) => {
                subject = Some(*w);
                spin_code = self.do_latch(&mut s, *w);
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
                        // Freeze the ghost: what the surviving statements really wrote.
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
            Action::Step5(i) => {
                let q = s.group[*i as usize];
                let w = q.w;
                let released_any = self.release_shared(&mut s, w, 0);
                s.rels.retain(|(_, t, _)| *t != w);
                for k in s.intents_of(w) {
                    s.resolve_q.push((w, k));
                }
                if let Some(e) = s.status.get_mut(&w) {
                    e.released = true;
                }
                if self.bug == Some(Bug::SharedReleaseNoWake) && released_any {
                    // Seed 46: the release dropped the wake; the generic step-5 wake
                    // happens in a separate step.
                    s.pending_wake = Some(w);
                } else {
                    self.wake(&mut s, w);
                }
                s.group[*i as usize].done = true;
                s.txns[w as usize].phase = Phase::Done { aborted: false };
                if s.group.iter().all(|r| r.done) {
                    s.group.clear();
                }
            }
            Action::Wake(w) => {
                s.pending_wake = None;
                self.wake(&mut s, *w);
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
                    let cc = matches!(s.txns[*w as usize].phase, Phase::Parked { cc: true });
                    s.txns[*w as usize].phase = if cc { Phase::CommitCheck } else { Phase::Op };
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
                    s.edges.retain(|(x, _, _)| x != w);
                    self.do_fail(&mut s, *w);
                }
            }
        }
        // I-PROGRESS accounting: any other actor's step resets the counter; the
        // actor's own retry bumps it, a mid-loop step keeps it, anything else clears.
        for j in 0..3 {
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
                let mut prev = Lock::None;
                for l in &i.layers {
                    if l.lock < prev || l.lock < implied(l.data) || l.lock < Lock::NoKeyUpd {
                        return Err(format!(
                            "I-LOCK: intent on {k:?} of W{} breaks §2.1 layer lock rules",
                            i.owner
                        ));
                    }
                    if l.dseq > l.seq {
                        return Err(format!(
                            "I-LOCK: layer on {k:?} has data_seq {} > seq {}",
                            l.dseq, l.seq
                        ));
                    }
                    prev = l.lock;
                }
            }
            // I-ONE-INTENT is structural here (one intent slot per key); violating it
            // by overwriting surfaces as I-COUNT below.
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
        // I-COUNT.
        for (t, e) in &s.status {
            let n = s.intents_of(*t).len() as i8;
            if e.count != n {
                return Err(format!(
                    "I-COUNT: intent_count(W{t}) = {}, KV holds {n}",
                    e.count
                ));
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
        // Ghost oracles: reads and unique/FK state, for every S <= visible_ts.
        for target in 0..=s.visible_ts {
            for k in KEYS {
                let got = s.read_at(k, target);
                let want = match s.ed_at(k, target) {
                    Some(ED::Dead) => None,
                    Some(ED::Row { val, .. }) => Some(val),
                    Some(ED::Entry { .. }) => Some(1),
                    None => None,
                };
                if got != want {
                    return Err(format!(
                        "I-ATOMIC: {k:?} at S={target} reads {got:?}, committed state says {want:?}"
                    ));
                }
            }
            let mut rows: Vec<(LKey, Option<LKey>, bool)> = Vec::new();
            let mut entries: Vec<(LKey, LKey)> = Vec::new();
            for k in KEYS {
                match s.ed_at(k, target) {
                    Some(ED::Row { uval, child, .. }) => rows.push((k, uval, child)),
                    Some(ED::Entry { row }) => entries.push((k, row)),
                    _ => {}
                }
            }
            for (e, _) in &entries {
                if matches!(e, LKey::U0 | LKey::U1) {
                    let n = rows.iter().filter(|r| r.1 == Some(*e)).count();
                    if n > 1 {
                        return Err(format!(
                            "I-UNIQUE: {n} live rows share unique value {e:?} at S={target}"
                        ));
                    }
                    if n == 0 {
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
                if *child && !matches!(s.ed_at(LKey::T0, target), Some(ED::Row { .. })) {
                    return Err(format!(
                        "I-FK: live child {k:?} at S={target} has no live parent"
                    ));
                }
            }
        }
        // I-LIVE.
        for (x, t, kind) in &s.edges {
            if !s.in_step5_window(*t) && !s.edge_current(*x, *t, *kind) {
                return Err(format!("I-LIVE: stale wait edge W{x} -> W{t} ({kind:?})"));
            }
        }
        for w in 0..s.ntxns() {
            let t = &s.txns[w];
            if matches!(t.phase, Phase::Parked { .. }) && t.cancel {
                return Err(format!("I-LIVE: cancelled W{w} stays parked"));
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
            && s.channel.is_empty()
            && s.group.is_empty()
            && s.resolve_q.is_empty()
            && s.cleanup_q.is_empty()
            && s.edges.is_empty()
            && s.shared.is_empty()
            && s.rels.is_empty()
            && s.pending_wake.is_none()
            && (0..s.ntxns()).all(|w| matches!(s.txns[w].phase, Phase::Done { .. }))
    }
}
