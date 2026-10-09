//! G0-write (C-T0 §11): the write path — §5.0-§5.5 including §5.3.1 ON CONFLICT
//! (C-G0wb), §6 (locks, waits, wake generations, deadlock DFS, cancel), §7.1
//! (abort), §7.3 (intent removal) and §7.4 (status truncation with
//! remembered-TxnId lookups), plus the commit thread of §3 with status-set,
//! visible-advance, the shared-lock release and the step-5 wake as separate
//! steps, and the async resolver. Checked: I-ONE-INTENT, I-WW, I-LOCK,
//! I-UNIQUE, I-FK, I-HALLOWEEN, I-ATOMIC, I-LIVE (a/b/c), I-PROGRESS (retry
//! bound), I-RC-MONO, I-TRUNC, I-COUNT, and no lost update under RC.
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
//! - The §5.3.1 pre-check (`ArbPre`) takes `latch_key(arb)` for its read of the
//!   arbiter key (plus the inline §7.3 removal of an ended foreign intent, as
//!   §5.1's block). The arbiter **lock** runs as an ordinary §5.1 row op
//!   (`SecRead`/`SecPlace` over the synthesized lock op) with base `v_r.ts` and
//!   no EPQ: with that base a non-empty N restarts the arbiter (§5.3.1(3)). The
//!   **abandon** (`Abandon`, phase `ArbAb`) rolls back to `sa` as `ROLLBACK TO`
//!   — one latch section per visited key, guarded exactly like `RollbackTo`.
//!   The insert path's arbiter entry op routes its "live entry of another row"
//!   23505 and its waits into the abandon (§5.3.1(2)), so both a conflict and a
//!   wait can land between the attempt's write and its rollback.
//!
//! ## Workloads (fixed, chosen by the initial `Choose`)
//!
//! Workloads are concurrent except the three named in Scope cuts whose seeds are
//! inherently sequential. Writes are read-modify-write (`v = v + 1`) under RC, so a
//! skipped or stale EPQ re-check is a visible lost update; `W_epq` adds a qual
//! (`UPDATE ... WHERE v = 0`) that a concurrent update makes fail, so the EPQ-fail
//! skip path is reachable.
//!
//! | # | name | program (RC unless noted) |
//! |---|------|----------------------------|
//! | 0 | `main` | W0 `UPDATE k0; SAVEPOINT; UPDATE k1; SELECT k0 FOR UPDATE; ROLLBACK TO`; W1 `SELECT k1 FOR KEY SHARE; DELETE k0`; W2 `UPDATE k1; UPDATE k0`; cancel enabled |
//! | 1 | `lockdata` | W0 `UPDATE k0; FOR NO KEY UPDATE k0; SAVEPOINT; UPDATE k1; ROLLBACK TO` |
//! | 2 | `uniq` | W0 `INSERT (t0,u0); INSERT (t1,u0)` |
//! | 3 | `uniqdup` | W0 `SAVEPOINT; INSERT (t0,u0); ROLLBACK TO; INSERT (t0,u0)`; W1 `INSERT (t1,u0)` |
//! | 4 | `ownabsent` | preload row t1 + entry u0→t1; W0 `FOR NO KEY UPDATE t1; INSERT (t1,u1)` |
//! | 5 | `deferrable` | W0 `INSERT (t0,d0)`; W1 `INSERT (t1,d1)` (deferrable unique value, prefix `/i/`, check at commit); cancel enabled |
//! | 6 | `fk` | preload parent t0, no child; W0 `DELETE parent t0` (end-of-stmt check); W1 `read parent; FOR KEY SHARE t0 (FK); INSERT child c1` |
//! | 7 | `keyshare` | preload t0; Ta `DELETE t0`; Tb `INSERT t0` (over the tombstone); R (RR) `FOR KEY SHARE t0` below both |
//! | 8 | `share` | preload t0; W0 `SAVEPOINT; FOR KEY SHARE t0; ROLLBACK TO`; W1 `DELETE t0` |
//! | 9 | `epq` | preload t0=0; W0 `UPDATE t0`; W2 `UPDATE t0`; W1 `UPDATE t0 WHERE v = 0` |
//! | 10 | `epqshare` | preload t0; W0 `UPDATE t0`; W2 `UPDATE t0`; W1 `FOR KEY SHARE t0` |
//! | 11 | `dlk3` | preload t0,t1,c0; W0 `UPDATE t0; UPDATE t1`; W1 `UPDATE t1; UPDATE c0`; W2 `UPDATE c0; UPDATE t0` (3-txn cycle) |
//! | 12 | `epqtomb` | preload t0; W0 `DELETE t0`; W1 `UPDATE t0` |
//! | 13 | `rellock` | preload t0; W0 `LOCK TABLE R EXCLUSIVE`; W1 relation lock R, `UPDATE t0` |
//! | 14 | `ocdup` | preload t0, u0→t0, t1, c1; W0 `DELETE t1; INSERT (t1,u0) ON CONFLICT (u0) DO UPDATE SET v=v+1 WHERE v=0` (AFTER event `UPDATE c1`); W1 `UPDATE t0; INSERT (c0,u0)`; W2 `DELETE t0; DELETE u0` |
//! | 15 | `ocnothing` | preload t0, u0→t0; W0 `INSERT (t1,u0) ON CONFLICT (u0) DO NOTHING`; W1 `DELETE t0; DELETE u0` |
//! | 16 | `fkmoved` | preload parent t0; W0 `DELETE t0 (moved); INSERT t1` (PK change, end-of-stmt check); W1 `read parent; FOR KEY SHARE t0 (FK); INSERT child c1`; W2 `DELETE t0` (plain, end-of-stmt check) |
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
//! | 25 | deferrable | | 53 | ocdup |
//! | 26 | main | | 54 | ocdup |
//! | 58 | fkmoved | | 56 | ocdup |
//! | 59 | dlk3 | | 57 | ocdup |
//! | | | | 64 | ocdup |
//!
//! ## Mutation checks
//!
//! Beyond the seeds, the clean model must catch protocol deviations that are not §11
//! seeds. They are **not** model configurations (review 2): mutation testing is done by
//! temporarily editing the protocol code, running `g0_write_clean_model_holds`, and
//! reverting. The checked mutants, each of which must fail the clean test:
//!
//! - both members of a deadlock cycle raise 40P01 (I-LIVE b; the ghost `d40p01` record
//!   sees the second victim off-cycle once the first victim's edges are gone);
//! - EPQ ignores the WHERE qual (the qual ghost below flags an applied update whose
//!   qual fails on the latest committed version);
//! - an RC UPDATE skips EPQ and places from the stale snapshot (the qual ghost and the
//!   increment oracle both flag the lost update);
//! - an UPDATE's EPQ passes over a tombstone (the qual ghost and the increment oracle
//!   both flag the update of a dead row).
//!
//! Two ghost records back the per-state checks (both written by `next`, read only by
//! `check`, never by the protocol):
//!
//! - **Qual ghost.** When an RC UPDATE/DELETE row op with no own intent completes —
//!   applied (the row lock is granted and the layer placed) or skipped by EPQ — the
//!   ghost evaluates the op's WHERE qual against the latest committed version of the
//!   key at that moment and records the pair (ghost applies?, applied?); `check` flags
//!   a mismatch. (`W_epq`'s `WHERE v = 0` makes both outcomes reachable.)
//! - **40P01 ghost.** Every 40P01 records whether its victim was on a cycle of the
//!   **current** wait-for edges at that moment, computed by `ghost_on_cycle` over the
//!   edge set alone (not by the detector's DFS result and not by any record the
//!   detector writes); `check` flags a victim that was not on a cycle.
//! - **Arbiter ghost** (§5.3.1). Every ON CONFLICT statement that ends with **no**
//!   row outcome records whether the arbiter key's current state (§5.3's rule) is
//!   live at that moment; `check` flags a no-outcome completion over a not-live
//!   arbiter key. §5.3.1(3) never skips — a conflict that vanished must restart
//!   the arbiter and insert — and the legal no-outcome completions (DO NOTHING on
//!   a live conflict, a false DO UPDATE `WHERE` on the locked row) always face a
//!   live arbiter key, so the ghost is sound (it reads the committed state and the
//!   own intent only, never any record the protocol's decision writes).
//! - **Event ghost queue.** Each AFTER event is queued twice: in the protocol's
//!   queue with the tag the code writes, and in a ghost queue with the spec's
//!   `sa`. An abandon discards tag `>= sa` from both. An event that fires while
//!   absent from the ghost queue (seed 64: tagged `seq0`, survived the abandon)
//!   applies its write without a ghost net effect, so the commit oracle flags the
//!   extra increment; the ghost queue itself is never read by the protocol.
//! - **FK error log.** Every FK EPQ failure records whether the parent moved
//!   (§5.3: 23503 vs 40001). Both codes abort in-model — the log is reachability
//!   evidence only (`fkmoved` exercises both: 1272 moved-parent failures in the
//!   clean run), not an oracle.
//!
//! ## Scope cuts
//!
//! - No crash, fsync, epoch or `/sys` records (G0-commit owns them); the KV is one map,
//!   a batch is one atomic write, a view is a copy. Views open and close inside one step
//!   (the FK read, the parent-side scan), so `min(registered view counters)` is always
//!   `u32::MAX`; G0-commit owns the non-trivial truncation view-counter conditions.
//! - I-PROGRESS is checked by the §11 retry bound (a per-actor spin counter, bumped by
//!   each non-parking retry and reset by any other actor's step — and by the actor's
//!   own inline §7.3 removals, which write state and so cannot recur without bound),
//!   not by lasso detection over the counter-dropping abstraction; on these fixed
//!   workloads the bound subsumes the lasso (every spin loop here is unbounded rather
//!   than cyclic).
//! - The write-set log is the in-memory list §5.5 describes (one entry per layer change);
//!   its spill to disk and layer compaction are not modelled. Layer existence (the seqs
//!   of an active txn's layers, from a bug-free ghost log) is checked; layer *contents*
//!   are checked only through the commit oracle (data) and I-LOCK interactions (locks).
//! - Qual semantics: a qual is a single `WHERE v = c` equality on the modelled value
//!   (no expression quals, no subqueries); it is evaluated at the statement scan and by
//!   EPQ against the remembered version, and by the qual ghost against the latest
//!   committed version at the deciding moment. A skipped statement row invisible at S
//!   never reaches EPQ (skipped at the scan), so the ghost only sees targeted rows.
//! - Sequential seeds (review 2 relaxes review 1's all-concurrent rule) are caught in
//!   single-txn workloads because their deviation is bookkeeping inside one txn, with
//!   nothing for a second txn to race: seed 15 (`lockdata`) is layer bookkeeping — a
//!   lock-only layer replacing own data loses the update at commit; seed 19 (`uniq`) is
//!   the unique check reading the txn's own intent; seed 24 (`ownabsent`) is the unique
//!   check falling through an own `Absent` layer to committed state. Their observation
//!   (a lost update, a duplicate key at commit) needs no interleaving; the decoy second
//!   txns of review 1 are removed.
//! - Seed 16 (rollback drops the restored top layer's lock) is caught when a second txn
//!   places over the lock-less pending intent — an I-ONE-INTENT/I-COUNT breach between
//!   two txns. A standing dual-lock state is unreachable from it because §5.1's
//!   holders-check precedes every lock-raising step, so no structural layer-shape rule is
//!   used (and none is needed).
//! - Relation locks have two modes (RowExclusive, Exclusive) exercised by `rellock` only;
//!   their release is fused into the step-5 wake (no per-key latch). No NOWAIT / SKIP
//!   LOCKED / `lock_timeout`, no advisory locks, no SIREAD or
//!   SSI state (G0-ssi), no `WITH HOLD` cursors, no deferred-trigger fixpoint, no NULLS
//!   DISTINCT, and the deadlock DFS runs whenever a cycle exists (`deadlock_timeout` is
//!   not modelled).
//! - ON CONFLICT (C-G0wb) is modelled for RC only: §5.3.1(3)'s "Under RR/SER, if r's
//!   newest committed version is newer than S: 40001 (DO NOTHING too)" is a scope cut
//!   (every ON CONFLICT txn here is RC), as are multi-row arbiter statements, arbing on
//!   a PK or a deferrable index, `SET CONSTRAINTS`, and BEFORE triggers. Moved
//!   tombstones (`Delete { moved }`, §2.1) exist for the FK-parent path: §5.3's FK
//!   EPQ failure raises 23503, or 40001 when the parent moved (both abort in-model;
//!   the codes are recorded in `errs`); the general §5.2 rule "EPQ that reaches a
//!   moved-tombstone raises 40001" is a scope cut — no non-FK row op here meets one.
//!   The ON CONFLICT attempt's queued *checks* are modelled through AFTER events
//!   (same tag-and-discard rule, §5.5); end-of-statement FK checks remain
//!   statement-end scans. An attempt abandons only from the insert path (§5.3.1(2));
//!   the arbiter lock's waits follow §5.1 directly.
//! - Two C-G0wb conformance notes. (1) The unique check now follows §5.3's current-state
//!   rule exactly: an own `Delete` top layer is *not* live ("own `Delete` → proceed"),
//!   so a txn can delete and re-insert one key; C-G0wa's code read the committed state
//!   there, which no pre-C-G0wb workload exercises (no delete-then-insert of one key),
//!   and `ocdup` needs the spec's behaviour. (2) The I-UNIQUE oracle now tracks entry
//!   liveness (`Entry` live, `Dead` kills): C-G0wa's workloads never deleted a unique
//!   entry, so the check misread a legal row+entry delete as an orphaned entry;
//!   `ocdup`/`ocnothing`/`fkmoved` delete entries.

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
        Data::Delete { .. } => Lock::Update,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Data {
    Absent,
    Write {
        val: u8,
        kc: bool,
    },
    /// §2.1 `Delete { moved }`: a PK-changing UPDATE's moved-tombstone at the old
    /// `/t/` key (C-G0wb; exercised by the FK-child workload).
    Delete {
        moved: bool,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum VerData {
    Live { val: u8, kc: bool },
    Tomb { moved: bool },
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
        Data::Delete { moved } => Some(VerData::Tomb { moved }),
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
    /// Seed 53: ON CONFLICT lock runs EPQ and skips a deleted conflicting row.
    ArbLockEpq,
    /// Seed 54: abandoned ON CONFLICT attempt removes whole intents instead of
    /// rolling back to its internal savepoint.
    AbandonWholeIntent,
    /// Seed 56: ON CONFLICT attempt writes at `seq0` instead of `sa`, so abandoning
    /// leaves its new arbiter intent.
    ArbSeq0,
    /// Seed 57: arbiter lock measures newer versions against `S` instead of `v_r`.
    ArbBaseS,
    /// Seed 58: FK child check skips on a failed EPQ instead of raising 23503.
    FkEpqSkip,
    /// Seed 64: ON CONFLICT attempt's queued checks or AFTER events tagged `seq0`,
    /// surviving an abandon.
    EventTagSeq0,
}

impl Bug {
    pub const ALL: [Bug; 27] = [
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
        Bug::ArbLockEpq,
        Bug::AbandonWholeIntent,
        Bug::ArbSeq0,
        Bug::ArbBaseS,
        Bug::FkEpqSkip,
        Bug::EventTagSeq0,
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
            Bug::ArbLockEpq => 53,
            Bug::AbandonWholeIntent => 54,
            Bug::ArbSeq0 => 56,
            Bug::ArbBaseS => 57,
            Bug::FkEpqSkip => 58,
            Bug::EventTagSeq0 => 64,
        }
    }
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
    /// §2.1 `Delete { moved }`: a PK-changing UPDATE writes `moved: true` at the
    /// old `/t/` key (plus the new row's key-existence ops).
    Delete {
        key: LKey,
        moved: bool,
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
    /// §5.3.1 `INSERT ... ON CONFLICT (arb) DO UPDATE/NOTHING`. Runs the arbiter
    /// protocol; on the insert path the ops that follow it in the statement are
    /// the proposed row and its entries, placed at the attempt's `sa` (§5.3.1).
    /// An entry key's live version value names the owning row (`KEYS` index), so
    /// the pre-check can find `r`. `qual` is the DO UPDATE's `WHERE v = qual`
    /// (evaluated on the locked row; a false qual leaves the lock, §5.3.1(3));
    /// `ev` is an AFTER-trigger internal `UPDATE <key> v = v + 1` queued (tagged
    /// `sa`) per row outcome.
    OnConflict {
        arb: LKey,
        row: LKey,
        upd: bool,
        qual: Option<u8>,
        ev: Option<LKey>,
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
            Op::Delete {
                key: LKey::T0,
                moved: false,
            },
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

/// W_lockdata: seed 15 — the lock-only layer before the savepoint survives `ROLLBACK TO`,
/// so a lock-only layer that replaced own data (seed 15) commits `Absent` and loses the
/// update (sequential: the deviation is layer bookkeeping inside W0).
const W_LOCKDATA: Workload = Workload {
    txns: &[rc![
        stmt![upd!(LKey::T0)],
        stmt![Op::LockOnly {
            key: LKey::T0,
            upd: false,
        }],
        stmt![Op::Savepoint],
        stmt![upd!(LKey::T1)],
        stmt![Op::RollbackTo],
    ]],
    preload: &[
        pre_row!(LKey::T0, 0, None, false),
        pre_row!(LKey::T1, 0, None, false),
    ],
    cancel: false,
    deferrable: false,
};

/// W_uniq: seed 19 — the second insert's unique check must see W0's own live entry
/// (sequential: the check reads the txn's own intent).
const W_UNIQ: Workload = Workload {
    txns: &[rc![
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
    ]],
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
/// (live) even though W0's own top layer on t1 is `Absent` (lock-only); sequential: the
/// check falls through the txn's own layer stack.
const W_OWNABSENT: Workload = Workload {
    txns: &[rc![
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
    ]],
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
        rc![pcheck![Op::Delete {
            key: LKey::T0,
            moved: false,
        }]],
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
        rc![stmt![Op::Delete {
            key: LKey::T0,
            moved: false,
        }]],
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
        rc![stmt![Op::Delete {
            key: LKey::T0,
            moved: false,
        }]],
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
        rc![stmt![Op::Delete {
            key: LKey::T0,
            moved: false,
        }]],
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

/// W_ocdup (C-G0wb): the §5.3.1 ON CONFLICT DO UPDATE workload. W0 deletes t1
/// (the pre-`sa` layer the seed-54 abandon must keep), then runs the arbiter
/// statement (`WHERE v = 0`, AFTER event `UPDATE c1`). W1 updates the
/// conflicting row and then inserts the racing row (seed 57: `v_r` above W0's
/// `S`; seeds 54/56/64: the insert-path abandon — over a wait or a live entry —
/// and the events queued during the attempt). W2 only deletes row+entry and
/// stays deleted (seed 53: the conflicting row dies between the pre-check and
/// the lock, so the arbiter key is *not* live when the buggy EPQ skips).
const W_OCDUP: Workload = Workload {
    txns: &[
        rc![
            stmt![Op::Delete {
                key: LKey::T1,
                moved: false,
            }],
            stmt![
                Op::OnConflict {
                    arb: LKey::U0,
                    row: LKey::T1,
                    upd: true,
                    qual: Some(0),
                    ev: Some(LKey::C1),
                },
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
        rc![
            stmt![upd!(LKey::T0)],
            stmt![
                Op::KeyExist {
                    key: LKey::C0,
                    kind: KeyKind::Pk {
                        uval: Some(LKey::U0),
                        child: false,
                    },
                },
                Op::KeyExist {
                    key: LKey::U0,
                    kind: KeyKind::Unique { row: LKey::C0 },
                },
            ],
        ],
        rc![stmt![
            Op::Delete {
                key: LKey::T0,
                moved: false,
            },
            Op::Delete {
                key: LKey::U0,
                moved: false,
            },
        ]],
    ],
    preload: &[
        pre_row!(LKey::T0, 0, Some(LKey::U0), false),
        (
            LKey::U0,
            1,
            VerData::Live { val: 0, kc: false },
            ED::Entry { row: LKey::T0 },
        ),
        pre_row!(LKey::T1, 0, None, false),
        pre_row!(LKey::C1, 0, None, false),
    ],
    cancel: false,
    deferrable: false,
};

/// W_ocnothing: the §5.3.1 DO NOTHING shape — a live conflict skips the
/// proposed row; a conflict deleted by W1 before the pre-check lets the insert
/// path run (coverage workload; no owned seed).
const W_OCNOTHING: Workload = Workload {
    txns: &[
        rc![stmt![
            Op::OnConflict {
                arb: LKey::U0,
                row: LKey::T1,
                upd: false,
                qual: None,
                ev: None,
            },
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
        rc![stmt![
            Op::Delete {
                key: LKey::T0,
                moved: false,
            },
            Op::Delete {
                key: LKey::U0,
                moved: false,
            },
        ]],
    ],
    preload: &[
        pre_row!(LKey::T0, 0, Some(LKey::U0), false),
        (
            LKey::U0,
            1,
            VerData::Live { val: 0, kc: false },
            ED::Entry { row: LKey::T0 },
        ),
    ],
    cancel: false,
    deferrable: false,
};

/// W_fkmoved (C-G0wb): the §5.3 FK-child EPQ workload. W1 reads the parent,
/// takes the FK KEY SHARE and inserts the child. W0 moves the parent (a
/// PK-changing UPDATE: moved-tombstone at t0 plus the new key t1 — the EPQ
/// reaches the moved-tombstone and raises 40001); W2 plainly deletes it (the
/// EPQ fails on the tombstone and raises 23503 — seed 58's skip must not
/// happen). Both movers run the parent-side end-of-statement check.
const W_FKMOVED: Workload = Workload {
    txns: &[
        rc![pcheck![
            Op::Delete {
                key: LKey::T0,
                moved: true,
            },
            Op::KeyExist {
                key: LKey::T1,
                kind: KeyKind::Pk {
                    uval: None,
                    child: false,
                },
            },
        ]],
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
        rc![pcheck![Op::Delete {
            key: LKey::T0,
            moved: false,
        }]],
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
    W_OCDUP,
    W_OCNOTHING,
    W_FKMOVED,
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
    /// The ON CONFLICT insert path found a conflict or a wait on the arbiter
    /// key (§5.3.1(2)): roll back to `sa` (one latched section per visited key),
    /// then restart from the pre-check (after the wait, if one was needed).
    ArbAb,
    /// End-of-statement AFTER events pending: the head of `evq` runs as an
    /// internal command at a fresh seq (§5.3).
    EvSnap,
    /// The head event's write applied; its ghost net effect and the queue
    /// advance next.
    EvNext,
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

/// §5.3.1 attempt state: the internal savepoint's seq, the conflicting row and
/// its newest committed data version `v_r`, and the DO UPDATE's WHERE qual.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct ArbSt {
    sa: u8,
    r: LKey,
    v_r: (Ts, VerData),
    qual: Option<u8>,
}

/// A queued AFTER-trigger event (§5.3.1/§5.5): `(tag, target)`. The protocol
/// queue `evq` carries the tags the code under test writes (seed 64: `seq0`);
/// the ghost queue `gevq` always carries the spec's `sa` and is the oracle's
/// notion of which events are live.
type EvEnt = (u8, LKey);

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
    /// The command seq layer placement uses (§2.1/§5.3.1): `seq0` for ordinary
    /// statements and internal event commands, the attempt's `sa` inside an
    /// ON CONFLICT attempt.
    cmd_seq: u8,
    /// ON CONFLICT attempt in flight (§5.3.1).
    arb: Option<ArbSt>,
    /// The attempt is on the insert path (the ops after the OnConflict op are
    /// the proposed row and its entries).
    ains: bool,
    /// The attempt produced a row outcome (a placed insert or update).
    aeff: bool,
    /// The current synthesized op is the §5.3.1(3) arbiter lock (never runs EPQ).
    alock: bool,
    /// The current op is an internal event command; its ghost net effect is
    /// recorded at `EvNext` (from the ghost event queue), not at placement.
    evop: bool,
    /// The synthesized current op (the arbiter lock/update, an event command).
    synth: Option<Op>,
    /// Queued AFTER events: the protocol's queue (tags as written) and the ghost's.
    evq: Vec<EvEnt>,
    gevq: Vec<EvEnt>,
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
            cmd_seq: 0,
            arb: None,
            ains: false,
            aeff: false,
            alock: false,
            evop: false,
            synth: None,
            evq: Vec::new(),
            gevq: Vec::new(),
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
/// retry loop (reset), continued the loop without retrying (keep), retried
/// without parking (bump), or retried after itself changing intent state — an
/// inline §7.3 removal writes the KV, so it cannot recur without bound and
/// resets the backstop (§11: "preceded by ... a change of committed or intent
/// state").
#[derive(Clone, Copy, PartialEq, Eq)]
enum Spin {
    Done,
    Mid,
    Retry,
    Removed,
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
    /// Ghost: every raised 40P01 with whether its victim was on a cycle of the
    /// **current** wait-for edges at that moment (I-LIVE b, §6) — computed by
    /// `ghost_on_cycle` over the edge set, not by the detector.
    d40p01: Vec<(u8, bool)>,
    /// Ghost: outcome of every completed RC UPDATE/DELETE row op with no own
    /// intent — (txn, key, the qual evaluates on the latest committed version,
    /// the op applied) — `check` flags a mismatch (review rework 2).
    qlog: Vec<(u8, LKey, bool, bool)>,
    /// Ghost (§5.3.1): every ON CONFLICT statement that ended with **no** row
    /// outcome — (txn, arbiter key, the key's current state is live). §5.3.1(3)
    /// never skips: a vanished conflict must restart the arbiter; `check` flags
    /// a no-outcome completion over a not-live arbiter key.
    alog: Vec<(u8, LKey, bool)>,
    /// Observation log: every FK EPQ failure with whether the parent moved
    /// (§5.3: 23503 vs 40001; both abort in-model — the codes are recorded as
    /// reachability evidence only).
    errs: Vec<(u8, bool)>,
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
    /// §5.3.1 attempt: take the internal savepoint `sa` (when fresh) and run the
    /// arbiter pre-check on `latch_key(arb)`.
    ArbPre(u8),
    /// Abandon the attempt: roll back to `sa` (§5.5), then restart from the
    /// pre-check (after the pending wait, if any).
    Abandon(u8),
    /// Start the head AFTER event's internal command (§5.3).
    EvSnap(u8),
    /// Finish the head event (ghost net effect, queue advance).
    EvNext(u8),
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
        | Op::Delete { key, .. }
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
        if let Some(op) = t.synth {
            return op;
        }
        WORKLOADS[self.wl as usize].txns[w as usize].stmts[t.stmt as usize].ops[t.op as usize]
    }

    /// The OnConflict op of `w`'s current statement, if any (None before `Choose`
    /// or for statements without one).
    fn oc_of(&self, w: u8) -> Option<Op> {
        let t = &self.txns[w as usize];
        if (t.stmt as usize) < WORKLOADS[self.wl as usize].txns[w as usize].stmts.len() {
            WORKLOADS[self.wl as usize].txns[w as usize].stmts[t.stmt as usize]
                .ops
                .iter()
                .find(|o| matches!(o, Op::OnConflict { .. }))
                .copied()
        } else {
            None
        }
    }

    /// §5.3's "current state" liveness of key `k` for txn `w`, bug-free: the own
    /// intent's top-layer data if it is `Write` (live) or `Delete` (not live —
    /// §5.3: "own `Delete` → proceed"), otherwise the newest committed version.
    fn cur_live(&self, k: LKey, w: u8) -> bool {
        let own_top = match &self.slot(k).intent {
            Some(i) if i.owner == w => Some(top_layer(i).data),
            _ => None,
        };
        match own_top {
            Some(Data::Write { .. }) => true,
            Some(Data::Delete { .. }) => false,
            _ => matches!(self.committed_state(k), Some((_, VerData::Live { .. }))),
        }
    }

    /// The row a live entry key `k` (whose live version's value names the owning
    /// row, `KEYS`-indexed) points at, for txn `w`'s current-state read.
    fn cur_row(&self, k: LKey, w: u8, dflt: LKey) -> LKey {
        let val = match &self.slot(k).intent {
            Some(i) if i.owner == w => match top_layer(i).data {
                Data::Write { val, .. } => Some(val),
                _ => None,
            },
            _ => None,
        };
        let val = val.or(match self.committed_state(k) {
            Some((_, VerData::Live { val, .. })) => Some(val),
            _ => None,
        });
        KEYS.get(val.map_or(256, |v| v as usize))
            .copied()
            .unwrap_or(dflt)
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
                            Data::Delete { .. } => None,
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
                        Data::Delete { .. } => return None,
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
        // Ghost logs: each (txn, stmt, op) — and each 40P01 raiser — records at
        // most once, so sorting collapses states that differ only in the order
        // completions happened in.
        self.qlog.sort_unstable();
        self.qlog.dedup();
        self.d40p01.sort_unstable();
        self.d40p01.dedup();
        self.alog.sort_unstable();
        self.alog.dedup();
        self.errs.sort_unstable();
        self.errs.dedup();
        // (`evq`/`gevq` keep their order: events fire FIFO, §5.3.)
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
        for k in s.intents_of(w) {
            s.cleanup_q.push((w, k));
        }
        self.wake(s, w);
        s.txns[w as usize].phase = Phase::Done { aborted: true };
    }

    /// Raise 40P01 for `w` (§6): the ghost first records whether the victim was
    /// on a cycle of the **current** wait-for edges at this moment (I-LIVE b),
    /// then the victim removes its own edges and aborts.
    fn raise_40p01(&self, s: &mut State, w: u8) {
        let on = ghost_on_cycle(&s.edges, w);
        s.d40p01.push((w, on));
        s.edges.retain(|(x, _, _)| *x != w);
        self.do_fail(s, w);
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
        let cseq = s.txns[w as usize].cmd_seq;
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
            Op::Delete { moved, .. } => (Data::Delete { moved }, seq0),
            Op::KeyExist { kind, .. } => {
                // An entry key's value names the owning row (KEYS index), so the
                // §5.3.1 pre-check can find `r`; a row key carries the row value.
                let val = match kind {
                    KeyKind::Pk { .. } => 1,
                    KeyKind::Unique { row } | KeyKind::Def { row } => row as u8,
                };
                (Data::Write { val, kc: false }, seq0)
            }
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
        let seq = cseq.max(top_seq);
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
        // Ghost net effect of the surviving statements. An internal event
        // command's effect is recorded at `EvNext` (from the ghost event queue),
        // so an event that should have been discarded by an abandon (seed 64)
        // applies its write without a ghost effect — the commit oracle flags it.
        if !s.txns[w as usize].evop {
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
        }
        // §5.3.1: a row outcome of an attempt queues its AFTER event, tagged
        // `sa` (seed 64 tags it `seq0`, so an abandon no longer discards it).
        if let Some(a) = s.txns[w as usize].arb {
            let outcome = (s.txns[w as usize].ains
                && is_key_exist(op)
                && key
                    == s.oc_of(w).map_or(a.r, |o| match o {
                        Op::OnConflict { row, .. } => row,
                        _ => a.r,
                    }))
                || (!s.txns[w as usize].ains
                    && s.txns[w as usize].synth.is_some()
                    && is_data_row_op(op));
            if outcome {
                let ev = s.oc_of(w).and_then(|o| match o {
                    Op::OnConflict { ev, .. } => ev,
                    _ => None,
                });
                if let Some(target) = ev {
                    let tag = if self.bug == Some(Bug::EventTagSeq0) {
                        seq0
                    } else {
                        a.sa
                    };
                    s.txns[w as usize].aeff = true;
                    s.txns[w as usize].evq.push((tag, target));
                    s.txns[w as usize].gevq.push((a.sa, target));
                } else {
                    s.txns[w as usize].aeff = true;
                }
            }
        }
        // Ghost (review rework 2): an RC row op applied with no own intent — the
        // moment the row lock is granted — against the qual evaluated on the
        // latest committed version.
        if !own && s.iso(w) == Iso::Rc && is_data_row_op(op) {
            let qual = match op {
                Op::Write { qual, .. } => qual,
                _ => None,
            };
            let ghost = ghost_qual(s, key, qual);
            s.qlog.push((w, key, ghost, true));
        }
        // §5.3.1(3): the arbiter lock is placed; the DO UPDATE is computed from
        // the locked row (own data if any, else `v_r`) and applied through §5.4.
        // A false WHERE leaves the lock and ends the statement with no outcome.
        if s.txns[w as usize].alock && matches!(op, Op::LockOnly { .. }) {
            let a = s.txns[w as usize].arb.unwrap_or(ArbSt {
                sa: 0,
                r: key,
                v_r: (0, VerData::Tomb { moved: false }),
                qual: None,
            });
            let val = s.read_own(key, a.v_r.0, w, seq0);
            let applies = match a.qual {
                Some(q) => val == Some(q),
                None => true,
            };
            {
                let txn = &mut s.txns[w as usize];
                txn.base = a.v_r.0;
                txn.epq_count = 0;
                txn.epq_v = None;
                txn.epq_n.clear();
                txn.wait = None;
                txn.wait_cc = false;
                txn.holders.clear();
            }
            if applies {
                s.txns[w as usize].synth = Some(Op::Write {
                    key: a.r,
                    kc: false,
                    qual: None,
                });
                s.txns[w as usize].phase = Phase::Op;
            } else {
                // A false WHERE leaves the lock (§5.3.1(3)); the statement ends
                // with no outcome (the arbiter ghost checks liveness).
                s.txns[w as usize].alock = false;
                s.txns[w as usize].synth = None;
                self.advance_op(s, w);
            }
            return;
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
                        VerData::Tomb { .. } => true,
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
        let evop;
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
            txn.alock = false;
            txn.synth = None;
            evop = txn.evop;
        }
        if evop {
            // The head AFTER event's write applied; its ghost net effect and the
            // queue advance in `EvNext` (§5.3).
            s.txns[w as usize].phase = Phase::EvNext;
            return;
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
        if s.txns[w as usize].arb.is_some() {
            // The ON CONFLICT statement's ops are done: end the attempt (§5.3.1),
            // then run the queued AFTER events before the next statement.
            self.arb_done(s, w);
            return;
        }
        self.advance_stmt(s, w);
    }

    /// End of an ON CONFLICT statement (§5.3.1): pop the attempt's savepoint
    /// push, record the no-outcome ghost (a no-outcome completion requires the
    /// conflict to still exist), clear the attempt, then drain AFTER events.
    fn arb_done(&self, s: &mut State, w: u8) {
        let (arb_key, aeff, sa) = match s.oc_of(w) {
            Some(Op::OnConflict { arb, .. }) => (
                arb,
                s.txns[w as usize].aeff,
                s.txns[w as usize].arb.map_or(0, |a| a.sa),
            ),
            _ => (LKey::U0, false, 0),
        };
        if s.txns[w as usize].sp.last().is_some_and(|(q, _)| *q == sa) {
            s.txns[w as usize].sp.pop();
        }
        if !aeff {
            let live = s.cur_live(arb_key, w);
            s.alog.push((w, arb_key, live));
        }
        {
            let txn = &mut s.txns[w as usize];
            txn.arb = None;
            txn.ains = false;
            txn.aeff = false;
            txn.alock = false;
            txn.synth = None;
            txn.evop = false;
            txn.cmd_seq = txn.seq0;
        }
        if s.txns[w as usize].evq.is_empty() {
            self.advance_stmt(s, w);
        } else {
            s.txns[w as usize].phase = Phase::EvSnap;
        }
    }

    /// §5.3: the head AFTER event runs as an internal command at a fresh seq
    /// (greater than the statement's), reading the latest committed state plus
    /// own writes through §5.1 like any row op.
    fn do_ev_snap(&self, s: &mut State, w: u8) {
        let head = s.txns[w as usize].evq.first().copied();
        let visible = s.visible_ts;
        {
            let txn = &mut s.txns[w as usize];
            txn.seq += 1;
            txn.seq0 = txn.seq;
            txn.cmd_seq = txn.seq;
            txn.observing = 0;
            txn.snap = Some(visible);
            txn.base = visible;
            txn.skip = 0;
        }
        let Some((tag, target)) = head else {
            self.advance_stmt(s, w);
            return;
        };
        let seq0 = s.txns[w as usize].seq0;
        if s.read_own(target, visible, w, seq0).is_none() {
            // The internal UPDATE's scan finds no row: the event is skipped;
            // both queues drop it.
            let txn = &mut s.txns[w as usize];
            txn.evq.remove(0);
            if let Some(i) = txn.gevq.iter().position(|e| *e == (tag, target)) {
                txn.gevq.remove(i);
            }
            if txn.evq.is_empty() {
                self.advance_stmt(s, w);
            }
            return;
        }
        let txn = &mut s.txns[w as usize];
        txn.evop = true;
        txn.synth = Some(Op::Write {
            key: target,
            kc: false,
            qual: None,
        });
        txn.phase = Phase::Op;
    }

    /// The head event's write applied (§5.3): record its ghost net effect iff it
    /// is still live in the ghost queue (an event an abandon should have
    /// discarded — seed 64 — applies without a ghost effect), then continue.
    fn do_ev_next(&self, s: &mut State, w: u8) {
        let head = s.txns[w as usize].evq.first().copied();
        {
            let txn = &mut s.txns[w as usize];
            txn.evop = false;
            txn.synth = None;
        }
        let Some((tag, target)) = head else {
            self.advance_stmt(s, w);
            return;
        };
        let txn = &mut s.txns[w as usize];
        txn.evq.remove(0);
        if let Some(i) = txn.gevq.iter().position(|e| *e == (tag, target)) {
            txn.gevq.remove(i);
            txn.expected[target as usize] = match txn.expected[target as usize] {
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
        if txn.evq.is_empty() {
            self.advance_stmt(s, w);
        }
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
                    txn.cmd_seq = txn.seq0;
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

    // ---- the §5.3.1 arbiter protocol ----

    /// Is `w`'s current op the arbiter entry op of an in-flight insert-path
    /// attempt? §5.3.1(2): its unique check finding a live entry of another
    /// row, or a wait, abandons the attempt.
    fn is_arb_entry(&self, s: &State, w: u8, key: LKey) -> bool {
        match s.txns[w as usize].arb {
            Some(a) => {
                let arb_key = s.oc_of(w).map_or(a.r, |o| match o {
                    Op::OnConflict { arb, .. } => arb,
                    _ => a.r,
                });
                s.txns[w as usize].ains
                    && matches!(s.cur_op(w), Op::KeyExist { .. })
                    && key == arb_key
            }
            None => false,
        }
    }

    /// §5.3.1 "restart from 1": drop the attempt's (write-less) savepoint push
    /// and re-enter the pre-check as a fresh attempt (a fresh `sa`).
    fn restart_arb(&self, s: &mut State, w: u8) {
        let sa = s.txns[w as usize].arb.map_or(0, |a| a.sa);
        if s.txns[w as usize].sp.last().is_some_and(|(q, _)| *q == sa) {
            s.txns[w as usize].sp.pop();
        }
        let txn = &mut s.txns[w as usize];
        txn.arb = None;
        txn.ains = false;
        txn.aeff = false;
        txn.alock = false;
        txn.synth = None;
        txn.evop = false;
        txn.cmd_seq = txn.seq0;
        txn.op = 0;
        txn.phase = Phase::Op;
    }

    /// §5.3.1 step 1: take the attempt's internal savepoint at a fresh `sa`
    /// (only when no attempt is in flight — a wait-restart re-uses it, having
    /// written nothing) and run the arbiter pre-check on `latch_key(arb)`.
    fn do_arb_pre(&self, s: &mut State, w: u8) -> Spin {
        let (arb_key, row_key, upd) = match s.cur_op(w) {
            Op::OnConflict { arb, row, upd, .. } => (arb, row, upd),
            // Only dispatched for the OnConflict op (actions); stay sound anyway.
            _ => return Spin::Done,
        };
        if s.txns[w as usize].arb.is_none() {
            let qual = match s.cur_op(w) {
                Op::OnConflict { qual, .. } => qual,
                _ => None,
            };
            let txn = &mut s.txns[w as usize];
            txn.seq += 1;
            let sa = txn.seq;
            txn.cmd_seq = if self.bug == Some(Bug::ArbSeq0) {
                // Seed 56: the attempt's writes place at seq0.
                txn.seq0
            } else {
                sa
            };
            let exp = txn.expected;
            txn.sp.push((sa, exp));
            txn.arb = Some(ArbSt {
                sa,
                r: row_key,
                v_r: (0, VerData::Tomb { moved: false }),
                qual,
            });
            txn.ains = false;
            txn.aeff = false;
            txn.alock = false;
        }
        let sa = s.txns[w as usize].arb.map_or(0, |a| a.sa);
        let qual = s.txns[w as usize].arb.and_then(|a| a.qual);
        s.latch.push((lk_of(arb_key), w));
        // 1. Foreign intent on the arbiter key, as §5.1's block (§5.3.1(1)).
        if let Some(i) = s.slot(arb_key).intent.clone() {
            if i.owner != w {
                let owner = i.owner;
                let top = top_layer(&i);
                if self.foreign_ended(s, owner) {
                    let committed_visible = matches!(
                        s.status.get(&owner).map(|e| e.st),
                        Some(St::Committed(c)) if c <= s.visible_ts
                    );
                    self.remove_intent(s, owner, arb_key, true);
                    if committed_visible {
                        s.txns[w as usize].observing |= 1 << owner;
                    }
                    unlatch(s, w);
                    return Spin::Removed; // re-read (the removal wrote state)
                }
                if top.data != Data::Absent {
                    // Pending or committed-not-visible: wait_for, restart from 1.
                    let g = s.status.get(&owner).map(|e| e.gen).unwrap_or(0);
                    let txn = &mut s.txns[w as usize];
                    txn.wait = Some(vec![(owner, g)]);
                    txn.wait_kind = EdgeKind::Key(arb_key, Lock::NoKeyUpd);
                    txn.wait_cc = false;
                    txn.phase = Phase::WaitReg;
                    unlatch(s, w);
                    return Spin::Mid;
                }
                // A lock-only foreign intent does not block the state read.
            }
        }
        // 2. The key's current state (§5.3's unique-check rule): a live entry of
        //    another row r is the conflict (§5.3.1(1) -> 3).
        let live = s.cur_live(arb_key, w);
        let r_conflict = live && s.cur_row(arb_key, w, row_key) != row_key;
        if r_conflict {
            let r = s.cur_row(arb_key, w, row_key);
            // v_r: the newest committed data version of r's /t/ key, read from a
            // registered view (§3.1 — the read is outside r's latch).
            s.view_counter += 1;
            if let Some(v_r) = s.committed_state(r) {
                let nops = WORKLOADS[s.wl as usize].txns[w as usize].stmts
                    [s.txns[w as usize].stmt as usize]
                    .ops
                    .len() as u8;
                let vis = s.visible_ts;
                s.txns[w as usize].arb = Some(ArbSt { sa, r, v_r, qual });
                if !upd {
                    // DO NOTHING: skip the proposed row (§5.3.1(3)); the
                    // statement ends with no outcome.
                    s.txns[w as usize].op = nops;
                    unlatch(s, w);
                    self.scan_ready(s, w); // -> arb_done (the ghost checks liveness)
                    return Spin::Done;
                }
                let txn = &mut s.txns[w as usize];
                txn.op = nops; // skip the proposed row's ops
                txn.base = if self.bug == Some(Bug::ArbBaseS) {
                    // Seed 57: measure newer versions against S, not v_r.
                    txn.snap.unwrap_or(vis)
                } else {
                    v_r.0
                };
                txn.alock = true;
                txn.synth = Some(Op::LockOnly { key: r, upd: false });
                txn.phase = Phase::Op;
                unlatch(s, w);
                return Spin::Mid;
            }
        }
        // 3. Insert path (§5.3.1(2)): the proposed row and its entries follow as
        //    key-existence ops through §5.1, placed at the attempt's `sa`.
        s.txns[w as usize].ains = true;
        s.txns[w as usize].op = 1;
        unlatch(s, w);
        self.scan_ready(s, w);
        Spin::Done
    }

    /// §5.3.1(2) abandon: roll back to `sa` exactly as `ROLLBACK TO SAVEPOINT`
    /// (§5.5 — only the layers this attempt pushed or modified are dropped, an
    /// intent is removed only if no layer remains), discard the checks and AFTER
    /// events queued during the attempt, wake waiters, then restart from the
    /// pre-check (after the pending wait, if one was needed).
    fn do_abandon(&self, s: &mut State, w: u8) -> Spin {
        let (sa, exp) = match s.txns[w as usize].sp.pop() {
            Some(x) => x,
            None => (u8::MAX, s.txns[w as usize].expected),
        };
        let keys: Vec<LKey> = s.txns[w as usize]
            .wlog
            .iter()
            .filter(|(q, _)| *q >= sa)
            .map(|(_, k)| *k)
            .collect();
        for k in keys {
            if self.bug == Some(Bug::AbandonWholeIntent) {
                // Seed 54: remove whole intents, losing layers below sa.
                self.remove_intent(s, w, k, true);
                continue;
            }
            let layers = s
                .slot(k)
                .intent
                .clone()
                .map(|i| i.layers)
                .unwrap_or_default();
            let kept: Vec<Layer> = layers.iter().copied().filter(|l| l.seq < sa).collect();
            if kept.is_empty() {
                self.remove_intent(s, w, k, true);
            } else {
                s.kv.entry(k).or_default().intent = Some(Intent {
                    owner: w,
                    layers: kept,
                });
            }
        }
        {
            let txn = &mut s.txns[w as usize];
            txn.wlog.retain(|(q, _)| *q < sa);
            txn.glog.retain(|(q, _)| *q < sa);
            // §5.5: pending checks and queued AFTER events with tag >= sa are
            // discarded. The ghost queue always carries the spec's `sa`; the
            // protocol queue carries what the code wrote (seed 64: `seq0`).
            txn.evq.retain(|(tag, _)| *tag < sa);
            txn.gevq.retain(|(tag, _)| *tag < sa);
            txn.expected = exp;
            txn.arb = None;
            txn.ains = false;
            txn.aeff = false;
            txn.alock = false;
            txn.synth = None;
            txn.evop = false;
            txn.cmd_seq = txn.seq0;
            txn.op = 0;
        }
        self.release_shared(s, w, sa);
        // §5.5/§5.3.1: bump the wake generation so waiters re-run.
        self.wake(s, w);
        let has_wait = s.txns[w as usize].wait.is_some();
        s.txns[w as usize].phase = if has_wait { Phase::WaitReg } else { Phase::Op };
        Spin::Retry
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
                    return Spin::Removed; // `continue` (the removal wrote state)
                }
                // R3W-8: no wait on a lock-only intent over a live row.
                if is_key_exist(op)
                    && top.data == Data::Absent
                    && matches!(s.committed_state(key), Some((_, VerData::Live { .. })))
                {
                    unlatch(s, w);
                    if self.is_arb_entry(s, w, key) {
                        // §5.3.1(2): the arbiter key's unique check found a live
                        // entry of another row: abandon the attempt.
                        s.txns[w as usize].phase = Phase::ArbAb;
                        return Spin::Mid;
                    }
                    self.do_fail(s, w); // 23505
                    return Spin::Done;
                }
                if conflicts(m, top.lock) {
                    let g = s.status.get(&owner).map(|e| e.gen).unwrap_or(0);
                    s.txns[w as usize].wait = Some(vec![(owner, g)]);
                    s.txns[w as usize].wait_kind = EdgeKind::Key(key, m);
                    s.txns[w as usize].wait_cc = false;
                    unlatch(s, w);
                    if self.is_arb_entry(s, w, key) {
                        // §5.3.1(2): must wait — abandon the attempt first, then
                        // wait, then restart from the pre-check.
                        s.txns[w as usize].phase = Phase::ArbAb;
                    } else {
                        s.txns[w as usize].phase = Phase::WaitReg;
                    }
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
                        VerData::Tomb { .. } => true,
                        VerData::Live { kc, .. } => *kc,
                    });
                    if bad {
                        unlatch(s, w);
                        self.do_fail(s, w); // 40001
                        return Spin::Done;
                    }
                    // KEY SHARE may proceed over plain newer writes.
                } else {
                    // RC.
                    if s.txns[w as usize].alock {
                        // §5.3.1(3): the ON CONFLICT lock never runs EPQ — with
                        // base = v_r.ts a non-empty N means the row changed
                        // since the pre-check, so the arbiter restarts (always
                        // preceded by a foreign change of state, §5.3.1).
                        if self.bug != Some(Bug::ArbLockEpq) {
                            unlatch(s, w);
                            self.restart_arb(s, w);
                            return Spin::Retry;
                        }
                        // Seed 53: the lock runs EPQ like a plain row op, so a
                        // deleted conflicting row is skipped instead of restarting.
                    }
                    // EPQ (§5.2), unlatched.
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
                // §5.3: the current state is the own intent's top layer if it is
                // `Write` (live) or `Delete` (not live — "own `Delete` → proceed",
                // so a delete-then-reinsert of one key in a txn inserts), else
                // the newest committed version.
                s.cur_live(key, w)
            };
            if live {
                unlatch(s, w);
                if self.is_arb_entry(s, w, key) {
                    // §5.3.1(2): the arbiter key's unique check found a live
                    // entry of another row: abandon the attempt.
                    s.txns[w as usize].phase = Phase::ArbAb;
                    return Spin::Mid;
                }
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
        s.txns[w as usize].phase = if self.is_arb_entry(s, w, key) {
            // §5.3.1(2): the arbiter entry must wait — abandon first.
            Phase::ArbAb
        } else {
            Phase::WaitReg
        };
        unlatch(s, w);
        Spin::Mid
    }

    /// §5.2 EPQ: re-evaluate the quals against the remembered version(s), unlatched.
    fn do_epq_step(&self, s: &mut State, w: u8) -> Spin {
        let v = s.txns[w as usize]
            .epq_v
            .unwrap_or((0, VerData::Tomb { moved: false }));
        let n = s.txns[w as usize].epq_n.clone();
        let op = s.cur_op(w);
        // The ON CONFLICT lock re-evaluates the DO UPDATE's WHERE (§5.3.1(3)
        // evaluates it on the locked row); it only runs EPQ at all under seed 53.
        let alock = s.txns[w as usize].alock;
        let aqual = s.txns[w as usize].arb.and_then(|a| a.qual);
        let pass = match op {
            Op::KeyShare { .. } => {
                // §5.1/§5.2: KEY SHARE examines every version above S.
                n.iter().all(|(_, vd)| match vd {
                    VerData::Tomb { .. } => false,
                    VerData::Live { kc, .. } => !*kc,
                })
            }
            Op::LockOnly { .. } if alock => match v.1 {
                VerData::Live { val, .. } => aqual.is_none_or(|q| q == val),
                VerData::Tomb { .. } => false,
            },
            Op::Write { qual, .. } => match v.1 {
                VerData::Live { val, .. } => qual.is_none_or(|q| q == val),
                VerData::Tomb { .. } => false,
            },
            Op::Delete { .. } => matches!(v.1, VerData::Live { .. }),
            _ => true,
        };
        if !pass {
            if matches!(op, Op::KeyShare { fk: true, .. }) {
                // §5.3: an FK EPQ failure raises 23503, never skips; a moved
                // parent raises 40001. Both abort; the raised code is recorded
                // for reachability evidence (see the module doc).
                let moved = n
                    .iter()
                    .any(|(_, vd)| matches!(vd, VerData::Tomb { moved: true }));
                s.errs.push((w, moved));
                if self.bug != Some(Bug::FkEpqSkip) {
                    self.do_fail(s, w); // 23503 / 40001
                } else {
                    // Seed 58: skip the row instead of raising.
                    self.advance_op(s, w);
                }
            } else {
                if alock {
                    // Seed 53's EPQ skipped the row: the arbiter statement ends
                    // with no outcome; `arb` stays armed so the statement-end
                    // ghost (§5.3.1) can check the arbiter key's liveness.
                    s.txns[w as usize].alock = false;
                    s.txns[w as usize].synth = None;
                }
                // Ghost (review rework 2): an RC row op skipped by EPQ, against
                // the qual evaluated on the latest committed version.
                if is_data_row_op(op) {
                    let key = op_key(op).unwrap_or(LKey::T0);
                    let qual = match op {
                        Op::Write { qual, .. } => qual,
                        _ => None,
                    };
                    let ghost = ghost_qual(s, key, qual);
                    s.qlog.push((w, key, ghost, false));
                }
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
                            return Spin::Removed;
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

/// Ghost qual evaluation (review rework 2): does the latest committed version of
/// `k` satisfy the op's WHERE qual (`WHERE v = c`; no qual always matches)? A
/// dead or absent row never applies. Read by `check` only.
fn ghost_qual(s: &State, k: LKey, qual: Option<u8>) -> bool {
    matches!(
        s.committed_state(k),
        Some((_, VerData::Live { val, .. })) if qual.is_none_or(|q| q == val)
    )
}

/// Ghost cycle test (I-LIVE b): is `w` on a cycle of `edges`? Reachability of
/// `w` from its own successors, computed over the edge set alone — not by the
/// detector's DFS and not by any record it writes.
fn ghost_on_cycle(edges: &[(u8, u8, EdgeKind)], w: u8) -> bool {
    let mut stack: Vec<u8> = edges
        .iter()
        .filter(|(x, _, _)| *x == w)
        .map(|(_, t, _)| *t)
        .collect();
    let mut seen = [false; NTXNS];
    while let Some(t) = stack.pop() {
        if t == w {
            return true;
        }
        if (t as usize) >= NTXNS || seen[t as usize] {
            continue;
        }
        seen[t as usize] = true;
        for (x, t2, _) in edges {
            if *x == t {
                stack.push(*t2);
            }
        }
    }
    false
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
            d40p01: Vec::new(),
            qlog: Vec::new(),
            alog: Vec::new(),
            errs: Vec::new(),
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
                    Op::OnConflict { arb, .. } => {
                        // §5.3.1(1): the pre-check's latch section.
                        if latch_free(s, lk_of(arb)) {
                            out.push(Action::ArbPre(w));
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
                Phase::ArbAb => {
                    // §5.3.1(2)/§5.5: one latch section per visited key (and
                    // per released shared lock), as ROLLBACK TO.
                    if self.rollback_latches_free(s, w) {
                        out.push(Action::Abandon(w));
                    }
                }
                Phase::EvSnap => out.push(Action::EvSnap(w)),
                Phase::EvNext => out.push(Action::EvNext(w)),
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
                    txn.cmd_seq = txn.seq0;
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
            Action::ArbPre(w) => {
                subject = Some(*w);
                spin_code = self.do_arb_pre(&mut s, *w);
            }
            Action::Abandon(w) => {
                subject = Some(*w);
                spin_code = self.do_abandon(&mut s, *w);
            }
            Action::EvSnap(w) => {
                subject = Some(*w);
                self.do_ev_snap(&mut s, *w);
            }
            Action::EvNext(w) => {
                subject = Some(*w);
                self.do_ev_next(&mut s, *w);
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
                    // §6: exactly one member of the cycle is aborted — the DFS
                    // runner raising 40P01. The other members are woken by its
                    // abort.
                    self.raise_40p01(&mut s, *w);
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
                Spin::Done | Spin::Removed => 0,
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
                // An entry key's liveness: `Entry` makes it live (pointing at its
                // row), `Dead` kills it (C-G0wb: workloads that delete a row
                // delete its entry in the same statement).
                let mut entry: Option<LKey> = None;
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
                                entry = None;
                            }
                            ED::Entry { row } => entry = Some(*row),
                        }
                    }
                }
                if let Some(row) = entry {
                    entries.push((k, row));
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
        // I-LIVE(b): a 40P01 is raised only by a victim that was on a cycle of
        // the current wait-for edges at that moment (§6: exactly one per cycle —
        // once the first victim's edges are gone, a second 40P01 from the same
        // cycle is off-cycle and flagged here).
        for (w, on) in &s.d40p01 {
            if !on {
                return Err(format!(
                    "I-LIVE(b): W{w} raised 40P01 while not on a cycle of current wait-for edges (§6)"
                ));
            }
        }
        // Qual ghost: an RC UPDATE/DELETE's outcome must match its WHERE qual
        // evaluated against the latest committed version at the deciding moment.
        for (w, k, ghost, applied) in &s.qlog {
            if ghost != applied {
                return Err(format!(
                    "I-RC-MONO/qual: W{w}'s row op on {k:?} was {} but the qual evaluated on the latest committed version says {}",
                    if *applied { "applied" } else { "skipped" },
                    if *ghost { "apply" } else { "skip" }
                ));
            }
        }
        // §5.3.1 ghost: an ON CONFLICT statement that ends with no row outcome
        // must still face a live conflict — a conflict that vanished (the row
        // was deleted, moved or re-owned) must restart the arbiter and insert,
        // never skip the proposed row (seed 53).
        for (w, k, live) in &s.alog {
            if !live {
                return Err(format!(
                    "C-T0 §5.3.1: W{w}'s ON CONFLICT statement ended with no row outcome while the arbiter key {k:?} is not live (the lock must restart the arbiter, never skip)"
                ));
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
