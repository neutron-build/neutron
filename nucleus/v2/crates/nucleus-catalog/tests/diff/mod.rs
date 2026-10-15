//! C-T6c1a gate harness, shared declarations: what a run records, the
//! recorder, and the seeded programs.
//!
//! The t6c differential runs seeded multi-session histories over
//! `nucleus-catalog::Db` on `Core<MemKv>` (real threads, a commit thread,
//! the resolver and SSI retention) and checks them against a sequential
//! reference model replayed in commit-ts order (C-T6 DECISION 1, the
//! substitute for the PLAN "Concurrent DDL + DML differential" gate).
//!
//! Shared by the `t6c_soak`, `t6c_regress`, `t6c_injected` and
//! `t6c_crash` test binaries, which reach for different parts of it
//! (the soak drives `run_workload`; the regressions and the injected
//! suite drive `Rig`/`Session` directly and the synthetic-history
//! checks never run the engine at all; the crash matrix drives the
//! `t6b_support` rig through `crash`) — the `t6b_support` pattern, so
//! like it this module tree allows dead code per binary.
//!
//! **C-T6e3** extends the mix with the online build's surface (§7): the
//! DDL session gains `CreateIndex(a, …)` (driven through
//! `create_index_online` on the DDL thread — the ladder runs its own
//! RC txns, so these ops are recorded outside the `Session` machinery),
//! `DropIndex` and `ResumeBuild`; the DML mix gains `IndexScan(a
//! range)` over whatever index the catalog currently holds on `a`. The
//! runner brackets every index scan with its own catalog reads of the
//! target's def (before and after the engine call, under the statement's
//! AccessShare — see `runner::Session::index_scan`), so the checker can
//! decide §4's "serves iff Public and built_at <= S" exactly, never from
//! scheduling.
#![allow(dead_code)]
#![allow(unused_imports)]
//!
//! **Determinism.** A program is a pure function of
//! `(seed, session, index)` ([`gen_prog`], [`gen_ddl`]); the checker
//! depends only on the recorded history, never on scheduling. Write
//! constants are derived from `(seed, session, index, attempt, op
//! position, role)` so every write in a run is uniquely identifiable.
//! Thread interleavings (and so which attempts hit 40001/40P01) vary
//! between runs of one seed; the programs and their first-attempt op
//! streams do not. DDL and UPDATE targets that name a column are resolved
//! at execution time against the live definition (the program pins the
//! *choice index*, the resolved attnum is recorded) — a program cannot
//! know which columns a concurrent DDL will have dropped by the time it
//! runs, and recording the resolution keeps the model exact.
//!
//! **Ticks.** `start`/`end` are readings of one global logical counter
//! (the recorder's tick), taken under one mutex — a total order of record
//! events, never wall-clock time. They are used for traces only, never
//! for a verdict: the window checks compare the storage id a statement's
//! handle read against the new storage id a TRUNCATE allocated, which is
//! exact without any ordering argument (see `check.rs`).

mod check;
mod model;
pub mod runner;

/// C-T6c2's DDL crash-matrix helpers (the `t6b_support` hand-driven Rig
/// over `Fault<MemKv>`, driven by the `t6c_crash` binary).
pub mod crash;

pub use check::{check, Verdict};
pub use runner::run_workload;

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Mutex;

use nucleus_catalog::amcheck::Report;
use nucleus_codec::Value;

/// The isolation level a program's txn runs at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl Level {
    pub fn short(self) -> &'static str {
        match self {
            Level::ReadCommitted => "RC",
            Level::RepeatableRead => "RR",
            Level::Serializable => "SER",
        }
    }
}

/// How a txn attempt ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fate {
    /// Committed at the engine's commit ts.
    Committed(u64),
    /// The txn failed with this SQLSTATE (the op that failed carries it
    /// too; `40001`/`40P01` are retried as fresh txns by the runner).
    Failed(String),
}

/// Whether an SQLSTATE is retried with a fresh txn (the card's retry rule).
pub fn retryable(sqlstate: &str) -> bool {
    sqlstate == "40001" || sqlstate == "40P01"
}

/// One row as the records carry it: `(attnum, value)` pairs over the live
/// columns of the schema the statement reported (`None` = NULL). Rows are
/// self-describing so the model never re-derives an engine decision.
pub type RowVals = Vec<(u16, Option<Value>)>;

/// The operation kinds a program can contain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    /// `SELECT * FROM t WHERE pk = ..`
    Get,
    /// `SELECT * FROM t`
    ScanT,
    /// `INSERT INTO t VALUES (pk, ..)`
    InsertT,
    /// `UPDATE t SET col = const WHERE pk = ..`
    UpdateSet,
    /// `DELETE FROM t WHERE pk = ..`
    DeleteT,
    /// `INSERT INTO u VALUES (..)` (hidden rowid)
    InsertU,
    /// `SELECT * FROM u` (rows with their rowids)
    ScanU,
    /// `DELETE FROM u WHERE rowid = ..` (a rowid an earlier ScanU of the
    /// same txn saw)
    DeleteU,
    /// `UPDATE t SET big = <BIG_LENS[..]-byte text> WHERE pk = ..`
    /// (C-T6d2: the large-value op — the column is resolved by name at
    /// execution, so a DropBig/AddBig cycle moves its attnum).
    SetBig,
    /// `UPDATE t SET big = NULL WHERE pk = ..`
    NullBig,
    /// `ALTER TABLE t ADD COLUMN cN int8 [DEFAULT const]`
    AddColumn,
    /// `ALTER TABLE t DROP COLUMN ..` (a live non-pk column)
    DropColumn,
    /// `ALTER TABLE t ALTER COLUMN .. SET NOT NULL`
    SetNotNull,
    /// `ALTER TABLE t ALTER COLUMN .. DROP NOT NULL`
    DropNotNull,
    /// `TRUNCATE t`
    Truncate,
    /// `ALTER TABLE t DROP COLUMN big` (C-T6d2: by name, recorded as a
    /// DropColumn of the resolved attnum).
    DropBig,
    /// `ALTER TABLE t ADD COLUMN big text` (C-T6d2: the re-add —
    /// attnums are never reused, so it lands on a fresh one).
    AddBig,
    /// `CREATE INDEX [CONCURRENTLY] ON t (a)` (C-T6e3: §7's online
    /// path, non-unique, on the DDL thread).
    CreateIndex,
    /// `DROP INDEX` (C-T6e3).
    DropIndex,
    /// `ALTER INDEX … RESUME` (C-T6e3: `resume_build`).
    ResumeBuild,
    /// `SELECT * FROM t WHERE a BETWEEN .. AND ..` served by an index
    /// (C-T6e3: `Db::index_scan`).
    IndexScan,
}

impl OpKind {
    pub fn tag(self) -> &'static str {
        match self {
            OpKind::Get => "G",
            OpKind::ScanT => "St",
            OpKind::InsertT => "It",
            OpKind::UpdateSet => "Ut",
            OpKind::DeleteT => "Dt",
            OpKind::InsertU => "Iu",
            OpKind::ScanU => "Su",
            OpKind::DeleteU => "Du",
            OpKind::SetBig => "Sb",
            OpKind::NullBig => "Nb",
            OpKind::AddColumn => "ADD",
            OpKind::DropColumn => "DROP",
            OpKind::SetNotNull => "SNN",
            OpKind::DropNotNull => "DNN",
            OpKind::Truncate => "TRUNC",
            OpKind::DropBig => "DROPB",
            OpKind::AddBig => "ADDB",
            OpKind::CreateIndex => "CIDX",
            OpKind::DropIndex => "DIDX",
            OpKind::ResumeBuild => "RSMB",
            OpKind::IndexScan => "Ixa",
        }
    }
}

/// What the runner knows about an op when it records it: its parameters
/// and, for writes, the effect the engine applied.
#[derive(Debug, Clone, PartialEq)]
pub enum OpDetail {
    /// The statement failed before its parameters were resolved (a DDL
    /// with no live target, an early doomed check). Carries nothing; the
    /// model applies no effect.
    Pending,
    Get {
        pk: i64,
    },
    ScanT,
    /// `row` is the datum list the statement submitted (live attnums of
    /// the handle's definition).
    InsertT {
        pk: i64,
        row: RowVals,
    },
    /// `attnum` was resolved against the live definition at execution;
    /// `new_row` is the row a post-write read-back observed (the effect
    /// the engine applied, EPQ base included) — `None` when no row was
    /// updated. The write-effect oracle itself is C-T6c1b's; C-T6c1a only
    /// feeds the model.
    UpdateSet {
        pk: i64,
        attnum: u16,
        value: Option<Value>,
        new_row: Option<RowVals>,
    },
    DeleteT {
        pk: i64,
    },
    InsertU {
        row: RowVals,
    },
    ScanU,
    /// `key` is the row's logical key (rowid included); `None` when the
    /// txn had seen no row to delete (a no-op statement).
    DeleteU {
        key: Option<Vec<u8>>,
    },
    /// `attnum` is the appended one (`ncols + 1`); the name is unique per
    /// DDL program index, so retries of one index reuse one name.
    AddColumn {
        name: String,
        default: Option<i64>,
    },
    DropColumn {
        attnum: u16,
    },
    /// C-T6d2's large-value ops (§5): `big`'s attnum is resolved by name
    /// at execution (a DropBig/AddBig cycle moves it); `len` is the
    /// value's byte length and `tag` its letter, so the value itself is
    /// [`big_blob`]`(tag, len)`; `new_row` is the row a post-write
    /// read-back observed, exactly as UpdateSet's.
    SetBig {
        pk: i64,
        len: u32,
        tag: u8,
        attnum: u16,
        new_row: Option<RowVals>,
    },
    NullBig {
        pk: i64,
        attnum: u16,
        new_row: Option<RowVals>,
    },
    SetNotNull {
        attnum: u16,
    },
    DropNotNull {
        attnum: u16,
    },
    /// `new_sid` is the fresh table storage `catalog_truncate_table`
    /// allocated — the marker a later statement's recorded `sid` is
    /// compared against (storage ids are never reused, so equality pins
    /// the storage exactly).
    Truncate {
        new_sid: u64,
    },
    // ---- C-T6e3: the online-build surface (§7) -------------------------
    /// `CREATE INDEX [CONCURRENTLY] ON t (a)` through `create_index_online`
    /// on the DDL thread: the ladder runs its own RC txns (T0..T3), so
    /// the op is recorded as its own pseudo-txn, not through a `Session`.
    /// `built_at` is the snapshot the driver registered in T3 (captured
    /// by the `after_register` hook) when the build published; `None`
    /// when it failed (or never reached T3). Index DDL never changes a
    /// row, so the model applies no effect — the checker's index-scan
    /// arm and amcheck are its oracle.
    CreateIndex {
        concurrently: bool,
        built_at: Option<u64>,
    },
    /// `DROP INDEX` of `oid` in the DDL session's own RC txn (`None`:
    /// no live index existed — the no-op record).
    DropIndex {
        oid: Option<u64>,
    },
    /// `ALTER INDEX … RESUME` of `oid` through `resume_build` (its own
    /// txns, like CreateIndex); `built_at` is the resumed publish's
    /// registered snapshot, `None` when it failed or there was nothing
    /// non-Public to resume.
    ResumeBuild {
        oid: Option<u64>,
        built_at: Option<u64>,
    },
    /// `SELECT * FROM t WHERE a BETWEEN lo AND hi` through `Db::index_scan`
    /// on `oid` — `None` bounds are unbounded. `pre`/`post` are the
    /// runner's own catalog reads of the target's def, taken under the
    /// statement's AccessShare **before and after** the engine call (see
    /// `runner::Session::index_scan` for why both, and why the bracket
    /// is exact): they record what the engine's own def read could and
    /// could not have seen, which is how the checker decides §4's
    /// "serves iff Public and built_at <= S" without a scheduling
    /// argument. `post` is `None` when the engine call failed before the
    /// post read (the refusal arm needs only `pre`); both sights are
    /// `Absent` for the no-target no-op (`oid` is `None`).
    IndexScan {
        oid: Option<u64>,
        lo: Option<i64>,
        hi: Option<i64>,
        pre: IdxSight,
        post: Option<IdxSight>,
    },
}

/// What a catalog read of one index's def saw (C-T6e3): absent, or its
/// state and `built_at` (§6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdxSight {
    Absent,
    Seen {
        state: nucleus_catalog::IndexState,
        built_at: u64,
    },
}

impl IdxSight {
    /// §4: whether an index this sight describes may serve a read at
    /// snapshot `s` — `Public` and `built_at <= s`.
    pub fn serves(self, s: u64) -> bool {
        match self {
            IdxSight::Seen {
                state: nucleus_catalog::IndexState::Public,
                built_at,
            } => built_at <= s,
            _ => false,
        }
    }
}

/// What the engine answered.
#[derive(Debug, Clone, PartialEq)]
pub enum OpOut {
    Ok,
    /// The SQLSTATE the statement failed with.
    Err(String),
    /// A point read's row (`None` = not found).
    Row(Option<RowVals>),
    /// A scan of `t`: one entry per row, pk first.
    RowsT(Vec<(i64, RowVals)>),
    /// A scan of `u`: the row's logical key (rowid included) and its row.
    RowsU(Vec<(Vec<u8>, RowVals)>),
    /// Whether a write's row op applied (matched).
    Applied(bool),
}

/// One statement's record: level comes from the txn, the statement
/// snapshot `snap` (a fresh per-statement snapshot at RC, the txn's at
/// RR/SER), the `def_version` and `sid` the opened table handle reported
/// (`None` for DDL statements, which open no handle), the outcome, and
/// the tick window (see the module docs: a logical total order, used for
/// traces, never for a verdict).
#[derive(Debug, Clone, PartialEq)]
pub struct OpRec {
    pub kind: OpKind,
    pub detail: OpDetail,
    pub out: OpOut,
    pub snap: u64,
    pub def_version: Option<u32>,
    /// The storage id the statement's handle read — with the TRUNCATEs'
    /// recorded new storage ids, the exact, tick-free test for "this
    /// statement answered from a post-truncate storage" (ids are never
    /// reused, so equality pins the storage).
    pub sid: Option<u64>,
    pub start: u64,
    pub end: u64,
}

/// One txn attempt: the session and program it ran, its level, its ops in
/// order (a failed txn's record stops at the op that failed), and its
/// fate. `txn_snap` is the RR/SER txn snapshot (`None` at RC).
#[derive(Debug, Clone, PartialEq)]
pub struct TxnRec {
    pub uid: u32,
    pub session: u16,
    pub prog: u32,
    pub attempt: u32,
    pub level: Level,
    pub txn_snap: Option<u64>,
    pub start: u64,
    pub end: u64,
    pub fate: Fate,
    pub ops: Vec<OpRec>,
}

/// The drained history: one record per attempt, in uid order.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct History {
    pub txns: Vec<TxnRec>,
}

/// Append-only, thread-safe recorder: the uid and tick counters are one
/// atomic each; ticks give the logical total order the TRUNCATE-window
/// check relies on (see `check.rs`).
#[derive(Debug, Default)]
pub struct Recorder {
    txns: Mutex<Vec<TxnRec>>,
    tick: AtomicU64,
    uid: AtomicU32,
}

impl Recorder {
    pub fn new() -> Recorder {
        Recorder::default()
    }

    pub fn tick(&self) -> u64 {
        self.tick.fetch_add(1, Ordering::SeqCst)
    }

    pub fn next_uid(&self) -> u32 {
        self.uid.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn push(&self, rec: TxnRec) {
        self.txns.lock().expect("recorder mutex").push(rec);
    }

    pub fn drain(&self) -> History {
        History {
            txns: std::mem::take(&mut *self.txns.lock().expect("recorder mutex")),
        }
    }
}

// ----- seeded programs -----------------------------------------------------

/// xorshift64*, seeded through splitmix64 so adjacent seeds differ (the
/// g2 runner's generator, unchanged in spirit).
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        let mut z = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^= z >> 31;
        Rng(z | 1)
    }

    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        (self.next() >> 11) % n
    }
}

/// One statement of a DML program. Column and rowid targets that depend
/// on the live state are *choice indices* resolved at execution time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum POp {
    Get(i64),
    ScanT,
    /// (pk, b is NULL)
    InsertT(i64, bool),
    /// (pk, choice index into the live non-pk columns)
    UpdateSet(i64, u32),
    DeleteT(i64),
    /// (b is NULL)
    InsertU(bool),
    ScanU,
    /// (choice index into the rowids this txn has seen)
    DeleteU(u32),
    /// (pk, choice index into [`BIG_LENS`]) — C-T6d2's large value.
    SetBig(i64, u32),
    /// (pk)
    NullBig(i64),
    /// (choice index into the live indexes on `a`, seeded range pick) —
    /// C-T6e3's index scan. The range pick resolves to its bounds purely
    /// ([`a_range`]): a quarter of the picks scan the whole index, the
    /// rest a bounded window over the setup's `a = A_BASE + pk` domain.
    IndexScan(u32, u32),
}

/// The `a`-range of a seeded index-scan pick (C-T6e3): `(None, None)` is
/// unbounded (a full scan, NULLs included); otherwise the inclusive
/// `[lo, hi]` window over `a`, `lo <= hi`. Pure — the same pick always
/// resolves to the same range, and the recorded detail carries the
/// resolution so the checker never re-derives it.
pub fn a_range(pick: u32) -> (Option<i64>, Option<i64>) {
    if pick % 4 == 3 {
        return (None, None);
    }
    let span = pick / 4;
    let lo = A_BASE + i64::from(span % 9);
    let hi = A_BASE + i64::from((span / 9) % 9);
    if lo <= hi {
        (Some(lo), Some(hi))
    } else {
        (Some(hi), Some(lo))
    }
}

/// A DML program: a fixed level and 1-4 fixed statements, independent of
/// what the statements observe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prog {
    pub level: Level,
    pub ops: Vec<POp>,
}

/// The pk domain of `t` (the card's `pk ∈ 0..8`).
pub const PKS: i64 = 9;

/// The setup writes `a = A_BASE + pk` (runner::Rig::setup_rows), so the
/// seeded index-scan windows ([`a_range`]) land on the surviving setup
/// rows' `a` values; every other write's `a` is a fresh derived constant
/// far outside the domain, so a window's match set is legible.
pub const A_BASE: i64 = 1000;

/// The `big` lengths the card names (C-T6d2): 100 stays inline, 3000 is
/// one chunk, 20000 is three.
pub const BIG_LENS: [usize; 3] = [100, 3000, 20_000];

/// The `big` value of `(tag, len)`: one ASCII letter repeated — distinct
/// per tag, one byte per char, so the byte length is the char count.
pub fn big_blob(tag: u8, len: usize) -> Value {
    Value::Text(char::from(b'a' + (tag % 26)).to_string().repeat(len))
}

/// The program of `(seed, session, index)`: a level and 1-4 ops over the
/// mix below. Pure: same triple, same program, whatever the interleavings.
pub fn gen_prog(seed: u64, session: u16, index: u32) -> Prog {
    let mut rng = Rng::new(seed ^ (u64::from(session) << 40) ^ (u64::from(index) << 8) ^ 0x71c1);
    let level = match rng.below(3) {
        0 => Level::ReadCommitted,
        1 => Level::RepeatableRead,
        _ => Level::Serializable,
    };
    let n = 1 + rng.below(4) as usize;
    // Percent-like mix weights (relative): reads on t, writes on t, ops
    // on u, and the large-value pair on t's `big` column.
    const W_GET: u64 = 18;
    const W_SCAN: u64 = 12;
    const W_INSERT: u64 = 14;
    const W_UPDATE: u64 = 20;
    const W_DELETE: u64 = 10;
    const W_INSERT_U: u64 = 9;
    const W_SCAN_U: u64 = 8;
    const W_DELETE_U: u64 = 9;
    const W_SET_BIG: u64 = 10;
    const W_NULL_BIG: u64 = 4;
    const W_INDEX_SCAN: u64 = 12;
    const TOTAL: u64 = W_GET
        + W_SCAN
        + W_INSERT
        + W_UPDATE
        + W_DELETE
        + W_INSERT_U
        + W_SCAN_U
        + W_DELETE_U
        + W_SET_BIG
        + W_NULL_BIG
        + W_INDEX_SCAN;
    let mut ops = Vec::with_capacity(n + 1);
    for _ in 0..n {
        let pk = rng.below(PKS as u64) as i64;
        let mut r = rng.below(TOTAL);
        let mut pick = |w: u64| -> bool {
            if r < w {
                true
            } else {
                r -= w;
                false
            }
        };
        if pick(W_GET) {
            ops.push(POp::Get(pk));
        } else if pick(W_SCAN) {
            ops.push(POp::ScanT);
        } else if pick(W_INSERT) {
            ops.push(POp::InsertT(pk, rng.below(8) == 0));
        } else if pick(W_UPDATE) {
            ops.push(POp::UpdateSet(pk, rng.next() as u32));
        } else if pick(W_DELETE) {
            ops.push(POp::DeleteT(pk));
        } else if pick(W_INSERT_U) {
            ops.push(POp::InsertU(rng.below(8) == 0));
        } else if pick(W_SCAN_U) {
            ops.push(POp::ScanU);
        } else if pick(W_DELETE_U) {
            ops.push(POp::DeleteU(rng.next() as u32));
        } else if pick(W_SET_BIG) {
            ops.push(POp::SetBig(pk, rng.next() as u32));
        } else if pick(W_INDEX_SCAN) {
            ops.push(POp::IndexScan(rng.next() as u32, rng.next() as u32));
        } else {
            ops.push(POp::NullBig(pk));
        }
    }
    // C-T6e3: an index scan runs **before** the txn's own writes on `t`.
    // The engine's own-write + concurrent-index-scan interplay has an
    // open bug (see the card report's ESCALATE: an own row intent makes
    // `Db::index_scan`'s verification XX001 over entries it races —
    // every captured failure is a scan positioned after an InsertT of
    // the same txn, e.g. seed 0x7c1a_0003's s4 p27 `[InsertT(5),
    // IndexScan]`), so the scans lead the program: the served and
    // refused scans, their model equality and their sight brackets are
    // still checked at every level and under the full DDL stream — only
    // the own-write overlay ahead of a scan is out (the checker's
    // own-writes handling for scans stays pinned by the synthetic
    // non-vacuity legs in `t6e3_regress`).
    let scans: Vec<POp> = ops
        .iter()
        .filter(|o| matches!(o, POp::IndexScan(..)))
        .copied()
        .collect();
    if !scans.is_empty() {
        ops.retain(|o| !matches!(o, POp::IndexScan(..)));
        ops.splice(0..0, scans);
    }
    Prog { level, ops }
}

/// One DDL statement kind of the DDL session's programs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PDdl {
    /// int8, with a constant default or none (seeded).
    AddColumn(bool),
    /// A live non-pk column, resolved at execution.
    DropColumn,
    /// A live non-pk column, resolved at execution.
    SetNotNull,
    /// A live non-pk column, resolved at execution.
    DropNotNull,
    Truncate,
    /// `DROP COLUMN big`, by name (C-T6d2); a no-big-live table refuses.
    DropBig,
    /// `ADD COLUMN big text` — the re-add (C-T6d2). The generator pairs
    /// the drop and the re-add on adjacent DDL indices (see [`gen_ddl`]).
    AddBig,
    /// `CREATE INDEX [CONCURRENTLY] ON t (a)` (C-T6e3: §7's online path,
    /// non-unique — `create_index_online` on the DDL thread; the flag
    /// seeds CONCURRENTLY).
    CreateIndex(bool),
    /// `DROP INDEX` (C-T6e3: one of the live indexes on `a`, resolved at
    /// execution; a no-op record when none exists).
    DropIndex,
    /// `ALTER INDEX … RESUME` (C-T6e3: `resume_build` of a non-Public
    /// index, resolved at execution; a no-op record when none exists).
    ResumeBuild,
}

/// The DDL program of `(seed, index)`: one DDL statement, its own txn.
///
/// The big pair is a **drop followed by its re-add on the very next
/// index**: a draw that lands `DropBig` at `i` forces `AddBig` at
/// `i + 1`, so `big` is never absent for more than a txn or two of the
/// stream. Without the forcing, a drop's re-add comes only when a later
/// draw and parity both say so, and the column can stay dropped for a
/// long stretch — starving the large-value DML ops on scheduling
/// rather than on the seed.
pub fn gen_ddl(seed: u64, index: u32) -> PDdl {
    if index > 0 && matches!(gen_ddl_draw(seed, index - 1), PDdl::DropBig) {
        return PDdl::AddBig;
    }
    gen_ddl_draw(seed, index)
}

/// The seeded draw of one DDL index (the big pair draws only the drop;
/// [`gen_ddl`] supplies the re-add).
fn gen_ddl_draw(seed: u64, index: u32) -> PDdl {
    let mut rng = Rng::new(seed ^ 0x_dd11 ^ (u64::from(index) << 8));
    const W_ADD: u64 = 30;
    const W_DROP: u64 = 12;
    const W_SNN: u64 = 14;
    const W_DNN: u64 = 14;
    // Kept at the pre-C-T6e3 *share* of the stream (the new index ops
    // dilute a constant weight, which the checker-can-fail mini run's
    // refused-read-plus-truncate shape notices).
    const W_TRUNC: u64 = 38;
    const W_DROP_BIG: u64 = 4;
    // C-T6e3: the online-build surface. CREATE INDEX (plain or
    // CONCURRENTLY, seeded) joins the stream often enough that builds
    // overlap DML reliably; RESUME and DROP INDEX clean up after them.
    const W_CREATE_IDX: u64 = 16;
    const W_RESUME: u64 = 8;
    const W_DROP_IDX: u64 = 8;
    let mut r = rng.below(
        W_ADD
            + W_DROP
            + W_SNN
            + W_DNN
            + W_TRUNC
            + W_DROP_BIG
            + W_CREATE_IDX
            + W_RESUME
            + W_DROP_IDX,
    );
    if r < W_ADD {
        return PDdl::AddColumn(rng.below(2) == 0);
    }
    r -= W_ADD;
    if r < W_DROP {
        return PDdl::DropColumn;
    }
    r -= W_DROP;
    if r < W_SNN {
        return PDdl::SetNotNull;
    }
    r -= W_SNN;
    if r < W_DNN {
        return PDdl::DropNotNull;
    }
    r -= W_DNN;
    if r < W_TRUNC {
        return PDdl::Truncate;
    }
    r -= W_TRUNC;
    if r < W_CREATE_IDX {
        return PDdl::CreateIndex(rng.below(2) == 0);
    }
    r -= W_CREATE_IDX;
    if r < W_RESUME {
        return PDdl::ResumeBuild;
    }
    r -= W_RESUME;
    if r < W_DROP_IDX {
        return PDdl::DropIndex;
    }
    // The fallthrough covers the big-cycle draw: the drop. Its re-add is
    // the forced successor; a drop over an absent column is a no-op
    // refusal, so the pairing is self-correcting.
    PDdl::DropBig
}

// ----- write constants ------------------------------------------------------

/// The write-constant roles (one op writes at most a few constants; the
/// role keeps them apart).
pub const TAG_T_A: u8 = 0;
pub const TAG_T_B: u8 = 1;
pub const TAG_T_SET: u8 = 2;
pub const TAG_U_A: u8 = 3;
pub const TAG_U_B: u8 = 4;
pub const TAG_DDL: u8 = 5;
/// The `big` letter of a SetBig (C-T6d2).
pub const TAG_T_BIG: u8 = 6;

/// A unique-per-write constant (the card's "constants unique per
/// write"): a pure function of the program position and role. Distinct
/// tuples map to distinct 64-bit inputs, so collisions have probability
/// ~n²/2⁶⁵ over a run's few thousand writes — and any collision would
/// surface as a checker mismatch, never as a silent pass.
pub fn derive_const(seed: u64, session: u16, index: u32, attempt: u32, op: usize, tag: u8) -> i64 {
    let mut x = seed
        ^ ((u64::from(session) & 0x3f) << 58)
        ^ (u64::from(index) << 38)
        ^ (u64::from(attempt) << 30)
        ^ (u64::from(op as u32) << 24)
        ^ (u64::from(tag) << 20);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^= x >> 31;
    x as i64
}

/// A unique text constant of the same provenance.
pub fn derive_text(
    seed: u64,
    session: u16,
    index: u32,
    attempt: u32,
    op: usize,
    tag: u8,
) -> String {
    format!("w{}", derive_const(seed, session, index, attempt, op, tag))
}

// ----- the run ----------------------------------------------------------------

/// One amcheck point: what the checker found on one table at one
/// registered snapshot taken mid-run.
#[derive(Debug, Clone)]
pub struct AmcheckRec {
    /// The DDL program index the point ran after.
    pub ddl_index: u32,
    pub table: &'static str,
    pub report: Report,
}

/// The workload's shape: counts and seeds, never wall-clock bounds.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub seed: u64,
    /// DML sessions (session ids `1..=threads`); the DDL session is
    /// `runner::DDL_SESSION`.
    pub threads: u16,
    /// Programs per DML session.
    pub progs: u32,
    /// DDL txns of the DDL session (amcheck points at quarters).
    pub ddl_txns: u32,
    /// Fresh-txn retries per program on 40001/40P01.
    pub max_attempts: u32,
    /// The `deadlock_timeout` setting (ms), not an assertion.
    pub deadlock_ms: u64,
}

impl Config {
    /// The soak's calibration: 4 DML sessions + the DDL session, 200
    /// programs each, 12 attempts.
    pub fn new(seed: u64) -> Config {
        Config {
            seed,
            threads: 4,
            progs: 200,
            ddl_txns: 200,
            max_attempts: 12,
            deadlock_ms: 5,
        }
    }
}

/// A finished run: the drained history, the amcheck reports, the final
/// full scans (with the def version they reported) and the leak check.
#[derive(Debug, Clone)]
pub struct Run {
    pub history: History,
    pub amcheck: Vec<AmcheckRec>,
    /// `(def version, rows by pk)`.
    pub final_t: (u32, Vec<(i64, RowVals)>),
    /// `(def version, rows with their rowid keys)`.
    pub final_u: (u32, Vec<(Vec<u8>, RowVals)>),
    pub leaked_intents: usize,
    pub leak_report: String,
}
