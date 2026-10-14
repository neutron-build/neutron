//! C-T6c1a gate harness, shared declarations: what a run records, the
//! recorder, and the seeded programs.
//!
//! The t6c differential runs seeded multi-session histories over
//! `nucleus-catalog::Db` on `Core<MemKv>` (real threads, a commit thread,
//! the resolver and SSI retention) and checks them against a sequential
//! reference model replayed in commit-ts order (C-T6 DECISION 1, the
//! substitute for the PLAN "Concurrent DDL + DML differential" gate).
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
mod runner;

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
            OpKind::AddColumn => "ADD",
            OpKind::DropColumn => "DROP",
            OpKind::SetNotNull => "SNN",
            OpKind::DropNotNull => "DNN",
            OpKind::Truncate => "TRUNC",
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
    // Percent-like mix weights (relative): reads on t, writes on t, ops on u.
    const W_GET: u64 = 18;
    const W_SCAN: u64 = 12;
    const W_INSERT: u64 = 14;
    const W_UPDATE: u64 = 20;
    const W_DELETE: u64 = 10;
    const W_INSERT_U: u64 = 9;
    const W_SCAN_U: u64 = 8;
    const W_DELETE_U: u64 = 9;
    const TOTAL: u64 =
        W_GET + W_SCAN + W_INSERT + W_UPDATE + W_DELETE + W_INSERT_U + W_SCAN_U + W_DELETE_U;
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
        } else {
            ops.push(POp::DeleteU(rng.next() as u32));
        }
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
}

/// The DDL program of `(seed, index)`: one DDL statement, its own txn.
pub fn gen_ddl(seed: u64, index: u32) -> PDdl {
    let mut rng = Rng::new(seed ^ 0x_dd11 ^ (u64::from(index) << 8));
    const W_ADD: u64 = 30;
    const W_DROP: u64 = 12;
    const W_SNN: u64 = 14;
    const W_DNN: u64 = 14;
    const W_TRUNC: u64 = 30;
    let mut r = rng.below(W_ADD + W_DROP + W_SNN + W_DNN + W_TRUNC);
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
    PDdl::Truncate
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
