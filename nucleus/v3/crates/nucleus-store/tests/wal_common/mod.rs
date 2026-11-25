#![allow(dead_code)]
// Test code (as `clippy.toml` says: "Tests may unwrap/expect/panic; the
// engine may not"): the workspace's allowance covers `#[test]` functions
// only, and these helpers are called from them alone.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
// The two test binaries (wal_walk, wal_crash) each include this module and
// use a subset of it, so the rest would warn as dead code in each.

//! Shared helpers of card A-6b's tests (Work 10), written from the spec and
//! from card A-6a's pinned bytes, independent of the code under test:
//! the record assembler, the placement oracle, fixture F, the hook
//! environment, and the SimEnv rounds' fault modes.
//!
//! The oracles call nothing in `nucleus_store::wal` to compute an expected
//! value. `assemble` builds a record byte by byte with A-6a's offsets and
//! `crc32c` (A-2-0's, pinned by its own test vector); `oracle_place` is
//! S-STORE §12.2's sentence; `oracle_stream` places a plan with it. The
//! crate's own names (`segment_path`, `segment_first`) are not used to build
//! a fixture or an expectation: the helpers below spell the name out.

use std::io::ErrorKind;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use nucleus_env::sim::{DirLoss, FaultKnobs, LifeOutcome, Sim};
use nucleus_env::{Env, EnvError, File, OpenMode};
use nucleus_store::StoreError;
use nucleus_store::crc::crc32c;
use nucleus_store::wal::append::Appended;
use nucleus_store::wal::walk::{Walk, WalkStart, walk};
use nucleus_store::wal::{Body, Op, PREV_NONE, Record, RecordKind, TxnId};

// ---------------------------------------------------------------------
// The bytes: A-6a's pinned layout (S-STORE §12.2, §12.3)
// ---------------------------------------------------------------------

/// The segment size of every test: above the 212 floor, so the largest
/// record is 256 bytes.
pub const S: u64 = 1024;
/// The record header's length.
pub const HEADER: usize = 53;
/// The kind byte of a `Commit`.
pub const KIND_COMMIT: u8 = 1;
/// The kind byte of a `SegmentEnd`.
pub const KIND_SEGMENT_END: u8 = 4;
/// The Ddl op's tag.
pub const TAG_DDL: u8 = 3;

/// The first LSN of the segment `lsn` lies in, spelled out: `lsn − lsn % S`.
pub fn seg_of(lsn: u64) -> u64 {
    lsn - lsn % S
}

/// The name of the segment whose first LSN is `first`, spelled out
/// (§12.1: 16 lowercase hex digits, `.log`).
pub fn seg_name(first: u64) -> String {
    format!("wal/{first:016x}.log")
}

/// The same name without `wal/`: what a listing of `wal/` holds.
pub fn seg_file(first: u64) -> String {
    format!("{first:016x}.log")
}

/// The payload that makes a `Commit` exactly `len` bytes long (Work 10):
/// one `Ddl` op of `len − 58` bytes (tag 3, a `u32` length, the bytes; A-6a's
/// op encoding), for record `i`, whose bytes are `(31 × i + j) mod 251` at
/// byte `j`. `58 ≤ len ≤ 256`.
pub fn ddl_payload(len: u64, record: u64) -> Vec<u8> {
    let n = len - 58;
    let mut p = Vec::with_capacity(5 + n as usize);
    p.push(TAG_DDL);
    p.extend_from_slice(&(n as u32).to_le_bytes());
    for j in 0..n {
        p.push((31 * record + j) as u8 % 251);
    }
    p
}

/// A record assembled byte by byte with a correct CRC, independent of the
/// crate's encoder: A-6a's 53-byte header (magic, `len`, `crc`, `lsn`,
/// `prev_lsn`, `prev_crc`, `kind`, `txn.boot`, `txn.n`, `commit_ts`, all
/// little-endian) and `payload`, with `crc = crc32c(0, &bytes[12..])` at
/// bytes 8..12. One argument per header field: the card's signature.
#[allow(clippy::too_many_arguments)]
pub fn assemble(
    lsn: u64,
    prev_lsn: u64,
    prev_crc: u32,
    kind: u8,
    boot: u32,
    n: u64,
    commit_ts: u64,
    payload: &[u8],
) -> Vec<u8> {
    let mut b = Vec::with_capacity(HEADER + payload.len());
    b.extend_from_slice(b"NWAL");
    b.extend_from_slice(&((HEADER + payload.len()) as u32).to_le_bytes());
    b.extend_from_slice(&0u32.to_le_bytes());
    b.extend_from_slice(&lsn.to_le_bytes());
    b.extend_from_slice(&prev_lsn.to_le_bytes());
    b.extend_from_slice(&prev_crc.to_le_bytes());
    b.push(kind);
    b.extend_from_slice(&boot.to_le_bytes());
    b.extend_from_slice(&n.to_le_bytes());
    b.extend_from_slice(&commit_ts.to_le_bytes());
    b.extend_from_slice(payload);
    let crc = crc32c(0, &b[12..]);
    b[8..12].copy_from_slice(&crc.to_le_bytes());
    b
}

/// A `Commit` record value for the appender (the code under test's input,
/// not an expectation): `boot`'s transaction `n` at `commit_ts`, holding one
/// `Ddl` op whose bytes are those of `payload`, a [`ddl_payload`] (its tag
/// and `u32` length dropped, since A-6a's encoder writes them itself).
pub fn commit<'a>(boot: u32, n: u64, commit_ts: u64, payload: &'a [u8]) -> Record<'a> {
    Record {
        txn: TxnId { boot, n },
        body: Body::Commit {
            commit_ts,
            ops: vec![Op::Ddl {
                bytes: &payload[5..],
            }],
        },
    }
}

/// The `lsn` an assembled record carries.
pub fn lsn_of(bytes: &[u8]) -> u64 {
    let mut at = [0u8; 8];
    at.copy_from_slice(&bytes[12..20]);
    u64::from_le_bytes(at)
}

/// The `crc` an assembled record carries.
pub fn crc_of(bytes: &[u8]) -> u32 {
    let mut at = [0u8; 4];
    at.copy_from_slice(&bytes[8..12]);
    u32::from_le_bytes(at)
}

/// Makes `wal/` if it is missing.
pub fn wal_dir<E: Env>(env: &E) {
    match env.create_dir("wal") {
        Ok(()) => {}
        Err(EnvError::AlreadyExists { .. }) => {}
        Err(e) => panic!("create wal/: {e}"),
    }
}

/// Writes an assembled record at its own `lsn` into its segment's file,
/// creating the file and setting its length to `S` first if the file is
/// missing (Work 10: crafted streams are built only this way and by
/// [`craft`]).
pub fn put<E: Env>(env: &E, bytes: &[u8]) {
    wal_dir(env);
    let lsn = lsn_of(bytes);
    let path = seg_name(seg_of(lsn));
    let f = match env.open(&path, OpenMode::ReadWrite) {
        Ok(f) => f,
        Err(EnvError::NotFound { .. }) => {
            let f = env
                .open(&path, OpenMode::CreateNew)
                .expect("create a segment file");
            f.set_len(S).expect("preallocate a segment file");
            f
        }
        Err(e) => panic!("open {}: {e}", path),
    };
    f.write_at(lsn % S, bytes).expect("write a record");
}

/// Crafts a segment file of exactly `file_len` bytes holding `parts`, each
/// at its offset, the rest zeros: for the short and the cut-short segments
/// (a file a device shortened, or one whose preallocation was cut short).
pub fn craft<E: Env>(env: &E, first: u64, file_len: u64, parts: &[(u64, &[u8])]) {
    wal_dir(env);
    let path = seg_name(first);
    let f = env
        .open(&path, OpenMode::CreateNew)
        .expect("create a segment file");
    f.set_len(file_len).expect("set a segment file's length");
    for (off, bytes) in parts {
        // No part runs past the file's end: the file is as short as asked.
        let room = usize::try_from(file_len.saturating_sub(*off)).unwrap_or(0);
        let upto = bytes.len().min(room);
        if upto > 0 {
            f.write_at(*off, &bytes[..upto]).expect("write a part");
        }
    }
}

/// Preallocates segment `first` to `S` bytes of zeros if its file is missing
/// (the table's fresh stores).
pub fn zeros<E: Env>(env: &E, first: u64) {
    wal_dir(env);
    let path = seg_name(first);
    if let Err(EnvError::NotFound { .. }) = env.open(&path, OpenMode::ReadWrite) {
        let f = env
            .open(&path, OpenMode::CreateNew)
            .expect("create a segment file");
        f.set_len(S).expect("preallocate a segment file");
    }
}

/// Every file under `wal/` with its bytes and its length, by name: for the
/// "writes nothing" checks.
pub fn snapshot<E: Env>(env: &E) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    let names = env.list_dir("wal").expect("list wal/");
    for name in names {
        if name == "wal" {
            continue;
        }
        let f = env
            .open(&format!("wal/{name}"), OpenMode::Read)
            .expect("open a file under wal/");
        let len = f.len().expect("a file's length");
        let mut bytes = vec![0u8; len as usize];
        let mut at = 0usize;
        while at < bytes.len() {
            let n = f.read_at(at as u64, &mut bytes[at..]).expect("read a file");
            assert!(n > 0, "a short read of {name} before its end");
            at += n;
        }
        out.push((name, bytes));
    }
    out.sort();
    out
}

// ---------------------------------------------------------------------
// The placement oracle (S-STORE §12.2, "Records never span segments")
// ---------------------------------------------------------------------

/// §12.2's sentence, written from the spec and never from A-6a's `place`:
/// with `room = S − pos % S`, a record of `len` bytes stays at `pos` iff
/// `len == room` or `len + 53 ≤ room`; otherwise a `SegmentEnd` goes at
/// `pos` and the record at the next segment's first LSN. The result is
/// (the `SegmentEnd`'s start, the record's LSN).
pub fn oracle_place(pos: u64, len: u64) -> (Option<u64>, u64) {
    let room = S - pos % S;
    if len == room || len + 53 <= room {
        (None, pos)
    } else {
        (Some(pos), pos + room)
    }
}

/// One record of a plan: a `Commit` of `len` bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Plan {
    /// The record's length in bytes, 58 to 256.
    pub len: u64,
    /// The writing boot's id.
    pub boot: u32,
    /// The transaction's `n`.
    pub n: u64,
    /// The commit timestamp.
    pub commit_ts: u64,
}

/// One record of the oracle stream: what a walk that accepts it must see.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleRecord {
    /// Its start.
    pub lsn: u64,
    /// Its length.
    pub len: u64,
    /// Its kind.
    pub kind: RecordKind,
    /// Its boot id.
    pub boot: u32,
    /// Its `txn.n`: 0 in a `SegmentEnd`.
    pub n: u64,
    /// Its `commit_ts`: 0 in a `SegmentEnd`.
    pub commit_ts: u64,
    /// Its payload: the `Ddl` op for a `Commit`, nothing for a `SegmentEnd`.
    pub payload: Vec<u8>,
}

impl OracleRecord {
    /// The LSN just past the record.
    pub fn end(&self) -> u64 {
        self.lsn + self.len
    }
}

/// The oracle stream (Work 12): the plan placed by [`oracle_place`] from
/// `start`, `SegmentEnd`s included, in LSN order. It never calls the code
/// under test, nor A-6a's `place`. A `Commit` of one `Ddl` of `n` bytes is
/// `58 + n` long and a `SegmentEnd` 53 (A-6a's pinned bytes).
pub fn oracle_stream(start: u64, plan: &[Plan]) -> Vec<OracleRecord> {
    let mut out = Vec::new();
    let mut pos = start;
    for (i, p) in plan.iter().enumerate() {
        let (segment_end, lsn) = oracle_place(pos, p.len);
        if let Some(q) = segment_end {
            out.push(OracleRecord {
                lsn: q,
                len: HEADER as u64,
                kind: RecordKind::SegmentEnd,
                boot: p.boot,
                n: 0,
                commit_ts: 0,
                payload: Vec::new(),
            });
        }
        out.push(OracleRecord {
            lsn,
            len: p.len,
            kind: RecordKind::Commit,
            boot: p.boot,
            n: p.n,
            commit_ts: p.commit_ts,
            payload: ddl_payload(p.len, i as u64),
        });
        pos = lsn + p.len;
    }
    out
}

/// The oracle's position after its first `m` records: the start when `m` is
/// 0; otherwise the record's end or, after a `SegmentEnd` or a record that
/// ends exactly at its segment's end, the next segment's first LSN (§12.5's
/// hops).
pub fn after(stream: &[OracleRecord], m: usize, start: u64) -> u64 {
    match m.checked_sub(1).and_then(|i| stream.get(i)) {
        None => start,
        Some(last) => {
            let end = last.end();
            if last.kind == RecordKind::SegmentEnd {
                (last.lsn / S + 1) * S
            } else {
                end
            }
        }
    }
}

/// Fixture F (Work 10): 22 `Commit` records of boot 1, `n = 1..=22`,
/// `commit_ts = 100 + n`, of the card's lengths.
pub fn fixture_f() -> Vec<Plan> {
    const LENGTHS: [u64; 22] = [
        200, 200, 200, 200, 224, 100, 100, 100, 200, 216, 256, 200, 200, 167, 200, 200, 200, 200,
        71, 100, 58, 58,
    ];
    LENGTHS
        .iter()
        .enumerate()
        .map(|(i, len)| Plan {
            len: *len,
            boot: 1,
            n: i as u64 + 1,
            commit_ts: 100 + i as u64 + 1,
        })
        .collect()
}

// ---------------------------------------------------------------------
// What a walk saw
// ---------------------------------------------------------------------

/// An owned copy of one op the walk accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnedOp {
    /// A `Ddl` op's bytes.
    Ddl(Vec<u8>),
}

/// One record a walk accepted, as owned values (Work 12): its `lsn`, `len`,
/// kind, boot, `n`, `commit_ts` and ops.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Seen {
    /// The record's start.
    pub lsn: u64,
    /// Its length in bytes.
    pub len: u32,
    /// Its kind.
    pub kind: RecordKind,
    /// Its boot id.
    pub boot: u32,
    /// Its `txn.n`.
    pub n: u64,
    /// Its `commit_ts`.
    pub commit_ts: u64,
    /// Its ops: the fixture holds one `Ddl` in a `Commit`, none in a
    /// `SegmentEnd`.
    pub ops: Vec<OwnedOp>,
}

impl Seen {
    /// What a walk must see for oracle record `o`.
    pub fn of(o: &OracleRecord) -> Self {
        // The op's bytes: after the tag and the `u32` length.
        let ops = if o.kind == RecordKind::Commit {
            vec![OwnedOp::Ddl(o.payload[5..].to_vec())]
        } else {
            Vec::new()
        };
        Self {
            lsn: o.lsn,
            len: o.len as u32,
            kind: o.kind,
            boot: o.boot,
            n: o.n,
            commit_ts: o.commit_ts,
            ops,
        }
    }
}

/// The stream as `Seen` values.
pub fn seen_stream(stream: &[OracleRecord]) -> Vec<Seen> {
    stream.iter().map(Seen::of).collect()
}

/// Runs the walk over `env`, appending every record its visitor sees to
/// `seen` as owned values, and returns what it ended with. A walk that
/// returns an error leaves in `seen` the records it accepted before.
pub fn walk_into<E: Env>(
    env: &E,
    start: &WalkStart,
    seen: &mut Vec<Seen>,
) -> Result<Walk, StoreError> {
    walk(env, S, start, &mut |acc| {
        // The fixture's records hold one `Ddl` each, or no op.
        let ops = match &acc.record.body {
            Body::Commit { ops, .. } | Body::Chunk { ops, .. } => ops
                .iter()
                .map(|op| match op {
                    Op::Ddl { bytes } => OwnedOp::Ddl(bytes.to_vec()),
                    other => panic!("the fixture holds only Ddl ops, not {other:?}"),
                })
                .collect(),
            _ => Vec::new(),
        };
        seen.push(Seen {
            lsn: acc.frame.lsn,
            len: acc.frame.len,
            kind: RecordKind::from_byte(acc.frame.kind).expect("a checked kind byte"),
            boot: acc.frame.txn.boot,
            n: acc.frame.txn.n,
            commit_ts: acc.frame.commit_ts,
            ops,
        });
        Ok(())
    })
}

/// [`walk_into`] with its own list.
pub fn collect<E: Env>(env: &E, start: &WalkStart) -> Result<(Walk, Vec<Seen>), StoreError> {
    let mut seen = Vec::new();
    let walk = walk_into(env, start, &mut seen)?;
    Ok((walk, seen))
}

/// A fresh walk start.
pub fn start(lsn: u64, prev: (u64, u32), boot_id: u32) -> WalkStart {
    WalkStart {
        lsn,
        prev_lsn: prev.0,
        prev_crc: prev.1,
        boot_id,
    }
}

/// The walk start `(0, PREV_NONE, 0)`: a fresh file's.
pub fn start0() -> WalkStart {
    start(0, PREV_NONE, 0)
}

// ---------------------------------------------------------------------
// The hook environment (Work 10): logs every call, fails armed calls
// ---------------------------------------------------------------------

/// One logged call: `(op, path, a, b)`. For `open`, `a` is the mode
/// ([`READ`], [`RW`] or [`CREATE`]) and `b` 0; for `read_at` and `write_at`,
/// `a` is the offset and `b` the length; for `set_len`, `a` is the length;
/// for `rename`, `path` is `"from -> to"`; every other call has `a = b = 0`
/// except `sleep` (`a`, nanoseconds), `now` (`a`, nanoseconds) and `rand`
/// (`a`, the value).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Call {
    /// The method's name.
    pub op: &'static str,
    /// The path the call names.
    pub path: String,
    /// Its first argument.
    pub a: u64,
    /// Its second argument.
    pub b: u64,
}

/// `OpenMode::Read` in a logged `open` call.
pub const READ: u64 = 0;
/// `OpenMode::ReadWrite` in a logged `open` call.
pub const RW: u64 = 1;
/// `OpenMode::CreateNew` in a logged `open` call.
pub const CREATE: u64 = 2;

/// The mode's number in a logged `open` call.
pub fn mode_code(mode: OpenMode) -> u64 {
    match mode {
        OpenMode::Read => READ,
        OpenMode::ReadWrite => RW,
        OpenMode::CreateNew => CREATE,
    }
}

/// The write-class calls a read-only open refuses (S-STORE §14.1).
pub const WRITE_CLASS: [&str; 8] = [
    "write_at",
    "set_len",
    "sync_data",
    "sync_all",
    "create_dir",
    "rename",
    "remove",
    "sync_dir",
];

/// Whether `op` is write-class.
pub fn is_write_class(op: &str) -> bool {
    WRITE_CLASS.contains(&op)
}

/// A fault armed on calls that match it: the op, the path, and an offset
/// (`a`) at or above `min_a`; it fails each matching call with `error`,
/// before the inner `Env` is called. A `once` arm fires for the first
/// matching call only.
#[derive(Debug, Clone)]
pub struct Arm {
    op: &'static str,
    path: String,
    min_a: u64,
    error: EnvError,
    once: bool,
}

impl Arm {
    /// An arm that fails every matching call.
    pub fn always(op: &'static str, path: &str, min_a: u64, error: EnvError) -> Self {
        Self {
            op,
            path: path.to_string(),
            min_a,
            error,
            once: false,
        }
    }

    /// An arm that fails the first matching call only.
    pub fn once(op: &'static str, path: &str, min_a: u64, error: EnvError) -> Self {
        Self {
            once: true,
            ..Self::always(op, path, min_a, error)
        }
    }

    fn matches(&self, call: &Call) -> bool {
        self.op == call.op && self.path == call.path && call.a >= self.min_a
    }
}

/// An I/O error a hook fails a call with (SQLSTATE 58030 at the session).
pub fn io_error(op: &'static str, path: &str) -> EnvError {
    EnvError::Io {
        op,
        path: path.to_string(),
        kind: ErrorKind::Other,
        detail: "injected by the test's hook".to_string(),
    }
}

/// A no-space error a hook fails a call with (SQLSTATE 53100).
pub fn no_space(op: &'static str, path: &str) -> EnvError {
    EnvError::NoSpace {
        op,
        path: path.to_string(),
    }
}

/// The log and the arms.
#[derive(Debug, Default)]
struct Hooks {
    log: Vec<Call>,
    arms: Vec<Arm>,
    refuse_writes: bool,
}

/// Locks the hook state, recovering from poison: a panicking test thread must
/// not turn a hook into a second panic.
fn hooks_lock(h: &Mutex<Hooks>) -> MutexGuard<'_, Hooks> {
    h.lock().unwrap_or_else(PoisonError::into_inner)
}

/// An `Env` whose `File` wraps the inner file (Work 10). It logs every call
/// and fails a call that matches an armed fault with that error, before the
/// inner `Env` is called. Its one `Mutex` (log and arms) is a leaf: taken
/// inside a call only to push or match, never across the inner call. The
/// refusing variant ([`HookEnv::refusing_writes`]) fails every write-class
/// call with `EnvError::ReadOnly`, as a read-only open's `Env` does.
#[derive(Debug, Clone)]
pub struct HookEnv<E: Env> {
    inner: E,
    hooks: Arc<Mutex<Hooks>>,
}

impl<E: Env> HookEnv<E> {
    /// A pass-through environment over `inner` that logs every call.
    pub fn new(inner: E) -> Self {
        Self {
            inner,
            hooks: Arc::new(Mutex::new(Hooks::default())),
        }
    }

    /// The variant that refuses every write-class call with
    /// `EnvError::ReadOnly` (S-STORE §14.1, "What it refuses").
    pub fn refusing_writes(inner: E) -> Self {
        let env = Self::new(inner);
        hooks_lock(&env.hooks).refuse_writes = true;
        env
    }

    /// Arms a fault.
    pub fn arm(&self, arm: Arm) {
        hooks_lock(&self.hooks).arms.push(arm);
    }

    /// Removes every armed fault.
    pub fn disarm(&self) {
        hooks_lock(&self.hooks).arms.clear();
    }

    /// The log so far.
    pub fn log(&self) -> Vec<Call> {
        hooks_lock(&self.hooks).log.clone()
    }

    /// Forgets the log so far.
    pub fn forget(&self) {
        hooks_lock(&self.hooks).log.clear();
    }

    /// The log's write-class calls, in order (the order of every
    /// write-class call: the tests' `wal_op_order`).
    pub fn write_class(&self) -> Vec<Call> {
        self.log()
            .into_iter()
            .filter(|c| is_write_class(c.op))
            .collect()
    }

    /// The log's calls as `(op, path)` pairs, in order.
    pub fn ops(&self) -> Vec<(&'static str, String)> {
        self.log().into_iter().map(|c| (c.op, c.path)).collect()
    }

    /// The logged `open` calls' paths with their modes.
    pub fn opens(&self) -> Vec<(String, u64)> {
        self.log()
            .into_iter()
            .filter(|c| c.op == "open")
            .map(|c| (c.path, c.a))
            .collect()
    }

    /// Whether every logged call is an `open` with `OpenMode::Read` or a
    /// `read_at`: the walk writes nothing.
    pub fn reads_only(&self) -> bool {
        self.log()
            .iter()
            .all(|c| (c.op == "open" && c.a == READ) || c.op == "read_at")
    }

    /// Every logged `read_at`'s offset, in order.
    pub fn read_offsets(&self) -> Vec<u64> {
        self.log()
            .iter()
            .filter(|c| c.op == "read_at")
            .map(|c| c.a)
            .collect()
    }

    /// Logs `call`, then fails it with the first armed fault it matches.
    fn enter(&self, call: Call) -> Result<(), EnvError> {
        let mut h = hooks_lock(&self.hooks);
        h.log.push(call.clone());
        if h.refuse_writes && is_write_class(call.op) {
            return Err(EnvError::ReadOnly { path: call.path });
        }
        let hit = h.arms.iter().position(|arm| arm.matches(&call));
        if let Some(i) = hit {
            let arm = if h.arms[i].once {
                h.arms.remove(i)
            } else {
                h.arms[i].clone()
            };
            return Err(arm.error);
        }
        Ok(())
    }
}

impl<E: Env> Env for HookEnv<E> {
    type File = HookFile<E::File>;

    fn open(&self, path: &str, mode: OpenMode) -> Result<Self::File, EnvError> {
        self.enter(Call {
            op: "open",
            path: path.to_string(),
            a: mode_code(mode),
            b: 0,
        })?;
        Ok(HookFile {
            inner: self.inner.open(path, mode)?,
            path: path.to_string(),
            hooks: Arc::clone(&self.hooks),
        })
    }

    fn create_dir(&self, path: &str) -> Result<(), EnvError> {
        self.enter(Call {
            op: "create_dir",
            path: path.to_string(),
            a: 0,
            b: 0,
        })?;
        self.inner.create_dir(path)
    }

    fn sync_dir(&self, path: &str) -> Result<(), EnvError> {
        self.enter(Call {
            op: "sync_dir",
            path: path.to_string(),
            a: 0,
            b: 0,
        })?;
        self.inner.sync_dir(path)
    }

    fn rename(&self, from: &str, to: &str) -> Result<(), EnvError> {
        self.enter(Call {
            op: "rename",
            path: format!("{from} -> {to}"),
            a: 0,
            b: 0,
        })?;
        self.inner.rename(from, to)
    }

    fn remove(&self, path: &str) -> Result<(), EnvError> {
        self.enter(Call {
            op: "remove",
            path: path.to_string(),
            a: 0,
            b: 0,
        })?;
        self.inner.remove(path)
    }

    fn exists(&self, path: &str) -> Result<bool, EnvError> {
        self.enter(Call {
            op: "exists",
            path: path.to_string(),
            a: 0,
            b: 0,
        })?;
        self.inner.exists(path)
    }

    fn list_dir(&self, path: &str) -> Result<Vec<String>, EnvError> {
        self.enter(Call {
            op: "list_dir",
            path: path.to_string(),
            a: 0,
            b: 0,
        })?;
        self.inner.list_dir(path)
    }

    fn now(&self) -> std::time::Duration {
        let d = self.inner.now();
        self.enter(Call {
            op: "now",
            path: String::new(),
            a: d.as_nanos() as u64,
            b: 0,
        })
        .ok();
        d
    }

    fn sleep(&self, d: std::time::Duration) {
        if self
            .enter(Call {
                op: "sleep",
                path: String::new(),
                a: d.as_nanos() as u64,
                b: 0,
            })
            .is_err()
        {
            return;
        }
        self.inner.sleep(d)
    }

    fn rand(&self) -> u64 {
        let v = self.inner.rand();
        self.enter(Call {
            op: "rand",
            path: String::new(),
            a: v,
            b: 0,
        })
        .ok();
        v
    }
}

/// An open file of a [`HookEnv`]: the inner file behind the same log and
/// arms. A handle's calls name its open-time path.
#[derive(Debug)]
pub struct HookFile<F: File> {
    inner: F,
    path: String,
    hooks: Arc<Mutex<Hooks>>,
}

impl<F: File> HookFile<F> {
    /// Logs and arms a file call, as [`HookEnv::enter`].
    fn enter(&self, op: &'static str, a: u64, b: u64) -> Result<(), EnvError> {
        let call = Call {
            op,
            path: self.path.clone(),
            a,
            b,
        };
        let mut h = hooks_lock(&self.hooks);
        h.log.push(call.clone());
        if h.refuse_writes && is_write_class(op) {
            return Err(EnvError::ReadOnly { path: call.path });
        }
        let hit = h.arms.iter().position(|arm| arm.matches(&call));
        if let Some(i) = hit {
            let arm = if h.arms[i].once {
                h.arms.remove(i)
            } else {
                h.arms[i].clone()
            };
            return Err(arm.error);
        }
        Ok(())
    }
}

impl<F: File> File for HookFile<F> {
    fn read_at(&self, off: u64, buf: &mut [u8]) -> Result<usize, EnvError> {
        self.enter("read_at", off, buf.len() as u64)?;
        self.inner.read_at(off, buf)
    }

    fn write_at(&self, off: u64, buf: &[u8]) -> Result<(), EnvError> {
        self.enter("write_at", off, buf.len() as u64)?;
        self.inner.write_at(off, buf)
    }

    fn sync_data(&self) -> Result<(), EnvError> {
        self.enter("sync_data", 0, 0)?;
        self.inner.sync_data()
    }

    fn sync_all(&self) -> Result<(), EnvError> {
        self.enter("sync_all", 0, 0)?;
        self.inner.sync_all()
    }

    fn len(&self) -> Result<u64, EnvError> {
        self.enter("len", 0, 0)?;
        self.inner.len()
    }

    fn set_len(&self, len: u64) -> Result<(), EnvError> {
        self.enter("set_len", len, 0)?;
        self.inner.set_len(len)
    }

    fn lock_exclusive(&self) -> Result<(), EnvError> {
        self.enter("lock_exclusive", 0, 0)?;
        self.inner.lock_exclusive()
    }

    fn lock_shared(&self) -> Result<(), EnvError> {
        self.enter("lock_shared", 0, 0)?;
        self.inner.lock_shared()
    }

    fn unlock(&self) -> Result<(), EnvError> {
        self.enter("unlock", 0, 0)?;
        self.inner.unlock()
    }
}

// ---------------------------------------------------------------------
// The SimEnv rounds (Work 10): fault modes, seeds, lives
// ---------------------------------------------------------------------

/// One fault mode of a round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mode {
    /// Its name.
    pub name: &'static str,
    /// Its index: a round's seed is `10_000 × index + k`.
    pub index: u64,
    /// How its power cut resolves each directory's unsynced ops; `None` for
    /// a process crash, which cuts no power.
    pub power_cut: Option<DirLoss>,
    /// The knobs of its power cut.
    pub knobs: FaultKnobs,
}

const NO_TEARS: FaultKnobs = FaultKnobs {
    keep_latest_pm: 0,
    tear_pm: 0,
};
const COIN: FaultKnobs = FaultKnobs {
    keep_latest_pm: 500,
    tear_pm: 500,
};
const MOSTLY: FaultKnobs = FaultKnobs {
    keep_latest_pm: 900,
    tear_pm: 100,
};

/// **P0**: `DirLoss::Prefix`, `keep_latest_pm: 0, tear_pm: 0`.
pub const P0: Mode = Mode {
    name: "P0",
    index: 0,
    power_cut: Some(DirLoss::Prefix),
    knobs: NO_TEARS,
};
/// **P1**: `DirLoss::Independent`, 500, 500.
pub const P1: Mode = Mode {
    name: "P1",
    index: 1,
    power_cut: Some(DirLoss::Independent),
    knobs: COIN,
};
/// **P2**: `Prefix`, 900, 100.
pub const P2: Mode = Mode {
    name: "P2",
    index: 2,
    power_cut: Some(DirLoss::Prefix),
    knobs: MOSTLY,
};
/// **P3**: `Independent`, 0, 0: every unsynced write and directory op is
/// lost, deterministically.
pub const P3: Mode = Mode {
    name: "P3",
    index: 3,
    power_cut: Some(DirLoss::Independent),
    knobs: NO_TEARS,
};
/// **C**: a process crash, no power cut: the page cache survives.
pub const C: Mode = Mode {
    name: "C",
    index: 4,
    power_cut: None,
    knobs: NO_TEARS,
};

/// The four power-cut modes.
pub const POWER_MODES: [Mode; 4] = [P0, P1, P2, P3];

/// A fresh `Sim` for a round of `mode` at crash point `k` (`0` for a round
/// with no crash): its knobs and its `DirLoss` are the mode's, and its seed
/// is `10_000 × mode index + k`.
pub fn sim_for(mode: &Mode, k: u64) -> Sim {
    let mut sim = Sim::new(10_000 * mode.index + k, mode.knobs);
    if let Some(loss) = mode.power_cut {
        sim.set_dir_loss(loss);
    }
    sim
}

/// What one life did that every setup, dry and check life must have done:
/// no crash, no halt, no panic, `Ok(())`.
pub fn assert_clean(out: &LifeOutcome) {
    assert!(!out.panicked, "the life panicked");
    assert!(
        !out.crashed && !out.deadlock,
        "the life halted: crashed={} deadlock={}",
        out.crashed,
        out.deadlock
    );
    assert!(
        out.livelock.is_none(),
        "the life livelocked at {:?}",
        out.livelock
    );
    assert!(
        out.halts.is_empty(),
        "the life raised halts: {:?}",
        out.halts
    );
    assert_eq!(out.results, vec![Ok(())], "the life's result");
}

/// What a life crashed at its `k`-th yield must have done: the crash fired
/// at that yield and nowhere else, the body saw `Crashed`, and no task
/// panicked.
pub fn assert_crashed_at(out: &LifeOutcome, k: u64) {
    assert!(out.crashed, "the life did not crash at yield {k}");
    assert_eq!(out.yields, k, "the crash fired at another yield");
    assert!(!out.deadlock, "the life deadlocked");
    assert!(!out.panicked, "the life panicked");
    assert!(
        out.halts.is_empty(),
        "the life raised halts: {:?}",
        out.halts
    );
    assert_eq!(
        out.results,
        vec![Err(EnvError::Crashed)],
        "the life's result"
    );
}

/// Maps a `StoreError` to the `EnvError` a life returns with (Work 10):
/// `Env(e)` gives `e` (a crash comes back as `Crashed`); anything else is
/// `Halted` with its text, which fails the life.
pub fn to_env_error(e: StoreError) -> EnvError {
    match e {
        StoreError::Env(e) => e,
        other => EnvError::Halted {
            what: other.to_string(),
        },
    }
}

/// What a round's life records as it goes (Work 10): each append's return
/// and each sync's report. The body takes its mutex only between Env calls.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Progress {
    /// Every `append` that returned, in order.
    pub appended: Vec<Appended>,
    /// Every `sync`'s report (`durable_end`), in order.
    pub reports: Vec<u64>,
}

impl Progress {
    /// The last report, or `None` when the life synced nothing.
    pub fn last_report(&self) -> Option<u64> {
        self.reports.last().copied()
    }
}

/// Locks a progress record, recovering from poison.
pub fn progress_lock(p: &Mutex<Progress>) -> MutexGuard<'_, Progress> {
    p.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A shared progress record.
pub fn progress() -> Arc<Mutex<Progress>> {
    Arc::new(Mutex::new(Progress::default()))
}

/// The length of the segment file `first` on the simulated disk between
/// lives, or `None` when no file has that name.
pub fn file_len(sim: &Sim, first: u64) -> Option<u64> {
    let id = sim.file_id(&seg_name(first)).ok().flatten()?;
    Some(sim.disk().cur_len(id))
}

/// Whether a segment file `first` is on the simulated disk between lives.
pub fn file_exists(sim: &Sim, first: u64) -> bool {
    sim.file_id(&seg_name(first)).ok().flatten().is_some()
}

/// The names in `wal/` on the simulated disk between lives, sorted.
pub fn wal_names(sim: &Sim) -> Vec<String> {
    let mut names = sim.disk().dir_names(nucleus_env::sim::WAL_DIR);
    names.sort();
    names
}

/// The current length of the file at `path`, through `env`.
pub fn len_of<E: Env>(env: &E, path: &str) -> Result<u64, EnvError> {
    env.open(path, OpenMode::Read)?.len()
}
