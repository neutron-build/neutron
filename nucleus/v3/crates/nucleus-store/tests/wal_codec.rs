//! Card A-6a's tests (Work 8): the WAL record codec of S-STORE §12.2–§12.3.
//! Golden vectors; the two §15.1 forms this card owns (`t_drop_restart_floor`,
//! `t_ckpt_dep`); round trips; the frame checks; malformed records; the op
//! order rule; the seal against a direct CRC; the placement rule; and the
//! fuzz targets (`fuzz_record`, `fuzz_place`, `fuzz_reach_counts`), driven
//! by proptest.
//!
//! The fixture is held still (card A-6a, Work 8): the five golden records,
//! the segment sizes 256, 4096, 65536 and 64 MiB, the fixed seed and count
//! of `fuzz_reach_counts`, and the value lists of the two named tests. Only
//! the proptest drivers draw at random, and their properties hold for every
//! input; every other generator is a xorshift from a fixed seed.
//!
//! The oracles are this file's own, written from the card's tables and not
//! read from the crate: the offsets, kind bytes, tags and flag bits below;
//! `assemble`, which builds a record byte by byte with a correct CRC (so the
//! decoder can be shown records the encoder refuses); `model_record`, an
//! encoder of every kind and op; `model_frame`, a reading of the frame
//! checks; and `model_place`, the placement rule in `u128` arithmetic. The
//! CRC is `nucleus_store::crc::crc32c`, A-2-0's, pinned there by its test
//! vector; the golden vectors were computed by the card's author with an
//! independent bitwise CRC32C.

use nucleus_store::StoreError;
use nucleus_store::crc::crc32c;
use nucleus_store::wal::{
    Body, Chain, Frame, FrameError, Op, PREV_NONE, Placement, RECORD_HEADER_LEN, RECORD_MAGIC,
    Record, RecordKind, SEGMENT_END_LEN, Sealed, TxnId, decode_body, encode, max_record_len, place,
    read_frame,
};
use proptest::prelude::*;
use proptest::sample::Index;
use proptest::test_runner::{Config, TestRunner};

// ---------------------------------------------------------------------
// The layout, from the card's tables (Work 2 and Work 3)
// ---------------------------------------------------------------------

/// The header's length: magic through `commit_ts`.
const HEADER: usize = 53;
/// Where `len` sits.
const LEN_AT: usize = 4;
/// Where `crc` sits.
const CRC_AT: usize = 8;
/// Where `lsn` sits: the first byte under the CRC.
const LSN_AT: usize = 12;
/// Where `prev_lsn` sits.
const PREV_LSN_AT: usize = 20;
/// Where `prev_crc` sits.
const PREV_CRC_AT: usize = 28;
/// Where `kind` sits.
const KIND_AT: usize = 32;
/// Where `txn.boot` sits.
const BOOT_AT: usize = 33;
/// Where `txn.n` sits.
const N_AT: usize = 37;
/// Where `commit_ts` sits.
const TS_AT: usize = 45;

/// Kind bytes (§12.2).
const K_COMMIT: u8 = 1;
const K_CHUNK: u8 = 2;
const K_MARKER: u8 = 3;
const K_SEGEND: u8 = 4;

/// Op tags (card A-6a, Work 3).
const T_CREATE: u8 = 1;
const T_DROP: u8 = 2;
const T_DDL: u8 = 3;
const T_PUT: u8 = 4;
const T_DELETE: u8 = 5;
const T_REQUIRE: u8 = 6;

/// The fixture's segment sizes (card A-6a, Work 8).
const SEG_SIZES: [u64; 4] = [256, 4096, 65536, 64 << 20];

// ---------------------------------------------------------------------
// Bytes
// ---------------------------------------------------------------------

/// Lower-case hex of `b`.
fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// The bytes a lower-case hex string spells.
fn unhex(s: &str) -> Vec<u8> {
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap_or(0))
        .collect()
}

/// The little-endian `u32` at `at`.
fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}

/// The little-endian `u64` at `at`.
fn u64_at(b: &[u8], at: usize) -> u64 {
    let mut le = [0u8; 8];
    le.copy_from_slice(&b[at..at + 8]);
    u64::from_le_bytes(le)
}

/// A record's header fields, for `assemble`.
#[derive(Clone, Copy, Debug)]
struct Hdr {
    lsn: u64,
    prev_lsn: u64,
    prev_crc: u32,
    kind: u8,
    boot: u32,
    n: u64,
    commit_ts: u64,
}

/// A Commit header with ordinary fields.
fn commit_hdr() -> Hdr {
    Hdr {
        lsn: 4096,
        prev_lsn: 4000,
        prev_crc: 0x1234_5678,
        kind: K_COMMIT,
        boot: 7,
        n: 42,
        commit_ts: 99,
    }
}

/// A Chunk header with ordinary fields.
fn chunk_hdr() -> Hdr {
    Hdr {
        kind: K_CHUNK,
        commit_ts: 0,
        ..commit_hdr()
    }
}

/// A record built byte by byte from the card's table, with a correct CRC:
/// magic, `len`, `crc`, the chain fields, `kind`, `txn` and `commit_ts`,
/// then `payload` (card A-6a, Work 8's `assemble`).
fn assemble(h: &Hdr, payload: &[u8]) -> Vec<u8> {
    let len = HEADER + payload.len();
    let mut b = Vec::with_capacity(len);
    b.extend_from_slice(b"NWAL");
    b.extend_from_slice(&(len as u32).to_le_bytes());
    b.extend_from_slice(&[0; 4]);
    b.extend_from_slice(&h.lsn.to_le_bytes());
    b.extend_from_slice(&h.prev_lsn.to_le_bytes());
    b.extend_from_slice(&h.prev_crc.to_le_bytes());
    b.push(h.kind);
    b.extend_from_slice(&h.boot.to_le_bytes());
    b.extend_from_slice(&h.n.to_le_bytes());
    b.extend_from_slice(&h.commit_ts.to_le_bytes());
    b.extend_from_slice(payload);
    reseal(&mut b);
    b
}

/// Re-seals a record in place: `crc` becomes the CRC32C of bytes 12..`len`,
/// when `len` is at least a header and at most the buffer.
fn reseal(b: &mut [u8]) {
    if b.len() < HEADER {
        return;
    }
    let len = u32_at(b, LEN_AT) as usize;
    if (HEADER..=b.len()).contains(&len) {
        let crc = crc32c(0, &b[LSN_AT..len]);
        b[CRC_AT..CRC_AT + 4].copy_from_slice(&crc.to_le_bytes());
    }
}

/// Whether `r` is an XX001 (`StoreError::Corrupt`).
fn is_corrupt<T>(r: &Result<T, StoreError>) -> bool {
    matches!(r, Err(StoreError::Corrupt(_)))
}

/// Whether `r` is a caller's bug (`StoreError::Invalid`).
fn is_invalid<T>(r: &Result<T, StoreError>) -> bool {
    matches!(r, Err(StoreError::Invalid(_)))
}

// ---------------------------------------------------------------------
// The model encoder (the card's tables, written here, not the crate's)
// ---------------------------------------------------------------------

/// Appends byte string `s`: a `u32` length, then its bytes.
fn model_string(out: &mut Vec<u8>, s: &[u8]) {
    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
    out.extend_from_slice(s);
}

/// Appends an optional `before`: a presence byte, then a byte string.
fn model_before(out: &mut Vec<u8>, before: Option<&[u8]>) {
    match before {
        None => out.push(0),
        Some(b) => {
            out.push(1);
            model_string(out, b);
        }
    }
}

/// Appends `op` as the card's op table gives it. The match names every op,
/// so an op added to the crate fails to compile here until it is modelled.
fn model_op(out: &mut Vec<u8>, op: &Op<'_>) {
    match *op {
        Op::CreateTree { tree_id, kind } => {
            out.push(T_CREATE);
            out.extend_from_slice(&tree_id.to_le_bytes());
            out.push(kind);
        }
        Op::DropTree { tree_id, w } => {
            out.push(T_DROP);
            out.extend_from_slice(&tree_id.to_le_bytes());
            out.extend_from_slice(&w.to_le_bytes());
        }
        Op::Ddl { bytes } => {
            out.push(T_DDL);
            model_string(out, bytes);
        }
        Op::Put {
            tree_id,
            key,
            key_changed,
            value,
            before,
        } => {
            out.push(T_PUT);
            out.extend_from_slice(&tree_id.to_le_bytes());
            model_string(out, key);
            out.push(u8::from(key_changed));
            model_string(out, value);
            model_before(out, before);
        }
        Op::Delete {
            tree_id,
            key,
            moved,
            revert,
            before,
        } => {
            out.push(T_DELETE);
            out.extend_from_slice(&tree_id.to_le_bytes());
            model_string(out, key);
            out.push(u8::from(moved) | (u8::from(revert) << 1));
            model_before(out, before);
        }
        Op::RequireCkpt { ckpt_id } => {
            out.push(T_REQUIRE);
            out.extend_from_slice(&ckpt_id.to_le_bytes());
        }
    }
}

/// A body's payload as the card's table gives it.
fn model_payload(body: &Body<'_>) -> Vec<u8> {
    let mut out = Vec::new();
    match body {
        Body::Commit { ops, .. } => ops.iter().for_each(|op| model_op(&mut out, op)),
        Body::Chunk { seq, ops } => {
            out.extend_from_slice(&seq.to_le_bytes());
            ops.iter().for_each(|op| model_op(&mut out, op));
        }
        Body::CommitMarker { nchunks, .. } => out.extend_from_slice(&nchunks.to_le_bytes()),
        Body::SegmentEnd => {}
    }
    out
}

/// A record's header as the card's table gives it, sealed with `chain`.
fn model_header(rec: &Record<'_>, chain: Chain) -> Hdr {
    let (kind, commit_ts) = match rec.body {
        Body::Commit { commit_ts, .. } => (K_COMMIT, commit_ts),
        Body::Chunk { .. } => (K_CHUNK, 0),
        Body::CommitMarker { commit_ts, .. } => (K_MARKER, commit_ts),
        Body::SegmentEnd => (K_SEGEND, 0),
    };
    let Chain {
        lsn,
        prev_lsn,
        prev_crc,
    } = chain;
    let TxnId { boot, n } = rec.txn;
    Hdr {
        lsn,
        prev_lsn,
        prev_crc,
        kind,
        boot,
        n,
        commit_ts,
    }
}

/// The whole record, sealed with `chain`, from the model.
fn model_record(rec: &Record<'_>, chain: Chain) -> Vec<u8> {
    assemble(&model_header(rec, chain), &model_payload(&rec.body))
}

/// A record's ops (none for a CommitMarker or a SegmentEnd).
fn ops_of<'r, 'a>(rec: &'r Record<'a>) -> &'r [Op<'a>] {
    match &rec.body {
        Body::Commit { ops, .. } | Body::Chunk { ops, .. } => ops,
        Body::CommitMarker { .. } | Body::SegmentEnd => &[],
    }
}

/// An op's index in `fuzz_reach_counts`' rows: its tag minus one.
fn op_index(op: &Op<'_>) -> usize {
    match op {
        Op::CreateTree { .. } => 0,
        Op::DropTree { .. } => 1,
        Op::Ddl { .. } => 2,
        Op::Put { .. } => 3,
        Op::Delete { .. } => 4,
        Op::RequireCkpt { .. } => 5,
    }
}

/// Checks every field of `frame` against the bytes of the record it was
/// read from. The pattern names every field, so a field added to `Frame`
/// fails to compile here until it is checked.
fn check_frame(frame: &Frame<'_>, bytes: &[u8]) -> Result<(), String> {
    let Frame {
        len,
        crc,
        lsn,
        prev_lsn,
        prev_crc,
        kind,
        txn: TxnId { boot, n },
        commit_ts,
        payload,
    } = *frame;
    let want_len = u32_at(bytes, LEN_AT);
    let got = (
        len,
        crc,
        lsn,
        prev_lsn,
        prev_crc,
        kind,
        boot,
        n,
        commit_ts,
        hex(payload),
    );
    let want = (
        want_len,
        u32_at(bytes, CRC_AT),
        u64_at(bytes, LSN_AT),
        u64_at(bytes, PREV_LSN_AT),
        u32_at(bytes, PREV_CRC_AT),
        bytes[KIND_AT],
        u32_at(bytes, BOOT_AT),
        u64_at(bytes, N_AT),
        u64_at(bytes, TS_AT),
        hex(&bytes[HEADER..want_len as usize]),
    );
    if got == want {
        Ok(())
    } else {
        Err(format!("frame {got:?}, bytes say {want:?}"))
    }
}

/// `decode_body` of the frame `read_frame` reads from `bytes`; `Err` if the
/// frame does not read.
fn decode(bytes: &[u8]) -> Result<Result<Record<'_>, StoreError>, String> {
    read_frame(bytes)
        .map(|f| decode_body(&f))
        .map_err(|e| format!("read_frame refused {}: {e:?}", hex(bytes)))
}

/// Encodes `rec`, seals it with `chain`, reads and decodes it back, and
/// checks each step: the bytes against the model, the frame's fields
/// against the bytes (its chain fields are `chain`'s), and the decoded
/// record against `rec`.
fn round_trip(rec: &Record<'_>, chain: Chain) -> Result<Sealed, String> {
    let unsealed = encode(rec).map_err(|e| format!("encode refused {rec:?}: {e}"))?;
    let want = model_record(rec, chain);
    if unsealed.len() != want.len() {
        return Err(format!(
            "encode gave {} bytes, the model {} for {rec:?}",
            unsealed.len(),
            want.len()
        ));
    }
    let sealed = unsealed.seal(chain);
    if sealed.bytes() != want.as_slice() {
        return Err(format!(
            "sealed {}\nmodel  {}\nfor {rec:?}",
            hex(sealed.bytes()),
            hex(&want)
        ));
    }
    if sealed.crc() != u32_at(&want, CRC_AT) || sealed.lsn() != chain.lsn {
        return Err(format!(
            "Sealed::crc {:#010x} / lsn {} disagree with the bytes",
            sealed.crc(),
            sealed.lsn()
        ));
    }
    let frame = read_frame(sealed.bytes()).map_err(|e| format!("read_frame: {e:?}"))?;
    check_frame(&frame, &want)?;
    if (frame.lsn, frame.prev_lsn, frame.prev_crc) != (chain.lsn, chain.prev_lsn, chain.prev_crc) {
        return Err(format!("the frame's chain fields are not {chain:?}"));
    }
    let back = decode_body(&frame).map_err(|e| format!("decode_body: {e}"))?;
    if back != *rec {
        return Err(format!("decoded {back:?}\nencoded {rec:?}"));
    }
    Ok(sealed)
}

/// The `(index, value)` pairs that `pick` takes from the ops of `rec` after
/// encode, seal, `read_frame` and `decode_body`, and no other check: the
/// value round trip that §15.1's two forms for this card state. Their tests
/// run it over every case before `round_trip`'s byte checks, so a codec that
/// loses a value fails there first.
fn decoded_picks(
    rec: &Record<'_>,
    chain: Chain,
    pick: fn(&Op<'_>) -> Option<u64>,
) -> Result<Vec<(usize, u64)>, String> {
    let sealed = encode(rec).map_err(|e| format!("encode: {e}"))?.seal(chain);
    let frame = read_frame(sealed.bytes()).map_err(|e| format!("read_frame: {e:?}"))?;
    let back = decode_body(&frame).map_err(|e| format!("decode_body: {e}"))?;
    Ok(ops_of(&back)
        .iter()
        .enumerate()
        .filter_map(|(i, op)| pick(op).map(|v| (i, v)))
        .collect())
}

/// A `DropTree{12, w}`'s `w`.
fn drop_w(op: &Op<'_>) -> Option<u64> {
    match op {
        Op::DropTree { tree_id: 12, w } => Some(*w),
        _ => None,
    }
}

/// A `RequireCkpt`'s `ckpt_id`.
fn require_id(op: &Op<'_>) -> Option<u64> {
    match op {
        Op::RequireCkpt { ckpt_id } => Some(*ckpt_id),
        _ => None,
    }
}

// ---------------------------------------------------------------------
// Owned records, for the generators
// ---------------------------------------------------------------------

/// An op that owns its byte strings.
#[derive(Clone, Debug)]
enum OwnedOp {
    CreateTree(u64, u8),
    DropTree(u64, u64),
    Ddl(Vec<u8>),
    Put(u64, Vec<u8>, bool, Vec<u8>, Option<Vec<u8>>),
    Delete(u64, Vec<u8>, bool, bool, Option<Vec<u8>>),
    RequireCkpt(u64),
}

impl OwnedOp {
    /// The borrowed op.
    fn op(&self) -> Op<'_> {
        match self {
            OwnedOp::CreateTree(tree_id, kind) => Op::CreateTree {
                tree_id: *tree_id,
                kind: *kind,
            },
            OwnedOp::DropTree(tree_id, w) => Op::DropTree {
                tree_id: *tree_id,
                w: *w,
            },
            OwnedOp::Ddl(bytes) => Op::Ddl { bytes },
            OwnedOp::Put(tree_id, key, key_changed, value, before) => Op::Put {
                tree_id: *tree_id,
                key,
                key_changed: *key_changed,
                value,
                before: before.as_deref(),
            },
            OwnedOp::Delete(tree_id, key, moved, revert, before) => Op::Delete {
                tree_id: *tree_id,
                key,
                moved: *moved,
                revert: *revert,
                before: before.as_deref(),
            },
            OwnedOp::RequireCkpt(ckpt_id) => Op::RequireCkpt { ckpt_id: *ckpt_id },
        }
    }
}

/// A body that owns its ops.
#[derive(Clone, Debug)]
enum OwnedBody {
    Commit(u64, Vec<OwnedOp>),
    Chunk(u32, Vec<OwnedOp>),
    CommitMarker(u64, u32),
    SegmentEnd,
}

/// A record that owns its ops.
#[derive(Clone, Debug)]
struct OwnedRecord {
    txn: TxnId,
    body: OwnedBody,
}

impl OwnedRecord {
    /// The borrowed record.
    fn record(&self) -> Record<'_> {
        let body = match &self.body {
            OwnedBody::Commit(commit_ts, o) => Body::Commit {
                commit_ts: *commit_ts,
                ops: o.iter().map(OwnedOp::op).collect(),
            },
            OwnedBody::Chunk(seq, o) => Body::Chunk {
                seq: *seq,
                ops: o.iter().map(OwnedOp::op).collect(),
            },
            OwnedBody::CommitMarker(commit_ts, nchunks) => Body::CommitMarker {
                commit_ts: *commit_ts,
                nchunks: *nchunks,
            },
            OwnedBody::SegmentEnd => Body::SegmentEnd,
        };
        Record {
            txn: self.txn,
            body,
        }
    }
}

/// `ops` put in a valid order (§12.3; card A-6a, Work 3): the first
/// `RequireCkpt` only, as op 0; then the `CreateTree`, `DropTree` and `Ddl`
/// ops; then the `Put` and `Delete` ops; each group in the order drawn, and
/// `revert` cleared outside a Chunk.
fn valid_order(ops: Vec<OwnedOp>, in_chunk: bool) -> Vec<OwnedOp> {
    let mut require = None;
    let mut ddl = Vec::new();
    let mut data = Vec::new();
    for op in ops {
        match op {
            OwnedOp::RequireCkpt(_) => {
                if require.is_none() {
                    require = Some(op);
                }
            }
            OwnedOp::CreateTree(..) | OwnedOp::DropTree(..) | OwnedOp::Ddl(_) => ddl.push(op),
            OwnedOp::Put(..) => data.push(op),
            OwnedOp::Delete(tree_id, key, moved, revert, before) => data.push(OwnedOp::Delete(
                tree_id,
                key,
                moved,
                revert && in_chunk,
                before,
            )),
        }
    }
    require.into_iter().chain(ddl).chain(data).collect()
}

/// A xorshift64 generator from a fixed seed: the deterministic draws of
/// this file outside proptest.
struct XorShift(u64);

impl XorShift {
    /// The generator from `seed` (0 is replaced, since xorshift stays at 0).
    fn new(seed: u64) -> Self {
        XorShift(if seed == 0 {
            0x9E37_79B9_7F4A_7C15
        } else {
            seed
        })
    }

    /// The next 64 bits.
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// A draw in `0..n`.
    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    /// A coin.
    fn coin(&mut self) -> bool {
        self.below(2) == 1
    }

    /// Up to `max` bytes.
    fn bytes(&mut self, max: u64) -> Vec<u8> {
        let n = self.below(max + 1);
        (0..n).map(|_| self.next_u64() as u8).collect()
    }
}

/// An op of a kind drawn with each of the six equally likely, its fields
/// drawn too; `revert` only in a Chunk.
fn random_op(rng: &mut XorShift, in_chunk: bool) -> OwnedOp {
    let tree_id = rng.next_u64();
    match rng.below(6) {
        0 => OwnedOp::CreateTree(tree_id, rng.next_u64() as u8),
        1 => OwnedOp::DropTree(tree_id, rng.next_u64()),
        2 => OwnedOp::Ddl(rng.bytes(16)),
        3 => {
            let key = rng.bytes(16);
            let key_changed = rng.coin();
            let value = rng.bytes(16);
            let before = rng.coin().then(|| rng.bytes(16));
            OwnedOp::Put(tree_id, key, key_changed, value, before)
        }
        4 => {
            let key = rng.bytes(16);
            let moved = rng.coin();
            let revert = in_chunk && rng.coin();
            let before = rng.coin().then(|| rng.bytes(16));
            OwnedOp::Delete(tree_id, key, moved, revert, before)
        }
        _ => OwnedOp::RequireCkpt(rng.next_u64()),
    }
}

/// A Commit or a Chunk (a coin) of up to eight drawn ops in a valid order,
/// with a drawn txn, commit_ts or seq, and a drawn chain.
fn random_commit_or_chunk(rng: &mut XorShift) -> (OwnedRecord, Chain) {
    let in_chunk = rng.coin();
    let k = rng.below(9);
    let drawn = (0..k).map(|_| random_op(rng, in_chunk)).collect();
    let ops = valid_order(drawn, in_chunk);
    let txn = TxnId {
        boot: rng.next_u64() as u32,
        n: 1 + rng.below(u64::MAX - 1),
    };
    let body = if in_chunk {
        OwnedBody::Chunk(rng.next_u64() as u32, ops)
    } else {
        OwnedBody::Commit(1 + rng.below(u64::MAX - 1), ops)
    };
    let chain = Chain {
        lsn: rng.next_u64(),
        prev_lsn: rng.next_u64(),
        prev_crc: rng.next_u64() as u32,
    };
    (OwnedRecord { txn, body }, chain)
}

// ---------------------------------------------------------------------
// The golden records (card A-6a, Work 8)
// ---------------------------------------------------------------------

/// One golden record: the record, its chain, and the bytes it must seal to.
struct Golden {
    name: &'static str,
    record: Record<'static>,
    chain: Chain,
    len: usize,
    crc: u32,
    bytes: Vec<u8>,
}

/// The card's five golden records. The first four carry the card's bytes;
/// the fifth, for which the card gives the fields, the length and the crc,
/// is assembled from its fields here and its crc compared to the card's.
fn golden() -> [Golden; 5] {
    [
        Golden {
            name: "Commit with every op",
            record: Record {
                txn: TxnId { boot: 7, n: 42 },
                body: Body::Commit {
                    commit_ts: 99,
                    ops: vec![
                        Op::RequireCkpt { ckpt_id: 5 },
                        Op::CreateTree {
                            tree_id: 11,
                            kind: 1,
                        },
                        Op::DropTree { tree_id: 12, w: 77 },
                        Op::Ddl { bytes: b"ddl" },
                        Op::Put {
                            tree_id: 11,
                            key: b"k",
                            key_changed: true,
                            value: b"v",
                            before: None,
                        },
                        Op::Delete {
                            tree_id: 11,
                            key: b"k2",
                            moved: true,
                            revert: false,
                            before: Some(b"old"),
                        },
                    ],
                },
            },
            chain: Chain {
                lsn: 4096,
                prev_lsn: 4000,
                prev_crc: 0x1234_5678,
            },
            len: 142,
            crc: 0xE8B8_8C5C,
            bytes: unhex(
                "4e57414c8e0000005c8cb8e80010000000000000a00f0000000000007856341201070000002a000000000000006300000000000000060500000000000000010b0000000000000001020c000000000000004d00000000000000030300000064646c040b00000000000000010000006b01010000007600050b00000000000000020000006b320101030000006f6c64",
            ),
        },
        Golden {
            name: "Chunk with DropTree and a revert",
            record: Record {
                txn: TxnId { boot: 7, n: 43 },
                body: Body::Chunk {
                    seq: 3,
                    ops: vec![
                        Op::DropTree { tree_id: 12, w: 77 },
                        Op::Delete {
                            tree_id: 11,
                            key: b"k",
                            moved: false,
                            revert: true,
                            before: None,
                        },
                    ],
                },
            },
            chain: Chain {
                lsn: 8192,
                prev_lsn: 4096,
                prev_crc: 0x0BAD_F00D,
            },
            len: 90,
            crc: 0x37C7_0955,
            bytes: unhex(
                "4e57414c5a0000005509c737002000000000000000100000000000000df0ad0b02070000002b00000000000000000000000000000003000000020c000000000000004d00000000000000050b00000000000000010000006b0200",
            ),
        },
        Golden {
            name: "CommitMarker",
            record: Record {
                txn: TxnId { boot: 7, n: 43 },
                body: Body::CommitMarker {
                    commit_ts: 100,
                    nchunks: 2,
                },
            },
            chain: Chain {
                lsn: 12288,
                prev_lsn: 8192,
                prev_crc: 0x0BAD_BEEF,
            },
            len: 57,
            crc: 0xB501_0B2E,
            bytes: unhex(
                "4e57414c390000002e0b01b500300000000000000020000000000000efbead0b03070000002b00000000000000640000000000000002000000",
            ),
        },
        Golden {
            name: "SegmentEnd",
            record: Record {
                txn: TxnId { boot: 7, n: 0 },
                body: Body::SegmentEnd,
            },
            chain: Chain {
                lsn: 65483,
                prev_lsn: 65400,
                prev_crc: 0xDEAD_BEEF,
            },
            len: 53,
            crc: 0x3BED_804A,
            bytes: unhex(
                "4e57414c350000004a80ed3bcbff00000000000078ff000000000000efbeadde040700000000000000000000000000000000000000",
            ),
        },
        Golden {
            name: "a boot's first record, no predecessor",
            record: Record {
                txn: TxnId { boot: 0, n: 1 },
                body: Body::Commit {
                    commit_ts: 1,
                    ops: vec![],
                },
            },
            chain: Chain {
                lsn: 0,
                prev_lsn: PREV_NONE.0,
                prev_crc: PREV_NONE.1,
            },
            len: 53,
            crc: 0x5845_216B,
            bytes: assemble(
                &Hdr {
                    lsn: 0,
                    prev_lsn: u64::MAX,
                    prev_crc: 0,
                    kind: K_COMMIT,
                    boot: 0,
                    n: 1,
                    commit_ts: 1,
                },
                &[],
            ),
        },
    ]
}

/// The golden vectors (card A-6a, Work 8): each record, encoded and sealed
/// with its chain, is the card's bytes exactly, and `read_frame` and
/// `decode_body` give it back.
#[test]
fn wal_golden() {
    assert_eq!(RECORD_MAGIC, *b"NWAL");
    assert_eq!(RECORD_HEADER_LEN, HEADER);
    assert_eq!(SEGMENT_END_LEN, HEADER as u64);
    assert_eq!(PREV_NONE, (u64::MAX, 0));
    for g in golden() {
        assert_eq!(g.bytes.len(), g.len, "{}: the fixture's length", g.name);
        assert_eq!(
            u32_at(&g.bytes, CRC_AT),
            g.crc,
            "{}: the fixture's crc",
            g.name
        );
        assert_eq!(
            model_record(&g.record, g.chain),
            g.bytes,
            "{}: model",
            g.name
        );
        let unsealed = encode(&g.record).unwrap();
        assert_eq!(unsealed.len(), g.len, "{}", g.name);
        let sealed = unsealed.seal(g.chain);
        assert_eq!(hex(sealed.bytes()), hex(&g.bytes), "{}", g.name);
        assert_eq!(sealed.crc(), g.crc, "{}", g.name);
        assert_eq!(sealed.lsn(), g.chain.lsn, "{}", g.name);
        assert_eq!(sealed.clone().into_bytes(), g.bytes, "{}", g.name);
        let frame = read_frame(&g.bytes).unwrap();
        check_frame(&frame, &g.bytes).unwrap();
        assert_eq!(decode_body(&frame).unwrap(), g.record, "{}", g.name);
        assert_eq!(
            round_trip(&g.record, g.chain).unwrap(),
            sealed,
            "{}",
            g.name
        );
    }
}

// ---------------------------------------------------------------------
// §15.1's two forms for this card
// ---------------------------------------------------------------------

/// `t_drop_restart_floor`, A-6a's form (§15.1; I-GCW): `DropTree`'s `w`
/// encodes and decodes inside `Commit` and `Chunk` records, and the fuzz
/// targets reach it. DROP-OP-W-ZERO fails (1).
#[test]
fn t_drop_restart_floor() {
    // (1) `w` exact, alone and among other ops, in a Commit and a Chunk.
    let chain = Chain {
        lsn: 1 << 20,
        prev_lsn: (1 << 20) - 90,
        prev_crc: 0xFEED_F00D,
    };
    let mut cases = Vec::new();
    for w in [1, 77, (1u64 << 40) + 3, u64::MAX] {
        let drop = Op::DropTree { tree_id: 12, w };
        let commit = |ops: Vec<Op<'static>>| Record {
            txn: TxnId { boot: 7, n: 44 },
            body: Body::Commit {
                commit_ts: 1000,
                ops,
            },
        };
        let chunk = |ops: Vec<Op<'static>>| Record {
            txn: TxnId { boot: 7, n: 45 },
            body: Body::Chunk { seq: 2, ops },
        };
        let records = [
            commit(vec![drop]),
            commit(vec![
                Op::RequireCkpt { ckpt_id: 9 },
                Op::CreateTree {
                    tree_id: 13,
                    kind: 1,
                },
                drop,
                Op::Ddl { bytes: b"swap" },
                Op::Put {
                    tree_id: 13,
                    key: b"k",
                    key_changed: false,
                    value: b"v",
                    before: None,
                },
                Op::Delete {
                    tree_id: 13,
                    key: b"j",
                    moved: true,
                    revert: false,
                    before: Some(b"old"),
                },
            ]),
            chunk(vec![drop]),
            chunk(vec![
                Op::CreateTree {
                    tree_id: 13,
                    kind: 2,
                },
                drop,
                Op::Ddl { bytes: b"swap" },
                Op::Delete {
                    tree_id: 13,
                    key: b"k",
                    moved: false,
                    revert: true,
                    before: None,
                },
            ]),
        ];
        cases.extend(records.into_iter().map(|rec| (w, rec)));
    }
    for (w, rec) in &cases {
        let ws: Vec<u64> = decoded_picks(rec, chain, drop_w)
            .unwrap()
            .into_iter()
            .map(|(_, v)| v)
            .collect();
        assert_eq!(ws, [*w], "w {w} in {rec:?}");
    }
    // (1) Every byte against the model; (2) the fuzz target takes each.
    for (w, rec) in &cases {
        let sealed = round_trip(rec, chain).unwrap();
        assert_eq!(fuzz_record(sealed.bytes()), Ok(()), "w {w} in {rec:?}");
    }
    // (1) The two golden records holding DropTree{12, 77}, byte for byte.
    for g in golden().into_iter().take(2) {
        let sealed = encode(&g.record).unwrap().seal(g.chain);
        assert_eq!(hex(sealed.bytes()), hex(&g.bytes), "{}", g.name);
        let back = decode(&g.bytes).unwrap().unwrap();
        assert!(
            ops_of(&back).contains(&Op::DropTree { tree_id: 12, w: 77 }),
            "{}: {back:?}",
            g.name
        );
    }
    // (2) The fuzz targets reach it, in Commit and in Chunk records.
    let counts = fuzz_reach_counts(1, 2000);
    println!("fuzz_reach_counts(1, 2000) = {counts:?}");
    assert!(
        counts[0][1] >= 1 && counts[1][1] >= 1,
        "DropTree not reached: {counts:?}"
    );
}

/// `t_ckpt_dep`, A-6a's form (§15.1; I-CKPT-DEP): `RequireCkpt` encodes and
/// decodes inside `Commit` and `Chunk` records, first in its record, the
/// fuzz targets reach it, and no other op's bytes decode as it. §15 names
/// no mutant for this form; card mutants REQUIRE-ID-LOW32 and
/// REQUIRE-NOT-FIRST fail it.
#[test]
fn t_ckpt_dep() {
    let chain = Chain {
        lsn: 3 << 20,
        prev_lsn: (3 << 20) - 57,
        prev_crc: 0x00DD_BA11,
    };
    let txn = TxnId { boot: 3, n: 9 };
    let put = Op::Put {
        tree_id: 21,
        key: b"pk",
        key_changed: false,
        value: b"row",
        before: None,
    };
    let create = Op::CreateTree {
        tree_id: 21,
        kind: 2,
    };
    let revert = Op::Delete {
        tree_id: 21,
        key: b"pk",
        moved: false,
        revert: true,
        before: None,
    };
    // (1) Round trips with `ckpt_id` exact, as op 0: first the values alone.
    let mut cases = Vec::new();
    for ckpt_id in [1, (1u64 << 32) + 7, u64::MAX - 1] {
        let req = Op::RequireCkpt { ckpt_id };
        let bodies = [
            Body::Commit {
                commit_ts: 500,
                ops: vec![req],
            },
            Body::Commit {
                commit_ts: 500,
                ops: vec![req, create, put],
            },
            Body::Chunk {
                seq: 0,
                ops: vec![req],
            },
            Body::Chunk {
                seq: 1,
                ops: vec![req, revert],
            },
        ];
        cases.extend(
            bodies
                .into_iter()
                .map(|body| (ckpt_id, Record { txn, body })),
        );
    }
    for (ckpt_id, rec) in &cases {
        let ids = decoded_picks(rec, chain, require_id).unwrap();
        assert_eq!(ids, [(0, *ckpt_id)], "ckpt_id {ckpt_id} in {rec:?}");
    }
    // (1) Every byte against the model; (4) the fuzz target takes each.
    for (ckpt_id, rec) in &cases {
        let sealed = round_trip(rec, chain).unwrap();
        assert_eq!(
            fuzz_record(sealed.bytes()),
            Ok(()),
            "ckpt_id {ckpt_id} in {rec:?}"
        );
    }
    // (2) First in its record: refused by the encoder, Corrupt from the
    // decoder, in a Commit and in a Chunk.
    let req = Op::RequireCkpt { ckpt_id: 5 };
    for ops in [[create, req], [put, req], [req, req]] {
        for in_chunk in [false, true] {
            let body = if in_chunk {
                Body::Chunk {
                    seq: 0,
                    ops: ops.to_vec(),
                }
            } else {
                Body::Commit {
                    commit_ts: 500,
                    ops: ops.to_vec(),
                }
            };
            let rec = Record { txn, body };
            assert!(is_invalid(&encode(&rec)), "encode took {rec:?}");
            let bytes = model_record(&rec, chain);
            let got = decode(&bytes).unwrap();
            assert!(is_corrupt(&got), "decode_body took {rec:?}: {got:?}");
        }
    }
    // (3) No other op's bytes decode as it: every tag byte, then eight
    // bytes of 0x01, as a Commit's whole payload.
    for tag in 0..=255u8 {
        let mut payload = vec![tag];
        payload.extend_from_slice(&[0x01; 8]);
        let bytes = assemble(&commit_hdr(), &payload);
        let got = decode(&bytes).unwrap();
        if tag == T_REQUIRE {
            let rec = got.unwrap();
            assert_eq!(
                rec.body,
                Body::Commit {
                    commit_ts: 99,
                    ops: vec![Op::RequireCkpt {
                        ckpt_id: 0x0101_0101_0101_0101
                    }],
                },
            );
        } else {
            assert!(is_corrupt(&got), "tag {tag}: {got:?}");
        }
    }
    // (3) Records of every other op kind, random fields, round-trip with no
    // RequireCkpt in the result (proptest, 128 cases).
    let mut runner = TestRunner::new(Config {
        cases: 128,
        source_file: Some(file!()),
        ..Config::default()
    });
    let strategy = (
        prop_oneof![
            (
                any::<u32>(),
                1..=u64::MAX,
                1..u64::MAX,
                ops_strategy(false, false)
            )
                .prop_map(|(boot, n, ts, ops)| OwnedRecord {
                    txn: TxnId { boot, n },
                    body: OwnedBody::Commit(ts, ops),
                }),
            (
                any::<u32>(),
                1..=u64::MAX,
                any::<u32>(),
                ops_strategy(true, false)
            )
                .prop_map(|(boot, n, seq, ops)| OwnedRecord {
                    txn: TxnId { boot, n },
                    body: OwnedBody::Chunk(seq, ops),
                }),
        ],
        chain_strategy(),
    );
    runner
        .run(&strategy, |(owned, chain)| {
            let rec = owned.record();
            let sealed = round_trip(&rec, chain).map_err(TestCaseError::fail)?;
            let back = decode(sealed.bytes())
                .map_err(TestCaseError::fail)?
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
            prop_assert!(
                !ops_of(&back)
                    .iter()
                    .any(|op| matches!(op, Op::RequireCkpt { .. })),
                "a RequireCkpt decoded from {rec:?}"
            );
            Ok(())
        })
        .unwrap();
    // (4) The fuzz targets reach it, in Commit and in Chunk records.
    let counts = fuzz_reach_counts(1, 2000);
    println!("fuzz_reach_counts(1, 2000) = {counts:?}");
    assert!(
        counts[0][5] >= 1 && counts[1][5] >= 1,
        "RequireCkpt not reached: {counts:?}"
    );
}

// ---------------------------------------------------------------------
// Round trips, frames, malformed records, order, seal, placement
// ---------------------------------------------------------------------

/// Up to 300 bytes (card A-6a, Work 8: "random lengths (0–300 bytes per
/// string)").
fn bytes_strategy() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..=300)
}

/// An op with random fields, of any kind, or of any kind but
/// `RequireCkpt`.
fn op_strategy(with_require: bool) -> BoxedStrategy<OwnedOp> {
    let create = (any::<u64>(), any::<u8>()).prop_map(|(t, k)| OwnedOp::CreateTree(t, k));
    let drop = (any::<u64>(), any::<u64>()).prop_map(|(t, w)| OwnedOp::DropTree(t, w));
    let ddl = bytes_strategy().prop_map(OwnedOp::Ddl);
    let put = (
        any::<u64>(),
        bytes_strategy(),
        any::<bool>(),
        bytes_strategy(),
        proptest::option::of(bytes_strategy()),
    )
        .prop_map(|(t, k, c, v, b)| OwnedOp::Put(t, k, c, v, b));
    let delete = (
        any::<u64>(),
        bytes_strategy(),
        any::<bool>(),
        any::<bool>(),
        proptest::option::of(bytes_strategy()),
    )
        .prop_map(|(t, k, m, r, b)| OwnedOp::Delete(t, k, m, r, b));
    if with_require {
        let require = any::<u64>().prop_map(OwnedOp::RequireCkpt);
        prop_oneof![create, drop, ddl, put, delete, require].boxed()
    } else {
        prop_oneof![create, drop, ddl, put, delete].boxed()
    }
}

/// Up to six random ops, put in a valid order.
fn ops_strategy(in_chunk: bool, with_require: bool) -> impl Strategy<Value = Vec<OwnedOp>> {
    proptest::collection::vec(op_strategy(with_require), 0..=6)
        .prop_map(move |ops| valid_order(ops, in_chunk))
}

/// A valid record of any kind with random fields.
fn record_strategy() -> impl Strategy<Value = OwnedRecord> {
    prop_oneof![
        (
            any::<u32>(),
            1..=u64::MAX,
            1..u64::MAX,
            ops_strategy(false, true)
        )
            .prop_map(|(boot, n, ts, ops)| OwnedRecord {
                txn: TxnId { boot, n },
                body: OwnedBody::Commit(ts, ops),
            }),
        (
            any::<u32>(),
            1..=u64::MAX,
            any::<u32>(),
            ops_strategy(true, true)
        )
            .prop_map(|(boot, n, seq, ops)| OwnedRecord {
                txn: TxnId { boot, n },
                body: OwnedBody::Chunk(seq, ops),
            }),
        (any::<u32>(), 1..=u64::MAX, 1..u64::MAX, 1..=u32::MAX).prop_map(
            |(boot, n, ts, nchunks)| OwnedRecord {
                txn: TxnId { boot, n },
                body: OwnedBody::CommitMarker(ts, nchunks),
            }
        ),
        any::<u32>().prop_map(|boot| OwnedRecord {
            txn: TxnId { boot, n: 0 },
            body: OwnedBody::SegmentEnd,
        }),
    ]
}

/// A random chain.
fn chain_strategy() -> impl Strategy<Value = Chain> {
    (any::<u64>(), any::<u64>(), any::<u32>()).prop_map(|(lsn, prev_lsn, prev_crc)| Chain {
        lsn,
        prev_lsn,
        prev_crc,
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Valid records of every kind with random ops, lengths and chains:
    /// encode, seal, `read_frame` and `decode_body` give the record back,
    /// the bytes are the model's, and the frame's chain fields are the
    /// chain's.
    #[test]
    fn wal_round_trip(owned in record_strategy(), chain in chain_strategy()) {
        let rec = owned.record();
        let sealed = round_trip(&rec, chain).map_err(TestCaseError::fail)?;
        let frame = read_frame(sealed.bytes()).map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
        prop_assert_eq!(
            (frame.lsn, frame.prev_lsn, frame.prev_crc),
            (chain.lsn, chain.prev_lsn, chain.prev_crc)
        );
        prop_assert_eq!(fuzz_record(sealed.bytes()), Ok(()));
    }
}

/// The frame checks (card A-6a, Work 5): a record followed by more bytes
/// reads only its `len`; a bad magic is `Magic`; a short buffer or a bad
/// `len` is `Length`; one flipped bit in any byte from 12 to `len`, the
/// chain fields included, or in `crc` itself, is `Crc`; and the checks run
/// in the card's order.
#[test]
fn wal_frame_checks() {
    // The chain fields are under the CRC that `seal` writes and `read_frame`
    // checks: in each golden record as the crate seals it, one flipped bit
    // of any byte from `crc` to `len`, `lsn`, `prev_lsn` and `prev_crc`
    // included, is `Crc`.
    for g in golden() {
        let sealed = encode(&g.record).unwrap().seal(g.chain);
        let rec = sealed.bytes();
        assert!(read_frame(rec).is_ok(), "{}: the crate's seal", g.name);
        for i in CRC_AT..rec.len() {
            let mut m = rec.to_vec();
            m[i] ^= 1 << (i % 8);
            assert_eq!(
                read_frame(&m),
                Err(FrameError::Crc),
                "{}: the crate's seal, byte {i} flipped",
                g.name
            );
        }
    }
    for g in golden() {
        let rec = &g.bytes;
        // Followed by 100 extra bytes, of two contents: the same frame.
        let mut a = rec.clone();
        a.extend_from_slice(&[0xA5; 100]);
        let mut b = rec.clone();
        b.extend_from_slice(&[0x5A; 100]);
        let fa = read_frame(&a).unwrap();
        let fb = read_frame(&b).unwrap();
        assert_eq!(fa, fb, "{}", g.name);
        assert_eq!(fa.len as usize, rec.len(), "{}", g.name);
        check_frame(&fa, rec).unwrap();
        // A flipped magic byte.
        for i in 0..4 {
            let mut m = rec.clone();
            m[i] ^= 0x20;
            assert_eq!(
                read_frame(&m),
                Err(FrameError::Magic),
                "{}: byte {i}",
                g.name
            );
        }
        // `len` 52, 0, and one past the buffer; and buffers shorter than a
        // header.
        for len in [0, 1, 52, rec.len() as u32 + 1, u32::MAX] {
            let mut m = rec.clone();
            m[LEN_AT..LEN_AT + 4].copy_from_slice(&len.to_le_bytes());
            assert_eq!(
                read_frame(&m),
                Err(FrameError::Length),
                "{}: len {len}",
                g.name
            );
        }
        for cut in 0..HEADER {
            assert_eq!(
                read_frame(&rec[..cut]),
                Err(FrameError::Length),
                "{}: {cut} bytes",
                g.name
            );
        }
        // The record cut short of its `len`, or `len` one short of the
        // record: `Length`, then `Crc`.
        assert_eq!(read_frame(&rec[..rec.len() - 1]), Err(FrameError::Length));
        let mut short = rec.clone();
        short[LEN_AT..LEN_AT + 4].copy_from_slice(&(rec.len() as u32 - 1).to_le_bytes());
        if rec.len() > HEADER {
            assert_eq!(read_frame(&short), Err(FrameError::Crc), "{}", g.name);
        }
        // One flipped bit of each byte from `crc` to `len`: the chain
        // fields are under the CRC (§12.2).
        for i in CRC_AT..rec.len() {
            let mut m = rec.clone();
            m[i] ^= 1 << (i % 8);
            assert_eq!(read_frame(&m), Err(FrameError::Crc), "{}: byte {i}", g.name);
        }
        // The order: a short buffer before the magic, the magic before
        // `len`, `len` before the CRC.
        let mut m = rec.clone();
        m[0] ^= 0x20;
        assert_eq!(read_frame(&m[..HEADER - 1]), Err(FrameError::Length));
        m[LEN_AT..LEN_AT + 4].copy_from_slice(&52u32.to_le_bytes());
        assert_eq!(read_frame(&m), Err(FrameError::Magic));
        let mut m = rec.clone();
        m[LEN_AT..LEN_AT + 4].copy_from_slice(&(rec.len() as u32 + 1).to_le_bytes());
        m[PREV_LSN_AT] ^= 1;
        assert_eq!(read_frame(&m), Err(FrameError::Length));
    }
}

/// Malformed records (card A-6a, Work 2, 3 and 5): each, assembled with a
/// correct CRC, reads as a frame and is `Corrupt` from `decode_body`; the
/// ones a `Record` can express are `Invalid` from `encode`.
#[test]
fn wal_malformed() {
    let commit = commit_hdr();
    let chunk = chunk_hdr();
    let marker = Hdr {
        kind: K_MARKER,
        ..commit
    };
    let segend = Hdr {
        kind: K_SEGEND,
        n: 0,
        commit_ts: 0,
        ..commit
    };
    // A Put and a Delete with given flags, presence byte and tail.
    let put = |flags: u8, presence: &[u8]| {
        let mut p = vec![T_PUT];
        p.extend_from_slice(&11u64.to_le_bytes());
        model_string(&mut p, b"k");
        p.push(flags);
        model_string(&mut p, b"v");
        p.extend_from_slice(presence);
        p
    };
    let delete = |flags: u8| {
        let mut p = vec![T_DELETE];
        p.extend_from_slice(&11u64.to_le_bytes());
        model_string(&mut p, b"k");
        p.push(flags);
        p.push(0);
        p
    };
    let require = {
        let mut p = vec![T_REQUIRE];
        p.extend_from_slice(&5u64.to_le_bytes());
        p
    };
    let with = |mut p: Vec<u8>, tail: &[u8]| {
        p.extend_from_slice(tail);
        p
    };
    let cases: Vec<(&str, Hdr, Vec<u8>)> = vec![
        ("kind 0", Hdr { kind: 0, ..commit }, vec![]),
        ("kind 5", Hdr { kind: 5, ..commit }, vec![]),
        (
            "kind 255",
            Hdr {
                kind: 255,
                ..commit
            },
            vec![],
        ),
        ("Commit n 0", Hdr { n: 0, ..commit }, vec![]),
        (
            "Commit commit_ts 0",
            Hdr {
                commit_ts: 0,
                ..commit
            },
            vec![],
        ),
        (
            "Commit commit_ts u64::MAX",
            Hdr {
                commit_ts: u64::MAX,
                ..commit
            },
            vec![],
        ),
        ("Chunk n 0", Hdr { n: 0, ..chunk }, vec![3, 0, 0, 0]),
        (
            "Chunk commit_ts 99",
            Hdr {
                commit_ts: 99,
                ..chunk
            },
            vec![3, 0, 0, 0],
        ),
        ("Chunk payload 0 bytes", chunk, vec![]),
        ("Chunk payload 1 byte", chunk, vec![3]),
        ("Chunk payload 2 bytes", chunk, vec![3, 0]),
        ("Chunk payload 3 bytes", chunk, vec![3, 0, 0]),
        ("CommitMarker n 0", Hdr { n: 0, ..marker }, vec![2, 0, 0, 0]),
        (
            "CommitMarker commit_ts 0",
            Hdr {
                commit_ts: 0,
                ..marker
            },
            vec![2, 0, 0, 0],
        ),
        (
            "CommitMarker commit_ts u64::MAX",
            Hdr {
                commit_ts: u64::MAX,
                ..marker
            },
            vec![2, 0, 0, 0],
        ),
        ("CommitMarker payload 3 bytes", marker, vec![2, 0, 0]),
        ("CommitMarker payload 5 bytes", marker, vec![2, 0, 0, 0, 0]),
        ("CommitMarker payload empty", marker, vec![]),
        ("CommitMarker nchunks 0", marker, vec![0, 0, 0, 0]),
        ("SegmentEnd n 1", Hdr { n: 1, ..segend }, vec![]),
        (
            "SegmentEnd commit_ts 5",
            Hdr {
                commit_ts: 5,
                ..segend
            },
            vec![],
        ),
        ("SegmentEnd with a payload", segend, vec![0]),
        ("op tag 0", commit, vec![0]),
        ("op tag 7", commit, vec![7]),
        ("op tag 255", commit, vec![255]),
        ("op tag 7 after an op", commit, with(require.clone(), &[7])),
        ("Put flags 2", commit, put(2, &[0])),
        ("Put flags 0x80", commit, put(0x80, &[0])),
        ("Delete flags 4", commit, delete(4)),
        (
            "Delete flags 0x80",
            chunk,
            with(vec![1, 0, 0, 0], &delete(0x80)),
        ),
        ("revert Delete in a Commit", commit, delete(2)),
        ("revert and moved Delete in a Commit", commit, delete(3)),
        ("presence byte 2", commit, put(0, &[2])),
        ("presence byte 255", commit, put(0, &[255, 0, 0, 0, 0])),
        ("Ddl string past the payload", commit, {
            let mut p = vec![T_DDL];
            p.extend_from_slice(&10u32.to_le_bytes());
            p.extend_from_slice(b"ddl");
            p
        }),
        ("Ddl length past the payload", commit, vec![T_DDL, 1, 0]),
        ("Put key past the payload", commit, {
            let mut p = vec![T_PUT];
            p.extend_from_slice(&11u64.to_le_bytes());
            p.extend_from_slice(&u32::MAX.to_le_bytes());
            p
        }),
        (
            "Put before past the payload",
            commit,
            put(0, &[1, 9, 0, 0, 0, 1]),
        ),
        ("CreateTree cut short", commit, vec![T_CREATE, 1, 2, 3]),
        (
            "DropTree cut short",
            commit,
            vec![T_DROP, 12, 0, 0, 0, 0, 0, 0, 0, 77],
        ),
        (
            "one byte 0 left after the last op",
            commit,
            with(require.clone(), &[0]),
        ),
        (
            "one byte 1 left after the last op",
            commit,
            with(require.clone(), &[T_CREATE]),
        ),
        (
            "one byte 6 left after the last op",
            commit,
            with(require.clone(), &[T_REQUIRE]),
        ),
        (
            "one byte left after a Chunk's last op",
            chunk,
            with(with(vec![1, 0, 0, 0], &delete(2)), &[T_DELETE]),
        ),
        ("a Ddl after a Put", commit, {
            let mut p = put(0, &[0]);
            p.extend_from_slice(&[T_DDL, 0, 0, 0, 0]);
            p
        }),
    ];
    for (name, hdr, payload) in &cases {
        let bytes = assemble(hdr, payload);
        let got = decode(&bytes).unwrap();
        assert!(is_corrupt(&got), "{name}: decode_body gave {got:?}");
    }
    // The same records, where a `Record` expresses them: `Invalid`.
    let txn = TxnId { boot: 7, n: 42 };
    let n0 = TxnId { boot: 7, n: 0 };
    let refused = [
        (
            "Commit n 0",
            Record {
                txn: n0,
                body: Body::Commit {
                    commit_ts: 99,
                    ops: vec![],
                },
            },
        ),
        (
            "Commit commit_ts 0",
            Record {
                txn,
                body: Body::Commit {
                    commit_ts: 0,
                    ops: vec![],
                },
            },
        ),
        (
            "Commit commit_ts u64::MAX",
            Record {
                txn,
                body: Body::Commit {
                    commit_ts: u64::MAX,
                    ops: vec![],
                },
            },
        ),
        (
            "Chunk n 0",
            Record {
                txn: n0,
                body: Body::Chunk {
                    seq: 3,
                    ops: vec![],
                },
            },
        ),
        (
            "CommitMarker n 0",
            Record {
                txn: n0,
                body: Body::CommitMarker {
                    commit_ts: 99,
                    nchunks: 2,
                },
            },
        ),
        (
            "CommitMarker commit_ts 0",
            Record {
                txn,
                body: Body::CommitMarker {
                    commit_ts: 0,
                    nchunks: 2,
                },
            },
        ),
        (
            "CommitMarker commit_ts u64::MAX",
            Record {
                txn,
                body: Body::CommitMarker {
                    commit_ts: u64::MAX,
                    nchunks: 2,
                },
            },
        ),
        (
            "CommitMarker nchunks 0",
            Record {
                txn,
                body: Body::CommitMarker {
                    commit_ts: 99,
                    nchunks: 0,
                },
            },
        ),
        (
            "SegmentEnd n 1",
            Record {
                txn: TxnId { boot: 7, n: 1 },
                body: Body::SegmentEnd,
            },
        ),
        (
            "revert Delete in a Commit",
            Record {
                txn,
                body: Body::Commit {
                    commit_ts: 99,
                    ops: vec![Op::Delete {
                        tree_id: 11,
                        key: b"k",
                        moved: false,
                        revert: true,
                        before: None,
                    }],
                },
            },
        ),
    ];
    for (name, rec) in &refused {
        let got = encode(rec);
        assert!(is_invalid(&got), "{name}: encode gave {got:?}");
        // The assembled form of the same record is refused by the decoder.
        let bytes = model_record(
            rec,
            Chain {
                lsn: 4096,
                prev_lsn: 4000,
                prev_crc: 1,
            },
        );
        assert!(is_corrupt(&decode(&bytes).unwrap()), "{name}: decoded");
    }
}

/// The order rule (§12.3; card A-6a, Work 3): the valid orders encode and
/// decode; a DDL-class op after a data op is refused both ways.
#[test]
fn wal_op_order() {
    let req = Op::RequireCkpt { ckpt_id: 8 };
    let ddl = Op::Ddl { bytes: b"cat" };
    let create = Op::CreateTree {
        tree_id: 30,
        kind: 4,
    };
    let drop = Op::DropTree { tree_id: 31, w: 6 };
    let put = Op::Put {
        tree_id: 30,
        key: b"a",
        key_changed: true,
        value: b"b",
        before: Some(b"c"),
    };
    let delete = Op::Delete {
        tree_id: 30,
        key: b"a",
        moved: false,
        revert: false,
        before: None,
    };
    let chain = Chain {
        lsn: 77,
        prev_lsn: 24,
        prev_crc: 3,
    };
    let body = |in_chunk: bool, ops: &[Op<'static>]| {
        if in_chunk {
            Body::Chunk {
                seq: 4,
                ops: ops.to_vec(),
            }
        } else {
            Body::Commit {
                commit_ts: 12,
                ops: ops.to_vec(),
            }
        }
    };
    let txn = TxnId { boot: 1, n: 2 };
    let good: [&[Op<'static>]; 5] = [
        &[req, ddl, create, drop, put, delete],
        &[drop, ddl, put],
        &[],
        &[req, delete, put, delete],
        &[create, create, drop, drop],
    ];
    for ops in good {
        for in_chunk in [false, true] {
            let rec = Record {
                txn,
                body: body(in_chunk, ops),
            };
            round_trip(&rec, chain).unwrap();
        }
    }
    let bad: [&[Op<'static>]; 5] = [
        &[put, create],
        &[delete, ddl],
        &[put, drop],
        &[req, put, ddl],
        &[create, put, delete, create],
    ];
    for ops in bad {
        for in_chunk in [false, true] {
            let rec = Record {
                txn,
                body: body(in_chunk, ops),
            };
            assert!(is_invalid(&encode(&rec)), "encode took {rec:?}");
            let bytes = model_record(&rec, chain);
            let got = decode(&bytes).unwrap();
            assert!(is_corrupt(&got), "decode_body took {rec:?}: {got:?}");
        }
    }
}

/// The total lengths `wal_seal_matches_direct` seals (card A-6a, Work 8:
/// 64 lengths from 53 to 70,000, among them 53, 57, 58, 59, 4096 and
/// 65536).
const SEAL_LENGTHS: [usize; 64] = [
    53, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 70, 75, 84, 85, 86, 95, 100, 127, 128, 129,
    200, 255, 256, 257, 300, 511, 512, 513, 1000, 1023, 1024, 1025, 2047, 2048, 2049, 3000, 4095,
    4096, 4097, 5000, 8191, 8192, 8193, 10000, 16383, 16384, 16385, 20000, 32767, 32768, 32769,
    40000, 50000, 60000, 65535, 65536, 65537, 66000, 68000, 69000, 69999, 70000,
];

/// The seal against a direct CRC (card A-6a, Work 4 and 8): for records of
/// 64 total lengths and drawn chains, `seal`'s `crc` is the CRC32C of bytes
/// 12..`len` computed directly, and is the field at bytes 8..12.
#[test]
fn wal_seal_matches_direct() {
    let mut rng = XorShift::new(0xA6A0_5EA1);
    let mut sealed_lengths = 0;
    for &len in &SEAL_LENGTHS {
        let ddl: Vec<u8> = (0..len.saturating_sub(58))
            .map(|_| rng.next_u64() as u8)
            .collect();
        let txn = TxnId {
            boot: rng.next_u64() as u32,
            n: 1 + rng.below(1 << 40),
        };
        let records: Vec<Record<'_>> = match len {
            53 => vec![
                Record {
                    txn: TxnId { n: 0, ..txn },
                    body: Body::SegmentEnd,
                },
                Record {
                    txn,
                    body: Body::Commit {
                        commit_ts: 5,
                        ops: vec![],
                    },
                },
            ],
            57 => vec![
                Record {
                    txn,
                    body: Body::CommitMarker {
                        commit_ts: 6,
                        nchunks: 3,
                    },
                },
                Record {
                    txn,
                    body: Body::Chunk {
                        seq: 7,
                        ops: vec![],
                    },
                },
            ],
            _ => vec![Record {
                txn,
                body: Body::Commit {
                    commit_ts: 8,
                    ops: vec![Op::Ddl { bytes: &ddl }],
                },
            }],
        };
        let chains = [
            Chain {
                lsn: rng.next_u64(),
                prev_lsn: rng.next_u64(),
                prev_crc: rng.next_u64() as u32,
            },
            Chain {
                lsn: 0,
                prev_lsn: PREV_NONE.0,
                prev_crc: PREV_NONE.1,
            },
            Chain {
                lsn: u64::MAX,
                prev_lsn: u64::MAX,
                prev_crc: u32::MAX,
            },
        ];
        for rec in &records {
            for chain in chains {
                let unsealed = encode(rec).unwrap();
                assert_eq!(unsealed.len(), len);
                let sealed = unsealed.seal(chain);
                let b = sealed.bytes();
                let direct = crc32c(0, &b[LSN_AT..len]);
                assert_eq!(sealed.crc(), direct, "len {len}, {chain:?}");
                assert_eq!(u32_at(b, CRC_AT), direct, "len {len}, {chain:?}");
                assert_eq!(u32_at(b, LEN_AT) as usize, len);
                assert_eq!(
                    (
                        u64_at(b, LSN_AT),
                        u64_at(b, PREV_LSN_AT),
                        u32_at(b, PREV_CRC_AT)
                    ),
                    (chain.lsn, chain.prev_lsn, chain.prev_crc)
                );
                assert!(read_frame(b).is_ok(), "len {len}");
            }
        }
        sealed_lengths += 1;
    }
    assert_eq!(sealed_lengths, 64);
}

/// The placement rule (card A-6a, Work 6), at each segment size: (a) a
/// room of exactly the record ends at the segment's end; (b) a room of the
/// record plus 53 places in place; (c) a room of the record plus 52, or
/// plus 1, hops; (d) a segment's first LSN places in place; (e) a record of
/// 52 bytes or past `max_record_len` is `Invalid`; (f) a room of 1–52 is
/// `Invalid`; (g) positions near `u64::MAX` are `Invalid`, never an
/// overflow.
#[test]
fn wal_place() {
    let here = |pos: u64| Placement {
        segment_end: None,
        lsn: pos,
    };
    let hop = |pos: u64, next: u64| Placement {
        segment_end: Some(pos),
        lsn: next,
    };
    for seg in SEG_SIZES {
        let max = max_record_len(seg);
        assert_eq!(max, seg / 4);
        for len in [53, 54, (53 + max) / 2, max - 1, max] {
            for k in [0u64, 1, 5, 1 << 20] {
                let start = k * seg;
                let end = start + seg;
                // (a)
                let pos = end - len;
                assert_eq!(
                    place(pos, len, seg).unwrap(),
                    here(pos),
                    "(a) seg {seg} len {len}"
                );
                assert_eq!(pos + len, end);
                // (b)
                let pos = end - (len + 53);
                assert_eq!(
                    place(pos, len, seg).unwrap(),
                    here(pos),
                    "(b) seg {seg} len {len}"
                );
                // (c)
                for room in [len + 52, len + 1] {
                    let pos = end - room;
                    assert_eq!(
                        place(pos, len, seg).unwrap(),
                        hop(pos, end),
                        "(c) seg {seg} len {len} room {room}"
                    );
                }
                // Rooms shorter than the record hop too.
                for room in [53, len - 1, len.saturating_sub(10).max(53)] {
                    if (53..len).contains(&room) {
                        let pos = end - room;
                        assert_eq!(place(pos, len, seg).unwrap(), hop(pos, end));
                    }
                }
                // (d)
                assert_eq!(
                    place(start, len, seg).unwrap(),
                    here(start),
                    "(d) seg {seg}"
                );
                // (f)
                for room in 1..53 {
                    let got = place(end - room, len, seg);
                    assert!(is_invalid(&got), "(f) seg {seg} room {room}: {got:?}");
                }
            }
        }
        // (e)
        for len in [0, 1, 52, max + 1, seg, u64::MAX] {
            let got = place(0, len, seg);
            assert!(is_invalid(&got), "(e) seg {seg} len {len}: {got:?}");
        }
        // (g) The last segment below 2^64 is never used, and a hop into it
        // is refused; nothing overflows.
        let last = u64::MAX - u64::MAX % seg;
        for pos in [u64::MAX, u64::MAX - 52, u64::MAX - 53, last, last + 1] {
            for len in [53, max] {
                let got = place(pos, len, seg);
                assert!(
                    is_invalid(&got),
                    "(g) seg {seg} pos {pos} len {len}: {got:?}"
                );
            }
        }
        let got = place(last - (max + 1), max, seg);
        assert!(
            is_invalid(&got),
            "(g) seg {seg}: a hop into the last segment: {got:?}"
        );
        assert_eq!(place(last - seg, max, seg).unwrap(), here(last - seg));
    }
    // A segment smaller than 212 bytes holds no record; 212 holds one of
    // 53 bytes; a size of 0 divides nothing.
    for seg in [0, 1, 52, 53, 211] {
        for len in [0, 52, 53, 54] {
            assert!(is_invalid(&place(0, len, seg)), "seg {seg} len {len}");
        }
    }
    assert_eq!(max_record_len(212), 53);
    assert_eq!(place(0, 53, 212).unwrap(), here(0));
    assert_eq!(place(159, 53, 212).unwrap(), here(159));
    assert_eq!(place(106, 53, 212).unwrap(), here(106));
    assert_eq!(place(107, 53, 212).unwrap(), hop(107, 212));
    assert!(is_invalid(&place(160, 53, 212)));
}

// ---------------------------------------------------------------------
// The fuzz targets (`fn(&[u8])`; §6.2's "the WAL decoder")
// ---------------------------------------------------------------------

/// The frame checks as the card states them (Work 5), for the fuzz target:
/// `(len, crc)` of a frame that reads.
fn model_frame(data: &[u8]) -> Result<(u32, u32), FrameError> {
    if data.len() < HEADER {
        return Err(FrameError::Length);
    }
    if data[..4] != *b"NWAL" {
        return Err(FrameError::Magic);
    }
    let len = u32_at(data, LEN_AT);
    let crc = u32_at(data, CRC_AT);
    if !(HEADER..=data.len()).contains(&(len as usize)) {
        return Err(FrameError::Length);
    }
    if crc32c(0, &data[LSN_AT..len as usize]) != crc {
        return Err(FrameError::Crc);
    }
    Ok((len, crc))
}

/// The record fuzz target, returning what it decoded: `read_frame` agrees
/// with the frame model; a frame that reads, and decodes, re-encodes,
/// sealed with its own chain fields, to exactly its bytes, by the crate's
/// encoder and by the model (only canonical records decode). Never panics.
fn fuzz_record_decoded(data: &[u8]) -> Result<Option<Record<'_>>, String> {
    let frame = match (read_frame(data), model_frame(data)) {
        (Err(got), Err(want)) if got == want => return Ok(None),
        (Ok(frame), Ok(_)) => frame,
        (got, want) => {
            return Err(format!(
                "read_frame gave {got:?}, the model {want:?}, for {}",
                hex(data)
            ));
        }
    };
    check_frame(&frame, data)?;
    let record = match decode_body(&frame) {
        Ok(record) => record,
        Err(StoreError::Corrupt(_)) => return Ok(None),
        Err(other) => return Err(format!("decode_body failed with {other:?}, not Corrupt")),
    };
    let chain = Chain {
        lsn: frame.lsn,
        prev_lsn: frame.prev_lsn,
        prev_crc: frame.prev_crc,
    };
    let len = frame.len as usize;
    let sealed = encode(&record)
        .map_err(|e| format!("decoded {record:?}, which does not encode: {e}"))?
        .seal(chain);
    if Some(sealed.bytes()) != data.get(..len) || sealed.crc() != frame.crc {
        return Err(format!(
            "decoded {record:?} from {}, which encodes to {}",
            hex(&data[..len]),
            hex(sealed.bytes())
        ));
    }
    if model_record(&record, chain) != data[..len] {
        return Err(format!(
            "decoded {record:?} from {}, which the model does not encode so",
            hex(&data[..len])
        ));
    }
    Ok(Some(record))
}

/// The record fuzz target (card A-6a, Work 8): `read_frame`; on `Ok`,
/// `decode_body`; on `Ok`, the record re-encodes to the input's first
/// `len` bytes. Never panics.
fn fuzz_record(data: &[u8]) -> Result<(), String> {
    fuzz_record_decoded(data).map(|_| ())
}

/// The placement rule in `u128` arithmetic, from the card's Work 6, with
/// this crate's bound: a record goes only in a segment whose end is a
/// `u64`. `None` for `Invalid`.
fn model_place(pos: u64, len: u64, seg: u64) -> Option<Placement> {
    if seg == 0 || len < 53 || len > seg / 4 {
        return None;
    }
    let (pos, len, seg) = (u128::from(pos), u128::from(len), u128::from(seg));
    let top = u128::from(u64::MAX);
    let start = pos - pos % seg;
    let end = start + seg;
    let room = end - pos;
    if end > top || room < 53 {
        return None;
    }
    if len == room || len + 53 <= room {
        return Some(Placement {
            segment_end: None,
            lsn: pos as u64,
        });
    }
    if end + seg > top {
        return None;
    }
    Some(Placement {
        segment_end: Some(pos as u64),
        lsn: end as u64,
    })
}

/// The append position from its eight bytes, reduced into a useful range
/// by its low two bits: anywhere; uniform in one of the first 1024
/// segments; at a room within −32..128 bytes of the record's length (the
/// region where in-place and hop decide); or in the top three segments
/// below 2^64.
fn reduce_append_pos(a: u64, record_len: u64, seg: u64) -> u64 {
    let x = a >> 2;
    let k = (x >> 40) % 1024;
    match a & 3 {
        0 => a,
        1 => k * seg + x % seg,
        2 => {
            let room = (record_len + x % 160).saturating_sub(32).clamp(1, seg);
            k * seg + (seg - room)
        }
        _ => u64::MAX - x % (3 * seg),
    }
}

/// The placement fuzz target (card A-6a, Work 8): `append_pos`,
/// `record_len` and `seg_size` from 24 bytes (`seg_size` among the four
/// sizes, `record_len` in 1 to `seg_size / 2`); `place` agrees with the
/// model, and on `Ok` the record lies in one segment, the room after it is
/// 0 or at least 53, and a `SegmentEnd` fits where one is placed. Never
/// panics.
fn fuzz_place(data: &[u8]) -> Result<(), String> {
    let word = |i: usize| {
        let mut le = [0u8; 8];
        for (d, s) in le.iter_mut().zip(data.iter().skip(8 * i)) {
            *d = *s;
        }
        u64::from_le_bytes(le)
    };
    let seg = SEG_SIZES[(word(2) % 4) as usize];
    let len = 1 + word(1) % (seg / 2);
    let pos = reduce_append_pos(word(0), len, seg);
    let got = place(pos, len, seg);
    let p = match (&got, model_place(pos, len, seg)) {
        (Err(StoreError::Invalid(_)), None) => return Ok(()),
        (Ok(p), Some(want)) if *p == want => *p,
        (_, want) => {
            return Err(format!(
                "place({pos}, {len}, {seg}) = {got:?}, the model gives {want:?}"
            ));
        }
    };
    let fail = |what: &str| Err(format!("place({pos}, {len}, {seg}) = {p:?}: {what}"));
    let offset = p.lsn % seg;
    let Some(used) = offset.checked_add(len).filter(|u| *u <= seg) else {
        return fail("the record crosses its segment's end");
    };
    let tail = seg - used;
    if tail != 0 && tail < 53 {
        return fail("the room after the record is 1–52 bytes");
    }
    match p.segment_end {
        None if p.lsn != pos => fail("placed in place away from the append position"),
        None => Ok(()),
        Some(at) => {
            let fits = at % seg + 53 <= seg;
            let next = (at - at % seg).checked_add(seg);
            if at != pos || !fits || offset != 0 || next != Some(p.lsn) {
                fail(
                    "the SegmentEnd does not fit, or the record is not at the next segment's start",
                )
            } else {
                Ok(())
            }
        }
    }
}

/// Drives `fuzz_record` over `n` Commit and Chunk records from a xorshift
/// at `seed` (each op kind equally likely, then put in a valid order), and
/// counts, per record kind ([0] Commit, [1] Chunk), the ops of each kind
/// ([tag − 1]) that it decoded (card A-6a, Work 8). A record the target
/// refuses fails the call.
fn fuzz_reach_counts(seed: u64, n: usize) -> [[usize; 6]; 2] {
    let mut rng = XorShift::new(seed);
    let mut counts = [[0usize; 6]; 2];
    let mut failures = Vec::new();
    for _ in 0..n {
        let (owned, chain) = random_commit_or_chunk(&mut rng);
        let rec = owned.record();
        let row = usize::from(matches!(rec.body, Body::Chunk { .. }));
        let sealed = match encode(&rec) {
            Ok(unsealed) => unsealed.seal(chain),
            Err(e) => {
                failures.push(format!("encode refused {rec:?}: {e}"));
                continue;
            }
        };
        match fuzz_record_decoded(sealed.bytes()) {
            Ok(Some(decoded)) => {
                for op in ops_of(&decoded) {
                    counts[row][op_index(op)] += 1;
                }
            }
            Ok(None) => failures.push(format!("{rec:?} did not decode")),
            Err(e) => failures.push(e),
        }
    }
    assert!(
        failures.is_empty(),
        "fuzz_record refused {} generated records, the first: {:?}",
        failures.len(),
        failures.first()
    );
    counts
}

/// A mutation of a corpus record: 1–4 byte overwrites, a truncation or an
/// extension (card A-6a, Work 8).
#[derive(Clone, Debug)]
enum Mutation {
    /// Bytes written at positions within the record.
    Overwrite(Vec<(Index, u8)>),
    /// The record cut to a length below its own.
    Truncate(Index),
    /// Bytes appended.
    Extend(Vec<u8>),
}

/// The mutations, overwrites weighted up.
fn mutation() -> impl Strategy<Value = Mutation> {
    prop_oneof![
        4 => proptest::collection::vec((any::<Index>(), any::<u8>()), 1..=4).prop_map(Mutation::Overwrite),
        1 => any::<Index>().prop_map(Mutation::Truncate),
        1 => proptest::collection::vec(any::<u8>(), 1..=64).prop_map(Mutation::Extend),
    ]
}

/// `rec` mutated, then, when `reseal_it`, re-sealed with a correct CRC so
/// that `decode_body` is reached.
fn mutate(rec: &[u8], m: &Mutation, reseal_it: bool) -> Vec<u8> {
    let mut out = rec.to_vec();
    match m {
        Mutation::Overwrite(writes) => {
            for (at, byte) in writes {
                let i = at.index(out.len());
                out[i] = *byte;
            }
        }
        Mutation::Truncate(at) => out.truncate(at.index(out.len())),
        Mutation::Extend(tail) => out.extend_from_slice(tail),
    }
    if reseal_it {
        reseal(&mut out);
    }
    out
}

/// The mutated driver's corpus: the five golden records and 59 drawn
/// Commits and Chunks from a fixed seed.
fn corpus() -> Vec<Vec<u8>> {
    let mut rng = XorShift::new(0x00C0_8B05);
    let mut out: Vec<Vec<u8>> = golden().into_iter().map(|g| g.bytes).collect();
    while out.len() < 64 {
        let (owned, chain) = random_commit_or_chunk(&mut rng);
        if let Ok(unsealed) = encode(&owned.record()) {
            out.push(unsealed.seal(chain).into_bytes());
        }
    }
    out
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Arbitrary bytes into the record target.
    #[test]
    fn wal_fuzz_bytes(data in proptest::collection::vec(any::<u8>(), 0..=400)) {
        fuzz_record(&data).map_err(TestCaseError::fail)?;
    }

    /// Corpus records, mutated and in half the cases re-sealed, into the
    /// record target.
    #[test]
    fn wal_fuzz_mutated(pick in any::<Index>(), m in mutation(), reseal_it in any::<bool>()) {
        let corpus = corpus();
        let data = mutate(&corpus[pick.index(corpus.len())], &m, reseal_it);
        fuzz_record(&data).map_err(TestCaseError::fail)?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]

    /// Random 24-byte inputs into the placement target.
    #[test]
    fn wal_fuzz_place(data in proptest::collection::vec(any::<u8>(), 24)) {
        fuzz_place(&data).map_err(TestCaseError::fail)?;
    }
}

/// `RecordKind`'s bytes are the table's (§12.2), and no other byte is a
/// kind.
#[test]
fn wal_kind_bytes() {
    let kinds = [
        (RecordKind::Commit, K_COMMIT),
        (RecordKind::Chunk, K_CHUNK),
        (RecordKind::CommitMarker, K_MARKER),
        (RecordKind::SegmentEnd, K_SEGEND),
    ];
    for (kind, byte) in kinds {
        assert_eq!(kind.byte(), byte);
        assert_eq!(RecordKind::from_byte(byte), Some(kind));
    }
    for b in (0..=255u8).filter(|b| !(1..=4).contains(b)) {
        assert_eq!(RecordKind::from_byte(b), None, "byte {b}");
    }
}
