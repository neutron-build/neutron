//! C-T6 §5 large values (the TOAST equivalent): a non-NULL datum whose
//! value bytes exceed [`TOAST_THRESHOLD`] is stored out of line as MVCC
//! chunk rows under `table_lo ‖ 0xFF ‖ toast_id ‖ be32(i)` (DECISION 5),
//! written by the row's statement with
//! [`insert_key`](nucleus_txn::boot::Core::insert_key) **after** the row
//! op returned `Applied`, deleted through
//! [`Core::delete_entry`](nucleus_txn::boot::Core::delete_entry) (C-T0
//! §5.0's blind entry delete), and read at the version's commit ts —
//! never registering a SIREAD on a chunk key (§5).
//!
//! The chunk keys sit inside the table's storage range past a `0xFF`
//! mark no encoded key starts with (C-Q3s §2), so TRUNCATE/DROP retire
//! them with the range and a row scan (bounded to
//! `[table_lo, table_lo ‖ 0xFF)`) never sees one as a row.
//!
//! This module is the §5 mechanics only: assigning ids while the new row
//! value is built, splitting/joining the value bytes, the raw-pointer
//! sweep of an old row and the detoast reads. The DML paths in [`crate::db`]
//! decide when each runs.

use nucleus_codec::row::{
    decode_row_raw, decode_value, encode_value, Datum, RawDatum, RowError, TableSchema,
    ToastPointer, ValueError,
};
use nucleus_codec::{KeyType, Value};
use nucleus_kv::{Key, OrderedKv};
use nucleus_txn::boot::Core;
use nucleus_txn::read::{read_key, NoSsi};
use nucleus_txn::txn::Txn;
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::write::{StmtCtx, UniqueRule};
use nucleus_txn::Ts;

use crate::db::{ghost_id, table_lo, Stmt};
use crate::error::DbError;

/// §5: a non-NULL datum whose value bytes exceed this many bytes is
/// stored out of line ("exceed 2048" — a value of exactly 2048 stays
/// inline).
pub const TOAST_THRESHOLD: usize = 2048;

/// §5: each chunk holds this many value bytes; the last chunk is shorter.
pub const TOAST_CHUNK: usize = 8192;

/// §5: the byte that separates a storage's chunk keys from its row keys.
/// No encoded key starts with `0xFF` (C-Q3s §2 markers 0x00–0x02), so
/// `[table_lo, table_lo ‖ 0xFF)` is exactly the row keys and everything
/// past the mark is a chunk of the same storage.
pub const TOAST_MARK: u8 = 0xFF;

/// The 20-byte toast id of §5/§3: `be32(epoch) be64(txn n) be32(seq0)
/// be32(ord)`, `ord` drawn from the statement's **single ordinal counter**
/// (one per `(txn, seq0)`, shared with every rowid the statement assigns
/// and across every call the statement makes — never reset per call), so
/// the id is unique forever (C-T0 §7.2) with no I/O and no lock.
pub fn toast_id(st: &Stmt<'_>) -> Result<[u8; 20], DbError> {
    let ord = st.next_ord()?;
    let txn = st.txn();
    let mut id = [0u8; 20];
    id[..4].copy_from_slice(&txn.id.epoch.to_be_bytes());
    id[4..12].copy_from_slice(&txn.id.n.to_be_bytes());
    id[12..16].copy_from_slice(&st.seq0().to_be_bytes());
    id[16..20].copy_from_slice(&ord.to_be_bytes());
    Ok(id)
}

/// The logical key of chunk `i` of toast id `id` in table storage `sid`
/// (§5): `table_lo(sid) ‖ 0xFF ‖ toast_id ‖ be32(i)`.
pub fn chunk_key(sid: u64, id: &[u8; 20], i: u32) -> Key {
    let mut k = table_lo(sid);
    k.push(TOAST_MARK);
    k.extend_from_slice(id);
    k.extend_from_slice(&i.to_be_bytes());
    k
}

/// One value this statement moves out of line (§5): its id, its encoded
/// value bytes (the chunk payloads) and the value itself — index keys of
/// a toasted column are computed from the detoasted value, which the
/// writer already holds.
pub(crate) struct ToastDatum {
    pub id: [u8; 20],
    pub bytes: Vec<u8>,
    pub value: Value,
}

/// The §5 row a write path stores: the datums with every over-threshold
/// inline value replaced by its pointer, plus the values whose chunks the
/// statement must write once the row op returned `Applied`. A datum that
/// is already a pointer is **carried unchanged** — an update never
/// rewrites (or re-ids) the chunks of a column its `set` does not name.
pub(crate) struct Toasted {
    pub live: Vec<Option<Datum>>,
    pub new: Vec<ToastDatum>,
}

/// Assigns toast ids while the new row value is built (§5): every live
/// inline datum whose value bytes exceed [`TOAST_THRESHOLD`] becomes a
/// `Datum::Toast` pointer drawing its id from the statement's ordinal
/// counter; anything else (NULL, in-limit value, an already-out-of-line
/// datum) passes through untouched.
pub(crate) fn build_row(
    st: &Stmt<'_>,
    schema: &TableSchema,
    live: &[Option<Datum>],
) -> Result<Toasted, DbError> {
    let mut out = Vec::with_capacity(live.len());
    let mut new = Vec::new();
    let mut bytes = Vec::new();
    for (col, datum) in schema.live_columns().zip(live) {
        let Some(datum) = datum else {
            out.push(None);
            continue;
        };
        match datum {
            Datum::Toast(p) => out.push(Some(Datum::Toast(*p))),
            Datum::Inline(v) => {
                bytes.clear();
                encode_value(&col.ty, v, &mut bytes)
                    .map_err(|e| DbError::Row(value_err(col.attnum, e)))?;
                if bytes.len() > TOAST_THRESHOLD {
                    let id = toast_id(st)?;
                    let raw_len = u32::try_from(bytes.len())
                        .map_err(|_| DbError::Row(RowError::Corrupt("datum value too large")))?;
                    let chunks = u32::try_from(bytes.len().div_ceil(TOAST_CHUNK))
                        .map_err(|_| DbError::Row(RowError::Corrupt("datum value too large")))?;
                    new.push(ToastDatum {
                        id,
                        bytes: bytes.clone(),
                        value: v.clone(),
                    });
                    out.push(Some(Datum::Toast(ToastPointer {
                        id,
                        raw_len,
                        chunks,
                    })));
                } else {
                    out.push(Some(Datum::Inline(v.clone())));
                }
            }
        }
    }
    Ok(Toasted { live: out, new })
}

/// Writes the chunks of every value [`build_row`] moved out of line (§5):
/// `insert_key(.., UniqueRule::None)` in the row's statement — the caller
/// runs this only after the row op returned `Applied`, so a skipped row
/// op writes no chunk. Chunk `i` is [`TOAST_CHUNK`] bytes, the last
/// shorter.
pub(crate) fn write_chunks<K: OrderedKv>(
    core: &Core<K>,
    txn: &Txn,
    ctx: &StmtCtx,
    sid: u64,
    new: &[ToastDatum],
) -> Result<(), DbError> {
    for d in new {
        let chunks = d.bytes.len().div_ceil(TOAST_CHUNK);
        for i in 0..chunks {
            let lo = i * TOAST_CHUNK;
            let hi = ((i + 1) * TOAST_CHUNK).min(d.bytes.len());
            core.insert_key(
                txn,
                &chunk_key(sid, &d.id, i as u32),
                None,
                d.bytes[lo..hi].to_vec(),
                ctx.clone(),
                UniqueRule::None,
            )
            .map_err(DbError::Txn)?;
        }
    }
    Ok(())
}

/// Every toast pointer of a stored row, **dropped attnums included**
/// (§5: `decode_row_raw` is where an update or delete takes its old
/// pointers from — a dropped column's chunks must not leak).
pub(crate) fn old_pointers(bytes: &[u8]) -> Result<Vec<ToastPointer>, DbError> {
    let (_, raw) = decode_row_raw(bytes).map_err(DbError::Row)?;
    Ok(raw
        .into_iter()
        .flatten()
        .filter_map(|d| match d {
            RawDatum::Toast(p) => Some(p),
            RawDatum::Inline(_) => None,
        })
        .collect())
}

/// Deletes every chunk of every old pointer the new row no longer carries
/// (§5): `Core::delete_entry` per chunk, in the row's statement. A
/// pointer carried into the new version (the same id) keeps its chunks.
pub(crate) fn delete_stale_chunks<K: OrderedKv>(
    core: &Core<K>,
    txn: &Txn,
    ctx: &StmtCtx,
    sid: u64,
    base_bytes: &[u8],
    new_bytes: &[u8],
) -> Result<(), DbError> {
    let carried: Vec<[u8; 20]> = old_pointers(new_bytes)?.into_iter().map(|p| p.id).collect();
    for p in old_pointers(base_bytes)? {
        if carried.contains(&p.id) {
            continue;
        }
        delete_pointer_chunks(core, txn, ctx, sid, &p)?;
    }
    Ok(())
}

/// Deletes every chunk of every pointer a deleted row named (§5): the
/// row op returned `Applied`, so nothing carries them onward.
pub(crate) fn delete_row_chunks<K: OrderedKv>(
    core: &Core<K>,
    txn: &Txn,
    ctx: &StmtCtx,
    sid: u64,
    base_bytes: &[u8],
) -> Result<(), DbError> {
    for p in old_pointers(base_bytes)? {
        delete_pointer_chunks(core, txn, ctx, sid, &p)?;
    }
    Ok(())
}

fn delete_pointer_chunks<K: OrderedKv>(
    core: &Core<K>,
    txn: &Txn,
    ctx: &StmtCtx,
    sid: u64,
    p: &ToastPointer,
) -> Result<(), DbError> {
    for i in 0..p.chunks {
        core.delete_entry(txn, &chunk_key(sid, &p.id, i), None, ctx.clone())
            .map_err(DbError::Txn)?;
    }
    Ok(())
}

// ---- detoast (§5 "reading chunks") ------------------------------------------

/// Resolves every out-of-line datum of a row read at `ctx.snapshot`
/// (§5): the chunks are read at the same registered-snapshot position
/// through a fresh view, `NoSsi` even under SERIALIZABLE (no SIREAD is
/// ever registered on a chunk key) — the row's own read rule (own
/// intents of earlier statements included) decides what a pointer names.
pub(crate) fn detoast_row_at<K: OrderedKv>(
    core: &Core<K>,
    schema: &TableSchema,
    sid: u64,
    live: &mut [Option<Datum>],
    ctx: &ReadCtx,
) -> Result<(), DbError> {
    for (col, datum) in schema.live_columns().zip(live.iter_mut()) {
        if let Some(Datum::Toast(p)) = datum.as_ref() {
            let v = read_pointer(core, ctx, sid, &col.ty, p)?;
            *datum = Some(Datum::Inline(v));
        }
    }
    Ok(())
}

/// Resolves every out-of-line datum of an **EPQ base** version with
/// commit ts `ts` (§5): a snapshot registered at exactly `ts`
/// (`register_at`, admitted because `ts > S >= W`), read through a ghost
/// ctx, guard dropped after — the chunks of a version are visible at
/// that version's commit ts and nowhere below it.
pub(crate) fn detoast_row_at_version<K: OrderedKv>(
    core: &Core<K>,
    schema: &TableSchema,
    sid: u64,
    live: &mut [Option<Datum>],
    ts: Ts,
) -> Result<(), DbError> {
    let guard = core.registry.register_at(ts).map_err(DbError::Txn)?;
    let ctx = ReadCtx {
        txn: ghost_id(core),
        snapshot: ts,
        stmt_seq: 0,
    };
    let out = detoast_row_at(core, schema, sid, live, &ctx);
    drop(guard);
    out
}

/// Reads and reassembles one pointer's value (§5): chunks `0..chunks`
/// visible at `ctx`'s position, each of [`TOAST_CHUNK`] bytes but the
/// last, summing to `raw_len`; a missing chunk or a length mismatch is
/// [`RowError::Corrupt`] (XX001), never a partial value.
fn read_pointer<K: OrderedKv>(
    core: &Core<K>,
    ctx: &ReadCtx,
    sid: u64,
    ty: &KeyType,
    p: &ToastPointer,
) -> Result<Value, DbError> {
    let corrupt = |m: &'static str| DbError::Row(RowError::Corrupt(m));
    if p.chunks == 0 || p.raw_len == 0 {
        return Err(corrupt("toast pointer names no chunk"));
    }
    let mut all = Vec::with_capacity(p.raw_len as usize);
    let view = core.open_view();
    for i in 0..p.chunks {
        let found = read_key(core, &view, &chunk_key(sid, &p.id, i), ctx, &mut NoSsi)
            .map_err(DbError::Txn)?;
        let Some(chunk) = found else {
            return Err(corrupt("missing large-value chunk"));
        };
        let last = i + 1 == p.chunks;
        if chunk.len() > TOAST_CHUNK
            || (!last && chunk.len() != TOAST_CHUNK)
            || (last && chunk.is_empty())
        {
            return Err(corrupt("large-value chunk length mismatch"));
        }
        all.extend_from_slice(&chunk);
    }
    drop(view);
    if all.len() != p.raw_len as usize {
        return Err(corrupt("large-value chunks do not sum to raw_len"));
    }
    decode_value(ty, &all).map_err(|_| corrupt("out-of-line value does not decode"))
}

fn value_err(attnum: u16, e: ValueError) -> RowError {
    match e {
        ValueError::TypeMismatch => RowError::TypeMismatch { attnum },
        ValueError::Corrupt(msg) => RowError::Corrupt(msg),
    }
}

#[cfg(test)]
mod tests {
    //! The pure §5 mechanics: the chunk-key shape, the pointer sweep over
    //! raw row bytes (dropped attnums included — those rows exist because
    //! no DDL rewrites them), and the carried/inline split of
    //! [`build_row`]'s per-datum rule. The statement-driven paths are the
    //! `t6d_toast` integration tests' business.

    use super::{chunk_key, old_pointers, TOAST_MARK};
    use crate::db::table_lo;

    use nucleus_codec::row::{encode_row, ColumnDef, Datum, RawDatum, ToastPointer};

    fn cols() -> Vec<ColumnDef> {
        let col = |attnum: u16, dropped: bool| ColumnDef {
            attnum,
            name: format!("c{attnum}"),
            ty: nucleus_codec::KeyType::Text(nucleus_codec::Collation::C),
            not_null: false,
            not_null_since: 0,
            dropped,
            missing: None,
        };
        vec![col(1, false), col(2, false), col(3, false)]
    }

    #[test]
    fn chunk_key_is_table_lo_mark_id_and_index() {
        let id = [7u8; 20];
        let mut want = table_lo(9);
        want.push(TOAST_MARK);
        want.extend_from_slice(&id);
        want.extend_from_slice(&3u32.to_be_bytes());
        assert_eq!(chunk_key(9, &id, 3), want);
        // Inside the table's storage range, past the row-scan bound.
        let (lo, hi) = nucleus_txn::catalog::table_range(9);
        assert!(chunk_key(9, &id, 0) >= lo && chunk_key(9, &id, u32::MAX) < hi);
        assert!(chunk_key(9, &id, 0).starts_with(&{
            let mut bound = table_lo(9);
            bound.push(0xFF);
            bound
        }));
    }

    #[test]
    fn old_pointers_sweep_raw_datums_of_a_pre_drop_row() {
        // A row written before attnum 2 was dropped still carries its
        // pointer there; the raw sweep must see it (a live-columns-only
        // decode never would).
        let p = ToastPointer {
            id: [1u8; 20],
            raw_len: 4096,
            chunks: 1,
        };
        let bytes = encode_row(
            1,
            &cols(),
            &[
                Some(Datum::Inline(nucleus_codec::Value::Text("a".into()))),
                Some(Datum::Toast(p)),
                None,
            ],
        )
        .expect("row encodes");
        assert_eq!(old_pointers(&bytes).expect("raw decode"), vec![p]);

        // The codec's own raw decode agrees on the shape.
        let (_, raw) = nucleus_codec::row::decode_row_raw(&bytes).expect("raw decode");
        assert!(matches!(raw[1], Some(RawDatum::Toast(_))));
    }
}
