//! amcheck-lite (C-T6 §8, cards C-T6b3b and C-T6d2): the consistency
//! checker a harness runs against one table at a **registered
//! snapshot** — I-ROWFMT, I-IDX (with I-UNIQUE for unique indexes),
//! I-IDX-WO, I-IDX-DO and I-TOAST (§5, both directions).
//!
//! [`check_table`] reads the catalog at `at.ts()` (never the latest
//! catalog: a def change committed after `at` must not change a verdict
//! taken at `at`), and every data read — rows and index entries — goes
//! through the §4 read path over registered views (§3.1), so intents and
//! versions are settled by the same visibility rule every reader uses.
//! Problems are **violations** in a [`Report`] (the check is a checker,
//! not an error); [`Err`] is only for what stops the check — a relation
//! the catalog at `at` has no row for (42P01), a def that will not
//! decode, a KV error.
//!
//! What is checked, per §8:
//!
//! - **I-ROWFMT**: each row visible at `at` decodes under the def at
//!   `at` — which includes `schema_version <= def.version` (the §3
//!   decode rule) and §2/§3's era-scoped NOT NULL clause: the decode
//!   runs through [`TableSchema::decode_row`], the same single code
//!   path the DML/EPQ reads use (`decode_row_at_era` in `db.rs`), so a
//!   pre-constraint NULL (a row whose header version is below the
//!   installing DDL's `not_null_since`) is as legal here as it is on
//!   the read path, and a NULL at or after it is a violation here too —
//!   and its key re-encodes: the pk columns of a pk table
//!   (§3), or the rowid of a hidden-rowid table (in the key only, never
//!   a column, re-encoded from the key itself).
//! - **I-IDX**: each `Public` index with `built_at <= at`: the visible
//!   entries equal the entries computed from the visible rows, both
//!   directions, payload compared. The version header's `key_changed`
//!   bit is **not significant** (§7.3: a delete + re-insert of a `/u/`
//!   entry in one txn resolves to header `0x01` where a fresh insert
//!   writes `0x00`); the read path yields the payload past the header,
//!   so the comparison ignores it by construction. Unique ones also get
//!   **I-UNIQUE**: no two visible rows compute the same `/u/` entry
//!   (C-T0 §5.3; a NULLS-DISTINCT NULL key goes to `/i/` and is exempt).
//! - **I-IDX-WO**: each `WriteOnly` index: every visible entry is
//!   computed from a visible row. Missing entries are legal — that is
//!   the state's meaning mid-build (§7); only unexplained entries are
//!   violations.
//! - **I-IDX-DO**: each `DeleteOnly` index whose idx row is visible at
//!   `at` (every one the catalog scan at `at` returns) has **no** visible
//!   entry: a DeleteOnly index never inserts.
//! - **I-TOAST** (§5, C-T6d2): every pointer of every visible row — read
//!   with [`decode_row_raw`], so pointers in **dropped** attnums count
//!   (no DDL rewrites rows, §3; a dropped column's chunks must not leak)
//!   — names chunks `0..chunk_count` visible at `at` with lengths
//!   summing to `raw_len`, and every visible chunk of the storage is
//!   named by such a pointer. A missing or short chunk is a dangling
//!   pointer; an unnamed chunk is an orphan; a pointer counting more
//!   chunks than the storage has visible is reported dangling without
//!   being walked (a planted huge count cannot spin the check).

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use nucleus_codec::row::{decode_row_raw, Datum, RawDatum, TableSchema, ToastPointer};
use nucleus_codec::{decode_key_prefix, encode_key, KeyColumn, KeyType, Value};
use nucleus_kv::{Key, OrderedKv};
use nucleus_txn::boot::Core;
use nucleus_txn::catalog::{index_ranges, table_range, Catalog, IdxStorage};
use nucleus_txn::locks::RelLockMode;
use nucleus_txn::read::{scan, NoSsi};
use nucleus_txn::registry::SnapshotGuard;
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::{Ts, TxnError};

use crate::db::{chunk_prefix, ghost_id, table_lo, Db, IndexHandle, TableHandle};
use crate::defs::{IndexDef, IndexState};
use crate::error::DbError;
use crate::index;
use crate::toast::{chunk_key, TOAST_CHUNK, TOAST_MARK};

/// The amcheck report: every invariant violation found, as strings each
/// prefixed with the class of the finding — the invariant's name
/// (`"I-ROWFMT: …"`, `"I-IDX: …"`, …) for a violation, `"UNCHECKED: …"`
/// for a surface the checker cannot check yet (a `IdxStorage::Range`
/// index, C-T6b2) — so a harness can assert on the class of a finding.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Report {
    pub violations: Vec<String>,
}

impl Report {
    /// `true` iff no invariant was violated at the snapshot.
    pub fn is_clean(&self) -> bool {
        self.violations.is_empty()
    }

    fn push(&mut self, msg: String) {
        self.violations.push(msg);
    }
}

/// Checks relation `oid` at the registered snapshot `at` (§8).
///
/// The guard must come from the same core as `db` (it pins `at.ts()` for
/// the catalog read and stays registered for the whole check, so GC
/// cannot retire a version the check is about to read, §3.1/§9.1).
///
/// - `Err(DbError::UndefinedTable)` — the catalog at `at` has no row for
///   `oid` (a relation that did not exist at `at`, §4).
/// - `Err(DbError::Row(_))` — the relation's def at `at` does not decode;
///   nothing can be checked below it.
/// - `Ok(report)` — the check ran; `report.is_clean()` is the verdict.
pub fn check_table<K: OrderedKv>(
    db: &Db<K>,
    oid: u64,
    at: &SnapshotGuard<'_>,
) -> Result<Report, DbError> {
    let mut report = Report::default();
    // §8: the catalog at `at.ts()`. The ghost reader owns nothing and is
    // never allocated, so its reads see exactly the committed state.
    let ctx = ReadCtx {
        txn: ghost_id(&db.core),
        snapshot: at.ts(),
        stmt_seq: 0,
    };
    let rel = {
        let view = db.core.open_view();
        Catalog::read_rel(&db.core, &view, &ctx, oid).map_err(DbError::Txn)?
    };
    let Some(rel) = rel else {
        return Err(DbError::UndefinedTable);
    };
    let schema = TableSchema::decode(&rel.def)?;
    let idxs = {
        let view = db.core.open_view();
        Catalog::scan_idxs_of_rel(&db.core, &view, &ctx, oid).map_err(DbError::Txn)?
    };

    // ---- I-ROWFMT ---------------------------------------------------------

    // §8 through §4's decode: `TableSchema::decode_row` is the one rule
    // (the NOT NULL era clause included — a shared helper, never a
    // second copy), so amcheck's verdict on a row can never drift from
    // the read path's.
    let rows = visible_rows(&db.core, rel.storage_id, &ctx)?;
    let mut decoded: Vec<(Key, Vec<Option<Datum>>)> = Vec::with_capacity(rows.len());
    for (key, bytes) in &rows {
        match schema.decode_row(bytes) {
            Ok(live) => {
                check_row_key(&mut report, oid, &schema, rel.storage_id, key, &live);
                decoded.push((key.clone(), live));
            }
            Err(e) => report.push(format!(
                "I-ROWFMT: relation {oid} row {key:?} does not decode under the def (version {}) at {:?}: {e}",
                schema.version, at.ts()
            )),
        }
    }

    // ---- the index invariants (I-IDX, I-UNIQUE, I-IDX-WO, I-IDX-DO) ------

    // The computed-entry side needs only the schema and the index handle;
    // a TableHandle is the shape `index::entry` takes.
    let table = TableHandle {
        txn: ghost_id(&db.core),
        seq0: 0,
        lock: RelLockMode::AccessShare,
        oid,
        storage_id: rel.storage_id,
        schema: schema.clone(),
        def_version: schema.version,
        indexes: Vec::new(),
    };

    for idx in idxs {
        let isid = match idx.storage {
            IdxStorage::Id(isid) => isid,
            IdxStorage::Range(..) => {
                // FU-4a: its own report class — an admission that this
                // surface is not checkable yet (C-T6b2's range checks),
                // never an "I-IDX" violation string. Conservative: it
                // still makes the report unclean.
                report.push(format!(
                    "UNCHECKED: index {} of relation {oid} has explicit entry ranges (C-T6b2): not checkable",
                    idx.oid
                ));
                continue;
            }
        };
        let def = match IndexDef::decode(&idx.def) {
            Ok(def) => def,
            Err(e) => {
                report.push(format!(
                    "I-IDX: index {} of relation {oid} has an undecodable def: {e}",
                    idx.oid
                ));
                continue;
            }
        };
        let handle = IndexHandle {
            oid: idx.oid,
            storage_id: isid,
            def,
        };

        // The entries computed from the visible rows (§6 layout). A row
        // that failed I-ROWFMT contributes none — its entries are
        // unknown, and each such row is already reported above.
        let mut computed: BTreeMap<Key, Vec<u8>> = BTreeMap::new();
        let mut unique_dups: Vec<Key> = Vec::new();
        for (row_key, live) in &decoded {
            match index::entry(&table, &handle, row_key, live) {
                Ok((k, v, _rule)) => {
                    if handle.def.unique && computed.contains_key(&k) {
                        unique_dups.push(k.clone());
                    }
                    computed.insert(k, v);
                }
                Err(e) => report.push(format!(
                    "I-IDX: index {} of relation {oid}: entry of row {row_key:?} does not compute: {e}",
                    idx.oid
                )),
            }
        }

        match handle.def.state {
            // §4: an index serves a read at `at` iff Public and
            // `built_at <= at`; a Public index with `built_at > at` is
            // not read at `at` and not checked either.
            IndexState::Public if handle.def.built_at <= at.ts().0 => {
                for dup in unique_dups {
                    report.push(format!(
                        "I-UNIQUE: index {} of relation {oid}: two visible rows compute the same unique entry {dup:?}"
                    , idx.oid));
                }
                let visible = visible_entries(&db.core, isid, &ctx)?;
                for (k, v) in &computed {
                    match visible.get(k) {
                        None => report.push(format!(
                            "I-IDX: index {} of relation {oid}: entry {k:?} computed from a visible row is missing at {:?}",
                            idx.oid, at.ts()
                        )),
                        Some(p) if p != v => report.push(format!(
                            "I-IDX: index {} of relation {oid}: entry {k:?} has payload {p:?}, computed {v:?}",
                            idx.oid
                        )),
                        Some(_) => {}
                    }
                }
                for k in visible.keys() {
                    if !computed.contains_key(k) {
                        report.push(format!(
                            "I-IDX: index {} of relation {oid}: visible entry {k:?} is computed from no visible row",
                            idx.oid
                        ));
                    }
                }
            }
            IndexState::Public => {}
            IndexState::WriteOnly => {
                // §8: every visible entry must be computed from a
                // visible row; missing entries are the state's meaning.
                let visible = visible_entries(&db.core, isid, &ctx)?;
                for (k, p) in &visible {
                    if computed.get(k) != Some(p) {
                        report.push(format!(
                            "I-IDX-WO: index {} of relation {oid}: visible entry {k:?} is computed from no visible row",
                            idx.oid
                        ));
                    }
                }
            }
            IndexState::DeleteOnly => {
                // §8: a DeleteOnly index whose idx row is visible at
                // `at` (every one this scan returned) has no visible
                // entry.
                let visible = visible_entries(&db.core, isid, &ctx)?;
                if let Some((k, _)) = visible.first_key_value() {
                    report.push(format!(
                        "I-IDX-DO: index {} of relation {oid} is DeleteOnly at {:?} but has {} visible entries (e.g. {k:?})",
                        idx.oid, at.ts(), visible.len()
                    ));
                }
            }
        }
    }

    // ---- I-TOAST (§5) ------------------------------------------------------

    check_toast(
        &mut report,
        &db.core,
        oid,
        rel.storage_id,
        &rows,
        &ctx,
        at.ts(),
    )?;
    Ok(report)
}

/// The §4-visible rows of table storage `sid` at `ctx.snapshot`, in key
/// order, through one registered view per range: the §5 bound
/// `[table_lo, table_lo ‖ 0xFF)` keeps large-value chunks out of a row
/// scan (chunks are not rows; I-TOAST is C-T6d2's).
fn visible_rows<K: OrderedKv>(
    core: &Core<K>,
    sid: u64,
    ctx: &ReadCtx,
) -> Result<Vec<(Key, Vec<u8>)>, TxnError> {
    let lo = table_lo(sid);
    let hi = chunk_prefix(&lo);
    let view = core.open_view();
    scan(
        core,
        &view,
        (
            Bound::Included(lo.as_slice()),
            Bound::Excluded(hi.as_slice()),
        ),
        ctx,
        &mut NoSsi,
    )
    .collect::<Result<Vec<_>, _>>()
}

/// The §4-visible entries of index storage `isid` at `ctx.snapshot`, both
/// entry ranges (`/u/` then `/i/`), as `(logical key, payload)`.
///
/// §8: the version header's `key_changed` bit is **not significant**.
/// The read path returns the payload past the header, so comparisons
/// against these payloads ignore the header by construction — a
/// delete + re-insert of a `/u/` entry in one txn (header `0x01`) and a
/// fresh insert of it (header `0x00`) both yield the same payload.
fn visible_entries<K: OrderedKv>(
    core: &Core<K>,
    isid: u64,
    ctx: &ReadCtx,
) -> Result<BTreeMap<Key, Vec<u8>>, TxnError> {
    let mut out = BTreeMap::new();
    for (lo, hi) in index_ranges(isid) {
        let view = core.open_view();
        let rows = scan(
            core,
            &view,
            (
                Bound::Included(lo.as_slice()),
                Bound::Excluded(hi.as_slice()),
            ),
            ctx,
            &mut NoSsi,
        )
        .collect::<Result<Vec<_>, _>>()?;
        out.extend(rows);
    }
    Ok(out)
}

// ---- I-TOAST (§5) --------------------------------------------------------------

/// The §5 sizes of the large-value chunks of storage `sid` visible at
/// `ctx.snapshot`: everything from `table_lo ‖ 0xFF` (the mark no encoded
/// key starts with, so it is exactly where rows end) to the table
/// range's end, through the same §4 read path every reader uses.
fn visible_chunk_sizes<K: OrderedKv>(
    core: &Core<K>,
    sid: u64,
    ctx: &ReadCtx,
) -> Result<BTreeMap<Key, usize>, TxnError> {
    let mut lo = table_lo(sid);
    lo.push(TOAST_MARK);
    let hi = table_range(sid).1;
    let view = core.open_view();
    let mut out = BTreeMap::new();
    for (key, payload) in scan(
        core,
        &view,
        (
            Bound::Included(lo.as_slice()),
            Bound::Excluded(hi.as_slice()),
        ),
        ctx,
        &mut NoSsi,
    )
    .collect::<Result<Vec<_>, _>>()?
    {
        out.insert(key, payload.len());
    }
    Ok(out)
}

/// §5 I-TOAST, both directions at the check's snapshot:
///
/// - every pointer of every visible row — collected with
///   [`decode_row_raw`], which returns every attnum `<= natts` of the
///   row's own definition, **dropped included** (§3: no DDL rewrites
///   rows, so a pre-drop row still names its chunks there and a
///   live-columns decode would orphan them) — names chunks
///   `0..chunk_count` visible at the snapshot, each [`TOAST_CHUNK`]
///   bytes but a shorter non-empty last, summing to `raw_len`;
/// - every visible chunk of the storage is named by such a pointer.
///
/// A pointer naming more chunks than the storage has visible is
/// dangling at some index and is reported without being walked — the
/// bound that keeps a planted huge-but-self-consistent chunk count from
/// spinning the check (C-T6d2fix, gate F-5).
///
/// A row that does not even raw-decode is skipped: I-ROWFMT has already
/// reported it, and its pointers are unknowable. Violations are named
/// `"I-TOAST: …"`.
fn check_toast<K: OrderedKv>(
    report: &mut Report,
    core: &Core<K>,
    oid: u64,
    sid: u64,
    rows: &[(Key, Vec<u8>)],
    ctx: &ReadCtx,
    at: Ts,
) -> Result<(), TxnError> {
    let chunks = visible_chunk_sizes(core, sid, ctx)?;
    // Every chunk key a visible row's pointer names (§5's second half).
    let mut named: BTreeSet<Key> = BTreeSet::new();
    for (key, bytes) in rows {
        let Ok((_, raw)) = decode_row_raw(bytes) else {
            continue;
        };
        for p in raw.iter().flatten().filter_map(pointer_of) {
            if p.chunks == 0 || p.raw_len == 0 {
                report.push(format!(
                    "I-TOAST: relation {oid} row {key:?}: pointer {:?} names no chunk",
                    p.id
                ));
                continue;
            }
            if u64::from(p.chunks) * (TOAST_CHUNK as u64) < u64::from(p.raw_len) {
                // Even `chunks` full chunks cannot reach `raw_len`: the
                // pointer is corrupt, and iterating it cannot be bounded
                // by the store (a planted huge count would spin).
                report.push(format!(
                    "I-TOAST: relation {oid} row {key:?}: pointer {:?} counts {} chunks, which cannot sum to raw_len {}",
                    p.id, p.chunks, p.raw_len
                ));
                continue;
            }
            if p.chunks as usize > chunks.len() {
                // The pointer names more chunks than the storage has
                // visible, so it is dangling at some index — reported
                // without iterating it (C-T6d2fix, gate F-5: a planted
                // huge-but-self-consistent count — u32::MAX chunks with
                // raw_len u32::MAX — passes the guard above, and the
                // walk below would insert into `named` once per index).
                report.push(format!(
                    "I-TOAST: relation {oid} row {key:?}: pointer {:?} counts {} chunks, but the storage has {} visible (dangling at some index)",
                    p.id, p.chunks, chunks.len()
                ));
                continue;
            }
            let mut missing: Option<u32> = None;
            let mut shape_bad = false;
            let mut sum: u64 = 0;
            for i in 0..p.chunks {
                let ck = chunk_key(sid, &p.id, i);
                named.insert(ck.clone());
                let Some(&len) = chunks.get(&ck) else {
                    missing = missing.or(Some(i));
                    continue;
                };
                let last = i + 1 == p.chunks;
                let shape_ok =
                    len <= TOAST_CHUNK && if last { len > 0 } else { len == TOAST_CHUNK };
                if !shape_ok && !shape_bad {
                    shape_bad = true;
                    report.push(format!(
                        "I-TOAST: relation {oid} row {key:?}: chunk {i} of pointer {:?} is {len} bytes, not the §5 shape",
                        p.id
                    ));
                }
                sum += len as u64;
            }
            if let Some(i) = missing {
                report.push(format!(
                    "I-TOAST: relation {oid} row {key:?}: chunk {i} of pointer {:?} is not visible at {at:?} (dangling pointer)",
                    p.id
                ));
            } else if !shape_bad && sum != u64::from(p.raw_len) {
                report.push(format!(
                    "I-TOAST: relation {oid} row {key:?}: the chunks of pointer {:?} sum to {sum}, raw_len says {}",
                    p.id, p.raw_len
                ));
            }
        }
    }
    for ck in chunks.keys() {
        if !named.contains(ck) {
            report.push(format!(
                "I-TOAST: relation {oid}: visible chunk {ck:?} is named by no pointer of a visible row (orphan)"
            ));
        }
    }
    Ok(())
}

/// The toast pointers of one raw-decoded row.
fn pointer_of(d: &RawDatum) -> Option<&ToastPointer> {
    match d {
        RawDatum::Toast(p) => Some(p),
        RawDatum::Inline(_) => None,
    }
}

/// I-ROWFMT's key half for one decoded row: a pk table's pk columns
/// re-encode to the row's key past `table_lo` (§3); a hidden-rowid
/// table's rowid is in the key only, never a column, so the key's value
/// part must be one canonical bytea encoding (decode + re-encode).
fn check_row_key(
    report: &mut Report,
    oid: u64,
    schema: &TableSchema,
    sid: u64,
    key: &[u8],
    live: &[Option<Datum>],
) {
    let lo = table_lo(sid);
    if !key.starts_with(&lo) {
        report.push(format!(
            "I-ROWFMT: relation {oid} row key {key:?} is outside its table storage"
        ));
        return;
    }
    let suffix = &key[lo.len()..];
    let mismatch =
        || format!("I-ROWFMT: relation {oid} row key {key:?} does not re-encode from its row");
    if schema.hidden_rowid {
        let cols = [KeyColumn::asc(KeyType::Bytea)];
        match decode_key_prefix(&cols, suffix) {
            Ok((vals, used)) => {
                let mut re = Vec::new();
                if used != suffix.len()
                    || encode_key(&cols, &vals, &mut re).is_err()
                    || re.as_slice() != suffix
                {
                    report.push(mismatch());
                }
            }
            Err(_) => report.push(mismatch()),
        }
        return;
    }
    let mut vals: Vec<Option<Value>> = Vec::with_capacity(schema.pk.len());
    let mut cols: Vec<KeyColumn> = Vec::with_capacity(schema.pk.len());
    for &attnum in &schema.pk {
        let idx = usize::from(attnum).checked_sub(1);
        let Some(col) = idx.and_then(|i| schema.columns.get(i)) else {
            report.push(format!(
                "I-ROWFMT: relation {oid} primary key names a missing attnum {attnum}"
            ));
            return;
        };
        cols.push(KeyColumn::asc(col.ty.clone()));
        let Some(pos) = schema.live_columns().position(|c| c.attnum == attnum) else {
            report.push(format!(
                "I-ROWFMT: relation {oid} primary key names a dropped attnum {attnum}"
            ));
            return;
        };
        match live.get(pos) {
            Some(None) => {
                report.push(format!(
                    "I-ROWFMT: relation {oid} row key {key:?}: primary-key column {attnum} is NULL"
                ));
                return;
            }
            Some(Some(Datum::Inline(v))) => vals.push(Some(v.clone())),
            Some(Some(Datum::Toast(_))) => {
                report.push(format!(
                    "I-ROWFMT: relation {oid} row key {key:?}: primary-key column {attnum} is out of line"
                ));
                return;
            }
            None => {
                report.push(format!(
                    "I-ROWFMT: relation {oid} row key {key:?}: decoded row is short a pk column"
                ));
                return;
            }
        }
    }
    let mut enc = Vec::new();
    match encode_key(&cols, &vals, &mut enc) {
        Err(_) => report.push(mismatch()),
        Ok(()) if enc.as_slice() != suffix => report.push(mismatch()),
        Ok(()) => {}
    }
}
