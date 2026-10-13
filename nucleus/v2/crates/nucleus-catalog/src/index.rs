//! C-T6 §6 index entries and their maintenance: computing an index's
//! entry for a row version (the `/u/` and `/i/` layouts), the §6 key
//! limit (2704 bytes: an insert over it is 54000, an oversize delete is
//! skipped — no such entry can exist), and the per-state maintenance the
//! typed DML drives after its row op returned `Applied`:
//!
//! - **DeleteOnly**: a delete, or an update that changes the entry,
//!   deletes the old entry (blindly, through
//!   [`Core::delete_entry`](nucleus_txn::boot::Core::delete_entry)); an
//!   insert does nothing.
//! - **WriteOnly / Public**: full maintenance — inserts insert entries
//!   (`/u/` with `UniqueRule::Unique { same_row: Some(row key) }`), a
//!   delete deletes every entry of the deleted version, an update that
//!   changes an entry deletes the old and inserts the new.
//!
//! Entries are always computed from the row version the row op actually
//! acted on (its EPQ base), never from the statement's snapshot row.

use nucleus_codec::row::{value_repr_eq, Datum, TableSchema};
use nucleus_codec::{encode_key, KeyColumn, Value};
use nucleus_kv::{Key, OrderedKv};
use nucleus_txn::boot::Core;
use nucleus_txn::catalog::index_ranges;
use nucleus_txn::txn::Txn;
use nucleus_txn::write::StmtCtx;

use crate::db::{IndexHandle, TableHandle, KEY_LIMIT};
use crate::defs::IndexState;
use crate::error::DbError;

/// The `/u/{be64 sid}/` prefix of index storage `sid` (§6: unique entries
/// with no NULL key column, or `nulls_not_distinct`).
fn u_lo(sid: u64) -> Key {
    index_ranges(sid).remove(0).0
}

/// The `/i/{be64 sid}/` prefix of index storage `sid` (§6: every other
/// entry).
fn i_lo(sid: u64) -> Key {
    index_ranges(sid).remove(1).0
}

/// The key columns of `idx` under `schema`, in index order (§6): each
/// indexed attnum must name a live column of the definition.
fn key_columns(schema: &TableSchema, idx: &IndexHandle) -> Result<Vec<KeyColumn>, DbError> {
    let mut cols = Vec::with_capacity(idx.def.columns.len());
    for col in &idx.def.columns {
        let Some(found) = schema
            .live_columns()
            .find(|c| c.attnum == col.attnum)
            .map(|c| c.ty.clone())
        else {
            return Err(DbError::Row(nucleus_codec::row::RowError::Corrupt(
                "index names a dropped column",
            )));
        };
        cols.push(KeyColumn {
            ty: found,
            dir: col.dir,
            nulls: col.nulls,
        });
    }
    Ok(cols)
}

/// The key values of `row` for `idx`, in index order (`None` = NULL). An
/// out-of-line datum cannot be an index key (toast is C-T6d1; the typed
/// layer never writes one yet).
fn key_values(
    schema: &TableSchema,
    idx: &IndexHandle,
    row: &[Option<Datum>],
) -> Result<Vec<Option<Value>>, DbError> {
    let mut vals = Vec::with_capacity(idx.def.columns.len());
    for col in &idx.def.columns {
        let pos = schema.live_columns().position(|c| c.attnum == col.attnum);
        let Some(pos) = pos else {
            return Err(DbError::Row(nucleus_codec::row::RowError::Corrupt(
                "index names a dropped column",
            )));
        };
        match row.get(pos) {
            Some(None) => vals.push(None),
            Some(Some(Datum::Inline(v))) => vals.push(Some(v.clone())),
            Some(Some(Datum::Toast(_))) => {
                return Err(DbError::Row(nucleus_codec::row::RowError::Corrupt(
                    "indexed column is out of line",
                )))
            }
            None => {
                return Err(DbError::Row(nucleus_codec::row::RowError::Arity {
                    expected: schema.live_columns().count(),
                    got: row.len(),
                }))
            }
        }
    }
    Ok(vals)
}

/// One computed entry (§6), before the key limit is applied.
struct Built {
    key: Key,
    value: Vec<u8>,
    rule: nucleus_txn::write::UniqueRule,
    /// The encoded index key's byte length (the bytes that encode the
    /// indexed columns — the row key suffix of an `/i/` entry and the
    /// storage prefix are not part of it, exactly as §3 measures the
    /// encoded primary key past `table_lo`).
    key_len: usize,
}

/// Computes `idx`'s entry for one row version (§6): unique with no NULL
/// key column (or `nulls_not_distinct`) → `/u/{sid}/` ‖ key with the row
/// key as its payload; otherwise `/i/{sid}/` ‖ key ‖ row key with an
/// empty payload. The insert rule of a `/u/` entry is
/// `Unique { same_row: Some(row key) }`; an `/i/` entry has none.
fn build(
    h: &TableHandle,
    idx: &IndexHandle,
    row_key: &[u8],
    row: &[Option<Datum>],
) -> Result<Built, DbError> {
    let cols = key_columns(&h.schema, idx)?;
    let vals = key_values(&h.schema, idx, row)?;
    let has_null = vals.iter().any(|v| v.is_none());
    if idx.def.unique && (idx.def.nulls_not_distinct || !has_null) {
        let mut key = u_lo(idx.storage_id);
        let start = key.len();
        encode_key(&cols, &vals, &mut key)
            .map_err(|_| DbError::Row(nucleus_codec::row::RowError::Corrupt("index key")))?;
        let key_len = key.len() - start;
        Ok(Built {
            key,
            value: row_key.to_vec(),
            rule: nucleus_txn::write::UniqueRule::Unique {
                same_row: Some(row_key.to_vec()),
            },
            key_len,
        })
    } else {
        let mut key = i_lo(idx.storage_id);
        let start = key.len();
        encode_key(&cols, &vals, &mut key)
            .map_err(|_| DbError::Row(nucleus_codec::row::RowError::Corrupt("index key")))?;
        let key_len = key.len() - start;
        key.extend_from_slice(row_key);
        Ok(Built {
            key,
            value: Vec::new(),
            rule: nucleus_txn::write::UniqueRule::None,
            key_len,
        })
    }
}

/// `idx`'s entry for one row version of the opened table `h`, per §6:
/// `(entry key, entry value, insert rule)`. Pure — no limit check, no
/// I/O; the limit is [`entry_for_insert`]'s business (insert) and
/// [`entry_for_delete`]'s (skip).
pub(crate) fn entry(
    h: &TableHandle,
    idx: &IndexHandle,
    row_key: &[u8],
    row: &[Option<Datum>],
) -> Result<(Key, Vec<u8>, nucleus_txn::write::UniqueRule), DbError> {
    let b = build(h, idx, row_key, row)?;
    Ok((b.key, b.value, b.rule))
}

/// The entry an insert must place (§6 key limit): an encoded index key
/// over [`KEY_LIMIT`] bytes makes the insert fail with 54000 — no such
/// entry may be created.
pub(crate) fn entry_for_insert(
    h: &TableHandle,
    idx: &IndexHandle,
    row_key: &[u8],
    row: &[Option<Datum>],
) -> Result<(Key, Vec<u8>, nucleus_txn::write::UniqueRule), DbError> {
    let b = build(h, idx, row_key, row)?;
    if b.key_len > KEY_LIMIT {
        return Err(DbError::ProgramLimitExceeded);
    }
    Ok((b.key, b.value, b.rule))
}

/// The entry a delete must tombstone (§6 key limit): an entry whose
/// encoded index key is over [`KEY_LIMIT`] bytes cannot exist (its insert
/// would have failed 54000), so the delete skips it — `None`.
pub(crate) fn entry_for_delete(
    h: &TableHandle,
    idx: &IndexHandle,
    row_key: &[u8],
    row: &[Option<Datum>],
) -> Result<Option<Key>, DbError> {
    let b = build(h, idx, row_key, row)?;
    if b.key_len > KEY_LIMIT {
        return Ok(None);
    }
    Ok(Some(b.key))
}

/// One row version positioned for maintenance: its logical row key and
/// its decoded live datums.
pub(crate) struct IndexRow<'a> {
    pub key: &'a [u8],
    pub live: &'a [Option<Datum>],
}

/// Insert maintenance (§6): every WriteOnly/Public index of the list gets
/// the new row's entry (`insert_key`, the statement's ctx); a DeleteOnly
/// index gets nothing — inserts are not its business.
pub(crate) fn maintain_insert<K: OrderedKv>(
    core: &Core<K>,
    txn: &Txn,
    ctx: &StmtCtx,
    h: &TableHandle,
    indexes: &[IndexHandle],
    row: IndexRow<'_>,
) -> Result<(), DbError> {
    for idx in indexes {
        match idx.def.state {
            IndexState::DeleteOnly => {}
            IndexState::WriteOnly | IndexState::Public => {
                let (key, value, rule) = entry_for_insert(h, idx, row.key, row.live)?;
                core.insert_key(txn, &key, None, value, ctx.clone(), rule)
                    .map_err(DbError::Txn)?;
            }
        }
    }
    Ok(())
}

/// Delete maintenance (§6): after the row op's `Applied`, **every** index
/// in every state gets the base version's entry deleted through
/// [`Core::delete_entry`] — blind, so it never skips and never raises
/// 40001. An oversize entry key is skipped (§6: no such entry can exist).
pub(crate) fn maintain_delete<K: OrderedKv>(
    core: &Core<K>,
    txn: &Txn,
    ctx: &StmtCtx,
    h: &TableHandle,
    indexes: &[IndexHandle],
    base: IndexRow<'_>,
) -> Result<(), DbError> {
    for idx in indexes {
        if let Some(key) = entry_for_delete(h, idx, base.key, base.live)? {
            core.delete_entry(txn, &key, None, ctx.clone())
                .map_err(DbError::Txn)?;
        }
    }
    Ok(())
}

/// Update maintenance (§6): per index state, for every entry the update
/// changed (key or payload), delete the old entry of the **base** version
/// and — for WriteOnly/Public — insert the new one. An unchanged entry is
/// left alone (no tombstone, no new version).
pub(crate) fn maintain_update<K: OrderedKv>(
    core: &Core<K>,
    txn: &Txn,
    ctx: &StmtCtx,
    h: &TableHandle,
    indexes: &[IndexHandle],
    old: IndexRow<'_>,
    new: IndexRow<'_>,
) -> Result<(), DbError> {
    for idx in indexes {
        let old_e = entry(h, idx, old.key, old.live)?;
        let new_e = entry(h, idx, new.key, new.live)?;
        if old_e.0 == new_e.0 && old_e.1 == new_e.1 {
            continue;
        }
        if let Some(key) = entry_for_delete(h, idx, old.key, old.live)? {
            core.delete_entry(txn, &key, None, ctx.clone())
                .map_err(DbError::Txn)?;
        }
        match idx.def.state {
            IndexState::DeleteOnly => {}
            IndexState::WriteOnly | IndexState::Public => {
                let (key, value, rule) = entry_for_insert(h, idx, new.key, new.live)?;
                core.insert_key(txn, &key, None, value, ctx.clone(), rule)
                    .map_err(DbError::Txn)?;
            }
        }
    }
    Ok(())
}

/// Whether `set` changes a key column (C-T0 §1: a column of any
/// non-partial, non-expression unique index — in **any** state, §6 — or
/// of the primary key) of `base`: the caller's `key_cols_changed`, which
/// picks the row op's requested mode (`Update` vs `NoKeyUpdate`) and the
/// `key_changed` version bit (§2.1).
pub(crate) fn key_cols_changed(
    schema: &TableSchema,
    indexes: &[IndexHandle],
    set: &[(u16, Option<Datum>)],
    base: &[Option<Datum>],
) -> bool {
    for &(attnum, ref datum) in set {
        let key_col = schema.pk.contains(&attnum)
            || indexes
                .iter()
                .any(|ix| ix.def.unique && ix.def.columns.iter().any(|c| c.attnum == attnum));
        if !key_col {
            continue;
        }
        let Some(pos) = schema.live_columns().position(|c| c.attnum == attnum) else {
            continue;
        };
        let changed = match (base.get(pos), datum) {
            (Some(Some(Datum::Inline(a))), Some(Datum::Inline(b))) => !value_repr_eq(a, b),
            (Some(None), None) => false,
            // An out-of-line datum never compares equal (toast is C-T6d1).
            (Some(Some(Datum::Toast(_))), _) | (_, Some(Datum::Toast(_))) => true,
            _ => true,
        };
        if changed {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    //! The pure §6 layout pins: which prefix an entry lands under, its
    //! payload and rule, and the 2704-byte key limit's two sides. The
    //! maintenance dispatch is driven end-to-end by the `t6b2a_*`
    //! integration tests.

    use super::{entry, entry_for_delete, entry_for_insert};
    use crate::db::{IndexHandle, TableHandle, KEY_LIMIT};
    use crate::defs::{IndexColumn, IndexDef, IndexState};
    use crate::error::DbError;

    use nucleus_codec::row::{ColumnDef, Datum, TableSchema};
    use nucleus_codec::{Collation, Direction, KeyType, Nulls, Value};
    use nucleus_txn::locks::RelLockMode;
    use nucleus_txn::write::UniqueRule;
    use nucleus_txn::TxnId;

    fn ok<T, E: std::fmt::Debug>(r: Result<T, E>) -> T {
        match r {
            Ok(v) => v,
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }

    fn err_of<T, E: std::fmt::Debug>(r: Result<T, E>) -> E {
        match r {
            Err(e) => e,
            Ok(_) => panic!("unexpected ok"),
        }
    }

    fn schema() -> TableSchema {
        ok(TableSchema::new(
            vec![
                ColumnDef {
                    attnum: 1,
                    name: "id".into(),
                    ty: KeyType::Int4,
                    not_null: true,
                    dropped: false,
                    missing: None,
                },
                ColumnDef {
                    attnum: 2,
                    name: "payload".into(),
                    ty: KeyType::Text(Collation::C),
                    not_null: false,
                    dropped: false,
                    missing: None,
                },
            ],
            vec![1],
        ))
    }

    fn handle(schema: TableSchema, indexes: Vec<IndexHandle>) -> TableHandle {
        TableHandle {
            txn: TxnId { epoch: 1, n: 1 },
            seq0: 1,
            lock: RelLockMode::RowExclusive,
            oid: 7,
            storage_id: 3,
            def_version: schema.version,
            schema,
            indexes,
        }
    }

    fn idx(attnum: u16, unique: bool, state: IndexState) -> IndexHandle {
        IndexHandle {
            oid: 11,
            storage_id: 5,
            def: IndexDef {
                columns: vec![IndexColumn {
                    attnum,
                    dir: Direction::Asc,
                    nulls: Nulls::Last,
                }],
                unique,
                nulls_not_distinct: false,
                concurrently: false,
                state,
                built_at: 0,
            },
        }
    }

    fn row(id: i32, payload: Option<&str>) -> Vec<Option<Datum>> {
        vec![
            Some(Datum::Inline(Value::Int4(id))),
            payload.map(|p| Datum::Inline(Value::Text(p.to_string()))),
        ]
    }

    /// The `/u/` and `/i/` prefixes of storage 5 (§6).
    fn prefixes() -> (Vec<u8>, Vec<u8>) {
        let ranges = nucleus_txn::catalog::index_ranges(5);
        (ranges[0].0.clone(), ranges[1].0.clone())
    }

    #[test]
    fn entry_layout_unique_non_unique_and_nulls() {
        let h = handle(schema(), Vec::new());
        let row_key = b"/t/row1".to_vec();
        let (u, i) = prefixes();

        // Unique, no NULL: /u/ ‖ key, payload = the row key, unique rule.
        let (key, value, rule) = ok(entry(
            &h,
            &idx(2, true, IndexState::Public),
            &row_key,
            &row(1, Some("a")),
        ));
        let mut want = u.clone();
        nucleus_codec::encode_key(
            &[nucleus_codec::KeyColumn::asc(KeyType::Text(Collation::C))],
            &[Some(Value::Text("a".into()))],
            &mut want,
        )
        .map_err(|e| panic!("{e:?}"))
        .ok();
        assert_eq!(key, want, "the unique entry key is /u/ ‖ encoded key");
        assert_eq!(value, row_key, "the /u/ payload is the row key");
        assert_eq!(
            rule,
            UniqueRule::Unique {
                same_row: Some(row_key.clone())
            }
        );

        // Unique with a NULL key column (NULLS DISTINCT): /i/, no rule.
        let (key, value, rule) = ok(entry(
            &h,
            &idx(2, true, IndexState::Public),
            &row_key,
            &row(1, None),
        ));
        assert!(key.starts_with(&i), "a NULL unique key goes to /i/");
        assert!(key.ends_with(&row_key), "the /i/ key ends with the row key");
        assert_eq!(value, Vec::<u8>::new(), "the /i/ payload is empty");
        assert_eq!(rule, UniqueRule::None);

        // NULLS NOT DISTINCT: NULL is a /u/ key like any value.
        let mut nnd = idx(2, true, IndexState::Public);
        nnd.def.nulls_not_distinct = true;
        let (key, _, rule) = ok(entry(&h, &nnd, &row_key, &row(1, None)));
        assert!(key.starts_with(&u), "NULLS NOT DISTINCT uses /u/");
        assert_eq!(
            rule,
            UniqueRule::Unique {
                same_row: Some(row_key.clone())
            }
        );

        // Non-unique: always /i/ ‖ key ‖ row key.
        let (key, value, rule) = ok(entry(
            &h,
            &idx(2, false, IndexState::WriteOnly),
            &row_key,
            &row(1, Some("a")),
        ));
        let mut want = i.clone();
        nucleus_codec::encode_key(
            &[nucleus_codec::KeyColumn::asc(KeyType::Text(Collation::C))],
            &[Some(Value::Text("a".into()))],
            &mut want,
        )
        .map_err(|e| panic!("{e:?}"))
        .ok();
        want.extend_from_slice(&row_key);
        assert_eq!(key, want, "the non-unique entry key is /i/ ‖ key ‖ row key");
        assert_eq!(value, Vec::<u8>::new());
        assert_eq!(rule, UniqueRule::None);

        // An int4 index column encodes its own type (attnum 1 here).
        let (key, _, _) = ok(entry(
            &h,
            &idx(1, false, IndexState::Public),
            &row_key,
            &row(9, Some("a")),
        ));
        let mut want = i;
        nucleus_codec::encode_key(
            &[nucleus_codec::KeyColumn::asc(KeyType::Int4)],
            &[Some(Value::Int4(9))],
            &mut want,
        )
        .map_err(|e| panic!("{e:?}"))
        .ok();
        want.extend_from_slice(&row_key);
        assert_eq!(key, want);
    }

    #[test]
    fn oversize_keys_are_54000_on_insert_and_skipped_on_delete() {
        let h = handle(schema(), Vec::new());
        let row_key = b"/t/row1".to_vec();
        // marker + text bytes + the 2-byte text terminator: a text of
        // KEY_LIMIT - 3 bytes encodes to exactly KEY_LIMIT.
        let fits = "a".repeat(KEY_LIMIT - 3);
        let over = "a".repeat(KEY_LIMIT - 2);
        let ix = idx(2, false, IndexState::WriteOnly);

        ok(entry_for_insert(&h, &ix, &row_key, &row(1, Some(&fits))));
        let err = err_of(entry_for_insert(&h, &ix, &row_key, &row(1, Some(&over))));
        assert!(matches!(err, DbError::ProgramLimitExceeded));
    }

    #[test]
    fn oversize_delete_is_skipped() {
        let h = handle(schema(), Vec::new());
        let row_key = b"/t/row1".to_vec();
        let over = "a".repeat(KEY_LIMIT - 2);
        let ix = idx(2, true, IndexState::DeleteOnly);
        assert_eq!(
            ok(entry_for_delete(&h, &ix, &row_key, &row(1, Some(&over)))),
            None,
            "an oversize entry delete is skipped: no such entry can exist"
        );
        // In-limit deletes name their entry.
        assert!(ok(entry_for_delete(&h, &ix, &row_key, &row(1, Some("a")))).is_some());
    }

    #[test]
    fn a_dropped_or_out_of_line_indexed_column_is_corrupt() {
        let mut h = handle(schema(), Vec::new());
        // Drop attnum 2 (dropped columns stay in the list).
        h.schema = ok(h.schema.drop_column(2));
        let ix = idx(2, false, IndexState::Public);
        let err = err_of(entry(&h, &ix, b"k", &row(1, Some("a"))));
        assert!(matches!(
            err,
            DbError::Row(nucleus_codec::row::RowError::Corrupt(_))
        ));
        // An out-of-line datum is not an index key (toast is C-T6d1).
        let h2 = handle(schema(), Vec::new());
        let toasted = vec![
            Some(Datum::Inline(Value::Int4(1))),
            Some(Datum::Toast(nucleus_codec::row::ToastPointer {
                id: [0u8; 20],
                raw_len: 1,
                chunks: 1,
            })),
        ];
        let err = err_of(entry(&h2, &ix, b"k", &toasted));
        assert!(matches!(
            err,
            DbError::Row(nucleus_codec::row::RowError::Corrupt(_))
        ));
    }
}
