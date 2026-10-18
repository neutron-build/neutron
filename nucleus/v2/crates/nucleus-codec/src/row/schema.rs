//! Table definition bytes (C-T6 §3): the payload of `RelRow.def`.
//!
//! ```text
//! def   = 0x54 0x01 be32(version) u8(hidden_rowid) be16(ncols) col*
//!         be16(npk) be16(pk_attnum)*
//! col   = be16(attnum) be32(namelen) name type flags(1)
//!         [be32(not_null_since)] [be32(len) value_bytes]
//! flags = bit0 not_null, bit1 dropped, bit2 has_missing,
//!         bit4 has_not_null_since
//! ```
//!
//! `not_null_since` (§2/§3's NOT NULL era marker: the def version at
//! which the column's constraint (last) came into force, read by
//! `decode_row`'s per-column era rule) persists as a `be32` word right
//! after the flags byte, present exactly when it **deviates from the
//! re-derivation** — `version` for a NOT NULL column, 0 for a nullable
//! one. Every def a DDL helper can produce round-trips exactly: a
//! marker born at the def's own version (CREATE TABLE, ADD COLUMN ...
//! NOT NULL, the SET NOT NULL that just bumped the version) equals the
//! re-derivation and is elided, and the first later DDL — which keeps
//! the marker and bumps the version — writes the word. The elision is
//! what keeps the golden def corpus (`tests/golden_row/defs.tsv`, all
//! version-1 defs, markers 0/1) byte-identical: that corpus is outside
//! C-T6e4fix2's Touch-only list and cannot be regenerated, while an
//! unconditional word would change every pinned hex. The format tag
//! stays `0x54 0x01`: no persisted datadir exists before the first
//! release (C-K4 owns format versioning from then). The presence bit
//! is bit4 (not the free bit3) because the def-decode totality pin
//! (`tests/row_defs.rs`) holds flags byte `0x08` unknown, and that
//! file's assertions are outside this card's limits.
//!
//! Because the word is only absent when the re-derivation is exact,
//! a hand-broken in-memory marker on a **live** column (a nonzero
//! `not_null_since` on a nullable one, a zero or above-version one on
//! a NOT NULL one) is **written faithfully** and rejected on decode
//! (XX001) rather than silently canonicalized away — the three
//! rejections below. The dropped state is the one mask: encode
//! canonicalizes it wholesale (flags bit1 only), and a dropped column
//! carries no era.
//! The §3.1 type tags are pinned as the const table below and on disk;
//! they are never derived from `KeyType`'s discriminant order. Decode is
//! total: an unknown tag, truncation, trailing bytes, attnums out of
//! order, a pk entry naming a dropped or missing attnum, a `version` of
//! 0, or a `hidden_rowid` byte that is not 0/1 or disagrees with `npk`
//! are [`RowError::Corrupt`] (XX001), never a panic.
//!
//! The DDL helpers are pure byte-level schema evolution: each returns
//! the next version (`version + 1`, §3) and nothing else — locks,
//! validating scans, index drops and volatile-default refusals are the
//! caller's. Rows are never rewritten: every row written later encodes
//! at the current version ("lazy upgrade", §3).

use super::{check_attnums, decode_row, encode_row, value_error, ColumnDef, Datum, RowError};
use crate::row::value::{decode_value, encode_value};
use crate::value::{Collation, KeyType, Value};

/// §3.1 persisted type tags, pinned as a table: the on-disk numbers are
/// constants, not `KeyType` discriminant values. `encode_type` /
/// `decode_type` are the only readers and writers of these.
const TAG_BOOL: u8 = 0x01;
const TAG_INT2: u8 = 0x02;
const TAG_INT4: u8 = 0x03;
const TAG_INT8: u8 = 0x04;
const TAG_FLOAT4: u8 = 0x05;
const TAG_FLOAT8: u8 = 0x06;
const TAG_NUMERIC: u8 = 0x07;
/// Text: followed by a collation byte (§3.1).
const TAG_TEXT: u8 = 0x08;
const TAG_BYTEA: u8 = 0x09;
const TAG_DATE: u8 = 0x0A;
const TAG_TIME: u8 = 0x0B;
const TAG_TIMESTAMP: u8 = 0x0C;
const TAG_TIMESTAMPTZ: u8 = 0x0D;
const TAG_INTERVAL: u8 = 0x0E;
const TAG_UUID: u8 = 0x0F;
const TAG_JSONB: u8 = 0x10;
/// Array: followed by its element type (§3.1).
const TAG_ARRAY: u8 = 0x11;

/// Collation byte of the text tag (§3.1): `0x01` = C.
const COLLATION_C: u8 = 0x01;

/// §3 column flags.
const FLAG_NOT_NULL: u8 = 1 << 0;
const FLAG_DROPPED: u8 = 1 << 1;
const FLAG_MISSING: u8 = 1 << 2;
/// The `be32(not_null_since)` era-marker word follows the flags byte
/// (§3; see the module docs for the elision rule and the bit4 choice).
const FLAG_SINCE: u8 = 1 << 4;
const FLAG_KNOWN: u8 = FLAG_NOT_NULL | FLAG_DROPPED | FLAG_MISSING | FLAG_SINCE;

const DEF_MAGIC: [u8; 2] = [0x54, 0x01];

/// A table definition (§3): the bytes of `RelRow.def` plus the pure DDL
/// helpers that evolve it. Invariants, checked by [`TableSchema::encode`]
/// and [`TableSchema::new`]: `columns[i].attnum == i + 1` (attnums are
/// never reused) and `hidden_rowid == pk.is_empty()`.
#[derive(Debug, Clone, PartialEq)]
pub struct TableSchema {
    /// 1 at CREATE TABLE, +1 per DDL that changed `def` (§3).
    pub version: u32,
    /// Rows are keyed by a hidden rowid rather than a primary key:
    /// true iff `pk` is empty (§3).
    pub hidden_rowid: bool,
    /// Every attnum ever allocated, `1..=ncols` in order, dropped
    /// columns included.
    pub columns: Vec<ColumnDef>,
    /// Primary-key attnums, in key order; each must name a live column.
    pub pk: Vec<u16>,
}

impl TableSchema {
    /// CREATE TABLE: version 1. `pk` entries must name live columns of
    /// `columns` (which must be attnums `1..=n` in order);
    /// `hidden_rowid` is derived from `pk.is_empty()`. A NOT NULL
    /// column gets `not_null_since = 1` — the create version, the era
    /// its constraint came into force (pk columns included); a nullable
    /// one gets 0.
    pub fn new(columns: Vec<ColumnDef>, pk: Vec<u16>) -> Result<TableSchema, RowError> {
        check_attnums(&columns)?;
        check_pk(&columns, &pk)?;
        let mut columns = columns;
        for col in &mut columns {
            col.not_null_since = u32::from(col.not_null);
        }
        Ok(TableSchema {
            version: 1,
            hidden_rowid: pk.is_empty(),
            columns,
            pk,
        })
    }

    /// Serialises the definition (§3). The struct invariants are checked
    /// first, so a broken in-memory schema is an [`RowError::InvalidDdl`]
    /// rather than broken bytes: attnums `1..=n` in order, `hidden_rowid
    /// == pk.is_empty()`, every pk attnum live and in range. The
    /// `not_null_since` era marker persists as the flags-gated `be32`
    /// word of §3's `col` layout (elided exactly when it equals the
    /// re-derivation — see the module docs), so every DDL-producible
    /// schema round-trips through the bytes with its markers exact.
    pub fn encode(&self) -> Result<Vec<u8>, RowError> {
        check_attnums(&self.columns)?;
        check_pk(&self.columns, &self.pk)?;
        if self.hidden_rowid != self.pk.is_empty() {
            return Err(RowError::InvalidDdl(
                "hidden_rowid must equal pk.is_empty()",
            ));
        }
        let ncols = u16::try_from(self.columns.len())
            .map_err(|_| RowError::InvalidDdl("too many columns"))?;
        let npk = u16::try_from(self.pk.len())
            .map_err(|_| RowError::InvalidDdl("too many primary-key columns"))?;
        let mut out = Vec::with_capacity(10 + self.columns.len() * 12 + self.pk.len() * 2);
        out.extend_from_slice(&DEF_MAGIC);
        out.extend_from_slice(&self.version.to_be_bytes());
        out.push(u8::from(self.hidden_rowid));
        out.extend_from_slice(&ncols.to_be_bytes());
        for col in &self.columns {
            out.extend_from_slice(&col.attnum.to_be_bytes());
            let namelen = u32::try_from(col.name.len())
                .map_err(|_| RowError::InvalidDdl("column name too long"))?;
            out.extend_from_slice(&namelen.to_be_bytes());
            out.extend_from_slice(col.name.as_bytes());
            encode_type(&col.ty, &mut out)?;
            let mut flags = flag_bits(col);
            // §3's era-marker word: present exactly when the marker
            // deviates from the re-derivation (`version` for a NOT NULL
            // column, 0 otherwise) — see the module docs. Keyed on the
            // canonical flags bit, so a hand-broken marker on a live
            // nullable column (a stale `not_null_since` no DDL can
            // produce) is written faithfully and rejected on decode,
            // never silently canonicalized away. The one mask is the
            // dropped state, which encode canonicalizes wholesale
            // (bit1 only — the `encode_canonicalizes_dropped_not_null`
            // pin): a dropped column has no era to carry.
            let derived = if flags & FLAG_NOT_NULL != 0 {
                self.version
            } else {
                0
            };
            if !col.dropped && col.not_null_since != derived {
                flags |= FLAG_SINCE;
            }
            let mut value_bytes = Vec::new();
            if let Some(v) = &col.missing {
                flags |= FLAG_MISSING;
                encode_value(&col.ty, v, &mut value_bytes)
                    .map_err(|e| value_error(col.attnum, e))?;
            }
            out.push(flags);
            if flags & FLAG_SINCE != 0 {
                out.extend_from_slice(&col.not_null_since.to_be_bytes());
            }
            if flags & FLAG_MISSING != 0 {
                let len = u32::try_from(value_bytes.len())
                    .map_err(|_| RowError::Corrupt("missing value too large"))?;
                out.extend_from_slice(&len.to_be_bytes());
                out.extend_from_slice(&value_bytes);
            }
        }
        out.extend_from_slice(&npk.to_be_bytes());
        for &attnum in &self.pk {
            out.extend_from_slice(&attnum.to_be_bytes());
        }
        Ok(out)
    }

    /// Parses a definition (§3). Total: every malformed input is an
    /// `Err` ([`RowError::Corrupt`], XX001) — a `version` of 0 (§3: 1
    /// at CREATE, +1 per DDL), unknown type tags or collations,
    /// truncation, trailing bytes, attnums out of order, undecodable
    /// `missing` bytes, a pk entry naming a dropped or missing attnum,
    /// unknown flag bits, or a `hidden_rowid` byte that
    /// is not 0/1 or disagrees with `npk`. Duplicate pk entries are not
    /// rejected (§3 does not forbid them; the writer never makes them).
    ///
    /// The `not_null_since` era marker is read from the `be32` word when
    /// flag bit4 is set and re-derived otherwise (`version` for a NOT
    /// NULL column, 0 for a nullable one — exact whenever elided, see
    /// the module docs). The three inconsistent encodings are corruption:
    /// a marker word on a column whose `not_null` bit is clear (a
    /// nullable or dropped column carries no era), a word of 0 on a NOT
    /// NULL column, and a word above the def's own `version`.
    pub fn decode(bytes: &[u8]) -> Result<TableSchema, RowError> {
        let short = || RowError::Corrupt("truncated definition");
        let mut r = super::RowReader::new(bytes);
        let magic = r.take_array::<2>().map_err(|_| short())?;
        if magic != DEF_MAGIC {
            return Err(RowError::Corrupt("definition magic"));
        }
        let version = r.u32be().map_err(|_| short())?;
        if version == 0 {
            return Err(RowError::Corrupt("definition version 0"));
        }
        let hidden_rowid = match r.u8().map_err(|_| short())? {
            0 => false,
            1 => true,
            _ => return Err(RowError::Corrupt("hidden_rowid byte")),
        };
        let ncols = usize::from(r.u16be().map_err(|_| short())?);
        let mut columns = Vec::with_capacity(ncols);
        for i in 0..ncols {
            let attnum = r.u16be().map_err(|_| short())?;
            if usize::from(attnum) != i + 1 {
                return Err(RowError::Corrupt("definition attnums out of order"));
            }
            let namelen = r.u32be().map_err(|_| short())? as usize;
            let name_bytes = r.take(namelen).map_err(|_| short())?;
            let name = std::str::from_utf8(name_bytes)
                .map_err(|_| RowError::Corrupt("column name is not UTF-8"))?
                .to_string();
            let ty = decode_type(&mut r, 0)?;
            let flags = r.u8().map_err(|_| short())?;
            if flags & !FLAG_KNOWN != 0 {
                return Err(RowError::Corrupt("unknown definition flags"));
            }
            let not_null = flags & FLAG_NOT_NULL != 0;
            let not_null_since = if flags & FLAG_SINCE != 0 {
                let since = r.u32be().map_err(|_| short())?;
                if !not_null {
                    return Err(RowError::Corrupt(
                        "not_null_since word on a nullable column",
                    ));
                }
                if since == 0 {
                    return Err(RowError::Corrupt(
                        "not_null_since of zero on a NOT NULL column",
                    ));
                }
                if since > version {
                    return Err(RowError::Corrupt("not_null_since above the version"));
                }
                since
            } else if not_null {
                version
            } else {
                0
            };
            let missing = if flags & FLAG_MISSING != 0 {
                let len = r.u32be().map_err(|_| short())? as usize;
                let value_bytes = r.take(len).map_err(|_| short())?;
                Some(decode_value(&ty, value_bytes).map_err(|e| value_error(attnum, e))?)
            } else {
                None
            };
            columns.push(ColumnDef {
                attnum,
                name,
                ty,
                not_null,
                not_null_since,
                dropped: flags & FLAG_DROPPED != 0,
                missing,
            });
        }
        let npk = usize::from(r.u16be().map_err(|_| short())?);
        let mut pk = Vec::with_capacity(npk);
        for _ in 0..npk {
            let attnum = r.u16be().map_err(|_| short())?;
            let idx = usize::from(attnum)
                .checked_sub(1)
                .ok_or(RowError::Corrupt("primary key names a missing attnum"))?;
            let col = columns
                .get(idx)
                .ok_or(RowError::Corrupt("primary key names a missing attnum"))?;
            if col.dropped {
                return Err(RowError::Corrupt("primary key names a dropped attnum"));
            }
            pk.push(attnum);
        }
        if hidden_rowid != (npk == 0) {
            return Err(RowError::Corrupt("hidden_rowid disagrees with npk"));
        }
        if !r.done() {
            return Err(RowError::Corrupt("trailing bytes after definition"));
        }
        Ok(TableSchema {
            version,
            hidden_rowid,
            columns,
            pk,
        })
    }

    /// ADD COLUMN (§3): appends attnum `ncols + 1` and returns version
    /// `+ 1`. A duplicate *live* name is refused — a dropped column's
    /// name may be reused. A `missing` default (a non-volatile one,
    /// evaluated once by the caller at DDL time) must encode under `ty`;
    /// volatile defaults and the NOT NULL emptiness scan are the
    /// caller's (§3, §9). A NOT NULL column's era starts here:
    /// `not_null_since` is the DDL's new version.
    pub fn add_column(
        &self,
        name: &str,
        ty: KeyType,
        not_null: bool,
        missing: Option<Value>,
    ) -> Result<TableSchema, RowError> {
        if self.columns.iter().any(|c| !c.dropped && c.name == name) {
            return Err(RowError::InvalidDdl("duplicate column name"));
        }
        let attnum = u16::try_from(self.columns.len() + 1)
            .map_err(|_| RowError::InvalidDdl("too many columns"))?;
        // The type must itself be encodable (no array of array) and a
        // default must match it, or the def could not round-trip.
        let mut scratch = Vec::new();
        encode_type(&ty, &mut scratch)?;
        if let Some(v) = &missing {
            encode_value(&ty, v, &mut scratch).map_err(|e| value_error(attnum, e))?;
        }
        let mut next = self.bumped()?;
        let not_null_since = if not_null { next.version } else { 0 };
        next.columns.push(ColumnDef {
            attnum,
            name: name.to_string(),
            ty,
            not_null,
            not_null_since,
            dropped: false,
            missing,
        });
        Ok(next)
    }

    /// DROP COLUMN (§3): sets `dropped` and clears `not_null` (and its
    /// era marker); attnums are never reused, so the column stays listed
    /// forever. Refused for a primary-key column (0A000, §9) and for an
    /// unknown or already dropped attnum. Dropping the indexes that use
    /// the column is the caller's (same txn, §3).
    pub fn drop_column(&self, attnum: u16) -> Result<TableSchema, RowError> {
        let col = self.live_column(attnum)?;
        if self.pk.contains(&attnum) {
            return Err(RowError::InvalidDdl("cannot drop a primary-key column"));
        }
        let mut next = self.bumped()?;
        let col = &mut next.columns[usize::from(col.attnum) - 1];
        col.dropped = true;
        col.not_null = false;
        col.not_null_since = 0;
        Ok(next)
    }

    /// SET NOT NULL / DROP NOT NULL (§3): sets or clears the flag, no
    /// rewrite, version `+ 1`. The validating scan (any NULL, including
    /// a NULL `missing`, → 23502) is the caller's. Refused for an
    /// unknown or dropped attnum. The era marker moves with the flag: a
    /// SET that (re-)arms the constraint takes the DDL's new version —
    /// rows written while it was dropped may legally carry a NULL, and
    /// a re-SET after a DROP must stay lenient for them — while a SET on
    /// a column already under the constraint keeps the era it came into
    /// force (the constraint never lapsed); a DROP resets it to 0.
    pub fn set_not_null(&self, attnum: u16, not_null: bool) -> Result<TableSchema, RowError> {
        self.live_column(attnum)?;
        let mut next = self.bumped()?;
        let col = &mut next.columns[usize::from(attnum) - 1];
        col.not_null_since = match (not_null, col.not_null) {
            (true, false) => next.version,
            (true, true) => col.not_null_since,
            (false, _) => 0,
        };
        col.not_null = not_null;
        Ok(next)
    }

    /// The non-dropped columns, in attnum order — one `live` entry per
    /// element is what [`TableSchema::encode_row`] takes.
    pub fn live_columns(&self) -> impl Iterator<Item = &ColumnDef> {
        self.columns.iter().filter(|c| !c.dropped)
    }

    /// Encodes a row under this definition at its `version` (§2, §3
    /// lazy upgrade: `natts` = every attnum ever allocated).
    pub fn encode_row(&self, live: &[Option<Datum>]) -> Result<Vec<u8>, RowError> {
        encode_row(self.version, &self.columns, live)
    }

    /// Decodes a row under this definition (§3 decode rule), including
    /// the `VersionAhead` check against `self.version`.
    pub fn decode_row(&self, bytes: &[u8]) -> Result<Vec<Option<Datum>>, RowError> {
        decode_row(self.version, &self.columns, bytes)
    }

    /// The live column of `attnum`, or `InvalidDdl` if it is unknown or
    /// dropped.
    fn live_column(&self, attnum: u16) -> Result<&ColumnDef, RowError> {
        let idx = usize::from(attnum)
            .checked_sub(1)
            .ok_or(RowError::InvalidDdl("no such column"))?;
        let col = self
            .columns
            .get(idx)
            .ok_or(RowError::InvalidDdl("no such column"))?;
        if col.dropped {
            return Err(RowError::InvalidDdl("column already dropped"));
        }
        Ok(col)
    }

    /// `self` with `version + 1` (every DDL helper's version rule, §3).
    fn bumped(&self) -> Result<TableSchema, RowError> {
        let version = self
            .version
            .checked_add(1)
            .ok_or(RowError::InvalidDdl("version overflow"))?;
        Ok(TableSchema {
            version,
            ..self.clone()
        })
    }
}

/// §3: DROP COLUMN sets `dropped` and clears `not_null`, so a dropped
/// column never carries bit0. No DDL path can build the combination;
/// `encode` canonicalizes a hand-built state rather than persist it.
fn flag_bits(col: &ColumnDef) -> u8 {
    if col.dropped {
        return FLAG_DROPPED;
    }
    let mut bits = 0u8;
    if col.not_null {
        bits |= FLAG_NOT_NULL;
    }
    bits
}

/// Every pk attnum must name a live column of `columns` (which must
/// already be attnums `1..=n` in order).
fn check_pk(columns: &[ColumnDef], pk: &[u16]) -> Result<(), RowError> {
    for &attnum in pk {
        let idx = usize::from(attnum)
            .checked_sub(1)
            .ok_or(RowError::InvalidDdl("primary key names a missing attnum"))?;
        let col = columns
            .get(idx)
            .ok_or(RowError::InvalidDdl("primary key names a missing attnum"))?;
        if col.dropped {
            return Err(RowError::InvalidDdl("primary key names a dropped attnum"));
        }
    }
    Ok(())
}

/// Appends the §3.1 type bytes: one tag byte, plus the collation byte
/// for text and the element type for arrays.
fn encode_type(ty: &KeyType, out: &mut Vec<u8>) -> Result<(), RowError> {
    match ty {
        KeyType::Bool => out.push(TAG_BOOL),
        KeyType::Int2 => out.push(TAG_INT2),
        KeyType::Int4 => out.push(TAG_INT4),
        KeyType::Int8 => out.push(TAG_INT8),
        KeyType::Float4 => out.push(TAG_FLOAT4),
        KeyType::Float8 => out.push(TAG_FLOAT8),
        KeyType::Numeric => out.push(TAG_NUMERIC),
        KeyType::Text(collation) => {
            out.push(TAG_TEXT);
            match collation {
                Collation::C => out.push(COLLATION_C),
            }
        }
        KeyType::Bytea => out.push(TAG_BYTEA),
        KeyType::Date => out.push(TAG_DATE),
        KeyType::Time => out.push(TAG_TIME),
        KeyType::Timestamp => out.push(TAG_TIMESTAMP),
        KeyType::TimestampTz => out.push(TAG_TIMESTAMPTZ),
        KeyType::Interval => out.push(TAG_INTERVAL),
        KeyType::Uuid => out.push(TAG_UUID),
        KeyType::Jsonb => out.push(TAG_JSONB),
        KeyType::Array(elem) => {
            if matches!(**elem, KeyType::Array(_)) {
                return Err(RowError::InvalidDdl("array of array type"));
            }
            out.push(TAG_ARRAY);
            encode_type(elem, out)?;
        }
    }
    Ok(())
}

/// Reads the §3.1 type bytes. `depth` is the array nesting level: a
/// column's own tag is read at depth 0, an array's element at depth 1
/// — the most the format allows (an array of arrays is not a type,
/// C-Q3s §7). A `0x11` at depth ≥ 1 is rejected *before* any further
/// recursion, so however deeply corrupt bytes nest array tags, decode
/// consumes at most two frames, never one per input byte.
fn decode_type(r: &mut super::RowReader<'_>, depth: u8) -> Result<KeyType, RowError> {
    let short = || RowError::Corrupt("truncated type");
    let tag = r.u8().map_err(|_| short())?;
    Ok(match tag {
        TAG_BOOL => KeyType::Bool,
        TAG_INT2 => KeyType::Int2,
        TAG_INT4 => KeyType::Int4,
        TAG_INT8 => KeyType::Int8,
        TAG_FLOAT4 => KeyType::Float4,
        TAG_FLOAT8 => KeyType::Float8,
        TAG_NUMERIC => KeyType::Numeric,
        TAG_TEXT => match r.u8().map_err(|_| short())? {
            COLLATION_C => KeyType::Text(Collation::C),
            _ => return Err(RowError::Corrupt("unknown collation")),
        },
        TAG_BYTEA => KeyType::Bytea,
        TAG_DATE => KeyType::Date,
        TAG_TIME => KeyType::Time,
        TAG_TIMESTAMP => KeyType::Timestamp,
        TAG_TIMESTAMPTZ => KeyType::TimestampTz,
        TAG_INTERVAL => KeyType::Interval,
        TAG_UUID => KeyType::Uuid,
        TAG_JSONB => KeyType::Jsonb,
        TAG_ARRAY => {
            if depth > 0 {
                return Err(RowError::Corrupt("array of array type"));
            }
            KeyType::Array(Box::new(decode_type(r, depth + 1)?))
        }
        _ => return Err(RowError::Corrupt("unknown type tag")),
    })
}
