//! Row byte format (C-T6 §2): the payload of a live `/t/` version.
//!
//! ```text
//! row     = 0x52 0x01 be32(schema_version) be16(natts) nullmap datum*
//! nullmap = ceil(natts/8) bytes; attnum a is bit (a-1) % 8 (LSB first) of
//!           byte (a-1) / 8; 1 = NULL; bits past natts are 0.
//! datum   = for each attnum 1..=natts whose bit is 0, in attnum order:
//!           0x00 be32(len) value_bytes            inline
//!         | 0x01 toast_pointer                    out of line (§5)
//! toast_pointer = toast_id(20) be32(raw_len) be32(chunk_count)
//! ```
//!
//! `natts` counts every attnum allocated in the definition the row was
//! written under, dropped included; dropped attnums are written NULL.
//! Value bytes are lossless (§2.1, [`value`]): unlike the key encoding
//! (C-Q3s §8.1) they round-trip value representations. Trailing bytes,
//! short datums, unknown tags, undecodable values, or a NULL bit on a
//! live NOT NULL attnum `<= natts` are an error, never a panic (XX001).
//! The encoded table definition around [`ColumnDef`] (§3) is C-T6a2; the
//! decode rule implemented here is §3's, against a plain column list.

mod value;

pub use self::value::{decode_value, encode_value, ValueError};

use crate::value::{Jsonb, KeyType, Value};

/// One column of a table definition (C-T6 §3). The encoded `def` (and the
/// DDL rules that maintain it) are C-T6a2; rows are decoded against this
/// minimal list.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    /// 1-based; attnums are never reused, so a list is `1..=n` in order.
    pub attnum: u16,
    pub name: String,
    pub ty: KeyType,
    pub not_null: bool,
    pub dropped: bool,
    /// `ADD COLUMN` default, evaluated once at DDL time; served for rows
    /// written before the attnum existed (§3 decode rule).
    pub missing: Option<Value>,
}

/// A datum stored out of line (§5). `id` is the 20-byte toast id
/// `be32(epoch) be64(txn n) be32(seq0) be32(ord)`; chunks are read
/// separately by the storage layer, never decoded here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToastPointer {
    pub id: [u8; 20],
    /// Total value-bytes length of the chunks.
    pub raw_len: u32,
    /// Chunk count; chunk `i` is `8192` bytes, the last shorter.
    pub chunks: u32,
}

/// A column value as stored in a row.
#[derive(Debug, Clone, PartialEq)]
pub enum Datum {
    Inline(Value),
    Toast(ToastPointer),
}

/// A schema-less datum as returned by [`decode_row_raw`]: the undecoded
/// value bytes, or a toast pointer. Used to find old toast pointers (§5)
/// without a definition.
#[derive(Debug, Clone, PartialEq)]
pub enum RawDatum {
    Inline(Vec<u8>),
    Toast(ToastPointer),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowHeader {
    pub schema_version: u32,
    pub natts: u16,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RowError {
    #[error("corrupt row: {0}")]
    Corrupt(&'static str),
    #[error("column {attnum}: value does not match the column type")]
    TypeMismatch { attnum: u16 },
    #[error("column {attnum} is NOT NULL")]
    NotNull { attnum: u16 },
    #[error("{expected} live columns, {got} values")]
    Arity { expected: usize, got: usize },
    #[error("row schema version {row} is ahead of definition version {schema}")]
    VersionAhead { row: u32, schema: u32 },
    #[error("invalid definition: {0}")]
    InvalidDdl(&'static str),
}

const ROW_MAGIC: [u8; 2] = [0x52, 0x01];
const HEADER_LEN: usize = 8;
const TAG_INLINE: u8 = 0x00;
const TAG_TOAST: u8 = 0x01;
const TOAST_PTR_LEN: usize = 28;

/// ceil(n/8): the nullmap length for `n` positions (§2; §2.1 arrays).
fn nullmap_len(n: usize) -> usize {
    n.div_ceil(8)
}

/// Sets the null bit of 0-based position `i` (LSB first, §2).
fn set_null_bit(map: &mut [u8], i: usize) {
    map[i / 8] |= 1 << (i % 8);
}

/// The null bit of 0-based position `i` (LSB first, §2).
fn is_null_bit(map: &[u8], i: usize) -> bool {
    map[i / 8] & (1 << (i % 8)) != 0
}

/// Encodes one row (§2). `live` holds one entry per non-dropped column, in
/// attnum order; dropped attnums are written NULL. `natts` written is
/// `cols.len()`: every attnum the definition has ever allocated.
pub fn encode_row(
    version: u32,
    cols: &[ColumnDef],
    live: &[Option<Datum>],
) -> Result<Vec<u8>, RowError> {
    check_attnums(cols)?;
    let natts = u16::try_from(cols.len()).map_err(|_| RowError::InvalidDdl("too many columns"))?;
    let n_live = cols.iter().filter(|c| !c.dropped).count();
    if live.len() != n_live {
        return Err(RowError::Arity {
            expected: n_live,
            got: live.len(),
        });
    }
    let mut out = Vec::with_capacity(HEADER_LEN + nullmap_len(cols.len()));
    out.extend_from_slice(&ROW_MAGIC);
    out.extend_from_slice(&version.to_be_bytes());
    out.extend_from_slice(&natts.to_be_bytes());
    // Nullmap pass: a NULL entry for a live NOT NULL column is refused
    // before any datum byte is written.
    let mut nullmap = vec![0u8; nullmap_len(cols.len())];
    let mut live_at = 0usize;
    for col in cols {
        if col.dropped {
            set_null_bit(&mut nullmap, usize::from(col.attnum) - 1);
            continue;
        }
        if live[live_at].is_none() {
            if col.not_null {
                return Err(RowError::NotNull { attnum: col.attnum });
            }
            set_null_bit(&mut nullmap, usize::from(col.attnum) - 1);
        }
        live_at += 1;
    }
    out.extend_from_slice(&nullmap);
    // Datum pass, attnum order, only for attnums whose bit is clear.
    let mut value_bytes = Vec::new();
    let mut live_at = 0usize;
    for col in cols {
        if col.dropped {
            continue;
        }
        if let Some(datum) = &live[live_at] {
            match datum {
                Datum::Inline(v) => {
                    value_bytes.clear();
                    encode_value(&col.ty, v, &mut value_bytes)
                        .map_err(|e| value_error(col.attnum, e))?;
                    let len = u32::try_from(value_bytes.len())
                        .map_err(|_| RowError::Corrupt("datum value too large"))?;
                    out.push(TAG_INLINE);
                    out.extend_from_slice(&len.to_be_bytes());
                    out.extend_from_slice(&value_bytes);
                }
                Datum::Toast(p) => {
                    out.push(TAG_TOAST);
                    out.extend_from_slice(&p.id);
                    out.extend_from_slice(&p.raw_len.to_be_bytes());
                    out.extend_from_slice(&p.chunks.to_be_bytes());
                }
            }
        }
        live_at += 1;
    }
    Ok(out)
}

/// Parses the row header (§2). The nullmap and datums are not touched.
pub fn row_header(bytes: &[u8]) -> Result<RowHeader, RowError> {
    let mut r = RowReader::new(bytes);
    header(&mut r)
}

/// Decodes one row under a definition at `version` (§3 decode rule):
/// `v > V` → [`RowError::VersionAhead`], row `natts` above the definition
/// → [`RowError::Corrupt`]; then, for each live attnum in order: the row's
/// datum or NULL when the attnum fits in `natts`, `missing` (else NULL)
/// when it does not. Dropped attnums are skipped (their bytes, if any,
/// stay consumed). One entry per live column is returned, mirroring
/// `encode_row`'s `live`.
pub fn decode_row(
    version: u32,
    cols: &[ColumnDef],
    bytes: &[u8],
) -> Result<Vec<Option<Datum>>, RowError> {
    check_attnums(cols)?;
    let mut r = RowReader::new(bytes);
    let header = header(&mut r)?;
    if header.schema_version > version {
        return Err(RowError::VersionAhead {
            row: header.schema_version,
            schema: version,
        });
    }
    let natts = usize::from(header.natts);
    if natts > cols.len() {
        return Err(RowError::Corrupt("row natts exceeds the definition"));
    }
    let nullmap = r
        .take(nullmap_len(natts))
        .map_err(|_| RowError::Corrupt("truncated nullmap"))?;
    let mut out = Vec::with_capacity(cols.iter().filter(|c| !c.dropped).count());
    for col in cols {
        let attnum = usize::from(col.attnum);
        if col.dropped {
            // Not returned, but a stray datum (a corrupt row: the writer
            // NULLs dropped attnums) is still consumed to keep position.
            if attnum <= natts && !is_null_bit(nullmap, attnum - 1) {
                skip_datum(&mut r)?;
            }
            continue;
        }
        if attnum > natts {
            out.push(col.missing.clone().map(Datum::Inline));
        } else if is_null_bit(nullmap, attnum - 1) {
            if col.not_null {
                return Err(RowError::Corrupt("NULL bit on a NOT NULL column"));
            }
            out.push(None);
        } else {
            out.push(Some(read_datum(col.attnum, &col.ty, &mut r)?));
        }
    }
    end_of_row(&r)?;
    Ok(out)
}

/// Decodes a row without a schema: every attnum `<= natts`, dropped
/// included, value bytes left raw (§3; §5 reads old toast pointers this
/// way). Position `i - 1` of the result is attnum `i`.
pub fn decode_row_raw(bytes: &[u8]) -> Result<(RowHeader, Vec<Option<RawDatum>>), RowError> {
    let mut r = RowReader::new(bytes);
    let header = header(&mut r)?;
    let natts = usize::from(header.natts);
    let nullmap = r
        .take(nullmap_len(natts))
        .map_err(|_| RowError::Corrupt("truncated nullmap"))?;
    let mut out = Vec::with_capacity(natts);
    for attnum in 1..=natts {
        if is_null_bit(nullmap, attnum - 1) {
            out.push(None);
            continue;
        }
        match r.u8().map_err(|_| RowError::Corrupt("truncated datum"))? {
            TAG_INLINE => {
                let len = r
                    .u32be()
                    .map_err(|_| RowError::Corrupt("truncated datum"))?;
                let bytes = r
                    .take(len as usize)
                    .map_err(|_| RowError::Corrupt("truncated datum"))?;
                out.push(Some(RawDatum::Inline(bytes.to_vec())));
            }
            TAG_TOAST => out.push(Some(RawDatum::Toast(read_toast(&mut r)?))),
            _ => return Err(RowError::Corrupt("unknown datum tag")),
        }
    }
    end_of_row(&r)?;
    Ok((header, out))
}

/// Representation equality (C-Q3s §8.1): `==` on [`Value`], except that
/// floats compare by `to_bits` (so `-0`, `0` and NaN payloads stay
/// distinct) and containers recurse. This is the round-trip oracle of the
/// lossless row bytes: `decode_value` returns exactly what was encoded.
pub fn value_repr_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Float4(x), Value::Float4(y)) => x.to_bits() == y.to_bits(),
        (Value::Float8(x), Value::Float8(y)) => x.to_bits() == y.to_bits(),
        (Value::Array(x), Value::Array(y)) => {
            x.dims == y.dims
                && x.elems.len() == y.elems.len()
                && x.elems
                    .iter()
                    .zip(&y.elems)
                    .all(|(a, b)| opt_repr_eq(a.as_ref(), b.as_ref()))
        }
        (Value::Jsonb(x), Value::Jsonb(y)) => jsonb_repr_eq(x, y),
        _ => a == b,
    }
}

fn opt_repr_eq(a: Option<&Value>, b: Option<&Value>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => value_repr_eq(a, b),
        _ => false,
    }
}

fn jsonb_repr_eq(a: &Jsonb, b: &Jsonb) -> bool {
    match (a, b) {
        (Jsonb::Array(x), Jsonb::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(a, b)| jsonb_repr_eq(a, b))
        }
        (Jsonb::Object(x), Jsonb::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .zip(y)
                    .all(|((ka, va), (kb, vb))| ka == kb && jsonb_repr_eq(va, vb))
        }
        _ => a == b,
    }
}

fn value_error(attnum: u16, e: ValueError) -> RowError {
    match e {
        ValueError::TypeMismatch => RowError::TypeMismatch { attnum },
        ValueError::Corrupt(msg) => RowError::Corrupt(msg),
    }
}

fn check_attnums(cols: &[ColumnDef]) -> Result<(), RowError> {
    for (i, col) in cols.iter().enumerate() {
        if u64::from(col.attnum) != i as u64 + 1 {
            return Err(RowError::InvalidDdl("attnums must be 1..=n in order"));
        }
    }
    Ok(())
}

fn header(r: &mut RowReader<'_>) -> Result<RowHeader, RowError> {
    let short = RowError::Corrupt("truncated row header");
    let magic = r.take_array::<2>().map_err(|_| short)?;
    if magic != ROW_MAGIC {
        return Err(RowError::Corrupt("row magic"));
    }
    let schema_version = r.u32be().map_err(|_| RowError::Corrupt("truncated row header"))?;
    let natts = r.u16be().map_err(|_| RowError::Corrupt("truncated row header"))?;
    Ok(RowHeader { schema_version, natts })
}

fn read_datum(attnum: u16, ty: &KeyType, r: &mut RowReader<'_>) -> Result<Datum, RowError> {
    match r.u8().map_err(|_| RowError::Corrupt("truncated datum"))? {
        TAG_INLINE => {
            let len = r
                .u32be()
                .map_err(|_| RowError::Corrupt("truncated datum"))?;
            let bytes = r
                .take(len as usize)
                .map_err(|_| RowError::Corrupt("truncated datum"))?;
            Ok(Datum::Inline(
                decode_value(ty, bytes).map_err(|e| value_error(attnum, e))?,
            ))
        }
        TAG_TOAST => Ok(Datum::Toast(read_toast(r)?)),
        _ => Err(RowError::Corrupt("unknown datum tag")),
    }
}

fn read_toast(r: &mut RowReader<'_>) -> Result<ToastPointer, RowError> {
    let short = |_| RowError::Corrupt("truncated toast pointer");
    let id = r.take_array::<20>().map_err(short)?;
    let raw_len = r.u32be().map_err(short)?;
    let chunks = r.u32be().map_err(short)?;
    Ok(ToastPointer { id, raw_len, chunks })
}

fn skip_datum(r: &mut RowReader<'_>) -> Result<(), RowError> {
    match r.u8().map_err(|_| RowError::Corrupt("truncated datum"))? {
        TAG_INLINE => {
            let len = r
                .u32be()
                .map_err(|_| RowError::Corrupt("truncated datum"))?;
            r.take(len as usize)
                .map_err(|_| RowError::Corrupt("truncated datum"))?;
            Ok(())
        }
        TAG_TOAST => r
            .take(TOAST_PTR_LEN)
            .map_err(|_| RowError::Corrupt("truncated toast pointer"))
            .map(|_| ()),
        _ => Err(RowError::Corrupt("unknown datum tag")),
    }
}

fn end_of_row(r: &RowReader<'_>) -> Result<(), RowError> {
    if r.done() {
        Ok(())
    } else {
        Err(RowError::Corrupt("trailing bytes after row"))
    }
}

struct RowReader<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> RowReader<'a> {
    fn new(b: &'a [u8]) -> RowReader<'a> {
        RowReader { b, pos: 0 }
    }
    fn done(&self) -> bool {
        self.pos == self.b.len()
    }
    fn u8(&mut self) -> Result<u8, RowError> {
        let &b = self.b.get(self.pos).ok_or(RowError::Corrupt("truncated row"))?;
        self.pos += 1;
        Ok(b)
    }
    fn u16be(&mut self) -> Result<u16, RowError> {
        Ok(u16::from_be_bytes(self.take_array()?))
    }
    fn u32be(&mut self) -> Result<u32, RowError> {
        Ok(u32::from_be_bytes(self.take_array()?))
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], RowError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or(RowError::Corrupt("truncated row"))?;
        let s = self
            .b
            .get(self.pos..end)
            .ok_or(RowError::Corrupt("truncated row"))?;
        self.pos = end;
        Ok(s)
    }
    fn take_array<const N: usize>(&mut self) -> Result<[u8; N], RowError> {
        let s = self.take(N)?;
        s.try_into().map_err(|_| RowError::Corrupt("truncated row"))
    }
}
