//! nucleus-codec: memcomparable key encoding (C-K1). Normative spec:
//! `docs/C-Q3s-types-ordering.md`; section numbers below refer to it.
//!
//! `memcmp` of two encoded keys equals the SQL order of the keys (P-ORDER),
//! equal keys encode to identical bytes (P-EQ), and every encoding is
//! prefix-free (P-PREFIX). Any change to the bytes produced here fails the
//! golden corpus and needs a format version bump (C-K4).

mod decode;
mod encode;
pub mod kernels;
pub mod registry;
mod value;

pub use kernels::{
    compare, compare_key, eq, hash, in_bounds, try_compare, try_compare_key, try_eq, try_hash,
    try_in_bounds,
};
pub use registry::{ops, OpOid, TypeKind, TypeOps, OP_BTREE_CMP, OP_EQ, OP_HASH};
pub use value::{Array, ArrayDim, Collation, Decimal, Interval, Jsonb, KeyType, Numeric, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Nulls {
    First,
    Last,
}

/// One key column (§2).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KeyColumn {
    pub ty: KeyType,
    pub dir: Direction,
    pub nulls: Nulls,
}

impl KeyColumn {
    /// `ASC NULLS LAST`, PostgreSQL's default.
    pub fn asc(ty: KeyType) -> KeyColumn {
        KeyColumn {
            ty,
            dir: Direction::Asc,
            nulls: Nulls::Last,
        }
    }
    /// `DESC NULLS FIRST`, PostgreSQL's default for DESC.
    pub fn desc(ty: KeyType) -> KeyColumn {
        KeyColumn {
            ty,
            dir: Direction::Desc,
            nulls: Nulls::First,
        }
    }
    pub fn with_nulls(self, nulls: Nulls) -> KeyColumn {
        KeyColumn { nulls, ..self }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CodecError {
    #[error("{expected} key columns, {got} values")]
    Arity { expected: usize, got: usize },
    #[error("value does not match the key type")]
    TypeMismatch,
    #[error("invalid value: {0}")]
    InvalidValue(&'static str),
    #[error("key truncated")]
    Truncated,
    #[error("malformed key: {0}")]
    Malformed(&'static str),
    #[error("{0} trailing bytes after key")]
    Trailing(usize),
}

pub type Result<T> = std::result::Result<T, CodecError>;

/// Column marker bytes (§2).
const NULL_FIRST: u8 = 0x00;
const PRESENT: u8 = 0x01;
const NULL_LAST: u8 = 0x02;

/// Appends the key for `vals` (`None` = NULL) to `out`. On error `out` is
/// left with a partial key; callers discard it.
pub fn encode_key(cols: &[KeyColumn], vals: &[Option<Value>], out: &mut Vec<u8>) -> Result<()> {
    if cols.len() != vals.len() {
        return Err(CodecError::Arity {
            expected: cols.len(),
            got: vals.len(),
        });
    }
    for (col, val) in cols.iter().zip(vals) {
        match val {
            None => out.push(match col.nulls {
                Nulls::First => NULL_FIRST,
                Nulls::Last => NULL_LAST,
            }),
            Some(v) => {
                out.push(PRESENT);
                let start = out.len();
                encode::value(&col.ty, v, out)?;
                if col.dir == Direction::Desc {
                    out[start..].iter_mut().for_each(|b| *b = !*b);
                }
            }
        }
    }
    Ok(())
}

/// Decodes a whole key. Values come back in canonical form (§8.1).
pub fn decode_key(cols: &[KeyColumn], bytes: &[u8]) -> Result<Vec<Option<Value>>> {
    let (vals, used) = decode_key_prefix(cols, bytes)?;
    match bytes.len() - used {
        0 => Ok(vals),
        n => Err(CodecError::Trailing(n)),
    }
}

/// Decodes the key at the start of `bytes`; returns it and the bytes used,
/// so a suffix (such as a commit timestamp) can follow.
pub fn decode_key_prefix(cols: &[KeyColumn], bytes: &[u8]) -> Result<(Vec<Option<Value>>, usize)> {
    let mut r = decode::Reader::new(bytes);
    let mut vals = Vec::with_capacity(cols.len());
    for col in cols {
        r.set_mask(0);
        let marker = r.u8()?;
        let null = match col.nulls {
            Nulls::First => NULL_FIRST,
            Nulls::Last => NULL_LAST,
        };
        if marker == null {
            vals.push(None);
            continue;
        }
        if marker != PRESENT {
            return Err(CodecError::Malformed("column marker"));
        }
        r.set_mask(if col.dir == Direction::Desc { 0xFF } else { 0 });
        vals.push(Some(decode::value(&col.ty, &mut r)?));
    }
    Ok((vals, r.pos()))
}

/// Hash consistent with SQL equality (§8): FNV-1a-64 of the ASC encoding.
pub fn hash_value(ty: &KeyType, v: &Value) -> Result<u64> {
    let mut buf = Vec::new();
    encode::value(ty, v, &mut buf)?;
    Ok(buf.iter().fold(0xcbf2_9ce4_8422_2325_u64, |h, &b| {
        (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3)
    }))
}
