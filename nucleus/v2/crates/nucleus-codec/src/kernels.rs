//! Type kernels (C-Q3): the runtime comparison, equality and hash of values,
//! implemented once over the C-Q3s rules and reached through the registry
//! ([`crate::registry`]).
//!
//! # The rule
//!
//! No kernel here defines an order. Each one encodes its operands with the
//! codec's own value encoder (`encode::value`, the function `encode_key`
//! calls for a present ASC column and `hash_value` calls too) and decides on
//! the bytes. The only difference from a one-column `ASC NULLS LAST`
//! `encode_key` is its constant `0x01` "present" marker, which cannot change
//! an order or an equality between two non-NULL values; going to the value
//! encoder directly avoids deep-cloning every operand into the
//! `&[Option<Value>]` that `encode_key` takes. So, by construction:
//! - [`compare`] has the sign of `memcmp` of the ASC encodings (P-ORDER),
//! - [`eq`] is byte equality of the encodings (P-EQ), hence `compare == Equal`,
//! - [`hash`] is `hash_value`, a function of the encoding only (P-HASH), so
//!   `eq` implies equal hashes.
//!
//! A bug in a rule is therefore a codec bug with one fix site, and the codec's
//! golden corpus and properties cover the kernels too.
//!
//! # Direction and NULLs
//!
//! The per-value kernels take non-NULL values of one declared type. `DESC`
//! and `NULLS FIRST/LAST` are *key-column* concerns applied at encode time
//! (C-Q3s §2); they are not kernel parameters. For the composed case use
//! [`compare_key`], which encodes whole keys with their real columns.
//! NULL handling follows §2: for ordering and grouping (what these kernels
//! serve) NULL equals NULL and the placement is the column's. SQL
//! three-valued logic (`NULL = NULL` is NULL, `WHERE`, join predicates) is
//! the executor's business and is not modelled here. Array *elements* that
//! are NULL follow §7 (after non-NULL, fixed), inside the codec.
//!
//! # Invalid values
//!
//! The `try_*` functions return the codec's error for values that do not
//! encode (type mismatch, `time` out of range, malformed numeric digits,
//! jsonb nested deeper than the codec cap, a bad array shape). The
//! infallible [`compare`], [`eq`], [`hash`], [`in_bounds`] and
//! [`compare_key`] match the card's signatures and never panic; for an
//! invalid operand they return a fixed, documented fallback (see each
//! function). Callers that cannot guarantee validated values (binder output
//! and decoded keys are valid) should use the `try_*` forms.
//!
//! # Cost per call
//!
//! One growable `Vec<u8>` per call holding the encodings of all operands
//! (`compare`, `eq`, `in_bounds`: a single buffer, no per-operand
//! allocation, no clone of the values or the type). `hash` allocates one
//! buffer inside `hash_value`. `compare_key` likewise uses one buffer for
//! both keys. Whatever the encoder itself allocates (numeric and jsonb
//! normalisation) is on top. Nothing is cached, and nothing but `Vec` is
//! used, so the module is as `no_std`-friendly as the codec itself (which
//! today links `std`).

use std::cmp::Ordering;
use std::ops::Bound;

use crate::registry;
use crate::{encode, encode_key, hash_value, KeyColumn, KeyType, Result, Value};

/// Initial buffer capacity: covers fixed-width types and short strings
/// without a regrow.
const BUF_HINT: usize = 64;

// ---------------------------------------------------------------------------
// Fallible kernels (registry dispatch)
// ---------------------------------------------------------------------------

/// SQL-level btree comparison of two non-NULL values of declared type `ty`.
/// Equals the sign of `memcmp` of the ASC encodings (P-ORDER).
pub fn try_compare(ty: &KeyType, a: &Value, b: &Value) -> Result<Ordering> {
    (registry::ops(ty).compare)(ty, a, b)
}

/// Grouping equality (P-EQ): `a = b` iff the encodings are identical iff
/// `try_compare` is `Equal`.
pub fn try_eq(ty: &KeyType, a: &Value, b: &Value) -> Result<bool> {
    (registry::ops(ty).eq)(ty, a, b)
}

/// Engine hash (P-HASH): FNV-1a-64 of the ASC encoding, so equal values hash
/// equal.
pub fn try_hash(ty: &KeyType, v: &Value) -> Result<u64> {
    (registry::ops(ty).hash)(ty, v)
}

/// Whether `v` lies within `[lo, hi]` as index-scan bounds, decided by
/// comparing encodings: `Included` admits equality, `Excluded` does not,
/// `Unbounded` admits everything on that side. Bounds that cross (`lo > hi`)
/// simply admit nothing. All three operands must be valid values of `ty`
/// (an `Unbounded` side is not encoded).
pub fn try_in_bounds(
    ty: &KeyType,
    v: &Value,
    lo: Bound<&Value>,
    hi: Bound<&Value>,
) -> Result<bool> {
    let mut buf = Vec::with_capacity(BUF_HINT);
    encode::value(ty, v, &mut buf)?;
    let v_end = buf.len();
    let lo_end = encode_bound(ty, lo, &mut buf)?;
    encode_bound(ty, hi, &mut buf)?;
    let (key, rest) = buf.split_at(v_end);
    let (lo_bytes, hi_bytes) = rest.split_at(lo_end - v_end);
    let lo_ok = match lo {
        Bound::Unbounded => true,
        Bound::Included(_) => key >= lo_bytes,
        Bound::Excluded(_) => key > lo_bytes,
    };
    let hi_ok = match hi {
        Bound::Unbounded => true,
        Bound::Included(_) => key <= hi_bytes,
        Bound::Excluded(_) => key < hi_bytes,
    };
    Ok(lo_ok && hi_ok)
}

/// Comparison of two whole keys over their real columns (direction and NULL
/// placement included, §2/§3): the sign of `memcmp` of the two
/// `encode_key` outputs. This is the composed counterpart of
/// [`try_compare`]; ORDER BY and multi-column index order use it.
pub fn try_compare_key(
    cols: &[KeyColumn],
    a: &[Option<Value>],
    b: &[Option<Value>],
) -> Result<Ordering> {
    let mut buf = Vec::with_capacity(2 * BUF_HINT);
    encode_key(cols, a, &mut buf)?;
    let split = buf.len();
    encode_key(cols, b, &mut buf)?;
    let (ka, kb) = buf.split_at(split);
    Ok(ka.cmp(kb))
}

// ---------------------------------------------------------------------------
// Infallible forms (the card's signatures)
// ---------------------------------------------------------------------------

/// [`try_compare`] for validated values. An invalid operand sorts after every
/// valid one, and two invalid operands compare `Equal`, which keeps a sort
/// over mixed input total and panic-free. This is a safety net, not a rule:
/// it is not derived from C-Q3s.
pub fn compare(ty: &KeyType, a: &Value, b: &Value) -> Ordering {
    try_compare(ty, a, b).unwrap_or_else(|_| invalid_order(ty, a, b))
}

/// [`try_eq`] for validated values. If either operand is invalid the result
/// is `false`: an invalid value is never equal to anything (so, unlike
/// [`compare`], two invalid operands are not grouped together).
pub fn eq(ty: &KeyType, a: &Value, b: &Value) -> bool {
    try_eq(ty, a, b).unwrap_or(false)
}

/// [`try_hash`] for validated values. An invalid value hashes to `0`; it is
/// never `eq` to anything, so P-HASH is unaffected.
pub fn hash(ty: &KeyType, v: &Value) -> u64 {
    try_hash(ty, v).unwrap_or(0)
}

/// [`try_in_bounds`] for validated values; an invalid operand is in no range
/// (`false`).
pub fn in_bounds(ty: &KeyType, v: &Value, lo: Bound<&Value>, hi: Bound<&Value>) -> bool {
    try_in_bounds(ty, v, lo, hi).unwrap_or(false)
}

/// [`try_compare_key`] with the same invalid-operand fallback as [`compare`]
/// (a key that does not encode sorts last; two such keys are `Equal`).
pub fn compare_key(cols: &[KeyColumn], a: &[Option<Value>], b: &[Option<Value>]) -> Ordering {
    try_compare_key(cols, a, b).unwrap_or_else(|_| {
        let ok = |k: &[Option<Value>]| encode_key(cols, k, &mut Vec::new()).is_ok();
        order_invalid_last(ok(a), ok(b))
    })
}

fn invalid_order(ty: &KeyType, a: &Value, b: &Value) -> Ordering {
    let ok = |v: &Value| encode::value(ty, v, &mut Vec::new()).is_ok();
    order_invalid_last(ok(a), ok(b))
}

fn order_invalid_last(a_valid: bool, b_valid: bool) -> Ordering {
    match (a_valid, b_valid) {
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        _ => Ordering::Equal,
    }
}

// ---------------------------------------------------------------------------
// Encoding-level primitives behind every registry slot
// ---------------------------------------------------------------------------

/// Appends the encoding of a bound's value (if any); returns the buffer
/// length afterwards.
fn encode_bound(ty: &KeyType, b: Bound<&Value>, out: &mut Vec<u8>) -> Result<usize> {
    if let Bound::Included(x) | Bound::Excluded(x) = b {
        encode::value(ty, x, out)?;
    }
    Ok(out.len())
}

/// Both operands in one buffer; returns it with the split point.
fn encode_pair(ty: &KeyType, a: &Value, b: &Value) -> Result<(Vec<u8>, usize)> {
    let mut buf = Vec::with_capacity(2 * BUF_HINT);
    encode::value(ty, a, &mut buf)?;
    let split = buf.len();
    encode::value(ty, b, &mut buf)?;
    Ok((buf, split))
}

/// P-ORDER: `memcmp` of the two ASC encodings.
pub(crate) fn encoded_compare(ty: &KeyType, a: &Value, b: &Value) -> Result<Ordering> {
    let (buf, split) = encode_pair(ty, a, b)?;
    let (ka, kb) = buf.split_at(split);
    Ok(ka.cmp(kb))
}

/// P-EQ: byte equality of the two ASC encodings.
pub(crate) fn encoded_eq(ty: &KeyType, a: &Value, b: &Value) -> Result<bool> {
    let (buf, split) = encode_pair(ty, a, b)?;
    let (ka, kb) = buf.split_at(split);
    Ok(ka == kb)
}

/// P-HASH: the codec's hash of the ASC encoding.
pub(crate) fn encoded_hash(ty: &KeyType, v: &Value) -> Result<u64> {
    hash_value(ty, v)
}
