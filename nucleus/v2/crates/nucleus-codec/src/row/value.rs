//! Lossless per-type value bytes (C-T6 §2.1). Unlike the key encoding
//! (C-Q3s §4-§7) these round-trip value representations exactly: float
//! zero signs and NaN payloads, numeric display scale, the interval
//! month/day/time split, jsonb input key order and duplicate keys, array
//! dimensions. Canonicalisation is the type input function's job; a row
//! stores what it was given.

use super::{is_null_bit, nullmap_len, padding_bits_clear, set_null_bit};
use crate::encode::{check_dims, MAX_ARRAY_DIMS, MAX_JSONB_DEPTH};
use crate::value::{Array, ArrayDim, Decimal, Interval, Jsonb, KeyType, Numeric, Value};
use crate::CodecError;

/// Value-level failure of the §2.1 bytes: either the value's variant does
/// not match the declared type (the row level adds the attnum,
/// `RowError::TypeMismatch`), or the value / byte sequence cannot be
/// encoded or decoded at all (row level: `RowError::Corrupt`, XX001).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ValueError {
    #[error("value does not match the column type")]
    TypeMismatch,
    #[error("invalid value bytes: {0}")]
    Corrupt(&'static str),
}

/// §2.1 numeric tags. These are not the key-format tags (C-Q3s §4.2).
const NUM_NAN: u8 = 0x00;
const NUM_POS_INF: u8 = 0x01;
const NUM_NEG_INF: u8 = 0x02;
const NUM_FINITE: u8 = 0x03;

/// §2.1 jsonb tags.
const J_NULL: u8 = 0x00;
const J_FALSE: u8 = 0x01;
const J_TRUE: u8 = 0x02;
const J_NUMBER: u8 = 0x03;
const J_STRING: u8 = 0x04;
const J_ARRAY: u8 = 0x05;
const J_OBJECT: u8 = 0x06;

/// Appends the value bytes of `v` under `ty` (§2.1).
pub fn encode_value(ty: &KeyType, v: &Value, out: &mut Vec<u8>) -> Result<(), ValueError> {
    match (ty, v) {
        (KeyType::Bool, Value::Bool(b)) => out.push(u8::from(*b)),
        (KeyType::Int2, Value::Int2(x)) => out.extend_from_slice(&x.to_be_bytes()),
        (KeyType::Int4, Value::Int4(x)) | (KeyType::Date, Value::Date(x)) => {
            out.extend_from_slice(&x.to_be_bytes());
        }
        (KeyType::Int8, Value::Int8(x))
        | (KeyType::Time, Value::Time(x))
        | (KeyType::Timestamp, Value::Timestamp(x))
        | (KeyType::TimestampTz, Value::TimestampTz(x)) => {
            out.extend_from_slice(&x.to_be_bytes());
        }
        (KeyType::Float4, Value::Float4(x)) => out.extend_from_slice(&x.to_bits().to_be_bytes()),
        (KeyType::Float8, Value::Float8(x)) => out.extend_from_slice(&x.to_bits().to_be_bytes()),
        (KeyType::Numeric, Value::Numeric(n)) => numeric(n, out)?,
        (KeyType::Text(_), Value::Text(s)) => out.extend_from_slice(s.as_bytes()),
        (KeyType::Bytea, Value::Bytea(b)) => out.extend_from_slice(b),
        (KeyType::Interval, Value::Interval(i)) => interval(i, out),
        (KeyType::Uuid, Value::Uuid(u)) => out.extend_from_slice(u),
        (KeyType::Jsonb, Value::Jsonb(j)) => jsonb(j, out)?,
        (KeyType::Array(elem), Value::Array(a)) => array(elem, a, out)?,
        _ => return Err(ValueError::TypeMismatch),
    }
    Ok(())
}

/// Decodes a whole value: `bytes` is exactly one value's bytes (the datum
/// header or the array element prefix carries the length), so any leftover
/// is an error.
pub fn decode_value(ty: &KeyType, bytes: &[u8]) -> Result<Value, ValueError> {
    let mut r = ValueReader::new(bytes);
    let v = decode(ty, &mut r)?;
    if !r.done() {
        return Err(ValueError::Corrupt("trailing bytes in value"));
    }
    Ok(v)
}

// ---- encode ----

fn interval(i: &Interval, out: &mut Vec<u8>) {
    out.extend_from_slice(&i.months.to_be_bytes());
    out.extend_from_slice(&i.days.to_be_bytes());
    out.extend_from_slice(&i.micros.to_be_bytes());
}

fn numeric(n: &Numeric, out: &mut Vec<u8>) -> Result<(), ValueError> {
    match n {
        Numeric::NaN => out.push(NUM_NAN),
        Numeric::PosInf => out.push(NUM_POS_INF),
        Numeric::NegInf => out.push(NUM_NEG_INF),
        Numeric::Finite(d) => {
            out.push(NUM_FINITE);
            finite_numeric(d, out)?;
        }
    }
    Ok(())
}

/// `neg(1) be32(scale i32) be32(ndigits) digit(1)*`: the digits as stored,
/// display scale and leading/trailing zeros included — the representation,
/// not the normalised value.
fn finite_numeric(d: &Decimal, out: &mut Vec<u8>) -> Result<(), ValueError> {
    out.push(u8::from(d.negative));
    out.extend_from_slice(&d.scale.to_be_bytes());
    let ndigits = u32::try_from(d.digits.len())
        .map_err(|_| ValueError::Corrupt("too many numeric digits"))?;
    out.extend_from_slice(&ndigits.to_be_bytes());
    for &digit in &d.digits {
        if digit > 9 {
            return Err(ValueError::Corrupt("numeric digit out of range"));
        }
        out.push(digit);
    }
    Ok(())
}

fn jsonb(j: &Jsonb, out: &mut Vec<u8>) -> Result<(), ValueError> {
    jsonb_value(j, out, 0)
}

fn jsonb_value(j: &Jsonb, out: &mut Vec<u8>, depth: usize) -> Result<(), ValueError> {
    if depth > MAX_JSONB_DEPTH {
        return Err(ValueError::Corrupt("jsonb nesting too deep"));
    }
    match j {
        Jsonb::Null => out.push(J_NULL),
        Jsonb::Bool(false) => out.push(J_FALSE),
        Jsonb::Bool(true) => out.push(J_TRUE),
        Jsonb::Number(n) => {
            let Numeric::Finite(d) = n else {
                return Err(ValueError::Corrupt("jsonb number must be finite"));
            };
            out.push(J_NUMBER);
            finite_numeric(d, out)?;
        }
        Jsonb::String(s) => {
            out.push(J_STRING);
            len32(s.len(), out)?;
            out.extend_from_slice(s.as_bytes());
        }
        Jsonb::Array(items) => {
            out.push(J_ARRAY);
            len32(items.len(), out)?;
            for item in items {
                jsonb_value(item, out, depth + 1)?;
            }
        }
        // Input order and duplicate keys as given: jsonb input semantics
        // (dedup, storage order) belong to the key codec, not the row.
        Jsonb::Object(pairs) => {
            out.push(J_OBJECT);
            len32(pairs.len(), out)?;
            for (k, v) in pairs {
                len32(k.len(), out)?;
                out.extend_from_slice(k.as_bytes());
                jsonb_value(v, out, depth + 1)?;
            }
        }
    }
    Ok(())
}

fn len32(n: usize, out: &mut Vec<u8>) -> Result<(), ValueError> {
    let n = u32::try_from(n).map_err(|_| ValueError::Corrupt("jsonb length out of range"))?;
    out.extend_from_slice(&n.to_be_bytes());
    Ok(())
}

/// `u8 ndims (be32 len, be32 lower)* be32 nelems nullmap(nelems)
/// (be32 len elem_bytes)*`: one length-prefixed entry per non-NULL
/// element, in row-major order.
fn array(elem: &KeyType, a: &Array, out: &mut Vec<u8>) -> Result<(), ValueError> {
    if matches!(elem, KeyType::Array(_)) {
        return Err(ValueError::Corrupt("array of array type"));
    }
    // Shape is structure: the same rule the key codec enforces
    // (C-Q3s §7), so an unshapely array is refused before any byte.
    check_dims(a).map_err(shape_error)?;
    let ndims =
        u8::try_from(a.dims.len()).map_err(|_| ValueError::Corrupt("too many array dimensions"))?;
    out.push(ndims);
    for d in &a.dims {
        out.extend_from_slice(&d.len.to_be_bytes());
        out.extend_from_slice(&d.lower.to_be_bytes());
    }
    let nelems =
        u32::try_from(a.elems.len()).map_err(|_| ValueError::Corrupt("too many array elements"))?;
    out.extend_from_slice(&nelems.to_be_bytes());
    let mut nullmap = vec![0u8; nullmap_len(a.elems.len())];
    for (i, e) in a.elems.iter().enumerate() {
        if e.is_none() {
            set_null_bit(&mut nullmap, i);
        }
    }
    out.extend_from_slice(&nullmap);
    let mut elem_bytes = Vec::new();
    for v in a.elems.iter().flatten() {
        elem_bytes.clear();
        encode_value(elem, v, &mut elem_bytes)?;
        let len = u32::try_from(elem_bytes.len())
            .map_err(|_| ValueError::Corrupt("array element too large"))?;
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(&elem_bytes);
    }
    Ok(())
}

// ---- decode ----

fn decode(ty: &KeyType, r: &mut ValueReader<'_>) -> Result<Value, ValueError> {
    Ok(match ty {
        KeyType::Bool => match r.take(1)? {
            [0] => Value::Bool(false),
            [1] => Value::Bool(true),
            _ => return Err(ValueError::Corrupt("bool")),
        },
        KeyType::Int2 => Value::Int2(i16::from_be_bytes(r.take_array()?)),
        KeyType::Int4 => Value::Int4(i32::from_be_bytes(r.take_array()?)),
        KeyType::Int8 => Value::Int8(i64::from_be_bytes(r.take_array()?)),
        KeyType::Date => Value::Date(i32::from_be_bytes(r.take_array()?)),
        KeyType::Time => Value::Time(i64::from_be_bytes(r.take_array()?)),
        KeyType::Timestamp => Value::Timestamp(i64::from_be_bytes(r.take_array()?)),
        KeyType::TimestampTz => Value::TimestampTz(i64::from_be_bytes(r.take_array()?)),
        KeyType::Float4 => Value::Float4(f32::from_bits(u32::from_be_bytes(r.take_array()?))),
        KeyType::Float8 => Value::Float8(f64::from_bits(u64::from_be_bytes(r.take_array()?))),
        KeyType::Numeric => Value::Numeric(numeric_dec(r)?),
        KeyType::Text(_) => Value::Text(r.utf8_rest()?),
        KeyType::Bytea => Value::Bytea(r.rest().to_vec()),
        KeyType::Interval => Value::Interval(interval_dec(r)?),
        KeyType::Uuid => Value::Uuid(r.take_array()?),
        KeyType::Jsonb => Value::Jsonb(jsonb_dec(r, 0)?),
        KeyType::Array(elem) => Value::Array(array_dec(elem, r)?),
    })
}

fn numeric_dec(r: &mut ValueReader<'_>) -> Result<Numeric, ValueError> {
    match r.u8()? {
        NUM_NAN => Ok(Numeric::NaN),
        NUM_POS_INF => Ok(Numeric::PosInf),
        NUM_NEG_INF => Ok(Numeric::NegInf),
        NUM_FINITE => Ok(Numeric::Finite(finite_numeric_dec(r)?)),
        _ => Err(ValueError::Corrupt("numeric tag")),
    }
}

fn finite_numeric_dec(r: &mut ValueReader<'_>) -> Result<Decimal, ValueError> {
    let negative = match r.u8()? {
        0 => false,
        1 => true,
        _ => return Err(ValueError::Corrupt("numeric sign")),
    };
    let scale = r.i32be()?;
    let ndigits = r.u32be()? as usize;
    let digits = r.take(ndigits)?;
    if digits.iter().any(|&d| d > 9) {
        return Err(ValueError::Corrupt("numeric digit out of range"));
    }
    Ok(Decimal {
        negative,
        digits: digits.to_vec(),
        scale,
    })
}

fn interval_dec(r: &mut ValueReader<'_>) -> Result<Interval, ValueError> {
    Ok(Interval {
        months: i32::from_be_bytes(r.take_array()?),
        days: i32::from_be_bytes(r.take_array()?),
        micros: i64::from_be_bytes(r.take_array()?),
    })
}

fn jsonb_dec(r: &mut ValueReader<'_>, depth: usize) -> Result<Jsonb, ValueError> {
    if depth > MAX_JSONB_DEPTH {
        return Err(ValueError::Corrupt("jsonb nesting too deep"));
    }
    match r.u8()? {
        J_NULL => Ok(Jsonb::Null),
        J_FALSE => Ok(Jsonb::Bool(false)),
        J_TRUE => Ok(Jsonb::Bool(true)),
        // "numeric bytes from its neg byte": finite only, no tag byte.
        J_NUMBER => Ok(Jsonb::Number(Numeric::Finite(finite_numeric_dec(r)?))),
        J_STRING => {
            let s = r.utf8_take()?;
            Ok(Jsonb::String(s))
        }
        J_ARRAY => {
            let n = r.u32be()? as usize;
            let mut items = Vec::new();
            for _ in 0..n {
                items.push(jsonb_dec(r, depth + 1)?);
            }
            Ok(Jsonb::Array(items))
        }
        J_OBJECT => {
            let n = r.u32be()? as usize;
            let mut pairs = Vec::new();
            for _ in 0..n {
                let k = r.utf8_take()?;
                let v = jsonb_dec(r, depth + 1)?;
                pairs.push((k, v));
            }
            Ok(Jsonb::Object(pairs))
        }
        _ => Err(ValueError::Corrupt("jsonb tag")),
    }
}

fn array_dec(elem: &KeyType, r: &mut ValueReader<'_>) -> Result<Array, ValueError> {
    if matches!(elem, KeyType::Array(_)) {
        return Err(ValueError::Corrupt("array of array type"));
    }
    let ndims = usize::from(r.u8()?);
    let mut dims = Vec::with_capacity(ndims);
    for _ in 0..ndims {
        let len = r.i32be()?;
        let lower = r.i32be()?;
        dims.push(ArrayDim { len, lower });
    }
    let nelems = r.u32be()? as usize;
    // The shape is checked before any element is parsed (the encode-side
    // rule, mirrored over the on-disk nelems), so a bogus dims/nelems
    // pair cannot drive parsing.
    check_dims_at(&dims, nelems)?;
    let nullmap = r.take(nullmap_len(nelems))?;
    if !padding_bits_clear(nullmap, nelems) {
        return Err(ValueError::Corrupt("nullmap padding bits set"));
    }
    let mut elems = Vec::new();
    for i in 0..nelems {
        if is_null_bit(nullmap, i) {
            elems.push(None);
        } else {
            let len = r.u32be()? as usize;
            let bytes = r.take(len)?;
            elems.push(Some(decode_value(elem, bytes)?));
        }
    }
    let a = Array { dims, elems };
    // And the literal `check_dims` on the parsed array: it must agree
    // with the pre-parse rule above.
    check_dims(&a).map_err(shape_error)?;
    Ok(a)
}

/// Maps the key codec's shape verdict (`check_dims`) onto the value
/// error type.
fn shape_error(e: CodecError) -> ValueError {
    match e {
        CodecError::InvalidValue(msg) => ValueError::Corrupt(msg),
        _ => ValueError::Corrupt("invalid array shape"),
    }
}

/// The decode-side shape rule: `check_dims` mirrored over
/// `(dims, nelems)`, run before the elements are parsed. Keep in sync
/// with `crate::encode::check_dims`; the post-parse call in
/// [`array_dec`] fails loudly if they drift.
fn check_dims_at(dims: &[ArrayDim], nelems: usize) -> Result<(), ValueError> {
    if dims.len() > MAX_ARRAY_DIMS {
        return Err(ValueError::Corrupt("too many array dimensions"));
    }
    if dims.is_empty() != (nelems == 0) {
        return Err(ValueError::Corrupt("empty array must have no dimensions"));
    }
    let mut n: i64 = 1;
    for d in dims {
        if i64::from(d.len) < 1 || i64::from(d.lower) + i64::from(d.len) - 1 > i64::from(i32::MAX) {
            return Err(ValueError::Corrupt("array dimension out of range"));
        }
        n = n.saturating_mul(i64::from(d.len));
    }
    if !dims.is_empty() && n != nelems as i64 {
        return Err(ValueError::Corrupt(
            "array dimensions do not match element count",
        ));
    }
    Ok(())
}

struct ValueReader<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> ValueReader<'a> {
    fn new(b: &'a [u8]) -> ValueReader<'a> {
        ValueReader { b, pos: 0 }
    }
    fn done(&self) -> bool {
        self.pos == self.b.len()
    }
    fn u8(&mut self) -> Result<u8, ValueError> {
        let &b = self
            .b
            .get(self.pos)
            .ok_or(ValueError::Corrupt("truncated value"))?;
        self.pos += 1;
        Ok(b)
    }
    fn i32be(&mut self) -> Result<i32, ValueError> {
        Ok(i32::from_be_bytes(self.take_array()?))
    }
    fn u32be(&mut self) -> Result<u32, ValueError> {
        Ok(u32::from_be_bytes(self.take_array()?))
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], ValueError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or(ValueError::Corrupt("truncated value"))?;
        let s = self
            .b
            .get(self.pos..end)
            .ok_or(ValueError::Corrupt("truncated value"))?;
        self.pos = end;
        Ok(s)
    }
    fn take_array<const N: usize>(&mut self) -> Result<[u8; N], ValueError> {
        let s = self.take(N)?;
        s.try_into()
            .map_err(|_| ValueError::Corrupt("truncated value"))
    }
    /// The rest of the value: text and bytea are delimited by the datum
    /// header, not by their bytes.
    fn rest(&mut self) -> &'a [u8] {
        let s = &self.b[self.pos..];
        self.pos = self.b.len();
        s
    }
    fn utf8_rest(&mut self) -> Result<String, ValueError> {
        str_from_utf8(self.rest(), "text is not UTF-8")
    }
    fn utf8_take(&mut self) -> Result<String, ValueError> {
        let len = self.u32be()? as usize;
        let bytes = self.take(len)?;
        str_from_utf8(bytes, "jsonb string is not UTF-8")
    }
}

fn str_from_utf8(bytes: &[u8], msg: &'static str) -> Result<String, ValueError> {
    std::str::from_utf8(bytes)
        .map(str::to_string)
        .map_err(|_| ValueError::Corrupt(msg))
}
