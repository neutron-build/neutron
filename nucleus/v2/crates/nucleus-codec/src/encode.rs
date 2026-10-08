//! ASC value encodings (C-Q3s §4-§7). DESC is applied by the caller.

use crate::value::{Array, Decimal, Interval, Jsonb, KeyType, Numeric, Value};
use crate::{CodecError, Result};

pub(crate) const MAX_ARRAY_DIMS: usize = 6;
/// Nesting limit for jsonb, enforced by encoder and decoder alike.
pub(crate) const MAX_JSONB_DEPTH: usize = 1000;

pub(crate) const NUM_NEG_INF: u8 = 0x01;
pub(crate) const NUM_NEG: u8 = 0x02;
pub(crate) const NUM_ZERO: u8 = 0x03;
pub(crate) const NUM_POS: u8 = 0x04;
pub(crate) const NUM_POS_INF: u8 = 0x05;
pub(crate) const NUM_NAN: u8 = 0x06;

pub(crate) const JTOP_EMPTY_ARRAY: u8 = 0x01;
pub(crate) const JTOP_SCALAR: u8 = 0x02;
pub(crate) const JTOP_CONTAINER: u8 = 0x03;
pub(crate) const J_NULL: u8 = 0x01;
pub(crate) const J_STRING: u8 = 0x02;
pub(crate) const J_NUMBER: u8 = 0x03;
pub(crate) const J_BOOL: u8 = 0x04;
pub(crate) const J_ARRAY: u8 = 0x05;
pub(crate) const J_OBJECT: u8 = 0x06;

pub(crate) const ELEM_END: u8 = 0x00;
pub(crate) const ELEM_PRESENT: u8 = 0x01;
pub(crate) const ELEM_NULL: u8 = 0x02;

pub(crate) const F64_CANONICAL_NAN: u64 = 0x7FF8_0000_0000_0000;
pub(crate) const F32_CANONICAL_NAN: u32 = 0x7FC0_0000;

pub(crate) fn value(ty: &KeyType, v: &Value, out: &mut Vec<u8>) -> Result<()> {
    match (ty, v) {
        (KeyType::Bool, Value::Bool(b)) => out.push(u8::from(*b)),
        (KeyType::Int2, Value::Int2(x)) => {
            out.extend_from_slice(&((*x as u16) ^ (1 << 15)).to_be_bytes())
        }
        (KeyType::Int4, Value::Int4(x)) | (KeyType::Date, Value::Date(x)) => flip_i32(*x, out),
        (KeyType::Int8, Value::Int8(x))
        | (KeyType::Timestamp, Value::Timestamp(x))
        | (KeyType::TimestampTz, Value::TimestampTz(x)) => flip_i64(*x, out),
        (KeyType::Time, Value::Time(x)) => {
            // §4: the domain is 0 ..= 86_400_000_000 (one day, `24:00:00`
            // included). `flip(i64)` for anything inside it.
            if !(0..=Interval::USECS_PER_DAY).contains(x) {
                return Err(CodecError::InvalidValue("time out of range"));
            }
            flip_i64(*x, out)
        }
        (KeyType::Float4, Value::Float4(x)) => out.extend_from_slice(&f32_key(*x).to_be_bytes()),
        (KeyType::Float8, Value::Float8(x)) => out.extend_from_slice(&f64_key(*x).to_be_bytes()),
        (KeyType::Numeric, Value::Numeric(n)) => numeric(n, out)?,
        (KeyType::Text(_), Value::Text(s)) => escaped(s.as_bytes(), out),
        (KeyType::Bytea, Value::Bytea(b)) => escaped(b, out),
        (KeyType::Interval, Value::Interval(i)) => {
            out.extend_from_slice(&((interval_span(i) as u128) ^ (1 << 127)).to_be_bytes())
        }
        (KeyType::Uuid, Value::Uuid(u)) => out.extend_from_slice(u),
        (KeyType::Jsonb, Value::Jsonb(j)) => jsonb(j, out)?,
        (KeyType::Array(elem), Value::Array(a)) => array(elem, a, out)?,
        _ => return Err(CodecError::TypeMismatch),
    }
    Ok(())
}

pub(crate) fn flip_i32(x: i32, out: &mut Vec<u8>) {
    out.extend_from_slice(&((x as u32) ^ (1 << 31)).to_be_bytes());
}

fn flip_i64(x: i64, out: &mut Vec<u8>) {
    out.extend_from_slice(&((x as u64) ^ (1 << 63)).to_be_bytes());
}

/// §4.1: canonical NaN and +0, then sign-magnitude to unsigned order.
fn f64_key(x: f64) -> u64 {
    let bits = if x.is_nan() {
        F64_CANONICAL_NAN
    } else if x == 0.0 {
        0
    } else {
        x.to_bits()
    };
    if bits >> 63 == 0 {
        bits ^ (1 << 63)
    } else {
        !bits
    }
}

fn f32_key(x: f32) -> u32 {
    let bits = if x.is_nan() {
        F32_CANONICAL_NAN
    } else if x == 0.0 {
        0
    } else {
        x.to_bits()
    };
    if bits >> 31 == 0 {
        bits ^ (1 << 31)
    } else {
        !bits
    }
}

/// §4.3: PostgreSQL's `interval_cmp_value`.
pub(crate) fn interval_span(i: &Interval) -> i128 {
    let days = i128::from(i.months) * 30 + i128::from(i.days);
    days * i128::from(Interval::USECS_PER_DAY) + i128::from(i.micros)
}

/// §5.1.
pub(crate) fn escaped(bytes: &[u8], out: &mut Vec<u8>) {
    for &b in bytes {
        out.push(b);
        if b == 0 {
            out.push(0xFF);
        }
    }
    out.extend_from_slice(&[0x00, 0x01]);
}

/// §4.2.
pub(crate) fn numeric(n: &Numeric, out: &mut Vec<u8>) -> Result<()> {
    let d = match n {
        Numeric::NegInf => {
            out.push(NUM_NEG_INF);
            return Ok(());
        }
        Numeric::PosInf => {
            out.push(NUM_POS_INF);
            return Ok(());
        }
        Numeric::NaN => {
            out.push(NUM_NAN);
            return Ok(());
        }
        Numeric::Finite(d) => d,
    };
    let Some((digits, exp)) = normalize(d)? else {
        out.push(NUM_ZERO);
        return Ok(());
    };
    let mut body = Vec::with_capacity(4 + digits.len() / 2 + 2);
    flip_i32(exp, &mut body);
    for pair in digits.chunks(2) {
        let hi = pair[0];
        let lo = pair.get(1).copied().unwrap_or(0);
        body.push(10 * hi + lo + 1);
    }
    body.push(0x00);
    if d.negative {
        out.push(NUM_NEG);
        out.extend(body.iter().map(|b| !b));
    } else {
        out.push(NUM_POS);
        out.extend_from_slice(&body);
    }
    Ok(())
}

/// Significant digits `d1..dn` (no leading or trailing zeros) and `E` with
/// value `0.d1..dn * 10^E`, or `None` for zero.
fn normalize(d: &Decimal) -> Result<Option<(&[u8], i32)>> {
    if d.digits.iter().any(|&x| x > 9) {
        return Err(CodecError::InvalidValue("numeric digit out of range"));
    }
    let Some(start) = d.digits.iter().position(|&x| x != 0) else {
        return Ok(None);
    };
    let end = d
        .digits
        .iter()
        .rposition(|&x| x != 0)
        .map_or(start, |e| e + 1);
    let exp = d.digits.len() as i64 - start as i64 - i64::from(d.scale);
    let exp = i32::try_from(exp)
        .map_err(|_| CodecError::InvalidValue("numeric exponent out of range"))?;
    Ok(Some((&d.digits[start..end], exp)))
}

/// §6.
fn jsonb(j: &Jsonb, out: &mut Vec<u8>) -> Result<()> {
    out.push(match j {
        Jsonb::Array(a) if a.is_empty() => JTOP_EMPTY_ARRAY,
        Jsonb::Array(_) | Jsonb::Object(_) => JTOP_CONTAINER,
        _ => JTOP_SCALAR,
    });
    jsonb_value(j, out, 0)
}

fn jsonb_value(j: &Jsonb, out: &mut Vec<u8>, depth: usize) -> Result<()> {
    if depth > MAX_JSONB_DEPTH {
        return Err(CodecError::InvalidValue("jsonb nesting too deep"));
    }
    match j {
        Jsonb::Null => out.push(J_NULL),
        Jsonb::String(s) => {
            out.push(J_STRING);
            escaped(s.as_bytes(), out);
        }
        Jsonb::Number(n) => {
            if !matches!(n, Numeric::Finite(_)) {
                return Err(CodecError::InvalidValue("jsonb number must be finite"));
            }
            out.push(J_NUMBER);
            numeric(n, out)?;
        }
        Jsonb::Bool(b) => out.extend_from_slice(&[J_BOOL, u8::from(*b)]),
        Jsonb::Array(items) => {
            out.push(J_ARRAY);
            count(items.len(), out)?;
            for item in items {
                jsonb_value(item, out, depth + 1)?;
            }
        }
        Jsonb::Object(pairs) => {
            let pairs = canonical_pairs(pairs);
            out.push(J_OBJECT);
            count(pairs.len(), out)?;
            for (k, v) in pairs {
                escaped(k.as_bytes(), out);
                jsonb_value(v, out, depth + 1)?;
            }
        }
    }
    Ok(())
}

fn count(n: usize, out: &mut Vec<u8>) -> Result<()> {
    let n = u32::try_from(n).map_err(|_| CodecError::InvalidValue("jsonb container too large"))?;
    out.extend_from_slice(&n.to_be_bytes());
    Ok(())
}

/// jsonb input semantics: keys sorted by (length, bytes), last duplicate wins.
pub(crate) fn canonical_pairs(pairs: &[(String, Jsonb)]) -> Vec<&(String, Jsonb)> {
    let mut sorted: Vec<&(String, Jsonb)> = pairs.iter().collect();
    sorted.sort_by(|a, b| storage_order(&a.0, &b.0));
    let mut out: Vec<&(String, Jsonb)> = Vec::with_capacity(sorted.len());
    for p in sorted {
        match out.last_mut() {
            Some(last) if last.0 == p.0 => *last = p,
            _ => out.push(p),
        }
    }
    out
}

pub(crate) fn storage_order(a: &str, b: &str) -> std::cmp::Ordering {
    a.len()
        .cmp(&b.len())
        .then_with(|| a.as_bytes().cmp(b.as_bytes()))
}

/// §7.
fn array(elem_ty: &KeyType, a: &Array, out: &mut Vec<u8>) -> Result<()> {
    if matches!(elem_ty, KeyType::Array(_)) {
        return Err(CodecError::InvalidValue("array of array type"));
    }
    check_dims(a)?;
    for e in &a.elems {
        match e {
            None => out.push(ELEM_NULL),
            Some(v) => {
                out.push(ELEM_PRESENT);
                value(elem_ty, v, out)?;
            }
        }
    }
    out.push(ELEM_END);
    out.push(a.dims.len() as u8);
    for d in &a.dims {
        flip_i32(d.len, out);
    }
    for d in &a.dims {
        flip_i32(d.lower, out);
    }
    Ok(())
}

pub(crate) fn check_dims(a: &Array) -> Result<()> {
    if a.dims.len() > MAX_ARRAY_DIMS {
        return Err(CodecError::InvalidValue("too many array dimensions"));
    }
    if a.elems.is_empty() != a.dims.is_empty() {
        return Err(CodecError::InvalidValue(
            "empty array must have no dimensions",
        ));
    }
    let mut n: i64 = 1;
    for d in &a.dims {
        if d.len < 1 || i64::from(d.lower) + i64::from(d.len) - 1 > i64::from(i32::MAX) {
            return Err(CodecError::InvalidValue("array dimension out of range"));
        }
        n = n.saturating_mul(i64::from(d.len));
    }
    if !a.dims.is_empty() && n != a.elems.len() as i64 {
        return Err(CodecError::InvalidValue(
            "array dimensions do not match element count",
        ));
    }
    Ok(())
}
