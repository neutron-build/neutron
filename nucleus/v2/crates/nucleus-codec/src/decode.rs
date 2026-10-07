//! Decoding. Only canonical encodings are accepted, so for any bytes that
//! decode, re-encoding the result reproduces them exactly.

use crate::encode::{self, *};
use crate::value::{Array, ArrayDim, Decimal, Interval, Jsonb, KeyType, Numeric, Value};
use crate::{CodecError, Result};

pub(crate) struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
    mask: u8,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Reader<'a> {
        Reader {
            buf,
            pos: 0,
            mask: 0,
        }
    }
    /// XOR applied to every byte read (0xFF inside a DESC column).
    pub(crate) fn set_mask(&mut self, mask: u8) {
        self.mask = mask;
    }
    pub(crate) fn pos(&self) -> usize {
        self.pos
    }
    pub(crate) fn u8(&mut self) -> Result<u8> {
        let b = *self.buf.get(self.pos).ok_or(CodecError::Truncated)?;
        self.pos += 1;
        Ok(b ^ self.mask)
    }
    fn bytes<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut a = [0u8; N];
        for x in &mut a {
            *x = self.u8()?;
        }
        Ok(a)
    }
    fn i32(&mut self) -> Result<i32> {
        Ok((u32::from_be_bytes(self.bytes()?) ^ (1 << 31)) as i32)
    }
    fn i64(&mut self) -> Result<i64> {
        Ok((u64::from_be_bytes(self.bytes()?) ^ (1 << 63)) as i64)
    }
    fn escaped(&mut self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        loop {
            match self.u8()? {
                0x00 => match self.u8()? {
                    0xFF => out.push(0x00),
                    0x01 => return Ok(out),
                    _ => return Err(CodecError::Malformed("escape")),
                },
                b => out.push(b),
            }
        }
    }
}

pub(crate) fn value(ty: &KeyType, r: &mut Reader<'_>) -> Result<Value> {
    Ok(match ty {
        KeyType::Bool => match r.u8()? {
            0 => Value::Bool(false),
            1 => Value::Bool(true),
            _ => return Err(CodecError::Malformed("bool")),
        },
        KeyType::Int2 => Value::Int2((u16::from_be_bytes(r.bytes()?) ^ (1 << 15)) as i16),
        KeyType::Int4 => Value::Int4(r.i32()?),
        KeyType::Int8 => Value::Int8(r.i64()?),
        KeyType::Date => Value::Date(r.i32()?),
        KeyType::Time => Value::Time(r.i64()?),
        KeyType::Timestamp => Value::Timestamp(r.i64()?),
        KeyType::TimestampTz => Value::TimestampTz(r.i64()?),
        KeyType::Float4 => {
            let k = u32::from_be_bytes(r.bytes()?);
            let bits = if k >> 31 == 1 { k ^ (1 << 31) } else { !k };
            let x = f32::from_bits(bits);
            if (x.is_nan() && bits != F32_CANONICAL_NAN) || bits == 1 << 31 {
                return Err(CodecError::Malformed("non-canonical float4"));
            }
            Value::Float4(x)
        }
        KeyType::Float8 => {
            let k = u64::from_be_bytes(r.bytes()?);
            let bits = if k >> 63 == 1 { k ^ (1 << 63) } else { !k };
            let x = f64::from_bits(bits);
            if (x.is_nan() && bits != F64_CANONICAL_NAN) || bits == 1 << 63 {
                return Err(CodecError::Malformed("non-canonical float8"));
            }
            Value::Float8(x)
        }
        KeyType::Numeric => Value::Numeric(numeric(r)?),
        KeyType::Text(_) => Value::Text(
            String::from_utf8(r.escaped()?)
                .map_err(|_| CodecError::Malformed("text is not UTF-8"))?,
        ),
        KeyType::Bytea => Value::Bytea(r.escaped()?),
        KeyType::Interval => {
            Value::Interval(interval(i128::from_be_bytes(r.bytes()?) ^ i128::MIN)?)
        }
        KeyType::Uuid => Value::Uuid(r.bytes()?),
        KeyType::Jsonb => Value::Jsonb(jsonb(r)?),
        KeyType::Array(elem) => Value::Array(array(elem, r)?),
    })
}

fn numeric(r: &mut Reader<'_>) -> Result<Numeric> {
    let negative = match r.u8()? {
        NUM_NEG_INF => return Ok(Numeric::NegInf),
        NUM_ZERO => {
            return Ok(Numeric::Finite(Decimal {
                negative: false,
                digits: Vec::new(),
                scale: 0,
            }))
        }
        NUM_POS_INF => return Ok(Numeric::PosInf),
        NUM_NAN => return Ok(Numeric::NaN),
        NUM_NEG => true,
        NUM_POS => false,
        _ => return Err(CodecError::Malformed("numeric tag")),
    };
    let inv = if negative { 0xFF } else { 0 };
    let e = u32::from_be_bytes(r.bytes::<4>()?.map(|b| b ^ inv));
    let exp = (e ^ (1 << 31)) as i32;
    let mut digits = Vec::new();
    loop {
        match r.u8()? ^ inv {
            0 => break,
            b @ 1..=100 => {
                digits.push((b - 1) / 10);
                digits.push((b - 1) % 10);
            }
            _ => return Err(CodecError::Malformed("numeric digit pair")),
        }
    }
    if digits.last() == Some(&0) {
        digits.pop();
    }
    if digits.first().copied().unwrap_or(0) == 0 || digits.last() == Some(&0) {
        return Err(CodecError::Malformed("non-canonical numeric"));
    }
    let scale = i32::try_from(digits.len() as i64 - i64::from(exp))
        .map_err(|_| CodecError::Malformed("numeric scale out of range"))?;
    Ok(Numeric::Finite(Decimal {
        negative,
        digits,
        scale,
    }))
}

/// §4.3 canonical representative.
fn interval(span: i128) -> Result<Interval> {
    if span == encode::interval_span(&Interval::NEG_INFINITY) {
        return Ok(Interval::NEG_INFINITY);
    }
    if span == encode::interval_span(&Interval::INFINITY) {
        return Ok(Interval::INFINITY);
    }
    let day = i128::from(Interval::USECS_PER_DAY);
    let total_days = span.div_euclid(day);
    let rem = span.rem_euclid(day);
    let months = (total_days / 30).clamp(i128::from(i32::MIN), i128::from(i32::MAX));
    let d = total_days - months * 30;
    let days = d.clamp(i128::from(i32::MIN), i128::from(i32::MAX));
    let micros = rem + (d - days) * day;
    let bad = |_| CodecError::Malformed("interval out of range");
    Ok(Interval {
        months: i32::try_from(months).map_err(bad)?,
        days: i32::try_from(days).map_err(bad)?,
        micros: i64::try_from(micros).map_err(bad)?,
    })
}

fn jsonb(r: &mut Reader<'_>) -> Result<Jsonb> {
    let top = r.u8()?;
    let j = jsonb_value(r, 0)?;
    let expected = match &j {
        Jsonb::Array(a) if a.is_empty() => JTOP_EMPTY_ARRAY,
        Jsonb::Array(_) | Jsonb::Object(_) => JTOP_CONTAINER,
        _ => JTOP_SCALAR,
    };
    if top != expected {
        return Err(CodecError::Malformed("jsonb top-level class"));
    }
    Ok(j)
}

fn jsonb_value(r: &mut Reader<'_>, depth: usize) -> Result<Jsonb> {
    if depth > MAX_JSONB_DEPTH {
        return Err(CodecError::Malformed("jsonb nesting too deep"));
    }
    Ok(match r.u8()? {
        J_NULL => Jsonb::Null,
        J_STRING => Jsonb::String(jsonb_string(r)?),
        J_NUMBER => match numeric(r)? {
            n @ Numeric::Finite(_) => Jsonb::Number(n),
            _ => return Err(CodecError::Malformed("jsonb number must be finite")),
        },
        J_BOOL => match r.u8()? {
            0 => Jsonb::Bool(false),
            1 => Jsonb::Bool(true),
            _ => return Err(CodecError::Malformed("jsonb bool")),
        },
        J_ARRAY => {
            let n = u32::from_be_bytes(r.bytes()?);
            let mut items = Vec::new();
            for _ in 0..n {
                items.push(jsonb_value(r, depth + 1)?);
            }
            Jsonb::Array(items)
        }
        J_OBJECT => {
            let n = u32::from_be_bytes(r.bytes()?);
            let mut pairs: Vec<(String, Jsonb)> = Vec::new();
            for _ in 0..n {
                let k = jsonb_string(r)?;
                if let Some((prev, _)) = pairs.last() {
                    if encode::storage_order(prev, &k).is_ge() {
                        return Err(CodecError::Malformed("jsonb keys not in storage order"));
                    }
                }
                let v = jsonb_value(r, depth + 1)?;
                pairs.push((k, v));
            }
            Jsonb::Object(pairs)
        }
        _ => return Err(CodecError::Malformed("jsonb tag")),
    })
}

fn jsonb_string(r: &mut Reader<'_>) -> Result<String> {
    String::from_utf8(r.escaped()?).map_err(|_| CodecError::Malformed("jsonb string is not UTF-8"))
}

fn array(elem_ty: &KeyType, r: &mut Reader<'_>) -> Result<Array> {
    if matches!(elem_ty, KeyType::Array(_)) {
        return Err(CodecError::Malformed("array of array type"));
    }
    let mut elems = Vec::new();
    loop {
        match r.u8()? {
            ELEM_END => break,
            ELEM_NULL => elems.push(None),
            ELEM_PRESENT => elems.push(Some(value(elem_ty, r)?)),
            _ => return Err(CodecError::Malformed("array element marker")),
        }
    }
    let ndims = usize::from(r.u8()?);
    if ndims > MAX_ARRAY_DIMS {
        return Err(CodecError::Malformed("too many array dimensions"));
    }
    let mut dims = Vec::with_capacity(ndims);
    for _ in 0..ndims {
        dims.push(ArrayDim {
            len: r.i32()?,
            lower: 0,
        });
    }
    for d in &mut dims {
        d.lower = r.i32()?;
    }
    let a = Array { dims, elems };
    encode::check_dims(&a).map_err(|_| CodecError::Malformed("array dimensions"))?;
    Ok(a)
}
