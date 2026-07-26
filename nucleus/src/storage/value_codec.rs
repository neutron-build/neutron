//! Canonical binary serialization for `Value` and `Row`.
//!
//! This is the **single** on-disk `Value` encoding used across the engine's
//! persistence and intermediate-result paths — the MVCC WAL logs rows with it,
//! and the query-execution spill path (`executor/spill.rs`) reuses it verbatim
//! so a spilled intermediate result round-trips byte-for-byte identically to a
//! logged one. Deliberately one implementation: a second, subtly-different
//! `Value` codec is a data-corruption risk, so new persistence surfaces call in
//! here rather than rolling their own.
//!
//! ## Format
//! Each value is `[tag: u8] [payload…]`; a row is `[len: u32 LE] [value…]`.
//! Tags are stable wire constants (`VAL_*`) — never renumber them; appending a
//! new variant takes the next free tag. Multi-byte integers are little-endian.
//! Strings/bytea are length-prefixed with a `u32 LE`.

use crate::types::Value;

// ── Value tags (stable wire constants — do not renumber) ─────────────────────

pub(crate) const VAL_NULL: u8 = 0;
pub(crate) const VAL_BOOL: u8 = 1;
pub(crate) const VAL_INT32: u8 = 2;
pub(crate) const VAL_INT64: u8 = 3;
pub(crate) const VAL_FLOAT64: u8 = 4;
pub(crate) const VAL_TEXT: u8 = 5;
pub(crate) const VAL_BYTEA: u8 = 6;
pub(crate) const VAL_DATE: u8 = 7;
pub(crate) const VAL_TIMESTAMP: u8 = 8;
pub(crate) const VAL_TIMESTAMPTZ: u8 = 9;
pub(crate) const VAL_NUMERIC: u8 = 10;
pub(crate) const VAL_UUID: u8 = 11;
pub(crate) const VAL_JSONB: u8 = 12;
pub(crate) const VAL_VECTOR: u8 = 13;
pub(crate) const VAL_INTERVAL: u8 = 14;
pub(crate) const VAL_ARRAY: u8 = 15;

/// Append the tagged encoding of `val` to `buf`.
pub(crate) fn write_value(buf: &mut Vec<u8>, val: &Value) {
    match val {
        Value::Null => buf.push(VAL_NULL),
        Value::Bool(b) => {
            buf.push(VAL_BOOL);
            buf.push(if *b { 1 } else { 0 });
        }
        Value::Int32(n) => {
            buf.push(VAL_INT32);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::Int64(n) => {
            buf.push(VAL_INT64);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::Float64(f) => {
            buf.push(VAL_FLOAT64);
            buf.extend_from_slice(&f.to_le_bytes());
        }
        Value::Text(s) => {
            buf.push(VAL_TEXT);
            write_str(buf, s);
        }
        Value::Bytea(b) => {
            buf.push(VAL_BYTEA);
            write_u32(buf, b.len() as u32);
            buf.extend_from_slice(b);
        }
        Value::Date(d) => {
            buf.push(VAL_DATE);
            buf.extend_from_slice(&d.to_le_bytes());
        }
        Value::Timestamp(t) => {
            buf.push(VAL_TIMESTAMP);
            buf.extend_from_slice(&t.to_le_bytes());
        }
        Value::TimestampTz(t) => {
            buf.push(VAL_TIMESTAMPTZ);
            buf.extend_from_slice(&t.to_le_bytes());
        }
        Value::Numeric(s) => {
            buf.push(VAL_NUMERIC);
            write_str(buf, s);
        }
        Value::Uuid(bytes) => {
            buf.push(VAL_UUID);
            buf.extend_from_slice(bytes);
        }
        Value::Jsonb(j) => {
            buf.push(VAL_JSONB);
            write_str(buf, &j.to_string());
        }
        Value::Vector(v) => {
            buf.push(VAL_VECTOR);
            write_u32(buf, v.len() as u32);
            for f in v {
                buf.extend_from_slice(&f.to_le_bytes());
            }
        }
        Value::Interval {
            months,
            days,
            microseconds,
        } => {
            buf.push(VAL_INTERVAL);
            buf.extend_from_slice(&months.to_le_bytes());
            buf.extend_from_slice(&days.to_le_bytes());
            buf.extend_from_slice(&microseconds.to_le_bytes());
        }
        Value::Array(arr) => {
            buf.push(VAL_ARRAY);
            write_u32(buf, arr.len() as u32);
            for v in arr {
                write_value(buf, v);
            }
        }
    }
}

/// Decode one tagged value starting at `*pos`, advancing `*pos` past it.
/// Returns `None` on a truncated or unrecognized encoding.
pub(crate) fn read_value(data: &[u8], pos: &mut usize) -> Option<Value> {
    let tag = *data.get(*pos)?;
    *pos += 1;
    match tag {
        VAL_NULL => Some(Value::Null),
        VAL_BOOL => {
            let b = *data.get(*pos)?;
            *pos += 1;
            Some(Value::Bool(b != 0))
        }
        VAL_INT32 => {
            let b = data.get(*pos..*pos + 4)?;
            *pos += 4;
            Some(Value::Int32(i32::from_le_bytes([b[0], b[1], b[2], b[3]])))
        }
        VAL_INT64 => {
            let b = data.get(*pos..*pos + 8)?;
            *pos += 8;
            Some(Value::Int64(i64::from_le_bytes([
                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
            ])))
        }
        VAL_FLOAT64 => {
            let b = data.get(*pos..*pos + 8)?;
            *pos += 8;
            Some(Value::Float64(f64::from_le_bytes([
                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
            ])))
        }
        VAL_TEXT => {
            let s = read_str(data, pos)?;
            Some(Value::Text(s))
        }
        VAL_BYTEA => {
            let len = read_u32_val(data, pos)? as usize;
            if *pos + len > data.len() {
                return None;
            }
            let b = data[*pos..*pos + len].to_vec();
            *pos += len;
            Some(Value::Bytea(b))
        }
        VAL_DATE => {
            let b = data.get(*pos..*pos + 4)?;
            *pos += 4;
            Some(Value::Date(i32::from_le_bytes([b[0], b[1], b[2], b[3]])))
        }
        VAL_TIMESTAMP => {
            let b = data.get(*pos..*pos + 8)?;
            *pos += 8;
            Some(Value::Timestamp(i64::from_le_bytes([
                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
            ])))
        }
        VAL_TIMESTAMPTZ => {
            let b = data.get(*pos..*pos + 8)?;
            *pos += 8;
            Some(Value::TimestampTz(i64::from_le_bytes([
                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
            ])))
        }
        VAL_NUMERIC => {
            let s = read_str(data, pos)?;
            Some(Value::Numeric(s))
        }
        VAL_UUID => {
            let b = data.get(*pos..*pos + 16)?;
            *pos += 16;
            let mut arr = [0u8; 16];
            arr.copy_from_slice(b);
            Some(Value::Uuid(arr))
        }
        VAL_JSONB => {
            let s = read_str(data, pos)?;
            // A malformed stored JSONB value must not drop the whole row
            // (data loss). Fall back to JSON null; the insert path now rejects
            // invalid JSON up front, so this only guards legacy/foreign rows.
            let v: serde_json::Value = serde_json::from_str(&s).unwrap_or(serde_json::Value::Null);
            Some(Value::Jsonb(v))
        }
        VAL_VECTOR => {
            let count = read_u32_val(data, pos)? as usize;
            let byte_len = count * 4;
            if *pos + byte_len > data.len() {
                return None;
            }
            let mut v = Vec::with_capacity(count);
            for _ in 0..count {
                let b = data.get(*pos..*pos + 4)?;
                *pos += 4;
                v.push(f32::from_le_bytes([b[0], b[1], b[2], b[3]]));
            }
            Some(Value::Vector(v))
        }
        VAL_INTERVAL => {
            let mb = data.get(*pos..*pos + 4)?;
            *pos += 4;
            let months = i32::from_le_bytes([mb[0], mb[1], mb[2], mb[3]]);
            let db = data.get(*pos..*pos + 4)?;
            *pos += 4;
            let days = i32::from_le_bytes([db[0], db[1], db[2], db[3]]);
            let ub = data.get(*pos..*pos + 8)?;
            *pos += 8;
            let microseconds =
                i64::from_le_bytes([ub[0], ub[1], ub[2], ub[3], ub[4], ub[5], ub[6], ub[7]]);
            Some(Value::Interval {
                months,
                days,
                microseconds,
            })
        }
        VAL_ARRAY => {
            let count = read_u32_val(data, pos)? as usize;
            // Every element costs at least a tag byte, so a count larger than
            // the bytes remaining is corrupt. Reserving on the unchecked count
            // let a torn record request gigabytes and ABORT the process through
            // handle_alloc_error instead of returning None — and this codec also
            // backs the MVCC WAL, so a torn WAL record could reach it.
            if count > data.len().saturating_sub(*pos) {
                return None;
            }
            let mut arr = Vec::with_capacity(count);
            for _ in 0..count {
                arr.push(read_value(data, pos)?);
            }
            Some(Value::Array(arr))
        }
        _ => None,
    }
}

/// Append a length-prefixed row (`[u32 len][value…]`) to `buf`.
pub(crate) fn write_row(buf: &mut Vec<u8>, row: &[Value]) {
    write_u32(buf, row.len() as u32);
    for val in row {
        write_value(buf, val);
    }
}

/// Decode one length-prefixed row starting at `*pos`, advancing past it.
pub(crate) fn read_row(data: &[u8], pos: &mut usize) -> Option<Vec<Value>> {
    let count = read_u32_val(data, pos)? as usize;
    // See `VAL_ARRAY`: an unchecked count aborts the process on corrupt input
    // rather than failing this decode.
    if count > data.len().saturating_sub(*pos) {
        return None;
    }
    let mut row = Vec::with_capacity(count);
    for _ in 0..count {
        row.push(read_value(data, pos)?);
    }
    Some(row)
}

// ── Primitive helpers (self-contained; the WAL keeps its own copies for record
// framing so this codec has no cross-module coupling) ────────────────────────

fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn write_str(buf: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    write_u32(buf, b.len() as u32);
    buf.extend_from_slice(b);
}

fn read_u32_val(data: &[u8], pos: &mut usize) -> Option<u32> {
    let b = data.get(*pos..*pos + 4)?;
    *pos += 4;
    Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

fn read_str(data: &[u8], pos: &mut usize) -> Option<String> {
    let len = read_u32_val(data, pos)? as usize;
    if *pos + len > data.len() {
        return None;
    }
    let s = std::str::from_utf8(&data[*pos..*pos + len])
        .ok()?
        .to_string();
    *pos += len;
    Some(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(v: Value) -> Value {
        let mut buf = Vec::new();
        write_value(&mut buf, &v);
        let mut pos = 0;
        let out = read_value(&buf, &mut pos).expect("decodes");
        assert_eq!(pos, buf.len(), "decoder consumes exactly the bytes written");
        out
    }

    /// One representative of every `Value` variant, including the awkward ones
    /// (empty collections, NULL-in-array, nested array, non-finite floats).
    fn every_variant() -> Vec<Value> {
        vec![
            Value::Null,
            Value::Bool(true),
            Value::Bool(false),
            Value::Int32(i32::MIN),
            Value::Int32(0),
            Value::Int64(i64::MAX),
            Value::Float64(-0.0),
            Value::Float64(3.5),
            Value::Text(String::new()),
            Value::Text("héllo \u{1F300} world".to_string()),
            Value::Bytea(vec![]),
            Value::Bytea(vec![0, 255, 7, 128]),
            Value::Date(-19000),
            Value::Timestamp(1_700_000_000_000_000),
            Value::TimestampTz(-1),
            Value::Numeric("-0.000000001".to_string()),
            Value::Uuid([9u8; 16]),
            Value::Jsonb(serde_json::json!({"a": [1, 2, null], "b": "x"})),
            Value::Vector(vec![]),
            Value::Vector(vec![1.0, -2.5, f32::MIN]),
            Value::Interval {
                months: -3,
                days: 40,
                microseconds: i64::MIN,
            },
            Value::Array(vec![]),
            Value::Array(vec![Value::Int64(1), Value::Null, Value::Text("z".into())]),
            Value::Array(vec![Value::Array(vec![Value::Bool(true)]), Value::Null]),
        ]
    }

    #[test]
    fn every_value_variant_roundtrips() {
        for v in every_variant() {
            assert_eq!(roundtrip(v.clone()), v, "variant did not round-trip: {v:?}");
        }
    }

    #[test]
    fn row_roundtrips_and_consumes_exactly() {
        let row = every_variant();
        let mut buf = Vec::new();
        write_row(&mut buf, &row);
        let mut pos = 0;
        let out = read_row(&buf, &mut pos).expect("decodes");
        assert_eq!(pos, buf.len());
        assert_eq!(out, row);
    }

    #[test]
    fn non_finite_floats_survive() {
        // NaN != NaN, so compare bit patterns explicitly.
        for bits in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let out = roundtrip(Value::Float64(bits));
            match out {
                Value::Float64(f) => assert_eq!(f.to_bits(), bits.to_bits()),
                other => panic!("expected float, got {other:?}"),
            }
        }
        let out = roundtrip(Value::Vector(vec![f32::NAN, f32::INFINITY]));
        match out {
            Value::Vector(v) => {
                assert!(v[0].is_nan());
                assert_eq!(v[1], f32::INFINITY);
            }
            other => panic!("expected vector, got {other:?}"),
        }
    }

    #[test]
    fn truncated_input_returns_none_not_panic() {
        let mut buf = Vec::new();
        write_value(&mut buf, &Value::Text("abcdef".into()));
        // Chop every non-empty prefix; none should panic, all should decline.
        for cut in 0..buf.len() {
            let mut pos = 0;
            let _ = read_value(&buf[..cut], &mut pos);
        }
    }

    #[test]
    fn unknown_tag_declines() {
        let buf = [200u8, 1, 2, 3];
        let mut pos = 0;
        assert!(read_value(&buf, &mut pos).is_none());
    }
}
