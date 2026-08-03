//! Compact binary codec for KV values held outside the hot tier.
//!
//! Encodes a `Value` into the byte form used by the disk-backed `LsmTree` cold
//! tier, and decodes it back. Also used by the graph and document stores for
//! their own property encoding, which is why this is a codec module rather than
//! a store: it used to also contain a `TieredKvStore`, a second hot/cold KV
//! implementation with no callers anywhere in the tree. `KvStore` has its own
//! inline cold tier and is what the server constructs, so the unused twin was
//! removed rather than left as a thing a future fix might land in by mistake.
//!
//! ## Six tags, and what that costs
//!
//! Null, Bool, Int32, Int64, Float64 and Text encode exactly. Everything else
//! falls back to `Display` and decodes as `Text` — a long-standing tradeoff
//! shared with the KV WAL, where a `Bytea` has always returned as text after a
//! restart. Use [`is_losslessly_encodable`] before moving a value out of the
//! hot tier so eviction never changes a value's type as a side effect of memory
//! pressure.

use crate::types::Value;

// ============================================================================
// Value encoding/decoding for LsmTree binary storage
// ============================================================================

const TAG_NULL: u8 = 0;
const TAG_BOOL: u8 = 1;
const TAG_INT32: u8 = 2;
const TAG_INT64: u8 = 3;
const TAG_FLOAT64: u8 = 4;
const TAG_TEXT: u8 = 5;

/// Whether `encode_value` can represent this value without changing its type.
///
/// The codec carries six tags; everything else falls back to `Display` and
/// decodes as `Text`. That is a documented, long-standing tradeoff shared with
/// the KV WAL — a `Bytea` has always come back as text across a restart. It
/// becomes a live concern for the cold tier, because eviction can rewrite a
/// value's type *without* a restart, at an arbitrary moment, purely because
/// memory got tight. Callers use this to leave such values in the hot tier
/// instead: refusing to evict costs memory, and evicting costs correctness.
pub fn is_losslessly_encodable(v: &Value) -> bool {
    matches!(
        v,
        Value::Null
            | Value::Bool(_)
            | Value::Int32(_)
            | Value::Int64(_)
            | Value::Float64(_)
            | Value::Text(_)
    )
}

/// Encode a `Value` into a compact binary format for LsmTree storage.
pub fn encode_value(v: &Value) -> Vec<u8> {
    match v {
        Value::Null => vec![TAG_NULL],
        Value::Bool(b) => vec![TAG_BOOL, if *b { 1 } else { 0 }],
        Value::Int32(n) => {
            let mut buf = Vec::with_capacity(5);
            buf.push(TAG_INT32);
            buf.extend_from_slice(&n.to_le_bytes());
            buf
        }
        Value::Int64(n) => {
            let mut buf = Vec::with_capacity(9);
            buf.push(TAG_INT64);
            buf.extend_from_slice(&n.to_le_bytes());
            buf
        }
        Value::Float64(n) => {
            let mut buf = Vec::with_capacity(9);
            buf.push(TAG_FLOAT64);
            buf.extend_from_slice(&n.to_le_bytes());
            buf
        }
        Value::Text(s) => {
            let bytes = s.as_bytes();
            let mut buf = Vec::with_capacity(1 + 4 + bytes.len());
            buf.push(TAG_TEXT);
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
            buf
        }
        // For unsupported types, fall back to Text encoding via Display
        other => {
            let s = other.to_string();
            let bytes = s.as_bytes();
            let mut buf = Vec::with_capacity(1 + 4 + bytes.len());
            buf.push(TAG_TEXT);
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
            buf
        }
    }
}

/// Decode a `Value` from the compact binary format used in LsmTree storage.
pub fn decode_value(data: &[u8]) -> Value {
    if data.is_empty() {
        return Value::Null;
    }
    match data[0] {
        TAG_NULL => Value::Null,
        TAG_BOOL => {
            if data.len() < 2 {
                return Value::Null;
            }
            Value::Bool(data[1] != 0)
        }
        TAG_INT32 => {
            if data.len() < 5 {
                return Value::Null;
            }
            let n = i32::from_le_bytes([data[1], data[2], data[3], data[4]]);
            Value::Int32(n)
        }
        TAG_INT64 => {
            if data.len() < 9 {
                return Value::Null;
            }
            let n = i64::from_le_bytes([
                data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
            ]);
            Value::Int64(n)
        }
        TAG_FLOAT64 => {
            if data.len() < 9 {
                return Value::Null;
            }
            let n = f64::from_le_bytes([
                data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
            ]);
            Value::Float64(n)
        }
        TAG_TEXT => {
            if data.len() < 5 {
                return Value::Null;
            }
            let len = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
            if data.len() < 5 + len {
                return Value::Null;
            }
            let s = String::from_utf8_lossy(&data[5..5 + len]).into_owned();
            Value::Text(s)
        }
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    // 3.14/3.14159 here are arbitrary test fixtures, not PI approximations.
    #![allow(clippy::approx_constant)]
    use super::*;

    #[test]
    fn test_tiered_value_encoding_roundtrip() {
        let values = vec![
            Value::Null,
            Value::Bool(true),
            Value::Bool(false),
            Value::Int32(42),
            Value::Int32(-1),
            Value::Int32(i32::MAX),
            Value::Int32(i32::MIN),
            Value::Int64(123456789),
            Value::Int64(-987654321),
            Value::Int64(i64::MAX),
            Value::Int64(i64::MIN),
            Value::Float64(3.14159),
            Value::Float64(-0.0),
            Value::Float64(f64::INFINITY),
            Value::Text("hello world".into()),
            Value::Text(String::new()),
            Value::Text("unicode: \u{1F600}".into()),
        ];

        for v in &values {
            let encoded = encode_value(v);
            let decoded = decode_value(&encoded);
            assert_eq!(
                &decoded, v,
                "roundtrip failed for {v:?}: encoded={encoded:?}, decoded={decoded:?}"
            );
        }
    }
}
