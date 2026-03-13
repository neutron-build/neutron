//! RESP2 protocol encoder.
//!
//! Provides functions to encode Rust values into RESP2 wire format bytes.

use crate::types::Value;

/// Encode a RESP2 simple string: `+{s}\r\n`
pub fn encode_simple_string(s: &str) -> Vec<u8> {
    format!("+{s}\r\n").into_bytes()
}

/// Encode a RESP2 error: `-{msg}\r\n`
pub fn encode_error(msg: &str) -> Vec<u8> {
    format!("-{msg}\r\n").into_bytes()
}

/// Encode a RESP2 integer: `:{n}\r\n`
pub fn encode_integer(n: i64) -> Vec<u8> {
    format!(":{n}\r\n").into_bytes()
}

/// Encode a RESP2 bulk string: `${len}\r\n{data}\r\n`
pub fn encode_bulk_string(data: &[u8]) -> Vec<u8> {
    let mut out = format!("${}\r\n", data.len()).into_bytes();
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
    out
}

/// Encode a RESP2 null bulk string: `$-1\r\n`
pub fn encode_null_bulk() -> Vec<u8> {
    b"$-1\r\n".to_vec()
}

/// Encode a RESP2 array header: `*{len}\r\n`
pub fn encode_array_header(len: usize) -> Vec<u8> {
    format!("*{len}\r\n").into_bytes()
}

/// Encode a Nucleus `Value` as a RESP2 value.
///
/// - `Value::Null` becomes a null bulk string.
/// - `Value::Text(s)` becomes a bulk string.
/// - `Value::Int32(n)` / `Value::Int64(n)` become RESP integers.
/// - `Value::Float64(f)` becomes a bulk string of the formatted number.
/// - `Value::Bool(b)` becomes integer 1 or 0.
/// - All other variants use their `Display` representation as a bulk string.
pub fn encode_value(v: &Value) -> Vec<u8> {
    match v {
        Value::Null => encode_null_bulk(),
        Value::Text(s) => encode_bulk_string(s.as_bytes()),
        Value::Int32(n) => encode_integer(*n as i64),
        Value::Int64(n) => encode_integer(*n),
        Value::Float64(f) => encode_bulk_string(f.to_string().as_bytes()),
        Value::Bool(b) => encode_integer(if *b { 1 } else { 0 }),
        other => encode_bulk_string(other.to_string().as_bytes()),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_simple_string() {
        assert_eq!(encode_simple_string("OK"), b"+OK\r\n");
    }

    #[test]
    fn test_encode_error() {
        assert_eq!(encode_error("ERR something"), b"-ERR something\r\n");
    }

    #[test]
    fn test_encode_integer() {
        assert_eq!(encode_integer(42), b":42\r\n");
        assert_eq!(encode_integer(-1), b":-1\r\n");
        assert_eq!(encode_integer(0), b":0\r\n");
    }

    #[test]
    fn test_encode_bulk_string() {
        assert_eq!(encode_bulk_string(b"hello"), b"$5\r\nhello\r\n");
        assert_eq!(encode_bulk_string(b""), b"$0\r\n\r\n");
    }

    #[test]
    fn test_encode_null() {
        assert_eq!(encode_null_bulk(), b"$-1\r\n");
    }
}
