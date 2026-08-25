//! RESP2 protocol parser.
//!
//! Reads RESP2 values from an `AsyncBufRead` stream. Supports the five RESP2
//! types: simple strings, errors, integers, bulk strings, and arrays.

use std::io;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};

/// Maximum allowed bulk string size (512 MiB). Prevents memory-exhaustion DoS
/// from a malicious client sending `$999999999999\r\n`.
const MAX_BULK_STRING_LEN: usize = 512 * 1024 * 1024;

/// Maximum allowed array element count (1 million). Prevents OOM from
/// `*999999999\r\n` without constraining any realistic workload.
const MAX_ARRAY_COUNT: usize = 1_000_000;

/// Maximum allowed nesting depth for RESP arrays (128, Redis parity).
/// Prevents stack exhaustion from deeply nested arrays
/// (`*1\r\n*1\r\n*1\r\n…`) before authentication.
const MAX_NESTING_DEPTH: usize = 128;

/// Maximum length of a RESP line (64 KiB, Redis inline-command parity).
/// Type-prefix lines (`$5\r\n`, `*3\r\n`) are a handful of bytes; anything
/// larger is a hostile peer. Bulk payloads are NOT affected — they are read
/// with `read_exact` and capped by MAX_BULK_STRING_LEN.
const MAX_LINE_LEN: usize = 64 * 1024;

/// A RESP2 protocol value.
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
}

/// Read a single RESP value from the stream.
///
/// Returns an `io::Error` with `UnexpectedEof` if the connection is closed
/// cleanly before a complete value arrives.
pub async fn read_value<R: AsyncBufRead + Unpin>(reader: &mut R) -> io::Result<RespValue> {
    read_value_inner(reader, 0).await
}

/// Read one \n-terminated line, at most MAX_LINE_LEN bytes. Uses `take`
/// so a peer streaming without a newline cannot grow the buffer past the
/// cap; the +1 slack lets the length check distinguish "at cap" from
/// "over cap".
async fn read_line_bounded<R: AsyncBufRead + Unpin>(reader: &mut R) -> io::Result<String> {
    let mut line = String::new();
    let n = reader
        .take(MAX_LINE_LEN as u64 + 1)
        .read_line(&mut line)
        .await?;
    if line.len() > MAX_LINE_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("RESP line exceeds maximum length {MAX_LINE_LEN}"),
        ));
    }
    if n == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "connection closed",
        ));
    }
    Ok(line)
}

async fn read_value_inner<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    depth: usize,
) -> io::Result<RespValue> {
    let line = read_line_bounded(reader).await?;
    let line = line.trim_end_matches('\n').trim_end_matches('\r');

    if line.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty RESP line",
        ));
    }

    let prefix = line.as_bytes()[0];
    let payload = &line[1..];

    match prefix {
        b'+' => Ok(RespValue::SimpleString(payload.to_string())),
        b'-' => Ok(RespValue::Error(payload.to_string())),
        b':' => {
            let n = payload
                .parse::<i64>()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(RespValue::Integer(n))
        }
        b'$' => {
            let len = payload
                .parse::<i64>()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            if len < 0 {
                return Ok(RespValue::BulkString(None));
            }
            let len = len as usize;
            if len > MAX_BULK_STRING_LEN {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("bulk string length {len} exceeds maximum {MAX_BULK_STRING_LEN}"),
                ));
            }
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf).await?;
            // Read trailing \r\n
            let mut crlf = [0u8; 2];
            reader.read_exact(&mut crlf).await?;
            Ok(RespValue::BulkString(Some(buf)))
        }
        b'*' => {
            let count = payload
                .parse::<i64>()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            if count < 0 {
                return Ok(RespValue::Array(None));
            }
            let count = count as usize;
            if count > MAX_ARRAY_COUNT {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("array count {count} exceeds maximum {MAX_ARRAY_COUNT}"),
                ));
            }
            // Depth, not just count: `read_value_inner` recurses once per
            // nesting level, and MAX_ARRAY_COUNT caps elements per array,
            // never depth. Without this check a few hundred KB of `*1\r\n`
            // overflows the stack pre-auth and aborts the whole process.
            if depth >= MAX_NESTING_DEPTH {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("array nesting depth exceeds maximum {MAX_NESTING_DEPTH}"),
                ));
            }
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(Box::pin(read_value_inner(reader, depth + 1)).await?);
            }
            Ok(RespValue::Array(Some(items)))
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown RESP type byte: {}", other as char),
        )),
    }
}

/// Extract command arguments from a RESP array of bulk strings.
///
/// Returns `None` if the value is not an array or contains non-bulk-string
/// elements.
pub fn parse_command(value: RespValue) -> Option<Vec<Vec<u8>>> {
    match value {
        RespValue::Array(Some(items)) => {
            let mut args = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    RespValue::BulkString(Some(data)) => args.push(data),
                    _ => return None,
                }
            }
            Some(args)
        }
        // Inline command support: treat a simple string as a single-arg command
        RespValue::SimpleString(s) => {
            let parts: Vec<Vec<u8>> = s
                .split_whitespace()
                .map(|p| p.as_bytes().to_vec())
                .collect();
            if parts.is_empty() { None } else { Some(parts) }
        }
        _ => None,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn test_parse_simple_string() {
        let data = b"+OK\r\n";
        let mut reader = BufReader::new(Cursor::new(data.to_vec()));
        let val = read_value(&mut reader).await.unwrap();
        assert_eq!(val, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_parse_error() {
        let data = b"-ERR bad\r\n";
        let mut reader = BufReader::new(Cursor::new(data.to_vec()));
        let val = read_value(&mut reader).await.unwrap();
        assert_eq!(val, RespValue::Error("ERR bad".to_string()));
    }

    #[tokio::test]
    async fn test_parse_integer() {
        let data = b":42\r\n";
        let mut reader = BufReader::new(Cursor::new(data.to_vec()));
        let val = read_value(&mut reader).await.unwrap();
        assert_eq!(val, RespValue::Integer(42));
    }

    #[tokio::test]
    async fn test_parse_bulk_string() {
        let data = b"$5\r\nhello\r\n";
        let mut reader = BufReader::new(Cursor::new(data.to_vec()));
        let val = read_value(&mut reader).await.unwrap();
        assert_eq!(val, RespValue::BulkString(Some(b"hello".to_vec())));
    }

    #[tokio::test]
    async fn test_parse_null_bulk() {
        let data = b"$-1\r\n";
        let mut reader = BufReader::new(Cursor::new(data.to_vec()));
        let val = read_value(&mut reader).await.unwrap();
        assert_eq!(val, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_parse_array() {
        let data = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let mut reader = BufReader::new(Cursor::new(data.to_vec()));
        let val = read_value(&mut reader).await.unwrap();
        assert_eq!(
            val,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"GET".to_vec())),
                RespValue::BulkString(Some(b"key".to_vec())),
            ]))
        );
    }

    #[tokio::test]
    async fn test_rejects_deep_nesting() {
        // 200 levels of `*1` exceed the nesting cap. The parser must return a
        // protocol error, not recurse one frame per level until the stack
        // dies — parsing precedes AUTH, so this is remotely reachable
        // pre-auth (WIR-1).
        let mut data = Vec::new();
        for _ in 0..200 {
            data.extend_from_slice(b"*1\r\n");
        }
        data.extend_from_slice(b"+x\r\n");
        let mut reader = BufReader::new(Cursor::new(data));
        let err = read_value(&mut reader).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("nesting depth"),
            "expected depth error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_nesting_within_limit_parses() {
        // Control: a modest nesting depth still parses.
        let data = b"*1\r\n*1\r\n*1\r\n$3\r\nget\r\n";
        let mut reader = BufReader::new(Cursor::new(data.to_vec()));
        let val = read_value(&mut reader).await.unwrap();
        assert!(matches!(val, RespValue::Array(Some(_))));
    }

    #[tokio::test]
    async fn test_rejects_overlong_line() {
        // A type-prefix line past the cap must error instead of buffering an
        // unbounded prefix line (WIR-8). Bulk payloads are read with
        // read_exact and are not affected.
        let data = vec![b'A'; 70 * 1024];
        let mut reader = BufReader::new(Cursor::new(data));
        let err = read_value(&mut reader).await.unwrap_err();
        assert!(
            err.to_string().contains("maximum length"),
            "expected line-length error, got: {err}"
        );
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn test_line_within_limit_parses() {
        // Control: a line just under the cap still parses.
        let mut data = vec![b'+'];
        data.extend(std::iter::repeat_n(b'A', 60 * 1024));
        data.extend_from_slice(b"\r\n");
        let mut reader = BufReader::new(Cursor::new(data));
        let val = read_value(&mut reader).await.unwrap();
        match val {
            RespValue::SimpleString(s) => assert_eq!(s.len(), 60 * 1024),
            other => panic!("expected SimpleString, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_parse_command_extraction() {
        let array = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"SET".to_vec())),
            RespValue::BulkString(Some(b"mykey".to_vec())),
            RespValue::BulkString(Some(b"myvalue".to_vec())),
        ]));
        let args = parse_command(array).unwrap();
        assert_eq!(args.len(), 3);
        assert_eq!(args[0], b"SET");
        assert_eq!(args[1], b"mykey");
        assert_eq!(args[2], b"myvalue");
    }
}
