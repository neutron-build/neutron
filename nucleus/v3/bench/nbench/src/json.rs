//! Minimal JSON reader/writer for nbench results, configs and Qdrant payloads.
//!
//! Invariants owned here (card B-0: no new dependencies):
//! - The parser accepts exactly the JSON grammar it emits plus whitespace,
//!   rejects trailing garbage and truncated documents, and never panics on
//!   any input (PLAN-v3 §6.1: no panics on input-reachable paths).
//! - Object key lookup answers with the *first* occurrence of a key; the
//!   serializer preserves insertion order, so result files are reproducible
//!   byte-for-byte for the same data (§5.2 rule 6, provenance stability).
//! - Numbers round-trip through Rust `f64` shortest-repr formatting.

use crate::error::{Error, Result};
use std::fmt::Write as _;

/// A JSON value with insertion-ordered objects.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// `null`
    Null,
    /// `true` / `false`
    Bool(bool),
    /// Any JSON number, held as `f64` (integers up to 2^53 round-trip exactly).
    Num(f64),
    /// A string.
    Str(String),
    /// An array.
    Arr(Vec<Value>),
    /// An object as ordered key/value pairs.
    Obj(Vec<(String, Value)>),
}

impl Value {
    /// First-match object field lookup.
    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Obj(pairs) => pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    /// Object accessor returning a copy of the pairs.
    pub fn pairs(&self) -> Option<&[(String, Value)]> {
        match self {
            Value::Obj(pairs) => Some(pairs),
            _ => None,
        }
    }

    /// Borrow as a number.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Num(n) => Some(*n),
            _ => None,
        }
    }

    /// Borrow as a string slice.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }

    /// Borrow as a boolean.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Borrow as an array slice.
    pub fn as_arr(&self) -> Option<&[Value]> {
        match self {
            Value::Arr(a) => Some(a),
            _ => None,
        }
    }

    /// Build an object from key/value pairs (helper for result documents).
    pub fn obj(pairs: Vec<(&str, Value)>) -> Value {
        Value::Obj(pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
    }

    /// Build a string value.
    pub fn s(v: &str) -> Value {
        Value::Str(v.to_string())
    }

    /// Build a number value.
    pub fn n(v: f64) -> Value {
        Value::Num(v)
    }
}

/// Escape a string per JSON and append it to `out`.
fn write_escaped(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn write_value(out: &mut String, v: &Value) -> Result<()> {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Num(n) => {
            if !n.is_finite() {
                return Err(Error::Json(format!("cannot serialize {n} as JSON")));
            }
            // Rust's Display for f64 prints the shortest string that
            // round-trips, and prints integral values without a fraction.
            let _ = write!(out, "{n}");
        }
        Value::Str(s) => write_escaped(out, s),
        Value::Arr(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_value(out, item)?;
            }
            out.push(']');
        }
        Value::Obj(pairs) => {
            out.push('{');
            for (i, (k, val)) in pairs.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_escaped(out, k);
                out.push(':');
                write_value(out, val)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

/// Serialize a value to a JSON string (compact form).
pub fn to_string(v: &Value) -> Result<String> {
    let mut out = String::new();
    write_value(&mut out, v)?;
    Ok(out)
}

struct Parser<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn err(&self, msg: &str) -> Error {
        Error::Json(format!("{msg} at byte {}", self.pos))
    }

    fn peek(&self) -> Option<u8> {
        self.b.get(self.pos).copied()
    }

    fn skip_ws(&mut self) {
        while let Some(c) = self.peek() {
            if c == b' ' || c == b'\t' || c == b'\n' || c == b'\r' {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn expect(&mut self, c: u8) -> Result<()> {
        if self.peek() == Some(c) {
            self.pos += 1;
            Ok(())
        } else {
            Err(self.err(&format!("expected '{}'", c as char)))
        }
    }

    fn parse_value(&mut self) -> Result<Value> {
        self.skip_ws();
        match self.peek() {
            Some(b'{') => self.parse_obj(),
            Some(b'[') => self.parse_arr(),
            Some(b'"') => Ok(Value::Str(self.parse_string()?)),
            Some(b't') => self.parse_lit("true", Value::Bool(true)),
            Some(b'f') => self.parse_lit("false", Value::Bool(false)),
            Some(b'n') => self.parse_lit("null", Value::Null),
            Some(c) if c == b'-' || c.is_ascii_digit() => self.parse_num(),
            _ => Err(self.err("unexpected character")),
        }
    }

    fn parse_lit(&mut self, lit: &str, v: Value) -> Result<Value> {
        if self.b[self.pos..].starts_with(lit.as_bytes()) {
            self.pos += lit.len();
            Ok(v)
        } else {
            Err(self.err("invalid literal"))
        }
    }

    fn parse_num(&mut self) -> Result<Value> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        while let Some(c) = self.peek() {
            if c.is_ascii_digit() {
                self.pos += 1;
            } else {
                break;
            }
        }
        let mut frac = false;
        if self.peek() == Some(b'.') {
            frac = true;
            self.pos += 1;
            while let Some(c) = self.peek() {
                if c.is_ascii_digit() {
                    self.pos += 1;
                } else {
                    break;
                }
            }
        }
        if matches!(self.peek(), Some(b'e') | Some(b'E')) {
            frac = true;
            self.pos += 1;
            if matches!(self.peek(), Some(b'+') | Some(b'-')) {
                self.pos += 1;
            }
            while let Some(c) = self.peek() {
                if c.is_ascii_digit() {
                    self.pos += 1;
                } else {
                    break;
                }
            }
        }
        let text = std::str::from_utf8(&self.b[start..self.pos])
            .map_err(|_| self.err("bad utf8 in number"))?;
        let v: f64 = text.parse().map_err(|_| self.err("invalid number"))?;
        let _ = frac; // accepted grammar only; f64 handles both forms
        Ok(Value::Num(v))
    }

    fn parse_hex4(&mut self) -> Result<u32> {
        let mut v: u32 = 0;
        for _ in 0..4 {
            let c = self
                .peek()
                .ok_or_else(|| self.err("truncated \\u escape"))?;
            let d = (c as char)
                .to_digit(16)
                .ok_or_else(|| self.err("bad hex digit"))?;
            v = v * 16 + d;
            self.pos += 1;
        }
        Ok(v)
    }

    fn parse_string(&mut self) -> Result<String> {
        self.expect(b'"')?;
        let mut out = String::new();
        loop {
            let c = self.peek().ok_or_else(|| self.err("unterminated string"))?;
            match c {
                b'"' => {
                    self.pos += 1;
                    return Ok(out);
                }
                b'\\' => {
                    self.pos += 1;
                    let esc = self.peek().ok_or_else(|| self.err("truncated escape"))?;
                    self.pos += 1;
                    match esc {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'b' => out.push('\u{08}'),
                        b'f' => out.push('\u{0c}'),
                        b'u' => {
                            let hi = self.parse_hex4()?;
                            let cp = if (0xD800..0xDC00).contains(&hi) {
                                // surrogate pair
                                if self.peek() == Some(b'\\') {
                                    self.pos += 1;
                                    if self.peek() == Some(b'u') {
                                        self.pos += 1;
                                        let lo = self.parse_hex4()?;
                                        if !(0xDC00..0xE000).contains(&lo) {
                                            return Err(self.err("bad low surrogate"));
                                        }
                                        0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00)
                                    } else {
                                        return Err(self.err("lone surrogate"));
                                    }
                                } else {
                                    return Err(self.err("lone surrogate"));
                                }
                            } else {
                                hi
                            };
                            out.push(char::from_u32(cp).ok_or_else(|| self.err("bad code point"))?);
                        }
                        _ => return Err(self.err("unknown escape")),
                    }
                }
                _ => {
                    // consume one UTF-8 encoded char
                    let s = &self.b[self.pos..];
                    let ch_len = utf8_len(s).ok_or_else(|| self.err("bad utf8"))?;
                    let chunk = &s[..ch_len];
                    let ch = std::str::from_utf8(chunk)
                        .map_err(|_| self.err("bad utf8"))?
                        .chars()
                        .next();
                    match ch {
                        Some(ch) => out.push(ch),
                        None => return Err(self.err("bad utf8")),
                    }
                    self.pos += ch_len;
                }
            }
        }
    }

    fn parse_arr(&mut self) -> Result<Value> {
        self.expect(b'[')?;
        let mut items = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.pos += 1;
            return Ok(Value::Arr(items));
        }
        loop {
            items.push(self.parse_value()?);
            self.skip_ws();
            match self.peek() {
                Some(b',') => {
                    self.pos += 1;
                }
                Some(b']') => {
                    self.pos += 1;
                    return Ok(Value::Arr(items));
                }
                _ => return Err(self.err("expected ',' or ']'")),
            }
        }
    }

    fn parse_obj(&mut self) -> Result<Value> {
        self.expect(b'{')?;
        let mut pairs: Vec<(String, Value)> = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(Value::Obj(pairs));
        }
        loop {
            self.skip_ws();
            let k = self.parse_string()?;
            self.skip_ws();
            self.expect(b':')?;
            let v = self.parse_value()?;
            pairs.push((k, v));
            self.skip_ws();
            match self.peek() {
                Some(b',') => {
                    self.pos += 1;
                }
                Some(b'}') => {
                    self.pos += 1;
                    return Ok(Value::Obj(pairs));
                }
                _ => return Err(self.err("expected ',' or '}'")),
            }
        }
    }
}

fn utf8_len(b: &[u8]) -> Option<usize> {
    match b.first() {
        Some(c) if *c < 0x80 => Some(1),
        Some(c) if *c & 0xE0 == 0xC0 => Some(2),
        Some(c) if *c & 0xF0 == 0xE0 => Some(3),
        Some(c) if *c & 0xF8 == 0xF0 => Some(4),
        _ => None,
    }
}

/// Parse a complete JSON document; trailing content is an error.
pub fn parse(text: &str) -> Result<Value> {
    let mut p = Parser {
        b: text.as_bytes(),
        pos: 0,
    };
    let v = p.parse_value()?;
    p.skip_ws();
    if p.pos != text.len() {
        return Err(p.err("trailing content"));
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_nested_document() {
        let v = Value::obj(vec![
            ("name", Value::s("oltp-pgbench-pg17")),
            (
                "series",
                Value::Arr(vec![
                    Value::obj(vec![
                        ("clients", Value::n(1.0)),
                        ("tps", Value::n(1234.5)),
                        ("ok", Value::Bool(true)),
                    ]),
                    Value::obj(vec![("clients", Value::n(16.0)), ("tps", Value::Null)]),
                ]),
            ),
        ]);
        let text = to_string(&v).expect("serialize");
        let back = parse(&text).expect("parse");
        assert_eq!(v, back);
    }

    #[test]
    fn escapes_roundtrip() {
        let v = Value::Str("quote\" back\\ newline\n tab\t ctrl\u{01}".into());
        let text = to_string(&v).expect("serialize");
        assert_eq!(text, "\"quote\\\" back\\\\ newline\\n tab\\t ctrl\\u0001\"");
        assert_eq!(parse(&text).expect("parse"), v);
    }

    #[test]
    fn surrogate_pairs() {
        let v = parse("\"\\u00e9\\ud834\\udd1e\"").expect("parse");
        assert_eq!(v, Value::Str("\u{e9}\u{1D11E}".into()));
    }

    #[test]
    fn numbers_shortest_roundtrip() {
        for n in [0.0, 1.0, -2.5, 1e300, 0.1, 123456789.0, 3.7339132771] {
            let text = to_string(&Value::Num(n)).expect("serialize");
            let back = parse(&text).expect("parse");
            assert_eq!(back, Value::Num(n), "{text}");
        }
        // integral doubles print without a fraction
        assert_eq!(to_string(&Value::Num(16.0)).expect("serialize"), "16");
    }

    #[test]
    fn rejects_malformed() {
        for bad in [
            "",
            "{",
            "{\"a\":}",
            "[1,]",
            "{\"a\" 1}",
            "tru",
            "\"unterminated",
            "[1] trailing",
            "{\"a\":1,}",
            "NaN",
            "1e",
            "\"\\u12\"",
        ] {
            assert!(parse(bad).is_err(), "expected error for {bad:?}");
        }
    }

    #[test]
    fn whitespace_tolerated_everywhere() {
        let v = parse("  { \"a\" : [ 1 , 2 ] , \"b\" : null }  ").expect("parse");
        assert_eq!(
            v,
            Value::obj(vec![
                ("a", Value::Arr(vec![Value::n(1.0), Value::n(2.0)])),
                ("b", Value::Null),
            ])
        );
    }

    #[test]
    fn duplicate_keys_first_wins_on_lookup() {
        let v = parse("{\"k\":1,\"k\":2}").expect("parse");
        assert_eq!(v.get("k").and_then(Value::as_f64), Some(1.0));
        assert_eq!(v.pairs().map(<[_]>::len), Some(2));
    }

    #[test]
    fn accessors_reject_wrong_types() {
        let v = parse("{\"a\":\"s\",\"n\":3,\"b\":true,\"arr\":[1]}").expect("parse");
        assert_eq!(v.get("a").and_then(Value::as_f64), None);
        assert_eq!(v.get("n").and_then(Value::as_str), None);
        assert_eq!(v.get("b").and_then(Value::as_bool), Some(true));
        assert_eq!(
            v.get("arr").and_then(Value::as_arr).map(<[_]>::len),
            Some(1)
        );
        assert_eq!(v.get("missing"), None);
    }

    #[test]
    fn non_finite_rejected_on_serialize() {
        assert!(to_string(&Value::Num(f64::NAN)).is_err());
        assert!(to_string(&Value::Num(f64::INFINITY)).is_err());
    }
}
