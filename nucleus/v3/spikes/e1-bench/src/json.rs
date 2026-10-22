//! A minimal hand-rolled JSON writer (card X-1b Work 1: "Output is JSON,
//! written with std (no serde)").
//!
//! Invariants owned here:
//! - J-VALID: [`Json::render`] emits one syntactically valid JSON
//!   document for every value that can be constructed — strings are
//!   escaped (quotes, backslash, and every control byte below 0x20),
//!   floats are never `NaN`/`inf` (those render as `null`, the same
//!   downgrade serde's `arbitrary_precision` avoids by refusing), and
//!   integers never carry a decimal point.
//! - J-ORDER: object key order is the construction order, so a report's
//!   fields read top-down as the workload produced them (diff-friendly).

use std::fmt::Write as _;

/// One JSON value. `Int` and `Num` are separate so byte and txn counts
/// render without a decimal point (J-VALID).
#[derive(Clone, Debug, PartialEq)]
pub enum Json {
    /// `null`.
    Null,
    /// `true` / `false`.
    Bool(bool),
    /// An integer (rendered without a decimal point).
    Int(i64),
    /// A float (rendered with Rust's shortest round-trip formatting).
    Num(f64),
    /// A string (rendered escaped).
    Str(String),
    /// An array.
    Arr(Vec<Json>),
    /// An object with ordered keys.
    Obj(Vec<(String, Json)>),
}

impl Json {
    /// Render the value as one JSON document (J-VALID, J-ORDER).
    pub fn render(&self) -> String {
        let mut out = String::new();
        self.write_into(&mut out);
        out
    }

    /// Append the rendering of `self` to `out` ([`Json::render]'s core).
    fn write_into(&self, out: &mut String) {
        match self {
            Json::Null => out.push_str("null"),
            Json::Bool(true) => out.push_str("true"),
            Json::Bool(false) => out.push_str("false"),
            Json::Int(i) => {
                // i64 Display never produces JSON-invalid output.
                let _ = write!(out, "{i}");
            }
            Json::Num(f) => {
                if f.is_finite() {
                    // Shortest round-trip; `e`/`E` exponents are valid JSON.
                    let _ = write!(out, "{f}");
                } else {
                    out.push_str("null"); // J-VALID: never NaN/inf
                }
            }
            Json::Str(s) => write_escaped(out, s),
            Json::Arr(items) => {
                out.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    item.write_into(out);
                }
                out.push(']');
            }
            Json::Obj(pairs) => {
                out.push('{');
                for (i, (k, v)) in pairs.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write_escaped(out, k);
                    out.push(':');
                    v.write_into(out);
                }
                out.push('}');
            }
        }
    }
}

impl From<bool> for Json {
    fn from(v: bool) -> Self {
        Json::Bool(v)
    }
}

impl From<u64> for Json {
    fn from(v: u64) -> Self {
        // u64 stats fit i64 for every number this bench produces (≤ ~10¹⁰);
        // an overflow degrades to a float rather than wrapping (J-VALID).
        match i64::try_from(v) {
            Ok(i) => Json::Int(i),
            Err(_) => Json::Num(v as f64),
        }
    }
}

impl From<usize> for Json {
    fn from(v: usize) -> Self {
        Json::from(v as u64)
    }
}

impl From<u32> for Json {
    fn from(v: u32) -> Self {
        Json::Int(v as i64)
    }
}

impl From<f64> for Json {
    fn from(v: f64) -> Self {
        Json::Num(v)
    }
}

impl From<&str> for Json {
    fn from(v: &str) -> Self {
        Json::Str(v.to_string())
    }
}

impl From<String> for Json {
    fn from(v: String) -> Self {
        Json::Str(v)
    }
}

/// Append `s` as a JSON string literal (with quotes), escaping what JSON
/// requires (J-VALID). Bytes above 0x7F pass through unchanged — inputs
/// here are UTF-8 Rust strings, and JSON is UTF-8.
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
            '\u{0C}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Build an object from `(&str, Json)` pairs (a readability helper; the
/// key order is the argument order, J-ORDER).
pub fn obj(pairs: Vec<(&str, Json)>) -> Json {
    Json::Obj(pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}

/// Read a top-level field of an object value; `None` for absent keys
/// and non-objects (the report builder's small reader).
pub fn field(v: &Json, key: &str) -> Option<Json> {
    if let Json::Obj(pairs) = v {
        pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{Json, obj};

    /// J-VALID: strings with quotes, backslashes and control characters
    /// render escaped; the whole document is the expected text.
    #[test]
    fn escapes_strings() {
        let v = obj(vec![
            ("model", Json::from("13th Gen \"x\"\\y")),
            ("ctrl", Json::from("a\u{01}b\nc")),
        ]);
        assert_eq!(
            v.render(),
            "{\"model\":\"13th Gen \\\"x\\\"\\\\y\",\"ctrl\":\"a\\u0001b\\nc\"}"
        );
    }

    /// J-VALID: integers keep no decimal point, floats round-trip, and
    /// non-finite floats degrade to null instead of invalid JSON.
    #[test]
    fn renders_numbers() {
        let v = Json::Arr(vec![
            Json::from(7u64),
            Json::from(1.5f64),
            Json::Num(f64::NAN),
        ]);
        assert_eq!(v.render(), "[7,1.5,null]");
    }

    /// J-ORDER: object keys keep their construction order; nesting and
    /// empty containers render correctly.
    #[test]
    fn keeps_key_order() {
        let v = obj(vec![
            ("zz", Json::from(1u64)),
            ("inner", Json::Obj(vec![])),
            ("arr", Json::Arr(vec![])),
        ]);
        assert_eq!(v.render(), "{\"zz\":1,\"inner\":{},\"arr\":[]}");
    }
}
