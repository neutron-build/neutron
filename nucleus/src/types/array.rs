//! PostgreSQL array literals: the text form `{a,"b,c",NULL,{1,2}}` in both
//! directions.
//!
//! Parsing is grammar-driven (quotes, backslash escapes, NULL, nesting), not a
//! split on commas, and typing is the caller's: the parser yields the raw
//! element text and the caller casts each element to the declared type.
//! Output quotes exactly the elements PostgreSQL quotes.

use std::fmt::Write;

use super::Value;

/// One parsed element of an array literal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArrayLit {
    Null,
    Elem(String),
    Sub(Vec<ArrayLit>),
}

fn malformed(s: &str, why: &str) -> String {
    format!("malformed array literal: \"{s}\" ({why})")
}

/// Parse a rectangular `{...}` literal. Dimension decorations are refused:
/// Value::Array cannot retain PostgreSQL lower bounds, so accepting them
/// would silently change subscripting and equality semantics.
pub fn parse_array_literal(input: &str) -> Result<Vec<ArrayLit>, String> {
    let text = input.trim();
    if text.starts_with('[') {
        return Err(malformed(
            input,
            "explicit array dimensions are not supported",
        ));
    }
    let chars: Vec<char> = text.chars().collect();
    if chars.first() != Some(&'{') {
        return Err(malformed(input, "array value must start with \"{\""));
    }
    let mut pos = 0;
    let items = parse_level(&chars, &mut pos, input)?;
    if chars[pos..].iter().any(|c| !c.is_whitespace()) {
        return Err(malformed(input, "junk after closing right brace"));
    }
    array_shape(&items, input)?;
    Ok(items)
}

fn array_shape(items: &[ArrayLit], input: &str) -> Result<Vec<usize>, String> {
    let mut shape = vec![items.len()];
    let mut child_shape = None;
    for item in items {
        let child = match item {
            ArrayLit::Sub(inner) => array_shape(inner, input)?,
            _ => Vec::new(),
        };
        if child.contains(&0) {
            return Err(malformed(
                input,
                "nested empty arrays are not representable",
            ));
        }
        if child_shape
            .as_ref()
            .is_some_and(|expected| expected != &child)
        {
            return Err(malformed(
                input,
                "multidimensional arrays must have matching dimensions",
            ));
        }
        child_shape = Some(child);
    }
    if let Some(child) = child_shape {
        shape.extend(child);
    }
    Ok(shape)
}

fn skip_ws(chars: &[char], pos: &mut usize) {
    while *pos < chars.len() && chars[*pos].is_whitespace() {
        *pos += 1;
    }
}

/// `chars[*pos]` is the opening brace; on return `*pos` is past the matching
/// closing brace.
fn parse_level(chars: &[char], pos: &mut usize, input: &str) -> Result<Vec<ArrayLit>, String> {
    *pos += 1;
    let mut items = Vec::new();
    skip_ws(chars, pos);
    if chars.get(*pos) == Some(&'}') {
        *pos += 1;
        return Ok(items);
    }
    loop {
        skip_ws(chars, pos);
        let Some(&c) = chars.get(*pos) else {
            return Err(malformed(input, "unexpected end of input"));
        };
        match c {
            '{' => items.push(ArrayLit::Sub(parse_level(chars, pos, input)?)),
            '"' => {
                *pos += 1;
                let mut elem = String::new();
                loop {
                    match chars.get(*pos) {
                        None => return Err(malformed(input, "unexpected end of input")),
                        Some('"') => {
                            *pos += 1;
                            break;
                        }
                        Some('\\') => {
                            *pos += 1;
                            match chars.get(*pos) {
                                Some(&escaped) => elem.push(escaped),
                                None => return Err(malformed(input, "unexpected end of input")),
                            }
                            *pos += 1;
                        }
                        Some(&other) => {
                            elem.push(other);
                            *pos += 1;
                        }
                    }
                }
                items.push(ArrayLit::Elem(elem));
            }
            ',' | '}' => return Err(malformed(input, "unexpected array element")),
            _ => {
                let mut elem = String::new();
                let mut escaped = false;
                let mut significant_end = 0;
                while let Some(&c) = chars.get(*pos) {
                    match c {
                        ',' | '}' => break,
                        '\\' => {
                            *pos += 1;
                            match chars.get(*pos) {
                                Some(&c) => {
                                    elem.push(c);
                                    escaped = true;
                                    significant_end = elem.len();
                                }
                                None => return Err(malformed(input, "unexpected end of input")),
                            }
                            *pos += 1;
                        }
                        '"' | '{' => return Err(malformed(input, "unexpected array element")),
                        other => {
                            elem.push(other);
                            if !other.is_whitespace() {
                                significant_end = elem.len();
                            }
                            *pos += 1;
                        }
                    }
                }
                let trimmed = &elem[..significant_end];
                if !escaped && trimmed.eq_ignore_ascii_case("null") {
                    items.push(ArrayLit::Null);
                } else {
                    items.push(ArrayLit::Elem(trimmed.to_string()));
                }
            }
        }
        skip_ws(chars, pos);
        match chars.get(*pos) {
            Some(',') => *pos += 1,
            Some('}') => {
                *pos += 1;
                return Ok(items);
            }
            _ => return Err(malformed(input, "unexpected end of input")),
        }
    }
}

/// Validate array values before execution or storage, not only at wire encoding.
pub fn validate_array_shape(items: &[Value]) -> Result<Vec<usize>, String> {
    fn check_types(items: &[Value], kind: &mut Option<&'static str>) -> Result<(), String> {
        for item in items {
            match item {
                Value::Null => {}
                Value::Array(inner) => check_types(inner, kind)?,
                other => {
                    let next = match other {
                        Value::Int32(_) | Value::Int64(_) => "integer",
                        _ => other.type_name(),
                    };
                    if kind.is_some_and(|previous| previous != next) {
                        return Err("mixed array element types are not supported; cast every element to the same type".into());
                    }
                    *kind = Some(next);
                }
            }
        }
        Ok(())
    }
    check_types(items, &mut None)?;
    let mut shape = vec![items.len()];
    let mut child_shape = None;
    for item in items {
        let child = match item {
            Value::Array(inner) => validate_array_shape(inner)?,
            _ => Vec::new(),
        };
        if child.contains(&0) {
            return Err("nested empty arrays are not representable".into());
        }
        if child_shape
            .as_ref()
            .is_some_and(|expected| expected != &child)
        {
            return Err("multidimensional arrays must have matching dimensions".into());
        }
        child_shape = Some(child);
    }
    if let Some(child) = child_shape {
        shape.extend(child);
    }
    Ok(shape)
}

/// Build a `Value::Array` from parsed elements, casting each element's text
/// through `cast`. Nested arrays become nested `Value::Array`s.
pub fn array_value_from_literal(
    items: &[ArrayLit],
    cast: &mut dyn FnMut(&str) -> Result<Value, String>,
) -> Result<Value, String> {
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        out.push(match item {
            ArrayLit::Null => Value::Null,
            ArrayLit::Elem(text) => cast(text)?,
            ArrayLit::Sub(inner) => array_value_from_literal(inner, cast)?,
        });
    }
    Ok(Value::Array(out))
}

/// Element type-less reading of an element, for arrays whose declared element
/// type is not known (an uncast `= ANY('{1,2}')` operand): integers and floats
/// are recognised, everything else stays text.
pub fn guess_array_element(text: &str) -> Value {
    if let Ok(n) = text.parse::<i64>() {
        Value::Int64(n)
    } else if let Ok(f) = text.parse::<f64>() {
        Value::Float64(f)
    } else {
        Value::Text(text.to_string())
    }
}

fn needs_quoting(text: &str) -> bool {
    text.is_empty()
        || text.eq_ignore_ascii_case("null")
        || text
            .chars()
            .any(|c| matches!(c, '{' | '}' | ',' | '"' | '\\') || c.is_whitespace())
}

/// PostgreSQL's text output for an array; `render` gives the text of a
/// non-array, non-NULL element (a timestamptz needs the session zone, which
/// only the caller knows).
pub fn format_array(vals: &[Value], render: &dyn Fn(&Value) -> String) -> String {
    let mut out = String::from("{");
    for (i, v) in vals.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        match v {
            Value::Null => out.push_str("NULL"),
            Value::Array(inner) => out.push_str(&format_array(inner, render)),
            other => {
                let text = render(other);
                if needs_quoting(&text) {
                    out.push('"');
                    for c in text.chars() {
                        if c == '"' || c == '\\' {
                            out.push('\\');
                        }
                        out.push(c);
                    }
                    out.push('"');
                } else {
                    let _ = out.write_str(&text);
                }
            }
        }
    }
    out.push('}');
    out
}

/// An element as it appears inside an array: booleans are `t`/`f`.
pub fn array_element_text(v: &Value) -> String {
    match v {
        Value::Bool(b) => if *b { "t" } else { "f" }.to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn x09_escaped_unquoted_null_and_whitespace_remain_text() {
        assert_eq!(
            elems(parse_array_literal(r"{N\ULL,a\ }").unwrap()),
            vec![Some("NULL".into()), Some("a ".into())]
        );
    }

    #[test]
    fn x09_reject_unrepresentable_dimensions_and_ragged_literals() {
        for input in [
            "[0:1]={7,9}",
            "[1:999]={7,9}",
            "[junk]={7,9}",
            "{{1},{2,3}}",
            "{1,{2}}",
            "{{},{}}",
        ] {
            assert!(parse_array_literal(input).is_err(), "{input}");
        }
        assert!(parse_array_literal("{{1,2},{3,4}}").is_ok());
    }

    fn elems(items: Vec<ArrayLit>) -> Vec<Option<String>> {
        items
            .into_iter()
            .map(|i| match i {
                ArrayLit::Null => None,
                ArrayLit::Elem(s) => Some(s),
                ArrayLit::Sub(_) => panic!("nested"),
            })
            .collect()
    }

    #[test]
    fn parses_quotes_escapes_and_null() {
        let items = parse_array_literal(r#"{a,"b,c","d\"e",NULL,"","NULL", x y }"#).unwrap();
        assert_eq!(
            elems(items),
            vec![
                Some("a".into()),
                Some("b,c".into()),
                Some("d\"e".into()),
                None,
                Some("".into()),
                Some("NULL".into()),
                Some("x y".into()),
            ]
        );
    }

    #[test]
    fn parses_empty_and_nested() {
        assert!(parse_array_literal("{}").unwrap().is_empty());
        assert!(parse_array_literal("  { }  ").unwrap().is_empty());
        let nested = parse_array_literal("{{1,2},{3,4}}").unwrap();
        assert_eq!(nested.len(), 2);
        assert!(matches!(&nested[0], ArrayLit::Sub(inner) if inner.len() == 2));
    }

    #[test]
    fn rejects_malformed() {
        for bad in [
            "1,2", "{1,2", "{1,,2}", "{,}", "{1,2}}", "{\"a}", "{a\"b\"}", "",
        ] {
            assert!(parse_array_literal(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn formats_like_postgres() {
        let v = vec![
            Value::Text("a".into()),
            Value::Text("b,c".into()),
            Value::Text("d\"e".into()),
            Value::Null,
            Value::Text(String::new()),
            Value::Text("NULL".into()),
            Value::Text("x y".into()),
            Value::Text("back\\slash".into()),
        ];
        assert_eq!(
            format_array(&v, &array_element_text),
            r#"{a,"b,c","d\"e",NULL,"","NULL","x y","back\\slash"}"#
        );
        let bools = vec![Value::Bool(true), Value::Bool(false)];
        assert_eq!(format_array(&bools, &array_element_text), "{t,f}");
        let nested = vec![
            Value::Array(vec![Value::Int32(1), Value::Int32(2)]),
            Value::Array(vec![Value::Int32(3), Value::Int32(4)]),
        ];
        assert_eq!(format_array(&nested, &array_element_text), "{{1,2},{3,4}}");
    }

    #[test]
    fn round_trips_hostile_text() {
        let original = vec![
            Value::Text("a,b".into()),
            Value::Text("\"q\"".into()),
            Value::Text("{x}".into()),
            Value::Text("  padded ".into()),
            Value::Text("\\".into()),
        ];
        let text = format_array(&original, &array_element_text);
        let parsed = array_value_from_literal(&parse_array_literal(&text).unwrap(), &mut |s| {
            Ok(Value::Text(s.to_string()))
        })
        .unwrap();
        assert_eq!(parsed, Value::Array(original));
    }
}
