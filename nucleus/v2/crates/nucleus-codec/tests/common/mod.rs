//! Shared literal parsers for the golden corpus (golden.rs) and its
//! generator (gen_corpus.rs). Each integration test binary compiles this
//! module separately and uses a subset of it.
#![allow(dead_code)]

use nucleus_codec::{
    Array, ArrayDim, Collation, Decimal, Interval, Jsonb, KeyColumn, KeyType, Numeric, Value,
};

pub const USECS_PER_DAY: i64 = 86_400_000_000;
/// 2000-01-01 in days since 1970-01-01 (PG's date epoch).
pub const DAYS_2000_01_01: i64 = 10_957;

/// Days since 1970-01-01 in the proleptic Gregorian calendar
/// (Howard Hinnant's `days_from_civil`). Valid for any i64 date.
pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = if m > 2 { m - 3 } else { m + 9 }; // [0, 11]
    let doy = (153 * mp + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// The `KeyType` a golden file name stands for (scalar files + int4[]).
pub fn type_for(stem: &str) -> KeyType {
    match stem {
        "array" => KeyType::Array(Box::new(KeyType::Int4)),
        "composite" => panic!("composite has three columns, not one"),
        _ => scalar_type(stem),
    }
}

/// The key column(s) of a golden file: `ASC NULLS LAST` (PostgreSQL's
/// default), one column per scalar type, three for the composite file.
pub fn columns_for(stem: &str) -> Vec<KeyColumn> {
    match stem {
        "composite" => vec![
            KeyColumn::asc(KeyType::Int4),
            KeyColumn::asc(KeyType::Text(Collation::C)),
            KeyColumn::asc(KeyType::Float8),
        ],
        _ => vec![KeyColumn::asc(type_for(stem))],
    }
}

/// The scalar `KeyType` a golden file name stands for.
pub fn scalar_type(stem: &str) -> KeyType {
    match stem {
        "bool" => KeyType::Bool,
        "int2" => KeyType::Int2,
        "int4" => KeyType::Int4,
        "int8" => KeyType::Int8,
        "float4" => KeyType::Float4,
        "float8" => KeyType::Float8,
        "numeric" => KeyType::Numeric,
        "text" | "varchar" => KeyType::Text(Collation::C),
        "bytea" => KeyType::Bytea,
        "date" => KeyType::Date,
        "time" => KeyType::Time,
        "timestamp" => KeyType::Timestamp,
        "timestamptz" => KeyType::TimestampTz,
        "interval" => KeyType::Interval,
        "uuid" => KeyType::Uuid,
        "jsonb" => KeyType::Jsonb,
        other => panic!("unknown golden type: {other}"),
    }
}

/// Parse one `sql_literal` cell into the values of one golden row.
/// `NULL` (unquoted) is a NULL in every column position.
pub fn parse_row(stem: &str, lit: &str) -> Vec<Option<Value>> {
    if stem == "composite" {
        return parse_composite(lit);
    }
    let ty = type_for(stem);
    vec![if lit == "NULL" {
        None
    } else {
        Some(parse_value(&ty, lit))
    }]
}

fn parse_value(ty: &KeyType, lit: &str) -> Value {
    match ty {
        KeyType::Bool => Value::Bool(parse_bool(lit)),
        KeyType::Int2 => Value::Int2(lit.parse().unwrap_or_else(|e| panic!("{lit}: {e}"))),
        KeyType::Int4 => Value::Int4(lit.parse().unwrap_or_else(|e| panic!("{lit}: {e}"))),
        KeyType::Int8 => Value::Int8(lit.parse().unwrap_or_else(|e| panic!("{lit}: {e}"))),
        KeyType::Float4 => Value::Float4(parse_f32(lit)),
        KeyType::Float8 => Value::Float8(parse_f64(lit)),
        KeyType::Numeric => {
            Value::Numeric(Numeric::parse(lit).unwrap_or_else(|| panic!("{lit}: bad numeric")))
        }
        KeyType::Text(_) => Value::Text(parse_quoted(lit)),
        KeyType::Bytea => Value::Bytea(parse_bytea(lit)),
        KeyType::Date => Value::Date(parse_date(lit)),
        KeyType::Time => Value::Time(parse_time(lit)),
        KeyType::Timestamp => Value::Timestamp(parse_timestamp(lit, false)),
        KeyType::TimestampTz => Value::TimestampTz(parse_timestamp(lit, true)),
        KeyType::Interval => Value::Interval(parse_interval(lit)),
        KeyType::Uuid => Value::Uuid(parse_uuid(lit)),
        KeyType::Jsonb => Value::Jsonb(parse_jsonb(lit)),
        KeyType::Array(elem) => {
            if matches!(**elem, KeyType::Int4) {
                Value::Array(parse_int4_array(lit))
            } else {
                panic!("golden arrays are int4[]; got {lit}")
            }
        }
    }
}

fn parse_bool(lit: &str) -> bool {
    match lit.to_ascii_lowercase().as_str() {
        "t" | "true" | "y" | "yes" | "on" | "1" => true,
        "f" | "false" | "n" | "no" | "off" | "0" => false,
        _ => panic!("{lit}: bad bool"),
    }
}

fn parse_f32(lit: &str) -> f32 {
    let f: f32 = lit.parse().unwrap_or_else(|e| panic!("{lit}: {e}"));
    if f.is_nan() {
        f32::NAN
    } else {
        f
    }
}

fn parse_f64(lit: &str) -> f64 {
    let f: f64 = lit.parse().unwrap_or_else(|e| panic!("{lit}: {e}"));
    if f.is_nan() {
        f64::NAN
    } else {
        f
    }
}

/// `'...'` with `''` for an embedded quote (standard_conforming_strings).
fn parse_quoted(lit: &str) -> String {
    let inner = lit
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
        .unwrap_or_else(|| panic!("{lit}: not quoted"));
    let mut out = String::new();
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        if c == '\'' {
            assert_eq!(Some('\''), chars.next(), "{lit}: unterminated quote escape");
        }
        out.push(c);
    }
    out
}

/// PG bytea hex format: `\x` + hex digits.
fn parse_bytea(lit: &str) -> Vec<u8> {
    let hex = lit
        .strip_prefix("\\x")
        .unwrap_or_else(|| panic!("{lit}: not \\x"));
    assert_eq!(0, hex.len() % 2, "{lit}: odd hex");
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap_or_else(|e| panic!("{lit}: {e}")))
        .collect()
}

fn parse_date(lit: &str) -> i32 {
    if lit == "-infinity" {
        return i32::MIN;
    }
    if lit == "infinity" {
        return i32::MAX;
    }
    let (y, m, d) = split3(lit, '-').unwrap_or_else(|| panic!("{lit}: bad date"));
    let days = days_from_civil(y, m, d) - DAYS_2000_01_01;
    i32::try_from(days).unwrap_or_else(|e| panic!("{lit}: {e}"))
}

/// `H:M:S[.f]`, `24:00:00` included.
fn parse_time(lit: &str) -> i64 {
    let mut it = lit.split(':');
    let h: i64 = it
        .next()
        .unwrap_or_else(|| panic!("{lit}"))
        .parse()
        .unwrap_or_else(|e| panic!("{lit}: {e}"));
    let m: i64 = it
        .next()
        .unwrap_or_else(|| panic!("{lit}"))
        .parse()
        .unwrap_or_else(|e| panic!("{lit}: {e}"));
    let sstr = it.next().unwrap_or_else(|| panic!("{lit}: no seconds"));
    let (s, frac) = match sstr.split_once('.') {
        Some((s, f)) => (s, Some(f)),
        None => (sstr, None),
    };
    let s: i64 = s.parse().unwrap_or_else(|e| panic!("{lit}: {e}"));
    let micros = frac
        .map(|f| {
            let padded = format!("{f:0<6}");
            padded[..6]
                .parse::<i64>()
                .unwrap_or_else(|e| panic!("{lit}: {e}"))
        })
        .unwrap_or(0);
    (h * 3600 + m * 60 + s) * 1_000_000 + micros
}

/// Timestamp / timestamptz literal. A timestamptz carries a UTC offset
/// (`+HH`, `+HH:MM`, `-HH:MM`); the value stored is the UTC instant.
fn parse_timestamp(lit: &str, tz: bool) -> i64 {
    if lit == "-infinity" {
        return i64::MIN;
    }
    if lit == "infinity" {
        return i64::MAX;
    }
    let (front, offset) = if tz {
        match lit.rfind(['+', '-'].as_ref() as &[char]) {
            Some(i) if i > 10 => (&lit[..i], &lit[i..]),
            _ => panic!("{lit}: timestamptz literal needs an offset"),
        }
    } else {
        (lit, "")
    };
    let (date, time) = match front.split_once(' ') {
        Some((d, t)) => (d, t),
        None => (front, "00:00:00"),
    };
    let days = parse_date(date) as i64;
    let local = days * USECS_PER_DAY + parse_time(time);
    if offset.is_empty() {
        return local;
    }
    let sign = if offset.starts_with('-') { -1 } else { 1 };
    let ohm = &offset[1..];
    let off = match ohm.split_once(':') {
        Some((h, m)) => h.parse::<i64>().unwrap_or(0) * 3600 + m.parse::<i64>().unwrap_or(0) * 60,
        None => ohm.parse::<i64>().unwrap_or(0) * 3600,
    };
    local - sign * off * 1_000_000
}

fn split3(s: &str, sep: char) -> Option<(i64, i64, i64)> {
    let mut it = s.split(sep);
    let a = it.next()?.parse().ok()?;
    let b = it.next()?.parse().ok()?;
    let c = it.next()?.parse().ok()?;
    if it.next().is_some() {
        return None;
    }
    Some((a, b, c))
}

/// Interval literal: `infinity` / `-infinity` / `0` / a sum of signed
/// number+unit tokens (`2 years -3 days 01:02:03` style without the
/// hh:mm:ss form).
fn parse_interval(lit: &str) -> Interval {
    if lit == "-infinity" {
        return Interval::NEG_INFINITY;
    }
    if lit == "infinity" {
        return Interval::INFINITY;
    }
    if lit == "0" {
        return Interval {
            months: 0,
            days: 0,
            micros: 0,
        };
    }
    let mut months: i64 = 0;
    let mut days: i64 = 0;
    let mut micros: i64 = 0;
    let toks: Vec<&str> = lit.split_whitespace().collect();
    let mut i = 0;
    while i < toks.len() {
        let n: i64 = toks[i].parse().unwrap_or_else(|e| panic!("{lit}: {e}"));
        i += 1;
        let unit = toks
            .get(i)
            .unwrap_or_else(|| panic!("{lit}: unitless number"))
            .to_ascii_lowercase();
        i += 1;
        match unit.as_str() {
            "year" | "years" | "yr" | "yrs" => months += 12 * n,
            "mon" | "mons" | "month" | "months" => months += n,
            "week" | "weeks" | "w" => days += 7 * n,
            "day" | "days" | "d" => days += n,
            "hour" | "hours" | "hr" | "hrs" | "h" => micros += n * 3_600_000_000,
            "minute" | "minutes" | "min" | "mins" | "m" => micros += n * 60_000_000,
            "second" | "seconds" | "sec" | "secs" | "s" => micros += n * 1_000_000,
            "millisecond" | "milliseconds" | "ms" => micros += n * 1_000,
            "microsecond" | "microseconds" | "us" => micros += n,
            _ => panic!("{lit}: unknown unit {unit}"),
        }
    }
    Interval {
        months: i32::try_from(months).unwrap_or_else(|e| panic!("{lit}: {e}")),
        days: i32::try_from(days).unwrap_or_else(|e| panic!("{lit}: {e}")),
        micros,
    }
}

/// Canonical textual UUID (PG also accepts variants; the corpus does not).
fn parse_uuid(lit: &str) -> [u8; 16] {
    let b = lit.as_bytes();
    assert_eq!(36, b.len(), "{lit}: uuid length");
    for (i, &c) in b.iter().enumerate() {
        let dash = i == 8 || i == 13 || i == 18 || i == 23;
        assert_eq!(dash, c == b'-', "{lit}: dash at {i}");
    }
    let hex: String = lit.chars().filter(|c| *c != '-').collect();
    let mut out = [0u8; 16];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte =
            u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).unwrap_or_else(|e| panic!("{lit}: {e}"));
    }
    out
}

/// `(int,text,float8)` composite literal. Text elements are `'...'` quoted
/// when they contain spaces; the corpus contains no commas inside elements.
fn parse_composite(lit: &str) -> Vec<Option<Value>> {
    let inner = lit
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .unwrap_or_else(|| panic!("{lit}: not (...)"));
    let parts: Vec<&str> = inner.split(',').map(str::trim).collect();
    assert_eq!(3, parts.len(), "{lit}: composite has 3 columns");
    let c0 = if parts[0] == "NULL" {
        None
    } else {
        Some(Value::Int4(
            parts[0].parse().unwrap_or_else(|e| panic!("{lit}: {e}")),
        ))
    };
    let c1 = if parts[1] == "NULL" {
        None
    } else if parts[1].starts_with('\'') {
        Some(Value::Text(parse_quoted(parts[1])))
    } else {
        Some(Value::Text(parts[1].to_string()))
    };
    let c2 = if parts[2] == "NULL" {
        None
    } else {
        Some(Value::Float8(parse_f64(parts[2])))
    };
    vec![c0, c1, c2]
}

// ---- jsonb ----

struct JParser<'a> {
    b: &'a [u8],
    i: usize,
}

fn parse_jsonb(lit: &str) -> Jsonb {
    let mut p = JParser {
        b: lit.as_bytes(),
        i: 0,
    };
    let j = p.value();
    p.ws();
    assert_eq!(p.i, p.b.len(), "{lit}: trailing jsonb input");
    j
}

impl JParser<'_> {
    fn ws(&mut self) {
        while matches!(self.b.get(self.i), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.i += 1;
        }
    }
    fn eat(&mut self, c: u8) {
        assert_eq!(Some(&c), self.b.get(self.i), "expected {:?}", c as char);
        self.i += 1;
    }
    fn lit(&mut self, word: &str) {
        assert!(
            self.b[self.i..].starts_with(word.as_bytes()),
            "expected {word}"
        );
        self.i += word.len();
    }
    fn value(&mut self) -> Jsonb {
        self.ws();
        match *self
            .b
            .get(self.i)
            .unwrap_or_else(|| panic!("jsonb cut short"))
        {
            b'n' => {
                self.lit("null");
                Jsonb::Null
            }
            b't' => {
                self.lit("true");
                Jsonb::Bool(true)
            }
            b'f' => {
                self.lit("false");
                Jsonb::Bool(false)
            }
            b'"' => Jsonb::String(self.string()),
            b'-' | b'0'..=b'9' => Jsonb::Number(Numeric::Finite(self.number())),
            b'[' => {
                self.eat(b'[');
                let mut items = Vec::new();
                self.ws();
                if *self.b.get(self.i).unwrap_or(&b']') != b']' {
                    items.push(self.value());
                    self.ws();
                    while *self.b.get(self.i).unwrap_or(&b']') == b',' {
                        self.i += 1;
                        items.push(self.value());
                        self.ws();
                    }
                }
                self.eat(b']');
                Jsonb::Array(items)
            }
            b'{' => {
                self.eat(b'{');
                let mut pairs = Vec::new();
                self.ws();
                if *self.b.get(self.i).unwrap_or(&b'}') != b'}' {
                    let k = self.string();
                    self.ws();
                    self.eat(b':');
                    let v = self.value();
                    pairs.push((k, v));
                    self.ws();
                    while *self.b.get(self.i).unwrap_or(&b'}') == b',' {
                        self.i += 1;
                        let k = self.string();
                        self.ws();
                        self.eat(b':');
                        let v = self.value();
                        pairs.push((k, v));
                        self.ws();
                    }
                }
                self.eat(b'}');
                Jsonb::Object(pairs)
            }
            _ => panic!("unexpected jsonb byte"),
        }
    }
    fn string(&mut self) -> String {
        self.eat(b'"');
        let mut out = String::new();
        loop {
            let c = *self
                .b
                .get(self.i)
                .unwrap_or_else(|| panic!("unterminated jsonb string"));
            self.i += 1;
            match c {
                b'"' => return out,
                b'\\' => {
                    let e = *self
                        .b
                        .get(self.i)
                        .unwrap_or_else(|| panic!("unterminated escape"));
                    self.i += 1;
                    match e {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{0008}'),
                        b'f' => out.push('\u{000C}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let hex =
                                std::str::from_utf8(&self.b[self.i..self.i + 4]).unwrap_or("");
                            let cp = u32::from_str_radix(hex, 16)
                                .unwrap_or_else(|e| panic!("bad \\u: {e}"));
                            self.i += 4;
                            out.push(
                                char::from_u32(cp)
                                    .unwrap_or_else(|| panic!("bad codepoint {cp:x}")),
                            );
                        }
                        _ => panic!("bad escape {:?}", e as char),
                    }
                }
                _ => {
                    // copy one UTF-8 char
                    let len = utf8_len(c);
                    let s =
                        std::str::from_utf8(&self.b[self.i - 1..self.i - 1 + len]).unwrap_or("?");
                    self.i += len - 1;
                    out.push_str(s);
                }
            }
        }
    }
    /// JSON number: `-?int[.frac][eE[+-]exp]`, kept as a Decimal with
    /// `scale` adjusted by the exponent.
    fn number(&mut self) -> Decimal {
        let start = self.i;
        if *self.b.get(self.i).unwrap_or(&b'x') == b'-' {
            self.i += 1;
        }
        while matches!(self.b.get(self.i), Some(b'0'..=b'9')) {
            self.i += 1;
        }
        if *self.b.get(self.i).unwrap_or(&b'x') == b'.' {
            self.i += 1;
            while matches!(self.b.get(self.i), Some(b'0'..=b'9')) {
                self.i += 1;
            }
        }
        let mut exp = 0i64;
        if matches!(self.b.get(self.i), Some(b'e' | b'E')) {
            self.i += 1;
            let mut neg = false;
            if matches!(self.b.get(self.i), Some(b'+' | b'-')) {
                neg = *self.b.get(self.i).unwrap_or(&b'+') == b'-';
                self.i += 1;
            }
            let es = self.i;
            while matches!(self.b.get(self.i), Some(b'0'..=b'9')) {
                self.i += 1;
            }
            exp = std::str::from_utf8(&self.b[es..self.i])
                .unwrap_or("")
                .parse()
                .unwrap_or(0);
            if neg {
                exp = -exp;
            }
        }
        let text = std::str::from_utf8(&self.b[start..self.i]).unwrap_or("");
        let mut d = Decimal::parse(text).unwrap_or_else(|| panic!("{text}: bad number"));
        d.scale = i32::try_from(i64::from(d.scale) - exp).unwrap_or(0);
        d
    }
}

fn utf8_len(b: u8) -> usize {
    match b {
        0x00..=0x7F => 1,
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        _ => 4,
    }
}

// ---- int4 arrays ----

enum ANode {
    Leaf(i32),
    Null,
    Sub(Vec<ANode>),
}

/// `'{1,2,NULL}'`, `'[0:1]={1,2}'`, `'[[1,2],[3,4]]'`, `'[1:2][1:2]={{...}}'`.
fn parse_int4_array(lit: &str) -> Array {
    let (bounds, body) = match lit.split_once('=') {
        Some((b, rest)) => (Some(parse_bounds(b)), rest),
        None => (None, lit),
    };
    let node = parse_abody(body);
    let flat = flatten(&node);
    let elems: Vec<Option<Value>> = flat.iter().map(|e| e.map(Value::Int4)).collect();
    let dims = match bounds {
        Some(bs) => {
            let product: i64 = bs.iter().map(|&(l, _)| i64::from(l)).product();
            assert_eq!(
                product,
                elems.len() as i64,
                "{lit}: bounds do not match elements"
            );
            bs.into_iter()
                .map(|(len, lower)| ArrayDim { len, lower })
                .collect()
        }
        None => infer_dims(&node)
            .into_iter()
            .map(|(len, lower)| ArrayDim { len, lower })
            .collect(),
    };
    Array { dims, elems }
}

fn parse_bounds(s: &str) -> Vec<(i32, i32)> {
    let mut out = Vec::new();
    for part in s.split(']').filter(|p| !p.is_empty()) {
        let p = part.trim_start_matches('[');
        let (lo, hi) = p.split_once(':').unwrap_or_else(|| panic!("{s}: bound"));
        let lo: i32 = lo.parse().unwrap_or_else(|e| panic!("{s}: {e}"));
        let hi: i32 = hi.parse().unwrap_or_else(|e| panic!("{s}: {e}"));
        out.push((hi - lo + 1, lo));
    }
    out
}

fn parse_abody(s: &str) -> ANode {
    let b = s.as_bytes();
    assert!(
        b.first() == Some(&b'{') && b.last() == Some(&b'}'),
        "{s}: braces"
    );
    let inner = &s[1..s.len() - 1];
    if inner.is_empty() {
        return ANode::Sub(Vec::new());
    }
    let mut items = Vec::new();
    for part in split_top(inner) {
        if part == "NULL" {
            items.push(ANode::Null);
        } else if part.starts_with('{') {
            items.push(parse_abody(part));
        } else {
            let v: i32 = part.parse().unwrap_or_else(|e| panic!("{part}: {e}"));
            items.push(ANode::Leaf(v));
        }
    }
    ANode::Sub(items)
}

/// Split on commas not nested in braces.
fn split_top(s: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    for (i, c) in s.char_indices() {
        match c {
            '{' => depth += 1,
            '}' => depth -= 1,
            ',' if depth == 0 => {
                out.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(&s[start..]);
    out
}

fn flatten(n: &ANode) -> Vec<Option<i32>> {
    let mut out = Vec::new();
    walk(n, &mut out);
    out
}

fn walk(n: &ANode, out: &mut Vec<Option<i32>>) {
    match n {
        ANode::Leaf(v) => out.push(Some(*v)),
        ANode::Null => out.push(None),
        ANode::Sub(items) => items.iter().for_each(|i| walk(i, out)),
    }
}

/// Rectangular nesting -> (len, lower=1) per level.
fn infer_dims(n: &ANode) -> Vec<(i32, i32)> {
    match n {
        ANode::Sub(items) if items.is_empty() => Vec::new(),
        ANode::Sub(items) => match &items[0] {
            ANode::Sub(_) => {
                let first = infer_dims(&items[0]);
                assert!(
                    items.iter().all(|it| infer_dims(it) == first),
                    "ragged array"
                );
                let mut d = vec![(items.len() as i32, 1)];
                d.extend(first);
                d
            }
            _ => {
                assert!(
                    items.iter().all(|it| !matches!(it, ANode::Sub(_))),
                    "ragged array"
                );
                vec![(items.len() as i32, 1)]
            }
        },
        _ => panic!("array body must be a list"),
    }
}
