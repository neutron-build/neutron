//! Typed values the codec encodes, in PostgreSQL's physical shapes (C-Q3s §4).
//! `==` on these types is representation equality. SQL equality is what the
//! encoding defines: `enc(a) == enc(b)`.

use std::fmt;

/// Text collation of a key column (C-Q3s §5.2). ICU sort keys plug in here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Collation {
    /// Bytewise UTF-8 order.
    C,
}

/// Declared type of a key column. `varchar` uses `Text`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KeyType {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Numeric,
    Text(Collation),
    Bytea,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Interval,
    Uuid,
    Jsonb,
    /// Element type; must not itself be an array.
    Array(Box<KeyType>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Bool(bool),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Float4(f32),
    Float8(f64),
    Numeric(Numeric),
    Text(String),
    Bytea(Vec<u8>),
    /// Days since 2000-01-01; `i32::MIN` / `i32::MAX` are -infinity / infinity.
    Date(i32),
    /// Microseconds since midnight.
    Time(i64),
    /// Microseconds since 2000-01-01; `i64::MIN` / `i64::MAX` are the infinities.
    Timestamp(i64),
    /// As `Timestamp`, UTC.
    TimestampTz(i64),
    Interval(Interval),
    Uuid([u8; 16]),
    Jsonb(Jsonb),
    Array(Array),
}

/// PostgreSQL `numeric`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Numeric {
    NaN,
    PosInf,
    NegInf,
    Finite(Decimal),
}

/// `(-1)^negative * int(digits) * 10^-scale`. `digits` are 0..=9, most
/// significant first; leading and trailing zeros are allowed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decimal {
    pub negative: bool,
    pub digits: Vec<u8>,
    pub scale: i32,
}

/// PostgreSQL `interval`. PG17 infinities are all fields at their minimum
/// or maximum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}

impl Interval {
    pub const NEG_INFINITY: Interval = Interval {
        months: i32::MIN,
        days: i32::MIN,
        micros: i64::MIN,
    };
    pub const INFINITY: Interval = Interval {
        months: i32::MAX,
        days: i32::MAX,
        micros: i64::MAX,
    };
    pub const USECS_PER_DAY: i64 = 86_400_000_000;
}

/// PostgreSQL `jsonb`. Object pairs may arrive in any order with duplicates;
/// the encoder applies jsonb input semantics (C-Q3s §6 rule 7).
#[derive(Debug, Clone, PartialEq)]
pub enum Jsonb {
    Null,
    Bool(bool),
    /// Finite only.
    Number(Numeric),
    String(String),
    Array(Vec<Jsonb>),
    Object(Vec<(String, Jsonb)>),
}

/// PostgreSQL array: row-major elements plus per-dimension length and lower
/// bound. An empty array has no dimensions.
#[derive(Debug, Clone, PartialEq)]
pub struct Array {
    pub dims: Vec<ArrayDim>,
    pub elems: Vec<Option<Value>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArrayDim {
    pub len: i32,
    pub lower: i32,
}

impl Array {
    /// One-dimensional, lower bound 1 (`'{...}'`).
    pub fn from_elems(elems: Vec<Option<Value>>) -> Array {
        let dims = if elems.is_empty() {
            Vec::new()
        } else {
            vec![ArrayDim {
                len: i32::try_from(elems.len()).unwrap_or(i32::MAX),
                lower: 1,
            }]
        };
        Array { dims, elems }
    }
}

impl Decimal {
    /// `[+-]digits[.digits]`, at least one digit.
    pub fn parse(s: &str) -> Option<Decimal> {
        let (negative, body) = match s.as_bytes().first() {
            Some(b'-') => (true, &s[1..]),
            Some(b'+') => (false, &s[1..]),
            _ => (false, s),
        };
        let (int, frac) = match body.split_once('.') {
            Some((i, f)) => (i, f),
            None => (body, ""),
        };
        if int.is_empty() && frac.is_empty() {
            return None;
        }
        let mut digits = Vec::with_capacity(int.len() + frac.len());
        for c in int.bytes().chain(frac.bytes()) {
            if !c.is_ascii_digit() {
                return None;
            }
            digits.push(c - b'0');
        }
        let scale = i32::try_from(frac.len()).ok()?;
        Some(Decimal {
            negative,
            digits,
            scale,
        })
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s: String = self
            .digits
            .iter()
            .map(|d| char::from(b'0' + d % 10))
            .collect();
        if self.scale <= 0 {
            s.push_str(&"0".repeat(self.scale.unsigned_abs() as usize));
        } else {
            let scale = self.scale as usize;
            if s.len() <= scale {
                s.insert_str(0, &"0".repeat(scale + 1 - s.len()));
            }
            s.insert(s.len() - scale, '.');
        }
        if s.is_empty() {
            s.push('0');
        }
        let int_end = s.find('.').unwrap_or(s.len());
        let lead = s[..int_end]
            .bytes()
            .take_while(|&b| b == b'0')
            .count()
            .min(int_end.saturating_sub(1));
        if self.negative {
            f.write_str("-")?;
        }
        f.write_str(&s[lead..])
    }
}

impl Numeric {
    /// `NaN`, `Infinity`, `-Infinity`, or a `Decimal`.
    pub fn parse(s: &str) -> Option<Numeric> {
        match s {
            "NaN" => Some(Numeric::NaN),
            "Infinity" | "+Infinity" => Some(Numeric::PosInf),
            "-Infinity" => Some(Numeric::NegInf),
            _ => Decimal::parse(s).map(Numeric::Finite),
        }
    }
}

impl fmt::Display for Numeric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Numeric::NaN => f.write_str("NaN"),
            Numeric::PosInf => f.write_str("Infinity"),
            Numeric::NegInf => f.write_str("-Infinity"),
            Numeric::Finite(d) => d.fmt(f),
        }
    }
}
