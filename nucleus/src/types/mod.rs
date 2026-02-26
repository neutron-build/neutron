//! Core data types for Nucleus.

use std::fmt;
use std::hash::{Hash, Hasher};

/// A value in Nucleus. All data flows through this enum.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Text(String),
    /// JSONB: stored as a serde_json::Value for efficient manipulation.
    Jsonb(serde_json::Value),
    /// Date: days since 2000-01-01 (PostgreSQL epoch).
    Date(i32),
    /// Timestamp without timezone: microseconds since 2000-01-01.
    Timestamp(i64),
    /// Timestamp with timezone: microseconds since 2000-01-01 UTC.
    TimestampTz(i64),
    /// Exact numeric (stored as string for arbitrary precision).
    Numeric(String),
    /// UUID: 128-bit unique identifier.
    Uuid([u8; 16]),
    /// Raw bytes.
    Bytea(Vec<u8>),
    /// Array of values.
    Array(Vec<Value>),
    /// Dense vector for similarity search.
    Vector(Vec<f32>),
    /// Interval: months, days, microseconds (PostgreSQL-compatible).
    Interval { months: i32, days: i32, microseconds: i64 },
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Int32(n) => write!(f, "{n}"),
            Value::Int64(n) => write!(f, "{n}"),
            Value::Float64(n) => write!(f, "{n}"),
            Value::Text(s) => write!(f, "{s}"),
            Value::Jsonb(v) => write!(f, "{v}"),
            Value::Date(days) => {
                let (y, m, d) = days_to_ymd(*days);
                write!(f, "{y:04}-{m:02}-{d:02}")
            }
            Value::Timestamp(us) => format_timestamp(f, *us),
            Value::TimestampTz(us) => {
                format_timestamp(f, *us)?;
                write!(f, "+00")
            }
            Value::Numeric(s) => write!(f, "{s}"),
            Value::Uuid(bytes) => {
                write!(
                    f,
                    "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                    bytes[0], bytes[1], bytes[2], bytes[3],
                    bytes[4], bytes[5], bytes[6], bytes[7],
                    bytes[8], bytes[9], bytes[10], bytes[11],
                    bytes[12], bytes[13], bytes[14], bytes[15]
                )
            }
            Value::Bytea(b) => {
                write!(f, "\\x")?;
                for byte in b {
                    write!(f, "{byte:02x}")?;
                }
                Ok(())
            }
            Value::Array(vals) => {
                write!(f, "{{")?;
                for (i, v) in vals.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{v}")?;
                }
                write!(f, "}}")
            }
            Value::Vector(vec) => {
                write!(f, "[")?;
                for (i, v) in vec.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{v}")?;
                }
                write!(f, "]")
            }
            Value::Interval { months, days, microseconds } => {
                let mut parts = Vec::new();
                let years = months / 12;
                let rem_months = months % 12;
                if years != 0 { parts.push(format!("{years} year{}", if years.abs() != 1 { "s" } else { "" })); }
                if rem_months != 0 { parts.push(format!("{rem_months} mon{}", if rem_months.abs() != 1 { "s" } else { "" })); }
                if *days != 0 { parts.push(format!("{days} day{}", if days.abs() != 1 { "s" } else { "" })); }
                if *microseconds != 0 || parts.is_empty() {
                    let total_us = microseconds.unsigned_abs();
                    let h = total_us / 3_600_000_000;
                    let m = (total_us % 3_600_000_000) / 60_000_000;
                    let s = (total_us % 60_000_000) / 1_000_000;
                    let frac = total_us % 1_000_000;
                    let sign = if *microseconds < 0 { "-" } else { "" };
                    if frac > 0 {
                        parts.push(format!("{sign}{h:02}:{m:02}:{s:02}.{frac:06}"));
                    } else {
                        parts.push(format!("{sign}{h:02}:{m:02}:{s:02}"));
                    }
                }
                write!(f, "{}", parts.join(" "))
            }
        }
    }
}

/// Column data type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    Bool,
    Int32,
    Int64,
    Float64,
    Text,
    Jsonb,
    Date,
    Timestamp,
    TimestampTz,
    Numeric,
    Uuid,
    Bytea,
    Array(Box<DataType>),
    /// Vector type with specified dimensionality.
    Vector(usize),
    /// Interval type (months, days, microseconds).
    Interval,
    /// User-defined type (e.g. an enum created with CREATE TYPE … AS ENUM).
    /// Stores the type name; validation is done against the catalog.
    UserDefined(String),
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Bool => write!(f, "BOOLEAN"),
            DataType::Int32 => write!(f, "INTEGER"),
            DataType::Int64 => write!(f, "BIGINT"),
            DataType::Float64 => write!(f, "DOUBLE PRECISION"),
            DataType::Text => write!(f, "TEXT"),
            DataType::Jsonb => write!(f, "JSONB"),
            DataType::Date => write!(f, "DATE"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
            DataType::TimestampTz => write!(f, "TIMESTAMP WITH TIME ZONE"),
            DataType::Numeric => write!(f, "NUMERIC"),
            DataType::Uuid => write!(f, "UUID"),
            DataType::Bytea => write!(f, "BYTEA"),
            DataType::Array(inner) => write!(f, "{inner}[]"),
            DataType::Vector(dim) => write!(f, "VECTOR({})", dim),
            DataType::Interval => write!(f, "INTERVAL"),
            DataType::UserDefined(name) => write!(f, "{name}"),
        }
    }
}

/// A single row of data.
pub type Row = Vec<Value>;

// ============================================================================
// Helper functions
// ============================================================================

/// Convert days since 2000-01-01 to (year, month, day).
///
/// Uses the Meeus algorithm (Gregorian calendar from Julian Day Number).
pub fn days_to_ymd(days: i32) -> (i32, u32, u32) {
    let jdn = days + 2451545; // Convert to Julian Day Number (2000-01-01 = JDN 2451545)
    let a = jdn + 32044;
    let b = (4 * a + 3) / 146097;
    let c = a - (146097 * b) / 4;
    let d = (4 * c + 3) / 1461;
    let e = c - (1461 * d) / 4;
    let m = (5 * e + 2) / 153;

    let day = e - (153 * m + 2) / 5 + 1;
    let month = m + 3 - 12 * (m / 10);
    let year = 100 * b + d - 4800 + m / 10;
    (year, month as u32, day as u32)
}

/// Convert (year, month, day) to days since 2000-01-01.
///
/// Uses the PostgreSQL date2j algorithm.
pub fn ymd_to_days(year: i32, month: u32, day: u32) -> i32 {
    let (y, m) = if month > 2 {
        (year + 4800, (month + 1) as i32)
    } else {
        (year + 4799, (month + 13) as i32)
    };
    let century = y / 100;
    let jdn = y * 365 - 32167 + y / 4 - century + century / 4 + 7834 * m / 256 + day as i32;
    jdn - 2451545 // subtract J2000 epoch
}

/// Format microseconds since 2000-01-01 as "YYYY-MM-DD HH:MM:SS.ffffff".
fn format_timestamp(f: &mut fmt::Formatter<'_>, us: i64) -> fmt::Result {
    let total_secs = us / 1_000_000;
    let frac = (us % 1_000_000).unsigned_abs() as u32;
    let days = total_secs.div_euclid(86400) as i32;
    let time_secs = total_secs.rem_euclid(86400) as u32;
    let (y, m, d) = days_to_ymd(days);
    let hour = time_secs / 3600;
    let minute = (time_secs % 3600) / 60;
    let second = time_secs % 60;
    if frac > 0 {
        write!(f, "{y:04}-{m:02}-{d:02} {hour:02}:{minute:02}:{second:02}.{frac:06}")
    } else {
        write!(f, "{y:04}-{m:02}-{d:02} {hour:02}:{minute:02}:{second:02}")
    }
}

/// Parse a UUID string (with or without dashes) into 16 bytes.
pub fn parse_uuid(s: &str) -> Result<[u8; 16], String> {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return Err(format!("invalid UUID: {s}"));
    }
    let mut bytes = [0u8; 16];
    for i in 0..16 {
        bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|_| format!("invalid UUID hex: {s}"))?;
    }
    Ok(bytes)
}

// ============================================================================
// Type coercion and casting
// ============================================================================

impl Value {
    /// Cast this value to the target data type.
    pub fn cast(&self, target: &DataType) -> Result<Value, String> {
        if *self == Value::Null {
            return Ok(Value::Null);
        }
        match (self, target) {
            // Identity casts
            (Value::Bool(_), DataType::Bool) => Ok(self.clone()),
            (Value::Int32(_), DataType::Int32) => Ok(self.clone()),
            (Value::Int64(_), DataType::Int64) => Ok(self.clone()),
            (Value::Float64(_), DataType::Float64) => Ok(self.clone()),
            (Value::Text(_), DataType::Text) => Ok(self.clone()),
            (Value::Numeric(_), DataType::Numeric) => Ok(self.clone()),
            // Bool conversions
            (Value::Bool(b), DataType::Int32) => Ok(Value::Int32(if *b { 1 } else { 0 })),
            (Value::Bool(b), DataType::Int64) => Ok(Value::Int64(if *b { 1 } else { 0 })),
            (Value::Bool(b), DataType::Text) => Ok(Value::Text(b.to_string())),
            // Int32 conversions
            (Value::Int32(n), DataType::Int64) => Ok(Value::Int64(*n as i64)),
            (Value::Int32(n), DataType::Float64) => Ok(Value::Float64(*n as f64)),
            (Value::Int32(n), DataType::Text) => Ok(Value::Text(n.to_string())),
            (Value::Int32(n), DataType::Numeric) => Ok(Value::Numeric(n.to_string())),
            (Value::Int32(n), DataType::Bool) => Ok(Value::Bool(*n != 0)),
            // Int64 conversions
            (Value::Int64(n), DataType::Int32) => Ok(Value::Int32(*n as i32)),
            (Value::Int64(n), DataType::Float64) => Ok(Value::Float64(*n as f64)),
            (Value::Int64(n), DataType::Text) => Ok(Value::Text(n.to_string())),
            (Value::Int64(n), DataType::Numeric) => Ok(Value::Numeric(n.to_string())),
            (Value::Int64(n), DataType::Bool) => Ok(Value::Bool(*n != 0)),
            // Float64 conversions
            (Value::Float64(n), DataType::Int32) => Ok(Value::Int32(*n as i32)),
            (Value::Float64(n), DataType::Int64) => Ok(Value::Int64(*n as i64)),
            (Value::Float64(n), DataType::Text) => Ok(Value::Text(n.to_string())),
            (Value::Float64(n), DataType::Numeric) => Ok(Value::Numeric(n.to_string())),
            // Text conversions
            (Value::Text(s), DataType::Int32) => s.parse::<i32>().map(Value::Int32).map_err(|e| e.to_string()),
            (Value::Text(s), DataType::Int64) => s.parse::<i64>().map(Value::Int64).map_err(|e| e.to_string()),
            (Value::Text(s), DataType::Float64) => s.parse::<f64>().map(Value::Float64).map_err(|e| e.to_string()),
            (Value::Text(s), DataType::Bool) => match s.to_lowercase().as_str() {
                "true" | "t" | "1" | "yes" | "on" => Ok(Value::Bool(true)),
                "false" | "f" | "0" | "no" | "off" => Ok(Value::Bool(false)),
                _ => Err(format!("cannot cast '{s}' to boolean")),
            },
            (Value::Text(s), DataType::Numeric) => Ok(Value::Numeric(s.clone())),
            // Numeric conversions
            (Value::Numeric(s), DataType::Int32) => s.parse::<i32>().map(Value::Int32).map_err(|e| e.to_string()),
            (Value::Numeric(s), DataType::Int64) => s.parse::<i64>().map(Value::Int64).map_err(|e| e.to_string()),
            (Value::Numeric(s), DataType::Float64) => s.parse::<f64>().map(Value::Float64).map_err(|e| e.to_string()),
            (Value::Numeric(s), DataType::Text) => Ok(Value::Text(s.clone())),
            // Fallback: use Display
            (_, DataType::Text) => Ok(Value::Text(self.to_string())),
            _ => Err(format!("cannot cast {} to {target}", self.type_name())),
        }
    }

    /// Return the type name as a string.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Int32(_) => "integer",
            Value::Int64(_) => "bigint",
            Value::Float64(_) => "double precision",
            Value::Text(_) => "text",
            Value::Jsonb(_) => "jsonb",
            Value::Date(_) => "date",
            Value::Timestamp(_) => "timestamp",
            Value::TimestampTz(_) => "timestamptz",
            Value::Numeric(_) => "numeric",
            Value::Uuid(_) => "uuid",
            Value::Bytea(_) => "bytea",
            Value::Array(_) => "array",
            Value::Vector(_) => "vector",
            Value::Interval { .. } => "interval",
        }
    }

    /// Return a numeric rank for sorting different types.
    fn type_rank(&self) -> u8 {
        match self {
            Value::Bool(_) => 0,
            Value::Int32(_) => 1,
            Value::Int64(_) => 2,
            Value::Float64(_) => 3,
            Value::Numeric(_) => 4,
            Value::Text(_) => 5,
            Value::Date(_) => 6,
            Value::Timestamp(_) => 7,
            Value::TimestampTz(_) => 8,
            Value::Interval { .. } => 9,
            Value::Uuid(_) => 10,
            Value::Bytea(_) => 11,
            Value::Jsonb(_) => 12,
            Value::Array(_) => 13,
            Value::Vector(_) => 14,
            Value::Null => 255,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            Value::Null => {}
            Value::Bool(b) => b.hash(state),
            Value::Int32(n) => n.hash(state),
            Value::Int64(n) => n.hash(state),
            Value::Float64(f) => f.to_bits().hash(state),
            Value::Text(s) | Value::Numeric(s) => s.hash(state),
            Value::Jsonb(v) => format!("{v}").hash(state),
            Value::Date(d) => d.hash(state),
            Value::Timestamp(t) | Value::TimestampTz(t) => t.hash(state),
            Value::Uuid(u) => u.hash(state),
            Value::Bytea(b) => b.hash(state),
            Value::Array(a) => a.hash(state),
            Value::Vector(v) => {
                for f in v {
                    f.to_bits().hash(state);
                }
            }
            Value::Interval { months, days, microseconds } => {
                months.hash(state);
                days.hash(state);
                microseconds.hash(state);
            }
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Greater, // NULL sorts last
            (_, Value::Null) => Ordering::Less,
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Int32(a), Value::Int32(b)) => a.cmp(b),
            (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
            (Value::Int32(a), Value::Int64(b)) => (*a as i64).cmp(b),
            (Value::Int64(a), Value::Int32(b)) => a.cmp(&(*b as i64)),
            (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Numeric(a), Value::Numeric(b)) => {
                // Parse as f64 for proper numeric ordering (not lexicographic)
                let av: f64 = a.parse().unwrap_or(f64::NAN);
                let bv: f64 = b.parse().unwrap_or(f64::NAN);
                av.partial_cmp(&bv).unwrap_or(Ordering::Equal)
            }
            (Value::Date(a), Value::Date(b)) => a.cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
            (Value::TimestampTz(a), Value::TimestampTz(b)) => a.cmp(b),
            (Value::Uuid(a), Value::Uuid(b)) => a.cmp(b),
            (Value::Bytea(a), Value::Bytea(b)) => a.cmp(b),
            (Value::Interval { months: am, days: ad, microseconds: aus },
             Value::Interval { months: bm, days: bd, microseconds: bus }) => {
                // Convert to total microseconds for comparison (approximate)
                let a_total = *am as i64 * 30 * 86400 * 1_000_000 + *ad as i64 * 86400 * 1_000_000 + aus;
                let b_total = *bm as i64 * 30 * 86400 * 1_000_000 + *bd as i64 * 86400 * 1_000_000 + bus;
                a_total.cmp(&b_total)
            }
            // Cross-type: compare by type rank
            _ => self.type_rank().cmp(&other.type_rank()),
        }
    }
}

// ============================================================================
// Interval arithmetic helpers
// ============================================================================

/// Number of days in a given month (1-indexed).
pub fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

/// Add an interval to a date (days since 2000-01-01).
pub fn date_add_interval(date_days: i32, months: i32, days: i32) -> i32 {
    let (mut y, mut m, d) = days_to_ymd(date_days);
    // Add months
    let total_months = y * 12 + m as i32 - 1 + months;
    y = total_months.div_euclid(12);
    m = (total_months.rem_euclid(12) + 1) as u32;
    let max_d = days_in_month(y, m);
    let clamped_d = d.min(max_d);
    ymd_to_days(y, m, clamped_d) + days
}

/// Add an interval to a timestamp (microseconds since 2000-01-01).
pub fn timestamp_add_interval(ts_us: i64, months: i32, days: i32, microseconds: i64) -> i64 {
    let date_days = (ts_us / (86400 * 1_000_000)) as i32;
    let time_us = ts_us % (86400 * 1_000_000);
    let new_date = date_add_interval(date_days, months, days);
    new_date as i64 * 86400 * 1_000_000 + time_us + microseconds
}

// ============================================================================
// Numeric (arbitrary-precision) arithmetic helpers
// ============================================================================

/// Add two numeric strings, returning the result as a string.
///
/// Uses f64 internally as a pragmatic first step; a true arbitrary-precision
/// decimal library can replace this later.
pub fn numeric_add(a: &str, b: &str) -> String {
    let av: f64 = a.parse().unwrap_or(0.0);
    let bv: f64 = b.parse().unwrap_or(0.0);
    format_numeric(av + bv)
}

/// Subtract two numeric strings (a - b).
pub fn numeric_sub(a: &str, b: &str) -> String {
    let av: f64 = a.parse().unwrap_or(0.0);
    let bv: f64 = b.parse().unwrap_or(0.0);
    format_numeric(av - bv)
}

/// Multiply two numeric strings.
pub fn numeric_mul(a: &str, b: &str) -> String {
    let av: f64 = a.parse().unwrap_or(0.0);
    let bv: f64 = b.parse().unwrap_or(0.0);
    format_numeric(av * bv)
}

/// Divide two numeric strings (a / b), returning an error on division by zero.
pub fn numeric_div(a: &str, b: &str) -> Result<String, String> {
    let av: f64 = a.parse().unwrap_or(0.0);
    let bv: f64 = b.parse().unwrap_or(0.0);
    if bv == 0.0 {
        return Err("division by zero".to_string());
    }
    Ok(format_numeric(av / bv))
}

/// Remainder of two numeric strings (a % b).
pub fn numeric_rem(a: &str, b: &str) -> Result<String, String> {
    let av: f64 = a.parse().unwrap_or(0.0);
    let bv: f64 = b.parse().unwrap_or(0.0);
    if bv == 0.0 {
        return Err("division by zero".to_string());
    }
    Ok(format_numeric(av % bv))
}

/// Negate a numeric string.
pub fn numeric_neg(a: &str) -> String {
    let av: f64 = a.parse().unwrap_or(0.0);
    format_numeric(-av)
}

/// Absolute value of a numeric string.
pub fn numeric_abs(a: &str) -> String {
    let av: f64 = a.parse().unwrap_or(0.0);
    format_numeric(av.abs())
}

/// Format an f64 as a clean numeric string.
fn format_numeric(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < (i64::MAX as f64) {
        format!("{}", v as i64)
    } else {
        let s = format!("{:.17}", v);
        let s = s.trim_end_matches('0');
        let s = s.trim_end_matches('.');
        s.to_string()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_date_roundtrip() {
        // 2024-03-15
        let days = ymd_to_days(2024, 3, 15);
        let (y, m, d) = days_to_ymd(days);
        assert_eq!((y, m, d), (2024, 3, 15));
    }

    #[test]
    fn test_date_epoch() {
        // 2000-01-01 should be day 0
        let days = ymd_to_days(2000, 1, 1);
        assert_eq!(days, 0);
        let (y, m, d) = days_to_ymd(0);
        assert_eq!((y, m, d), (2000, 1, 1));
    }

    #[test]
    fn test_date_display() {
        let v = Value::Date(ymd_to_days(2024, 3, 15));
        assert_eq!(v.to_string(), "2024-03-15");
    }

    #[test]
    fn test_timestamp_display() {
        // 2024-01-01 12:30:45
        let days = ymd_to_days(2024, 1, 1) as i64;
        let us = days * 86400 * 1_000_000 + 12 * 3600 * 1_000_000 + 30 * 60 * 1_000_000 + 45 * 1_000_000;
        let v = Value::Timestamp(us);
        assert_eq!(v.to_string(), "2024-01-01 12:30:45");
    }

    #[test]
    fn test_uuid_display() {
        let bytes = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
            0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
        ];
        let v = Value::Uuid(bytes);
        assert_eq!(v.to_string(), "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn test_parse_uuid() {
        let bytes = parse_uuid("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(bytes[0], 0x55);
        assert_eq!(bytes[15], 0x00);
    }

    #[test]
    fn test_bytea_display() {
        let v = Value::Bytea(vec![0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(v.to_string(), "\\xdeadbeef");
    }

    #[test]
    fn test_array_display() {
        let v = Value::Array(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)]);
        assert_eq!(v.to_string(), "{1,2,3}");
    }

    #[test]
    fn test_numeric_display() {
        let v = Value::Numeric("3.14159265358979".into());
        assert_eq!(v.to_string(), "3.14159265358979");
    }

    #[test]
    fn test_interval_display() {
        let v = Value::Interval { months: 14, days: 3, microseconds: 3_661_000_000 };
        assert_eq!(v.to_string(), "1 year 2 mons 3 days 01:01:01");
    }

    #[test]
    fn test_interval_zero() {
        let v = Value::Interval { months: 0, days: 0, microseconds: 0 };
        assert_eq!(v.to_string(), "00:00:00");
    }

    #[test]
    fn test_cast_int_to_float() {
        let v = Value::Int32(42);
        assert_eq!(v.cast(&DataType::Float64).unwrap(), Value::Float64(42.0));
    }

    #[test]
    fn test_cast_text_to_int() {
        let v = Value::Text("123".into());
        assert_eq!(v.cast(&DataType::Int32).unwrap(), Value::Int32(123));
    }

    #[test]
    fn test_cast_bool_to_int() {
        assert_eq!(Value::Bool(true).cast(&DataType::Int32).unwrap(), Value::Int32(1));
        assert_eq!(Value::Bool(false).cast(&DataType::Int32).unwrap(), Value::Int32(0));
    }

    #[test]
    fn test_cast_null() {
        assert_eq!(Value::Null.cast(&DataType::Int32).unwrap(), Value::Null);
    }

    #[test]
    fn test_cast_invalid() {
        assert!(Value::Text("abc".into()).cast(&DataType::Int32).is_err());
    }

    #[test]
    fn test_value_ordering() {
        let mut vals = vec![Value::Int32(3), Value::Int32(1), Value::Null, Value::Int32(2)];
        vals.sort();
        assert_eq!(vals, vec![Value::Int32(1), Value::Int32(2), Value::Int32(3), Value::Null]);
    }

    #[test]
    fn test_date_add_interval() {
        let d = ymd_to_days(2024, 1, 31);
        let result = date_add_interval(d, 1, 0); // +1 month from Jan 31 -> Feb 29 (2024 is leap)
        let (y, m, day) = days_to_ymd(result);
        assert_eq!((y, m, day), (2024, 2, 29));
    }

    #[test]
    fn test_timestamp_add_interval() {
        let ts = ymd_to_days(2024, 1, 1) as i64 * 86400 * 1_000_000;
        let result = timestamp_add_interval(ts, 0, 1, 3_600_000_000);
        let expected = ymd_to_days(2024, 1, 2) as i64 * 86400 * 1_000_000 + 3_600_000_000;
        assert_eq!(result, expected);
    }

    #[test]
    fn test_days_in_month() {
        assert_eq!(days_in_month(2024, 2), 29); // leap year
        assert_eq!(days_in_month(2023, 2), 28);
        assert_eq!(days_in_month(2024, 1), 31);
        assert_eq!(days_in_month(2024, 4), 30);
    }

    #[test]
    fn test_interval_ordering() {
        let a = Value::Interval { months: 0, days: 0, microseconds: 1_000_000 };
        let b = Value::Interval { months: 0, days: 0, microseconds: 2_000_000 };
        assert!(a < b);
    }

    #[test]
    fn test_type_name() {
        assert_eq!(Value::Int32(1).type_name(), "integer");
        assert_eq!(Value::Text("x".into()).type_name(), "text");
        assert_eq!(Value::Null.type_name(), "null");
    }

    // ========================================================================
    // Numeric arithmetic tests
    // ========================================================================

    #[test]
    fn test_numeric_add_positive() {
        assert_eq!(numeric_add("1.5", "2.5"), "4");
    }

    #[test]
    fn test_numeric_add_negative() {
        assert_eq!(numeric_add("-3", "5"), "2");
    }

    #[test]
    fn test_numeric_sub() {
        assert_eq!(numeric_sub("10", "3"), "7");
    }

    #[test]
    fn test_numeric_mul() {
        assert_eq!(numeric_mul("6", "7"), "42");
    }

    #[test]
    fn test_numeric_div() {
        assert_eq!(numeric_div("10", "4").unwrap(), "2.5");
    }

    #[test]
    fn test_numeric_div_by_zero() {
        assert!(numeric_div("42", "0").is_err());
    }

    #[test]
    fn test_numeric_rem() {
        assert_eq!(numeric_rem("10", "3").unwrap(), "1");
    }

    #[test]
    fn test_numeric_neg() {
        assert_eq!(numeric_neg("42"), "-42");
        assert_eq!(numeric_neg("-7"), "7");
    }

    #[test]
    fn test_numeric_abs() {
        assert_eq!(numeric_abs("-42"), "42");
        assert_eq!(numeric_abs("42"), "42");
    }

    #[test]
    fn test_numeric_large_numbers() {
        assert_eq!(numeric_add("999999999999", "1"), "1000000000000");
        assert_eq!(numeric_mul("1000000", "1000000"), "1000000000000");
    }

    // ========================================================================
    // Property-based tests (proptest)
    // ========================================================================

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_int32_display_roundtrip(n in any::<i32>()) {
            let v = Value::Int32(n);
            let s = v.to_string();
            let parsed: i32 = s.parse().expect("Int32 display should be parseable");
            prop_assert_eq!(parsed, n);
        }

        #[test]
        fn prop_int64_display_roundtrip(n in any::<i64>()) {
            let v = Value::Int64(n);
            let s = v.to_string();
            let parsed: i64 = s.parse().expect("Int64 display should be parseable");
            prop_assert_eq!(parsed, n);
        }

        #[test]
        fn prop_float64_display_roundtrip(n in proptest::num::f64::NORMAL | proptest::num::f64::ZERO) {
            let v = Value::Float64(n);
            let s = v.to_string();
            let parsed: f64 = s.parse().expect("Float64 display should be parseable");
            prop_assert!((parsed - n).abs() < f64::EPSILON || (parsed == 0.0 && n == 0.0),
                "Float64 roundtrip failed: {} displayed as '{}' parsed as {}", n, s, parsed);
        }

        #[test]
        fn prop_bool_display_roundtrip(b in any::<bool>()) {
            let v = Value::Bool(b);
            let s = v.to_string();
            let parsed: bool = s.parse().expect("Bool display should be parseable");
            prop_assert_eq!(parsed, b);
        }

        #[test]
        fn prop_text_display_identity(s in "[a-zA-Z0-9_ ]{0,200}") {
            let v = Value::Text(s.clone());
            let displayed = v.to_string();
            prop_assert_eq!(displayed, s);
        }

        #[test]
        fn prop_display_never_panics(variant in 0u8..5, n in any::<i64>(), s in "[a-zA-Z0-9]{0,50}") {
            let v = match variant {
                0 => Value::Null,
                1 => Value::Bool(n % 2 == 0),
                2 => Value::Int32(n as i32),
                3 => Value::Int64(n),
                _ => Value::Text(s),
            };
            // Should never panic
            let _ = v.to_string();
        }
    }

    proptest! {
        /// Value::Text preserves arbitrary string content.
        #[test]
        fn prop_text_value_preserves_content(s in ".*") {
            let v = Value::Text(s.clone());
            match &v {
                Value::Text(inner) => prop_assert_eq!(inner, &s),
                _ => prop_assert!(false, "wrong variant"),
            }
        }

        /// Value::Bool display is always "true" or "false".
        #[test]
        fn prop_bool_value_display_valid(b in any::<bool>()) {
            let v = Value::Bool(b);
            let displayed = format!("{v}");
            prop_assert!(displayed == "true" || displayed == "false",
                "Bool display was '{}', expected 'true' or 'false'", displayed);
        }

        /// Casting Int32 to Int64 and back preserves the value.
        #[test]
        fn prop_cast_int32_to_int64_roundtrip(n in any::<i32>()) {
            let v = Value::Int32(n);
            let as_i64 = v.cast(&DataType::Int64).expect("Int32 -> Int64 cast");
            let back = as_i64.cast(&DataType::Int32).expect("Int64 -> Int32 cast");
            prop_assert_eq!(back, Value::Int32(n));
        }

        /// Casting Bool to Int32 and back preserves the value.
        #[test]
        fn prop_cast_bool_to_int_roundtrip(b in any::<bool>()) {
            let v = Value::Bool(b);
            let as_int = v.cast(&DataType::Int32).expect("Bool -> Int32 cast");
            let back = as_int.cast(&DataType::Bool).expect("Int32 -> Bool cast");
            prop_assert_eq!(back, Value::Bool(b));
        }

        /// Bytea display always starts with "\x" and has correct hex length.
        #[test]
        fn prop_bytea_display_format(data in proptest::collection::vec(any::<u8>(), 0..256)) {
            let v = Value::Bytea(data.clone());
            let displayed = format!("{v}");
            prop_assert!(displayed.starts_with("\\x"),
                "Bytea display should start with \\x, got '{}'", displayed);
            // Each byte is 2 hex chars, plus 2 for "\x" prefix
            prop_assert_eq!(displayed.len(), 2 + data.len() * 2,
                "Bytea display length mismatch for {} bytes", data.len());
        }

        /// UUID display is always 36 characters (8-4-4-4-12 with dashes).
        #[test]
        fn prop_uuid_display_format(bytes in proptest::collection::vec(any::<u8>(), 16..=16)) {
            let mut arr = [0u8; 16];
            arr.copy_from_slice(&bytes);
            let v = Value::Uuid(arr);
            let displayed = format!("{v}");
            prop_assert_eq!(displayed.len(), 36,
                "UUID display should be 36 chars, got {}: '{}'", displayed.len(), displayed);
            // Check dash positions
            let chars: Vec<char> = displayed.chars().collect();
            prop_assert_eq!(chars[8], '-');
            prop_assert_eq!(chars[13], '-');
            prop_assert_eq!(chars[18], '-');
            prop_assert_eq!(chars[23], '-');
        }

        /// Value ordering is reflexive: v == v for all variants.
        #[test]
        fn prop_value_ordering_reflexive(n in any::<i64>()) {
            let v = Value::Int64(n);
            prop_assert_eq!(v.cmp(&v), std::cmp::Ordering::Equal);
        }

        /// Null always sorts last (greater than any non-null value).
        #[test]
        fn prop_null_sorts_last(n in any::<i32>()) {
            let v = Value::Int32(n);
            prop_assert!(v < Value::Null, "Int32({}) should be less than Null", n);
        }

        /// Date roundtrip: ymd_to_days then days_to_ymd preserves valid dates.
        #[test]
        fn prop_date_ymd_roundtrip(
            y in 1i32..9999,
            m in 1u32..=12,
            d in 1u32..=28  // 28 is safe for all months
        ) {
            let days = ymd_to_days(y, m, d);
            let (ry, rm, rd) = days_to_ymd(days);
            prop_assert_eq!((ry, rm, rd), (y, m, d),
                "date roundtrip failed for {}-{:02}-{:02}", y, m, d);
        }

        /// Casting any Value to Text via cast() always succeeds.
        #[test]
        fn prop_cast_to_text_never_fails(n in any::<i64>()) {
            let values = vec![
                Value::Int32(n as i32),
                Value::Int64(n),
                Value::Bool(n % 2 == 0),
                Value::Null,
            ];
            for v in values {
                let result = v.cast(&DataType::Text);
                prop_assert!(result.is_ok(), "cast to Text failed for {:?}", v);
            }
        }
    }

}
