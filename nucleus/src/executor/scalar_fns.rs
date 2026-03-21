//! Scalar (non-aggregate) function evaluation.
//!
//! Contains the massive `eval_scalar_fn` dispatch function and `extract_fn_args`
//! helper. These implement all 208+ built-in SQL functions.

use std::collections::{HashMap, HashSet};
use sqlparser::ast;
use crate::types::{Row, Value};
use crate::vector;
use crate::fts;
use crate::timeseries;
use crate::graph::PropValue as GraphPropValue;
use crate::graph::cypher::parse_cypher;
use crate::graph::cypher_executor::execute_cypher;
#[cfg(feature = "server")]
use crate::reactive::ChangeType;
use super::types::ColMeta;
use super::{ExecError, ExecResult, Executor};
use super::helpers::*;
use super::session::sync_block_on;

impl Executor {

    /// Evaluate a scalar (non-aggregate) function call.
    pub(super) fn eval_scalar_fn(
        &self,
        fname: &str,
        func: &ast::Function,
        row: &Row,
        col_meta: &[ColMeta],
    ) -> Result<Value, ExecError> {
        let args = self.extract_fn_args(func, row, col_meta)?;

        match fname {
            // -- String functions --
            "UPPER" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => Ok(Value::Text(s.to_uppercase())),
                    Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Text(args[0].to_string().to_uppercase())),
                }
            }
            "LOWER" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => Ok(Value::Text(s.to_lowercase())),
                    Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Text(args[0].to_string().to_lowercase())),
                }
            }
            "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => Ok(Value::Int32(s.len() as i32)),
                    Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Int32(args[0].to_string().len() as i32)),
                }
            }
            "TRIM" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => Ok(Value::Text(s.trim().to_string())),
                    Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Text(args[0].to_string().trim().to_string())),
                }
            }
            "LTRIM" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => Ok(Value::Text(s.trim_start().to_string())),
                    Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Text(args[0].to_string().trim_start().to_string())),
                }
            }
            "RTRIM" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => Ok(Value::Text(s.trim_end().to_string())),
                    Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Text(args[0].to_string().trim_end().to_string())),
                }
            }
            "CONCAT" => {
                let mut result = String::new();
                for arg in &args {
                    match arg {
                        Value::Null => {} // CONCAT ignores nulls
                        Value::Text(s) => result.push_str(s),
                        other => result.push_str(&other.to_string()),
                    }
                }
                Ok(Value::Text(result))
            }
            "CONCAT_WS" => {
                if args.is_empty() {
                    return Err(ExecError::Unsupported("CONCAT_WS requires at least 1 arg".into()));
                }
                let sep = match &args[0] {
                    Value::Text(s) => s.clone(),
                    Value::Null => return Ok(Value::Null),
                    other => other.to_string(),
                };
                let parts: Vec<String> = args[1..]
                    .iter()
                    .filter(|a| !matches!(a, Value::Null))
                    .map(|a| match a {
                        Value::Text(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .collect();
                Ok(Value::Text(parts.join(&sep)))
            }
            "SUBSTRING" | "SUBSTR" => {
                if args.len() < 2 {
                    return Err(ExecError::Unsupported(
                        format!("{fname} requires at least 2 args"),
                    ));
                }
                let s = match &args[0] {
                    Value::Text(s) => s.clone(),
                    Value::Null => return Ok(Value::Null),
                    other => other.to_string(),
                };
                let start = value_to_i64(&args[1])? as usize;
                let start = if start > 0 { start - 1 } else { 0 }; // SQL is 1-indexed
                let len = if args.len() > 2 {
                    Some(value_to_i64(&args[2])? as usize)
                } else {
                    None
                };
                let chars: Vec<char> = s.chars().collect();
                let end = match len {
                    Some(l) => (start + l).min(chars.len()),
                    None => chars.len(),
                };
                let result: String = chars[start.min(chars.len())..end].iter().collect();
                Ok(Value::Text(result))
            }
            "REPLACE" => {
                require_args(fname, &args, 3)?;
                match (&args[0], &args[1], &args[2]) {
                    (Value::Text(s), Value::Text(from), Value::Text(to)) => {
                        Ok(Value::Text(s.replace(from.as_str(), to.as_str())))
                    }
                    (Value::Null, _, _) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("REPLACE requires text args".into())),
                }
            }
            "POSITION" | "STRPOS" => {
                require_args(fname, &args, 2)?;
                match (&args[0], &args[1]) {
                    (Value::Text(substr), Value::Text(s)) => {
                        let pos = s.find(substr.as_str()).map(|i| i + 1).unwrap_or(0);
                        Ok(Value::Int32(pos as i32))
                    }
                    _ => Ok(Value::Int32(0)),
                }
            }
            "LEFT" => {
                require_args(fname, &args, 2)?;
                match &args[0] {
                    Value::Text(s) => {
                        let n = value_to_i64(&args[1])? as usize;
                        Ok(Value::Text(s.chars().take(n).collect()))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("LEFT requires text".into())),
                }
            }
            "RIGHT" => {
                require_args(fname, &args, 2)?;
                match &args[0] {
                    Value::Text(s) => {
                        let n = value_to_i64(&args[1])? as usize;
                        let chars: Vec<char> = s.chars().collect();
                        let start = chars.len().saturating_sub(n);
                        Ok(Value::Text(chars[start..].iter().collect()))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("RIGHT requires text".into())),
                }
            }
            "REPEAT" => {
                require_args(fname, &args, 2)?;
                match &args[0] {
                    Value::Text(s) => {
                        let n = value_to_i64(&args[1])? as usize;
                        Ok(Value::Text(s.repeat(n)))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("REPEAT requires text".into())),
                }
            }
            "REVERSE" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => Ok(Value::Text(s.chars().rev().collect())),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("REVERSE requires text".into())),
                }
            }
            "SPLIT_PART" => {
                require_args(fname, &args, 3)?;
                match (&args[0], &args[1]) {
                    (Value::Text(s), Value::Text(delim)) => {
                        let part_num = value_to_i64(&args[2])? as usize;
                        if part_num == 0 {
                            return Err(ExecError::Unsupported("SPLIT_PART field position must be > 0".into()));
                        }
                        let parts: Vec<&str> = s.split(delim.as_str()).collect();
                        Ok(Value::Text(
                            parts.get(part_num - 1).unwrap_or(&"").to_string(),
                        ))
                    }
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("SPLIT_PART requires text args".into())),
                }
            }
            "TRANSLATE" => {
                require_args(fname, &args, 3)?;
                match (&args[0], &args[1], &args[2]) {
                    (Value::Text(s), Value::Text(from), Value::Text(to)) => {
                        let from_chars: Vec<char> = from.chars().collect();
                        let to_chars: Vec<char> = to.chars().collect();
                        let result: String = s
                            .chars()
                            .filter_map(|c| {
                                if let Some(pos) = from_chars.iter().position(|&fc| fc == c) {
                                    to_chars.get(pos).copied()
                                } else {
                                    Some(c)
                                }
                            })
                            .collect();
                        Ok(Value::Text(result))
                    }
                    (Value::Null, _, _) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("TRANSLATE requires text args".into())),
                }
            }
            "ASCII" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => Ok(Value::Int32(s.chars().next().map(|c| c as i32).unwrap_or(0))),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("ASCII requires text".into())),
                }
            }
            "CHR" => {
                require_args(fname, &args, 1)?;
                let n = value_to_i64(&args[0])? as u32;
                match char::from_u32(n) {
                    Some(c) => Ok(Value::Text(c.to_string())),
                    None => Err(ExecError::Unsupported(format!("invalid character code: {n}"))),
                }
            }
            "REGEXP_REPLACE" => {
                if args.len() < 3 {
                    return Err(ExecError::Unsupported("REGEXP_REPLACE requires at least 3 args".into()));
                }
                match (&args[0], &args[1], &args[2]) {
                    (Value::Text(s), Value::Text(pattern), Value::Text(replacement)) => {
                        // Limit regex pattern length to prevent excessive NFA compilation time.
                        const MAX_REGEX_PATTERN_LEN: usize = 1000;
                        if pattern.len() > MAX_REGEX_PATTERN_LEN {
                            return Err(ExecError::Runtime(format!(
                                "regex pattern too long ({} chars, max {})",
                                pattern.len(), MAX_REGEX_PATTERN_LEN
                            )));
                        }
                        // Optional 4th arg: flags ('g' = global replace)
                        let flags = args.get(3).and_then(|v| if let Value::Text(f) = v { Some(f.as_str()) } else { None }).unwrap_or("");
                        let re = regex::Regex::new(pattern).map_err(|e| {
                            ExecError::Runtime(format!("invalid regex pattern: {e}"))
                        })?;
                        let result = if flags.contains('g') {
                            re.replace_all(s, replacement.as_str()).into_owned()
                        } else {
                            re.replace(s, replacement.as_str()).into_owned()
                        };
                        Ok(Value::Text(result))
                    }
                    (Value::Null, _, _) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("REGEXP_REPLACE requires text args".into())),
                }
            }
            "REGEXP_MATCH" | "REGEXP_MATCHES" => {
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("REGEXP_MATCH requires at least 2 args".into()));
                }
                match (&args[0], &args[1]) {
                    (Value::Text(s), Value::Text(pattern)) => {
                        // Limit regex pattern length to prevent excessive NFA compilation time.
                        const MAX_REGEX_PATTERN_LEN: usize = 1000;
                        if pattern.len() > MAX_REGEX_PATTERN_LEN {
                            return Err(ExecError::Runtime(format!(
                                "regex pattern too long ({} chars, max {})",
                                pattern.len(), MAX_REGEX_PATTERN_LEN
                            )));
                        }
                        let re = regex::Regex::new(pattern).map_err(|e| {
                            ExecError::Runtime(format!("invalid regex pattern: {e}"))
                        })?;
                        match re.captures(s) {
                            Some(caps) => {
                                // Return array of captured groups (group 0 = full match)
                                let groups: Vec<Value> = caps.iter()
                                    .map(|m| match m {
                                        Some(m) => Value::Text(m.as_str().to_string()),
                                        None => Value::Null,
                                    })
                                    .collect();
                                Ok(Value::Array(groups))
                            }
                            None => Ok(Value::Null),
                        }
                    }
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("REGEXP_MATCH requires text args".into())),
                }
            }
            "STARTS_WITH" => {
                require_args(fname, &args, 2)?;
                match (&args[0], &args[1]) {
                    (Value::Text(s), Value::Text(prefix)) => Ok(Value::Bool(s.starts_with(prefix.as_str()))),
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("STARTS_WITH requires text args".into())),
                }
            }
            "ENDS_WITH" => {
                require_args(fname, &args, 2)?;
                match (&args[0], &args[1]) {
                    (Value::Text(s), Value::Text(suffix)) => Ok(Value::Bool(s.ends_with(suffix.as_str()))),
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("ENDS_WITH requires text args".into())),
                }
            }
            "OCTET_LENGTH" | "BIT_LENGTH" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => {
                        let bytes = s.len() as i32;
                        if fname == "BIT_LENGTH" {
                            Ok(Value::Int32(bytes * 8))
                        } else {
                            Ok(Value::Int32(bytes))
                        }
                    }
                    Value::Bytea(b) => {
                        let bytes = b.len() as i32;
                        if fname == "BIT_LENGTH" {
                            Ok(Value::Int32(bytes * 8))
                        } else {
                            Ok(Value::Int32(bytes))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported(format!("{fname} requires text or bytea"))),
                }
            }
            "INITCAP" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => {
                        let mut result = String::with_capacity(s.len());
                        let mut capitalize_next = true;
                        for c in s.chars() {
                            if c.is_alphanumeric() {
                                if capitalize_next {
                                    result.extend(c.to_uppercase());
                                    capitalize_next = false;
                                } else {
                                    result.extend(c.to_lowercase());
                                }
                            } else {
                                result.push(c);
                                capitalize_next = true;
                            }
                        }
                        Ok(Value::Text(result))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("INITCAP requires text".into())),
                }
            }
            "LPAD" => {
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("LPAD requires at least 2 args".into()));
                }
                match &args[0] {
                    Value::Text(s) => {
                        let target_len = value_to_i64(&args[1])? as usize;
                        let fill = if args.len() > 2 {
                            match &args[2] {
                                Value::Text(f) => f.clone(),
                                _ => " ".to_string(),
                            }
                        } else {
                            " ".to_string()
                        };
                        if s.len() >= target_len {
                            Ok(Value::Text(s[..target_len].to_string()))
                        } else {
                            let pad_len = target_len - s.len();
                            let padding: String = fill.chars().cycle().take(pad_len).collect();
                            Ok(Value::Text(format!("{padding}{s}")))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("LPAD requires text".into())),
                }
            }
            "RPAD" => {
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("RPAD requires at least 2 args".into()));
                }
                match &args[0] {
                    Value::Text(s) => {
                        let target_len = value_to_i64(&args[1])? as usize;
                        let fill = if args.len() > 2 {
                            match &args[2] {
                                Value::Text(f) => f.clone(),
                                _ => " ".to_string(),
                            }
                        } else {
                            " ".to_string()
                        };
                        if s.len() >= target_len {
                            Ok(Value::Text(s[..target_len].to_string()))
                        } else {
                            let pad_len = target_len - s.len();
                            let padding: String = fill.chars().cycle().take(pad_len).collect();
                            Ok(Value::Text(format!("{s}{padding}")))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("RPAD requires text".into())),
                }
            }

            // -- Math functions --
            "ABS" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Int32(n) => Ok(Value::Int32(n.abs())),
                    Value::Int64(n) => Ok(Value::Int64(n.abs())),
                    Value::Float64(n) => Ok(Value::Float64(n.abs())),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("ABS requires numeric".into())),
                }
            }
            "ROUND" => {
                if args.is_empty() {
                    return Err(ExecError::Unsupported("ROUND requires at least 1 arg".into()));
                }
                let decimals = if args.len() > 1 {
                    value_to_i64(&args[1])? as i32
                } else {
                    0
                };
                match &args[0] {
                    Value::Float64(n) => {
                        let factor = 10f64.powi(decimals);
                        Ok(Value::Float64((n * factor).round() / factor))
                    }
                    Value::Int32(_) | Value::Int64(_) => Ok(args[0].clone()),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("ROUND requires numeric".into())),
                }
            }
            "CEIL" | "CEILING" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Float64(n) => Ok(Value::Float64(n.ceil())),
                    Value::Int32(_) | Value::Int64(_) => Ok(args[0].clone()),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("CEIL requires numeric".into())),
                }
            }
            "FLOOR" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Float64(n) => Ok(Value::Float64(n.floor())),
                    Value::Int32(_) | Value::Int64(_) => Ok(args[0].clone()),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("FLOOR requires numeric".into())),
                }
            }
            "POWER" | "POW" => {
                require_args(fname, &args, 2)?;
                let base = value_to_f64(&args[0])?;
                let exp = value_to_f64(&args[1])?;
                Ok(Value::Float64(base.powf(exp)))
            }
            "SQRT" => {
                require_args(fname, &args, 1)?;
                let n = value_to_f64(&args[0])?;
                Ok(Value::Float64(n.sqrt()))
            }
            "SIGN" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Int32(n) => Ok(Value::Int32(n.signum())),
                    Value::Int64(n) => Ok(Value::Int64(n.signum())),
                    Value::Float64(n) => {
                        Ok(Value::Int32(if *n > 0.0 { 1 } else if *n < 0.0 { -1 } else { 0 }))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("SIGN requires numeric".into())),
                }
            }
            "LN" => {
                require_args(fname, &args, 1)?;
                let n = value_to_f64(&args[0])?;
                Ok(Value::Float64(n.ln()))
            }
            "LOG" => {
                if args.is_empty() {
                    return Err(ExecError::Unsupported("LOG requires at least 1 arg".into()));
                }
                if args.len() == 1 {
                    let n = value_to_f64(&args[0])?;
                    Ok(Value::Float64(n.log10()))
                } else {
                    let base = value_to_f64(&args[0])?;
                    let n = value_to_f64(&args[1])?;
                    Ok(Value::Float64(n.log(base)))
                }
            }
            "LOG10" => {
                require_args(fname, &args, 1)?;
                let n = value_to_f64(&args[0])?;
                Ok(Value::Float64(n.log10()))
            }
            "EXP" => {
                require_args(fname, &args, 1)?;
                let n = value_to_f64(&args[0])?;
                Ok(Value::Float64(n.exp()))
            }
            "MOD" => {
                require_args(fname, &args, 2)?;
                match (&args[0], &args[1]) {
                    (Value::Int32(a), Value::Int32(b)) if *b != 0 => Ok(Value::Int32(a % b)),
                    (Value::Int64(a), Value::Int64(b)) if *b != 0 => Ok(Value::Int64(a % b)),
                    (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(a % b)),
                    _ => Err(ExecError::Unsupported("MOD requires numeric".into())),
                }
            }
            "RANDOM" => {
                Ok(Value::Float64(rand::random::<f64>()))
            }
            "PI" => {
                Ok(Value::Float64(std::f64::consts::PI))
            }
            "TRUNC" | "TRUNCATE" => {
                if args.is_empty() {
                    return Err(ExecError::Unsupported("TRUNC requires at least 1 arg".into()));
                }
                let decimals = if args.len() > 1 { value_to_i64(&args[1])? as i32 } else { 0 };
                match &args[0] {
                    Value::Float64(n) => {
                        let factor = 10f64.powi(decimals);
                        Ok(Value::Float64((n * factor).trunc() / factor))
                    }
                    Value::Int32(_) | Value::Int64(_) => Ok(args[0].clone()),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("TRUNC requires numeric".into())),
                }
            }
            "DEGREES" => {
                require_args(fname, &args, 1)?;
                let n = value_to_f64(&args[0])?;
                Ok(Value::Float64(n.to_degrees()))
            }
            "RADIANS" => {
                require_args(fname, &args, 1)?;
                let n = value_to_f64(&args[0])?;
                Ok(Value::Float64(n.to_radians()))
            }
            "SIN" => {
                require_args(fname, &args, 1)?;
                Ok(Value::Float64(value_to_f64(&args[0])?.sin()))
            }
            "COS" => {
                require_args(fname, &args, 1)?;
                Ok(Value::Float64(value_to_f64(&args[0])?.cos()))
            }
            "TAN" => {
                require_args(fname, &args, 1)?;
                Ok(Value::Float64(value_to_f64(&args[0])?.tan()))
            }
            "ASIN" => {
                require_args(fname, &args, 1)?;
                Ok(Value::Float64(value_to_f64(&args[0])?.asin()))
            }
            "ACOS" => {
                require_args(fname, &args, 1)?;
                Ok(Value::Float64(value_to_f64(&args[0])?.acos()))
            }
            "ATAN" => {
                require_args(fname, &args, 1)?;
                Ok(Value::Float64(value_to_f64(&args[0])?.atan()))
            }
            "ATAN2" => {
                require_args(fname, &args, 2)?;
                let y = value_to_f64(&args[0])?;
                let x = value_to_f64(&args[1])?;
                Ok(Value::Float64(y.atan2(x)))
            }
            "GCD" => {
                require_args(fname, &args, 2)?;
                let mut a = value_to_i64(&args[0])?.abs();
                let mut b = value_to_i64(&args[1])?.abs();
                while b != 0 {
                    let t = b;
                    b = a % b;
                    a = t;
                }
                Ok(Value::Int64(a))
            }
            "LCM" => {
                require_args(fname, &args, 2)?;
                let a = value_to_i64(&args[0])?.abs();
                let b = value_to_i64(&args[1])?.abs();
                if a == 0 || b == 0 {
                    Ok(Value::Int64(0))
                } else {
                    let mut ga = a;
                    let mut gb = b;
                    while gb != 0 {
                        let t = gb;
                        gb = ga % gb;
                        ga = t;
                    }
                    Ok(Value::Int64(a / ga * b))
                }
            }
            "GENERATE_SERIES" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(ExecError::Unsupported("GENERATE_SERIES requires 2 or 3 args".into()));
                }
                let start = value_to_i64(&args[0])?;
                let stop = value_to_i64(&args[1])?;
                let step = if args.len() == 3 { value_to_i64(&args[2])? } else { 1 };
                if step == 0 {
                    return Err(ExecError::Unsupported("GENERATE_SERIES step cannot be 0".into()));
                }
                let mut vals = Vec::new();
                let mut current = start;
                if step > 0 {
                    while current <= stop {
                        vals.push(Value::Int64(current));
                        current += step;
                    }
                } else {
                    while current >= stop {
                        vals.push(Value::Int64(current));
                        current += step;
                    }
                }
                Ok(Value::Array(vals))
            }

            // -- Null handling functions --
            "COALESCE" => {
                for arg in &args {
                    if !matches!(arg, Value::Null) {
                        return Ok(arg.clone());
                    }
                }
                Ok(Value::Null)
            }
            "NULLIF" => {
                require_args(fname, &args, 2)?;
                if compare_values(&args[0], &args[1]) == Some(std::cmp::Ordering::Equal) {
                    Ok(Value::Null)
                } else {
                    Ok(args[0].clone())
                }
            }
            "GREATEST" => {
                let mut best: Option<Value> = None;
                for arg in &args {
                    if matches!(arg, Value::Null) {
                        return Ok(Value::Null);
                    }
                    best = Some(match best {
                        None => arg.clone(),
                        Some(cur) => {
                            if compare_values(arg, &cur) == Some(std::cmp::Ordering::Greater) {
                                arg.clone()
                            } else {
                                cur
                            }
                        }
                    });
                }
                Ok(best.unwrap_or(Value::Null))
            }
            "LEAST" => {
                let mut best: Option<Value> = None;
                for arg in &args {
                    if matches!(arg, Value::Null) {
                        return Ok(Value::Null);
                    }
                    best = Some(match best {
                        None => arg.clone(),
                        Some(cur) => {
                            if compare_values(arg, &cur) == Some(std::cmp::Ordering::Less) {
                                arg.clone()
                            } else {
                                cur
                            }
                        }
                    });
                }
                Ok(best.unwrap_or(Value::Null))
            }

            // -- Type/info functions --
            "TYPEOF" | "PG_TYPEOF" => {
                require_args(fname, &args, 1)?;
                let type_name = match &args[0] {
                    Value::Null => "null",
                    Value::Bool(_) => "boolean",
                    Value::Int32(_) => "integer",
                    Value::Int64(_) => "bigint",
                    Value::Float64(_) => "double precision",
                    Value::Text(_) => "text",
                    Value::Jsonb(_) => "jsonb",
                    Value::Date(_) => "date",
                    Value::Timestamp(_) => "timestamp without time zone",
                    Value::TimestampTz(_) => "timestamp with time zone",
                    Value::Numeric(_) => "numeric",
                    Value::Uuid(_) => "uuid",
                    Value::Bytea(_) => "bytea",
                    Value::Array(_) => "array",
                    Value::Vector(v) => {
                        return Ok(Value::Text(format!("vector({})", v.len())));
                    }
                    Value::Interval { .. } => "interval",
                };
                Ok(Value::Text(type_name.to_string()))
            }
            "VERSION" => {
                Ok(Value::Text(format!(
                    "PostgreSQL 16.0 (Nucleus {} — The Definitive Database)",
                    env!("CARGO_PKG_VERSION")
                )))
            }
            "CURRENT_DATABASE" => {
                Ok(Value::Text("nucleus".to_string()))
            }
            "CURRENT_SCHEMA" => {
                Ok(Value::Text("public".to_string()))
            }
            "CURRENT_USER" | "CURRENT_ROLE" | "SESSION_USER" => {
                Ok(Value::Text("nucleus".to_string()))
            }

            // -- Date/time functions --
            "NOW" | "CURRENT_TIMESTAMP" => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                // Convert Unix microseconds (epoch 1970) to PG microseconds (epoch 2000-01-01)
                let unix_us = now.as_micros() as i64;
                let pg_epoch_offset_us: i64 = 946_684_800 * 1_000_000; // 2000-01-01 in Unix microseconds
                Ok(Value::TimestampTz(unix_us - pg_epoch_offset_us))
            }
            "CURRENT_DATE" => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                // Days since 1970-01-01, convert to PG epoch (days since 2000-01-01)
                let unix_days = (now.as_secs() / 86400) as i32;
                let pg_epoch_days: i32 = 10957; // 2000-01-01 in days since 1970-01-01
                Ok(Value::Date(unix_days - pg_epoch_days))
            }
            "CURRENT_TIME" => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                let secs = now.as_secs();
                let time_of_day = secs % 86400;
                let hours = time_of_day / 3600;
                let minutes = (time_of_day % 3600) / 60;
                let seconds = time_of_day % 60;
                Ok(Value::Text(format!("{hours:02}:{minutes:02}:{seconds:02}")))
            }
            "CLOCK_TIMESTAMP" => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                let unix_us = now.as_micros() as i64;
                let pg_epoch_offset_us: i64 = 946_684_800 * 1_000_000;
                Ok(Value::TimestampTz(unix_us - pg_epoch_offset_us))
            }
            "EXTRACT" | "DATE_PART" => {
                require_args(fname, &args, 2)?;
                let field = match &args[0] {
                    Value::Text(s) => s.to_lowercase(),
                    _ => return Err(ExecError::Unsupported("EXTRACT field must be text".into())),
                };
                match &args[1] {
                    Value::Date(d) => {
                        let (y, m, day) = crate::types::days_to_ymd(*d);
                        match field.as_str() {
                            "year" => Ok(Value::Int32(y)),
                            "month" => Ok(Value::Int32(m as i32)),
                            "day" => Ok(Value::Int32(day as i32)),
                            "dow" | "dayofweek" => {
                                // 0 = Sunday
                                let jdn = *d + 2451545;
                                Ok(Value::Int32(jdn.rem_euclid(7)))
                            }
                            "doy" | "dayofyear" => {
                                let jan1 = crate::types::ymd_to_days(y, 1, 1);
                                Ok(Value::Int32(*d - jan1 + 1))
                            }
                            "epoch" => Ok(Value::Int64(*d as i64 * 86400)),
                            _ => Err(ExecError::Unsupported(format!("EXTRACT({field}) from date"))),
                        }
                    }
                    Value::Timestamp(ts) => {
                        let total_secs = *ts / 1_000_000;
                        let days = (total_secs / 86400) as i32;
                        let time_secs = total_secs % 86400;
                        let (y, m, day) = crate::types::days_to_ymd(days);
                        match field.as_str() {
                            "year" => Ok(Value::Int32(y)),
                            "month" => Ok(Value::Int32(m as i32)),
                            "day" => Ok(Value::Int32(day as i32)),
                            "hour" => Ok(Value::Int32((time_secs / 3600) as i32)),
                            "minute" => Ok(Value::Int32(((time_secs % 3600) / 60) as i32)),
                            "second" => Ok(Value::Int32((time_secs % 60) as i32)),
                            "epoch" => Ok(Value::Int64(total_secs)),
                            "dow" | "dayofweek" => {
                                let jdn = days + 2451545;
                                Ok(Value::Int32(jdn.rem_euclid(7)))
                            }
                            _ => Err(ExecError::Unsupported(format!("EXTRACT({field}) from timestamp"))),
                        }
                    }
                    Value::Int64(v) => {
                        // Treat as epoch seconds
                        let total_secs = *v;
                        let days = (total_secs / 86400) as i32;
                        let time_secs = total_secs % 86400;
                        let (y, m, day) = crate::types::days_to_ymd(days);
                        match field.as_str() {
                            "year" => Ok(Value::Int32(y)),
                            "month" => Ok(Value::Int32(m as i32)),
                            "day" => Ok(Value::Int32(day as i32)),
                            "hour" => Ok(Value::Int32((time_secs / 3600) as i32)),
                            "minute" => Ok(Value::Int32(((time_secs % 3600) / 60) as i32)),
                            "second" => Ok(Value::Int32((time_secs % 60) as i32)),
                            "epoch" => Ok(Value::Int64(total_secs)),
                            _ => Err(ExecError::Unsupported(format!("EXTRACT({field}) from integer"))),
                        }
                    }
                    Value::Text(s) => {
                        // Try to parse as date or timestamp
                        if let Some(d) = parse_date_string(s) {
                            let (y, m, day) = crate::types::days_to_ymd(d);
                            match field.as_str() {
                                "year" => Ok(Value::Int32(y)),
                                "month" => Ok(Value::Int32(m as i32)),
                                "day" => Ok(Value::Int32(day as i32)),
                                "epoch" => Ok(Value::Int64(d as i64 * 86400)),
                                _ => Err(ExecError::Unsupported(format!("EXTRACT({field}) from text"))),
                            }
                        } else {
                            Err(ExecError::Unsupported("cannot parse date/time from text".into()))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("EXTRACT requires date/timestamp".into())),
                }
            }
            "DATE_TRUNC" => {
                require_args(fname, &args, 2)?;
                let field = match &args[0] {
                    Value::Text(s) => s.to_lowercase(),
                    _ => return Err(ExecError::Unsupported("DATE_TRUNC field must be text".into())),
                };
                match &args[1] {
                    Value::Timestamp(ts) => {
                        let total_secs = *ts / 1_000_000;
                        let days = (total_secs / 86400) as i32;
                        let time_secs = total_secs % 86400;
                        let (y, m, _d) = crate::types::days_to_ymd(days);
                        let truncated_us = match field.as_str() {
                            "year" => crate::types::ymd_to_days(y, 1, 1) as i64 * 86400 * 1_000_000,
                            "month" => crate::types::ymd_to_days(y, m, 1) as i64 * 86400 * 1_000_000,
                            "day" => days as i64 * 86400 * 1_000_000,
                            "hour" => days as i64 * 86400 * 1_000_000 + (time_secs / 3600) * 3600 * 1_000_000,
                            "minute" => days as i64 * 86400 * 1_000_000 + (time_secs / 60) * 60 * 1_000_000,
                            _ => return Err(ExecError::Unsupported(format!("DATE_TRUNC({field})"))),
                        };
                        Ok(Value::Timestamp(truncated_us))
                    }
                    Value::Date(d) => {
                        let (y, m, _) = crate::types::days_to_ymd(*d);
                        let truncated = match field.as_str() {
                            "year" => crate::types::ymd_to_days(y, 1, 1),
                            "month" => crate::types::ymd_to_days(y, m, 1),
                            "day" => *d,
                            _ => return Err(ExecError::Unsupported(format!("DATE_TRUNC({field})"))),
                        };
                        Ok(Value::Date(truncated))
                    }
                    Value::Text(s) => {
                        if let Some((y, m, d, hour, minute, _second)) = parse_timestamp_parts(s) {
                            let result = match field.as_str() {
                                "year" => format!("{y:04}-01-01 00:00:00"),
                                "month" => format!("{y:04}-{m:02}-01 00:00:00"),
                                "day" => format!("{y:04}-{m:02}-{d:02} 00:00:00"),
                                "hour" => format!("{y:04}-{m:02}-{d:02} {hour:02}:00:00"),
                                "minute" => format!("{y:04}-{m:02}-{d:02} {hour:02}:{minute:02}:00"),
                                _ => return Err(ExecError::Unsupported(format!("DATE_TRUNC({field})"))),
                            };
                            Ok(Value::Text(result))
                        } else {
                            Err(ExecError::Unsupported(format!("cannot parse date/time: {s}")))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("DATE_TRUNC requires timestamp/date".into())),
                }
            }
            "AGE" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(ExecError::Unsupported("AGE requires 1 or 2 args".into()));
                }
                let d1 = match &args[0] {
                    Value::Date(d) => *d,
                    Value::Timestamp(ts) => (*ts / 1_000_000 / 86400) as i32,
                    Value::Text(s) => parse_date_string(s).ok_or_else(|| ExecError::Unsupported(format!("AGE cannot parse: {s}")))?,
                    _ => return Err(ExecError::Unsupported("AGE requires date/timestamp".into())),
                };
                let d2 = if args.len() == 2 {
                    match &args[1] {
                        Value::Date(d) => *d,
                        Value::Timestamp(ts) => (*ts / 1_000_000 / 86400) as i32,
                        Value::Text(s) => parse_date_string(s).ok_or_else(|| ExecError::Unsupported(format!("AGE cannot parse: {s}")))?,
                        _ => return Err(ExecError::Unsupported("AGE requires date/timestamp".into())),
                    }
                } else {
                    // age(date) = age from now
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    (now / 86400) as i32 - 10957 // adjust epoch 1970 -> 2000
                };
                let diff = (d1 - d2).abs();
                let years = diff / 365;
                let months = (diff % 365) / 30;
                let days = diff % 30;
                Ok(Value::Text(format!("{years} years {months} mons {days} days")))
            }
            "TO_CHAR" => {
                require_args(fname, &args, 2)?;
                let _fmt = match &args[1] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("TO_CHAR format must be text".into())),
                };
                // Simplified TO_CHAR: just convert to string representation
                match &args[0] {
                    Value::Date(d) => {
                        let (y, m, day) = crate::types::days_to_ymd(*d);
                        Ok(Value::Text(format!("{y:04}-{m:02}-{day:02}")))
                    }
                    Value::Timestamp(ts) => {
                        let total_secs = (*ts / 1_000_000) as u64;
                        Ok(Value::Text(format_timestamp(total_secs)))
                    }
                    Value::Int32(n) => Ok(Value::Text(format!("{n}"))),
                    Value::Int64(n) => Ok(Value::Text(format!("{n}"))),
                    Value::Float64(n) => Ok(Value::Text(format!("{n}"))),
                    Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Text(args[0].to_string())),
                }
            }
            "TO_DATE" => {
                require_args(fname, &args, 2)?;
                let s = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("TO_DATE requires text".into())),
                };
                match parse_date_string(&s) {
                    Some(d) => Ok(Value::Date(d)),
                    None => Err(ExecError::Unsupported(format!("cannot parse date: {s}"))),
                }
            }
            "TO_TIMESTAMP" => {
                if args.len() == 1 {
                    // to_timestamp(epoch_seconds)
                    match &args[0] {
                        Value::Int64(n) => Ok(Value::Timestamp(*n * 1_000_000)),
                        Value::Int32(n) => Ok(Value::Timestamp(*n as i64 * 1_000_000)),
                        Value::Float64(n) => Ok(Value::Timestamp((*n * 1_000_000.0) as i64)),
                        Value::Text(s) => {
                            // Try parsing as timestamp string (with time part)
                            if let Some((y, m, d, h, min, sec)) = parse_timestamp_parts(s) {
                                let days = crate::types::ymd_to_days(y, m, d) as i64;
                                let time_us = (h as i64 * 3600 + min as i64 * 60 + sec as i64) * 1_000_000;
                                Ok(Value::Timestamp(days * 86400 * 1_000_000 + time_us))
                            } else {
                                Err(ExecError::Unsupported(format!("cannot parse timestamp: {s}")))
                            }
                        }
                        Value::Null => Ok(Value::Null),
                        _ => Err(ExecError::Unsupported("TO_TIMESTAMP requires numeric or text".into())),
                    }
                } else {
                    require_args(fname, &args, 2)?;
                    let s = match &args[0] {
                        Value::Text(s) => s.clone(),
                        _ => return Err(ExecError::Unsupported("TO_TIMESTAMP requires text".into())),
                    };
                    if let Some((y, m, d, h, min, sec)) = parse_timestamp_parts(&s) {
                        let days = crate::types::ymd_to_days(y, m, d) as i64;
                        let time_us = (h as i64 * 3600 + min as i64 * 60 + sec as i64) * 1_000_000;
                        Ok(Value::Timestamp(days * 86400 * 1_000_000 + time_us))
                    } else {
                        Err(ExecError::Unsupported(format!("cannot parse timestamp: {s}")))
                    }
                }
            }
            "MAKE_DATE" => {
                require_args(fname, &args, 3)?;
                let y = value_to_i64(&args[0])? as i32;
                let m = value_to_i64(&args[1])? as u32;
                let d = value_to_i64(&args[2])? as u32;
                Ok(Value::Date(crate::types::ymd_to_days(y, m, d)))
            }

            // -- JSON functions --
            "JSON_BUILD_OBJECT" | "JSONB_BUILD_OBJECT" => {
                if args.len() % 2 != 0 {
                    return Err(ExecError::Unsupported(
                        "jsonb_build_object requires even number of args".into(),
                    ));
                }
                let mut map = serde_json::Map::new();
                for pair in args.chunks(2) {
                    let key = match &pair[0] {
                        Value::Text(s) => s.clone(),
                        other => other.to_string(),
                    };
                    let val = value_to_json(&pair[1]);
                    map.insert(key, val);
                }
                Ok(Value::Jsonb(serde_json::Value::Object(map)))
            }
            "JSON_BUILD_ARRAY" | "JSONB_BUILD_ARRAY" => {
                let arr: Vec<serde_json::Value> = args.iter().map(value_to_json).collect();
                Ok(Value::Jsonb(serde_json::Value::Array(arr)))
            }
            "JSON_ARRAY_LENGTH" | "JSONB_ARRAY_LENGTH" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Jsonb(serde_json::Value::Array(arr)) => {
                        Ok(Value::Int32(arr.len() as i32))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Null),
                }
            }
            "JSON_TYPEOF" | "JSONB_TYPEOF" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Jsonb(v) => {
                        let t = match v {
                            serde_json::Value::Null => "null",
                            serde_json::Value::Bool(_) => "boolean",
                            serde_json::Value::Number(_) => "number",
                            serde_json::Value::String(_) => "string",
                            serde_json::Value::Array(_) => "array",
                            serde_json::Value::Object(_) => "object",
                        };
                        Ok(Value::Text(t.to_string()))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("jsonb_typeof requires jsonb".into())),
                }
            }
            "TO_JSON" | "TO_JSONB" | "ROW_TO_JSON" => {
                require_args(fname, &args, 1)?;
                Ok(Value::Jsonb(value_to_json(&args[0])))
            }
            "JSONB_SET" | "JSON_SET" => {
                if args.len() < 3 {
                    return Err(ExecError::Unsupported("jsonb_set requires at least 3 args".into()));
                }
                let new_val = value_to_json(&args[2]);
                match (&args[0], &args[1]) {
                    (Value::Jsonb(target_json), Value::Jsonb(serde_json::Value::Array(path))) => {
                        let mut target = target_json.clone();
                        let path_strs: Vec<String> = path.iter().map(|p| match p {
                            serde_json::Value::String(s) => s.clone(),
                            other => other.to_string(),
                        }).collect();
                        jsonb_set_path(&mut target, &path_strs, new_val);
                        Ok(Value::Jsonb(target))
                    }
                    (Value::Jsonb(target_json), Value::Text(key)) => {
                        let mut target = target_json.clone();
                        if let serde_json::Value::Object(map) = &mut target {
                            map.insert(key.clone(), new_val);
                        }
                        Ok(Value::Jsonb(target))
                    }
                    (Value::Null, _) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("jsonb_set requires jsonb target".into())),
                }
            }
            "JSONB_PRETTY" | "JSON_PRETTY" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Jsonb(v) => Ok(Value::Text(serde_json::to_string_pretty(v).unwrap_or_default())),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("jsonb_pretty requires jsonb".into())),
                }
            }
            "JSONB_OBJECT_KEYS" | "JSON_OBJECT_KEYS" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Jsonb(serde_json::Value::Object(map)) => {
                        let keys: Vec<serde_json::Value> = map.keys()
                            .map(|k| serde_json::Value::String(k.clone()))
                            .collect();
                        Ok(Value::Jsonb(serde_json::Value::Array(keys)))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("jsonb_object_keys requires jsonb object".into())),
                }
            }
            "JSONB_STRIP_NULLS" | "JSON_STRIP_NULLS" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Jsonb(v) => Ok(Value::Jsonb(strip_json_nulls(v))),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("jsonb_strip_nulls requires jsonb".into())),
                }
            }
            "JSONB_EXTRACT_PATH" | "JSON_EXTRACT_PATH" => {
                if args.is_empty() {
                    return Err(ExecError::Unsupported("jsonb_extract_path requires at least 1 arg".into()));
                }
                let mut current = match &args[0] {
                    Value::Jsonb(v) => v.clone(),
                    Value::Null => return Ok(Value::Null),
                    _ => return Err(ExecError::Unsupported("jsonb_extract_path requires jsonb".into())),
                };
                for arg in &args[1..] {
                    let key = match arg {
                        Value::Text(s) => s.clone(),
                        other => other.to_string(),
                    };
                    current = match current {
                        serde_json::Value::Object(ref map) => map.get(&key).cloned().unwrap_or(serde_json::Value::Null),
                        serde_json::Value::Array(ref arr) => {
                            if let Ok(idx) = key.parse::<usize>() {
                                arr.get(idx).cloned().unwrap_or(serde_json::Value::Null)
                            } else {
                                serde_json::Value::Null
                            }
                        }
                        _ => serde_json::Value::Null,
                    };
                }
                Ok(Value::Jsonb(current))
            }
            "JSONB_EXTRACT_PATH_TEXT" | "JSON_EXTRACT_PATH_TEXT" => {
                if args.is_empty() {
                    return Err(ExecError::Unsupported("jsonb_extract_path_text requires at least 1 arg".into()));
                }
                let mut current = match &args[0] {
                    Value::Jsonb(v) => v.clone(),
                    Value::Null => return Ok(Value::Null),
                    _ => return Err(ExecError::Unsupported("jsonb_extract_path_text requires jsonb".into())),
                };
                for arg in &args[1..] {
                    let key = match arg {
                        Value::Text(s) => s.clone(),
                        other => other.to_string(),
                    };
                    current = match current {
                        serde_json::Value::Object(ref map) => map.get(&key).cloned().unwrap_or(serde_json::Value::Null),
                        serde_json::Value::Array(ref arr) => {
                            if let Ok(idx) = key.parse::<usize>() {
                                arr.get(idx).cloned().unwrap_or(serde_json::Value::Null)
                            } else {
                                serde_json::Value::Null
                            }
                        }
                        _ => serde_json::Value::Null,
                    };
                }
                match current {
                    serde_json::Value::Null => Ok(Value::Null),
                    serde_json::Value::String(s) => Ok(Value::Text(s)),
                    other => Ok(Value::Text(other.to_string())),
                }
            }

            // -- Geo/spatial functions --
            "GEO_DISTANCE" | "ST_DISTANCE" => {
                self.check_subsystem("geo")?;
                require_args(fname, &args, 4)?;
                let a = crate::geo::Point {
                    x: value_to_f64(&args[1])?, // lon
                    y: value_to_f64(&args[0])?, // lat
                };
                let b = crate::geo::Point {
                    x: value_to_f64(&args[3])?,
                    y: value_to_f64(&args[2])?,
                };
                Ok(Value::Float64(crate::geo::haversine_distance(&a, &b)))
            }
            "GEO_DISTANCE_EUCLIDEAN" | "ST_DISTANCE_EUCLIDEAN" => {
                require_args(fname, &args, 4)?;
                let a = crate::geo::Point {
                    x: value_to_f64(&args[0])?,
                    y: value_to_f64(&args[1])?,
                };
                let b = crate::geo::Point {
                    x: value_to_f64(&args[2])?,
                    y: value_to_f64(&args[3])?,
                };
                Ok(Value::Float64(crate::geo::euclidean_distance(&a, &b)))
            }
            "GEO_WITHIN" | "ST_DWITHIN" => {
                require_args(fname, &args, 5)?;
                let a = crate::geo::Point {
                    x: value_to_f64(&args[1])?,
                    y: value_to_f64(&args[0])?,
                };
                let b = crate::geo::Point {
                    x: value_to_f64(&args[3])?,
                    y: value_to_f64(&args[2])?,
                };
                let radius = value_to_f64(&args[4])?;
                Ok(Value::Bool(crate::geo::st_dwithin(&a, &b, radius)))
            }
            "GEO_AREA" | "ST_AREA" => {
                if args.len() < 6 || args.len() % 2 != 0 {
                    return Err(ExecError::Unsupported(
                        "ST_AREA requires at least 3 coordinate pairs (6 args)".into(),
                    ));
                }
                let exterior: Vec<crate::geo::Point> = args
                    .chunks(2)
                    .map(|pair| crate::geo::Point {
                        x: value_to_f64(&pair[0]).unwrap_or(0.0),
                        y: value_to_f64(&pair[1]).unwrap_or(0.0),
                    })
                    .collect();
                let poly = crate::geo::Polygon::new(exterior);
                Ok(Value::Float64(poly.area()))
            }

            // -- Vector similarity functions --
            "VECTOR_L2_DISTANCE" | "L2_DISTANCE" => {
                require_args(fname, &args, 2)?;
                let a = json_to_vector(&args[0])?;
                let b = json_to_vector(&args[1])?;
                Ok(Value::Float64(
                    crate::vector::distance(&a, &b, crate::vector::DistanceMetric::L2) as f64,
                ))
            }
            "VECTOR_COSINE_DISTANCE" | "COSINE_DISTANCE" => {
                require_args(fname, &args, 2)?;
                let a = json_to_vector(&args[0])?;
                let b = json_to_vector(&args[1])?;
                Ok(Value::Float64(
                    crate::vector::distance(&a, &b, crate::vector::DistanceMetric::Cosine) as f64,
                ))
            }
            "VECTOR_INNER_PRODUCT" | "INNER_PRODUCT" => {
                require_args(fname, &args, 2)?;
                let a = json_to_vector(&args[0])?;
                let b = json_to_vector(&args[1])?;
                // Return positive inner product (not negated)
                Ok(Value::Float64(
                    -crate::vector::distance(&a, &b, crate::vector::DistanceMetric::InnerProduct) as f64,
                ))
            }

            // -- Full-text search functions --
            "TS_RANK" | "FTS_RANK" => {
                self.check_subsystem("fts")?;
                // BM25 score for a document against a query
                require_args(fname, &args, 2)?;
                let doc = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => args[0].to_string(),
                };
                let query = match &args[1] {
                    Value::Text(s) => s.clone(),
                    _ => args[1].to_string(),
                };
                let tokens = crate::fts::tokenize(&doc);
                let query_tokens = crate::fts::tokenize(&query);
                // Simple TF score
                let mut score = 0.0f64;
                for qt in &query_tokens {
                    let tf = tokens.iter().filter(|t| t.term == qt.term).count() as f64;
                    score += tf / tokens.len().max(1) as f64;
                }
                Ok(Value::Float64(score))
            }
            "TO_TSVECTOR" => {
                require_args(fname, &args, 1)?;
                let text = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => args[0].to_string(),
                };
                let tokens = crate::fts::tokenize(&text);
                let terms: Vec<String> = tokens.into_iter().map(|t| t.term).collect();
                Ok(Value::Text(terms.join(" ")))
            }
            "TO_TSQUERY" => {
                require_args(fname, &args, 1)?;
                let text = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => args[0].to_string(),
                };
                let tokens = crate::fts::tokenize(&text);
                let terms: Vec<String> = tokens.into_iter().map(|t| t.term).collect();
                Ok(Value::Text(terms.join(" & ")))
            }
            "LEVENSHTEIN" => {
                require_args(fname, &args, 2)?;
                match (&args[0], &args[1]) {
                    (Value::Text(a), Value::Text(b)) => {
                        Ok(Value::Int32(crate::fts::levenshtein(a, b) as i32))
                    }
                    _ => Err(ExecError::Unsupported("LEVENSHTEIN requires text args".into())),
                }
            }

            // -- Time-series functions --
            "TIME_BUCKET" => {
                self.check_subsystem("timeseries")?;
                require_args(fname, &args, 2)?;
                let bucket_millis = value_to_i64(&args[0])? as u64;
                let ts = value_to_i64(&args[1])? as u64;
                if bucket_millis == 0 {
                    return Err(ExecError::Unsupported("TIME_BUCKET size must be positive".into()));
                }
                // Direct bucket calculation (same as timeseries::time_bucket but with raw millis)
                let bucket = (ts / bucket_millis) * bucket_millis;
                Ok(Value::Int64(bucket as i64))
            }

            // -- Sparse vector functions --
            "SPARSE_DOT_PRODUCT" => {
                require_args(fname, &args, 2)?;
                let a = json_to_sparse_vec(&args[0])?;
                let b = json_to_sparse_vec(&args[1])?;
                Ok(Value::Float64(a.dot(&b) as f64))
            }

            // -- Sparse vector index functions (shared persistent SparseIndex) --
            "SPARSE_INSERT" => {
                // sparse_insert(doc_id, json_vector) → true
                // Inserts the given sparse vector into the shared SparseIndex under doc_id.
                // json_vector: JSON object {"dim_index": weight, ...}
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("SPARSE_INSERT requires (doc_id, json_vector)".into()));
                }
                let doc_id = val_to_u64(&args[0], "SPARSE_INSERT doc_id")?;
                let vec = json_to_sparse_vec(&args[1])?;
                let nnz = vec.nnz();
                self.sparse_index.write().insert(doc_id, vec);
                // Each posting: doc_id(8) + weight(4) + index(4) = ~16 bytes.
                self.memory_allocator.lock().request("sparse", nnz * 16 + 32);
                Ok(Value::Bool(true))
            }
            "SPARSE_REMOVE" => {
                // sparse_remove(doc_id) → true/false
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("SPARSE_REMOVE requires (doc_id)".into()));
                }
                let doc_id = val_to_u64(&args[0], "SPARSE_REMOVE doc_id")?;
                let removed = self.sparse_index.write().remove(doc_id);
                if removed {
                    self.memory_allocator.lock().release("sparse", 256);
                }
                Ok(Value::Bool(removed))
            }
            "SPARSE_DOC_COUNT" => {
                // sparse_doc_count() → integer
                Ok(Value::Int64(self.sparse_index.read().doc_count() as i64))
            }
            "SPARSE_SEARCH" => {
                // sparse_search(json_query, top_k) → JSON [{doc_id, score}]
                // Exact brute-force search.
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("SPARSE_SEARCH requires (json_query, top_k)".into()));
                }
                let query = json_to_sparse_vec(&args[0])?;
                let top_k = (val_to_u64(&args[1], "SPARSE_SEARCH top_k")? as usize).min(10_000);
                let results = self.sparse_index.read().search_exact(&query, top_k);
                let json = results.iter()
                    .map(|(id, score)| format!(r#"{{"doc_id":{id},"score":{score:.6}}}"#))
                    .collect::<Vec<_>>()
                    .join(",");
                Ok(Value::Text(format!("[{json}]")))
            }
            "SPARSE_WAND" => {
                // sparse_wand(json_query, top_k) → JSON [{doc_id, score}]
                // WAND top-k search with pivot-based upper-bound pruning.
                // Faster than SPARSE_SEARCH for high-selectivity queries on large indexes.
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("SPARSE_WAND requires (json_query, top_k)".into()));
                }
                let query = json_to_sparse_vec(&args[0])?;
                let top_k = (val_to_u64(&args[1], "SPARSE_WAND top_k")? as usize).min(10_000);
                let results = self.sparse_index.read().search_wand_pruned(&query, top_k);
                let json = results.iter()
                    .map(|(id, score)| format!(r#"{{"doc_id":{id},"score":{score:.6}}}"#))
                    .collect::<Vec<_>>()
                    .join(",");
                Ok(Value::Text(format!("[{json}]")))
            }

            // -- Memory allocator query functions (Principle 2) --
            "MEM_USAGE" => {
                // mem_usage() → bytes currently tracked across all subsystems
                Ok(Value::Int64(self.memory_allocator.lock().total_allocated() as i64))
            }
            "MEM_BUDGET" => {
                // mem_budget() → total memory budget in bytes
                Ok(Value::Int64(self.memory_allocator.lock().total_budget() as i64))
            }
            "MEM_AVAILABLE" => {
                // mem_available() → budget - usage
                Ok(Value::Int64(self.memory_allocator.lock().available() as i64))
            }
            "MEM_UTILIZATION" => {
                // mem_utilization() → % of budget used (0.0–100.0)
                Ok(Value::Float64(self.memory_allocator.lock().utilization()))
            }
            "MEM_PRESSURE_EVENTS" => {
                // mem_pressure_events() → number of times pressure was applied
                Ok(Value::Int64(self.memory_allocator.lock().pressure_events() as i64))
            }
            "MEM_PEAK" => {
                // mem_peak() → high-water mark in bytes
                Ok(Value::Int64(self.memory_allocator.lock().peak_allocated() as i64))
            }
            "MEM_STATS" => {
                // mem_stats() → JSON object of all subsystem allocations
                let alloc = self.memory_allocator.lock();
                let mut parts: Vec<String> = alloc.all_allocations().iter().map(|a| {
                    format!(
                        r#"{{"name":"{n}","current_bytes":{c},"peak_bytes":{p},"allocation_count":{ac},"priority":"{pr:?}"}}"#,
                        n = a.name, c = a.current_bytes, p = a.peak_bytes,
                        ac = a.allocation_count, pr = a.priority,
                    )
                }).collect();
                parts.sort(); // deterministic order
                Ok(Value::Text(format!("[{}]", parts.join(","))))
            }

            // -- Hashing / utility functions --
            "GEN_RANDOM_UUID" | "UUID_GENERATE_V4" => {
                // Generate a random UUID v4 using rand::Rng.
                use rand::Rng;
                let mut bytes = [0u8; 16];
                rand::thread_rng().fill(&mut bytes);
                // Set version bits (v4) and variant bits (RFC 4122)
                bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
                bytes[8] = (bytes[8] & 0x3f) | 0x80; // variant 10xx
                Ok(Value::Uuid(bytes))
            }
            "MD5" => {
                require_args(fname, &args, 1)?;
                let text = match &args[0] {
                    Value::Text(s) => s.clone(),
                    Value::Null => return Ok(Value::Null),
                    other => other.to_string(),
                };
                // Use FNV-1a hash (fast, non-crypto) formatted as hex
                let hash = crate::blob::content_hash(text.as_bytes());
                Ok(Value::Text(format!("{hash:016x}")))
            }
            "ENCODE" => {
                require_args(fname, &args, 2)?;
                let data = match &args[0] {
                    Value::Text(s) => s.as_bytes().to_vec(),
                    _ => return Err(ExecError::Unsupported("ENCODE requires text input".into())),
                };
                let format = match &args[1] {
                    Value::Text(s) => s.to_lowercase(),
                    _ => return Err(ExecError::Unsupported("ENCODE format must be text".into())),
                };
                match format.as_str() {
                    "hex" => {
                        let hex: String = data.iter().map(|b| format!("{b:02x}")).collect();
                        Ok(Value::Text(hex))
                    }
                    "base64" => {
                        use base64::Engine;
                        Ok(Value::Text(base64::engine::general_purpose::STANDARD.encode(&data)))
                    }
                    _ => Err(ExecError::Unsupported(format!("unknown encoding: {format}"))),
                }
            }
            "DECODE" => {
                require_args(fname, &args, 2)?;
                let encoded = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("DECODE requires text input".into())),
                };
                let format = match &args[1] {
                    Value::Text(s) => s.to_lowercase(),
                    _ => return Err(ExecError::Unsupported("DECODE format must be text".into())),
                };
                match format.as_str() {
                    "hex" => {
                        let bytes: Vec<u8> = (0..encoded.len())
                            .step_by(2)
                            .filter_map(|i| u8::from_str_radix(&encoded[i..i + 2], 16).ok())
                            .collect();
                        Ok(Value::Text(String::from_utf8_lossy(&bytes).to_string()))
                    }
                    "base64" => {
                        use base64::Engine;
                        match base64::engine::general_purpose::STANDARD.decode(&encoded) {
                            Ok(bytes) => Ok(Value::Text(String::from_utf8_lossy(&bytes).to_string())),
                            Err(e) => Err(ExecError::Unsupported(format!("base64 decode error: {e}"))),
                        }
                    }
                    _ => Err(ExecError::Unsupported(format!("unknown encoding: {format}"))),
                }
            }

            // -- Sequence functions --
            "NEXTVAL" => {
                require_args(fname, &args, 1)?;
                let seq_name = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => args[0].to_string(),
                };
                let seqs = self.sequences.read();
                if let Some(seq_mutex) = seqs.get(&seq_name) {
                    let mut seq = seq_mutex.lock();
                    seq.current += seq.increment;
                    if seq.current > seq.max_value {
                        return Err(ExecError::Unsupported(format!(
                            "sequence {seq_name} reached max value"
                        )));
                    }
                    let val = seq.current;
                    drop(seq);
                    drop(seqs);
                    // Persist sequence state synchronously so it survives restart.
                    self.persist_sequences_sync();
                    Ok(Value::Int64(val))
                } else {
                    Err(ExecError::Unsupported(format!("sequence {seq_name} does not exist")))
                }
            }
            "CURRVAL" => {
                require_args(fname, &args, 1)?;
                let seq_name = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => args[0].to_string(),
                };
                let seqs = self.sequences.read();
                if let Some(seq_mutex) = seqs.get(&seq_name) {
                    let seq = seq_mutex.lock();
                    Ok(Value::Int64(seq.current))
                } else {
                    Err(ExecError::Unsupported(format!("sequence {seq_name} does not exist")))
                }
            }
            "SETVAL" => {
                require_args(fname, &args, 2)?;
                let seq_name = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => args[0].to_string(),
                };
                let new_val = value_to_i64(&args[1])?;
                let seqs = self.sequences.read();
                if let Some(seq_mutex) = seqs.get(&seq_name) {
                    let mut seq = seq_mutex.lock();
                    seq.current = new_val;
                    drop(seq);
                    drop(seqs);
                    self.persist_sequences_sync();
                    Ok(Value::Int64(new_val))
                } else {
                    Err(ExecError::Unsupported(format!("sequence {seq_name} does not exist")))
                }
            }

            // -- PostgreSQL system/catalog functions --
            "PG_BACKEND_PID" => {
                Ok(Value::Int32(std::process::id() as i32))
            }
            "TXID_CURRENT" => {
                Ok(Value::Int64(1))
            }
            "OBJ_DESCRIPTION" => {
                // Stub: always returns NULL
                Ok(Value::Null)
            }
            "COL_DESCRIPTION" => {
                // Stub: always returns NULL
                Ok(Value::Null)
            }
            "FORMAT_TYPE" => {
                // Map common PostgreSQL type OIDs to type names
                if args.is_empty() {
                    return Err(ExecError::Unsupported("FORMAT_TYPE requires at least 1 arg".into()));
                }
                let oid = value_to_i64(&args[0])?;
                let type_name = match oid {
                    16 => "boolean",
                    20 => "bigint",
                    21 => "smallint",
                    23 => "integer",
                    25 => "text",
                    700 => "real",
                    701 => "double precision",
                    1043 => "character varying",
                    1082 => "date",
                    1114 => "timestamp without time zone",
                    1184 => "timestamp with time zone",
                    1700 => "numeric",
                    2950 => "uuid",
                    3802 => "jsonb",
                    17 => "bytea",
                    1042 => "character",
                    1005 => "smallint[]",
                    1007 => "integer[]",
                    1009 => "text[]",
                    1016 => "bigint[]",
                    _ => "unknown",
                };
                Ok(Value::Text(type_name.to_string()))
            }
            "PG_GET_EXPR" => {
                // Return first arg as text
                if args.is_empty() {
                    return Ok(Value::Null);
                }
                match &args[0] {
                    Value::Text(s) => Ok(Value::Text(s.clone())),
                    Value::Null => Ok(Value::Null),
                    other => Ok(Value::Text(other.to_string())),
                }
            }
            "PG_TABLE_IS_VISIBLE" => {
                // Stub: always returns true
                Ok(Value::Bool(true))
            }
            "HAS_TABLE_PRIVILEGE" => {
                // has_table_privilege(table, privilege) or has_table_privilege(user, table, privilege)
                let (table_name, privilege) = if args.len() >= 3 {
                    // 3-arg form: (user, table, privilege) — ignore user, check current session
                    match (&args[1], &args[2]) {
                        (Value::Text(t), Value::Text(p)) => (t.clone(), p.clone()),
                        _ => return Ok(Value::Bool(true)),
                    }
                } else if args.len() == 2 {
                    match (&args[0], &args[1]) {
                        (Value::Text(t), Value::Text(p)) => (t.clone(), p.clone()),
                        _ => return Ok(Value::Bool(true)),
                    }
                } else {
                    return Ok(Value::Bool(true));
                };
                let priv_upper = privilege.to_uppercase();
                let priv_key = priv_upper.split_whitespace().next().unwrap_or(&priv_upper);
                let result = sync_block_on(self.check_privilege(&table_name, priv_key));
                Ok(Value::Bool(result))
            }
            "HAS_SCHEMA_PRIVILEGE" => {
                // Schema privileges: check if schema exists or is a well-known schema
                let schema = match args.last() {
                    Some(Value::Text(s)) => s.clone(),
                    _ => "public".to_string(),
                };
                // Extract just the schema name (first arg if 3 args, else first arg if 2 args)
                let schema_name = if args.len() >= 3 {
                    match &args[1] { Value::Text(s) => s.as_str(), _ => &schema }
                } else if args.len() == 2 {
                    match &args[0] { Value::Text(s) => s.as_str(), _ => &schema }
                } else {
                    &schema
                };
                Ok(Value::Bool(schema_name == "public" || schema_name == "pg_catalog" || schema_name == "information_schema"))
            }
            "PG_ENCODING_TO_CHAR" => {
                // Always return UTF8 regardless of encoding OID
                Ok(Value::Text("UTF8".to_string()))
            }
            "PG_POSTMASTER_START_TIME" => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                let ts = format_timestamp(now.as_secs());
                Ok(Value::Text(ts))
            }
            "QUOTE_IDENT" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => {
                        // Quote if contains special characters or is a keyword
                        let needs_quoting = s.is_empty()
                            || s.chars().any(|c| !c.is_ascii_alphanumeric() && c != '_')
                            || s.chars().next().is_some_and(|c| c.is_ascii_digit())
                            || s != &s.to_lowercase();
                        if needs_quoting {
                            // Escape any internal double quotes
                            let escaped = s.replace('"', "\"\"");
                            Ok(Value::Text(format!("\"{escaped}\"")))
                        } else {
                            Ok(Value::Text(s.clone()))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    other => Ok(Value::Text(format!("\"{}\"", other.to_string().replace('"', "\"\"")))),
                }
            }
            "PG_GET_USERBYID" => {
                // Always return "nucleus" regardless of OID
                Ok(Value::Text("nucleus".to_string()))
            }
            "PG_CATALOG.PG_GET_CONSTRAINTDEF" | "PG_GET_CONSTRAINTDEF" => {
                // Stub: returns NULL
                Ok(Value::Null)
            }
            "PG_CATALOG.PG_GET_INDEXDEF" | "PG_GET_INDEXDEF" => {
                // Stub: returns NULL
                Ok(Value::Null)
            }

            // -- Array functions --
            "ARRAY_LENGTH" => {
                // array_length(array, dimension) — dimension is always 1, ignored
                require_args(fname, &args, 2)?;
                match &args[0] {
                    Value::Array(vals) => Ok(Value::Int32(vals.len() as i32)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("ARRAY_LENGTH requires an array argument".into())),
                }
            }
            "ARRAY_UPPER" => {
                // array_upper(array, dimension) — returns upper bound (= length for dimension 1)
                require_args(fname, &args, 2)?;
                match &args[0] {
                    Value::Array(vals) => {
                        if vals.is_empty() {
                            Ok(Value::Null)
                        } else {
                            Ok(Value::Int32(vals.len() as i32))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("ARRAY_UPPER requires an array argument".into())),
                }
            }
            "ARRAY_LOWER" => {
                // array_lower(array, dimension) — always 1 for non-empty arrays (1-indexed)
                require_args(fname, &args, 2)?;
                match &args[0] {
                    Value::Array(vals) => {
                        if vals.is_empty() {
                            Ok(Value::Null)
                        } else {
                            Ok(Value::Int32(1))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("ARRAY_LOWER requires an array argument".into())),
                }
            }
            "ARRAY_APPEND" => {
                // array_append(array, element) — returns new array with element appended
                require_args(fname, &args, 2)?;
                match &args[0] {
                    Value::Array(vals) => {
                        let mut new_arr = vals.clone();
                        new_arr.push(args[1].clone());
                        Ok(Value::Array(new_arr))
                    }
                    Value::Null => {
                        // NULL array + element = single-element array
                        Ok(Value::Array(vec![args[1].clone()]))
                    }
                    _ => Err(ExecError::Unsupported("ARRAY_APPEND requires an array as first argument".into())),
                }
            }
            "ARRAY_CAT" => {
                // array_cat(array1, array2) — concatenates two arrays
                require_args(fname, &args, 2)?;
                let arr1 = match &args[0] {
                    Value::Array(v) => v.clone(),
                    Value::Null => Vec::new(),
                    _ => return Err(ExecError::Unsupported("ARRAY_CAT requires array arguments".into())),
                };
                let arr2 = match &args[1] {
                    Value::Array(v) => v.clone(),
                    Value::Null => Vec::new(),
                    _ => return Err(ExecError::Unsupported("ARRAY_CAT requires array arguments".into())),
                };
                let mut result = arr1;
                result.extend(arr2);
                Ok(Value::Array(result))
            }
            "UNNEST" => {
                // unnest(array) — set-returning function; for scalar context return first element
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Array(vals) => {
                        Ok(vals.first().cloned().unwrap_or(Value::Null))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("UNNEST requires an array argument".into())),
                }
            }
            "CARDINALITY" => {
                // cardinality(array) — total number of elements (flattened for multi-dim, but we only have 1-dim)
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Array(vals) => Ok(Value::Int32(vals.len() as i32)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("CARDINALITY requires an array argument".into())),
                }
            }

            // -- Vector functions --
            "VECTOR" => {
                // vector('[1.0,2.0,3.0]') or vector(array[1,2,3]) — construct vector from text or array
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => {
                        // Parse "[1.0,2.0,3.0]" format
                        let s = s.trim();
                        if !s.starts_with('[') || !s.ends_with(']') {
                            return Err(ExecError::Unsupported("vector literal must be [...]".into()));
                        }
                        let inner = &s[1..s.len()-1];
                        if inner.is_empty() {
                            return Ok(Value::Vector(Vec::new()));
                        }
                        let floats: Result<Vec<f32>, _> = inner.split(',')
                            .map(|v| v.trim().parse::<f32>())
                            .collect();
                        match floats {
                            Ok(vec) => Ok(Value::Vector(vec)),
                            Err(e) => Err(ExecError::Unsupported(format!("invalid vector literal: {e}")))
                        }
                    }
                    Value::Array(vals) => {
                        // Convert array of numbers to vector
                        let floats: Result<Vec<f32>, _> = vals.iter().map(|v| match v {
                            Value::Int32(n) => Ok(*n as f32),
                            Value::Int64(n) => Ok(*n as f32),
                            Value::Float64(n) => Ok(*n as f32),
                            Value::Null => Err(ExecError::Unsupported("vector elements cannot be null".into())),
                            _ => Err(ExecError::Unsupported("vector elements must be numeric".into()))
                        }).collect();
                        Ok(Value::Vector(floats?))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("vector() requires text or array".into()))
                }
            }
            "VECTOR_DISTANCE" => {
                self.check_subsystem("vector")?;
                // vector_distance(vec1, vec2, 'l2'|'cosine'|'inner') — compute distance between vectors
                if args.len() < 2 || args.len() > 3 {
                    return Err(ExecError::Unsupported("VECTOR_DISTANCE requires 2 or 3 args".into()));
                }
                let vec1 = match &args[0] {
                    Value::Vector(v) => v,
                    Value::Null => return Ok(Value::Null),
                    _ => return Err(ExecError::Unsupported("VECTOR_DISTANCE arg 1 must be vector".into())),
                };
                let vec2 = match &args[1] {
                    Value::Vector(v) => v,
                    Value::Null => return Ok(Value::Null),
                    _ => return Err(ExecError::Unsupported("VECTOR_DISTANCE arg 2 must be vector".into())),
                };
                if vec1.len() != vec2.len() {
                    return Err(ExecError::Unsupported(format!(
                        "vector dimensions must match: {} vs {}", vec1.len(), vec2.len()
                    )));
                }
                let metric = if args.len() == 3 {
                    match &args[2] {
                        Value::Text(s) => match s.to_lowercase().as_str() {
                            "l2" | "euclidean" => vector::DistanceMetric::L2,
                            "cosine" => vector::DistanceMetric::Cosine,
                            "inner" | "ip" | "dot" => vector::DistanceMetric::InnerProduct,
                            _ => return Err(ExecError::Unsupported(format!("unknown distance metric: {s}"))),
                        }
                        Value::Null => return Ok(Value::Null),
                        _ => return Err(ExecError::Unsupported("metric must be text".into())),
                    }
                } else {
                    vector::DistanceMetric::L2 // default to L2
                };
                let v1 = vector::Vector::new(vec1.clone());
                let v2 = vector::Vector::new(vec2.clone());
                let dist = vector::distance(&v1, &v2, metric);
                Ok(Value::Float64(dist as f64))
            }
            "VECTOR_DIMS" => {
                // vector_dims(vec) — get dimensionality of vector
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Vector(v) => Ok(Value::Int32(v.len() as i32)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("VECTOR_DIMS requires vector".into())),
                }
            }
            "NORMALIZE" => {
                // normalize(vec) — normalize vector to unit length
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Vector(v) => {
                        let vec = vector::Vector::new(v.clone());
                        let normalized = vec.normalize();
                        Ok(Value::Vector(normalized.data))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("NORMALIZE requires vector".into())),
                }
            }

            // ================================================================
            // Additional FTS functions
            // ================================================================

            "TS_MATCH" => {
                // ts_match(text_content, query_text) → boolean: does text match query?
                require_args(fname, &args, 2)?;
                match (&args[0], &args[1]) {
                    (Value::Text(content), Value::Text(query)) => {
                        let mut idx = fts::InvertedIndex::new();
                        idx.add_document(0, content);
                        let results = idx.search(query, 1);
                        Ok(Value::Bool(!results.is_empty()))
                    }
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("TS_MATCH requires (text, query_text)".into())),
                }
            }
            "PLAINTO_TSQUERY" => {
                // plainto_tsquery(text) → stemmed query representation
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(text) => {
                        let tokens = fts::tokenize(text);
                        let terms: Vec<String> = tokens.into_iter().map(|t| t.term).collect();
                        Ok(Value::Text(terms.join(" & ")))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("PLAINTO_TSQUERY requires text".into())),
                }
            }
            "TS_HEADLINE" => {
                // ts_headline(text, query) → text with matching terms highlighted
                require_args(fname, &args, 2)?;
                match (&args[0], &args[1]) {
                    (Value::Text(content), Value::Text(query)) => {
                        let query_tokens = fts::tokenize(query);
                        let query_terms: std::collections::HashSet<String> = query_tokens.iter().map(|t| t.term.clone()).collect();
                        let mut result = String::new();
                        for word in content.split_whitespace() {
                            if !result.is_empty() { result.push(' '); }
                            let stemmed = fts::stem(&word.to_lowercase());
                            if query_terms.contains(&stemmed) {
                                result.push_str(&format!("<b>{word}</b>"));
                            } else {
                                result.push_str(word);
                            }
                        }
                        Ok(Value::Text(result))
                    }
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("TS_HEADLINE requires (text, query_text)".into())),
                }
            }

            // ================================================================
            // Additional PostGIS-compatible geospatial functions
            // ================================================================

            "ST_MAKEPOINT" => {
                // st_makepoint(x, y) → 'POINT(x y)' text
                require_args(fname, &args, 2)?;
                let x = value_to_f64(&args[0])?;
                let y = value_to_f64(&args[1])?;
                Ok(Value::Text(format!("POINT({x} {y})")))
            }
            "ST_X" => {
                // st_x(point_text) → x coordinate
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => {
                        let p = parse_point_wkt(s).ok_or_else(|| ExecError::Unsupported("ST_X: invalid point WKT".into()))?;
                        Ok(Value::Float64(p.x))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("ST_X requires text POINT".into())),
                }
            }
            "ST_Y" => {
                // st_y(point_text) → y coordinate
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => {
                        let p = parse_point_wkt(s).ok_or_else(|| ExecError::Unsupported("ST_Y: invalid point WKT".into()))?;
                        Ok(Value::Float64(p.y))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("ST_Y requires text POINT".into())),
                }
            }
            "ST_CONTAINS" => {
                // st_contains(polygon_wkt, point_wkt) → boolean
                require_args(fname, &args, 2)?;
                match (&args[0], &args[1]) {
                    (Value::Text(poly_wkt), Value::Text(pt_wkt)) => {
                        let poly = parse_polygon_wkt(poly_wkt).ok_or_else(|| ExecError::Unsupported("ST_CONTAINS: invalid polygon WKT".into()))?;
                        let pt = parse_point_wkt(pt_wkt).ok_or_else(|| ExecError::Unsupported("ST_CONTAINS: invalid point WKT".into()))?;
                        Ok(Value::Bool(poly.contains(&pt)))
                    }
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("ST_CONTAINS requires (polygon_wkt, point_wkt)".into())),
                }
            }

            // ================================================================
            // Additional time-series functions
            // ================================================================

            "DATE_BIN" => {
                // date_bin(interval_text, timestamp_ms) → truncated timestamp
                require_args(fname, &args, 2)?;
                match &args[0] {
                    Value::Text(bucket_str) => {
                        let bucket = parse_bucket_size(bucket_str).ok_or_else(|| ExecError::Unsupported(format!("DATE_BIN: unknown interval '{bucket_str}'")))?;
                        let ts = value_to_i64(&args[1])? as u64;
                        Ok(Value::Int64(timeseries::time_bucket(ts, bucket) as i64))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("DATE_BIN requires (text, timestamp)".into())),
                }
            }

            // ================================================================
            // Graph utility functions
            // ================================================================

            "GRAPH_SHORTEST_PATH_LENGTH" => {
                // graph_shortest_path_length(edges_json, from_id, to_id) → path length or NULL
                // edges_json: '[{"from":1,"to":2},{"from":2,"to":3}]'
                require_args(fname, &args, 3)?;
                match &args[0] {
                    Value::Text(edges_json) => {
                        let from_id = value_to_i64(&args[1])? as u64;
                        let to_id = value_to_i64(&args[2])? as u64;
                        let mut gs = crate::graph::GraphStore::new();
                        // Parse edges and build graph
                        if let Ok(edges) = serde_json::from_str::<Vec<serde_json::Value>>(edges_json) {
                            // Collect all unique node IDs
                            let mut node_ids = std::collections::HashSet::new();
                            for edge in &edges {
                                if let (Some(f), Some(t)) = (edge.get("from").and_then(|v| v.as_u64()), edge.get("to").and_then(|v| v.as_u64())) {
                                    node_ids.insert(f);
                                    node_ids.insert(t);
                                }
                            }
                            // Create nodes (IDs are assigned sequentially, so we need a mapping)
                            let mut id_map: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
                            for &nid in &node_ids {
                                let internal_id = gs.create_node(vec![], std::collections::BTreeMap::new());
                                id_map.insert(nid, internal_id);
                            }
                            // Create edges
                            for edge in &edges {
                                if let (Some(f), Some(t)) = (edge.get("from").and_then(|v| v.as_u64()), edge.get("to").and_then(|v| v.as_u64()))
                                    && let (Some(&fi), Some(&ti)) = (id_map.get(&f), id_map.get(&t)) {
                                        gs.create_edge(fi, ti, "EDGE".to_string(), std::collections::BTreeMap::new());
                                    }
                            }
                            // Find shortest path
                            let mapped_from = id_map.get(&from_id).copied();
                            let mapped_to = id_map.get(&to_id).copied();
                            if let (Some(mf), Some(mt)) = (mapped_from, mapped_to) {
                                match gs.shortest_path(mf, mt, crate::graph::Direction::Outgoing, None) {
                                    Some(path) => Ok(Value::Int32((path.len() as i32) - 1)),
                                    None => Ok(Value::Null),
                                }
                            } else {
                                Ok(Value::Null)
                            }
                        } else {
                            Err(ExecError::Unsupported("GRAPH_SHORTEST_PATH_LENGTH: invalid edges JSON".into()))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("GRAPH_SHORTEST_PATH_LENGTH requires (edges_json, from_id, to_id)".into())),
                }
            }
            "GRAPH_NODE_DEGREE" => {
                // graph_node_degree(edges_json, node_id) → number of edges connected to node
                require_args(fname, &args, 2)?;
                match &args[0] {
                    Value::Text(edges_json) => {
                        let node_id = value_to_i64(&args[1])? as u64;
                        if let Ok(edges) = serde_json::from_str::<Vec<serde_json::Value>>(edges_json) {
                            let degree: usize = edges.iter().filter(|e| {
                                let f = e.get("from").and_then(|v| v.as_u64());
                                let t = e.get("to").and_then(|v| v.as_u64());
                                f == Some(node_id) || t == Some(node_id)
                            }).count();
                            Ok(Value::Int32(degree as i32))
                        } else {
                            Err(ExecError::Unsupported("GRAPH_NODE_DEGREE: invalid edges JSON".into()))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("GRAPH_NODE_DEGREE requires (edges_json, node_id)".into())),
                }
            }

            "CYPHER" => {
                // CYPHER(query_text) — execute a Cypher query against the persistent graph store.
                self.check_subsystem("graph")?;
                if args.is_empty() || args.len() > 1 {
                    return Err(ExecError::Unsupported("CYPHER requires exactly 1 argument (query string)".into()));
                }
                let cypher_text = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("CYPHER argument must be a text string".into())),
                };
                let parsed = parse_cypher(&cypher_text).map_err(|e| {
                    ExecError::Unsupported(format!("Cypher parse error: {e:?}"))
                })?;
                let result = {
                    let mut gs = self.graph_store.write();
                    execute_cypher(&mut gs, &parsed).map_err(|e| {
                        ExecError::Unsupported(format!("Cypher execution error: {e:?}"))
                    })?
                };
                // Convert CypherResult to a JSON-like text representation.
                // Format: columns as header, rows as JSON arrays.
                let mut lines = Vec::new();
                lines.push(result.columns.join(","));
                for row in &result.rows {
                    let cells: Vec<String> = row.iter().map(|v| match v {
                        GraphPropValue::Null => "null".to_string(),
                        GraphPropValue::Bool(b) => b.to_string(),
                        GraphPropValue::Int(n) => n.to_string(),
                        GraphPropValue::Float(f) => f.to_string(),
                        GraphPropValue::Text(s) => s.clone(),
                    }).collect();
                    lines.push(cells.join(","));
                }
                Ok(Value::Text(lines.join("\n")))
            }

            "ENCRYPTED_LOOKUP" => {
                // encrypted_lookup(index_name, value) — look up row IDs via encrypted index.
                require_args(fname, &args, 2)?;
                let idx_name = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("ENCRYPTED_LOOKUP arg 1 must be index name text".into())),
                };
                let lookup_val = match &args[1] {
                    Value::Text(s) => s.as_bytes().to_vec(),
                    Value::Int32(n) => n.to_string().into_bytes(),
                    Value::Int64(n) => n.to_string().into_bytes(),
                    Value::Null => return Ok(Value::Null),
                    other => format!("{other:?}").into_bytes(),
                };
                match self.encrypted_index_lookup(&idx_name, &lookup_val) {
                    Some(ids) => {
                        // Return as a comma-separated list of row IDs.
                        let id_strs: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
                        Ok(Value::Text(id_strs.join(",")))
                    }
                    None => Err(ExecError::Unsupported(format!("encrypted index '{idx_name}' not found"))),
                }
            }

            // ================================================================
            // KV store functions (Redis-compatible via SQL)
            // ================================================================

            "KV_GET" => {
                // kv_get(key) → value or NULL
                require_args(fname, &args, 1)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    Value::Null => return Ok(Value::Null),
                    other => other.to_string(),
                };
                Ok(self.kv_store.get(&key).unwrap_or(Value::Null))
            }
            "KV_SET" => {
                // kv_set(key, value) or kv_set(key, value, ttl_secs) → 'OK'
                if args.len() < 2 || args.len() > 3 {
                    return Err(ExecError::Unsupported("KV_SET requires 2 or 3 arguments".into()));
                }
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let value = args[1].clone();
                let ttl = if args.len() == 3 {
                    match &args[2] {
                        Value::Null => None,
                        v => Some(val_to_u64(v, "KV_SET ttl")?),
                    }
                } else {
                    None
                };
                self.kv_store.set(&key, value, ttl);
                Ok(Value::Text("OK".into()))
            }
            "KV_DEL" => {
                // kv_del(key) → true/false
                require_args(fname, &args, 1)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    Value::Null => return Ok(Value::Bool(false)),
                    other => other.to_string(),
                };
                Ok(Value::Bool(self.kv_store.del(&key)))
            }
            "KV_EXISTS" => {
                // kv_exists(key) → true/false
                require_args(fname, &args, 1)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    Value::Null => return Ok(Value::Bool(false)),
                    other => other.to_string(),
                };
                Ok(Value::Bool(self.kv_store.exists(&key)))
            }
            "KV_INCR" => {
                // kv_incr(key) or kv_incr(key, amount) → new value
                if args.is_empty() || args.len() > 2 {
                    return Err(ExecError::Unsupported("KV_INCR requires 1 or 2 arguments".into()));
                }
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let amount = if args.len() == 2 {
                    match &args[1] {
                        Value::Int32(n) => *n as i64,
                        Value::Int64(n) => *n,
                        _ => return Err(ExecError::Unsupported("KV_INCR amount must be integer".into())),
                    }
                } else {
                    1
                };
                match self.kv_store.incr_by(&key, amount) {
                    Ok(v) => Ok(Value::Int64(v)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }
            "KV_TTL" => {
                // kv_ttl(key) → remaining seconds (-1 = no TTL, -2 = missing)
                require_args(fname, &args, 1)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                Ok(Value::Int64(self.kv_store.ttl(&key)))
            }
            "KV_EXPIRE" => {
                // kv_expire(key, ttl_secs) → true/false
                require_args(fname, &args, 2)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let ttl = val_to_u64(&args[1], "KV_EXPIRE ttl")?;
                Ok(Value::Bool(self.kv_store.expire(&key, ttl)))
            }
            "KV_SETNX" => {
                // kv_setnx(key, value) → true if set, false if already exists
                require_args(fname, &args, 2)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                Ok(Value::Bool(self.kv_store.setnx(&key, args[1].clone())))
            }
            "KV_DBSIZE" => {
                // kv_dbsize() → count of non-expired keys
                Ok(Value::Int64(self.kv_store.dbsize() as i64))
            }
            "KV_FLUSHDB" => {
                // kv_flushdb() → 'OK'
                self.kv_store.flushdb();
                Ok(Value::Text("OK".into()))
            }

            // ================================================================
            // KV Collection functions: Lists
            // ================================================================

            "KV_LPUSH" => {
                // kv_lpush(key, value) → list length after push
                require_args(fname, &args, 2)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let val = args[1].clone();
                match self.kv_store.lpush(&key, val) {
                    Ok(len) => Ok(Value::Int64(len as i64)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }
            "KV_RPUSH" => {
                // kv_rpush(key, value) → list length after push
                require_args(fname, &args, 2)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let val = args[1].clone();
                match self.kv_store.rpush(&key, val) {
                    Ok(len) => Ok(Value::Int64(len as i64)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }
            "KV_LPOP" => {
                // kv_lpop(key) → popped value or NULL
                require_args(fname, &args, 1)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                match self.kv_store.lpop(&key) {
                    Ok(Some(v)) => Ok(v),
                    Ok(None) => Ok(Value::Null),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }
            "KV_RPOP" => {
                // kv_rpop(key) → popped value or NULL
                require_args(fname, &args, 1)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                match self.kv_store.rpop(&key) {
                    Ok(Some(v)) => Ok(v),
                    Ok(None) => Ok(Value::Null),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }
            "KV_LRANGE" => {
                // kv_lrange(key, start, stop) → comma-separated values
                require_args(fname, &args, 3)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let start = match &args[1] {
                    Value::Int32(n) => *n as i64,
                    Value::Int64(n) => *n,
                    _ => return Err(ExecError::Unsupported("KV_LRANGE start must be integer".into())),
                };
                let stop = match &args[2] {
                    Value::Int32(n) => *n as i64,
                    Value::Int64(n) => *n,
                    _ => return Err(ExecError::Unsupported("KV_LRANGE stop must be integer".into())),
                };
                match self.kv_store.lrange(&key, start, stop) {
                    Ok(vals) => {
                        let s: Vec<String> = vals.iter().map(|v| v.to_string()).collect();
                        Ok(Value::Text(s.join(",")))
                    }
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }
            "KV_LLEN" => {
                // kv_llen(key) → integer length
                require_args(fname, &args, 1)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                match self.kv_store.llen(&key) {
                    Ok(len) => Ok(Value::Int64(len as i64)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }
            "KV_LINDEX" => {
                // kv_lindex(key, index) → value at index or NULL
                require_args(fname, &args, 2)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let index = match &args[1] {
                    Value::Int32(n) => *n as i64,
                    Value::Int64(n) => *n,
                    _ => return Err(ExecError::Unsupported("KV_LINDEX index must be integer".into())),
                };
                match self.kv_store.lindex(&key, index) {
                    Ok(Some(v)) => Ok(v),
                    Ok(None) => Ok(Value::Null),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }

            // ================================================================
            // KV Collection functions: Hashes
            // ================================================================

            "KV_HSET" => {
                // kv_hset(key, field, value) → boolean (true if new field)
                require_args(fname, &args, 3)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let field = match &args[1] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let val = args[2].clone();
                match self.kv_store.hset(&key, &field, val) {
                    Ok(is_new) => Ok(Value::Bool(is_new)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }
            "KV_HGET" => {
                // kv_hget(key, field) → value or NULL
                require_args(fname, &args, 2)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let field = match &args[1] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                match self.kv_store.hget(&key, &field) {
                    Ok(Some(v)) => Ok(v),
                    Ok(None) => Ok(Value::Null),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }
            "KV_HDEL" => {
                // kv_hdel(key, field) → boolean
                require_args(fname, &args, 2)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let field = match &args[1] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                match self.kv_store.hdel(&key, &field) {
                    Ok(deleted) => Ok(Value::Bool(deleted)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }
            "KV_HGETALL" => {
                // kv_hgetall(key) → comma-separated "field=value" pairs
                require_args(fname, &args, 1)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                match self.kv_store.hgetall(&key) {
                    Ok(pairs) => {
                        let s: Vec<String> = pairs.iter()
                            .map(|(f, v)| format!("{}={}", f, v))
                            .collect();
                        Ok(Value::Text(s.join(",")))
                    }
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }
            "KV_HLEN" => {
                // kv_hlen(key) → integer
                require_args(fname, &args, 1)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                match self.kv_store.hlen(&key) {
                    Ok(len) => Ok(Value::Int64(len as i64)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }
            "KV_HEXISTS" => {
                // kv_hexists(key, field) → boolean
                require_args(fname, &args, 2)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let field = match &args[1] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                match self.kv_store.hexists(&key, &field) {
                    Ok(exists) => Ok(Value::Bool(exists)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }

            // ================================================================
            // KV Collection functions: Sets
            // ================================================================

            "KV_SADD" => {
                // kv_sadd(key, member) → boolean (true if new)
                require_args(fname, &args, 2)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let member = match &args[1] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                match self.kv_store.sadd(&key, &member) {
                    Ok(is_new) => Ok(Value::Bool(is_new)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }
            "KV_SREM" => {
                // kv_srem(key, member) → boolean
                require_args(fname, &args, 2)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let member = match &args[1] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                match self.kv_store.srem(&key, &member) {
                    Ok(removed) => Ok(Value::Bool(removed)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }
            "KV_SMEMBERS" => {
                // kv_smembers(key) → comma-separated members
                require_args(fname, &args, 1)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                match self.kv_store.smembers(&key) {
                    Ok(members) => Ok(Value::Text(members.join(","))),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }
            "KV_SISMEMBER" => {
                // kv_sismember(key, member) → boolean
                require_args(fname, &args, 2)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let member = match &args[1] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                match self.kv_store.sismember(&key, &member) {
                    Ok(is_member) => Ok(Value::Bool(is_member)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }
            "KV_SCARD" => {
                // kv_scard(key) → integer count
                require_args(fname, &args, 1)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                match self.kv_store.scard(&key) {
                    Ok(count) => Ok(Value::Int64(count as i64)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }

            // ================================================================
            // KV Collection functions: Sorted Sets
            // ================================================================

            "KV_ZADD" => {
                // kv_zadd(key, score, member) → boolean
                require_args(fname, &args, 3)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let score = match &args[1] {
                    Value::Float64(f) => *f,
                    Value::Int32(n) => *n as f64,
                    Value::Int64(n) => *n as f64,
                    _ => return Err(ExecError::Unsupported("KV_ZADD score must be numeric".into())),
                };
                let member = match &args[2] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                match self.kv_store.col_zadd(&key, &member, score) {
                    Ok(is_new) => Ok(Value::Bool(is_new)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }
            "KV_ZREM" => {
                // kv_zrem(key, member) → boolean
                require_args(fname, &args, 2)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let member = match &args[1] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                match self.kv_store.col_zrem(&key, &member) {
                    Ok(removed) => Ok(Value::Bool(removed)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }
            "KV_ZRANGE" => {
                // kv_zrange(key, start, stop) → comma-separated "member:score" pairs
                require_args(fname, &args, 3)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let start = match &args[1] {
                    Value::Int32(n) => *n as usize,
                    Value::Int64(n) => *n as usize,
                    _ => return Err(ExecError::Unsupported("KV_ZRANGE start must be integer".into())),
                };
                let stop = match &args[2] {
                    Value::Int32(n) => *n as usize,
                    Value::Int64(n) => *n as usize,
                    _ => return Err(ExecError::Unsupported("KV_ZRANGE stop must be integer".into())),
                };
                match self.kv_store.col_zrange(&key, start, stop) {
                    Ok(entries) => {
                        let s: Vec<String> = entries.iter()
                            .map(|e| format!("{}:{}", e.member, e.score))
                            .collect();
                        Ok(Value::Text(s.join(",")))
                    }
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }
            "KV_ZRANGEBYSCORE" => {
                // kv_zrangebyscore(key, min, max) → comma-separated "member:score" pairs
                require_args(fname, &args, 3)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let min_score = match &args[1] {
                    Value::Float64(f) => *f,
                    Value::Int32(n) => *n as f64,
                    Value::Int64(n) => *n as f64,
                    _ => return Err(ExecError::Unsupported("KV_ZRANGEBYSCORE min must be numeric".into())),
                };
                let max_score = match &args[2] {
                    Value::Float64(f) => *f,
                    Value::Int32(n) => *n as f64,
                    Value::Int64(n) => *n as f64,
                    _ => return Err(ExecError::Unsupported("KV_ZRANGEBYSCORE max must be numeric".into())),
                };
                match self.kv_store.col_zrangebyscore(&key, min_score, max_score) {
                    Ok(entries) => {
                        let s: Vec<String> = entries.iter()
                            .map(|e| format!("{}:{}", e.member, e.score))
                            .collect();
                        Ok(Value::Text(s.join(",")))
                    }
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }
            "KV_ZCARD" => {
                // kv_zcard(key) → integer count
                require_args(fname, &args, 1)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                match self.kv_store.col_zcard(&key) {
                    Ok(count) => Ok(Value::Int64(count as i64)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }

            // ================================================================
            // KV Collection functions: HyperLogLog
            // ================================================================

            "KV_PFADD" => {
                // kv_pfadd(key, element) → boolean
                require_args(fname, &args, 2)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let element = match &args[1] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                match self.kv_store.col_pfadd(&key, &element) {
                    Ok(changed) => Ok(Value::Bool(changed)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }
            "KV_PFCOUNT" => {
                // kv_pfcount(key) → integer estimate
                require_args(fname, &args, 1)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                match self.kv_store.col_pfcount(&key) {
                    Ok(count) => Ok(Value::Int64(count as i64)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }

            // ================================================================
            // Stream functions (Redis-style append-only logs)
            // ================================================================

            "STREAM_XADD" => {
                // stream_xadd(stream, field1, value1, ...) → stream entry ID
                if args.len() < 3 || args.len() % 2 == 0 {
                    return Err(ExecError::Unsupported(
                        "STREAM_XADD requires (stream, field1, value1, ...)".into(),
                    ));
                }
                let stream_name = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let mut fields = Vec::new();
                let mut i = 1;
                while i + 1 < args.len() {
                    let field = match &args[i] {
                        Value::Text(s) => s.clone(),
                        other => other.to_string(),
                    };
                    let value = match &args[i + 1] {
                        Value::Text(s) => s.clone(),
                        other => other.to_string(),
                    };
                    fields.push((field, value));
                    i += 2;
                }
                let mut streams = self.streams.write();
                let stream = streams.entry(stream_name.clone()).or_default();
                let id = stream.xadd(fields.clone());
                // Log to WAL after successful append
                if let Some(ref wal) = self.streams_wal {
                    let _ = wal.log_xadd(&stream_name, &id, &fields);
                }
                Ok(Value::Text(id.to_string()))
            }
            "STREAM_XLEN" => {
                // stream_xlen(stream) → integer count
                require_args(fname, &args, 1)?;
                let stream_name = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let streams = self.streams.read();
                let len = streams.get(&stream_name)
                    .map(|s| s.xlen())
                    .unwrap_or(0);
                Ok(Value::Int64(len as i64))
            }
            "STREAM_XRANGE" => {
                // stream_xrange(stream, start_ms, end_ms, count) → entries as text
                require_args(fname, &args, 4)?;
                let stream_name = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let start_ms = val_to_u64(&args[1], "STREAM_XRANGE start")?;
                let end_ms = val_to_u64(&args[2], "STREAM_XRANGE end")?;
                let count = val_to_u64(&args[3], "STREAM_XRANGE count")? as usize;
                let streams = self.streams.read();
                match streams.get(&stream_name) {
                    Some(stream) => {
                        let start_id = crate::pubsub::StreamEntryId::new(start_ms, 0);
                        let end_id = crate::pubsub::StreamEntryId::new(end_ms, u64::MAX);
                        let entries = stream.xrange(&start_id, &end_id, Some(count));
                        let parts: Vec<String> = entries.iter().map(|e| {
                            let fields: Vec<String> = e.fields.iter()
                                .map(|(k, v)| format!("{k}={v}"))
                                .collect();
                            format!("{}:{}", e.id, fields.join(";"))
                        }).collect();
                        Ok(Value::Text(parts.join(",")))
                    }
                    None => Ok(Value::Text(String::new())),
                }
            }
            "STREAM_XREAD" => {
                // stream_xread(stream, last_id_ms, count) → entries as text
                require_args(fname, &args, 3)?;
                let stream_name = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let last_id_ms = val_to_u64(&args[1], "STREAM_XREAD last_id")?;
                let count = val_to_u64(&args[2], "STREAM_XREAD count")? as usize;
                let streams = self.streams.read();
                match streams.get(&stream_name) {
                    Some(stream) => {
                        let last_id = crate::pubsub::StreamEntryId::new(last_id_ms, u64::MAX);
                        let entries = stream.xread(&last_id, count);
                        let parts: Vec<String> = entries.iter().map(|e| {
                            let fields: Vec<String> = e.fields.iter()
                                .map(|(k, v)| format!("{k}={v}"))
                                .collect();
                            format!("{}:{}", e.id, fields.join(";"))
                        }).collect();
                        Ok(Value::Text(parts.join(",")))
                    }
                    None => Ok(Value::Text(String::new())),
                }
            }
            "STREAM_XGROUP_CREATE" => {
                // stream_xgroup_create(stream, group, start_id_ms) → 'OK'
                require_args(fname, &args, 3)?;
                let stream_name = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let group = match &args[1] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let start_ms = val_to_u64(&args[2], "STREAM_XGROUP_CREATE start_id")?;
                let mut streams = self.streams.write();
                let stream = streams.entry(stream_name).or_default();
                stream.xgroup_create(&group, crate::pubsub::StreamEntryId::new(start_ms, 0));
                Ok(Value::Text("OK".into()))
            }
            "STREAM_XREADGROUP" => {
                // stream_xreadgroup(stream, group, consumer, count) → entries as text
                require_args(fname, &args, 4)?;
                let stream_name = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let group = match &args[1] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let consumer = match &args[2] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let count = val_to_u64(&args[3], "STREAM_XREADGROUP count")? as usize;
                let mut streams = self.streams.write();
                match streams.get_mut(&stream_name) {
                    Some(stream) => {
                        let entries = stream.xreadgroup(&group, &consumer, count);
                        let parts: Vec<String> = entries.iter().map(|e| {
                            let fields: Vec<String> = e.fields.iter()
                                .map(|(k, v)| format!("{k}={v}"))
                                .collect();
                            format!("{}:{}", e.id, fields.join(";"))
                        }).collect();
                        Ok(Value::Text(parts.join(",")))
                    }
                    None => Ok(Value::Text(String::new())),
                }
            }
            "STREAM_XACK" => {
                // stream_xack(stream, group, id_ms, id_seq) → integer count acknowledged
                require_args(fname, &args, 4)?;
                let stream_name = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let group = match &args[1] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let id_ms = val_to_u64(&args[2], "STREAM_XACK id_ms")?;
                let id_seq = val_to_u64(&args[3], "STREAM_XACK id_seq")?;
                let mut streams = self.streams.write();
                match streams.get_mut(&stream_name) {
                    Some(stream) => {
                        let acked = stream.xack(&group, &[crate::pubsub::StreamEntryId::new(id_ms, id_seq)]);
                        Ok(Value::Int64(acked as i64))
                    }
                    None => Ok(Value::Int64(0)),
                }
            }

            // ================================================================
            // Pub/Sub functions (publish/subscribe via SQL)
            // ================================================================

            "PUBSUB_PUBLISH" => {
                // pubsub_publish(channel, message) → integer subscriber count
                require_args(fname, &args, 2)?;
                let channel = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let message = match &args[1] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let mut hub = self.pubsub_sync.write();
                let count = hub.publish(&channel, message);
                Ok(Value::Int64(count as i64))
            }
            "PUBSUB_CHANNELS" => {
                // pubsub_channels() → comma-separated channel names
                let hub = self.pubsub_sync.read();
                let mut chans: Vec<String> = hub.channels().iter().map(|s| s.to_string()).collect();
                chans.sort();
                Ok(Value::Text(chans.join(",")))
            }
            "PUBSUB_SUBSCRIBERS" => {
                // pubsub_subscribers(channel) → integer subscriber count
                require_args(fname, &args, 1)?;
                let channel = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let hub = self.pubsub_sync.read();
                Ok(Value::Int64(hub.subscriber_count(&channel) as i64))
            }

            // ================================================================
            // Columnar storage functions (analytics via SQL)
            // ================================================================

            "COLUMNAR_INSERT" => {
                // columnar_insert(table, col1, val1, col2, val2, ...) → 'OK'
                // Inserts a single row into the columnar store as key-value pairs.
                if args.len() < 3 || args.len() % 2 == 0 {
                    return Err(ExecError::Unsupported(
                        "COLUMNAR_INSERT requires (table, col1, val1, col2, val2, ...)".into(),
                    ));
                }
                let table = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let mut columns: Vec<(String, crate::columnar::ColumnData)> = Vec::new();
                let mut i = 1;
                while i + 1 < args.len() {
                    let col_name = match &args[i] {
                        Value::Text(s) => s.clone(),
                        other => other.to_string(),
                    };
                    let col_data = match &args[i + 1] {
                        Value::Int32(n) => crate::columnar::ColumnData::Int32(vec![Some(*n)]),
                        Value::Int64(n) => crate::columnar::ColumnData::Int64(vec![Some(*n)]),
                        Value::Float64(f) => crate::columnar::ColumnData::Float64(vec![Some(*f)]),
                        Value::Bool(b) => crate::columnar::ColumnData::Bool(vec![Some(*b)]),
                        Value::Text(s) => crate::columnar::ColumnData::Text(vec![Some(s.clone())]),
                        Value::Null => crate::columnar::ColumnData::Text(vec![None]),
                        _ => crate::columnar::ColumnData::Text(vec![Some(args[i + 1].to_string())]),
                    };
                    columns.push((col_name, col_data));
                    i += 2;
                }
                let batch = crate::columnar::ColumnBatch::new(columns);
                self.columnar_store.write().append_with_dict(&table, batch);
                Ok(Value::Text("OK".into()))
            }
            "COLUMNAR_COUNT" => {
                // columnar_count(table) → row count
                require_args(fname, &args, 1)?;
                let table = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let count = self.columnar_store.read().row_count(&table);
                Ok(Value::Int64(count as i64))
            }
            "COLUMNAR_SUM" => {
                // columnar_sum(table, column) → sum as Float64
                require_args(fname, &args, 2)?;
                let table = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let col_name = match &args[1] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let store = self.columnar_store.read();
                let mut total = 0.0f64;
                for batch in store.batches_all(&table) {
                    total += crate::columnar::aggregate_sum(&batch, &col_name);
                }
                Ok(Value::Float64(total))
            }
            "COLUMNAR_AVG" => {
                // columnar_avg(table, column) → average as Float64
                require_args(fname, &args, 2)?;
                let table = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let col_name = match &args[1] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let store = self.columnar_store.read();
                let mut total_sum = 0.0f64;
                let mut total_count = 0usize;
                for batch in store.batches_all(&table) {
                    if let Some(col) = batch.column(&col_name) {
                        let cnt = crate::columnar::count_non_null(col);
                        total_sum += crate::columnar::aggregate_sum(&batch, &col_name);
                        total_count += cnt;
                    }
                }
                if total_count == 0 {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Float64(total_sum / total_count as f64))
                }
            }
            "COLUMNAR_MIN" => {
                // columnar_min(table, column) → min value
                require_args(fname, &args, 2)?;
                let table = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let col_name = match &args[1] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let store = self.columnar_store.read();
                let mut result: Option<f64> = None;
                for batch in store.batches_all(&table) {
                    let v = match crate::columnar::aggregate_min(&batch, &col_name) {
                        crate::columnar::AggValue::Float64(v) => Some(v),
                        crate::columnar::AggValue::Int64(v) => Some(v as f64),
                        crate::columnar::AggValue::Int32(v) => Some(v as f64),
                        _ => None,
                    };
                    if let Some(v) = v {
                        result = Some(result.map_or(v, |r: f64| r.min(v)));
                    }
                }
                match result {
                    Some(v) => Ok(Value::Float64(v)),
                    None => Ok(Value::Null),
                }
            }
            "COLUMNAR_MAX" => {
                // columnar_max(table, column) → max value
                require_args(fname, &args, 2)?;
                let table = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let col_name = match &args[1] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let store = self.columnar_store.read();
                let mut result: Option<f64> = None;
                for batch in store.batches_all(&table) {
                    let v = match crate::columnar::aggregate_max(&batch, &col_name) {
                        crate::columnar::AggValue::Float64(v) => Some(v),
                        crate::columnar::AggValue::Int64(v) => Some(v as f64),
                        crate::columnar::AggValue::Int32(v) => Some(v as f64),
                        _ => None,
                    };
                    if let Some(v) = v {
                        result = Some(result.map_or(v, |r: f64| r.max(v)));
                    }
                }
                match result {
                    Some(v) => Ok(Value::Float64(v)),
                    None => Ok(Value::Null),
                }
            }

            // ================================================================
            // Time-series functions
            // ================================================================

            "TS_INSERT" => {
                // ts_insert(series, timestamp_ms, value) → 'OK'
                require_args(fname, &args, 3)?;
                let series = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let ts = val_to_u64(&args[1], "TS_INSERT timestamp")?;
                let val = match &args[2] {
                    Value::Int32(n) => *n as f64,
                    Value::Int64(n) => *n as f64,
                    Value::Float64(f) => *f,
                    _ => return Err(ExecError::Unsupported("TS_INSERT value must be numeric".into())),
                };
                self.ts_store.write().insert(
                    &series,
                    crate::timeseries::DataPoint { timestamp: ts, tags: vec![], value: val },
                );
                Ok(Value::Text("OK".into()))
            }
            "TS_COUNT" => {
                // ts_count(series) → total points
                require_args(fname, &args, 1)?;
                let series = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let store = self.ts_store.read();
                let count = store.query(&series, 0, u64::MAX).len();
                Ok(Value::Int64(count as i64))
            }
            "TS_LAST" => {
                // ts_last(series) → last value as Float64, or NULL
                require_args(fname, &args, 1)?;
                let series = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let store = self.ts_store.read();
                match store.last_value(&series) {
                    Some(dp) => Ok(Value::Float64(dp.value)),
                    None => Ok(Value::Null),
                }
            }
            "TS_RANGE_COUNT" => {
                // ts_range_count(series, start_ms, end_ms) → count of points in range
                require_args(fname, &args, 3)?;
                let series = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let start = val_to_u64(&args[1], "TS_RANGE_COUNT start")?;
                let end = val_to_u64(&args[2], "TS_RANGE_COUNT end")?;
                let store = self.ts_store.read();
                let count = store.query(&series, start, end).len();
                Ok(Value::Int64(count as i64))
            }
            "TS_RANGE_AVG" => {
                // ts_range_avg(series, start_ms, end_ms) → average value in range
                require_args(fname, &args, 3)?;
                let series = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let start = val_to_u64(&args[1], "TS_RANGE_AVG start")?;
                let end = val_to_u64(&args[2], "TS_RANGE_AVG end")?;
                let store = self.ts_store.read();
                let points = store.query(&series, start, end);
                if points.is_empty() {
                    Ok(Value::Null)
                } else {
                    let sum: f64 = points.iter().map(|p| p.value).sum();
                    Ok(Value::Float64(sum / points.len() as f64))
                }
            }
            "TS_RETENTION" => {
                // ts_retention(max_age_ms) → 'OK' — sets global retention policy
                require_args(fname, &args, 1)?;
                let max_age = val_to_u64(&args[0], "TS_RETENTION max_age_ms")?;
                self.ts_store.write().set_retention(
                    crate::timeseries::RetentionPolicy { max_age_ms: max_age },
                );
                Ok(Value::Text("OK".into()))
            }

            // ================================================================
            // Document store functions (JSONB + GIN index via SQL)
            // ================================================================

            "DOC_INSERT" => {
                // doc_insert(json_text) → document ID
                require_args(fname, &args, 1)?;
                let json_text = match &args[0] {
                    Value::Text(s) => s.clone(),
                    Value::Null => return Ok(Value::Null),
                    other => other.to_string(),
                };
                let jv = parse_json_to_doc(&json_text)
                    .map_err(|e| ExecError::Unsupported(format!("DOC_INSERT invalid JSON: {e}")))?;
                let id = self.doc_store.write().insert(jv);
                Ok(Value::Int64(id as i64))
            }
            "DOC_GET" => {
                // doc_get(id) → JSON text or NULL
                require_args(fname, &args, 1)?;
                let id = val_to_u64(&args[0], "DOC_GET id")?;
                let store = self.doc_store.read();
                match store.get(id) {
                    Some(jv) => Ok(Value::Text(jv.to_json_string())),
                    None => Ok(Value::Null),
                }
            }
            "DOC_QUERY" => {
                // doc_query(json_query) → comma-separated IDs of matching docs (@> containment)
                require_args(fname, &args, 1)?;
                let json_text = match &args[0] {
                    Value::Text(s) => s.clone(),
                    Value::Null => return Ok(Value::Null),
                    other => other.to_string(),
                };
                let query = parse_json_to_doc(&json_text)
                    .map_err(|e| ExecError::Unsupported(format!("DOC_QUERY invalid JSON: {e}")))?;
                let store = self.doc_store.read();
                let mut ids = store.query_contains(&query);
                ids.sort();
                let id_strs: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
                Ok(Value::Text(id_strs.join(",")))
            }
            "DOC_PATH" => {
                // doc_path(id, path_key1, path_key2, ...) → JSON value at path, or NULL
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("DOC_PATH requires (id, key1, key2, ...)".into()));
                }
                let id = val_to_u64(&args[0], "DOC_PATH id")?;
                let path: Vec<String> = args[1..].iter().map(|a| match a {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                }).collect();
                let path_refs: Vec<&str> = path.iter().map(|s| s.as_str()).collect();
                let store = self.doc_store.read();
                match store.get(id) {
                    Some(doc) => match doc.get_path(&path_refs) {
                        Some(val) => Ok(Value::Text(val.to_json_string())),
                        None => Ok(Value::Null),
                    },
                    None => Ok(Value::Null),
                }
            }
            "DOC_COUNT" => {
                // doc_count() → total number of documents
                let count = self.doc_store.read().len();
                Ok(Value::Int64(count as i64))
            }

            // ── Full-text search (FTS) functions ─────────────────────
            "FTS_INDEX" => {
                // fts_index(doc_id, text) → true
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("FTS_INDEX requires (doc_id, text)".into()));
                }
                let doc_id = val_to_u64(&args[0], "FTS_INDEX doc_id")?;
                let text = match &args[1] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("FTS_INDEX: text must be a string".into())),
                };
                let text_len = text.len();
                self.fts_index.write().add_document(doc_id, &text);
                self.save_fts_index();
                // Track FTS memory usage (text bytes + estimated posting overhead).
                self.memory_allocator.lock().request("fts", text_len + 64);
                Ok(Value::Bool(true))
            }
            "FTS_REMOVE" => {
                // fts_remove(doc_id) → true
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("FTS_REMOVE requires (doc_id)".into()));
                }
                let doc_id = val_to_u64(&args[0], "FTS_REMOVE doc_id")?;
                self.fts_index.write().remove_document(doc_id);
                self.save_fts_index();
                self.memory_allocator.lock().release("fts", 64);
                Ok(Value::Bool(true))
            }
            "FTS_SEARCH" => {
                // fts_search(query, limit) → JSON array of [{doc_id, score}]
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("FTS_SEARCH requires (query, limit)".into()));
                }
                let query = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("FTS_SEARCH: query must be a string".into())),
                };
                let limit = (val_to_u64(&args[1], "FTS_SEARCH limit")? as usize).min(10_000);
                let results = self.fts_index.read().search(&query, limit);
                let json = results.iter()
                    .map(|(id, score)| format!(r#"{{"doc_id":{id},"score":{score:.6}}}"#))
                    .collect::<Vec<_>>()
                    .join(",");
                Ok(Value::Text(format!("[{json}]")))
            }
            "FTS_FUZZY_SEARCH" => {
                // fts_fuzzy_search(query, max_distance, limit) → JSON array of [{doc_id, score}]
                // Expands query terms via fuzzy matching then scores with BM25
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 3 {
                    return Err(ExecError::Unsupported(
                        "FTS_FUZZY_SEARCH requires (query, max_distance, limit)".into(),
                    ));
                }
                let query = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("FTS_FUZZY_SEARCH: query must be a string".into())),
                };
                let max_dist_raw = val_to_u64(&args[1], "FTS_FUZZY_SEARCH max_distance")? as usize;
                let max_dist = max_dist_raw.min(3); // Cap at 3 to prevent combinatorial explosion
                let limit = (val_to_u64(&args[2], "FTS_FUZZY_SEARCH limit")? as usize).min(10_000);
                let idx = self.fts_index.read();
                // Tokenize query, expand each term via fuzzy matching, collect all matching doc scores
                let query_tokens = fts::tokenize(&query);
                let mut scores: std::collections::HashMap<u64, f64> = std::collections::HashMap::new();
                for token in &query_tokens {
                    // Get fuzzy-expanded terms (includes exact if distance=0)
                    let expanded = fts::fuzzy_terms(&idx, &token.term, max_dist);
                    // Collect unique terms to search (avoids double-counting exact matches)
                    let mut seen_terms: HashSet<String> = HashSet::new();
                    for (expanded_term, _distance) in &expanded {
                        seen_terms.insert(expanded_term.to_string());
                    }
                    // Always include the original stemmed term
                    seen_terms.insert(token.term.clone());
                    for term in &seen_terms {
                        let term_results = idx.search(term, limit);
                        for (doc_id, score) in term_results {
                            *scores.entry(doc_id).or_default() += score;
                        }
                    }
                }
                let mut results: Vec<(u64, f64)> = scores.into_iter().collect();
                results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                results.truncate(limit);
                let json = results.iter()
                    .map(|(id, score)| format!(r#"{{"doc_id":{id},"score":{score:.6}}}"#))
                    .collect::<Vec<_>>()
                    .join(",");
                Ok(Value::Text(format!("[{json}]")))
            }
            "FTS_DOC_COUNT" => {
                // fts_doc_count() → number of indexed documents
                let count = self.fts_index.read().doc_count();
                Ok(Value::Int64(count as i64))
            }
            "FTS_TERM_COUNT" => {
                // fts_term_count() → number of unique terms in the index
                let count = self.fts_index.read().term_count();
                Ok(Value::Int64(count as i64))
            }
            "FTS_MATCH" => {
                // fts_match(doc_id, query) → true if doc_id appears in fts_search results.
                // Enables per-row FTS filtering in WHERE clauses:
                //   SELECT * FROM docs WHERE fts_match(id, 'machine learning')
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("FTS_MATCH requires (doc_id, query)".into()));
                }
                let doc_id = val_to_u64(&args[0], "FTS_MATCH doc_id")?;
                let query = match &args[1] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("FTS_MATCH: query must be a string".into())),
                };
                // Use posting-list membership check (O(terms × P), early exit)
                // rather than full BM25 search (O(N·P)) to check a single doc.
                let matched = self.fts_index.read().contains_doc(doc_id, &query);
                Ok(Value::Bool(matched))
            }

            // ── Blob storage functions ───────────────────────────────
            "BLOB_STORE" => {
                // blob_store(key, data_hex, content_type?) → blob_count
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("BLOB_STORE requires (key, data_hex [, content_type])".into()));
                }
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("BLOB_STORE: key must be a string".into())),
                };
                let data_hex = match &args[1] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("BLOB_STORE: data must be a hex string".into())),
                };
                // Validate size (100 MB max via SQL function; direct API has no limit)
                if data_hex.len() > 200_000_000 {
                    return Err(ExecError::Unsupported("BLOB_STORE: data exceeds 100 MB limit".into()));
                }
                // Decode hex → bytes
                let data = hex_decode(&data_hex).map_err(|e| ExecError::Unsupported(format!("BLOB_STORE: {e}")))?;
                let content_type = if args.len() > 2 {
                    match &args[2] {
                        Value::Text(s) => Some(s.clone()),
                        _ => None,
                    }
                } else {
                    None
                };
                self.blob_store.write().put(&key, &data, content_type.as_deref());
                Ok(Value::Bool(true))
            }
            "BLOB_GET" => {
                // blob_get(key) → hex-encoded data or NULL
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("BLOB_GET requires (key)".into()));
                }
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("BLOB_GET: key must be a string".into())),
                };
                match self.blob_store.read().get(&key) {
                    Some(data) => Ok(Value::Text(hex_encode(&data))),
                    None => Ok(Value::Null),
                }
            }
            "BLOB_DELETE" => {
                // blob_delete(key) → true/false
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("BLOB_DELETE requires (key)".into()));
                }
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("BLOB_DELETE: key must be a string".into())),
                };
                let removed = self.blob_store.write().delete(&key);
                Ok(Value::Bool(removed))
            }
            "BLOB_META" => {
                // blob_meta(key) → JSON metadata or NULL
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("BLOB_META requires (key)".into()));
                }
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("BLOB_META: key must be a string".into())),
                };
                let store = self.blob_store.read();
                match store.metadata(&key) {
                    Some(meta) => {
                        let json = format!(
                            r#"{{"size":{},"content_type":"{}","created_at":{},"updated_at":{}}}"#,
                            meta.size,
                            json_escape(meta.content_type.as_deref().unwrap_or("")),
                            meta.created_at,
                            meta.updated_at,
                        );
                        Ok(Value::Text(json))
                    }
                    None => Ok(Value::Null),
                }
            }
            "BLOB_TAG" => {
                // blob_tag(key, tag_key, tag_value) → true/false
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 3 {
                    return Err(ExecError::Unsupported("BLOB_TAG requires (key, tag_key, tag_value)".into()));
                }
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("BLOB_TAG: key must be a string".into())),
                };
                let tag_key = match &args[1] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("BLOB_TAG: tag_key must be a string".into())),
                };
                let tag_val = match &args[2] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("BLOB_TAG: tag_value must be a string".into())),
                };
                let ok = self.blob_store.write().set_tag(&key, &tag_key, &tag_val);
                Ok(Value::Bool(ok))
            }
            "BLOB_LIST" => {
                // blob_list(prefix?) → JSON array of keys
                let args = self.extract_fn_args(func, row, col_meta)?;
                let prefix = if !args.is_empty() {
                    match &args[0] {
                        Value::Text(s) => s.clone(),
                        _ => String::new(),
                    }
                } else {
                    String::new()
                };
                let store = self.blob_store.read();
                let keys = if prefix.is_empty() {
                    store.list_keys()
                } else {
                    store.list_prefix(&prefix)
                };
                let json = keys.iter()
                    .map(|k| format!(r#""{}""#, json_escape(k)))
                    .collect::<Vec<_>>()
                    .join(",");
                Ok(Value::Text(format!("[{json}]")))
            }
            "BLOB_COUNT" => {
                // blob_count() → number of stored blobs
                Ok(Value::Int64(self.blob_store.read().blob_count() as i64))
            }
            "BLOB_DEDUP_RATIO" => {
                // blob_dedup_ratio() → dedup ratio (logical / physical)
                Ok(Value::Float64(self.blob_store.read().dedup_ratio()))
            }

            // ── Graph store functions ────────────────────────────────
            "GRAPH_QUERY" => {
                // graph_query(cypher_text) → JSON result {columns, rows}
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("GRAPH_QUERY requires (cypher_text)".into()));
                }
                let cypher = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("GRAPH_QUERY: cypher must be a string".into())),
                };
                let stmt = parse_cypher(&cypher)
                    .map_err(|e| ExecError::Unsupported(format!("GRAPH_QUERY parse error: {e:?}")))?;
                let result = execute_cypher(&mut self.graph_store.write(), &stmt)
                    .map_err(|e| ExecError::Unsupported(format!("GRAPH_QUERY exec error: {e:?}")))?;
                // Serialize result to JSON
                let cols_json = result.columns.iter()
                    .map(|c| format!(r#""{}""#, json_escape(c)))
                    .collect::<Vec<_>>()
                    .join(",");
                let rows_json = result.rows.iter()
                    .map(|row_vals| {
                        let vals = row_vals.iter()
                            .map(prop_value_to_json)
                            .collect::<Vec<_>>()
                            .join(",");
                        format!("[{vals}]")
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                Ok(Value::Text(format!(r#"{{"columns":[{cols_json}],"rows":[{rows_json}]}}"#)))
            }
            "GRAPH_ADD_NODE" => {
                // graph_add_node(label, properties_json?) → node_id
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("GRAPH_ADD_NODE requires (label [, properties_json])".into()));
                }
                let label = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("GRAPH_ADD_NODE: label must be a string".into())),
                };
                let props = if args.len() > 1 {
                    match &args[1] {
                        Value::Text(s) => parse_json_to_graph_props(s)?,
                        _ => std::collections::BTreeMap::new(),
                    }
                } else {
                    std::collections::BTreeMap::new()
                };
                let id = self.graph_store.write().create_node(vec![label], props);
                Ok(Value::Int64(id as i64))
            }
            "GRAPH_ADD_EDGE" => {
                // graph_add_edge(from_id, to_id, edge_type, properties_json?) → edge_id or NULL
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 3 {
                    return Err(ExecError::Unsupported(
                        "GRAPH_ADD_EDGE requires (from_id, to_id, edge_type [, properties_json])".into(),
                    ));
                }
                let from = val_to_u64(&args[0], "GRAPH_ADD_EDGE from_id")?;
                let to = val_to_u64(&args[1], "GRAPH_ADD_EDGE to_id")?;
                let edge_type = match &args[2] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("GRAPH_ADD_EDGE: edge_type must be a string".into())),
                };
                let props = if args.len() > 3 {
                    match &args[3] {
                        Value::Text(s) => parse_json_to_graph_props(s)?,
                        _ => std::collections::BTreeMap::new(),
                    }
                } else {
                    std::collections::BTreeMap::new()
                };
                match self.graph_store.write().create_edge(from, to, edge_type, props) {
                    Some(eid) => Ok(Value::Int64(eid as i64)),
                    None => Ok(Value::Null),
                }
            }
            "GRAPH_DELETE_NODE" => {
                // graph_delete_node(node_id) → true/false
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("GRAPH_DELETE_NODE requires (node_id)".into()));
                }
                let id = val_to_u64(&args[0], "GRAPH_DELETE_NODE")?;
                Ok(Value::Bool(self.graph_store.write().delete_node(id)))
            }
            "GRAPH_DELETE_EDGE" => {
                // graph_delete_edge(edge_id) → true/false
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("GRAPH_DELETE_EDGE requires (edge_id)".into()));
                }
                let id = val_to_u64(&args[0], "GRAPH_DELETE_EDGE")?;
                Ok(Value::Bool(self.graph_store.write().delete_edge(id)))
            }
            "GRAPH_NEIGHBORS" => {
                // graph_neighbors(node_id, direction?) → JSON array of {neighbor_id, edge_id, edge_type}
                // direction: 'out' (default), 'in', 'both'
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("GRAPH_NEIGHBORS requires (node_id [, direction])".into()));
                }
                let node_id = val_to_u64(&args[0], "GRAPH_NEIGHBORS node_id")?;
                let dir = if args.len() > 1 {
                    match &args[1] {
                        Value::Text(s) => match s.to_lowercase().as_str() {
                            "in" | "incoming" => crate::graph::Direction::Incoming,
                            "both" => crate::graph::Direction::Both,
                            _ => crate::graph::Direction::Outgoing,
                        },
                        _ => crate::graph::Direction::Outgoing,
                    }
                } else {
                    crate::graph::Direction::Outgoing
                };
                let store = self.graph_store.read();
                let neighbors = store.neighbors(node_id, dir, None);
                let json = neighbors.iter()
                    .map(|(nid, edge)| {
                        format!(r#"{{"neighbor_id":{},"edge_id":{},"edge_type":"{}"}}"#, nid, edge.id, json_escape(&edge.edge_type))
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                Ok(Value::Text(format!("[{json}]")))
            }
            "GRAPH_SHORTEST_PATH" => {
                // graph_shortest_path(from_id, to_id) → JSON array of node IDs or NULL
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("GRAPH_SHORTEST_PATH requires (from_id, to_id)".into()));
                }
                let from = val_to_u64(&args[0], "GRAPH_SHORTEST_PATH from_id")?;
                let to = val_to_u64(&args[1], "GRAPH_SHORTEST_PATH to_id")?;
                let store = self.graph_store.read();
                match store.shortest_path(from, to, crate::graph::Direction::Outgoing, None) {
                    Some(path) => {
                        let json = path.iter()
                            .map(|id| id.to_string())
                            .collect::<Vec<_>>()
                            .join(",");
                        Ok(Value::Text(format!("[{json}]")))
                    }
                    None => Ok(Value::Null),
                }
            }
            "GRAPH_NODE_COUNT" => {
                Ok(Value::Int64(self.graph_store.read().node_count() as i64))
            }
            "GRAPH_EDGE_COUNT" => {
                Ok(Value::Int64(self.graph_store.read().edge_count() as i64))
            }

            // ── Reactive / CDC functions ─────────────────────────────
            #[cfg(feature = "server")]
            "SUBSCRIBE" => {
                // subscribe(query_text, table1 [, table2, ...]) → subscription_id
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("SUBSCRIBE requires (query_text, table1, ...)".into()));
                }
                let query_text = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("SUBSCRIBE: query_text must be a string".into())),
                };
                let tables: Vec<String> = args[1..].iter().filter_map(|v| {
                    match v { Value::Text(s) => Some(s.clone()), _ => None }
                }).collect();
                let (sub_id, _rx) = self.subscription_manager.write().subscribe(&query_text, tables);
                Ok(Value::Int64(sub_id as i64))
            }
            #[cfg(feature = "server")]
            "UNSUBSCRIBE" => {
                // unsubscribe(subscription_id) → true/false
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("UNSUBSCRIBE requires (subscription_id)".into()));
                }
                let id = val_to_u64(&args[0], "UNSUBSCRIBE")?;
                Ok(Value::Bool(self.subscription_manager.write().unsubscribe(id)))
            }
            #[cfg(feature = "server")]
            "SUBSCRIPTION_COUNT" => {
                Ok(Value::Int64(self.subscription_manager.read().active_count() as i64))
            }
            #[cfg(feature = "server")]
            "CDC_READ" => {
                // cdc_read(after_sequence, limit) → JSON array of log entries
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("CDC_READ requires (after_sequence, limit)".into()));
                }
                let after_seq = val_to_u64(&args[0], "CDC_READ after_sequence")?;
                let limit = (val_to_u64(&args[1], "CDC_READ limit")? as usize).min(100_000);
                let log = self.cdc_log.read();
                let entries = log.read_from(after_seq, limit);
                let json = entries.iter()
                    .map(|e| {
                        let change = match e.change_type {
                            ChangeType::Insert => "INSERT",
                            ChangeType::Update => "UPDATE",
                            ChangeType::Delete => "DELETE",
                        };
                        format!(
                            r#"{{"seq":{},"table":"{}","change":"{}","ts":{}}}"#,
                            e.sequence, json_escape(&e.table), change, e.timestamp
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                Ok(Value::Text(format!("[{json}]")))
            }
            #[cfg(feature = "server")]
            "CDC_TABLE_READ" => {
                // cdc_table_read(table, after_sequence, limit) → JSON array of log entries for a table
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 3 {
                    return Err(ExecError::Unsupported("CDC_TABLE_READ requires (table, after_sequence, limit)".into()));
                }
                let table = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("CDC_TABLE_READ: table must be a string".into())),
                };
                let after_seq = val_to_u64(&args[1], "CDC_TABLE_READ after_sequence")?;
                let limit = (val_to_u64(&args[2], "CDC_TABLE_READ limit")? as usize).min(100_000);
                let log = self.cdc_log.read();
                let entries = log.read_table_from(&table, after_seq, limit);
                let json = entries.iter()
                    .map(|e| {
                        let change = match e.change_type {
                            ChangeType::Insert => "INSERT",
                            ChangeType::Update => "UPDATE",
                            ChangeType::Delete => "DELETE",
                        };
                        format!(
                            r#"{{"seq":{},"table":"{}","change":"{}","ts":{}}}"#,
                            e.sequence, json_escape(&e.table), change, e.timestamp
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                Ok(Value::Text(format!("[{json}]")))
            }
            #[cfg(feature = "server")]
            "CDC_COUNT" => {
                Ok(Value::Int64(self.cdc_log.read().len() as i64))
            }

            // ── Datalog functions ──────────────────────────────────────
            "DATALOG_ASSERT" => {
                let args = self.extract_fn_args(func, row, col_meta)?;
                require_args(fname, &args, 1)?;
                let input = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                match self.datalog_store.write().sql_assert(&input) {
                    Ok(msg) => Ok(Value::Text(msg)),
                    Err(e) => Err(ExecError::Unsupported(e)),
                }
            }
            "DATALOG_RULE" => {
                let args = self.extract_fn_args(func, row, col_meta)?;
                require_args(fname, &args, 1)?;
                let input = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                match self.datalog_store.write().sql_rule(&input) {
                    Ok(msg) => Ok(Value::Text(msg)),
                    Err(e) => Err(ExecError::Unsupported(e)),
                }
            }
            "DATALOG_QUERY" => {
                let args = self.extract_fn_args(func, row, col_meta)?;
                require_args(fname, &args, 1)?;
                let input = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                match self.datalog_store.write().sql_query(&input) {
                    Ok(json) => Ok(Value::Text(json)),
                    Err(e) => Err(ExecError::Unsupported(e)),
                }
            }
            "DATALOG_RETRACT" => {
                let args = self.extract_fn_args(func, row, col_meta)?;
                require_args(fname, &args, 1)?;
                let input = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                match self.datalog_store.write().sql_retract(&input) {
                    Ok(msg) => Ok(Value::Text(msg)),
                    Err(e) => Err(ExecError::Unsupported(e)),
                }
            }
            "DATALOG_CLEAR" => {
                let args = self.extract_fn_args(func, row, col_meta)?;
                require_args(fname, &args, 1)?;
                let pred = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                match self.datalog_store.write().sql_clear(&pred) {
                    Ok(msg) => Ok(Value::Text(msg)),
                    Err(e) => Err(ExecError::Unsupported(e)),
                }
            }

            // ── Cross-model Datalog imports ──────────────────────────────
            "DATALOG_IMPORT" => {
                // DATALOG_IMPORT(table_name, predicate)
                // Scans a relational table and imports all rows as datalog facts.
                let args = self.extract_fn_args(func, row, col_meta)?;
                require_args(fname, &args, 2)?;
                let table_name = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let predicate = match &args[1] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let rows = sync_block_on(self.storage_for(&table_name).scan(&table_name))?;
                let string_rows: Vec<Vec<String>> = rows
                    .into_iter()
                    .map(|row| row.into_iter().map(|v| match v {
                        Value::Null => "null".to_string(),
                        Value::Text(s) => s,
                        other => other.to_string(),
                    }).collect())
                    .collect();
                let count = string_rows.len();
                self.datalog_store.write().import_rows(&predicate, string_rows);
                Ok(Value::Text(format!("IMPORTED {count} rows into {predicate}")))
            }
            "DATALOG_IMPORT_GRAPH" => {
                // DATALOG_IMPORT_GRAPH(predicate)
                // Imports all graph edges as facts: predicate(from_id, edge_type, to_id)
                let args = self.extract_fn_args(func, row, col_meta)?;
                require_args(fname, &args, 1)?;
                let predicate = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let gs = self.graph_store.read();
                let edge_rows: Vec<Vec<String>> = gs.all_edges()
                    .iter()
                    .map(|e| vec![
                        e.from.to_string(),
                        e.edge_type.clone(),
                        e.to.to_string(),
                    ])
                    .collect();
                drop(gs);
                let count = edge_rows.len();
                self.datalog_store.write().import_rows(&predicate, edge_rows);
                Ok(Value::Text(format!("IMPORTED {count} edges into {predicate}")))
            }
            "DATALOG_IMPORT_NODES" => {
                // DATALOG_IMPORT_NODES(predicate)
                // Imports all graph nodes as facts: predicate(node_id, label)
                let args = self.extract_fn_args(func, row, col_meta)?;
                require_args(fname, &args, 1)?;
                let predicate = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let gs = self.graph_store.read();
                let node_rows: Vec<Vec<String>> = gs.all_nodes()
                    .iter()
                    .flat_map(|n| {
                        if n.labels.is_empty() {
                            vec![vec![n.id.to_string(), String::new()]]
                        } else {
                            n.labels.iter().map(|l| vec![n.id.to_string(), l.clone()]).collect()
                        }
                    })
                    .collect();
                drop(gs);
                let count = node_rows.len();
                self.datalog_store.write().import_rows(&predicate, node_rows);
                Ok(Value::Text(format!("IMPORTED {count} node-label pairs into {predicate}")))
            }

            // ================================================================
            // ML / Embedding pipeline functions
            // ================================================================
            "EMBED" => {
                // embed(model_name, text) → FLOAT8[] (vector)
                // If an ONNX model is registered with the given name, it runs
                // real transformer inference. Otherwise falls back to the
                // built-in bag-of-words EmbeddingGenerator.
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("embed() requires 2 arguments: embed(model, text)".into()));
                }
                let _model_name = args[0].to_string().replace('\'', "");
                let text = match &args[1] {
                    Value::Text(s) => s.clone(),
                    Value::Null => return Ok(Value::Null),
                    other => other.to_string(),
                };

                // Check if an ONNX (or other registered) model exists for this name.
                #[cfg(feature = "onnx")]
                {
                    let registry = self.model_registry.read();
                    if registry.is_onnx_model(&_model_name) {
                        // Tokenize text into a simple float vector for the model.
                        // Real transformer models would need a proper tokenizer;
                        // for now we pass UTF-8 byte values normalized to [0,1].
                        let input: Vec<f32> = text.bytes().map(|b| b as f32 / 255.0).collect();
                        match registry.predict(&_model_name, &input) {
                            Ok(output) => {
                                let vec_str = format!("[{}]", output.iter().map(|v| format!("{v:.6}")).collect::<Vec<_>>().join(","));
                                return Ok(Value::Text(vec_str));
                            }
                            Err(e) => return Err(ExecError::Unsupported(format!("embed ONNX error: {e}"))),
                        }
                    }
                }

                // Fallback: built-in bag-of-words EmbeddingGenerator
                let mut emb_gen = crate::inference::EmbeddingGenerator::new();
                emb_gen.build_vocabulary(&[&text]);
                let vec = emb_gen.embed(&text);
                let vec_str = format!("[{}]", vec.iter().map(|v| format!("{v:.6}")).collect::<Vec<_>>().join(","));
                Ok(Value::Text(vec_str))
            }
            "CLASSIFY" => {
                // classify(model_name, input_values...) → TEXT (class label)
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("classify() requires at least 2 arguments: classify(model, input...)".into()));
                }
                let model_name = args[0].to_string().replace('\'', "");
                let input: Vec<f32> = args[1..].iter().filter_map(|v| match v {
                    Value::Float64(f) => Some(*f as f32),
                    Value::Int32(i) => Some(*i as f32),
                    Value::Int64(i) => Some(*i as f32),
                    _ => v.to_string().parse::<f32>().ok(),
                }).collect();
                let registry = self.model_registry.read();
                match registry.predict(&model_name, &input) {
                    Ok(probs) => {
                        // Return the index of the highest probability as the class
                        let class_idx = probs.iter()
                            .enumerate()
                            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                            .map(|(i, _)| i)
                            .unwrap_or(0);
                        Ok(Value::Text(format!("class_{class_idx}")))
                    }
                    Err(e) => Err(ExecError::Unsupported(format!("classify error: {e}"))),
                }
            }
            "PREDICT" => {
                // predict(model_name, input_values...) → FLOAT8[] (output vector)
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("predict() requires at least 2 arguments: predict(model, input...)".into()));
                }
                let model_name = args[0].to_string().replace('\'', "");
                let input: Vec<f32> = args[1..].iter().filter_map(|v| match v {
                    Value::Float64(f) => Some(*f as f32),
                    Value::Int32(i) => Some(*i as f32),
                    Value::Int64(i) => Some(*i as f32),
                    _ => v.to_string().parse::<f32>().ok(),
                }).collect();
                let registry = self.model_registry.read();
                match registry.predict(&model_name, &input) {
                    Ok(output) => {
                        let vec_str = format!("[{}]", output.iter().map(|v| format!("{v:.6}")).collect::<Vec<_>>().join(","));
                        Ok(Value::Text(vec_str))
                    }
                    Err(e) => Err(ExecError::Unsupported(format!("predict error: {e}"))),
                }
            }

            // ================================================================
            // Tensor functions — tensor_* SQL API
            // ================================================================

            "TENSOR_STORE" => {
                // tensor_store(name, version, shape_json[, dtype[, hex_data]]) → 'OK'
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 3 {
                    return Err(ExecError::Unsupported("tensor_store requires (name, version, shape_json[, dtype[, hex_data]])".into()));
                }
                let name = args[0].to_string().replace('\'', "");
                let version = args[1].to_string().replace('\'', "");
                let shape_json = args[2].to_string().replace('\'', "");
                let shape: Vec<usize> = serde_json::from_str::<Vec<usize>>(&shape_json)
                    .map_err(|e| ExecError::Unsupported(format!("tensor_store: invalid shape JSON: {e}")))?;
                let dtype_str = args.get(3).map(|v| v.to_string().replace('\'', "").to_lowercase())
                    .unwrap_or_else(|| "float32".to_string());
                let dtype = match dtype_str.as_str() {
                    "float16"  => crate::tensor::DType::Float16,
                    "float64"  => crate::tensor::DType::Float64,
                    "int8"     => crate::tensor::DType::Int8,
                    "int16"    => crate::tensor::DType::Int16,
                    "int32"    => crate::tensor::DType::Int32,
                    "int64"    => crate::tensor::DType::Int64,
                    "bfloat16" => crate::tensor::DType::BFloat16,
                    "bool"     => crate::tensor::DType::Bool,
                    _          => crate::tensor::DType::Float32,
                };
                let data = if let Some(hex_val) = args.get(4) {
                    let hex_str = hex_val.to_string().replace('\'', "");
                    (0..hex_str.len()).step_by(2)
                        .filter_map(|i| u8::from_str_radix(&hex_str[i..i+2], 16).ok())
                        .collect::<Vec<u8>>()
                } else {
                    let num_elements: usize = shape.iter().product();
                    vec![0u8; num_elements * dtype.element_size()]
                };
                let tensor = crate::tensor::Tensor::new(shape, dtype, data)
                    .map_err(|e| ExecError::Unsupported(format!("tensor_store: {e:?}")))?;
                self.tensor_store.write()
                    .put(&name, &version, tensor, std::collections::HashMap::new())
                    .map_err(|e| ExecError::Unsupported(format!("tensor_store: {e:?}")))?;
                Ok(Value::Text("OK".into()))
            }
            "TENSOR_SHAPE" => {
                // tensor_shape(name[, version]) → JSON shape e.g. '[3,4]'
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("tensor_shape requires (name)".into()));
                }
                let name = args[0].to_string().replace('\'', "");
                let store = self.tensor_store.read();
                let tensor = if let Some(ver) = args.get(1) {
                    let v = ver.to_string().replace('\'', "");
                    store.get(&name, &v).map_err(|e| ExecError::Unsupported(format!("tensor_shape: {e:?}")))?
                } else {
                    store.get_latest(&name).map_err(|e| ExecError::Unsupported(format!("tensor_shape: {e:?}")))?
                };
                let shape_json = format!("[{}]", tensor.shape.iter().map(|d| d.to_string()).collect::<Vec<_>>().join(","));
                Ok(Value::Text(shape_json))
            }
            "TENSOR_VERSIONS" => {
                // tensor_versions(name) → Int64 count of stored versions
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("tensor_versions requires (name)".into()));
                }
                let name = args[0].to_string().replace('\'', "");
                let store = self.tensor_store.read();
                let versions = store.list_versions(&name);
                Ok(Value::Int64(versions.len() as i64))
            }
            "TENSOR_LIST_VERSIONS" => {
                // tensor_list_versions(name) → TEXT JSON array of version strings
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("tensor_list_versions requires (name)".into()));
                }
                let name = args[0].to_string().replace('\'', "");
                let store = self.tensor_store.read();
                let versions = store.list_versions(&name);
                let json = format!("[{}]", versions.iter().map(|v| format!("\"{v}\"")).collect::<Vec<_>>().join(","));
                Ok(Value::Text(json))
            }
            "TENSOR_COUNT" => {
                // tensor_count() → Int64 total named tensors
                let store = self.tensor_store.read();
                Ok(Value::Int64(store.tensor_count() as i64))
            }
            "TENSOR_SIZE_BYTES" => {
                // tensor_size_bytes(name[, version]) → Int64 raw byte count
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("tensor_size_bytes requires (name)".into()));
                }
                let name = args[0].to_string().replace('\'', "");
                let store = self.tensor_store.read();
                let tensor = if let Some(ver) = args.get(1) {
                    let v = ver.to_string().replace('\'', "");
                    store.get(&name, &v).map_err(|e| ExecError::Unsupported(format!("tensor_size_bytes: {e:?}")))?
                } else {
                    store.get_latest(&name).map_err(|e| ExecError::Unsupported(format!("tensor_size_bytes: {e:?}")))?
                };
                Ok(Value::Int64(tensor.size_bytes() as i64))
            }

            // ================================================================
            // Compliance functions — pii_*, retention_*, gdpr_*
            // ================================================================

            "PII_DETECT" => {
                // pii_detect(column_name, sample1[, sample2, ...]) → TEXT JSON matches
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("pii_detect requires (column_name, sample...)".into()));
                }
                let col_name = args[0].to_string().replace('\'', "");
                let samples: Vec<String> = args[1..].iter().map(|v| v.to_string().replace('\'', "")).collect();
                let sample_refs: Vec<&str> = samples.iter().map(|s| s.as_str()).collect();
                let detector = crate::compliance::PiiDetector::new();
                let matches = detector.detect(&col_name, &sample_refs);
                let json = format!("[{}]", matches.iter().map(|m| {
                    format!("{{\"column\":\"{}\",\"category\":\"{:?}\",\"confidence\":{:.2}}}",
                        m.column_name, m.category, m.confidence)
                }).collect::<Vec<_>>().join(","));
                Ok(Value::Text(json))
            }
            "PII_DETECT_CATEGORY" => {
                // pii_detect_category(column_name, sample) → TEXT category or 'NONE'
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("pii_detect_category requires (column_name, sample)".into()));
                }
                let col_name = args[0].to_string().replace('\'', "");
                let sample = args[1].to_string().replace('\'', "");
                let detector = crate::compliance::PiiDetector::new();
                let matches = detector.detect(&col_name, &[sample.as_str()]);
                let category = matches.first()
                    .map(|m| format!("{:?}", m.category))
                    .unwrap_or_else(|| "NONE".to_string());
                Ok(Value::Text(category))
            }
            "RETENTION_SET" => {
                // retention_set(table, days, ts_column) → 'OK'
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 3 {
                    return Err(ExecError::Unsupported("retention_set requires (table, days, ts_col)".into()));
                }
                let table_name = args[0].to_string().replace('\'', "");
                let days = match &args[1] {
                    Value::Int32(n) => *n as u32,
                    Value::Int64(n) => *n as u32,
                    other => other.to_string().parse::<u32>().unwrap_or(30),
                };
                let ts_col = args[2].to_string().replace('\'', "");
                let now_ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                self.retention_engine.write().register(crate::compliance::RetentionPolicy {
                    table_name,
                    retention_days: days,
                    timestamp_column: ts_col,
                    created_at: now_ts,
                });
                Ok(Value::Text("OK".into()))
            }
            "RETENTION_CHECK" => {
                // retention_check() → TEXT JSON list of expired-data actions
                let now_ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let actions = self.retention_engine.read().find_all_expired(now_ts, |_| 0);
                let json = format!("[{}]", actions.iter().map(|a| {
                    format!("{{\"table\":\"{}\",\"condition\":\"{}\",\"estimated_rows\":{}}}",
                        a.table, a.condition.replace('"', "\\\""), a.estimated_rows)
                }).collect::<Vec<_>>().join(","));
                Ok(Value::Text(json))
            }
            "GDPR_DELETE_PLAN" => {
                // gdpr_delete_plan(table, id_col, id_val) → TEXT JSON deletion plan
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 3 {
                    return Err(ExecError::Unsupported("gdpr_delete_plan requires (table, id_col, id_val)".into()));
                }
                let table = args[0].to_string().replace('\'', "");
                let id_col = args[1].to_string().replace('\'', "");
                let id_val = args[2].to_string().replace('\'', "");
                let cascade = crate::compliance::DeletionCascade::new();
                let plan = cascade.plan_deletion(&table, &id_col, &id_val);
                let json = format!("[{}]", plan.steps.iter().map(|s| {
                    format!("{{\"table\":\"{}\",\"condition\":\"{}\"}}",
                        s.table, s.condition.replace('"', "\\\""))
                }).collect::<Vec<_>>().join(","));
                Ok(Value::Text(json))
            }

            // ================================================================
            // Row-level versioning functions — version_* SQL API
            // ================================================================

            "VERSION_BRANCH" => {
                // version_branch(new_name, from_branch) → 'OK'
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("version_branch requires (new_name, from_branch)".into()));
                }
                let new_name = args[0].to_string().replace('\'', "");
                let from = args[1].to_string().replace('\'', "");
                self.version_store.write().create_branch(&new_name, &from)
                    .map_err(|e| ExecError::Unsupported(format!("version_branch: {e:?}")))?;
                Ok(Value::Text("OK".into()))
            }
            "VERSION_COMMIT" => {
                // version_commit(branch, message) → Int64 commit ID
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("version_commit requires (branch, message)".into()));
                }
                let branch = args[0].to_string().replace('\'', "");
                let msg = args[1].to_string().replace('\'', "");
                let commit_id = self.version_store.write()
                    .commit(&branch, &msg, std::collections::HashMap::new())
                    .map_err(|e| ExecError::Unsupported(format!("version_commit: {e:?}")))?;
                Ok(Value::Int64(commit_id as i64))
            }
            "VERSION_LOG" => {
                // version_log(branch) → TEXT JSON array of commits
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("version_log requires (branch)".into()));
                }
                let branch = args[0].to_string().replace('\'', "");
                let store = self.version_store.read();
                let commits = store.log(&branch)
                    .map_err(|e| ExecError::Unsupported(format!("version_log: {e:?}")))?;
                let json = format!("[{}]", commits.iter().map(|c| {
                    format!("{{\"id\":{},\"message\":\"{}\",\"branch\":\"{}\",\"ts\":{}}}",
                        c.id, c.message.replace('"', "\\\""), c.branch, c.timestamp)
                }).collect::<Vec<_>>().join(","));
                Ok(Value::Text(json))
            }
            "VERSION_BRANCHES" => {
                // version_branches() → TEXT JSON array of branch names
                let store = self.version_store.read();
                let branches = store.list_branches();
                let json = format!("[{}]", branches.iter().map(|b| format!("\"{b}\"")).collect::<Vec<_>>().join(","));
                Ok(Value::Text(json))
            }

            // ================================================================
            // Database branching functions — db_branch_* SQL API
            // ================================================================

            "DB_BRANCH_CREATE" => {
                // db_branch_create(name[, parent_name]) → Int64 branch ID
                // parent_name defaults to 'main' if not provided
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("db_branch_create requires (name[, parent_name])".into()));
                }
                let name = args[0].to_string().replace('\'', "");
                let parent = args.get(1)
                    .map(|v| v.to_string().replace('\'', ""))
                    .unwrap_or_else(|| "main".to_string());
                let branch_id = self.branch_manager.write()
                    .create_branch(&name, &parent)
                    .map_err(|e| ExecError::Unsupported(format!("db_branch_create: {e:?}")))?;
                Ok(Value::Int64(branch_id as i64))
            }
            "DB_BRANCH_LIST" => {
                // db_branch_list() → TEXT JSON array of branch names
                let mgr = self.branch_manager.read();
                let branches = mgr.list_branches();
                let json = format!("[{}]", branches.iter().map(|b| format!("\"{}\"", b.name)).collect::<Vec<_>>().join(","));
                Ok(Value::Text(json))
            }
            "DB_BRANCH_DELETE" => {
                // db_branch_delete(name) → Bool
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("db_branch_delete requires (name)".into()));
                }
                let name = args[0].to_string().replace('\'', "");
                let ok = self.branch_manager.write().delete_branch(&name).is_ok();
                Ok(Value::Bool(ok))
            }
            "DB_BRANCH_MERGE" => {
                // db_branch_merge(source, target) → 'OK'
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("db_branch_merge requires (source, target)".into()));
                }
                let source = args[0].to_string().replace('\'', "");
                let target = args[1].to_string().replace('\'', "");
                self.branch_manager.write().merge(&source, &target)
                    .map_err(|e| ExecError::Unsupported(format!("db_branch_merge: {e:?}")))?;
                Ok(Value::Text("OK".into()))
            }
            "DB_BRANCH_DIFF" => {
                // db_branch_diff(branch_a, branch_b) → TEXT JSON diff summary
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("db_branch_diff requires (branch_a, branch_b)".into()));
                }
                let a = args[0].to_string().replace('\'', "");
                let b_arg = args[1].to_string().replace('\'', "");
                let diff = self.branch_manager.read().diff(&a, &b_arg)
                    .map_err(|e| ExecError::Unsupported(format!("db_branch_diff: {e:?}")))?;
                let json = format!("{{\"added\":{},\"modified\":{},\"deleted\":{}}}",
                    diff.added_pages.len(), diff.modified_pages.len(), diff.deleted_pages.len());
                Ok(Value::Text(json))
            }

            // ================================================================
            // Procedure scalar functions — proc_* SQL API
            // ================================================================

            "PROC_REGISTER" => {
                // proc_register(name, body) or proc_register(name, params_csv, body) → 'OK'
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("proc_register requires (name, body) or (name, params_csv, body)".into()));
                }
                let name = args[0].to_string().replace('\'', "").to_lowercase();
                let (param_names, body) = if args.len() >= 3 {
                    let params: Vec<String> = args[1].to_string().replace('\'', "")
                        .split(',')
                        .map(|p| p.trim().to_string())
                        .filter(|p| !p.is_empty())
                        .collect();
                    (params, args[2].to_string().replace('\'', ""))
                } else {
                    (Vec::new(), args[1].to_string().replace('\'', ""))
                };
                self.procedure_engine.write().register_sql(&name, "registered via SQL", param_names, &body);
                Ok(Value::Text("OK".into()))
            }
            "PROC_DROP" => {
                // proc_drop(name) → Bool (true if existed)
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("proc_drop requires (name)".into()));
                }
                let name = args[0].to_string().replace('\'', "").to_lowercase();
                let removed = self.procedure_engine.write().unregister(&name);
                Ok(Value::Bool(removed))
            }
            "PROC_LIST" => {
                // proc_list() → TEXT JSON array of procedure names
                let eng = self.procedure_engine.read();
                let procs = eng.list_procedures();
                let json = format!("[{}]", procs.iter().map(|m| format!("\"{}\"", m.name)).collect::<Vec<_>>().join(","));
                Ok(Value::Text(json))
            }

            _ => {
                // Try user-defined functions
                let udf_name = fname.to_lowercase();
                let func_def = {
                    let functions = self.functions.read();
                    functions.get(&udf_name).cloned()
                };
                if let Some(func_def) = func_def {
                    let args = self.extract_fn_args(func, row, col_meta)?;
                    let mut positional = Vec::with_capacity(func_def.params.len());
                    let mut named = HashMap::new();
                    // Substitute parameters ($1, $2, ... or named parameters).
                    for (i, (param_name, _)) in func_def.params.iter().enumerate() {
                        if let Some(val) = args.get(i) {
                            let replacement = sql_replacement_for_value(val);
                            positional.push(replacement.clone());
                            if !param_name.is_empty() {
                                named.insert(param_name.clone(), replacement);
                            }
                        } else {
                            positional.push("NULL".to_string());
                        }
                    }
                    let body = substitute_sql_placeholders(&func_def.body, &positional, &named);
                    // Execute the function body as SQL and return the result
                    let result = sync_block_on(self.execute(&body))?;
                    match result.first() {
                        Some(ExecResult::Select { rows, .. }) => {
                            if let Some(first_row) = rows.first() {
                                Ok(first_row.first().cloned().unwrap_or(Value::Null))
                            } else {
                                Ok(Value::Null)
                            }
                        }
                        _ => Ok(Value::Null),
                    }
                } else {
                    Err(ExecError::Unsupported(format!("unknown function: {fname}")))
                }
            },
        }
    }

    /// Extract function arguments as evaluated Values.
    pub(super) fn extract_fn_args(
        &self,
        func: &ast::Function,
        row: &Row,
        col_meta: &[ColMeta],
    ) -> Result<Vec<Value>, ExecError> {
        match &func.args {
            ast::FunctionArguments::None => Ok(Vec::new()),
            ast::FunctionArguments::List(list) => {
                let mut args = Vec::new();
                for arg in &list.args {
                    match arg {
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                            args.push(self.eval_row_expr(e, row, col_meta)?);
                        }
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => {
                            // COUNT(*) style — handled in aggregate path
                        }
                        _ => {
                            return Err(ExecError::Unsupported("named function args".into()));
                        }
                    }
                }
                Ok(args)
            }
            ast::FunctionArguments::Subquery(_) => {
                Err(ExecError::Unsupported("subquery in function args".into()))
            }
        }
    }
}
