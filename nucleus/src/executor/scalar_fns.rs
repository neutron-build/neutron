//! Scalar (non-aggregate) function evaluation.
//!
//! Contains the massive `eval_scalar_fn` dispatch function and `extract_fn_args`
//! helper. These implement all 208+ built-in SQL functions.

use super::helpers::*;
use super::session::sync_block_on;
use super::types::ColMeta;
use super::{ExecError, ExecResult, Executor};
use crate::fts;
use crate::graph::PropValue as GraphPropValue;
use crate::graph::cypher::parse_cypher;
use crate::graph::cypher_executor::execute_cypher;
#[cfg(feature = "server")]
use crate::reactive::ChangeType;
use crate::timeseries;
use crate::types::{Row, Value};
use crate::vector;
use sqlparser::ast;
use std::collections::{HashMap, HashSet};

impl Executor {
    /// Evaluate a scalar (non-aggregate) function call.
    /// Evaluate a scalar function, then refuse to report success for a KV write
    /// whose log would not take it.
    ///
    /// The KV mutators log an append failure and apply the change anyway. That
    /// keeps the live view usable, and it makes the statement's success a lie:
    /// durable mode promised the write would survive a restart. Both logs carry
    /// an edge-triggered failure flag for exactly this — drained here, per
    /// call, so the error lands on the statement that caused it rather than on
    /// whatever ran next. Same discipline as the Datalog and vector WAL appends
    /// (NU-013, NU-048): a failed append fails the statement.
    pub(super) fn eval_scalar_fn(
        &self,
        fname: &str,
        func: &ast::Function,
        row: &Row,
        col_meta: &[ColMeta],
    ) -> Result<Value, ExecError> {
        let result = self.eval_scalar_fn_inner(fname, func, row, col_meta);
        // The flag is drained on every KV-touching call (edge-triggered), but
        // only converted to an error when the call otherwise SUCCEEDED: the
        // SQL-path mutators below now fail directly with a specific error
        // when their own WAL append fails (S95 finding 8), and that error
        // must not be replaced by the generic one.
        #[cfg(feature = "server")]
        if touches_kv_logs(fname) && self.kv_write_failed() && result.is_ok() {
            return Err(ExecError::Runtime(format!(
                "{fname}: WAL write failed; the value is in memory only and \
                 will not survive a restart"
            )));
        }
        result
    }

    /// Reconcile the KV ledger with a collection key's real footprint.
    ///
    /// Take `before` from `kv_key_bytes` around the mutation and pass it here.
    /// The old sites charged fixed constants — a flat 64 bytes for an HLL whose
    /// register array is 16 KiB, and nothing at all for lists, hashes, sets and
    /// sorted sets — so the ceiling the allocator enforces could not see the
    /// collections it was meant to bound.
    #[cfg(feature = "server")]
    fn kv_reconcile(&self, key: &str, before: usize) {
        let after = self.kv_store.collections().key_memory_bytes(key);
        let mut alloc = self.memory_allocator.lock();
        if after > before {
            alloc.account_used("kv", after - before);
        } else {
            alloc.release("kv", before - after);
        }
    }

    /// A collection key's current footprint in bytes.
    #[cfg(feature = "server")]
    fn kv_key_bytes(&self, key: &str) -> usize {
        self.kv_store.collections().key_memory_bytes(key)
    }

    /// Drain the write-failure flag from both KV logs and the cold tier.
    #[cfg(feature = "server")]
    fn kv_write_failed(&self) -> bool {
        let strings = self
            .kv_store
            .wal()
            .map(|w| w.take_write_error())
            .unwrap_or(false);
        let collections = self
            .kv_store
            .collections_wal()
            .map(|w| w.take_write_error())
            .unwrap_or(false);
        let cold = self.kv_store.take_cold_write_error();
        strings || collections || cold
    }

    fn eval_scalar_fn_inner(
        &self,
        fname: &str,
        func: &ast::Function,
        row: &Row,
        col_meta: &[ColMeta],
    ) -> Result<Value, ExecError> {
        // `ARRAY (SELECT ...)` is an array constructor, not a call with
        // arguments — but sqlparser models it as a Function whose arguments are
        // a bare Subquery, so it arrives here and used to die in
        // `extract_fn_args` as Unsupported("subquery in function args").
        //
        // Postgrex's type bootstrap issues exactly this shape to collect a
        // composite type's attribute oids, so the gap locked every Elixir, Ecto
        // and Phoenix application out of Nucleus at connect time — and because
        // Postgrex retries the bootstrap rather than surfacing the error, it
        // presented as a `DBConnection` queue timeout naming nothing.
        //
        // Handled before `extract_fn_args` because that is the call that fails;
        // any other function receiving a bare subquery still rejects, but now
        // names itself.
        if let ast::FunctionArguments::Subquery(subquery) = &func.args {
            let bare = fname.strip_prefix("PG_CATALOG.").unwrap_or(fname);
            if bare.eq_ignore_ascii_case("ARRAY") {
                return self.eval_array_subquery(subquery, row, col_meta);
            }
            return Err(ExecError::Unsupported(format!(
                "subquery argument to {bare}()"
            )));
        }

        let args = self.extract_fn_args(func, row, col_meta)?;

        // SECURITY ORDERING: strip the schema qualifier BEFORE any policy check
        // reads the name. psql and ORMs schema-qualify builtin calls
        // (pg_catalog.array_length, …) and the prefix never changes semantics.
        // This strip used to sit AFTER the specialty fail-closed guard below,
        // which made the guard trivially defeatable: `pg_catalog.kv_set(...)`
        // did not match the "KV_" prefix, sailed past the check, and only THEN
        // had its prefix removed — so it executed. Every policy decision in this
        // function must see the same canonical name the dispatcher executes.
        let fname = fname.strip_prefix("PG_CATALOG.").unwrap_or(fname);

        // A mutation this transaction could not undo must not be accepted
        // inside one. See `refused_in_transaction`: ROLLBACK reverts a
        // write-set that does not cover these stores, so before this they
        // stayed written after a rollback the client was told had succeeded.
        if let Some(reason) = refused_in_transaction(fname)
            && self.session_in_txn()
        {
            return Err(ExecError::Unsupported(format!("{fname}: {reason}")));
        }

        if is_specialty_surface(fname) && self.any_rls_active() {
            return Err(ExecError::PermissionDenied(format!(
                "{fname} is unavailable while row-level security is active because this specialty-store surface has no policy-aware access path"
            )));
        }

        // Specialty-store writes reach the engine as ordinary `SELECT kv_set(…)`
        // queries, so the statement-level admission gate cannot see them. Check
        // the degraded-mode gate here too, or a read-only server would still
        // accept KV/document/graph/vector writes.
        if self.service.is_read_only() && super::admission::scalar_fn_mutates(fname) {
            self.service.admit_write(fname)?;
        }

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
                // Character count, not byte count (PostgreSQL): length('héllo')
                // is 5, not 6.
                match &args[0] {
                    Value::Text(s) => Ok(Value::Int32(s.chars().count() as i32)),
                    Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Int32(args[0].to_string().chars().count() as i32)),
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
                    return Err(ExecError::Unsupported(
                        "CONCAT_WS requires at least 1 arg".into(),
                    ));
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
                    return Err(ExecError::Unsupported(format!(
                        "{fname} requires at least 2 args"
                    )));
                }
                let s = match &args[0] {
                    Value::Text(s) => s.clone(),
                    Value::Null => return Ok(Value::Null),
                    other => other.to_string(),
                };
                // Compute indices in SIGNED space, then clamp to [0, len] and
                // ensure start <= end. A negative/garbage start (e.g. -3.14) cast
                // straight to usize wraps to a huge value and slicing panics with
                // "start > end"; signed math avoids that and matches SQL semantics
                // (1-indexed start; positions before 1 are clipped but still count
                // toward the length window).
                let chars: Vec<char> = s.chars().collect();
                let n = chars.len() as i64;
                let start0 = value_to_i64(&args[1])?.saturating_sub(1); // 0-indexed, may be < 0
                let end0 = if args.len() > 2 {
                    start0.saturating_add(value_to_i64(&args[2])?) // exclusive end
                } else {
                    n
                };
                let start = start0.clamp(0, n) as usize;
                let end = end0.clamp(0, n) as usize;
                let end = end.max(start);
                let result: String = chars[start..end].iter().collect();
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
            // POSITION(substr IN string) — sqlparser yields args [substr, string].
            "POSITION" => {
                require_args(fname, &args, 2)?;
                match (&args[0], &args[1]) {
                    (Value::Text(substr), Value::Text(s)) => {
                        let pos = char_index_of(s, substr);
                        Ok(Value::Int32(pos))
                    }
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Ok(Value::Int32(0)),
                }
            }
            // strpos(string, substr) — arguments in the OPPOSITE order to
            // POSITION; sharing the binding returned 0 for every found substr.
            "STRPOS" => {
                require_args(fname, &args, 2)?;
                match (&args[0], &args[1]) {
                    (Value::Text(s), Value::Text(substr)) => {
                        let pos = char_index_of(s, substr);
                        Ok(Value::Int32(pos))
                    }
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Ok(Value::Int32(0)),
                }
            }
            "LEFT" => {
                require_args(fname, &args, 2)?;
                match &args[0] {
                    Value::Text(s) => {
                        let n = value_to_i64(&args[1])?;
                        let chars: Vec<char> = s.chars().collect();
                        // Negative n: all but the last |n| characters (PG).
                        let take = if n >= 0 {
                            (n as usize).min(chars.len())
                        } else {
                            chars.len().saturating_sub((-n) as usize)
                        };
                        Ok(Value::Text(chars[..take].iter().collect()))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("LEFT requires text".into())),
                }
            }
            "RIGHT" => {
                require_args(fname, &args, 2)?;
                match &args[0] {
                    Value::Text(s) => {
                        let n = value_to_i64(&args[1])?;
                        let chars: Vec<char> = s.chars().collect();
                        // Negative n: all but the first |n| characters (PG).
                        let start = if n >= 0 {
                            chars.len().saturating_sub(n as usize)
                        } else {
                            ((-n) as usize).min(chars.len())
                        };
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
                        let n = bounded_len(value_to_i64(&args[1])?, "REPEAT")?;
                        // Bound the *total* output (count * width), not just the
                        // count, so REPEAT of a long string errors rather than
                        // OOM-aborting the process.
                        match s.len().checked_mul(n) {
                            Some(total) if total <= MAX_STR_OUTPUT => Ok(Value::Text(s.repeat(n))),
                            _ => Err(ExecError::Unsupported(
                                "REPEAT: result exceeds maximum length".into(),
                            )),
                        }
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
                            return Err(ExecError::Unsupported(
                                "SPLIT_PART field position must be > 0".into(),
                            ));
                        }
                        let parts: Vec<&str> = s.split(delim.as_str()).collect();
                        Ok(Value::Text(
                            parts.get(part_num - 1).unwrap_or(&"").to_string(),
                        ))
                    }
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported(
                        "SPLIT_PART requires text args".into(),
                    )),
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
                    _ => Err(ExecError::Unsupported(
                        "TRANSLATE requires text args".into(),
                    )),
                }
            }
            "ASCII" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => Ok(Value::Int32(
                        s.chars().next().map(|c| c as i32).unwrap_or(0),
                    )),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("ASCII requires text".into())),
                }
            }
            "CHR" => {
                require_args(fname, &args, 1)?;
                let n = value_to_i64(&args[0])? as u32;
                match char::from_u32(n) {
                    Some(c) => Ok(Value::Text(c.to_string())),
                    None => Err(ExecError::Unsupported(format!(
                        "invalid character code: {n}"
                    ))),
                }
            }
            "REGEXP_REPLACE" => {
                if args.len() < 3 {
                    return Err(ExecError::Unsupported(
                        "REGEXP_REPLACE requires at least 3 args".into(),
                    ));
                }
                match (&args[0], &args[1], &args[2]) {
                    (Value::Text(s), Value::Text(pattern), Value::Text(replacement)) => {
                        // Limit regex pattern length to prevent excessive NFA compilation time.
                        const MAX_REGEX_PATTERN_LEN: usize = 1000;
                        if pattern.len() > MAX_REGEX_PATTERN_LEN {
                            return Err(ExecError::Runtime(format!(
                                "regex pattern too long ({} chars, max {})",
                                pattern.len(),
                                MAX_REGEX_PATTERN_LEN
                            )));
                        }
                        // Optional 4th arg: flags ('g' = global replace)
                        let flags = args
                            .get(3)
                            .and_then(|v| {
                                if let Value::Text(f) = v {
                                    Some(f.as_str())
                                } else {
                                    None
                                }
                            })
                            .unwrap_or("");
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
                    _ => Err(ExecError::Unsupported(
                        "REGEXP_REPLACE requires text args".into(),
                    )),
                }
            }
            "REGEXP_MATCH" | "REGEXP_MATCHES" => {
                if args.len() < 2 {
                    return Err(ExecError::Unsupported(
                        "REGEXP_MATCH requires at least 2 args".into(),
                    ));
                }
                match (&args[0], &args[1]) {
                    (Value::Text(s), Value::Text(pattern)) => {
                        // Limit regex pattern length to prevent excessive NFA compilation time.
                        const MAX_REGEX_PATTERN_LEN: usize = 1000;
                        if pattern.len() > MAX_REGEX_PATTERN_LEN {
                            return Err(ExecError::Runtime(format!(
                                "regex pattern too long ({} chars, max {})",
                                pattern.len(),
                                MAX_REGEX_PATTERN_LEN
                            )));
                        }
                        let re = regex::Regex::new(pattern).map_err(|e| {
                            ExecError::Runtime(format!("invalid regex pattern: {e}"))
                        })?;
                        match re.captures(s) {
                            Some(caps) => {
                                // Return array of captured groups (group 0 = full match)
                                let groups: Vec<Value> = caps
                                    .iter()
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
                    _ => Err(ExecError::Unsupported(
                        "REGEXP_MATCH requires text args".into(),
                    )),
                }
            }
            "STARTS_WITH" => {
                require_args(fname, &args, 2)?;
                match (&args[0], &args[1]) {
                    (Value::Text(s), Value::Text(prefix)) => {
                        Ok(Value::Bool(s.starts_with(prefix.as_str())))
                    }
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported(
                        "STARTS_WITH requires text args".into(),
                    )),
                }
            }
            "ENDS_WITH" => {
                require_args(fname, &args, 2)?;
                match (&args[0], &args[1]) {
                    (Value::Text(s), Value::Text(suffix)) => {
                        Ok(Value::Bool(s.ends_with(suffix.as_str())))
                    }
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported(
                        "ENDS_WITH requires text args".into(),
                    )),
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
                    _ => Err(ExecError::Unsupported(format!(
                        "{fname} requires text or bytea"
                    ))),
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
                    return Err(ExecError::Unsupported(
                        "LPAD requires at least 2 args".into(),
                    ));
                }
                match &args[0] {
                    Value::Text(s) => {
                        let target_len = bounded_len(value_to_i64(&args[1])?, "LPAD")?;
                        let fill = if args.len() > 2 {
                            match &args[2] {
                                Value::Text(f) => f.clone(),
                                _ => " ".to_string(),
                            }
                        } else {
                            " ".to_string()
                        };
                        // Operate on characters, not bytes: byte slicing breaks
                        // Unicode (and panics on a non-char-boundary cut).
                        let char_count = s.chars().count();
                        if char_count >= target_len {
                            Ok(Value::Text(s.chars().take(target_len).collect()))
                        } else {
                            let pad_len = target_len - char_count;
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
                    return Err(ExecError::Unsupported(
                        "RPAD requires at least 2 args".into(),
                    ));
                }
                match &args[0] {
                    Value::Text(s) => {
                        let target_len = bounded_len(value_to_i64(&args[1])?, "RPAD")?;
                        let fill = if args.len() > 2 {
                            match &args[2] {
                                Value::Text(f) => f.clone(),
                                _ => " ".to_string(),
                            }
                        } else {
                            " ".to_string()
                        };
                        // Operate on characters, not bytes (see LPAD).
                        let char_count = s.chars().count();
                        if char_count >= target_len {
                            Ok(Value::Text(s.chars().take(target_len).collect()))
                        } else {
                            let pad_len = target_len - char_count;
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
                    // checked_abs: i32::MIN/i64::MIN have no positive representation
                    // and would panic on .abs(); surface as a Postgres-style range error.
                    Value::Int32(n) => n
                        .checked_abs()
                        .map(Value::Int32)
                        .ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                    Value::Int64(n) => n
                        .checked_abs()
                        .map(Value::Int64)
                        .ok_or_else(|| ExecError::Runtime("integer out of range".into())),
                    Value::Float64(n) => Ok(Value::Float64(n.abs())),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("ABS requires numeric".into())),
                }
            }
            "ROUND" => {
                if args.is_empty() {
                    return Err(ExecError::Unsupported(
                        "ROUND requires at least 1 arg".into(),
                    ));
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
                    // NUMERIC round is EXACT and half-away-from-zero (PG),
                    // supporting negative scale.
                    Value::Numeric(t) => {
                        let d = crate::types::parse_numeric(t).map_err(ExecError::Runtime)?;
                        Ok(Value::Numeric(
                            round_decimal_scaled(d, decimals)?.to_string(),
                        ))
                    }
                    // PG rounds integers by scale too: round(123, -1) = 120.
                    // Non-negative scale is a no-op — keep the input type.
                    Value::Int32(n) if decimals < 0 => Ok(Value::Numeric(
                        round_decimal_scaled(rust_decimal::Decimal::from(*n), decimals)?
                            .to_string(),
                    )),
                    Value::Int64(n) if decimals < 0 => Ok(Value::Numeric(
                        round_decimal_scaled(rust_decimal::Decimal::from(*n), decimals)?
                            .to_string(),
                    )),
                    Value::Int32(_) | Value::Int64(_) => Ok(args[0].clone()),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("ROUND requires numeric".into())),
                }
            }
            "CEIL" | "CEILING" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Float64(n) => Ok(Value::Float64(n.ceil())),
                    Value::Numeric(t) => crate::types::parse_numeric(t)
                        .map(|d| Value::Numeric(d.ceil().to_string()))
                        .map_err(ExecError::Runtime),
                    Value::Int32(_) | Value::Int64(_) => Ok(args[0].clone()),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("CEIL requires numeric".into())),
                }
            }
            "FLOOR" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Float64(n) => Ok(Value::Float64(n.floor())),
                    Value::Numeric(t) => crate::types::parse_numeric(t)
                        .map(|d| Value::Numeric(d.floor().to_string()))
                        .map_err(ExecError::Runtime),
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
                    Value::Float64(n) => Ok(Value::Int32(if *n > 0.0 {
                        1
                    } else if *n < 0.0 {
                        -1
                    } else {
                        0
                    })),
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
                    // Postgres errors (not NULL) on mod-by-zero; give the right message.
                    (Value::Int32(_), Value::Int32(0)) | (Value::Int64(_), Value::Int64(0)) => {
                        Err(ExecError::Runtime("division by zero".into()))
                    }
                    (Value::Float64(_), Value::Float64(b)) if *b == 0.0 => {
                        Err(ExecError::Runtime("division by zero".into()))
                    }
                    // checked_rem avoids the i32::MIN % -1 / i64::MIN % -1 panic (result is 0).
                    (Value::Int32(a), Value::Int32(b)) => {
                        Ok(Value::Int32(a.checked_rem(*b).unwrap_or(0)))
                    }
                    (Value::Int64(a), Value::Int64(b)) => {
                        Ok(Value::Int64(a.checked_rem(*b).unwrap_or(0)))
                    }
                    (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(a % b)),
                    _ => Err(ExecError::Unsupported("MOD requires numeric".into())),
                }
            }
            "RANDOM" => Ok(Value::Float64(rand::random::<f64>())),
            "PI" => Ok(Value::Float64(std::f64::consts::PI)),
            "TRUNC" | "TRUNCATE" => {
                if args.is_empty() {
                    return Err(ExecError::Unsupported(
                        "TRUNC requires at least 1 arg".into(),
                    ));
                }
                let decimals = if args.len() > 1 {
                    value_to_i64(&args[1])? as i32
                } else {
                    0
                };
                match &args[0] {
                    Value::Float64(n) => {
                        let factor = 10f64.powi(decimals);
                        Ok(Value::Float64((n * factor).trunc() / factor))
                    }
                    Value::Numeric(t) => crate::types::parse_numeric(t)
                        .map(|d| {
                            Value::Numeric(d.trunc_with_scale(decimals.max(0) as u32).to_string())
                        })
                        .map_err(ExecError::Runtime),
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
                // checked_abs: i64::MIN has no positive representation (would panic).
                let mut a = value_to_i64(&args[0])?
                    .checked_abs()
                    .ok_or_else(|| ExecError::Runtime("bigint out of range".into()))?;
                let mut b = value_to_i64(&args[1])?
                    .checked_abs()
                    .ok_or_else(|| ExecError::Runtime("bigint out of range".into()))?;
                while b != 0 {
                    let t = b;
                    b = a % b;
                    a = t;
                }
                Ok(Value::Int64(a))
            }
            "LCM" => {
                require_args(fname, &args, 2)?;
                let a = value_to_i64(&args[0])?
                    .checked_abs()
                    .ok_or_else(|| ExecError::Runtime("bigint out of range".into()))?;
                let b = value_to_i64(&args[1])?
                    .checked_abs()
                    .ok_or_else(|| ExecError::Runtime("bigint out of range".into()))?;
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
                    // a/ga is exact (ga | a); the multiply by b can overflow i64.
                    (a / ga)
                        .checked_mul(b)
                        .map(Value::Int64)
                        .ok_or_else(|| ExecError::Runtime("bigint out of range".into()))
                }
            }
            "GENERATE_SERIES" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(ExecError::Unsupported(
                        "GENERATE_SERIES requires 2 or 3 args".into(),
                    ));
                }
                let start = value_to_i64(&args[0])?;
                let stop = value_to_i64(&args[1])?;
                let step = if args.len() == 3 {
                    value_to_i64(&args[2])?
                } else {
                    1
                };
                if step == 0 {
                    return Err(ExecError::Unsupported(
                        "GENERATE_SERIES step cannot be 0".into(),
                    ));
                }
                // Bound the series cardinality so an enormous range can't build a
                // multi-billion-element vector and OOM-abort the process.
                let span = (stop as i128 - start as i128).abs();
                let count = (span / (step as i128).abs()) + 1;
                if count > MAX_STR_OUTPUT as i128 {
                    return Err(ExecError::Unsupported(format!(
                        "GENERATE_SERIES: {count} elements exceeds maximum {MAX_STR_OUTPUT}"
                    )));
                }
                let mut vals = Vec::new();
                let mut current = start;
                // checked_add: stepping past i64::MAX/MIN must stop the series, not
                // panic (debug) or wrap into an infinite loop (release).
                if step > 0 {
                    while current <= stop {
                        vals.push(Value::Int64(current));
                        match current.checked_add(step) {
                            Some(next) => current = next,
                            None => break,
                        }
                    }
                } else {
                    while current >= stop {
                        vals.push(Value::Int64(current));
                        match current.checked_add(step) {
                            Some(next) => current = next,
                            None => break,
                        }
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
            // GREATEST/LEAST IGNORE NULL arguments (PostgreSQL); only an
            // all-NULL argument list yields NULL. Propagating NULL was wrong.
            "GREATEST" => {
                let mut best: Option<Value> = None;
                for arg in &args {
                    if matches!(arg, Value::Null) {
                        continue;
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
                        continue;
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
            "VERSION" => Ok(Value::Text(format!(
                "PostgreSQL 16.0 (Nucleus {} — The Definitive Database)",
                env!("CARGO_PKG_VERSION")
            ))),
            "CURRENT_DATABASE" | "CURRENT_CATALOG" => Ok(Value::Text("nucleus".to_string())),
            // to_regtype('name') resolves a type name to its OID, NULL when
            // unknown (psycopg's TypeInfo.fetch probes extension types this
            // way and treats an empty result as "type not installed").
            "TO_REGTYPE" => Ok(match args.first() {
                Some(Value::Text(s)) => super::expr::regtype_oid(s)
                    .map(Value::Int32)
                    .unwrap_or(Value::Null),
                _ => Value::Null,
            }),
            "CURRENT_SCHEMA" => Ok(Value::Text("public".to_string())),
            // Identity functions report the SESSION's principal, not a fixed
            // name. PostgreSQL semantics: SESSION_USER is the authenticated
            // login role and is unaffected by SET ROLE; CURRENT_USER (and its
            // synonym CURRENT_ROLE) is the effective role. An unauthenticated
            // embedded session has no login role and keeps the bootstrap
            // identity.
            "SESSION_USER" => {
                let session = self.current_session();
                let login = session.authenticated_user.read().clone();
                Ok(Value::Text(login.unwrap_or_else(|| "nucleus".to_string())))
            }
            "CURRENT_USER" | "CURRENT_ROLE" => {
                let session = self.current_session();
                let effective = session
                    .current_role
                    .read()
                    .clone()
                    .or_else(|| session.authenticated_user.read().clone());
                Ok(Value::Text(
                    effective.unwrap_or_else(|| "nucleus".to_string()),
                ))
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
                            _ => Err(ExecError::Unsupported(format!(
                                "EXTRACT({field}) from date"
                            ))),
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
                            _ => Err(ExecError::Unsupported(format!(
                                "EXTRACT({field}) from timestamp"
                            ))),
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
                            _ => Err(ExecError::Unsupported(format!(
                                "EXTRACT({field}) from integer"
                            ))),
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
                                _ => Err(ExecError::Unsupported(format!(
                                    "EXTRACT({field}) from text"
                                ))),
                            }
                        } else {
                            Err(ExecError::Unsupported(
                                "cannot parse date/time from text".into(),
                            ))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported(
                        "EXTRACT requires date/timestamp".into(),
                    )),
                }
            }
            "DATE_TRUNC" => {
                require_args(fname, &args, 2)?;
                let field = match &args[0] {
                    Value::Text(s) => s.to_lowercase(),
                    _ => {
                        return Err(ExecError::Unsupported(
                            "DATE_TRUNC field must be text".into(),
                        ));
                    }
                };
                match &args[1] {
                    Value::Timestamp(ts) => {
                        let total_secs = *ts / 1_000_000;
                        let days = (total_secs / 86400) as i32;
                        let time_secs = total_secs % 86400;
                        let (y, m, _d) = crate::types::days_to_ymd(days);
                        let truncated_us = match field.as_str() {
                            "year" => crate::types::ymd_to_days(y, 1, 1) as i64 * 86400 * 1_000_000,
                            "month" => {
                                crate::types::ymd_to_days(y, m, 1) as i64 * 86400 * 1_000_000
                            }
                            "day" => days as i64 * 86400 * 1_000_000,
                            "hour" => {
                                days as i64 * 86400 * 1_000_000
                                    + (time_secs / 3600) * 3600 * 1_000_000
                            }
                            "minute" => {
                                days as i64 * 86400 * 1_000_000 + (time_secs / 60) * 60 * 1_000_000
                            }
                            _ => {
                                return Err(ExecError::Unsupported(format!("DATE_TRUNC({field})")));
                            }
                        };
                        Ok(Value::Timestamp(truncated_us))
                    }
                    Value::Date(d) => {
                        let (y, m, _) = crate::types::days_to_ymd(*d);
                        let truncated = match field.as_str() {
                            "year" => crate::types::ymd_to_days(y, 1, 1),
                            "month" => crate::types::ymd_to_days(y, m, 1),
                            "day" => *d,
                            _ => {
                                return Err(ExecError::Unsupported(format!("DATE_TRUNC({field})")));
                            }
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
                                "minute" => {
                                    format!("{y:04}-{m:02}-{d:02} {hour:02}:{minute:02}:00")
                                }
                                _ => {
                                    return Err(ExecError::Unsupported(format!(
                                        "DATE_TRUNC({field})"
                                    )));
                                }
                            };
                            Ok(Value::Text(result))
                        } else {
                            Err(ExecError::Unsupported(format!(
                                "cannot parse date/time: {s}"
                            )))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported(
                        "DATE_TRUNC requires timestamp/date".into(),
                    )),
                }
            }
            "AGE" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(ExecError::Unsupported("AGE requires 1 or 2 args".into()));
                }
                let d1 = match &args[0] {
                    Value::Date(d) => *d,
                    Value::Timestamp(ts) => (*ts / 1_000_000 / 86400) as i32,
                    Value::Text(s) => parse_date_string(s)
                        .ok_or_else(|| ExecError::Unsupported(format!("AGE cannot parse: {s}")))?,
                    _ => return Err(ExecError::Unsupported("AGE requires date/timestamp".into())),
                };
                let d2 = if args.len() == 2 {
                    match &args[1] {
                        Value::Date(d) => *d,
                        Value::Timestamp(ts) => (*ts / 1_000_000 / 86400) as i32,
                        Value::Text(s) => parse_date_string(s).ok_or_else(|| {
                            ExecError::Unsupported(format!("AGE cannot parse: {s}"))
                        })?,
                        _ => {
                            return Err(ExecError::Unsupported(
                                "AGE requires date/timestamp".into(),
                            ));
                        }
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
                Ok(Value::Text(format!(
                    "{years} years {months} mons {days} days"
                )))
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
                                let time_us =
                                    (h as i64 * 3600 + min as i64 * 60 + sec as i64) * 1_000_000;
                                Ok(Value::Timestamp(days * 86400 * 1_000_000 + time_us))
                            } else {
                                Err(ExecError::Unsupported(format!(
                                    "cannot parse timestamp: {s}"
                                )))
                            }
                        }
                        Value::Null => Ok(Value::Null),
                        _ => Err(ExecError::Unsupported(
                            "TO_TIMESTAMP requires numeric or text".into(),
                        )),
                    }
                } else {
                    require_args(fname, &args, 2)?;
                    let s = match &args[0] {
                        Value::Text(s) => s.clone(),
                        _ => {
                            return Err(ExecError::Unsupported(
                                "TO_TIMESTAMP requires text".into(),
                            ));
                        }
                    };
                    if let Some((y, m, d, h, min, sec)) = parse_timestamp_parts(&s) {
                        let days = crate::types::ymd_to_days(y, m, d) as i64;
                        let time_us = (h as i64 * 3600 + min as i64 * 60 + sec as i64) * 1_000_000;
                        Ok(Value::Timestamp(days * 86400 * 1_000_000 + time_us))
                    } else {
                        Err(ExecError::Unsupported(format!(
                            "cannot parse timestamp: {s}"
                        )))
                    }
                }
            }
            "MAKE_DATE" => {
                require_args(fname, &args, 3)?;
                // Validate on the i64 BEFORE the i32 cast — the cast used to
                // truncate silently and ymd_to_days has no year guard.
                let year = value_to_i64(&args[0])?;
                if !(crate::types::MIN_DATE_YEAR as i64..=crate::types::MAX_DATE_YEAR as i64)
                    .contains(&year)
                {
                    return Err(ExecError::Runtime("date field value out of range".into()));
                }
                let y = year as i32;
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
                    return Err(ExecError::Unsupported(
                        "jsonb_set requires at least 3 args".into(),
                    ));
                }
                let new_val = value_to_json(&args[2]);
                match (&args[0], &args[1]) {
                    (Value::Jsonb(target_json), Value::Jsonb(serde_json::Value::Array(path))) => {
                        let mut target = target_json.clone();
                        let path_strs: Vec<String> = path
                            .iter()
                            .map(|p| match p {
                                serde_json::Value::String(s) => s.clone(),
                                other => other.to_string(),
                            })
                            .collect();
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
                    _ => Err(ExecError::Unsupported(
                        "jsonb_set requires jsonb target".into(),
                    )),
                }
            }
            "JSONB_PRETTY" | "JSON_PRETTY" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Jsonb(v) => Ok(Value::Text(
                        serde_json::to_string_pretty(v).unwrap_or_default(),
                    )),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("jsonb_pretty requires jsonb".into())),
                }
            }
            "JSONB_OBJECT_KEYS" | "JSON_OBJECT_KEYS" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Jsonb(serde_json::Value::Object(map)) => {
                        let keys: Vec<serde_json::Value> = map
                            .keys()
                            .map(|k| serde_json::Value::String(k.clone()))
                            .collect();
                        Ok(Value::Jsonb(serde_json::Value::Array(keys)))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported(
                        "jsonb_object_keys requires jsonb object".into(),
                    )),
                }
            }
            "JSONB_STRIP_NULLS" | "JSON_STRIP_NULLS" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Jsonb(v) => Ok(Value::Jsonb(strip_json_nulls(v))),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported(
                        "jsonb_strip_nulls requires jsonb".into(),
                    )),
                }
            }
            "JSONB_EXTRACT_PATH" | "JSON_EXTRACT_PATH" => {
                if args.is_empty() {
                    return Err(ExecError::Unsupported(
                        "jsonb_extract_path requires at least 1 arg".into(),
                    ));
                }
                let mut current = match &args[0] {
                    Value::Jsonb(v) => v.clone(),
                    Value::Null => return Ok(Value::Null),
                    _ => {
                        return Err(ExecError::Unsupported(
                            "jsonb_extract_path requires jsonb".into(),
                        ));
                    }
                };
                for arg in &args[1..] {
                    let key = match arg {
                        Value::Text(s) => s.clone(),
                        other => other.to_string(),
                    };
                    current = match current {
                        serde_json::Value::Object(ref map) => {
                            map.get(&key).cloned().unwrap_or(serde_json::Value::Null)
                        }
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
                    return Err(ExecError::Unsupported(
                        "jsonb_extract_path_text requires at least 1 arg".into(),
                    ));
                }
                let mut current = match &args[0] {
                    Value::Jsonb(v) => v.clone(),
                    Value::Null => return Ok(Value::Null),
                    _ => {
                        return Err(ExecError::Unsupported(
                            "jsonb_extract_path_text requires jsonb".into(),
                        ));
                    }
                };
                for arg in &args[1..] {
                    let key = match arg {
                        Value::Text(s) => s.clone(),
                        other => other.to_string(),
                    };
                    current = match current {
                        serde_json::Value::Object(ref map) => {
                            map.get(&key).cloned().unwrap_or(serde_json::Value::Null)
                        }
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
                Ok(Value::Float64(crate::vector::distance(
                    &a,
                    &b,
                    crate::vector::DistanceMetric::Cosine,
                ) as f64))
            }
            "VECTOR_INNER_PRODUCT" | "INNER_PRODUCT" => {
                require_args(fname, &args, 2)?;
                let a = json_to_vector(&args[0])?;
                let b = json_to_vector(&args[1])?;
                // Return positive inner product (not negated)
                Ok(Value::Float64(-crate::vector::distance(
                    &a,
                    &b,
                    crate::vector::DistanceMetric::InnerProduct,
                ) as f64))
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
                // BM25 term-frequency saturation (Okapi BM25, k1=1.2) — the same
                // ranking shape FTS_SEARCH uses. FTS_RANK scores a single
                // (doc, query) pair, so there is no corpus: idf is a constant
                // (omitted; it doesn't affect relative order) and the length term
                // is neutral (avgdl == this document's length, so dl/avgdl == 1).
                // The previous raw tf/len score could rank documents INVERSELY to
                // FTS_SEARCH because linear tf divided by length disagrees with
                // BM25's saturated tf when both tf and length vary.
                const K1: f64 = 1.2;
                let mut score = 0.0f64;
                for qt in &query_tokens {
                    let tf = tokens.iter().filter(|t| t.term == qt.term).count() as f64;
                    if tf > 0.0 {
                        score += tf * (K1 + 1.0) / (tf + K1);
                    }
                }
                Ok(Value::Float64(score))
            }
            "BM25" => {
                // BM25(column, query) → relevance of this row's text under the
                // corpus statistics of the FTS index on `column`.
                //
                // Scoring is row-local: every input except N, avgdl, and one
                // document frequency per query term comes from the row itself,
                // so the index supplies a handful of numbers and the score needs
                // no plumbing through the executor. The per-row cost is
                // dominated by tokenizing the row's text, not by the lookup.
                require_args(fname, &args, 2)?;
                let (Value::Text(text), Value::Text(query)) = (&args[0], &args[1]) else {
                    if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
                        return Ok(Value::Null);
                    }
                    return Err(ExecError::Unsupported(
                        "BM25 requires (text column, query text)".into(),
                    ));
                };
                let (qualifier, column) = Self::fn_arg_column_ref(func, 0).ok_or_else(|| {
                    ExecError::Unsupported(
                        "BM25's first argument must be a column reference, because the \
                         corpus statistics are read from that column's FTS index"
                            .into(),
                    )
                })?;
                // An unqualified column resolves through the row's own metadata,
                // so `BM25(body, …)` works without spelling out the table.
                let table = qualifier.or_else(|| {
                    col_meta
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(&column))
                        .and_then(|c| c.table.clone())
                });
                let stats = self
                    .fts_stats_for_column(table.as_deref(), &column, query)
                    .ok_or_else(|| {
                        ExecError::Unsupported(format!(
                            "BM25 requires a full-text index on '{column}': \
                             CREATE INDEX ON <table> USING FTS ({column})"
                        ))
                    })?;
                Ok(Value::Float64(crate::fts::bm25_score(text, &stats)))
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
                    _ => Err(ExecError::Unsupported(
                        "LEVENSHTEIN requires text args".into(),
                    )),
                }
            }

            // -- Time-series functions --
            "TIME_BUCKET" => {
                self.check_subsystem("timeseries")?;
                require_args(fname, &args, 2)?;
                let bucket_millis = value_to_i64(&args[0])? as u64;
                let ts = value_to_i64(&args[1])? as u64;
                if bucket_millis == 0 {
                    return Err(ExecError::Unsupported(
                        "TIME_BUCKET size must be positive".into(),
                    ));
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
                    return Err(ExecError::Unsupported(
                        "SPARSE_INSERT requires (doc_id, json_vector)".into(),
                    ));
                }
                let doc_id = val_to_u64(&args[0], "SPARSE_INSERT doc_id")?;
                let vec = json_to_sparse_vec(&args[1])?;
                let nnz = vec.nnz();
                self.sparse_index.write().insert(doc_id, vec);
                // Each posting: doc_id(8) + weight(4) + index(4) = ~16 bytes.
                self.memory_allocator
                    .lock()
                    .request("sparse", nnz * 16 + 32);
                Ok(Value::Bool(true))
            }
            "SPARSE_REMOVE" => {
                // sparse_remove(doc_id) → true/false
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported(
                        "SPARSE_REMOVE requires (doc_id)".into(),
                    ));
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
                    return Err(ExecError::Unsupported(
                        "SPARSE_SEARCH requires (json_query, top_k)".into(),
                    ));
                }
                let query = json_to_sparse_vec(&args[0])?;
                let top_k = (val_to_u64(&args[1], "SPARSE_SEARCH top_k")? as usize).min(10_000);
                let results = self.sparse_index.read().search_exact(&query, top_k);
                let json = results
                    .iter()
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
                    return Err(ExecError::Unsupported(
                        "SPARSE_WAND requires (json_query, top_k)".into(),
                    ));
                }
                let query = json_to_sparse_vec(&args[0])?;
                let top_k = (val_to_u64(&args[1], "SPARSE_WAND top_k")? as usize).min(10_000);
                let results = self.sparse_index.read().search_wand_pruned(&query, top_k);
                let json = results
                    .iter()
                    .map(|(id, score)| format!(r#"{{"doc_id":{id},"score":{score:.6}}}"#))
                    .collect::<Vec<_>>()
                    .join(",");
                Ok(Value::Text(format!("[{json}]")))
            }

            // -- Memory allocator query functions (Principle 2) --
            "MEM_USAGE" => {
                // mem_usage() → bytes currently tracked across all subsystems
                Ok(Value::Int64(
                    self.memory_allocator.lock().total_allocated() as i64
                ))
            }
            "MEM_BUDGET" => {
                // mem_budget() → total memory budget in bytes
                Ok(Value::Int64(
                    self.memory_allocator.lock().total_budget() as i64
                ))
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
                Ok(Value::Int64(
                    self.memory_allocator.lock().pressure_events() as i64
                ))
            }
            "MEM_PEAK" => {
                // mem_peak() → high-water mark in bytes
                Ok(Value::Int64(
                    self.memory_allocator.lock().peak_allocated() as i64
                ))
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
                // Real MD5 (PostgreSQL's md5() is the cryptographic digest as
                // 32 lowercase hex chars). The prior FNV-1a stand-in produced
                // a wrong 16-char value that no client would accept.
                let digest = md5::compute(text.as_bytes());
                Ok(Value::Text(format!("{digest:x}")))
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
                        Ok(Value::Text(
                            base64::engine::general_purpose::STANDARD.encode(&data),
                        ))
                    }
                    _ => Err(ExecError::Unsupported(format!(
                        "unknown encoding: {format}"
                    ))),
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
                        // Operate on bytes and validate: the old code sliced
                        // `encoded[i..i+2]`, which panicked on odd-length input,
                        // and silently dropped invalid digit pairs.
                        let h = encoded.as_bytes();
                        if h.len() % 2 != 0 {
                            return Err(ExecError::Runtime(
                                "invalid hexadecimal data: odd number of digits".into(),
                            ));
                        }
                        let mut bytes = Vec::with_capacity(h.len() / 2);
                        let mut i = 0;
                        while i < h.len() {
                            let hi = (h[i] as char).to_digit(16);
                            let lo = (h[i + 1] as char).to_digit(16);
                            match (hi, lo) {
                                (Some(hi), Some(lo)) => bytes.push((hi * 16 + lo) as u8),
                                _ => {
                                    return Err(ExecError::Runtime(
                                        "invalid hexadecimal digit".into(),
                                    ));
                                }
                            }
                            i += 2;
                        }
                        Ok(Value::Text(String::from_utf8_lossy(&bytes).to_string()))
                    }
                    "base64" => {
                        use base64::Engine;
                        match base64::engine::general_purpose::STANDARD.decode(&encoded) {
                            Ok(bytes) => {
                                Ok(Value::Text(String::from_utf8_lossy(&bytes).to_string()))
                            }
                            Err(e) => {
                                Err(ExecError::Unsupported(format!("base64 decode error: {e}")))
                            }
                        }
                    }
                    _ => Err(ExecError::Unsupported(format!(
                        "unknown encoding: {format}"
                    ))),
                }
            }

            // -- Sequence functions --
            "NEXTVAL" | "SETVAL" if self.sequence_state_unreadable() => {
                // Startup could not read sequences.json. Resuming from a
                // default would reissue values already handed out, so refuse
                // until an operator resolves it — recoverable, unlike
                // duplicate primary keys. (NU-165)
                Err(ExecError::Runtime(
                    "sequence state could not be read at startup (sequences.json unreadable); \
                     refusing to issue values that may already have been used. Restore or \
                     remove the file and restart."
                        .into(),
                ))
            }
            "NEXTVAL" => {
                require_args(fname, &args, 1)?;
                let seq_name = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => args[0].to_string(),
                };
                let seqs = self.sequences.read();
                if let Some(seq_mutex) = seqs.get(&seq_name) {
                    let mut seq = seq_mutex.lock();
                    // checked_add: past i64::MAX the old unchecked `+=` handed
                    // out i64::MIN (release) or panicked (debug).
                    let next = seq.current.checked_add(seq.increment).ok_or_else(|| {
                        ExecError::Unsupported(format!("sequence {seq_name} reached max value"))
                    })?;
                    seq.current = next;
                    if seq.increment > 0 && seq.current > seq.max_value {
                        return Err(ExecError::Unsupported(format!(
                            "sequence {seq_name} reached max value"
                        )));
                    }
                    // Descending sequences burn down to MINVALUE and stop —
                    // the max check alone let them continue into negatives.
                    if seq.increment < 0 && seq.current < seq.min_value {
                        return Err(ExecError::Unsupported(format!(
                            "sequence {seq_name} reached min value"
                        )));
                    }
                    let val = seq.current;
                    drop(seq);
                    drop(seqs);
                    // Persist sequence state synchronously so it survives restart.
                    //
                    // The value is BURNED if this fails: it stays consumed in
                    // memory and is never returned. A sequence may skip values
                    // — every implementation does, on rollback — but handing
                    // out a value a restart will hand out again produces
                    // duplicate SERIAL keys and repeats identifiers the caller
                    // was told were unique. Failing loudly is the only answer
                    // that keeps the promise. (NU-165)
                    self.persist_sequences_sync().map_err(|e| {
                        ExecError::Runtime(format!(
                            "sequence {seq_name}: value {val} was consumed but could not be \
                             made durable ({e}); it will not be issued. Retry NEXTVAL."
                        ))
                    })?;
                    Ok(Value::Int64(val))
                } else {
                    Err(ExecError::Unsupported(format!(
                        "sequence {seq_name} does not exist"
                    )))
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
                    Err(ExecError::Unsupported(format!(
                        "sequence {seq_name} does not exist"
                    )))
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
                    self.persist_sequences_sync().map_err(|e| {
                        ExecError::Runtime(format!(
                            "sequence {seq_name}: SETVAL to {new_val} could not be made \
                             durable ({e}); the in-memory value changed but a restart \
                             would not see it"
                        ))
                    })?;
                    Ok(Value::Int64(new_val))
                } else {
                    Err(ExecError::Unsupported(format!(
                        "sequence {seq_name} does not exist"
                    )))
                }
            }

            // -- PostgreSQL system/catalog functions --
            "PG_BACKEND_PID" => Ok(Value::Int32(std::process::id() as i32)),
            // asyncpg runs `SELECT pg_advisory_unlock_all()` as part of the
            // reset it issues when a connection goes back to the pool. Without
            // this, RELEASING a pooled connection raised — so every Python
            // client using a pool (the default) broke on the second query, not
            // the first, which is a confusing place to discover it.
            //
            // Returning true is honest rather than a stub: Nucleus has no
            // advisory locks at all — `pg_advisory_lock` does not exist, so a
            // session cannot be holding one — and the function's guarantee is
            // "this session now holds no advisory locks", which is true of all
            // zero of them. If advisory locks are ever implemented, this must
            // release them instead of reporting success.
            "PG_ADVISORY_UNLOCK_ALL" => Ok(Value::Bool(true)),
            "CURRENT_SETTING" => {
                // current_setting(name [, missing_ok]) — session overrides
                // first, then the same static defaults SHOW reports. Prisma's
                // schema engine calls this during connection setup.
                let Some(Value::Text(name)) = args.first() else {
                    return Err(ExecError::Unsupported(
                        "current_setting requires a setting name".into(),
                    ));
                };
                let missing_ok = matches!(args.get(1), Some(Value::Bool(true)));
                let key = name.to_lowercase();
                let sess = self.current_session();
                let user_val = sess.settings.read().get(&key).cloned();
                let value = user_val.or_else(|| {
                    Some(match key.as_str() {
                        "server_version" => "16.0 (Nucleus)".into(),
                        "server_version_num" => "160000".into(),
                        "server_encoding" | "client_encoding" => "UTF8".into(),
                        "standard_conforming_strings" => "on".into(),
                        "timezone" => "UTC".into(),
                        "datestyle" => "ISO, MDY".into(),
                        "integer_datetimes" => "on".into(),
                        "intervalstyle" => "postgres".into(),
                        "search_path" => "\"$user\", public".into(),
                        "max_connections" => "100".into(),
                        "transaction_isolation" | "default_transaction_isolation" => {
                            "read committed".into()
                        }
                        "max_index_keys" => "32".into(),
                        "lc_collate" => "C".into(),
                        "lc_ctype" => "en_US.UTF-8".into(),
                        _ => return None,
                    })
                });
                match value {
                    Some(v) => Ok(Value::Text(v)),
                    None if missing_ok => Ok(Value::Null),
                    None => Err(ExecError::Unsupported(format!(
                        "unrecognized configuration parameter \"{name}\""
                    ))),
                }
            }
            "PG_GET_FUNCTIONDEF" => {
                // Function-definition SQL isn't reconstructable from the
                // registry's stored body alone; NULL keeps introspection
                // queries running (they fall back to prosrc).
                Ok(Value::Null)
            }
            "PG_GET_SERIAL_SEQUENCE" => {
                // No sequence objects exist; NULL matches Postgres for a
                // column with no owned sequence. ORM introspection (drizzle)
                // calls this per column to detect serial columns.
                Ok(Value::Null)
            }
            "TXID_CURRENT" => Ok(Value::Int64(1)),
            "OBJ_DESCRIPTION" => {
                // Stub: always returns NULL
                Ok(Value::Null)
            }
            "COL_DESCRIPTION" => {
                // Stub: always returns NULL
                Ok(Value::Null)
            }
            "FORMAT_TYPE" => {
                // format_type(type_oid[, typmod]) -> SQL display name; psql's
                // \d uses this for the Type column. Typmod is honoured where
                // it matters: vector dimension (pgvector encoding) and varchar
                // length. NULL oid -> NULL.
                if args.is_empty() {
                    return Err(ExecError::Unsupported(
                        "FORMAT_TYPE requires at least 1 arg".into(),
                    ));
                }
                if matches!(args[0], Value::Null) {
                    return Ok(Value::Null);
                }
                let oid = value_to_i64(&args[0])?;
                let typmod = match args.get(1) {
                    Some(Value::Int32(n)) => *n,
                    Some(Value::Int64(n)) => *n as i32,
                    _ => -1,
                };
                let type_name = match oid {
                    16 => "boolean".to_string(),
                    20 => "bigint".to_string(),
                    21 => "smallint".to_string(),
                    23 => "integer".to_string(),
                    25 => "text".to_string(),
                    700 => "real".to_string(),
                    701 => "double precision".to_string(),
                    1043 if typmod > 4 => format!("character varying({})", typmod - 4),
                    1043 => "character varying".to_string(),
                    1082 => "date".to_string(),
                    1114 => "timestamp without time zone".to_string(),
                    1184 => "timestamp with time zone".to_string(),
                    1186 => "interval".to_string(),
                    1700 => "numeric".to_string(),
                    2950 => "uuid".to_string(),
                    3802 => "jsonb".to_string(),
                    17 => "bytea".to_string(),
                    1042 => "character".to_string(),
                    1005 => "smallint[]".to_string(),
                    1007 => "integer[]".to_string(),
                    1009 => "text[]".to_string(),
                    1016 => "bigint[]".to_string(),
                    16385 if typmod > 0 => format!("vector({typmod})"),
                    16385 => "vector".to_string(),
                    _ => "unknown".to_string(),
                };
                Ok(Value::Text(type_name))
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
                // 3-arg form names the principal to test. Answering about the
                // CALLER instead reported `true` for every table whenever a
                // superuser asked, so the catalog and the engine disagreed about
                // who could read what — and this is the function an audit query
                // would trust.
                if args.len() >= 3
                    && let (Value::Text(user), Value::Text(t), Value::Text(p)) =
                        (&args[0], &args[1], &args[2])
                {
                    let priv_upper = p.to_uppercase();
                    let priv_key = priv_upper.split_whitespace().next().unwrap_or(&priv_upper);
                    let result = sync_block_on(self.check_privilege_for_role(user, t, priv_key));
                    return Ok(Value::Bool(result));
                }
                let (table_name, privilege) = if args.len() >= 3 {
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
                    match &args[1] {
                        Value::Text(s) => s.as_str(),
                        _ => &schema,
                    }
                } else if args.len() == 2 {
                    match &args[0] {
                        Value::Text(s) => s.as_str(),
                        _ => &schema,
                    }
                } else {
                    &schema
                };
                Ok(Value::Bool(
                    schema_name == "public"
                        || schema_name == "pg_catalog"
                        || schema_name == "information_schema",
                ))
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
                    other => Ok(Value::Text(format!(
                        "\"{}\"",
                        other.to_string().replace('"', "\"\"")
                    ))),
                }
            }
            "PG_GET_USERBYID" => {
                // Always return "nucleus" regardless of OID
                Ok(Value::Text("nucleus".to_string()))
            }
            "PG_GET_CONSTRAINTDEF" => {
                // Stub: returns NULL
                Ok(Value::Null)
            }
            "PG_GET_INDEXDEF" => {
                // pg_get_indexdef(index_oid[, colno, pretty]) — synthesize the
                // definition from the catalog (psql's \d parses the part after
                // "USING" for its Indexes section). Unknown OID -> NULL.
                let oid = match args.first() {
                    Some(Value::Int32(n)) => *n as i64,
                    Some(Value::Int64(n)) => *n,
                    _ => return Ok(Value::Null),
                };
                let tables = sync_block_on(self.catalog.list_tables());
                let indexes = sync_block_on(self.catalog.get_all_indexes());
                // Index OIDs are assigned positionally after table OIDs
                // (16384 + tables.len() + i) — must match pg_class/pg_index.
                let pos = oid - 16384 - tables.len() as i64;
                if pos < 0 || pos as usize >= indexes.len() {
                    return Ok(Value::Null);
                }
                let idx = &indexes[pos as usize];
                let unique = if idx.unique { "UNIQUE " } else { "" };
                Ok(Value::Text(format!(
                    "CREATE {}INDEX {} ON public.{} USING btree ({})",
                    unique,
                    idx.name,
                    idx.table_name,
                    idx.columns.join(", ")
                )))
            }
            "ARRAY_TO_STRING" => {
                // array_to_string(array, sep [, null_string]) — used by \l on
                // datacl. NULL array → NULL, matching Postgres.
                if args.len() < 2 || args.len() > 3 {
                    return Err(ExecError::Unsupported(
                        "ARRAY_TO_STRING requires 2 or 3 arguments".into(),
                    ));
                }
                match (&args[0], &args[1]) {
                    (Value::Null, _) => Ok(Value::Null),
                    (Value::Array(vals), Value::Text(sep)) => {
                        let null_str = match args.get(2) {
                            Some(Value::Text(s)) => Some(s.clone()),
                            _ => None,
                        };
                        let parts: Vec<String> = vals
                            .iter()
                            .filter_map(|v| match v {
                                Value::Null => null_str.clone(),
                                other => Some(other.to_string()),
                            })
                            .collect();
                        Ok(Value::Text(parts.join(sep)))
                    }
                    _ => Err(ExecError::Unsupported(
                        "ARRAY_TO_STRING requires (array, text) arguments".into(),
                    )),
                }
            }

            // -- Array functions --
            "ARRAY_LENGTH" => {
                // array_length(array, dimension) — dimension is always 1, ignored
                require_args(fname, &args, 2)?;
                match &args[0] {
                    Value::Array(vals) => Ok(Value::Int32(vals.len() as i32)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported(
                        "ARRAY_LENGTH requires an array argument".into(),
                    )),
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
                    _ => Err(ExecError::Unsupported(
                        "ARRAY_UPPER requires an array argument".into(),
                    )),
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
                    _ => Err(ExecError::Unsupported(
                        "ARRAY_LOWER requires an array argument".into(),
                    )),
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
                    _ => Err(ExecError::Unsupported(
                        "ARRAY_APPEND requires an array as first argument".into(),
                    )),
                }
            }
            "ARRAY_CAT" => {
                // array_cat(array1, array2) — concatenates two arrays
                require_args(fname, &args, 2)?;
                let arr1 = match &args[0] {
                    Value::Array(v) => v.clone(),
                    Value::Null => Vec::new(),
                    _ => {
                        return Err(ExecError::Unsupported(
                            "ARRAY_CAT requires array arguments".into(),
                        ));
                    }
                };
                let arr2 = match &args[1] {
                    Value::Array(v) => v.clone(),
                    Value::Null => Vec::new(),
                    _ => {
                        return Err(ExecError::Unsupported(
                            "ARRAY_CAT requires array arguments".into(),
                        ));
                    }
                };
                let mut result = arr1;
                result.extend(arr2);
                Ok(Value::Array(result))
            }
            "UNNEST" => {
                // unnest(array) — set-returning function; for scalar context return first element
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Array(vals) => Ok(vals.first().cloned().unwrap_or(Value::Null)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported(
                        "UNNEST requires an array argument".into(),
                    )),
                }
            }
            "CARDINALITY" => {
                // cardinality(array) — total number of elements (flattened for multi-dim, but we only have 1-dim)
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Array(vals) => Ok(Value::Int32(vals.len() as i32)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported(
                        "CARDINALITY requires an array argument".into(),
                    )),
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
                            return Err(ExecError::Unsupported(
                                "vector literal must be [...]".into(),
                            ));
                        }
                        let inner = &s[1..s.len() - 1];
                        if inner.is_empty() {
                            return Ok(Value::Vector(Vec::new()));
                        }
                        let floats: Result<Vec<f32>, _> =
                            inner.split(',').map(|v| v.trim().parse::<f32>()).collect();
                        match floats {
                            Ok(vec) => Ok(Value::Vector(vec)),
                            Err(e) => Err(ExecError::Unsupported(format!(
                                "invalid vector literal: {e}"
                            ))),
                        }
                    }
                    Value::Array(vals) => {
                        // Convert array of numbers to vector
                        let floats: Result<Vec<f32>, _> = vals
                            .iter()
                            .map(|v| match v {
                                Value::Int32(n) => Ok(*n as f32),
                                Value::Int64(n) => Ok(*n as f32),
                                Value::Float64(n) => Ok(*n as f32),
                                Value::Null => Err(ExecError::Unsupported(
                                    "vector elements cannot be null".into(),
                                )),
                                _ => Err(ExecError::Unsupported(
                                    "vector elements must be numeric".into(),
                                )),
                            })
                            .collect();
                        Ok(Value::Vector(floats?))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported(
                        "vector() requires text or array".into(),
                    )),
                }
            }
            "VECTOR_DISTANCE" => {
                self.check_subsystem("vector")?;
                // vector_distance(vec1, vec2, 'l2'|'cosine'|'inner') — compute distance between vectors
                if args.len() < 2 || args.len() > 3 {
                    return Err(ExecError::Unsupported(
                        "VECTOR_DISTANCE requires 2 or 3 args".into(),
                    ));
                }
                let vec1 = match &args[0] {
                    Value::Vector(v) => v,
                    Value::Null => return Ok(Value::Null),
                    _ => {
                        return Err(ExecError::Unsupported(
                            "VECTOR_DISTANCE arg 1 must be vector".into(),
                        ));
                    }
                };
                let vec2 = match &args[1] {
                    Value::Vector(v) => v,
                    Value::Null => return Ok(Value::Null),
                    _ => {
                        return Err(ExecError::Unsupported(
                            "VECTOR_DISTANCE arg 2 must be vector".into(),
                        ));
                    }
                };
                if vec1.len() != vec2.len() {
                    return Err(ExecError::Unsupported(format!(
                        "vector dimensions must match: {} vs {}",
                        vec1.len(),
                        vec2.len()
                    )));
                }
                let metric = if args.len() == 3 {
                    match &args[2] {
                        Value::Text(s) => match s.to_lowercase().as_str() {
                            "l2" | "euclidean" => vector::DistanceMetric::L2,
                            "cosine" => vector::DistanceMetric::Cosine,
                            "inner" | "ip" | "dot" => vector::DistanceMetric::InnerProduct,
                            _ => {
                                return Err(ExecError::Unsupported(format!(
                                    "unknown distance metric: {s}"
                                )));
                            }
                        },
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
                    _ => Err(ExecError::Unsupported(
                        "TS_MATCH requires (text, query_text)".into(),
                    )),
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
                    _ => Err(ExecError::Unsupported(
                        "PLAINTO_TSQUERY requires text".into(),
                    )),
                }
            }
            "TS_HEADLINE" => {
                // ts_headline(text, query) → text with matching terms highlighted
                require_args(fname, &args, 2)?;
                match (&args[0], &args[1]) {
                    (Value::Text(content), Value::Text(query)) => {
                        let query_tokens = fts::tokenize(query);
                        let query_terms: std::collections::HashSet<String> =
                            query_tokens.iter().map(|t| t.term.clone()).collect();
                        let mut result = String::new();
                        for word in content.split_whitespace() {
                            if !result.is_empty() {
                                result.push(' ');
                            }
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
                    _ => Err(ExecError::Unsupported(
                        "TS_HEADLINE requires (text, query_text)".into(),
                    )),
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
                        let p = parse_point_wkt(s).ok_or_else(|| {
                            ExecError::Unsupported("ST_X: invalid point WKT".into())
                        })?;
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
                        let p = parse_point_wkt(s).ok_or_else(|| {
                            ExecError::Unsupported("ST_Y: invalid point WKT".into())
                        })?;
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
                        let poly = parse_polygon_wkt(poly_wkt).ok_or_else(|| {
                            ExecError::Unsupported("ST_CONTAINS: invalid polygon WKT".into())
                        })?;
                        let pt = parse_point_wkt(pt_wkt).ok_or_else(|| {
                            ExecError::Unsupported("ST_CONTAINS: invalid point WKT".into())
                        })?;
                        Ok(Value::Bool(poly.contains(&pt)))
                    }
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported(
                        "ST_CONTAINS requires (polygon_wkt, point_wkt)".into(),
                    )),
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
                        let bucket = parse_bucket_size(bucket_str).ok_or_else(|| {
                            ExecError::Unsupported(format!(
                                "DATE_BIN: unknown interval '{bucket_str}'"
                            ))
                        })?;
                        let ts = value_to_i64(&args[1])? as u64;
                        Ok(Value::Int64(timeseries::time_bucket(ts, bucket) as i64))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported(
                        "DATE_BIN requires (text, timestamp)".into(),
                    )),
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
                        if let Ok(edges) =
                            serde_json::from_str::<Vec<serde_json::Value>>(edges_json)
                        {
                            // Collect all unique node IDs
                            let mut node_ids = std::collections::HashSet::new();
                            for edge in &edges {
                                if let (Some(f), Some(t)) = (
                                    edge.get("from").and_then(|v| v.as_u64()),
                                    edge.get("to").and_then(|v| v.as_u64()),
                                ) {
                                    node_ids.insert(f);
                                    node_ids.insert(t);
                                }
                            }
                            // Create nodes (IDs are assigned sequentially, so we need a mapping)
                            let mut id_map: std::collections::HashMap<u64, u64> =
                                std::collections::HashMap::new();
                            for &nid in &node_ids {
                                let internal_id =
                                    gs.create_node(vec![], std::collections::BTreeMap::new());
                                id_map.insert(nid, internal_id);
                            }
                            // Create edges
                            for edge in &edges {
                                if let (Some(f), Some(t)) = (
                                    edge.get("from").and_then(|v| v.as_u64()),
                                    edge.get("to").and_then(|v| v.as_u64()),
                                ) && let (Some(&fi), Some(&ti)) =
                                    (id_map.get(&f), id_map.get(&t))
                                {
                                    gs.create_edge(
                                        fi,
                                        ti,
                                        "EDGE".to_string(),
                                        std::collections::BTreeMap::new(),
                                    );
                                }
                            }
                            // Find shortest path
                            let mapped_from = id_map.get(&from_id).copied();
                            let mapped_to = id_map.get(&to_id).copied();
                            if let (Some(mf), Some(mt)) = (mapped_from, mapped_to) {
                                match gs.shortest_path(
                                    mf,
                                    mt,
                                    crate::graph::Direction::Outgoing,
                                    None,
                                ) {
                                    Some(path) => Ok(Value::Int32((path.len() as i32) - 1)),
                                    None => Ok(Value::Null),
                                }
                            } else {
                                Ok(Value::Null)
                            }
                        } else {
                            Err(ExecError::Unsupported(
                                "GRAPH_SHORTEST_PATH_LENGTH: invalid edges JSON".into(),
                            ))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported(
                        "GRAPH_SHORTEST_PATH_LENGTH requires (edges_json, from_id, to_id)".into(),
                    )),
                }
            }
            "GRAPH_NODE_DEGREE" => {
                // graph_node_degree(edges_json, node_id) → number of edges connected to node
                require_args(fname, &args, 2)?;
                match &args[0] {
                    Value::Text(edges_json) => {
                        let node_id = value_to_i64(&args[1])? as u64;
                        if let Ok(edges) =
                            serde_json::from_str::<Vec<serde_json::Value>>(edges_json)
                        {
                            let degree: usize = edges
                                .iter()
                                .filter(|e| {
                                    let f = e.get("from").and_then(|v| v.as_u64());
                                    let t = e.get("to").and_then(|v| v.as_u64());
                                    f == Some(node_id) || t == Some(node_id)
                                })
                                .count();
                            Ok(Value::Int32(degree as i32))
                        } else {
                            Err(ExecError::Unsupported(
                                "GRAPH_NODE_DEGREE: invalid edges JSON".into(),
                            ))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported(
                        "GRAPH_NODE_DEGREE requires (edges_json, node_id)".into(),
                    )),
                }
            }

            "CYPHER" => {
                // CYPHER(query_text) — execute a Cypher query against the persistent graph store.
                self.check_subsystem("graph")?;
                if args.is_empty() || args.len() > 1 {
                    return Err(ExecError::Unsupported(
                        "CYPHER requires exactly 1 argument (query string)".into(),
                    ));
                }
                let cypher_text = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => {
                        return Err(ExecError::Unsupported(
                            "CYPHER argument must be a text string".into(),
                        ));
                    }
                };
                let parsed = parse_cypher(&cypher_text)
                    .map_err(|e| ExecError::Unsupported(format!("Cypher parse error: {e:?}")))?;
                let result = {
                    let mut gs = self.graph_store.write();
                    let xact = self.cross_model_before_graph(&gs);
                    gs.clear_touched();
                    gs.set_xact_tag(xact);
                    let outcome = execute_cypher(&mut gs, &parsed).map_err(|e| {
                        ExecError::Unsupported(format!("Cypher execution error: {e:?}"))
                    });
                    let touched = gs.take_touched();
                    drop(gs);
                    self.cross_model_after_graph(touched);
                    outcome?
                };
                // Convert CypherResult to a JSON-like text representation.
                // Format: columns as header, rows as JSON arrays.
                let mut lines = Vec::new();
                lines.push(result.columns.join(","));
                for row in &result.rows {
                    let cells: Vec<String> = row
                        .iter()
                        .map(|v| match v {
                            GraphPropValue::Null => "null".to_string(),
                            GraphPropValue::Bool(b) => b.to_string(),
                            GraphPropValue::Int(n) => n.to_string(),
                            GraphPropValue::Float(f) => f.to_string(),
                            GraphPropValue::Text(s) => s.clone(),
                        })
                        .collect();
                    lines.push(cells.join(","));
                }
                Ok(Value::Text(lines.join("\n")))
            }

            "ENCRYPTED_LOOKUP" => {
                // encrypted_lookup(index_name, value) — look up row IDs via encrypted index.
                require_args(fname, &args, 2)?;
                let idx_name = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => {
                        return Err(ExecError::Unsupported(
                            "ENCRYPTED_LOOKUP arg 1 must be index name text".into(),
                        ));
                    }
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
                    None => Err(ExecError::Unsupported(format!(
                        "encrypted index '{idx_name}' not found"
                    ))),
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
                    return Err(ExecError::Unsupported(
                        "KV_SET requires 2 or 3 arguments".into(),
                    ));
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
                let estimated = key.len() + Self::estimate_value_bytes(&value) + 64;
                if !self.memory_allocator.lock().request("kv", estimated) {
                    return Err(ExecError::Unsupported(format!(
                        "KV_SET: memory budget exceeded (need {} bytes for key '{}')",
                        estimated, key
                    )));
                }
                let xact = self.cross_model_touch_kv(&key);
                self.kv_store
                    .set_xact(&key, value, ttl, xact)
                    .map_err(|e| wal_failure_to_exec_error(&format!("KV_SET '{key}'"), e))?;
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
                let xact = self.cross_model_touch_kv(&key);
                let deleted = self
                    .kv_store
                    .del_xact(&key, xact)
                    .map_err(|e| wal_failure_to_exec_error(&format!("KV_DEL '{key}'"), e))?;
                if deleted {
                    self.memory_allocator.lock().release("kv", key.len() + 96);
                }
                Ok(Value::Bool(deleted))
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
                    return Err(ExecError::Unsupported(
                        "KV_INCR requires 1 or 2 arguments".into(),
                    ));
                }
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let amount = if args.len() == 2 {
                    match &args[1] {
                        Value::Int32(n) => *n as i64,
                        Value::Int64(n) => *n,
                        _ => {
                            return Err(ExecError::Unsupported(
                                "KV_INCR amount must be integer".into(),
                            ));
                        }
                    }
                } else {
                    1
                };
                let xact = self.cross_model_touch_kv(&key);
                match self.kv_store.incr_by_xact(&key, amount, xact) {
                    Ok(v) => Ok(Value::Int64(v)),
                    Err(crate::kv::KvError::Wal(e)) => {
                        Err(wal_failure_to_exec_error(&format!("KV_INCR '{key}'"), e))
                    }
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
                let xact = self.cross_model_touch_kv(&key);
                Ok(Value::Bool(self.kv_store.expire_xact(&key, ttl, xact)))
            }
            "KV_SETNX" => {
                // kv_setnx(key, value) or kv_setnx(key, value, ttl_secs)
                // → true if set, false if already exists. With a TTL this is
                // the atomic lock acquire (Redis SET NX EX): value and expiry
                // commit in one critical section.
                if args.len() < 2 || args.len() > 3 {
                    return Err(ExecError::Unsupported(
                        "KV_SETNX requires 2 or 3 arguments".into(),
                    ));
                }
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let ttl = if args.len() == 3 {
                    match &args[2] {
                        Value::Null => None,
                        v => Some(val_to_u64(v, "KV_SETNX ttl")?),
                    }
                } else {
                    None
                };
                let estimated = key.len() + Self::estimate_value_bytes(&args[1]) + 64;
                if !self.memory_allocator.lock().request("kv", estimated) {
                    return Err(ExecError::Unsupported(format!(
                        "KV_SETNX: memory budget exceeded (need {} bytes)",
                        estimated
                    )));
                }
                let xact = self.cross_model_touch_kv(&key);
                let was_set = self
                    .kv_store
                    .setnx_ttl_xact(&key, args[1].clone(), ttl, xact);
                if !was_set {
                    // Key already existed, release the reservation
                    self.memory_allocator.lock().release("kv", estimated);
                }
                Ok(Value::Bool(was_set))
            }
            "KV_CDEL" => {
                // kv_cdel(key, expected) → true if the key held exactly
                // `expected` and was deleted. The safe lock release.
                require_args(fname, &args, 2)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    Value::Null => return Ok(Value::Bool(false)),
                    other => other.to_string(),
                };
                let xact = self.cross_model_touch_kv(&key);
                let deleted = self.kv_store.cdel_xact(&key, &args[1], xact);
                if deleted {
                    self.memory_allocator.lock().release("kv", key.len() + 96);
                }
                Ok(Value::Bool(deleted))
            }
            "KV_CEXPIRE" => {
                // kv_cexpire(key, expected, ttl_secs) → true if the key held
                // exactly `expected` and its TTL was updated. The lease
                // renewal heartbeat.
                require_args(fname, &args, 3)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    Value::Null => return Ok(Value::Bool(false)),
                    other => other.to_string(),
                };
                let ttl = val_to_u64(&args[2], "KV_CEXPIRE ttl")?;
                let xact = self.cross_model_touch_kv(&key);
                Ok(Value::Bool(
                    self.kv_store.cexpire_xact(&key, &args[1], ttl, xact),
                ))
            }
            "KV_DBSIZE" => {
                // kv_dbsize() → count of non-expired keys
                Ok(Value::Int64(self.kv_store.dbsize() as i64))
            }
            "KV_FLUSHDB" => {
                // kv_flushdb() → 'OK'
                self.cross_model_touch_kv_all();
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
                #[cfg(feature = "server")]
                let before_bytes = self.kv_key_bytes(&key);
                let outcome = match self.kv_store.lpush(&key, val) {
                    Ok(len) => Ok(Value::Int64(len as i64)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                };
                #[cfg(feature = "server")]
                self.kv_reconcile(&key, before_bytes);
                outcome
            }
            "KV_RPUSH" => {
                // kv_rpush(key, value) → list length after push
                require_args(fname, &args, 2)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let val = args[1].clone();
                #[cfg(feature = "server")]
                let before_bytes = self.kv_key_bytes(&key);
                let outcome = match self.kv_store.rpush(&key, val) {
                    Ok(len) => Ok(Value::Int64(len as i64)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                };
                #[cfg(feature = "server")]
                self.kv_reconcile(&key, before_bytes);
                outcome
            }
            "KV_LPOP" => {
                // kv_lpop(key) → popped value or NULL
                require_args(fname, &args, 1)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                #[cfg(feature = "server")]
                let before_bytes = self.kv_key_bytes(&key);
                let outcome = match self.kv_store.lpop(&key) {
                    Ok(Some(v)) => Ok(v),
                    Ok(None) => Ok(Value::Null),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                };
                #[cfg(feature = "server")]
                self.kv_reconcile(&key, before_bytes);
                outcome
            }
            "KV_RPOP" => {
                // kv_rpop(key) → popped value or NULL
                require_args(fname, &args, 1)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                #[cfg(feature = "server")]
                let before_bytes = self.kv_key_bytes(&key);
                let outcome = match self.kv_store.rpop(&key) {
                    Ok(Some(v)) => Ok(v),
                    Ok(None) => Ok(Value::Null),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                };
                #[cfg(feature = "server")]
                self.kv_reconcile(&key, before_bytes);
                outcome
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
                    _ => {
                        return Err(ExecError::Unsupported(
                            "KV_LRANGE start must be integer".into(),
                        ));
                    }
                };
                let stop = match &args[2] {
                    Value::Int32(n) => *n as i64,
                    Value::Int64(n) => *n,
                    _ => {
                        return Err(ExecError::Unsupported(
                            "KV_LRANGE stop must be integer".into(),
                        ));
                    }
                };
                match self.kv_store.lrange(&key, start, stop) {
                    Ok(vals) => {
                        // JSON array so list values containing ',' are not corrupted
                        // on the client (SDKs JSON-parse this, they don't split on ',').
                        let s: Vec<String> = vals.iter().map(|v| v.to_string()).collect();
                        Ok(Value::Text(
                            serde_json::to_string(&s).unwrap_or_else(|_| "[]".into()),
                        ))
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
                    _ => {
                        return Err(ExecError::Unsupported(
                            "KV_LINDEX index must be integer".into(),
                        ));
                    }
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
                #[cfg(feature = "server")]
                let before_bytes = self.kv_key_bytes(&key);
                let outcome = match self.kv_store.hset(&key, &field, val) {
                    Ok(is_new) => Ok(Value::Bool(is_new)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                };
                #[cfg(feature = "server")]
                self.kv_reconcile(&key, before_bytes);
                outcome
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
                #[cfg(feature = "server")]
                let before_bytes = self.kv_key_bytes(&key);
                let outcome = match self.kv_store.hdel(&key, &field) {
                    Ok(deleted) => Ok(Value::Bool(deleted)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                };
                #[cfg(feature = "server")]
                self.kv_reconcile(&key, before_bytes);
                outcome
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
                        // JSON array of [field, value] pairs: preserves order and never
                        // corrupts fields/values containing ',' or '=' (was "f=v,f=v").
                        let s: Vec<[String; 2]> = pairs
                            .iter()
                            .map(|(f, v)| [f.clone(), v.to_string()])
                            .collect();
                        Ok(Value::Text(
                            serde_json::to_string(&s).unwrap_or_else(|_| "[]".into()),
                        ))
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
                #[cfg(feature = "server")]
                let before_bytes = self.kv_key_bytes(&key);
                let outcome = match self.kv_store.sadd(&key, &member) {
                    Ok(is_new) => Ok(Value::Bool(is_new)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                };
                #[cfg(feature = "server")]
                self.kv_reconcile(&key, before_bytes);
                outcome
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
                #[cfg(feature = "server")]
                let before_bytes = self.kv_key_bytes(&key);
                let outcome = match self.kv_store.srem(&key, &member) {
                    Ok(removed) => Ok(Value::Bool(removed)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                };
                #[cfg(feature = "server")]
                self.kv_reconcile(&key, before_bytes);
                outcome
            }
            "KV_KEYS" => {
                // kv_keys(pattern) → JSON array of non-expired keys matching a
                // simple glob (* wildcard). Exposes KvStore::keys over SQL so
                // operators (teploy kv list) and apps can enumerate a shared
                // config namespace by prefix, e.g. KV_KEYS('flags/*').
                require_args(fname, &args, 1)?;
                let pattern = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let mut keys = self.kv_store.keys(&pattern);
                keys.sort();
                Ok(Value::Text(
                    serde_json::to_string(&keys).unwrap_or_else(|_| "[]".into()),
                ))
            }
            "KV_SMEMBERS" => {
                // kv_smembers(key) → comma-separated members
                require_args(fname, &args, 1)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                match self.kv_store.smembers(&key) {
                    // JSON array so members containing ',' survive the round-trip.
                    Ok(members) => Ok(Value::Text(
                        serde_json::to_string(&members).unwrap_or_else(|_| "[]".into()),
                    )),
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
                    _ => {
                        return Err(ExecError::Unsupported(
                            "KV_ZADD score must be numeric".into(),
                        ));
                    }
                };
                let member = match &args[2] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                #[cfg(feature = "server")]
                let before_bytes = self.kv_key_bytes(&key);
                let outcome = match self.kv_store.col_zadd(&key, &member, score) {
                    Ok(is_new) => Ok(Value::Bool(is_new)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                };
                #[cfg(feature = "server")]
                self.kv_reconcile(&key, before_bytes);
                outcome
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
                #[cfg(feature = "server")]
                let before_bytes = self.kv_key_bytes(&key);
                let outcome = match self.kv_store.col_zrem(&key, &member) {
                    Ok(removed) => Ok(Value::Bool(removed)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                };
                #[cfg(feature = "server")]
                self.kv_reconcile(&key, before_bytes);
                outcome
            }
            "KV_ZRANGE" => {
                // kv_zrange(key, start, stop) → comma-separated "member:score" pairs
                require_args(fname, &args, 3)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                // Signed, so -1 means "last". Casting to usize here turned -1
                // into usize::MAX, which the store then read as an empty range.
                let start = match &args[1] {
                    Value::Int32(n) => *n as i64,
                    Value::Int64(n) => *n,
                    _ => {
                        return Err(ExecError::Unsupported(
                            "KV_ZRANGE start must be integer".into(),
                        ));
                    }
                };
                let stop = match &args[2] {
                    Value::Int32(n) => *n as i64,
                    Value::Int64(n) => *n,
                    _ => {
                        return Err(ExecError::Unsupported(
                            "KV_ZRANGE stop must be integer".into(),
                        ));
                    }
                };
                match self.kv_store.col_zrange(&key, start, stop) {
                    Ok(entries) => {
                        // JSON array of [member, score] — never corrupts members
                        // containing ',' or ':' (was "member:score,member:score").
                        let s: Vec<(String, f64)> = entries
                            .iter()
                            .map(|e| (e.member.clone(), e.score))
                            .collect();
                        Ok(Value::Text(
                            serde_json::to_string(&s).unwrap_or_else(|_| "[]".into()),
                        ))
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
                    _ => {
                        return Err(ExecError::Unsupported(
                            "KV_ZRANGEBYSCORE min must be numeric".into(),
                        ));
                    }
                };
                let max_score = match &args[2] {
                    Value::Float64(f) => *f,
                    Value::Int32(n) => *n as f64,
                    Value::Int64(n) => *n as f64,
                    _ => {
                        return Err(ExecError::Unsupported(
                            "KV_ZRANGEBYSCORE max must be numeric".into(),
                        ));
                    }
                };
                match self.kv_store.col_zrangebyscore(&key, min_score, max_score) {
                    Ok(entries) => {
                        // JSON array of [member, score] — see KV_ZRANGE.
                        let s: Vec<(String, f64)> = entries
                            .iter()
                            .map(|e| (e.member.clone(), e.score))
                            .collect();
                        Ok(Value::Text(
                            serde_json::to_string(&s).unwrap_or_else(|_| "[]".into()),
                        ))
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
                // Precision 14 → 2^14 one-byte registers, so a new HLL key is
                // 16 KiB. The comment here said "fixed 16 bytes per key" and
                // the request was 64 — wrong by 1024x and 256x respectively.
                // The pre-flight reserves the real cost of creating one; the
                // reconcile below settles the ledger against the actual key.
                const HLL_REGISTERS_BYTES: usize = 1 << 14;
                if !self
                    .memory_allocator
                    .lock()
                    .request("kv", HLL_REGISTERS_BYTES)
                {
                    return Err(ExecError::Unsupported(
                        "KV_PFADD: memory budget exceeded".into(),
                    ));
                }
                #[cfg(feature = "server")]
                let before_bytes = self.kv_key_bytes(&key);
                let outcome = match self.kv_store.col_pfadd(&key, &element) {
                    Ok(changed) => Ok(Value::Bool(changed)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                };
                #[cfg(feature = "server")]
                self.kv_reconcile(&key, before_bytes);
                outcome
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
            "KV_PFMERGE" => {
                // kv_pfmerge(dest, src1 [, src2, ...]) → bool
                // Merge the union of the source HyperLogLogs into dest. A unique
                // count across buckets must use this, not a sum of per-bucket
                // PFCOUNTs (which over-counts elements seen in multiple buckets).
                if args.len() < 2 {
                    return Err(ExecError::Unsupported(
                        "KV_PFMERGE requires a destination and at least one source key".into(),
                    ));
                }
                let dest = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let sources: Vec<String> = args[1..]
                    .iter()
                    .map(|a| match a {
                        Value::Text(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .collect();
                let source_refs: Vec<&str> = sources.iter().map(|s| s.as_str()).collect();
                // A merge can create the destination: same 16 KiB, not 64 bytes.
                if !self.memory_allocator.lock().request("kv", 1 << 14) {
                    return Err(ExecError::Unsupported(
                        "KV_PFMERGE: memory budget exceeded".into(),
                    ));
                }
                #[cfg(feature = "server")]
                let before_bytes = self.kv_key_bytes(&dest);
                let outcome = match self.kv_store.col_pfmerge(&dest, &source_refs) {
                    Ok(()) => Ok(Value::Bool(true)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                };
                #[cfg(feature = "server")]
                self.kv_reconcile(&dest, before_bytes);
                outcome
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
                // A repeated field name is a client error, not silent
                // last-wins (S31-15): on read the duplicate collapses and
                // the first value is unrecoverable, which is data loss
                // dressed as syntax. The read side still renders duplicates
                // deterministically, because entries written before this
                // check can carry them.
                {
                    let mut seen = HashSet::new();
                    for (field, _) in &fields {
                        if !seen.insert(field.as_str()) {
                            return Err(ExecError::Unsupported(format!(
                                "STREAM_XADD field names must be unique: '{field}' given more \
                                 than once"
                            )));
                        }
                    }
                }
                // Record the before-image before taking the write guard —
                // an aborted transaction must not leave the appended entry
                // behind for consumers that already read it. Returns the
                // coordinating id the WAL record carries (S63): the txn's
                // xid inside BEGIN/COMMIT, XACT_AUTOCOMMIT outside.
                let xact = self.cross_model_touch_stream(&stream_name);
                let mut streams = self.streams.write();
                let stream = streams.entry(stream_name.clone()).or_default();
                let id = stream.xadd(fields.clone());
                // Log to WAL after successful append. A failure here used to be
                // discarded with `let _ =` and not even logged (S31-13): the
                // client got an entry id back for a write whose durable record
                // had failed, and the first symptom was missing entries after a
                // restart, with nothing in the log to correlate. Fail the
                // statement instead, and undo the in-memory append so the two
                // agree — an acknowledged write that was never logged is the
                // defect class this engine keeps finding.
                if let Some(ref wal) = self.streams_wal
                    && let Err(e) = wal.log_xadd(Some(xact), &stream_name, &id, &fields)
                {
                    // The entry just appended is the last one; `last_id` stays
                    // advanced (ids may skip, which is legal and harmless).
                    // Entries this append evicted via MAXLEN are already gone —
                    // that is unavoidable, and the WAL error means the stream is
                    // degraded regardless.
                    stream.entries.retain(|e| e.id != id);
                    tracing::error!(
                        stream = %stream_name,
                        entry = %id,
                        error = %e,
                        "STREAM_XADD failed to write its WAL record; rejecting the write"
                    );
                    return Err(match e.kind() {
                        std::io::ErrorKind::StorageFull => ExecError::DiskFull(format!(
                            "STREAM_XADD could not log entry {id} for stream '{stream_name}': {e}"
                        )),
                        _ => ExecError::Storage(crate::storage::StorageError::Io(format!(
                            "STREAM_XADD could not log entry {id} for stream '{stream_name}': {e}"
                        ))),
                    });
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
                let len = streams.get(&stream_name).map(|s| s.xlen()).unwrap_or(0);
                Ok(Value::Int64(len as i64))
            }
            "STREAM_XRANGE" => {
                // stream_xrange(stream, start, end, count) → entries as text
                // Bounds are a bare millisecond or a full "<ms>-<seq>" id.
                require_args(fname, &args, 4)?;
                let stream_name = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let start_id = stream_cursor_arg(&args[1], 0, "STREAM_XRANGE start")?;
                let end_id = stream_cursor_arg(&args[2], u64::MAX, "STREAM_XRANGE end")?;
                let count = val_to_u64(&args[3], "STREAM_XRANGE count")? as usize;
                let streams = self.streams.read();
                match streams.get(&stream_name) {
                    Some(stream) => {
                        let entries = stream.xrange(&start_id, &end_id, Some(count));
                        Ok(Value::Text(stream_entries_to_json(&entries)))
                    }
                    None => Ok(Value::Text(String::new())),
                }
            }
            "STREAM_XREAD" => {
                // stream_xread(stream, last_id, count) → entries as text
                //
                // `last_id` is a bare millisecond or the full "<ms>-<seq>" id
                // STREAM_XADD returns — the same composition fix STREAM_XACK
                // carries below, and here it is not a convenience. A bare
                // millisecond can only mean "strictly after that millisecond",
                // so a consumer that read up to `<ms>-0` and polls again with
                // `<ms>` is never served `<ms>-1`: entries appended in the
                // millisecond it last read are unreachable, silently, forever.
                // Found by `probe_streams_oracle`.
                require_args(fname, &args, 3)?;
                let stream_name = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let last_id = stream_cursor_arg(&args[1], u64::MAX, "STREAM_XREAD last_id")?;
                let count = val_to_u64(&args[2], "STREAM_XREAD count")? as usize;
                let streams = self.streams.read();
                match streams.get(&stream_name) {
                    Some(stream) => {
                        let entries = stream.xread(&last_id, count);
                        Ok(Value::Text(stream_entries_to_json(&entries)))
                    }
                    None => Ok(Value::Text(String::new())),
                }
            }
            "STREAM_XGROUP_CREATE" => {
                // stream_xgroup_create(stream, group, start_id_ms [, recreate])
                //   → BOOLEAN
                if args.len() != 3 && args.len() != 4 {
                    return Err(ExecError::Unsupported(
                        "STREAM_XGROUP_CREATE requires (stream, group, start_id[, recreate])"
                            .into(),
                    ));
                }
                let stream_name = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let group = match &args[1] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let start_ms = val_to_u64(&args[2], "STREAM_XGROUP_CREATE start_id")?;
                // Resetting a live group is destructive and now has to be asked
                // for by name (S31-11). It used to be what a plain create did.
                let recreate = match args.get(3) {
                    None | Some(Value::Null) => false,
                    Some(Value::Bool(b)) => *b,
                    Some(other) => {
                        return Err(ExecError::Unsupported(format!(
                            "STREAM_XGROUP_CREATE recreate must be BOOLEAN, got {other:?}"
                        )));
                    }
                };
                let xact = self.cross_model_touch_stream(&stream_name);
                let start_id = crate::pubsub::StreamEntryId::new(start_ms, 0);
                let mut streams = self.streams.write();
                let stream = streams.entry(stream_name.clone()).or_default();
                // A plain create against an existing group is BUSYGROUP, which
                // is what Redis returns and what the sibling implementation in
                // `kv::streams` has always returned. It used to be an
                // unconditional overwrite that reset the cursor, dropped the
                // pending list and answered `true` (S31-11) — so a re-run
                // provisioning script silently redelivered the whole stream or
                // silently abandoned everything unacked, and consumer-group
                // state is persisted, so it destroyed durable state.
                let prior = stream.groups.get(&group).cloned();
                if recreate {
                    stream.xgroup_recreate(&group, start_id.clone());
                } else if let Err(e) = stream.xgroup_create(&group, start_id.clone()) {
                    return Err(ExecError::ConstraintViolation(e));
                }
                // Group state does not survive a restart unless it is logged
                // (S31-05): entries replay from the log, a cursor cannot be
                // reconstructed from anything. A failed append must fail the
                // statement for the same reason STREAM_XADD's does (S31-13):
                // otherwise the caller is told a group exists that a restart
                // will not produce.
                if let Some(ref wal) = self.streams_wal
                    && let Err(e) =
                        wal.log_xgroup_create(Some(xact), &stream_name, &group, &start_id)
                {
                    match prior {
                        Some(g) => {
                            stream.groups.insert(group.clone(), g);
                        }
                        None => {
                            stream.groups.remove(&group);
                        }
                    }
                    tracing::error!(
                        stream = %stream_name,
                        group = %group,
                        error = %e,
                        "STREAM_XGROUP_CREATE failed to write its WAL record; rejecting the create"
                    );
                    return Err(match e.kind() {
                        std::io::ErrorKind::StorageFull => ExecError::DiskFull(format!(
                            "STREAM_XGROUP_CREATE could not log group '{group}' on stream \
                             '{stream_name}': {e}"
                        )),
                        _ => ExecError::Storage(crate::storage::StorageError::Io(format!(
                            "STREAM_XGROUP_CREATE could not log group '{group}' on stream \
                             '{stream_name}': {e}"
                        ))),
                    });
                }
                // Contract (§3.9): BOOLEAN.
                Ok(Value::Bool(true))
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
                // A read that advances the group cursor and records a pending
                // entry is a write as far as rollback is concerned.
                let xact = self.cross_model_touch_stream(&stream_name);
                let mut streams = self.streams.write();
                // A read against a group that does not exist must NOT read as
                // an empty batch (S31-05). Empty is what "caught up" looks
                // like, so a consumer whose group vanished — dropped by a
                // restart before group state was logged, or never created —
                // concluded it had nothing to do and silently skipped every
                // entry it had not yet processed, forever. Redis answers
                // NOGROUP here and so does the RESP surface of this engine
                // (`kv/streams.rs`); the SQL surface now agrees.
                let group_exists = streams
                    .get(&stream_name)
                    .is_some_and(|s| s.groups.contains_key(&group));
                if !group_exists {
                    return Err(ExecError::Runtime(format!(
                        "NOGROUP No such consumer group '{group}' for stream '{stream_name}'"
                    )));
                }
                let stream = streams
                    .get_mut(&stream_name)
                    .expect("stream presence checked above");
                let was_known = stream.groups[&group].consumers.contains(&consumer);
                let prev_last = stream.groups[&group].last_delivered_id.clone();
                let entries = stream.xreadgroup(&group, &consumer, count);
                let delivered: Vec<crate::pubsub::StreamEntryId> =
                    entries.iter().map(|e| e.id.clone()).collect();
                let json = stream_entries_to_json(&entries);
                // Log the cursor advance and the pending-list additions. An
                // idle poll that delivers nothing and registers no new consumer
                // changes no state, so it is not logged — otherwise a polling
                // consumer would grow the log without bound. A failure to log
                // fails the statement (S31-13, the one arm that still
                // discarded it): the client would otherwise be told a cursor
                // advanced that a restart will not reproduce, which
                // redelivers — or silently skips — everything after it.
                if !delivered.is_empty() || !was_known {
                    let last = stream.groups[&group].last_delivered_id.clone();
                    if let Some(ref wal) = self.streams_wal
                        && let Err(e) = wal.log_xreadgroup(
                            Some(xact),
                            &stream_name,
                            &group,
                            &consumer,
                            &last,
                            &delivered,
                        )
                    {
                        // Undo the in-memory delivery so the statement's
                        // effects and its durable record agree.
                        if let Some(g) = stream.groups.get_mut(&group) {
                            g.last_delivered_id = prev_last;
                            if was_known {
                                if let Some(pel) = g.pending.get_mut(&consumer) {
                                    pel.retain(|id| !delivered.contains(id));
                                }
                            } else {
                                g.consumers.remove(&consumer);
                                g.pending.remove(&consumer);
                            }
                        }
                        tracing::error!(
                            stream = %stream_name,
                            group = %group,
                            consumer = %consumer,
                            error = %e,
                            "STREAM_XREADGROUP failed to write its WAL record; rejecting the read"
                        );
                        return Err(match e.kind() {
                            std::io::ErrorKind::StorageFull => ExecError::DiskFull(format!(
                                "STREAM_XREADGROUP could not log the delivery from '{group}' on \
                                 stream '{stream_name}': {e}"
                            )),
                            _ => ExecError::Storage(crate::storage::StorageError::Io(format!(
                                "STREAM_XREADGROUP could not log the delivery from '{group}' on \
                                 stream '{stream_name}': {e}"
                            ))),
                        });
                    }
                }
                Ok(Value::Text(json))
            }
            "STREAM_XACK" => {
                // stream_xack(stream, group, id_ms, id_seq) → count acknowledged
                // stream_xack(stream, group, '<ms>-<seq>') → the same
                //
                // The three-argument form exists because the four-argument one
                // does not compose with XADD, which returns the id as a single
                // "<ms>-<seq>" string. Every SDK had to split that string to
                // acknowledge an entry it had just added, and every SDK's live
                // conformance case for consumer groups was marked xfail with
                // the same note: "the two ends of the same API do not compose."
                //
                // Splitting it in five clients would have been five chances to
                // disagree about the separator, so the engine accepts the shape
                // its own XADD produces. The four-argument form is unchanged.
                require_args(fname, &args, 3)?;
                let stream_name = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let group = match &args[1] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let (id_ms, id_seq) = if args.len() >= 4 {
                    (
                        val_to_u64(&args[2], "STREAM_XACK id_ms")?,
                        val_to_u64(&args[3], "STREAM_XACK id_seq")?,
                    )
                } else {
                    let raw = match &args[2] {
                        Value::Text(s) => s.clone(),
                        other => other.to_string(),
                    };
                    // A bare "<ms>" is accepted as seq 0 — the same reading
                    // Redis gives an id with no sequence part.
                    match raw.split_once('-') {
                        Some((ms, seq)) => (
                            ms.trim().parse::<u64>().map_err(|_| {
                                ExecError::Unsupported(format!(
                                    "STREAM_XACK id {raw:?} is not <ms>-<seq>"
                                ))
                            })?,
                            seq.trim().parse::<u64>().map_err(|_| {
                                ExecError::Unsupported(format!(
                                    "STREAM_XACK id {raw:?} is not <ms>-<seq>"
                                ))
                            })?,
                        ),
                        None => (
                            raw.trim().parse::<u64>().map_err(|_| {
                                ExecError::Unsupported(format!(
                                    "STREAM_XACK id {raw:?} is not <ms>-<seq>"
                                ))
                            })?,
                            0,
                        ),
                    }
                };
                let xact = self.cross_model_touch_stream(&stream_name);
                let id = crate::pubsub::StreamEntryId::new(id_ms, id_seq);
                let mut streams = self.streams.write();
                match streams.get_mut(&stream_name) {
                    Some(stream) => {
                        // Collect the PEL owners BEFORE logging or removing
                        // (S31-15): the ack record must name whose pending
                        // lists held the entry, and the removal itself is
                        // what destroys that fact. Only a removal changes
                        // the pending list; an ack of an id that is not
                        // pending is a no-op and stays out of the log —
                        // without the record a restart would restore to the
                        // PEL an entry a consumer had already acknowledged.
                        let owners: Vec<(String, Vec<crate::pubsub::StreamEntryId>)> = stream
                            .groups
                            .get(&group)
                            .map(|g| {
                                g.pending
                                    .iter()
                                    .filter(|(_, pel)| pel.contains(&id))
                                    .map(|(consumer, _)| {
                                        (consumer.clone(), std::slice::from_ref(&id).to_vec())
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();
                        // Log before removing: on an append failure the
                        // statement fails and the PEL is untouched, because
                        // nothing has happened to undo yet. Acknowledging an
                        // ack no restart will reproduce is the same defect
                        // class S31-13 fixed for XADD/XGROUP_CREATE/
                        // XREADGROUP — this was the last arm to swallow it.
                        if !owners.is_empty()
                            && let Some(ref wal) = self.streams_wal
                            && let Err(e) =
                                wal.log_xack_owned(Some(xact), &stream_name, &group, &owners)
                        {
                            tracing::error!(
                                stream = %stream_name,
                                group = %group,
                                id = %id,
                                error = %e,
                                "STREAM_XACK failed to write its WAL record; rejecting the ack"
                            );
                            return Err(match e.kind() {
                                std::io::ErrorKind::StorageFull => ExecError::DiskFull(format!(
                                    "STREAM_XACK could not log the ack of {id} for group \
                                     '{group}' on stream '{stream_name}': {e}"
                                )),
                                _ => ExecError::Storage(crate::storage::StorageError::Io(format!(
                                    "STREAM_XACK could not log the ack of {id} for group \
                                     '{group}' on stream '{stream_name}': {e}"
                                ))),
                            });
                        }
                        let acked = stream.xack(&group, std::slice::from_ref(&id));
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
                let estimated = crate::columnar::segment::estimate_batch_size(&batch);
                if !self.memory_allocator.lock().request("columnar", estimated) {
                    return Err(ExecError::Unsupported(format!(
                        "COLUMNAR_INSERT: memory budget exceeded (need {} bytes for table '{}')",
                        estimated, table
                    )));
                }
                {
                    let mut store = self.columnar_store.write();
                    let xact = self.cross_model_before_columnar(&store);
                    store.append_with_dict_in_xact(&table, batch, xact);
                }
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
                    _ => {
                        return Err(ExecError::Unsupported(
                            "TS_INSERT value must be numeric".into(),
                        ));
                    }
                };
                {
                    let mut store = self.ts_store.write();
                    let xact = self.cross_model_before_ts(&store);
                    store.clear_touched();
                    store.set_xact_tag(xact);
                    store.insert(
                        &series,
                        crate::timeseries::DataPoint {
                            timestamp: ts,
                            tags: vec![],
                            value: val,
                        },
                    );
                    store.take_xact_tag();
                    let touched = store.take_touched();
                    drop(store);
                    self.cross_model_after_ts(touched);
                }
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
            "TS_RANGE" => {
                // ts_range(series, start_ms, end_ms) → JSON [{"t":ms,"v":value}]
                //
                // Raw point retrieval had no SQL surface at all, only the
                // aggregates, and every SDK answered that differently: Python
                // synthesised it from sixty bucketed TS_RANGE_AVG calls (sixty
                // round trips to read points the store already had, and wrong
                // wherever a bucket held more than one point), Go refused with
                // "raw point retrieval is not supported by the engine" — false
                // at the store level, true at the SQL surface it could see —
                // and TypeScript, Rust and Elixir simply had no method.
                //
                // Three answers to one question is a contract gap, and the
                // right place to close it is here rather than in five clients.
                require_args(fname, &args, 3)?;
                let series = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let start = val_to_u64(&args[1], "TS_RANGE start")?;
                let end = val_to_u64(&args[2], "TS_RANGE end")?;
                let store = self.ts_store.read();
                let points = store.query(&series, start, end);
                let items: Vec<serde_json::Value> = points
                    .iter()
                    .map(|p| serde_json::json!({ "t": p.timestamp, "v": p.value }))
                    .collect();
                Ok(Value::Text(serde_json::Value::Array(items).to_string()))
            }
            "TS_RETENTION" => {
                // ts_retention(max_age_ms) → 'OK' — sets global retention policy
                require_args(fname, &args, 1)?;
                let max_age = val_to_u64(&args[0], "TS_RETENTION max_age_ms")?;
                self.ts_store
                    .write()
                    .set_retention(crate::timeseries::RetentionPolicy {
                        max_age_ms: max_age,
                    });
                Ok(Value::Text("OK".into()))
            }

            // ================================================================
            // Document store functions (JSONB + GIN index via SQL)
            // ================================================================
            "DOC_INSERT" => {
                // doc_insert(json_text) → document ID
                // doc_insert(collection, json_text) → document ID
                //
                // The two-argument form places the document in a named
                // collection; the one-argument form uses the default (unnamed)
                // one, which is where every document written before collections
                // existed lives. Distinguishing on arity keeps every existing
                // caller working unchanged.
                if args.len() != 1 && args.len() != 2 {
                    return Err(ExecError::Unsupported(
                        "DOC_INSERT requires (json) or (collection, json)".into(),
                    ));
                }
                let (collection, json_arg) = if args.len() == 2 {
                    (doc_collection_arg(&args[0], "DOC_INSERT")?, &args[1])
                } else {
                    (String::new(), &args[0])
                };
                let json_text = match json_arg {
                    Value::Text(s) => s.clone(),
                    Value::Null => return Ok(Value::Null),
                    other => other.to_string(),
                };
                let jv = parse_json_to_doc(&json_text)
                    .map_err(|e| ExecError::Unsupported(format!("DOC_INSERT invalid JSON: {e}")))?;
                let id = {
                    let mut store = self.doc_store.write();
                    let xact = self.cross_model_before_doc(&store);
                    store.clear_touched();
                    let id = store
                        .insert_in_xact(&collection, jv, xact)
                        .map_err(|e| wal_failure_to_exec_error("DOC_INSERT", e))?;
                    let touched = store.take_touched();
                    drop(store);
                    self.cross_model_after_doc(touched);
                    id
                };
                Ok(Value::Int64(id as i64))
            }
            "DOC_UPDATE" => {
                // doc_update(id, json_text) → bool. Replaces the document in
                // place (preserving its id) when it exists; false otherwise.
                // doc_update(collection, id, json_text) → bool, scoped to a
                // collection: a document in another one is reported absent, so
                // one collection can never overwrite another's document.
                // The document store has no SQL-exposed mutation otherwise —
                // clients previously (and wrongly) tried `UPDATE documents`,
                // a relation that does not exist.
                if args.len() != 2 && args.len() != 3 {
                    return Err(ExecError::Unsupported(
                        "DOC_UPDATE requires (id, json) or (collection, id, json)".into(),
                    ));
                }
                let (collection, id_arg, json_arg) = if args.len() == 3 {
                    (
                        doc_collection_arg(&args[0], "DOC_UPDATE")?,
                        &args[1],
                        &args[2],
                    )
                } else {
                    (String::new(), &args[0], &args[1])
                };
                let id = val_to_u64(id_arg, "DOC_UPDATE id")?;
                let json_text = match json_arg {
                    Value::Text(s) => s.clone(),
                    Value::Null => return Ok(Value::Bool(false)),
                    other => other.to_string(),
                };
                let jv = parse_json_to_doc(&json_text)
                    .map_err(|e| ExecError::Unsupported(format!("DOC_UPDATE invalid JSON: {e}")))?;
                let mut store = self.doc_store.write();
                if store.get_in(&collection, id).is_none() {
                    return Ok(Value::Bool(false));
                }
                let xact = self.cross_model_before_doc(&store);
                store.clear_touched();
                store
                    .insert_with_id_in_xact(id, &collection, jv, xact)
                    .map_err(|e| wal_failure_to_exec_error("DOC_UPDATE", e))?;
                let touched = store.take_touched();
                drop(store);
                self.cross_model_after_doc(touched);
                Ok(Value::Bool(true))
            }
            "DOC_DELETE" => {
                // doc_delete(id) → bool (true if the document existed).
                // doc_delete(collection, id) → bool, scoped to a collection.
                if args.is_empty() || args.len() > 2 {
                    return Err(ExecError::Unsupported(
                        "DOC_DELETE requires (id) or (collection, id)".into(),
                    ));
                }
                let (collection, id_arg) = if args.len() == 2 {
                    (doc_collection_arg(&args[0], "DOC_DELETE")?, &args[1])
                } else {
                    (String::new(), &args[0])
                };
                let id = val_to_u64(id_arg, "DOC_DELETE id")?;
                let mut store = self.doc_store.write();
                let xact = self.cross_model_before_doc(&store);
                store.clear_touched();
                let removed = store
                    .delete_in_xact(&collection, id, xact)
                    .map_err(|e| wal_failure_to_exec_error("DOC_DELETE", e))?;
                let touched = store.take_touched();
                drop(store);
                self.cross_model_after_doc(touched);
                Ok(Value::Bool(removed))
            }
            "DOC_GET" => {
                // doc_get(id) → JSON text or NULL
                // doc_get(collection, id) → JSON text or NULL, scoped: a
                // document in another collection reads as absent.
                if args.is_empty() || args.len() > 2 {
                    return Err(ExecError::Unsupported(
                        "DOC_GET requires (id) or (collection, id)".into(),
                    ));
                }
                let (collection, id_arg) = if args.len() == 2 {
                    (doc_collection_arg(&args[0], "DOC_GET")?, &args[1])
                } else {
                    (String::new(), &args[0])
                };
                let id = val_to_u64(id_arg, "DOC_GET id")?;
                let store = self.doc_store.read();
                match store.get_in(&collection, id) {
                    Some(jv) => Ok(Value::Text(jv.to_json_string())),
                    None => Ok(Value::Null),
                }
            }
            "DOC_QUERY" => {
                // doc_query(json_query) → comma-separated IDs of matching docs (@> containment)
                // doc_query(collection, json_query) → the same, restricted to
                // one collection.
                if args.is_empty() || args.len() > 2 {
                    return Err(ExecError::Unsupported(
                        "DOC_QUERY requires (json) or (collection, json)".into(),
                    ));
                }
                let (collection, json_arg) = if args.len() == 2 {
                    (doc_collection_arg(&args[0], "DOC_QUERY")?, &args[1])
                } else {
                    (String::new(), &args[0])
                };
                let json_text = match json_arg {
                    Value::Text(s) => s.clone(),
                    Value::Null => return Ok(Value::Null),
                    other => other.to_string(),
                };
                let query = parse_json_to_doc(&json_text)
                    .map_err(|e| ExecError::Unsupported(format!("DOC_QUERY invalid JSON: {e}")))?;
                let store = self.doc_store.read();
                let mut ids = store.query_contains_in(&collection, &query);
                ids.sort();
                let id_strs: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
                Ok(Value::Text(id_strs.join(",")))
            }
            "DOC_PATH" | "DOC_PATH_IN" => {
                // doc_path(id, path_key1, path_key2, ...) → JSON value at path, or NULL
                // doc_path_in(collection, id, key1, ...) → the same, scoped.
                //
                // A separate NAME rather than the arity overload the other
                // DOC_* functions use, because the path tail is variadic:
                // `DOC_PATH('users', 1, 'name')` and `DOC_PATH(1, 'a', 'b')`
                // have the same shape, and over pgwire an id can arrive as
                // text, so no rule could tell a collection from an id without
                // guessing. Guessing wrong would read another collection's
                // document, which is the whole thing this must not do.
                let scoped = fname == "DOC_PATH_IN";
                let min = if scoped { 3 } else { 2 };
                if args.len() < min {
                    return Err(ExecError::Unsupported(
                        if scoped {
                            "DOC_PATH_IN requires (collection, id, key1, key2, ...)"
                        } else {
                            "DOC_PATH requires (id, key1, key2, ...)"
                        }
                        .into(),
                    ));
                }
                let (collection, rest) = if scoped {
                    (doc_collection_arg(&args[0], "DOC_PATH_IN")?, &args[1..])
                } else {
                    (String::new(), &args[..])
                };
                let id = val_to_u64(&rest[0], "DOC_PATH id")?;
                let path: Vec<String> = rest[1..]
                    .iter()
                    .map(|a| match a {
                        Value::Text(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .collect();
                let path_refs: Vec<&str> = path.iter().map(|s| s.as_str()).collect();
                let store = self.doc_store.read();
                match store.get_in(&collection, id) {
                    Some(doc) => match doc.get_path(&path_refs) {
                        Some(val) => Ok(Value::Text(val.to_json_string())),
                        None => Ok(Value::Null),
                    },
                    None => Ok(Value::Null),
                }
            }
            "DOC_COUNT" => {
                // doc_count() → documents in the default collection
                // doc_count(collection) → documents in that collection
                //
                // Note the zero-argument form counts the DEFAULT collection,
                // not every document: a count that silently spanned collections
                // would report other tenants' rows to a caller that cannot read
                // them.
                if args.len() > 1 {
                    return Err(ExecError::Unsupported(
                        "DOC_COUNT requires () or (collection)".into(),
                    ));
                }
                let collection = if args.len() == 1 {
                    doc_collection_arg(&args[0], "DOC_COUNT")?
                } else {
                    String::new()
                };
                let count = self.doc_store.read().len_in(&collection);
                Ok(Value::Int64(count as i64))
            }

            // ── Full-text search (FTS) functions ─────────────────────
            "FTS_INDEX" => {
                // fts_index(doc_id, text) → true
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported(
                        "FTS_INDEX requires (doc_id, text)".into(),
                    ));
                }
                let doc_id = val_to_u64(&args[0], "FTS_INDEX doc_id")?;
                let text = match &args[1] {
                    Value::Text(s) => s.clone(),
                    _ => {
                        return Err(ExecError::Unsupported(
                            "FTS_INDEX: text must be a string".into(),
                        ));
                    }
                };
                let text_len = text.len();
                let estimated = text_len + 64;
                if !self.memory_allocator.lock().request("fts", estimated) {
                    return Err(ExecError::Unsupported(format!(
                        "FTS_INDEX: memory budget exceeded (need {} bytes for doc {})",
                        estimated, doc_id
                    )));
                }
                // No checkpoint here: the write is durable through the FTS
                // WAL (fsynced at the commit boundary since NU-006), and
                // `fts_index.json` is a periodic CHECKPOINT rather than the
                // store (NU-014). Rewriting the whole serialized index on
                // every FTS_INDEX was O(index) per write and is what made the
                // JSON the only durable copy in the first place.
                self.fts_index.write().add_document(doc_id, &text);
                // Record mutation for potential rollback
                self.cross_model_fts_added(doc_id);
                Ok(Value::Bool(true))
            }
            "FTS_INDEX_FACETED" => {
                // fts_index_faceted(doc_id, text, facet_field, facet_value) → true
                // Index a document tagged with a single facet (e.g. site_id) so
                // FTS_SEARCH_FILTER can scope BM25 ranking to that partition.
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 4 {
                    return Err(ExecError::Unsupported(
                        "FTS_INDEX_FACETED requires (doc_id, text, facet_field, facet_value)"
                            .into(),
                    ));
                }
                let doc_id = val_to_u64(&args[0], "FTS_INDEX_FACETED doc_id")?;
                let text = match &args[1] {
                    Value::Text(s) => s.clone(),
                    _ => {
                        return Err(ExecError::Unsupported(
                            "FTS_INDEX_FACETED: text must be a string".into(),
                        ));
                    }
                };
                let field = match &args[2] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let value = match &args[3] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let estimated = text.len() + field.len() + value.len() + 96;
                if !self.memory_allocator.lock().request("fts", estimated) {
                    return Err(ExecError::Unsupported(format!(
                        "FTS_INDEX_FACETED: memory budget exceeded (need {estimated} bytes for doc {doc_id})"
                    )));
                }
                let mut facets = std::collections::HashMap::new();
                facets.insert(field, vec![value]);
                self.fts_index
                    .write()
                    .add_document_with_facets(doc_id, &text, facets);
                self.cross_model_fts_added(doc_id);
                Ok(Value::Bool(true))
            }
            "FTS_REMOVE" => {
                // fts_remove(doc_id) → true
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported(
                        "FTS_REMOVE requires (doc_id)".into(),
                    ));
                }
                let doc_id = val_to_u64(&args[0], "FTS_REMOVE doc_id")?;
                // Capture state before removal for potential rollback
                self.cross_model_fts_removing(doc_id);
                self.fts_index.write().remove_document(doc_id);
                self.memory_allocator.lock().release("fts", 64);
                Ok(Value::Bool(true))
            }
            "FTS_SEARCH" => {
                // fts_search(query, limit) → JSON array of [{doc_id, score}]
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported(
                        "FTS_SEARCH requires (query, limit)".into(),
                    ));
                }
                let query = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => {
                        return Err(ExecError::Unsupported(
                            "FTS_SEARCH: query must be a string".into(),
                        ));
                    }
                };
                let limit = (val_to_u64(&args[1], "FTS_SEARCH limit")? as usize).min(10_000);
                let results = self.fts_index.read().search(&query, limit);
                let json = results
                    .iter()
                    .map(|(id, score)| format!(r#"{{"doc_id":{id},"score":{score:.6}}}"#))
                    .collect::<Vec<_>>()
                    .join(",");
                Ok(Value::Text(format!("[{json}]")))
            }
            "FTS_SEARCH_FILTER" => {
                // fts_search_filter(query, limit, facet_field, facet_value)
                //   → JSON array of [{doc_id, score}], scoped to documents whose
                //   facet `field` contains `value` (e.g. site_id), so one busy
                //   partition's hits don't crowd out the rest.
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 4 {
                    return Err(ExecError::Unsupported(
                        "FTS_SEARCH_FILTER requires (query, limit, facet_field, facet_value)"
                            .into(),
                    ));
                }
                let query = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => {
                        return Err(ExecError::Unsupported(
                            "FTS_SEARCH_FILTER: query must be a string".into(),
                        ));
                    }
                };
                let limit = (val_to_u64(&args[1], "FTS_SEARCH_FILTER limit")? as usize).min(10_000);
                let field = match &args[2] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let value = match &args[3] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let results = self
                    .fts_index
                    .read()
                    .search_filtered(&query, limit, &field, &value);
                let json = results
                    .iter()
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
                    _ => {
                        return Err(ExecError::Unsupported(
                            "FTS_FUZZY_SEARCH: query must be a string".into(),
                        ));
                    }
                };
                let max_dist_raw = val_to_u64(&args[1], "FTS_FUZZY_SEARCH max_distance")? as usize;
                let max_dist = max_dist_raw.min(3); // Cap at 3 to prevent combinatorial explosion
                let limit = (val_to_u64(&args[2], "FTS_FUZZY_SEARCH limit")? as usize).min(10_000);
                let idx = self.fts_index.read();
                // Tokenize query, expand each term via fuzzy matching, collect all matching doc scores
                let query_tokens = fts::tokenize(&query);
                let mut scores: std::collections::HashMap<u64, f64> =
                    std::collections::HashMap::new();
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
                let json = results
                    .iter()
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
                    return Err(ExecError::Unsupported(
                        "FTS_MATCH requires (doc_id, query)".into(),
                    ));
                }
                let doc_id = val_to_u64(&args[0], "FTS_MATCH doc_id")?;
                let query = match &args[1] {
                    Value::Text(s) => s.clone(),
                    _ => {
                        return Err(ExecError::Unsupported(
                            "FTS_MATCH: query must be a string".into(),
                        ));
                    }
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
                    return Err(ExecError::Unsupported(
                        "BLOB_STORE requires (key, data_hex [, content_type])".into(),
                    ));
                }
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => {
                        return Err(ExecError::Unsupported(
                            "BLOB_STORE: key must be a string".into(),
                        ));
                    }
                };
                let data_hex = match &args[1] {
                    Value::Text(s) => s.clone(),
                    _ => {
                        return Err(ExecError::Unsupported(
                            "BLOB_STORE: data must be a hex string".into(),
                        ));
                    }
                };
                // Validate size (100 MB max via SQL function; direct API has no limit)
                if data_hex.len() > 200_000_000 {
                    return Err(ExecError::Unsupported(
                        "BLOB_STORE: data exceeds 100 MB limit".into(),
                    ));
                }
                // Decode hex → bytes
                let data = hex_decode(&data_hex)
                    .map_err(|e| ExecError::Unsupported(format!("BLOB_STORE: {e}")))?;
                let content_type = if args.len() > 2 {
                    match &args[2] {
                        Value::Text(s) => Some(s.clone()),
                        _ => None,
                    }
                } else {
                    None
                };
                {
                    let mut store = self.blob_store.write();
                    self.cross_model_before_blob(&store);
                    store.clear_touched();
                    store.put(&key, &data, content_type.as_deref());
                    let touched = store.take_touched();
                    drop(store);
                    self.cross_model_after_blob(touched);
                }
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
                    _ => {
                        return Err(ExecError::Unsupported(
                            "BLOB_GET: key must be a string".into(),
                        ));
                    }
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
                    _ => {
                        return Err(ExecError::Unsupported(
                            "BLOB_DELETE: key must be a string".into(),
                        ));
                    }
                };
                let mut store = self.blob_store.write();
                self.cross_model_before_blob(&store);
                store.clear_touched();
                let removed = store.delete(&key);
                let touched = store.take_touched();
                drop(store);
                self.cross_model_after_blob(touched);
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
                    _ => {
                        return Err(ExecError::Unsupported(
                            "BLOB_META: key must be a string".into(),
                        ));
                    }
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
                    return Err(ExecError::Unsupported(
                        "BLOB_TAG requires (key, tag_key, tag_value)".into(),
                    ));
                }
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => {
                        return Err(ExecError::Unsupported(
                            "BLOB_TAG: key must be a string".into(),
                        ));
                    }
                };
                let tag_key = match &args[1] {
                    Value::Text(s) => s.clone(),
                    _ => {
                        return Err(ExecError::Unsupported(
                            "BLOB_TAG: tag_key must be a string".into(),
                        ));
                    }
                };
                let tag_val = match &args[2] {
                    Value::Text(s) => s.clone(),
                    _ => {
                        return Err(ExecError::Unsupported(
                            "BLOB_TAG: tag_value must be a string".into(),
                        ));
                    }
                };
                let mut store = self.blob_store.write();
                self.cross_model_before_blob(&store);
                store.clear_touched();
                let ok = store.set_tag(&key, &tag_key, &tag_val);
                let touched = store.take_touched();
                drop(store);
                self.cross_model_after_blob(touched);
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
                let json = keys
                    .iter()
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
                    return Err(ExecError::Unsupported(
                        "GRAPH_QUERY requires (cypher_text)".into(),
                    ));
                }
                let cypher = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => {
                        return Err(ExecError::Unsupported(
                            "GRAPH_QUERY: cypher must be a string".into(),
                        ));
                    }
                };
                let stmt = parse_cypher(&cypher).map_err(|e| {
                    ExecError::Unsupported(format!("GRAPH_QUERY parse error: {e:?}"))
                })?;
                let result = {
                    let mut gs = self.graph_store.write();
                    let xact = self.cross_model_before_graph(&gs);
                    gs.clear_touched();
                    gs.set_xact_tag(xact);
                    let outcome = execute_cypher(&mut gs, &stmt).map_err(|e| {
                        ExecError::Unsupported(format!("GRAPH_QUERY exec error: {e:?}"))
                    });
                    let touched = gs.take_touched();
                    drop(gs);
                    self.cross_model_after_graph(touched);
                    outcome?
                };
                // Serialize result to JSON
                let cols_json = result
                    .columns
                    .iter()
                    .map(|c| format!(r#""{}""#, json_escape(c)))
                    .collect::<Vec<_>>()
                    .join(",");
                let rows_json = result
                    .rows
                    .iter()
                    .map(|row_vals| {
                        let vals = row_vals
                            .iter()
                            .map(prop_value_to_json)
                            .collect::<Vec<_>>()
                            .join(",");
                        format!("[{vals}]")
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                Ok(Value::Text(format!(
                    r#"{{"columns":[{cols_json}],"rows":[{rows_json}]}}"#
                )))
            }
            "GRAPH_ADD_NODE" => {
                // graph_add_node(label, properties_json?) → node_id
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported(
                        "GRAPH_ADD_NODE requires (label [, properties_json])".into(),
                    ));
                }
                let label = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => {
                        return Err(ExecError::Unsupported(
                            "GRAPH_ADD_NODE: label must be a string".into(),
                        ));
                    }
                };
                let props = if args.len() > 1 {
                    match &args[1] {
                        Value::Text(s) => parse_json_to_graph_props(s)?,
                        _ => std::collections::BTreeMap::new(),
                    }
                } else {
                    std::collections::BTreeMap::new()
                };
                let id = {
                    let mut gs = self.graph_store.write();
                    let xact = self.cross_model_before_graph(&gs);
                    gs.clear_touched();
                    gs.set_xact_tag(xact);
                    let id = gs.create_node(vec![label], props);
                    let touched = gs.take_touched();
                    drop(gs);
                    self.cross_model_after_graph(touched);
                    id
                };
                Ok(Value::Int64(id as i64))
            }
            "GRAPH_ADD_EDGE" => {
                // graph_add_edge(from_id, to_id, edge_type, properties_json?) → edge_id or NULL
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 3 {
                    return Err(ExecError::Unsupported(
                        "GRAPH_ADD_EDGE requires (from_id, to_id, edge_type [, properties_json])"
                            .into(),
                    ));
                }
                let from = val_to_u64(&args[0], "GRAPH_ADD_EDGE from_id")?;
                let to = val_to_u64(&args[1], "GRAPH_ADD_EDGE to_id")?;
                let edge_type = match &args[2] {
                    Value::Text(s) => s.clone(),
                    _ => {
                        return Err(ExecError::Unsupported(
                            "GRAPH_ADD_EDGE: edge_type must be a string".into(),
                        ));
                    }
                };
                let props = if args.len() > 3 {
                    match &args[3] {
                        Value::Text(s) => parse_json_to_graph_props(s)?,
                        _ => std::collections::BTreeMap::new(),
                    }
                } else {
                    std::collections::BTreeMap::new()
                };
                let created = {
                    let mut gs = self.graph_store.write();
                    let xact = self.cross_model_before_graph(&gs);
                    gs.clear_touched();
                    gs.set_xact_tag(xact);
                    let created = gs.create_edge(from, to, edge_type, props);
                    let touched = gs.take_touched();
                    drop(gs);
                    self.cross_model_after_graph(touched);
                    created
                };
                match created {
                    Some(eid) => Ok(Value::Int64(eid as i64)),
                    None => Ok(Value::Null),
                }
            }
            "GRAPH_DELETE_NODE" => {
                // graph_delete_node(node_id) → true/false
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported(
                        "GRAPH_DELETE_NODE requires (node_id)".into(),
                    ));
                }
                let id = val_to_u64(&args[0], "GRAPH_DELETE_NODE")?;
                let mut gs = self.graph_store.write();
                let xact = self.cross_model_before_graph(&gs);
                gs.clear_touched();
                gs.set_xact_tag(xact);
                let removed = gs.delete_node(id);
                let touched = gs.take_touched();
                drop(gs);
                self.cross_model_after_graph(touched);
                Ok(Value::Bool(removed))
            }
            "GRAPH_DELETE_EDGE" => {
                // graph_delete_edge(edge_id) → true/false
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported(
                        "GRAPH_DELETE_EDGE requires (edge_id)".into(),
                    ));
                }
                let id = val_to_u64(&args[0], "GRAPH_DELETE_EDGE")?;
                let mut gs = self.graph_store.write();
                let xact = self.cross_model_before_graph(&gs);
                gs.clear_touched();
                gs.set_xact_tag(xact);
                let removed = gs.delete_edge(id);
                let touched = gs.take_touched();
                drop(gs);
                self.cross_model_after_graph(touched);
                Ok(Value::Bool(removed))
            }
            "GRAPH_NEIGHBORS" => {
                // graph_neighbors(node_id, direction?) → JSON array of {neighbor_id, edge_id, edge_type}
                // direction: 'out' (default), 'in', 'both'
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported(
                        "GRAPH_NEIGHBORS requires (node_id [, direction])".into(),
                    ));
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
                let json = neighbors
                    .iter()
                    .map(|(nid, edge)| {
                        format!(
                            r#"{{"neighbor_id":{},"edge_id":{},"edge_type":"{}"}}"#,
                            nid,
                            edge.id,
                            json_escape(&edge.edge_type)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                Ok(Value::Text(format!("[{json}]")))
            }
            "GRAPH_SHORTEST_PATH" => {
                // graph_shortest_path(from_id, to_id) → JSON array of node IDs or NULL
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported(
                        "GRAPH_SHORTEST_PATH requires (from_id, to_id)".into(),
                    ));
                }
                let from = val_to_u64(&args[0], "GRAPH_SHORTEST_PATH from_id")?;
                let to = val_to_u64(&args[1], "GRAPH_SHORTEST_PATH to_id")?;
                let store = self.graph_store.read();
                match store.shortest_path(from, to, crate::graph::Direction::Outgoing, None) {
                    Some(path) => {
                        let json = path
                            .iter()
                            .map(|id| id.to_string())
                            .collect::<Vec<_>>()
                            .join(",");
                        Ok(Value::Text(format!("[{json}]")))
                    }
                    None => Ok(Value::Null),
                }
            }
            "GRAPH_NODE_COUNT" => Ok(Value::Int64(self.graph_store.read().node_count() as i64)),
            "GRAPH_EDGE_COUNT" => Ok(Value::Int64(self.graph_store.read().edge_count() as i64)),

            // ── Reactive / CDC functions ─────────────────────────────
            #[cfg(feature = "server")]
            "SUBSCRIBE" => {
                // subscribe(query_text, table1 [, table2, ...]) → subscription_id
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported(
                        "SUBSCRIBE requires (query_text, table1, ...)".into(),
                    ));
                }
                let query_text = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => {
                        return Err(ExecError::Unsupported(
                            "SUBSCRIBE: query_text must be a string".into(),
                        ));
                    }
                };
                let tables: Vec<String> = args[1..]
                    .iter()
                    .filter_map(|v| match v {
                        Value::Text(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect();
                let (sub_id, _rx) = self
                    .subscription_manager
                    .write()
                    .subscribe(&query_text, tables);
                Ok(Value::Int64(sub_id as i64))
            }
            #[cfg(feature = "server")]
            "UNSUBSCRIBE" => {
                // unsubscribe(subscription_id) → true/false
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported(
                        "UNSUBSCRIBE requires (subscription_id)".into(),
                    ));
                }
                let id = val_to_u64(&args[0], "UNSUBSCRIBE")?;
                Ok(Value::Bool(
                    self.subscription_manager.write().unsubscribe(id),
                ))
            }
            #[cfg(feature = "server")]
            "SUBSCRIPTION_COUNT" => Ok(Value::Int64(
                self.subscription_manager.read().active_count() as i64,
            )),
            #[cfg(feature = "server")]
            "CDC_READ" => {
                // cdc_read(after_sequence, limit) → JSON array of log entries
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported(
                        "CDC_READ requires (after_sequence, limit)".into(),
                    ));
                }
                let after_seq = val_to_u64(&args[0], "CDC_READ after_sequence")?;
                let limit = (val_to_u64(&args[1], "CDC_READ limit")? as usize).min(100_000);
                let log = self.cdc_log.read();
                let entries = log.read_from(after_seq, limit);
                let json = entries
                    .iter()
                    .map(|e| {
                        let change = match e.change_type {
                            ChangeType::Insert => "INSERT",
                            ChangeType::Update => "UPDATE",
                            ChangeType::Delete => "DELETE",
                        };
                        format!(
                            r#"{{"seq":{},"table":"{}","change":"{}","ts":{}}}"#,
                            e.sequence,
                            json_escape(&e.table),
                            change,
                            e.timestamp
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
                    return Err(ExecError::Unsupported(
                        "CDC_TABLE_READ requires (table, after_sequence, limit)".into(),
                    ));
                }
                let table = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => {
                        return Err(ExecError::Unsupported(
                            "CDC_TABLE_READ: table must be a string".into(),
                        ));
                    }
                };
                let after_seq = val_to_u64(&args[1], "CDC_TABLE_READ after_sequence")?;
                let limit = (val_to_u64(&args[2], "CDC_TABLE_READ limit")? as usize).min(100_000);
                let log = self.cdc_log.read();
                let entries = log.read_table_from(&table, after_seq, limit);
                let json = entries
                    .iter()
                    .map(|e| {
                        let change = match e.change_type {
                            ChangeType::Insert => "INSERT",
                            ChangeType::Update => "UPDATE",
                            ChangeType::Delete => "DELETE",
                        };
                        format!(
                            r#"{{"seq":{},"table":"{}","change":"{}","ts":{}}}"#,
                            e.sequence,
                            json_escape(&e.table),
                            change,
                            e.timestamp
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                Ok(Value::Text(format!("[{json}]")))
            }
            #[cfg(feature = "server")]
            "CDC_COUNT" => Ok(Value::Int64(self.cdc_log.read().len() as i64)),

            // ── Datalog functions ──────────────────────────────────────
            "DATALOG_ASSERT" => {
                let args = self.extract_fn_args(func, row, col_meta)?;
                require_args(fname, &args, 1)?;
                let input = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let (result, xact) = {
                    let mut store = self.datalog_store.write();
                    let xact = self.cross_model_before_datalog(&store);
                    store.clear_touched();
                    let result = store.sql_assert(&input);
                    let (touched, rules) = store.take_touched();
                    drop(store);
                    self.cross_model_after_datalog(touched, rules);
                    (result, xact)
                };
                match result {
                    Ok(msg) => {
                        // Append to the Datalog WAL, tagged with the
                        // coordinating transaction id (S63). Startup opens
                        // this WAL and restores from it, but nothing ever
                        // wrote to it, so the model looked durable in
                        // review, its direct WAL tests passed, and every
                        // fact asserted through SQL vanished on restart.
                        // Failing the statement is the point: a silent
                        // append failure is the same defect one layer down.
                        // (NU-013)
                        self.log_datalog(|wal| wal.log_assert(Some(xact), &input))?;
                        Ok(Value::Text(msg))
                    }
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
                let (result, xact) = {
                    let mut store = self.datalog_store.write();
                    let xact = self.cross_model_before_datalog(&store);
                    store.clear_touched();
                    let result = store.sql_rule(&input);
                    let (touched, rules) = store.take_touched();
                    drop(store);
                    self.cross_model_after_datalog(touched, rules);
                    (result, xact)
                };
                match result {
                    Ok(msg) => {
                        // Append to the Datalog WAL, tagged with the
                        // coordinating transaction id (S63); see the
                        // DATALOG_ASSERT arm for the NU-013 history.
                        self.log_datalog(|wal| wal.log_rule(Some(xact), &input))?;
                        Ok(Value::Text(msg))
                    }
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
                let (result, xact) = {
                    let mut store = self.datalog_store.write();
                    let xact = self.cross_model_before_datalog(&store);
                    store.clear_touched();
                    let result = store.sql_retract(&input);
                    let (touched, rules) = store.take_touched();
                    drop(store);
                    self.cross_model_after_datalog(touched, rules);
                    (result, xact)
                };
                match result {
                    Ok(msg) => {
                        // Append to the Datalog WAL, tagged with the
                        // coordinating transaction id (S63); see the
                        // DATALOG_ASSERT arm for the NU-013 history.
                        self.log_datalog(|wal| wal.log_retract(Some(xact), &input))?;
                        Ok(Value::Text(msg))
                    }
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
                let (result, xact) = {
                    let mut store = self.datalog_store.write();
                    let xact = self.cross_model_before_datalog(&store);
                    store.clear_touched();
                    let result = store.sql_clear(&pred);
                    let (touched, rules) = store.take_touched();
                    drop(store);
                    self.cross_model_after_datalog(touched, rules);
                    (result, xact)
                };
                match result {
                    Ok(msg) => {
                        // Append to the Datalog WAL, tagged with the
                        // coordinating transaction id (S63); see the
                        // DATALOG_ASSERT arm for the NU-013 history.
                        self.log_datalog(|wal| wal.log_clear(Some(xact), &pred))?;
                        Ok(Value::Text(msg))
                    }
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
                    .map(|row| {
                        row.into_iter()
                            .map(|v| match v {
                                Value::Null => "null".to_string(),
                                Value::Text(s) => s,
                                other => other.to_string(),
                            })
                            .collect()
                    })
                    .collect();
                let count = string_rows.len();
                self.datalog_store
                    .write()
                    .import_rows(&predicate, string_rows);
                Ok(Value::Text(format!(
                    "IMPORTED {count} rows into {predicate}"
                )))
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
                let edge_rows: Vec<Vec<String>> = gs
                    .all_edges()
                    .iter()
                    .map(|e| vec![e.from.to_string(), e.edge_type.clone(), e.to.to_string()])
                    .collect();
                drop(gs);
                let count = edge_rows.len();
                self.datalog_store
                    .write()
                    .import_rows(&predicate, edge_rows);
                Ok(Value::Text(format!(
                    "IMPORTED {count} edges into {predicate}"
                )))
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
                let node_rows: Vec<Vec<String>> = gs
                    .all_nodes()
                    .iter()
                    .flat_map(|n| {
                        if n.labels.is_empty() {
                            vec![vec![n.id.to_string(), String::new()]]
                        } else {
                            n.labels
                                .iter()
                                .map(|l| vec![n.id.to_string(), l.clone()])
                                .collect()
                        }
                    })
                    .collect();
                drop(gs);
                let count = node_rows.len();
                self.datalog_store
                    .write()
                    .import_rows(&predicate, node_rows);
                Ok(Value::Text(format!(
                    "IMPORTED {count} node-label pairs into {predicate}"
                )))
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
                    return Err(ExecError::Unsupported(
                        "embed() requires 2 arguments: embed(model, text)".into(),
                    ));
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
                                let vec_str = format!(
                                    "[{}]",
                                    output
                                        .iter()
                                        .map(|v| format!("{v:.6}"))
                                        .collect::<Vec<_>>()
                                        .join(",")
                                );
                                return Ok(Value::Text(vec_str));
                            }
                            Err(e) => {
                                return Err(ExecError::Unsupported(format!(
                                    "embed ONNX error: {e}"
                                )));
                            }
                        }
                    }
                }

                // Fallback: built-in bag-of-words EmbeddingGenerator
                let mut emb_gen = crate::inference::EmbeddingGenerator::new();
                emb_gen.build_vocabulary(&[&text]);
                let vec = emb_gen.embed(&text);
                let vec_str = format!(
                    "[{}]",
                    vec.iter()
                        .map(|v| format!("{v:.6}"))
                        .collect::<Vec<_>>()
                        .join(",")
                );
                Ok(Value::Text(vec_str))
            }
            "CLASSIFY" => {
                // classify(model_name, input_values...) → TEXT (class label)
                if args.len() < 2 {
                    return Err(ExecError::Unsupported(
                        "classify() requires at least 2 arguments: classify(model, input...)"
                            .into(),
                    ));
                }
                let model_name = args[0].to_string().replace('\'', "");
                let input: Vec<f32> = args[1..]
                    .iter()
                    .filter_map(|v| match v {
                        Value::Float64(f) => Some(*f as f32),
                        Value::Int32(i) => Some(*i as f32),
                        Value::Int64(i) => Some(*i as f32),
                        _ => v.to_string().parse::<f32>().ok(),
                    })
                    .collect();
                let registry = self.model_registry.read();
                match registry.predict(&model_name, &input) {
                    Ok(probs) => {
                        // Return the index of the highest probability as the class
                        let class_idx = probs
                            .iter()
                            .enumerate()
                            .max_by(|(_, a), (_, b)| {
                                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                            })
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
                    return Err(ExecError::Unsupported(
                        "predict() requires at least 2 arguments: predict(model, input...)".into(),
                    ));
                }
                let model_name = args[0].to_string().replace('\'', "");
                let input: Vec<f32> = args[1..]
                    .iter()
                    .filter_map(|v| match v {
                        Value::Float64(f) => Some(*f as f32),
                        Value::Int32(i) => Some(*i as f32),
                        Value::Int64(i) => Some(*i as f32),
                        _ => v.to_string().parse::<f32>().ok(),
                    })
                    .collect();
                let registry = self.model_registry.read();
                match registry.predict(&model_name, &input) {
                    Ok(output) => {
                        let vec_str = format!(
                            "[{}]",
                            output
                                .iter()
                                .map(|v| format!("{v:.6}"))
                                .collect::<Vec<_>>()
                                .join(",")
                        );
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
                    return Err(ExecError::Unsupported(
                        "tensor_store requires (name, version, shape_json[, dtype[, hex_data]])"
                            .into(),
                    ));
                }
                let name = args[0].to_string().replace('\'', "");
                let version = args[1].to_string().replace('\'', "");
                let shape_json = args[2].to_string().replace('\'', "");
                let shape: Vec<usize> =
                    serde_json::from_str::<Vec<usize>>(&shape_json).map_err(|e| {
                        ExecError::Unsupported(format!("tensor_store: invalid shape JSON: {e}"))
                    })?;
                let dtype_str = args
                    .get(3)
                    .map(|v| v.to_string().replace('\'', "").to_lowercase())
                    .unwrap_or_else(|| "float32".to_string());
                let dtype = match dtype_str.as_str() {
                    "float16" => crate::tensor::DType::Float16,
                    "float64" => crate::tensor::DType::Float64,
                    "int8" => crate::tensor::DType::Int8,
                    "int16" => crate::tensor::DType::Int16,
                    "int32" => crate::tensor::DType::Int32,
                    "int64" => crate::tensor::DType::Int64,
                    "bfloat16" => crate::tensor::DType::BFloat16,
                    "bool" => crate::tensor::DType::Bool,
                    _ => crate::tensor::DType::Float32,
                };
                let data = if let Some(hex_val) = args.get(4) {
                    let hex_str = hex_val.to_string().replace('\'', "");
                    // Slice over BYTES, not chars: indexing a &str by byte offset
                    // panics if the offset lands inside a multi-byte UTF-8 char
                    // (e.g. "日本語🎉"). Hex is ASCII, so a window that isn't valid
                    // ASCII/UTF-8 (or isn't valid hex) is simply dropped.
                    let bytes = hex_str.as_bytes();
                    (0..bytes.len())
                        .step_by(2)
                        .filter(|&i| i + 2 <= bytes.len())
                        .filter_map(|i| {
                            std::str::from_utf8(&bytes[i..i + 2])
                                .ok()
                                .and_then(|s| u8::from_str_radix(s, 16).ok())
                        })
                        .collect::<Vec<u8>>()
                } else {
                    let num_elements: usize = shape.iter().product();
                    vec![0u8; num_elements * dtype.element_size()]
                };
                let tensor = crate::tensor::Tensor::new(shape, dtype, data)
                    .map_err(|e| ExecError::Unsupported(format!("tensor_store: {e:?}")))?;
                self.tensor_store
                    .write()
                    .put(&name, &version, tensor, std::collections::HashMap::new())
                    .map_err(|e| ExecError::Unsupported(format!("tensor_store: {e:?}")))?;
                Ok(Value::Text("OK".into()))
            }
            "TENSOR_SHAPE" => {
                // tensor_shape(name[, version]) → JSON shape e.g. '[3,4]'
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported(
                        "tensor_shape requires (name)".into(),
                    ));
                }
                let name = args[0].to_string().replace('\'', "");
                let store = self.tensor_store.read();
                let tensor = if let Some(ver) = args.get(1) {
                    let v = ver.to_string().replace('\'', "");
                    store
                        .get(&name, &v)
                        .map_err(|e| ExecError::Unsupported(format!("tensor_shape: {e:?}")))?
                } else {
                    store
                        .get_latest(&name)
                        .map_err(|e| ExecError::Unsupported(format!("tensor_shape: {e:?}")))?
                };
                let shape_json = format!(
                    "[{}]",
                    tensor
                        .shape
                        .iter()
                        .map(|d| d.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                );
                Ok(Value::Text(shape_json))
            }
            "TENSOR_VERSIONS" => {
                // tensor_versions(name) → Int64 count of stored versions
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported(
                        "tensor_versions requires (name)".into(),
                    ));
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
                    return Err(ExecError::Unsupported(
                        "tensor_list_versions requires (name)".into(),
                    ));
                }
                let name = args[0].to_string().replace('\'', "");
                let store = self.tensor_store.read();
                let versions = store.list_versions(&name);
                let json = format!(
                    "[{}]",
                    versions
                        .iter()
                        .map(|v| format!("\"{v}\""))
                        .collect::<Vec<_>>()
                        .join(",")
                );
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
                    return Err(ExecError::Unsupported(
                        "tensor_size_bytes requires (name)".into(),
                    ));
                }
                let name = args[0].to_string().replace('\'', "");
                let store = self.tensor_store.read();
                let tensor = if let Some(ver) = args.get(1) {
                    let v = ver.to_string().replace('\'', "");
                    store
                        .get(&name, &v)
                        .map_err(|e| ExecError::Unsupported(format!("tensor_size_bytes: {e:?}")))?
                } else {
                    store
                        .get_latest(&name)
                        .map_err(|e| ExecError::Unsupported(format!("tensor_size_bytes: {e:?}")))?
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
                    return Err(ExecError::Unsupported(
                        "pii_detect requires (column_name, sample...)".into(),
                    ));
                }
                let col_name = compliance_text_arg(&args[0]);
                let samples: Vec<String> = args[1..].iter().map(compliance_text_arg).collect();
                let sample_refs: Vec<&str> = samples.iter().map(|s| s.as_str()).collect();
                let detector = crate::compliance::PiiDetector::new();
                let matches = detector.detect(&col_name, &sample_refs);
                let json =
                    format!("[{}]", matches.iter().map(|m| {
                    format!("{{\"column\":\"{}\",\"category\":\"{:?}\",\"confidence\":{:.2}}}",
                        m.column_name, m.category, m.confidence)
                }).collect::<Vec<_>>().join(","));
                Ok(Value::Text(json))
            }
            "PII_DETECT_CATEGORY" => {
                // pii_detect_category(column_name, sample) → TEXT category or 'NONE'
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported(
                        "pii_detect_category requires (column_name, sample)".into(),
                    ));
                }
                let col_name = compliance_text_arg(&args[0]);
                let sample = compliance_text_arg(&args[1]);
                let detector = crate::compliance::PiiDetector::new();
                let matches = detector.detect(&col_name, &[sample.as_str()]);
                let category = matches
                    .first()
                    .map(|m| format!("{:?}", m.category))
                    .unwrap_or_else(|| "NONE".to_string());
                Ok(Value::Text(category))
            }
            "RETENTION_SET" => {
                // retention_set(table, days, ts_column) → 'OK'
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 3 {
                    return Err(ExecError::Unsupported(
                        "retention_set requires (table, days, ts_col)".into(),
                    ));
                }
                let table_name = compliance_text_arg(&args[0]);
                let days = match &args[1] {
                    Value::Int32(n) => *n as u32,
                    Value::Int64(n) => *n as u32,
                    other => other.to_string().parse::<u32>().unwrap_or(30),
                };
                let ts_col = compliance_text_arg(&args[2]);
                let now_ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                // Nothing enforces this. No background task, no statement and
                // no code path deletes a row on the strength of a registered
                // policy — `RETENTION_CHECK` reports what WOULD expire and is
                // the only other reader of this engine. Saying so out loud is
                // the same decision taken for triggers on 2026-08-19: keep
                // accepting (rejecting breaks any script already calling it),
                // warn at registration, and document it. Implementing the
                // sweep is the real fix and is a product decision about
                // deleting data, filed as `OPEN_WORK.md` §0f.
                tracing::warn!(
                    target: "nucleus::compliance",
                    "RETENTION_SET registered a {days}-day policy on '{table_name}' \
                     ({ts_col}), but retention is ADVISORY in this build: nothing \
                     deletes expired rows. Use RETENTION_CHECK to see what would \
                     expire, and delete it yourself."
                );
                self.retention_engine
                    .write()
                    .register(crate::compliance::RetentionPolicy {
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
                let json =
                    format!("[{}]", actions.iter().map(|a| {
                    format!("{{\"table\":\"{}\",\"condition\":\"{}\",\"estimated_rows\":{}}}",
                        a.table, a.condition.replace('"', "\\\""), a.estimated_rows)
                }).collect::<Vec<_>>().join(","));
                Ok(Value::Text(json))
            }
            "GDPR_DELETE_PLAN" => {
                // gdpr_delete_plan(table, id_col, id_val) → TEXT JSON deletion plan
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 3 {
                    return Err(ExecError::Unsupported(
                        "gdpr_delete_plan requires (table, id_col, id_val)".into(),
                    ));
                }
                // extract_fn_args returns typed Values; Text renders bare, so
                // the old `.replace('\'', "")` was blind apostrophe-STRIPPING,
                // not quote-unwrapping — it corrupted any value containing
                // one, and the plan's automation silently matched nothing.
                let table = compliance_text_arg(&args[0]);
                let id_col = compliance_text_arg(&args[1]);
                let id_val = compliance_text_arg(&args[2]);
                let cascade = crate::compliance::DeletionCascade::new();
                let plan = cascade.plan_deletion(&table, &id_col, &id_val);
                let json = format!(
                    "[{}]",
                    plan.steps
                        .iter()
                        .map(|s| {
                            format!(
                                "{{\"table\":\"{}\",\"condition\":\"{}\"}}",
                                s.table,
                                s.condition.replace('"', "\\\"")
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                );
                Ok(Value::Text(json))
            }

            // ================================================================
            // Row-level versioning functions — version_* SQL API
            // ================================================================
            "VERSION_BRANCH" => {
                // version_branch(new_name, from_branch) → 'OK'
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported(
                        "version_branch requires (new_name, from_branch)".into(),
                    ));
                }
                let new_name = args[0].to_string().replace('\'', "");
                let from = args[1].to_string().replace('\'', "");
                self.version_store
                    .write()
                    .create_branch(&new_name, &from)
                    .map_err(|e| ExecError::Unsupported(format!("version_branch: {e:?}")))?;
                Ok(Value::Text("OK".into()))
            }
            "VERSION_COMMIT" => {
                // version_commit(branch, message) → Int64 commit ID
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported(
                        "version_commit requires (branch, message)".into(),
                    ));
                }
                let branch = args[0].to_string().replace('\'', "");
                let msg = args[1].to_string().replace('\'', "");
                let commit_id = self
                    .version_store
                    .write()
                    .commit(&branch, &msg, std::collections::HashMap::new())
                    .map_err(|e| ExecError::Unsupported(format!("version_commit: {e:?}")))?;
                Ok(Value::Int64(commit_id as i64))
            }
            "VERSION_LOG" => {
                // version_log(branch) → TEXT JSON array of commits
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported(
                        "version_log requires (branch)".into(),
                    ));
                }
                let branch = args[0].to_string().replace('\'', "");
                let store = self.version_store.read();
                let commits = store
                    .log(&branch)
                    .map_err(|e| ExecError::Unsupported(format!("version_log: {e:?}")))?;
                let json = format!(
                    "[{}]",
                    commits
                        .iter()
                        .map(|c| {
                            format!(
                                "{{\"id\":{},\"message\":\"{}\",\"branch\":\"{}\",\"ts\":{}}}",
                                c.id,
                                c.message.replace('"', "\\\""),
                                c.branch,
                                c.timestamp
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                );
                Ok(Value::Text(json))
            }
            "VERSION_BRANCHES" => {
                // version_branches() → TEXT JSON array of branch names
                let store = self.version_store.read();
                let branches = store.list_branches();
                let json = format!(
                    "[{}]",
                    branches
                        .iter()
                        .map(|b| format!("\"{b}\""))
                        .collect::<Vec<_>>()
                        .join(",")
                );
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
                    return Err(ExecError::Unsupported(
                        "db_branch_create requires (name[, parent_name])".into(),
                    ));
                }
                let name = args[0].to_string().replace('\'', "");
                let parent = args
                    .get(1)
                    .map(|v| v.to_string().replace('\'', ""))
                    .unwrap_or_else(|| "main".to_string());
                let branch_id = self
                    .branch_manager
                    .write()
                    .create_branch(&name, &parent)
                    .map_err(|e| ExecError::Unsupported(format!("db_branch_create: {e:?}")))?;
                Ok(Value::Int64(branch_id as i64))
            }
            "DB_BRANCH_LIST" => {
                // db_branch_list() → TEXT JSON array of branch names
                let mgr = self.branch_manager.read();
                let branches = mgr.list_branches();
                let json = format!(
                    "[{}]",
                    branches
                        .iter()
                        .map(|b| format!("\"{}\"", b.name))
                        .collect::<Vec<_>>()
                        .join(",")
                );
                Ok(Value::Text(json))
            }
            "DB_BRANCH_DELETE" => {
                // db_branch_delete(name) → Bool
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported(
                        "db_branch_delete requires (name)".into(),
                    ));
                }
                let name = args[0].to_string().replace('\'', "");
                let ok = self.branch_manager.write().delete_branch(&name).is_ok();
                Ok(Value::Bool(ok))
            }
            "DB_BRANCH_MERGE" => {
                // db_branch_merge(source, target) → 'OK'
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported(
                        "db_branch_merge requires (source, target)".into(),
                    ));
                }
                let source = args[0].to_string().replace('\'', "");
                let target = args[1].to_string().replace('\'', "");
                self.branch_manager
                    .write()
                    .merge(&source, &target)
                    .map_err(|e| ExecError::Unsupported(format!("db_branch_merge: {e:?}")))?;
                Ok(Value::Text("OK".into()))
            }
            "DB_BRANCH_DIFF" => {
                // db_branch_diff(branch_a, branch_b) → TEXT JSON diff summary
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported(
                        "db_branch_diff requires (branch_a, branch_b)".into(),
                    ));
                }
                let a = args[0].to_string().replace('\'', "");
                let b_arg = args[1].to_string().replace('\'', "");
                let diff = self
                    .branch_manager
                    .read()
                    .diff(&a, &b_arg)
                    .map_err(|e| ExecError::Unsupported(format!("db_branch_diff: {e:?}")))?;
                let json = format!(
                    "{{\"added\":{},\"modified\":{},\"deleted\":{}}}",
                    diff.added_pages.len(),
                    diff.modified_pages.len(),
                    diff.deleted_pages.len()
                );
                Ok(Value::Text(json))
            }

            // ================================================================
            // Procedure scalar functions — proc_* SQL API
            // ================================================================
            "PROC_REGISTER" => {
                // proc_register(name, body) or proc_register(name, params_csv, body) → 'OK'
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported(
                        "proc_register requires (name, body) or (name, params_csv, body)".into(),
                    ));
                }
                let name = args[0].to_string().replace('\'', "").to_lowercase();
                let (param_names, body) = if args.len() >= 3 {
                    let params: Vec<String> = args[1]
                        .to_string()
                        .replace('\'', "")
                        .split(',')
                        .map(|p| p.trim().to_string())
                        .filter(|p| !p.is_empty())
                        .collect();
                    (params, args[2].to_string().replace('\'', ""))
                } else {
                    (Vec::new(), args[1].to_string().replace('\'', ""))
                };
                self.procedure_engine.write().register_sql(
                    &name,
                    "registered via SQL",
                    param_names,
                    &body,
                );
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
                let json = format!(
                    "[{}]",
                    procs
                        .iter()
                        .map(|m| format!("\"{}\"", m.name))
                        .collect::<Vec<_>>()
                        .join(",")
                );
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
                    // Depth guard: the body is executed as SQL whose
                    // expressions may invoke this UDF again — unbounded
                    // recursion used to overflow the stack (PRC-1).
                    let _depth = self.enter_call()?;
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
                    // Execute the function body as SQL and return the result.
                    // materialize() first: under stream_results=on the body
                    // yields a SelectStream, which used to fall through to
                    // NULL (PRC-8).
                    let result = sync_block_on(async {
                        let results = self.execute(&body).await?;
                        let mut first = None;
                        for r in results {
                            if first.is_none() {
                                first = Some(r.materialize().await?);
                            }
                        }
                        Ok::<Option<ExecResult>, ExecError>(first)
                    })?;
                    match result {
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
            }
        }
    }

    /// Extract function arguments as evaluated Values.
    /// The `(qualifier, column)` a function argument names, when that argument
    /// is a plain column reference. Used by functions whose meaning depends on
    /// *which* column was passed, not only on the value it evaluated to.
    pub(super) fn fn_arg_column_ref(
        func: &ast::Function,
        idx: usize,
    ) -> Option<(Option<String>, String)> {
        let ast::FunctionArguments::List(list) = &func.args else {
            return None;
        };
        let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) = list.args.get(idx)?
        else {
            return None;
        };
        match expr {
            ast::Expr::Identifier(id) => Some((None, id.value.clone())),
            ast::Expr::CompoundIdentifier(parts) if parts.len() >= 2 => Some((
                Some(parts[parts.len() - 2].value.clone()),
                parts[parts.len() - 1].value.clone(),
            )),
            _ => None,
        }
    }

    /// `ARRAY (SELECT ...)` — collect the first column of every row the
    /// subquery returns into a single array value.
    ///
    /// Matches PostgreSQL: row order is whatever the subquery's own ORDER BY
    /// produces, an empty result is an **empty array rather than NULL** (the
    /// distinction matters to Postgrex, which indexes the result directly), and
    /// only the first projected column participates.
    ///
    /// Correlation is supported the same way scalar subqueries do it, by
    /// substituting outer references before execution — Postgrex's bootstrap is
    /// correlated on `t.typrelid`, so an uncorrelated-only implementation would
    /// have returned every attribute in the catalog for every row.
    fn eval_array_subquery(
        &self,
        subquery: &ast::Query,
        row: &Row,
        col_meta: &[ColMeta],
    ) -> Result<Value, ExecError> {
        self.check_subquery_depth()?;
        let resolved = substitute_outer_refs_in_query(subquery, row, col_meta);
        let sub_result = sync_block_on(self.execute_query(resolved));
        self.query_depth
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        match sub_result? {
            ExecResult::Select { rows, .. } => Ok(Value::Array(
                rows.into_iter()
                    .filter_map(|r| r.into_iter().next())
                    .collect(),
            )),
            // A non-SELECT cannot appear here (the parser only accepts a query),
            // but an empty array is the safe reading if one ever does: it keeps
            // the "never NULL" contract the caller indexes against.
            _ => Ok(Value::Array(Vec::new())),
        }
    }

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

/// Serialize stream entries as the contract wire format
/// (FRAMEWORK_CONTRACT.md §3.9: TEXT (JSON)):
/// `[{"id":"<ms>-<seq>","fields":{"k":"v",...}}]`.
/// The Go, TypeScript, Julia, and Elixir SDKs all JSON-parse this value;
/// the previous ad-hoc `id:k=v;k=v,...` text broke every one of them the
/// moment a field value contained `,`, `;`, or `=` — unparseable by
/// construction for JSON payloads.
/// Parse a stream position argument: either a bare millisecond (the historical
/// BIGINT form) or the full `"<ms>-<seq>"` id `STREAM_XADD` returns.
///
/// `default_seq` is what a bare millisecond means at this call site — the
/// sequence to fill in when the caller only named a millisecond. `XREAD`'s
/// Text argument for the compliance functions: the extracted Values are
/// typed, so a Text is taken verbatim (apostrophes intact) — the old
/// `.replace('\'', "")` stripped legitimate apostrophes out of every value.
/// Map a specialty-store WAL-append failure to a statement error, the same
/// way the streams path does: a full disk is `DiskFull` (SQLSTATE 53100) so
/// clients can tell "free space and retry" apart, everything else is an IO
/// storage error. S95 finding 8: the SQL surface must not acknowledge a
/// write whose durable record the log refused.
fn wal_failure_to_exec_error(what: &str, e: std::io::Error) -> ExecError {
    match e.kind() {
        std::io::ErrorKind::StorageFull => ExecError::DiskFull(format!("{what}: {e}")),
        _ => ExecError::Storage(crate::storage::StorageError::Io(format!("{what}: {e}"))),
    }
}

fn compliance_text_arg(v: &Value) -> String {
    match v {
        Value::Text(s) => s.clone(),
        other => other.to_string(),
    }
}

/// cursor and `XRANGE`'s end bound take `u64::MAX` (after / through the whole
/// millisecond); `XRANGE`'s start bound takes 0 (from its beginning).
///
/// The two forms are not interchangeable and that is the point: a millisecond
/// cannot address an entry, so a caller resuming from one either re-reads or
/// skips whatever else landed in it. Accepting the id the API itself hands out
/// is what makes resuming exact.
fn stream_cursor_arg(
    v: &Value,
    default_seq: u64,
    context: &str,
) -> Result<crate::pubsub::StreamEntryId, ExecError> {
    if let Value::Text(t) = v
        && let Some((ms, seq)) = t.trim().split_once('-')
    {
        let ms = ms
            .trim()
            .parse::<u64>()
            .map_err(|_| ExecError::Unsupported(format!("{context}: {t:?} is not <ms>-<seq>")))?;
        let seq = seq
            .trim()
            .parse::<u64>()
            .map_err(|_| ExecError::Unsupported(format!("{context}: {t:?} is not <ms>-<seq>")))?;
        return Ok(crate::pubsub::StreamEntryId::new(ms, seq));
    }
    Ok(crate::pubsub::StreamEntryId::new(
        val_to_u64(v, context)?,
        default_seq,
    ))
}

fn stream_entries_to_json(entries: &[&crate::pubsub::StreamEntry]) -> String {
    let items: Vec<serde_json::Value> = entries
        .iter()
        .map(|e| {
            // A JSON object cannot carry duplicate keys, so entries whose
            // XADD repeated a field name collapse DETERMINISTICALLY
            // last-wins here, while the RESP render (`encode_stream_entries`,
            // a flat array) preserves both pairs like Redis. The write side
            // rejects new duplicates (S31-15); this collapse only serves
            // entries written before that check existed.
            let fields: serde_json::Map<String, serde_json::Value> = e
                .fields
                .iter()
                .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                .collect();
            serde_json::json!({ "id": e.id.to_string(), "fields": fields })
        })
        .collect();
    serde_json::Value::Array(items).to_string()
}

/// Every scalar function that writes durable state, as one list.
///
/// This is the authority two other places used to restate by hand:
/// `side_effecting_return_type` below (so pgwire's Describe never
/// probe-executes a mutator) and `admission::scalar_fn_mutates` (so a
/// read-only server refuses one). They drifted, in both directions —
/// `NEXTVAL`, `SETVAL`, `RETENTION_SET`, `STREAM_XREADGROUP`, `SUBSCRIBE` and
/// `UNSUBSCRIBE` were declared side-effecting here yet admitted while
/// degraded, while `CYPHER`, `VERSION_BRANCH` and `VERSION_COMMIT` were
/// refused while degraded yet unknown to Describe. `mutating_registries_agree`
/// derives both directions from this list, so the next mutator added in one
/// place fails a test instead of shipping. (NU-216)
/// Whether a call can append to the KV string log or the KV collections log.
///
/// Derived from `SIDE_EFFECTING_FN_NAMES` rather than hand-listed: a KV or
/// stream mutator added there is covered here the moment it lands, which is the
/// property a hand-maintained second list never has.
#[cfg(feature = "server")]
pub(crate) fn touches_kv_logs(fname: &str) -> bool {
    (fname.starts_with("KV_") || fname.starts_with("STREAM_"))
        && SIDE_EFFECTING_FN_NAMES.contains(&fname)
}

#[cfg(feature = "server")]
pub(crate) const SIDE_EFFECTING_FN_NAMES: &[&str] = &[
    "BLOB_DELETE",
    "BLOB_STORE",
    "BLOB_TAG",
    "COLUMNAR_INSERT",
    "CYPHER",
    "DATALOG_ASSERT",
    "DATALOG_CLEAR",
    "DATALOG_IMPORT",
    "DATALOG_IMPORT_GRAPH",
    "DATALOG_IMPORT_NODES",
    "DATALOG_RETRACT",
    "DATALOG_RULE",
    "DB_BRANCH_CREATE",
    "DB_BRANCH_DELETE",
    "DB_BRANCH_MERGE",
    "DOC_DELETE",
    "DOC_INSERT",
    "DOC_UPDATE",
    "FTS_INDEX",
    "FTS_INDEX_FACETED",
    "FTS_REMOVE",
    "GRAPH_ADD_EDGE",
    "GRAPH_ADD_NODE",
    "GRAPH_DELETE_EDGE",
    "GRAPH_DELETE_NODE",
    "KV_CDEL",
    "KV_CEXPIRE",
    "KV_DEL",
    "KV_EXPIRE",
    "KV_FLUSHDB",
    "KV_HDEL",
    "KV_HSET",
    "KV_INCR",
    "KV_LPOP",
    "KV_LPUSH",
    "KV_PFADD",
    "KV_PFMERGE",
    "KV_RPOP",
    "KV_RPUSH",
    "KV_SADD",
    "KV_SET",
    "KV_SETNX",
    "KV_SREM",
    "KV_ZADD",
    "KV_ZREM",
    "NEXTVAL",
    "PROC_DROP",
    "PROC_REGISTER",
    "PUBSUB_PUBLISH",
    "RETENTION_SET",
    "SETVAL",
    "SPARSE_INSERT",
    "SPARSE_REMOVE",
    "STREAM_XACK",
    "STREAM_XADD",
    "STREAM_XGROUP_CREATE",
    "STREAM_XREADGROUP",
    "SUBSCRIBE",
    "TENSOR_STORE",
    "TS_INSERT",
    "TS_RETENTION",
    "UNSUBSCRIBE",
    "VERSION_BRANCH",
    "VERSION_COMMIT",
];

/// Does this function reach a specialty store, rather than computing over the
/// arguments it was handed?
///
/// Specialty stores do not yet carry table-policy metadata, so while any RLS
/// policy is active for a principal, their direct SQL functions would be an
/// alternate read/write channel around the secured relational path. They fail
/// closed until a store has native policy semantics — which is the option
/// M5/N15 explicitly allows ("implement those boundaries OR keep each surface
/// unavailable while protected relational state exists").
///
/// This is a `fn` rather than an inline expression so the RLS guard and the
/// test that audits it cannot classify differently. The test
/// (`test_specialty_surface_guard`) reads THIS file, finds every dispatch arm
/// that touches a store field, and requires this function to return `true` for
/// it — so a new specialty function is a failing test rather than a silent
/// hole. That is how `RETENTION_SET`/`RETENTION_CHECK` were found: they touch
/// `retention_engine` and matched no prefix.
pub(crate) fn is_specialty_surface(fname: &str) -> bool {
    // The text-search functions collide with the time-series `TS_` prefix
    // without belonging to any specialty store: they are pure computations over
    // their arguments, reaching no keyspace the secured relational path does not
    // already cover. Gating them denied the PostgreSQL-compatible spelling of a
    // plain expression under RLS.
    if matches!(fname, "TS_MATCH" | "TS_RANK" | "TS_HEADLINE" | "FTS_RANK") {
        return false;
    }
    const PREFIXES: [&str; 20] = [
        "COLUMNAR_",
        "DOC_",
        "FTS_",
        "GRAPH_",
        "CDC_",
        "KV_",
        "TS_",
        "STREAM_",
        "BLOB_",
        "SPARSE_",
        "LO_",
        "DATALOG_",
        "ENCRYPTED_",
        "DB_BRANCH_",
        "VERSION_",
        "TENSOR_",
        "PUBSUB_",
        "PROC_",
        "SUBSCRIPTION_",
        // Compliance retention: `RETENTION_SET` registers a deletion policy
        // against a named TABLE and `RETENTION_CHECK` enumerates every table
        // with an estimated row count — the second is a direct read of which
        // protected tables exist and how big they are. Neither matched a
        // prefix, so both were callable by any principal with RLS active.
        "RETENTION_",
    ];
    PREFIXES.iter().any(|prefix| fname.starts_with(prefix))
        || matches!(
            fname,
            "VECTOR_SEARCH"
                | "VECTOR_INSERT"
                | "VECTOR_DELETE"
                | "CYPHER"
                | "SUBSCRIBE"
                | "UNSUBSCRIBE"
        )
}

/// Why a mutating specialty function is refused inside an explicit
/// transaction, or `None` if it is allowed there.
///
/// ROLLBACK reverts a per-session cross-model write-set (`executor::cross_model`)
/// that covers KV strings, graph, document, datalog, time-series points, blob,
/// vector, streams and FTS. Everything else a specialty function can mutate is
/// NOT in that write-set, and before this it stayed written after a ROLLBACK
/// the client was told had succeeded — measured, not assumed: `KV_HSET`,
/// `KV_LPUSH`, `KV_SADD` and `COLUMNAR_INSERT` all survived one.
///
/// Refusing them inside a transaction is M8's declared contract ("implement
/// the boundaries or fail loud"), and it is the honest half: a client that
/// cannot get the guarantee should be told, not acknowledged. Outside a
/// transaction they behave exactly as before.
///
/// `test_specialty_surface_guard` requires every name in
/// `SIDE_EFFECTING_FN_NAMES` to be either structurally enlisted (its dispatch
/// arm records into the write-set), listed here, or listed in
/// `NON_TRANSACTIONAL_BY_DESIGN` — so a new mutating function cannot quietly
/// join the silent-loss set.
pub(crate) fn refused_in_transaction(fname: &str) -> Option<&'static str> {
    // KV collection types live in a separate keyspace from KV strings, and the
    // transaction snapshot covers only the string store.
    if matches!(
        fname,
        "KV_HSET"
            | "KV_HDEL"
            | "KV_LPUSH"
            | "KV_RPUSH"
            | "KV_LPOP"
            | "KV_RPOP"
            | "KV_SADD"
            | "KV_SREM"
            | "KV_ZADD"
            | "KV_ZREM"
            | "KV_PFADD"
            | "KV_PFMERGE"
    ) {
        return Some(
            "KV collection types (hashes, lists, sets, sorted sets, HyperLogLog) are not              covered by transaction rollback; call it outside an explicit transaction",
        );
    }
    match fname {
        "COLUMNAR_INSERT" => Some(
            "the columnar store is not covered by transaction rollback; insert outside an              explicit transaction",
        ),
        "SPARSE_INSERT" | "SPARSE_REMOVE" => Some(
            "the sparse index is not covered by transaction rollback; call it outside an              explicit transaction",
        ),
        "TENSOR_STORE" => Some(
            "the tensor store is not covered by transaction rollback; store outside an              explicit transaction",
        ),
        "DATALOG_IMPORT" | "DATALOG_IMPORT_GRAPH" | "DATALOG_IMPORT_NODES" => Some(
            "bulk Datalog import is not covered by transaction rollback (single-fact              DATALOG_ASSERT/RETRACT are); import outside an explicit transaction",
        ),
        "TS_RETENTION" | "RETENTION_SET" => Some(
            "retention policy changes are not covered by transaction rollback; set them              outside an explicit transaction",
        ),
        "PROC_REGISTER" | "PROC_DROP" => Some(
            "stored-procedure registration is not covered by transaction rollback; register              outside an explicit transaction",
        ),
        "DB_BRANCH_CREATE" | "DB_BRANCH_DELETE" | "DB_BRANCH_MERGE" | "VERSION_BRANCH"
        | "VERSION_COMMIT" => Some(
            "branch and version operations are not covered by transaction rollback; run them              outside an explicit transaction",
        ),
        "PUBSUB_PUBLISH" | "SUBSCRIBE" | "UNSUBSCRIBE" => Some(
            "pub/sub delivery is immediate and cannot be un-published by a ROLLBACK;              publish or subscribe outside an explicit transaction",
        ),
        _ => None,
    }
}

/// Mutating functions that are deliberately NOT transactional, with the reason.
///
/// Only sequences: PostgreSQL's `nextval`/`setval` do not roll back either —
/// that is the documented contract, and every `SERIAL` column depends on it, so
/// refusing them inside a transaction would break ordinary INSERTs.
/// Read only by the enlistment audit — it is a declaration, and the audit is
/// what makes the declaration load-bearing.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const NON_TRANSACTIONAL_BY_DESIGN: [(&str, &str); 2] = [
    ("NEXTVAL", "sequences do not roll back in PostgreSQL either"),
    ("SETVAL", "sequences do not roll back in PostgreSQL either"),
];

/// Return type of a *side-effecting* built-in scalar function, or `None`
/// for pure ones. This is the registry the pgwire Describe path uses to
/// answer "what columns would this SELECT produce?" WITHOUT executing:
/// probe-executing a mutating function at Describe time fires its effect
/// twice per client Execute (node-postgres always Describes), and the
/// client then receives the second evaluation's result — KV_SETNX
/// returned false while the key was in fact set.
///
/// Keep this in sync with `eval_scalar_fn`: any new arm that WRITES
/// (kv/doc/stream/graph/fts/blob/columnar/datalog/tensor/sequence/...)
/// must be added here with its return type.
#[cfg(feature = "server")]
pub(crate) fn side_effecting_return_type(name: &str) -> Option<crate::types::DataType> {
    use crate::types::DataType;
    let dt = match name {
        // -- booleans: did-it-happen results --
        "KV_DEL"
        | "KV_EXPIRE"
        | "KV_SETNX"
        | "KV_CDEL"
        | "KV_CEXPIRE"
        | "KV_HSET"
        | "KV_HDEL"
        | "KV_SADD"
        | "KV_SREM"
        | "KV_ZADD"
        | "KV_ZREM"
        | "KV_PFADD"
        | "KV_PFMERGE"
        | "SPARSE_INSERT"
        | "SPARSE_REMOVE"
        | "FTS_INDEX"
        | "FTS_INDEX_FACETED"
        | "FTS_REMOVE"
        | "DOC_DELETE"
        | "DOC_UPDATE"
        | "BLOB_STORE"
        | "BLOB_DELETE"
        | "BLOB_TAG"
        | "GRAPH_DELETE_NODE"
        | "GRAPH_DELETE_EDGE"
        | "DB_BRANCH_DELETE"
        | "PROC_DROP"
        | "UNSUBSCRIBE"
        | "STREAM_XGROUP_CREATE" => DataType::Bool,
        // `VERSION_BRANCH` writes and returns 'OK'; it was gated by read-only
        // admission but missing from THIS registry, which is the other half of
        // the same defect — Describe probe-executes anything it does not know
        // to be side-effecting, and that is how KV_SETNX once ran twice per
        // client Execute (see this function's header).
        "VERSION_BRANCH" => DataType::Text,
        "VERSION_COMMIT" => DataType::Int64,
        // CYPHER runs an arbitrary Cypher statement, which may CREATE or DELETE.
        "CYPHER" => DataType::Text,
        // -- integers: ids, counts, sequence values --
        "NEXTVAL" | "SETVAL" | "KV_INCR" | "KV_LPUSH" | "KV_RPUSH" | "STREAM_XACK"
        | "PUBSUB_PUBLISH" | "DOC_INSERT" | "GRAPH_ADD_NODE" | "GRAPH_ADD_EDGE" | "SUBSCRIBE"
        | "DB_BRANCH_CREATE" => DataType::Int64,
        // -- text: status strings, stream ids, popped values --
        "KV_SET"
        | "KV_FLUSHDB"
        | "KV_LPOP"
        | "KV_RPOP"
        | "STREAM_XADD"
        | "STREAM_XREADGROUP"
        | "COLUMNAR_INSERT"
        | "TS_INSERT"
        | "TS_RETENTION"
        | "DATALOG_ASSERT"
        | "DATALOG_RULE"
        | "DATALOG_RETRACT"
        | "DATALOG_CLEAR"
        | "DATALOG_IMPORT"
        | "DATALOG_IMPORT_GRAPH"
        | "DATALOG_IMPORT_NODES"
        | "TENSOR_STORE"
        | "RETENTION_SET"
        | "DB_BRANCH_MERGE"
        | "PROC_REGISTER" => DataType::Text,
        _ => return None,
    };
    Some(dt)
}

/// Return type of read-only Nucleus scalar extensions, for wire-level
/// Describe. These are pure reads, so the describe path COULD probe-execute
/// them — but a probe of `SELECT FTS_SEARCH($1, $2)` with unbound
/// placeholders errors inside the function, leaving Describe with zero
/// columns while Execute returns one (pgx: "number of field descriptions
/// must equal number of values" — dogfood finding #22 tail). Static typing
/// avoids the probe entirely.
#[cfg(feature = "server")]
pub(crate) fn extension_scalar_return_type(name: &str) -> Option<crate::types::DataType> {
    use crate::types::DataType;
    let dt = match name {
        // JSON-array result strings
        "FTS_SEARCH" | "FTS_FUZZY_SEARCH" | "FTS_SEARCH_FILTER" => DataType::Text,
        "FTS_DOC_COUNT" | "FTS_TERM_COUNT" => DataType::Int64,
        "FTS_MATCH" => DataType::Bool,
        "BM25" => DataType::Float64,
        // Document reads, for the same reason as the FTS entries above and
        // found the same way — by running a real client against a real server.
        // A probe of `SELECT DOC_GET($1)` fails inside the function (an
        // unbound placeholder is not an id), so Describe reported ZERO columns
        // while Execute returned one. asyncpg enforces that strictly, so every
        // document read from the Python client raised
        // "the number of columns in the result row (1) is different from what
        // was described (0)" — meaning `Document.get`/`get_path` had never
        // worked over pgwire from Python at all.
        "DOC_GET" | "DOC_PATH" | "DOC_PATH_IN" | "DOC_QUERY" => DataType::Text,
        "DOC_COUNT" => DataType::Int64,

        // The rest of the read-only extension surface, added 2026-08-11 for
        // exactly the reason the document entries above were: with a BOUND
        // PARAMETER the describe probe fails inside the function (an unbound
        // placeholder is not an id, a key or a timestamp), so Describe reported
        // ZERO columns while Execute returned one, and asyncpg refuses that
        // with "the number of columns in the result row (1) is different from
        // what was described (0)".
        //
        // These are not hypothetical shapes: each one below is a query string
        // taken verbatim from the Go/Python/Rust SDKs, and all 18 were measured
        // describing zero columns against a live server. So `Graph.Neighbors`,
        // `Graph.ShortestPath`, `CDC.Read`, `TimeSeries.RangeCount/RangeAvg`,
        // `Streams.XRange/XRead`, `Blob.Get/Meta`, `Datalog.Query` and the KV
        // range reads had never worked over pgwire from Python — the same
        // never-worked-at-all class as `Document.get`, found the same way.
        //
        // The literal-argument form always worked, which is what kept this
        // hidden: the probe can execute when the arguments are constants, so
        // every psql check and every test that inlines its values passes.
        //
        // Return types are each read off the function's own `Ok(Value::…)`,
        // not guessed: describing a type the executor does not produce would
        // trade a loud failure for a wrong decode in binary format.
        "BLOB_GET" | "BLOB_META" => DataType::Text,
        "CDC_READ" | "CDC_TABLE_READ" => DataType::Text,
        "DATALOG_QUERY" => DataType::Text,
        "GEO_AREA" => DataType::Float64,
        "GRAPH_NEIGHBORS" | "GRAPH_QUERY" | "GRAPH_SHORTEST_PATH" => DataType::Text,
        // KV list/sorted-set reads return a JSON array; KV_LINDEX returns the
        // element, which every push path stores as text.
        "KV_LINDEX" | "KV_LRANGE" | "KV_ZRANGE" | "KV_ZRANGEBYSCORE" => DataType::Text,
        "STREAM_XRANGE" | "STREAM_XREAD" => DataType::Text,
        "TIME_BUCKET" => DataType::Int64,
        "TS_RANGE_AVG" => DataType::Float64,
        "TS_RANGE_COUNT" => DataType::Int64,
        // TS_LAST returns Value::Float64 and TS_COUNT Value::Int64, but both
        // were absent from this map, so Describe fell through to the default and
        // declared them `varchar`. A client that believes Describe — as any
        // statically typed one must — then asks for the wrong Rust type and
        // fails to deserialize. This is the same defect class as the
        // statement-Describe typing fixed in 89d90e9: the declared type and the
        // executed type disagreeing, which is invisible to a client that decodes
        // everything as text and fatal to one that does not.
        "TS_LAST" => DataType::Float64,
        "TS_COUNT" => DataType::Int64,
        "TS_RANGE" => DataType::Text,
        _ => return None,
    };
    Some(dt)
}

/// 1-based character index of `needle` in `haystack`, or 0 if absent — the
/// POSITION/strpos return contract. Byte `find` then converted to a char
/// index so multibyte text reports character positions like PostgreSQL.
pub(super) fn char_index_of(haystack: &str, needle: &str) -> i32 {
    if needle.is_empty() {
        return 1;
    }
    match haystack.find(needle) {
        Some(byte_idx) => (haystack[..byte_idx].chars().count() + 1) as i32,
        None => 0,
    }
}

/// ROUND a Decimal to `dp` decimal places, supporting NEGATIVE scale
/// (PG: round(123, -1) = 120). `round_dp_with_strategy` takes u32, so
/// negative scales are computed as divide -> round at 0 dp -> multiply.
fn round_decimal_scaled(
    d: rust_decimal::Decimal,
    dp: i32,
) -> Result<rust_decimal::Decimal, ExecError> {
    if dp >= 0 {
        return Ok(d.round_dp_with_strategy(
            dp as u32,
            rust_decimal::RoundingStrategy::MidpointAwayFromZero,
        ));
    }
    let exp = -(dp as i64);
    // 10^28 is the largest power of ten a Decimal mantissa (< 7.9e28) holds;
    // from 10^29 on, checked_mul would error — but every representable value
    // rounds to 0 at that scale (PG returns 0), so short-circuit there.
    if exp > 28 {
        return Ok(rust_decimal::Decimal::ZERO);
    }
    let mut scale = rust_decimal::Decimal::ONE;
    for _ in 0..exp {
        scale = scale
            .checked_mul(rust_decimal::Decimal::from(10u64))
            .ok_or_else(|| ExecError::Runtime("numeric value out of range".into()))?;
    }
    let scaled = d
        .checked_div(scale)
        .ok_or_else(|| ExecError::Runtime("numeric value out of range".into()))?;
    scaled
        .round_dp_with_strategy(0, rust_decimal::RoundingStrategy::MidpointAwayFromZero)
        .checked_mul(scale)
        .ok_or_else(|| ExecError::Runtime("numeric value out of range".into()))
}
