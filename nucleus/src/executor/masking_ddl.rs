//! `CREATE MASKING POLICY` / `DROP MASKING POLICY` / `SHOW MASKING POLICIES`.
//!
//! Column masking was enforced and tested before this and had no way to
//! declare a policy over the wire: `MaskingEngine::add_policy` is a Rust API,
//! so a pgwire client — which is every client — could not create one. An
//! enforcement engine nobody can reach is a feature only the test suite has.
//!
//! The grammar is hand-parsed rather than added to `sqlparser`, matching how
//! this codebase already carries its non-standard statements (`BACKUP DATABASE
//! TO`, `SUBSCRIBE`, `CACHE_SET`, `SHOW MEMORY`):
//!
//! ```sql
//! CREATE MASKING POLICY ON users (email) TO analyst USING EMAIL;
//! CREATE MASKING POLICY ON users (ssn)   TO analyst USING REDACT '***';
//! CREATE MASKING POLICY ON users (card)  TO analyst USING PARTIAL (4, 4, '*');
//! CREATE MASKING POLICY ON users (name)  TO analyst USING HASH;
//! DROP MASKING POLICY ON users (email) TO analyst;
//! SHOW MASKING POLICIES;
//! ```
//!
//! A policy is identified by `(table, column, role)` because that is what
//! `MaskingEngine` stores and what `remove_policy` takes — inventing a policy
//! NAME here would add a second identity for the same object and a field the
//! persisted form does not have.
//!
//! Creation also resolves the column's stable id, which nothing else could:
//! `MaskingPolicy::column_id` carried the comment "masking has no CREATE DDL
//! surface yet, so there is no statement at which to resolve the id", leaving
//! the id unbound until a rename happened to stamp it. Binding it here closes
//! the window where a mask written for one column could follow its name onto a
//! different one.

use crate::security::{MaskingPolicy, MaskingRule};

use super::{ExecError, ExecResult, Executor};
use crate::types::{DataType, Value};

impl Executor {
    /// Masking DDL has no sqlparser AST, so it never reaches
    /// `execute_statement_inner`'s `is_policy_ddl`/`is_ddl` classification —
    /// those matches are over `Statement` variants and this DDL is dispatched
    /// by raw prefix. Do not "complete" that classification by adding dead
    /// match arms; this method is the raw arm's equivalent of the is_ddl
    /// publish/persist block: inside a transaction, mark the staged catalog
    /// dirty so COMMIT publishes and persists it (savepoints already snapshot
    /// `security_pending`, so ROLLBACK TO SAVEPOINT works too); in autocommit,
    /// persist now, restoring the prior policy state on failure — the same
    /// contract the is_ddl block gives every other DDL statement.
    pub(super) async fn finalize_masking_ddl(&self) -> Result<(), ExecError> {
        let session = self.current_session();
        let mut txn = session.txn_state.write().await;
        if txn.active {
            txn.policy_dirty = true; // COMMIT publishes + persists
            return Ok(());
        }
        drop(txn);
        #[cfg(feature = "server")]
        {
            let before = self.security.read().clone_policy_state();
            self.plan_cache.write().clear();
            self.ast_cache.write().clear();
            self.query_cache_invalidate_all();
            if let Err(e) = self
                .storage
                .flush_schema()
                .await
                .map_err(ExecError::Storage)
            {
                *self.security.write() = before;
                self.bump_policy_gen();
                return Err(e);
            }
            if let Err(e) = self.persist_catalog().await {
                *self.security.write() = before;
                self.bump_policy_gen();
                return Err(e);
            }
        }
        Ok(())
    }

    /// `CREATE MASKING POLICY ON <table> (<column>) TO <role> USING <rule>`
    pub(super) fn execute_create_masking_policy(&self, raw: &str) -> Result<ExecResult, ExecError> {
        self.require_security_admin("create masking policies")?;
        let parsed = parse_masking_target(raw, "CREATE MASKING POLICY")?;
        let rule = parse_masking_rule(&parsed.tail)?;

        // The column must exist, and its stable id is resolved here.
        let table_def = self
            .catalog
            .get_table_cached(&parsed.table)
            .ok_or_else(|| ExecError::TableNotFound(parsed.table.clone()))?;
        let column = table_def
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(&parsed.column))
            .ok_or_else(|| {
                ExecError::Unsupported(format!(
                    "column '{}' does not exist on '{}'",
                    parsed.column, parsed.table
                ))
            })?;

        // The role must exist, for the same reason CREATE POLICY checks it: a
        // mask naming a role that does not exist never applies, and reads as
        // protection.
        if let Ok(roles) = self.roles.try_read() {
            if !roles.contains_key(&parsed.role) {
                return Err(ExecError::Unsupported(format!(
                    "role '{}' does not exist",
                    parsed.role
                )));
            }
        } else {
            return Err(ExecError::Runtime(
                "role catalog is busy; retry CREATE MASKING POLICY".into(),
            ));
        }

        let policy = MaskingPolicy {
            table: table_def.name.clone(),
            column: column.name.clone(),
            role: parsed.role.clone(),
            rule,
            column_id: column.id,
        };
        // Only read by the audit record, which is server-only.
        #[cfg_attr(not(feature = "server"), allow(unused_variables))]
        let replaced = self.with_mutable_security(|security| {
            let replaced =
                security
                    .masking
                    .remove_policy(&policy.table, &policy.column, &policy.role);
            security.masking.add_policy(policy.clone());
            replaced
        })?;
        // Masking changes what a query returns, so cached results and plans
        // keyed on the policy generation have to be invalidated.
        self.bump_policy_gen();
        #[cfg(feature = "server")]
        self.audit(
            crate::audit::AuditKind::PolicyChanged,
            &format!("{}.{}", table_def.name, column.name),
            &format!(
                "by {}; CREATE MASKING POLICY for {}{}",
                self.acting_principal(),
                parsed.role,
                if replaced { " (replaced)" } else { "" }
            ),
            None,
        );
        Ok(ExecResult::Command {
            tag: "CREATE MASKING POLICY".into(),
            rows_affected: 0,
        })
    }

    /// `DROP MASKING POLICY ON <table> (<column>) TO <role>`
    pub(super) fn execute_drop_masking_policy(&self, raw: &str) -> Result<ExecResult, ExecError> {
        self.require_security_admin("drop masking policies")?;
        let parsed = parse_masking_target(raw, "DROP MASKING POLICY")?;
        if !parsed.tail.trim().is_empty() {
            return Err(ExecError::Unsupported(format!(
                "unexpected trailing input in DROP MASKING POLICY: {}",
                parsed.tail.trim()
            )));
        }
        let removed = self.with_mutable_security(|security| {
            security
                .masking
                .remove_policy(&parsed.table, &parsed.column, &parsed.role)
        })?;
        if !removed {
            return Err(ExecError::Unsupported(format!(
                "no masking policy on {}({}) for role '{}'",
                parsed.table, parsed.column, parsed.role
            )));
        }
        self.bump_policy_gen();
        #[cfg(feature = "server")]
        self.audit(
            crate::audit::AuditKind::PolicyChanged,
            &format!("{}.{}", parsed.table, parsed.column),
            &format!(
                "by {}; DROP MASKING POLICY for {}",
                self.acting_principal(),
                parsed.role
            ),
            None,
        );
        Ok(ExecResult::Command {
            tag: "DROP MASKING POLICY".into(),
            rows_affected: 0,
        })
    }

    /// `SHOW MASKING POLICIES` — introspection, so a policy that exists can be
    /// seen without reading the metadata file.
    pub(super) fn execute_show_masking_policies(&self) -> Result<ExecResult, ExecError> {
        self.require_security_admin("inspect masking policies")?;
        // Reads the transaction's staged view, so a policy created inside an
        // open transaction is visible to the session that created it and to
        // nobody else — the same visibility rule as RLS policy DDL.
        let rows: Vec<Vec<Value>> = self.with_visible_security(|security| {
            security
                .masking
                .all_policies()
                .iter()
                .map(|p| {
                    vec![
                        Value::Text(p.table.clone()),
                        Value::Text(p.column.clone()),
                        Value::Text(p.role.clone()),
                        Value::Text(render_rule(&p.rule)),
                    ]
                })
                .collect()
        });
        Ok(ExecResult::Select {
            columns: ["table", "column", "role", "rule"]
                .into_iter()
                .map(|n| (n.to_string(), DataType::Text))
                .collect(),
            rows,
        })
    }
}

/// `ON <table> ( <column> ) TO <role>` plus whatever follows it.
struct MaskingTarget {
    table: String,
    column: String,
    role: String,
    tail: String,
}

fn parse_masking_target(raw: &str, keyword: &str) -> Result<MaskingTarget, ExecError> {
    let body = raw.trim().trim_end_matches(';').trim();
    let rest = body
        .get(keyword.len()..)
        .ok_or_else(|| ExecError::Unsupported(format!("{keyword} requires ON <table> (<column>)")))?
        .trim();
    let rest = strip_keyword(rest, "ON").ok_or_else(|| {
        ExecError::Unsupported(format!("{keyword} requires ON <table> (<column>)"))
    })?;

    let open = rest.find('(').ok_or_else(|| {
        ExecError::Unsupported(format!("{keyword} requires a column in parentheses"))
    })?;
    let close = rest.find(')').ok_or_else(|| {
        ExecError::Unsupported(format!("{keyword} requires a column in parentheses"))
    })?;
    if close < open {
        return Err(ExecError::Unsupported(format!(
            "{keyword}: unbalanced parentheses"
        )));
    }
    let table = unquote(rest[..open].trim());
    let column = unquote(rest[open + 1..close].trim());
    if table.is_empty() || column.is_empty() {
        return Err(ExecError::Unsupported(format!(
            "{keyword} requires both a table and a column"
        )));
    }

    let after = rest[close + 1..].trim();
    let after = strip_keyword(after, "TO")
        .ok_or_else(|| ExecError::Unsupported(format!("{keyword} requires TO <role>")))?;
    let (role, tail) = match after.find(char::is_whitespace) {
        Some(i) => (&after[..i], after[i..].trim()),
        None => (after, ""),
    };
    let role = unquote(role.trim());
    if role.is_empty() {
        return Err(ExecError::Unsupported(format!(
            "{keyword} requires TO <role>"
        )));
    }
    Ok(MaskingTarget {
        table,
        column,
        role,
        tail: tail.to_string(),
    })
}

/// `USING <rule>`.
fn parse_masking_rule(tail: &str) -> Result<MaskingRule, ExecError> {
    let rest = strip_keyword(tail.trim(), "USING").ok_or_else(|| {
        ExecError::Unsupported(
            "CREATE MASKING POLICY requires USING REDACT '<text>' | EMAIL | PARTIAL (n, m [, '<char>']) | HASH | NONE"
                .into(),
        )
    })?;
    let upper = rest.to_ascii_uppercase();
    if upper == "EMAIL" {
        return Ok(MaskingRule::EmailMask);
    }
    if upper == "HASH" {
        return Ok(MaskingRule::Hash);
    }
    if upper == "NONE" {
        return Ok(MaskingRule::None);
    }
    if let Some(arg) = strip_keyword(rest, "REDACT") {
        let text = unquote(arg.trim());
        if text.is_empty() {
            return Err(ExecError::Unsupported(
                "USING REDACT requires a replacement string, e.g. REDACT '***'".into(),
            ));
        }
        return Ok(MaskingRule::Redact(text));
    }
    if let Some(arg) = strip_keyword(rest, "PARTIAL") {
        let arg = arg.trim();
        let inner = arg
            .strip_prefix('(')
            .and_then(|s| s.strip_suffix(')'))
            .ok_or_else(|| {
                ExecError::Unsupported(
                    "USING PARTIAL requires (show_first, show_last [, 'mask_char'])".into(),
                )
            })?;
        let parts: Vec<&str> = inner.split(',').map(str::trim).collect();
        if parts.len() < 2 || parts.len() > 3 {
            return Err(ExecError::Unsupported(
                "USING PARTIAL requires (show_first, show_last [, 'mask_char'])".into(),
            ));
        }
        let show_first = parts[0].parse::<usize>().map_err(|_| {
            ExecError::Unsupported(format!("USING PARTIAL: '{}' is not a count", parts[0]))
        })?;
        let show_last = parts[1].parse::<usize>().map_err(|_| {
            ExecError::Unsupported(format!("USING PARTIAL: '{}' is not a count", parts[1]))
        })?;
        let mask_char = match parts.get(2) {
            Some(c) => {
                let c = unquote(c);
                let mut chars = c.chars();
                match (chars.next(), chars.next()) {
                    (Some(ch), None) => ch,
                    _ => {
                        return Err(ExecError::Unsupported(
                            "USING PARTIAL: the mask character must be exactly one character"
                                .into(),
                        ));
                    }
                }
            }
            None => '*',
        };
        return Ok(MaskingRule::Partial {
            show_first,
            show_last,
            mask_char,
        });
    }
    Err(ExecError::Unsupported(format!(
        "unknown masking rule '{rest}'; expected REDACT '<text>' | EMAIL | PARTIAL (n, m [, '<char>']) | HASH | NONE"
    )))
}

/// Render a rule back into the syntax that would create it, so `SHOW` output
/// can be pasted into a `CREATE`.
fn render_rule(rule: &MaskingRule) -> String {
    match rule {
        MaskingRule::Redact(text) => format!("REDACT '{text}'"),
        MaskingRule::EmailMask => "EMAIL".into(),
        MaskingRule::Partial {
            show_first,
            show_last,
            mask_char,
        } => format!("PARTIAL ({show_first}, {show_last}, '{mask_char}')"),
        MaskingRule::Hash => "HASH".into(),
        MaskingRule::None => "NONE".into(),
    }
}

/// Strip a leading keyword, case-insensitively, requiring a word boundary
/// after it. `None` when the input does not start with it.
fn strip_keyword<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    let input = input.trim_start();
    if input.len() < keyword.len() || !input[..keyword.len()].eq_ignore_ascii_case(keyword) {
        return None;
    }
    let rest = &input[keyword.len()..];
    match rest.chars().next() {
        None => Some(""),
        Some(c) if c.is_whitespace() || c == '(' => Some(rest.trim_start()),
        Some(_) => None,
    }
}

/// Remove one layer of `'…'` or `"…"` quoting.
fn unquote(s: &str) -> String {
    let s = s.trim();
    for q in ['\'', '"'] {
        if s.len() >= 2 && s.starts_with(q) && s.ends_with(q) {
            return s[1..s.len() - 1].to_string();
        }
    }
    s.to_string()
}
