//! Row-level security (RLS) and data masking engine.
//!
//! Supports:
//!   - Row-level security policies per table
//!   - Policy evaluation with session context (current user, tenant, role)
//!   - Column-level data masking per role
//!   - Immutable access audit log
//!
//! Replaces Postgres RLS, application-level masking, and audit log systems.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

// ============================================================================
// Session context
// ============================================================================

/// Session context for policy evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionContext {
    pub user: String,
    pub roles: Vec<String>,
    pub tenant_id: Option<String>,
    pub properties: HashMap<String, String>,
    /// Set only from catalog-backed role attributes. Never derived from a
    /// client-writable setting.
    #[serde(default)]
    pub bypass_rls: bool,
}

impl SessionContext {
    pub fn new(user: &str) -> Self {
        Self {
            user: user.to_string(),
            roles: Vec::new(),
            tenant_id: None,
            properties: HashMap::new(),
            bypass_rls: false,
        }
    }

    pub fn with_role(mut self, role: &str) -> Self {
        self.roles.push(role.to_string());
        self
    }

    pub fn with_tenant(mut self, tenant_id: &str) -> Self {
        self.tenant_id = Some(tenant_id.to_string());
        self
    }

    pub fn with_property(mut self, key: &str, value: &str) -> Self {
        self.properties.insert(key.to_string(), value.to_string());
        self
    }

    pub fn with_bypass_rls(mut self, bypass_rls: bool) -> Self {
        self.bypass_rls = bypass_rls;
        self
    }

    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|r| r == role)
    }
}

// ============================================================================
// Row-Level Security
// ============================================================================

/// The operation type a policy applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyCommand {
    Select,
    Insert,
    Update,
    Delete,
    All,
}

/// Ordering comparison available to a row-security predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CmpOp {
    Lt,
    LtEq,
    Gt,
    GtEq,
    NotEq,
}

impl CmpOp {
    /// The SQL spelling, used when rendering a policy back to DDL.
    pub fn as_sql(self) -> &'static str {
        match self {
            CmpOp::Lt => "<",
            CmpOp::LtEq => "<=",
            CmpOp::Gt => ">",
            CmpOp::GtEq => ">=",
            CmpOp::NotEq => "<>",
        }
    }

    /// The same test with the operands swapped, so `100 < amount` compiles to
    /// the same predicate as `amount > 100`.
    pub fn flipped(self) -> Self {
        match self {
            CmpOp::Lt => CmpOp::Gt,
            CmpOp::LtEq => CmpOp::GtEq,
            CmpOp::Gt => CmpOp::Lt,
            CmpOp::GtEq => CmpOp::LtEq,
            CmpOp::NotEq => CmpOp::NotEq,
        }
    }

    fn admits(self, ordering: std::cmp::Ordering) -> bool {
        use std::cmp::Ordering;
        match self {
            CmpOp::Lt => ordering == Ordering::Less,
            CmpOp::LtEq => ordering != Ordering::Greater,
            CmpOp::Gt => ordering == Ordering::Greater,
            CmpOp::GtEq => ordering != Ordering::Less,
            CmpOp::NotEq => ordering != Ordering::Equal,
        }
    }
}

/// Compare two rendered cell values the way the policy author meant.
///
/// The RLS row map is stringly-typed, so `"10" < "9"` would hold under a plain
/// lexical compare and a policy like `amount > 100` would admit rows it must
/// not. Both sides are therefore parsed as numbers first and compared
/// numerically when both parse; anything else (text, dates, uuids) falls back to
/// a lexical compare, which is the right order for those. Dates and timestamps
/// render ISO-8601, so lexical order is chronological order for them too.
fn compare_cells(left: &str, right: &str) -> std::cmp::Ordering {
    if let (Ok(l), Ok(r)) = (left.parse::<f64>(), right.parse::<f64>()) {
        // Both numeric: NaN cannot appear from a parsed literal, so a total
        // order over the parsed values is safe.
        if let Some(ordering) = l.partial_cmp(&r) {
            return ordering;
        }
    }
    left.cmp(right)
}

/// A predicate that can be evaluated against a row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RlsPredicate {
    /// Column must equal a constant string value.
    ColumnEqStr {
        column: String,
        value: String,
        #[serde(default)]
        column_id: u32,
    },
    /// Column must equal the session's tenant_id.
    ColumnEqTenant {
        column: String,
        #[serde(default)]
        column_id: u32,
    },
    /// Column must equal the session's user.
    ColumnEqUser {
        column: String,
        #[serde(default)]
        column_id: u32,
    },
    /// Column ordered against a constant (`<`, `<=`, `>`, `>=`, `<>`).
    ColumnCmp {
        column: String,
        op: CmpOp,
        value: String,
        #[serde(default)]
        column_id: u32,
    },
    /// Column must be one of a constant list (`IN`).
    ColumnInList {
        column: String,
        values: Vec<String>,
        #[serde(default)]
        column_id: u32,
    },
    /// Column `IS NULL`, or `IS NOT NULL` when `negated`.
    ColumnIsNull {
        column: String,
        negated: bool,
        #[serde(default)]
        column_id: u32,
    },
    /// The session must have a specific role.
    HasRole { role: String },
    /// AND of two predicates.
    And(Box<RlsPredicate>, Box<RlsPredicate>),
    /// OR of two predicates.
    Or(Box<RlsPredicate>, Box<RlsPredicate>),
    /// Negation of a predicate.
    Not(Box<RlsPredicate>),
    /// Always true (permissive default).
    AlwaysTrue,
    /// Always false (restrictive default).
    AlwaysFalse,
}

impl RlsPredicate {
    /// Rewrite the cached column NAME of every leaf whose stable id matches
    /// `column_id`, and report whether anything changed.
    ///
    /// The id is the authority; the name is a cache, kept because evaluation
    /// looks rows up by name and dumps have to render one. `ALTER TABLE ...
    /// RENAME COLUMN` refreshes the cache through this, so the policy keeps
    /// meaning the same COLUMN rather than following the old name to whatever
    /// later answers to it. Predicates loaded from a pre-id snapshot carry
    /// `column_id == 0` and are deliberately not matched — nothing can claim
    /// them, so they keep their name-based behaviour instead of being captured
    /// by an unrelated column that happens to hold id 0.
    pub fn rename_column(&mut self, column_id: u32, new_name: &str) -> bool {
        if column_id == 0 {
            return false;
        }
        match self {
            RlsPredicate::ColumnEqStr {
                column,
                column_id: id,
                ..
            }
            | RlsPredicate::ColumnEqTenant {
                column,
                column_id: id,
            }
            | RlsPredicate::ColumnEqUser {
                column,
                column_id: id,
            }
            | RlsPredicate::ColumnCmp {
                column,
                column_id: id,
                ..
            }
            | RlsPredicate::ColumnInList {
                column,
                column_id: id,
                ..
            }
            | RlsPredicate::ColumnIsNull {
                column,
                column_id: id,
                ..
            } => {
                if *id == column_id && column != new_name {
                    *column = new_name.to_string();
                    true
                } else {
                    false
                }
            }
            RlsPredicate::And(a, b) | RlsPredicate::Or(a, b) => {
                // Both sides, not short-circuited: a rename must reach every leaf.
                let left = a.rename_column(column_id, new_name);
                let right = b.rename_column(column_id, new_name);
                left || right
            }
            RlsPredicate::Not(inner) => inner.rename_column(column_id, new_name),
            RlsPredicate::HasRole { .. } | RlsPredicate::AlwaysTrue | RlsPredicate::AlwaysFalse => {
                false
            }
        }
    }

    /// Bind each leaf's stable column id by resolving its name once, at
    /// CREATE POLICY time, against the catalog.
    ///
    /// After this the name is only a cache: the id is what the policy means.
    /// `resolve` returns `None` for a name that is not a column, which
    /// `validate_rls_columns` rejects separately — leaving the id at 0 here
    /// keeps that the single place the error is raised.
    pub fn bind_column_ids(&mut self, resolve: &dyn Fn(&str) -> Option<u32>) {
        match self {
            RlsPredicate::ColumnEqStr {
                column,
                column_id: id,
                ..
            }
            | RlsPredicate::ColumnEqTenant {
                column,
                column_id: id,
            }
            | RlsPredicate::ColumnEqUser {
                column,
                column_id: id,
            }
            | RlsPredicate::ColumnCmp {
                column,
                column_id: id,
                ..
            }
            | RlsPredicate::ColumnInList {
                column,
                column_id: id,
                ..
            }
            | RlsPredicate::ColumnIsNull {
                column,
                column_id: id,
                ..
            } => {
                if let Some(resolved) = resolve(column) {
                    *id = resolved;
                }
            }
            RlsPredicate::And(a, b) | RlsPredicate::Or(a, b) => {
                a.bind_column_ids(resolve);
                b.bind_column_ids(resolve);
            }
            RlsPredicate::Not(inner) => inner.bind_column_ids(resolve),
            RlsPredicate::HasRole { .. } | RlsPredicate::AlwaysTrue | RlsPredicate::AlwaysFalse => {
            }
        }
    }

    /// Stable ids of every column this predicate reads.
    ///
    /// Used to answer "does anything depend on this column" before a DROP.
    pub fn referenced_column_ids(&self, out: &mut Vec<u32>) {
        match self {
            RlsPredicate::ColumnEqStr { column_id, .. }
            | RlsPredicate::ColumnEqTenant { column_id, .. }
            | RlsPredicate::ColumnEqUser { column_id, .. }
            | RlsPredicate::ColumnCmp { column_id, .. }
            | RlsPredicate::ColumnInList { column_id, .. }
            | RlsPredicate::ColumnIsNull { column_id, .. } => {
                if *column_id != 0 {
                    out.push(*column_id);
                }
            }
            RlsPredicate::And(a, b) | RlsPredicate::Or(a, b) => {
                a.referenced_column_ids(out);
                b.referenced_column_ids(out);
            }
            RlsPredicate::Not(inner) => inner.referenced_column_ids(out),
            RlsPredicate::HasRole { .. } | RlsPredicate::AlwaysTrue | RlsPredicate::AlwaysFalse => {
            }
        }
    }

    /// Column NAMES this predicate reads — the fallback dependency check for
    /// legacy predicates that carry no id.
    pub fn referenced_column_names(&self, out: &mut Vec<String>) {
        match self {
            RlsPredicate::ColumnEqStr { column, .. }
            | RlsPredicate::ColumnEqTenant { column, .. }
            | RlsPredicate::ColumnEqUser { column, .. }
            | RlsPredicate::ColumnCmp { column, .. }
            | RlsPredicate::ColumnInList { column, .. }
            | RlsPredicate::ColumnIsNull { column, .. } => out.push(column.clone()),
            RlsPredicate::And(a, b) | RlsPredicate::Or(a, b) => {
                a.referenced_column_names(out);
                b.referenced_column_names(out);
            }
            RlsPredicate::Not(inner) => inner.referenced_column_names(out),
            RlsPredicate::HasRole { .. } | RlsPredicate::AlwaysTrue | RlsPredicate::AlwaysFalse => {
            }
        }
    }

    /// Evaluate the predicate against a row (column_name → value map) and session context.
    pub fn evaluate(&self, row: &HashMap<String, String>, ctx: &SessionContext) -> bool {
        match self {
            RlsPredicate::ColumnEqStr { column, value, .. } => row.get(column) == Some(value),
            RlsPredicate::ColumnEqTenant { column, .. } => {
                if let Some(tenant) = &ctx.tenant_id {
                    row.get(column) == Some(tenant)
                } else {
                    false
                }
            }
            RlsPredicate::ColumnEqUser { column, .. } => row.get(column) == Some(&ctx.user),
            // A NULL column is ABSENT from the map, so `get` yields None and
            // every comparison below denies. That is SQL's rule — a comparison
            // with NULL is unknown, and unknown never grants — and it is the
            // fail-closed direction, so a row whose guarded column is NULL is
            // withheld rather than leaked.
            RlsPredicate::ColumnCmp {
                column, op, value, ..
            } => row
                .get(column)
                .is_some_and(|cell| op.admits(compare_cells(cell, value))),
            RlsPredicate::ColumnInList { column, values, .. } => row
                .get(column)
                .is_some_and(|cell| values.iter().any(|candidate| candidate == cell)),
            RlsPredicate::ColumnIsNull {
                column, negated, ..
            } => row.contains_key(column) == *negated,
            RlsPredicate::HasRole { role } => ctx.has_role(role),
            RlsPredicate::And(a, b) => a.evaluate(row, ctx) && b.evaluate(row, ctx),
            RlsPredicate::Or(a, b) => a.evaluate(row, ctx) || b.evaluate(row, ctx),
            RlsPredicate::Not(p) => !p.evaluate(row, ctx),
            RlsPredicate::AlwaysTrue => true,
            RlsPredicate::AlwaysFalse => false,
        }
    }
}

/// A row-level security policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RlsPolicy {
    pub name: String,
    pub table: String,
    pub command: PolicyCommand,
    /// Roles this policy applies to (empty = all roles).
    pub target_roles: Vec<String>,
    /// The predicate that must be true for a row to be visible/writable.
    pub predicate: RlsPredicate,
    /// Predicate applied to the new row for INSERT/UPDATE. When omitted,
    /// PostgreSQL semantics reuse `predicate`.
    #[serde(default)]
    pub check_predicate: Option<RlsPredicate>,
    /// Whether this is a permissive or restrictive policy.
    pub permissive: bool,
}

/// Row-level security engine.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RlsEngine {
    /// table_name → list of policies
    policies: HashMap<String, Vec<RlsPolicy>>,
    /// Tables with RLS enabled.
    enabled_tables: std::collections::HashSet<String>,
}

impl RlsEngine {
    pub fn new() -> Self {
        Self {
            policies: HashMap::new(),
            enabled_tables: std::collections::HashSet::new(),
        }
    }

    /// Enable RLS on a table.
    pub fn enable_rls(&mut self, table: &str) {
        self.enabled_tables.insert(table.to_string());
    }

    /// Disable RLS on a table.
    pub fn disable_rls(&mut self, table: &str) {
        self.enabled_tables.remove(table);
    }

    pub fn rename_table(&mut self, old: &str, new: &str) {
        if self.enabled_tables.remove(old) {
            self.enabled_tables.insert(new.to_string());
        }
        if let Some(mut policies) = self.policies.remove(old) {
            for policy in &mut policies {
                policy.table = new.to_string();
            }
            self.policies.insert(new.to_string(), policies);
        }
    }

    /// Point every predicate on `table` that reads column `column_id` at its
    /// new name. Returns true if anything changed.
    ///
    /// Matching is by stable id, so this is an exact refresh of a cached name
    /// rather than a guess at which references meant the renamed column.
    pub fn rename_column(&mut self, table: &str, column_id: u32, new_name: &str) -> bool {
        let Some(policies) = self.policies.get_mut(table) else {
            return false;
        };
        let mut changed = false;
        for policy in policies.iter_mut() {
            changed |= policy.predicate.rename_column(column_id, new_name);
            if let Some(check) = policy.check_predicate.as_mut() {
                changed |= check.rename_column(column_id, new_name);
            }
        }
        changed
    }

    /// Names of the policies on `table` whose predicates read `column_id`, or
    /// — for predicates predating column ids — the column named `column_name`.
    ///
    /// The name fallback exists because a policy loaded from a pre-id snapshot
    /// has nothing else to match on, and silently permitting a DROP that
    /// orphans it would be the failure this whole change is closing.
    pub fn policies_depending_on_column(
        &self,
        table: &str,
        column_id: u32,
        column_name: &str,
    ) -> Vec<String> {
        let Some(policies) = self.policies.get(table) else {
            return Vec::new();
        };
        let mut dependents = Vec::new();
        for policy in policies {
            let mut ids = Vec::new();
            let mut names = Vec::new();
            policy.predicate.referenced_column_ids(&mut ids);
            policy.predicate.referenced_column_names(&mut names);
            if let Some(check) = policy.check_predicate.as_ref() {
                check.referenced_column_ids(&mut ids);
                check.referenced_column_names(&mut names);
            }
            let by_id = column_id != 0 && ids.contains(&column_id);
            let by_legacy_name = names.iter().any(|n| n == column_name);
            if by_id || by_legacy_name {
                dependents.push(policy.name.clone());
            }
        }
        dependents
    }

    /// Remove the named policies from `table` (CASCADE for a column drop).
    pub fn drop_policies_named(&mut self, table: &str, names: &[String]) {
        if let Some(policies) = self.policies.get_mut(table) {
            policies.retain(|p| !names.contains(&p.name));
        }
    }

    pub fn drop_table(&mut self, table: &str) {
        self.enabled_tables.remove(table);
        self.policies.remove(table);
    }

    /// Check if RLS is enabled on a table.
    pub fn is_enabled(&self, table: &str) -> bool {
        self.enabled_tables.contains(table)
    }

    /// Whether RLS is enabled on any table at all (cheap wholesale gate).
    pub fn any_enabled(&self) -> bool {
        !self.enabled_tables.is_empty()
    }

    /// Names of all RLS-enabled tables (for catalog persistence).
    pub fn enabled_tables(&self) -> Vec<String> {
        self.enabled_tables.iter().cloned().collect()
    }

    /// All policies across all tables (for catalog persistence).
    pub fn all_policies(&self) -> Vec<&RlsPolicy> {
        self.policies.values().flatten().collect()
    }

    /// Add a policy.
    pub fn add_policy(&mut self, policy: RlsPolicy) {
        self.policies
            .entry(policy.table.clone())
            .or_default()
            .push(policy);
    }

    /// Return a policy by table/name.
    pub fn policy(&self, table: &str, name: &str) -> Option<&RlsPolicy> {
        self.policies
            .get(table)
            .and_then(|policies| policies.iter().find(|p| p.name == name))
    }

    /// Remove a policy by name and table.
    pub fn remove_policy(&mut self, table: &str, name: &str) -> bool {
        if let Some(policies) = self.policies.get_mut(table) {
            let before = policies.len();
            policies.retain(|p| p.name != name);
            policies.len() < before
        } else {
            false
        }
    }

    /// Check if a row is visible for a given operation and session context.
    /// Returns true if the row passes all applicable policies.
    pub fn check_row(
        &self,
        table: &str,
        command: PolicyCommand,
        row: &HashMap<String, String>,
        ctx: &SessionContext,
    ) -> bool {
        // Superuser bypasses RLS
        if ctx.bypass_rls {
            return true;
        }

        // If RLS is not enabled on this table, allow all
        if !self.is_enabled(table) {
            return true;
        }

        let policies = match self.policies.get(table) {
            Some(p) => p,
            None => return false, // RLS enabled with no policies = default deny
        };

        // Filter applicable policies
        let applicable: Vec<&RlsPolicy> = policies
            .iter()
            .filter(|p| {
                // Command match
                (p.command == command || p.command == PolicyCommand::All)
                // Role match (empty = all roles)
                && (p.target_roles.is_empty()
                    || p.target_roles
                        .iter()
                        .any(|r| r.eq_ignore_ascii_case("public") || ctx.has_role(r)))
            })
            .collect();

        if applicable.is_empty() {
            // No applicable policies with RLS enabled = deny
            return false;
        }

        // Permissive policies: at least one must pass
        let permissive: Vec<&&RlsPolicy> = applicable.iter().filter(|p| p.permissive).collect();
        let restrictive: Vec<&&RlsPolicy> = applicable.iter().filter(|p| !p.permissive).collect();

        // If there are permissive policies, at least one must allow
        // At least one permissive policy is required. Restrictive policies can
        // narrow access, but can never grant it by themselves.
        let permissive_pass = permissive.iter().any(|p| p.predicate.evaluate(row, ctx));

        // All restrictive policies must allow
        let restrictive_pass = restrictive.iter().all(|p| p.predicate.evaluate(row, ctx));

        permissive_pass && restrictive_pass
    }

    /// Check a proposed INSERT/UPDATE row using WITH CHECK semantics.
    pub fn check_new_row(
        &self,
        table: &str,
        command: PolicyCommand,
        row: &HashMap<String, String>,
        ctx: &SessionContext,
    ) -> bool {
        if ctx.bypass_rls {
            return true;
        }
        if !self.is_enabled(table) {
            return true;
        }
        let Some(policies) = self.policies.get(table) else {
            return false;
        };
        let applicable: Vec<&RlsPolicy> = policies
            .iter()
            .filter(|p| {
                (p.command == command || p.command == PolicyCommand::All)
                    && (p.target_roles.is_empty()
                        || p.target_roles
                            .iter()
                            .any(|r| r.eq_ignore_ascii_case("public") || ctx.has_role(r)))
            })
            .collect();
        if applicable.is_empty() {
            return false;
        }
        let permissive_pass = applicable.iter().filter(|p| p.permissive).any(|p| {
            p.check_predicate
                .as_ref()
                .unwrap_or(&p.predicate)
                .evaluate(row, ctx)
        });
        let restrictive_pass = applicable.iter().filter(|p| !p.permissive).all(|p| {
            p.check_predicate
                .as_ref()
                .unwrap_or(&p.predicate)
                .evaluate(row, ctx)
        });
        permissive_pass && restrictive_pass
    }

    /// Filter rows based on RLS policies. Returns indices of visible rows.
    pub fn filter_rows(
        &self,
        table: &str,
        command: PolicyCommand,
        rows: &[HashMap<String, String>],
        ctx: &SessionContext,
    ) -> Vec<usize> {
        rows.iter()
            .enumerate()
            .filter(|(_, row)| self.check_row(table, command, row, ctx))
            .map(|(i, _)| i)
            .collect()
    }
}

// ============================================================================
// Data Masking
// ============================================================================

/// How to mask a column's value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MaskingRule {
    /// Full redaction: replace with a constant.
    Redact(String),
    /// Email masking: t***@example.com
    EmailMask,
    /// Partial mask: show first N and last M characters.
    Partial {
        show_first: usize,
        show_last: usize,
        mask_char: char,
    },
    /// Hash the value (for pseudonymization).
    Hash,
    /// No masking (pass through).
    None,
}

impl MaskingRule {
    /// Apply the masking rule to a value.
    pub fn apply(&self, value: &str) -> String {
        match self {
            MaskingRule::Redact(replacement) => replacement.clone(),
            MaskingRule::EmailMask => {
                if let Some(at_pos) = value.find('@') {
                    let local = &value[..at_pos];
                    let domain = &value[at_pos..];
                    if local.len() <= 1 {
                        format!("*{domain}")
                    } else {
                        let first = &local[..1];
                        let stars = "*".repeat(local.len() - 1);
                        format!("{first}{stars}{domain}")
                    }
                } else {
                    "*".repeat(value.len())
                }
            }
            MaskingRule::Partial {
                show_first,
                show_last,
                mask_char,
            } => {
                let chars: Vec<char> = value.chars().collect();
                let len = chars.len();
                if *show_first + *show_last >= len {
                    return value.to_string();
                }
                let mut result = String::new();
                for (i, c) in chars.iter().enumerate() {
                    if i < *show_first || i >= len - *show_last {
                        result.push(*c);
                    } else {
                        result.push(*mask_char);
                    }
                }
                result
            }
            MaskingRule::Hash => {
                // Simple hash for pseudonymization (not crypto — use for demo)
                let mut hash: u64 = 5381;
                for byte in value.bytes() {
                    hash = hash.wrapping_mul(33).wrapping_add(byte as u64);
                }
                format!("{hash:016x}")
            }
            MaskingRule::None => value.to_string(),
        }
    }
}

/// A masking policy: which columns to mask for which roles.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaskingPolicy {
    pub table: String,
    pub column: String,
    pub role: String,
    pub rule: MaskingRule,
    /// Stable id of the masked column — see [`crate::catalog::ColumnDef::id`].
    ///
    /// A mask stored against a NAME stops applying when the column is renamed,
    /// and starts applying to an unrelated column if that name is later
    /// recreated. That failure direction is OPEN: the value is returned
    /// unmasked, unlike an RLS predicate losing its column, which denies.
    ///
    /// `0` means unbound. Masking has no `CREATE` DDL surface yet, so there is
    /// no statement at which to resolve the id; it is stamped the first time a
    /// rename matches the rule by name, and honoured from then on.
    #[serde(default)]
    pub column_id: u32,
}

/// Data masking engine.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MaskingEngine {
    /// (table, column, role) → masking rule
    policies: Vec<MaskingPolicy>,
}

impl MaskingEngine {
    pub fn new() -> Self {
        Self {
            policies: Vec::new(),
        }
    }

    /// Add a masking policy.
    pub fn add_policy(&mut self, policy: MaskingPolicy) {
        self.policies.push(policy);
    }

    pub fn all_policies(&self) -> &[MaskingPolicy] {
        &self.policies
    }

    pub fn remove_policy(&mut self, table: &str, column: &str, role: &str) -> bool {
        let before = self.policies.len();
        self.policies
            .retain(|p| p.table != table || p.column != column || p.role != role);
        before != self.policies.len()
    }

    pub fn rename_table(&mut self, old: &str, new: &str) {
        for policy in &mut self.policies {
            if policy.table == old {
                policy.table = new.to_string();
            }
        }
    }

    /// Point every mask on `table` for the renamed column at its new name.
    ///
    /// Matches by stable id once one is bound, else by the old name — which is
    /// unambiguous AT a rename, because a table cannot hold two columns of the
    /// same name. The id is stamped while we are here, so the later hazard is
    /// closed too: if that old name is recreated by `ADD COLUMN`, the mask stays
    /// on the column it was written for instead of jumping to the new one.
    pub fn rename_column(
        &mut self,
        table: &str,
        column_id: u32,
        old_name: &str,
        new_name: &str,
    ) -> bool {
        let mut changed = false;
        for policy in &mut self.policies {
            if policy.table != table {
                continue;
            }
            let matches = if policy.column_id != 0 && column_id != 0 {
                policy.column_id == column_id
            } else {
                policy.column == old_name
            };
            if matches {
                policy.column = new_name.to_string();
                if column_id != 0 {
                    policy.column_id = column_id;
                }
                changed = true;
            }
        }
        changed
    }

    /// Roles whose masks on `table` read the given column, by id or by name.
    pub fn masks_depending_on_column(
        &self,
        table: &str,
        column_id: u32,
        column_name: &str,
    ) -> Vec<String> {
        self.policies
            .iter()
            .filter(|p| {
                p.table == table
                    && ((column_id != 0 && p.column_id == column_id) || p.column == column_name)
            })
            .map(|p| p.role.clone())
            .collect()
    }

    /// Drop every mask on `table` for the given column (CASCADE on a drop).
    pub fn drop_masks_for_column(&mut self, table: &str, column_id: u32, column_name: &str) {
        self.policies.retain(|p| {
            !(p.table == table
                && ((column_id != 0 && p.column_id == column_id) || p.column == column_name))
        });
    }

    pub fn drop_table(&mut self, table: &str) {
        self.policies.retain(|policy| policy.table != table);
    }

    /// Get the masking rule for a specific table/column/role combination.
    /// Whether any masking policy exists at all.
    ///
    /// The executor uses this to decide whether a query must take the secured
    /// path. Masking that is declared and never applied is worse than absent:
    /// an absent feature is not relied on.
    pub fn any_policies(&self) -> bool {
        !self.policies.is_empty()
    }

    /// Whether any masking policy covers `table`.
    pub fn covers_table(&self, table: &str) -> bool {
        self.policies.iter().any(|p| p.table == table)
    }

    pub fn get_rule(&self, table: &str, column: &str, ctx: &SessionContext) -> &MaskingRule {
        for policy in &self.policies {
            if policy.table == table && policy.column == column && ctx.has_role(&policy.role) {
                return &policy.rule;
            }
        }
        &MaskingRule::None
    }

    /// Apply masking to a row (column_name → value map).
    pub fn mask_row(
        &self,
        table: &str,
        row: &HashMap<String, String>,
        ctx: &SessionContext,
    ) -> HashMap<String, String> {
        row.iter()
            .map(|(col, val)| {
                let rule = self.get_rule(table, col, ctx);
                (col.clone(), rule.apply(val))
            })
            .collect()
    }
}

// ============================================================================
// Access Audit Log
// ============================================================================

/// An entry in the audit log.
#[derive(Debug, Clone)]
pub struct AuditEntry {
    pub id: u64,
    pub timestamp: u64,
    pub user: String,
    pub action: String,
    pub table: Option<String>,
    pub query: String,
    pub rows_affected: usize,
    pub success: bool,
}

/// Append-only immutable audit log.
pub struct AuditLog {
    entries: Vec<AuditEntry>,
    next_id: u64,
}

impl Default for AuditLog {
    fn default() -> Self {
        Self::new()
    }
}

impl AuditLog {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            next_id: 1,
        }
    }

    /// Log an access event.
    pub fn log(
        &mut self,
        user: &str,
        action: &str,
        table: Option<&str>,
        query: &str,
        rows_affected: usize,
        success: bool,
    ) -> u64 {
        let id = self.next_id;
        self.next_id += 1;

        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        self.entries.push(AuditEntry {
            id,
            timestamp: ts,
            user: user.to_string(),
            action: action.to_string(),
            table: table.map(|t| t.to_string()),
            query: query.to_string(),
            rows_affected,
            success,
        });

        id
    }

    /// Query audit log entries for a specific user.
    pub fn entries_by_user(&self, user: &str) -> Vec<&AuditEntry> {
        self.entries.iter().filter(|e| e.user == user).collect()
    }

    /// Query audit log entries for a specific table.
    pub fn entries_by_table(&self, table: &str) -> Vec<&AuditEntry> {
        self.entries
            .iter()
            .filter(|e| e.table.as_deref() == Some(table))
            .collect()
    }

    /// Get all entries (newest first).
    pub fn all_entries(&self) -> &[AuditEntry] {
        &self.entries
    }

    /// Total number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// ============================================================================
// Unified security manager
// ============================================================================

/// Unified security manager combining RLS, masking, and audit.
pub struct SecurityManager {
    pub rls: RlsEngine,
    pub masking: MaskingEngine,
    pub audit: AuditLog,
}

impl Default for SecurityManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SecurityManager {
    pub fn new() -> Self {
        Self {
            rls: RlsEngine::new(),
            masking: MaskingEngine::new(),
            audit: AuditLog::new(),
        }
    }

    /// Clone durable policy state without copying the append-only runtime audit log.
    pub fn clone_policy_state(&self) -> Self {
        Self {
            rls: self.rls.clone(),
            masking: self.masking.clone(),
            audit: AuditLog::new(),
        }
    }
}

// ============================================================================
// Per-Tenant Encryption Key Isolation
// ============================================================================

/// Manages per-tenant encryption keys with rotation support.
pub struct TenantKeyManager {
    /// Active keys per tenant (tenant_id -> (key_id, key_bytes)).
    active_keys: HashMap<String, (u32, Vec<u8>)>,
    /// Key history for decrypting old data (tenant_id -> vec of (key_id, key_bytes)).
    key_history: HashMap<String, Vec<(u32, Vec<u8>)>>,
    /// Global default key for tenants without a dedicated key.
    default_key: Option<(u32, Vec<u8>)>,
    /// Next key ID counter.
    next_key_id: u32,
}

impl Default for TenantKeyManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TenantKeyManager {
    pub fn new() -> Self {
        Self {
            active_keys: HashMap::new(),
            key_history: HashMap::new(),
            default_key: None,
            next_key_id: 1,
        }
    }

    /// Set the global default encryption key.
    pub fn set_default_key(&mut self, key_bytes: Vec<u8>) -> u32 {
        let key_id = self.next_key_id;
        self.next_key_id += 1;
        self.default_key = Some((key_id, key_bytes));
        key_id
    }

    /// Register a dedicated encryption key for a tenant.
    pub fn register_tenant_key(&mut self, tenant_id: &str, key_bytes: Vec<u8>) -> u32 {
        let key_id = self.next_key_id;
        self.next_key_id += 1;
        // Archive the old key if one exists.
        if let Some(old) = self.active_keys.get(tenant_id) {
            self.key_history
                .entry(tenant_id.to_string())
                .or_default()
                .push(old.clone());
        }
        self.active_keys
            .insert(tenant_id.to_string(), (key_id, key_bytes));
        key_id
    }

    /// Rotate the key for a tenant: archive the old key, set the new one.
    pub fn rotate_key(&mut self, tenant_id: &str, new_key_bytes: Vec<u8>) -> Result<u32, String> {
        if !self.active_keys.contains_key(tenant_id) {
            return Err(format!("no existing key for tenant {tenant_id}"));
        }
        Ok(self.register_tenant_key(tenant_id, new_key_bytes))
    }

    /// Get the active key for a tenant. Falls back to default key.
    pub fn get_active_key(&self, tenant_id: &str) -> Option<(u32, &[u8])> {
        if let Some((id, bytes)) = self.active_keys.get(tenant_id) {
            return Some((*id, bytes.as_slice()));
        }
        self.default_key
            .as_ref()
            .map(|(id, bytes)| (*id, bytes.as_slice()))
    }

    /// Get a specific key by key_id (searches active + history across all tenants).
    pub fn get_key_by_id(&self, key_id: u32) -> Option<&[u8]> {
        // Check default key.
        if let Some((id, bytes)) = &self.default_key
            && *id == key_id
        {
            return Some(bytes.as_slice());
        }
        // Check active keys.
        for (id, bytes) in self.active_keys.values() {
            if *id == key_id {
                return Some(bytes.as_slice());
            }
        }
        // Check key history.
        for history in self.key_history.values() {
            for (id, bytes) in history {
                if *id == key_id {
                    return Some(bytes.as_slice());
                }
            }
        }
        None
    }

    /// Remove all keys for a tenant (e.g., on tenant deletion).
    pub fn revoke_tenant(&mut self, tenant_id: &str) -> bool {
        let had_active = self.active_keys.remove(tenant_id).is_some();
        let had_history = self.key_history.remove(tenant_id).is_some();
        had_active || had_history
    }

    /// List all tenant IDs that have dedicated keys.
    pub fn tenant_ids(&self) -> Vec<String> {
        self.active_keys.keys().cloned().collect()
    }

    /// Count of active tenant keys (not counting default).
    pub fn tenant_count(&self) -> usize {
        self.active_keys.len()
    }

    /// Return the number of historical (rotated) keys for a tenant.
    pub fn key_history_count(&self, tenant_id: &str) -> usize {
        self.key_history.get(tenant_id).map_or(0, |h| h.len())
    }
}

// ============================================================================
// Key rotation (checklist 4.2)
// ============================================================================

/// State of a key rotation process.
#[derive(Debug, Clone)]
pub enum RotationState {
    Idle,
    InProgress {
        old_key_id: u32,
        new_key_id: u32,
        progress_pct: u8,
    },
    Completed {
        old_key_id: u32,
        new_key_id: u32,
    },
}

/// Record of a completed key rotation.
#[derive(Debug, Clone)]
pub struct RotationRecord {
    pub old_key_id: u32,
    pub new_key_id: u32,
    pub started_at_ms: u64,
    pub completed_at_ms: Option<u64>,
    pub pages_re_encrypted: u64,
}

/// Manages encryption key lifecycle: creation, rotation, and retirement.
pub struct KeyRotationManager {
    next_key_id: u32,
    keys: Vec<(u32, String, Vec<u8>, bool)>, // (id, algorithm, material, is_active)
    state: RotationState,
    rotation_history: Vec<RotationRecord>,
}

impl Default for KeyRotationManager {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyRotationManager {
    pub fn new() -> Self {
        Self {
            next_key_id: 1,
            keys: Vec::new(),
            state: RotationState::Idle,
            rotation_history: Vec::new(),
        }
    }

    /// Create a new key with the given algorithm and material, marking it active.
    pub fn create_key(&mut self, algorithm: &str, material: Vec<u8>) -> u32 {
        // Deactivate all existing keys.
        for k in &mut self.keys {
            k.3 = false;
        }
        let id = self.next_key_id;
        self.next_key_id += 1;
        self.keys.push((id, algorithm.to_string(), material, true));
        id
    }

    /// Return the currently active key (id, algorithm, material).
    pub fn active_key(&self) -> Option<(u32, &str, &[u8])> {
        self.keys
            .iter()
            .find(|k| k.3)
            .map(|k| (k.0, k.1.as_str(), k.2.as_slice()))
    }

    /// Look up a key by its ID.
    pub fn get_key(&self, key_id: u32) -> Option<(u32, &str, &[u8])> {
        self.keys
            .iter()
            .find(|k| k.0 == key_id)
            .map(|k| (k.0, k.1.as_str(), k.2.as_slice()))
    }

    /// Begin a key rotation: create a new key and set state to InProgress.
    pub fn begin_rotation(
        &mut self,
        algorithm: &str,
        new_material: Vec<u8>,
    ) -> Result<u32, String> {
        if matches!(&self.state, RotationState::InProgress { .. }) {
            return Err("rotation already in progress".into());
        }
        let old_key_id = match self.active_key() {
            Some((id, _, _)) => id,
            None => return Err("no active key to rotate from".into()),
        };
        let new_key_id = self.create_key(algorithm, new_material);
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.state = RotationState::InProgress {
            old_key_id,
            new_key_id,
            progress_pct: 0,
        };
        self.rotation_history.push(RotationRecord {
            old_key_id,
            new_key_id,
            started_at_ms: ts,
            completed_at_ms: None,
            pages_re_encrypted: 0,
        });
        Ok(new_key_id)
    }

    /// Update progress percentage of an in-progress rotation.
    pub fn update_progress(&mut self, pct: u8) {
        if let RotationState::InProgress {
            old_key_id,
            new_key_id,
            ..
        } = self.state
        {
            self.state = RotationState::InProgress {
                old_key_id,
                new_key_id,
                progress_pct: pct,
            };
        }
    }

    /// Finalize the current rotation.
    pub fn complete_rotation(&mut self, pages_re_encrypted: u64) -> Result<(), String> {
        if let RotationState::InProgress {
            old_key_id,
            new_key_id,
            ..
        } = self.state
        {
            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            self.state = RotationState::Completed {
                old_key_id,
                new_key_id,
            };
            if let Some(record) = self.rotation_history.last_mut() {
                record.completed_at_ms = Some(ts);
                record.pages_re_encrypted = pages_re_encrypted;
            }
            Ok(())
        } else {
            Err("no rotation in progress".into())
        }
    }

    /// Cancel an in-progress rotation, reverting active key to the old one.
    pub fn cancel_rotation(&mut self) -> Result<(), String> {
        if let RotationState::InProgress {
            old_key_id,
            new_key_id,
            ..
        } = self.state
        {
            // Reactivate old key, deactivate new.
            for k in &mut self.keys {
                k.3 = k.0 == old_key_id;
            }
            self.state = RotationState::Idle;
            // Remove last history record (incomplete).
            if let Some(last) = self.rotation_history.last()
                && last.old_key_id == old_key_id
                && last.new_key_id == new_key_id
            {
                self.rotation_history.pop();
            }
            Ok(())
        } else {
            Err("no rotation in progress".into())
        }
    }

    pub fn rotation_state(&self) -> &RotationState {
        &self.state
    }
    pub fn rotation_history(&self) -> &[RotationRecord] {
        &self.rotation_history
    }
    pub fn key_count(&self) -> usize {
        self.keys.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn rls_tenant_isolation() {
        let mut engine = RlsEngine::new();
        engine.enable_rls("orders");
        engine.add_policy(RlsPolicy {
            name: "tenant_isolation".into(),
            table: "orders".into(),
            command: PolicyCommand::All,
            target_roles: vec![],
            predicate: RlsPredicate::ColumnEqTenant {
                column: "org_id".into(),
                column_id: 0,
            },
            check_predicate: None,
            permissive: true,
        });

        let ctx = SessionContext::new("alice").with_tenant("org_1");

        let row1 = make_row(&[("id", "1"), ("org_id", "org_1"), ("amount", "100")]);
        let row2 = make_row(&[("id", "2"), ("org_id", "org_2"), ("amount", "200")]);

        assert!(engine.check_row("orders", PolicyCommand::Select, &row1, &ctx));
        assert!(!engine.check_row("orders", PolicyCommand::Select, &row2, &ctx));
    }

    #[test]
    fn rls_superuser_bypass() {
        let mut engine = RlsEngine::new();
        engine.enable_rls("orders");
        engine.add_policy(RlsPolicy {
            name: "deny_all".into(),
            table: "orders".into(),
            command: PolicyCommand::All,
            target_roles: vec![],
            predicate: RlsPredicate::AlwaysFalse,
            check_predicate: None,
            permissive: true,
        });

        let row = make_row(&[("id", "1")]);

        // The bypass comes from the ATTRIBUTE.
        let privileged = SessionContext::new("admin")
            .with_role("superuser")
            .with_bypass_rls(true);
        assert!(engine.check_row("orders", PolicyCommand::Select, &row, &privileged));

        // The NAME alone must confer nothing. This assertion is the point of the
        // test: it used to pass with only `.with_role("superuser")`, which meant
        // any role a security admin happened to call "superuser" -- or any role
        // reaching that name through GRANT membership -- silently bypassed RLS,
        // masking and privilege checks while `pg_roles.rolsuper` reported false.
        let named_only = SessionContext::new("mallory").with_role("superuser");
        assert!(
            !engine.check_row("orders", PolicyCommand::Select, &row, &named_only),
            "a role NAMED superuser must not bypass RLS without the attribute"
        );
    }

    #[test]
    fn comparison_predicate_orders_numerically_not_lexically() {
        // The trap this exists to avoid: "9" > "100" holds lexically, but
        // 9 > 100 does not. A lexical compare would ADMIT a row the policy
        // excludes, so the numeric path is a security property, not a nicety.
        let predicate = RlsPredicate::ColumnCmp {
            column: "amount".into(),
            op: CmpOp::Gt,
            value: "100".into(),
            column_id: 0,
        };
        let ctx = SessionContext::new("u");
        assert!(predicate.evaluate(&make_row(&[("amount", "200")]), &ctx));
        assert!(!predicate.evaluate(&make_row(&[("amount", "9")]), &ctx));
        assert!(!predicate.evaluate(&make_row(&[("amount", "100")]), &ctx));
    }

    #[test]
    fn comparison_and_in_list_deny_a_null_column() {
        // A NULL cell is ABSENT from the row map. SQL says a comparison with
        // NULL is unknown, and unknown never grants — so every form denies.
        let ctx = SessionContext::new("u");
        let null_row = make_row(&[("id", "1")]);

        for op in [CmpOp::Lt, CmpOp::LtEq, CmpOp::Gt, CmpOp::GtEq, CmpOp::NotEq] {
            let predicate = RlsPredicate::ColumnCmp {
                column: "amount".into(),
                op,
                value: "100".into(),
                column_id: 0,
            };
            assert!(
                !predicate.evaluate(&null_row, &ctx),
                "{op:?} must not admit a row whose column is NULL"
            );
        }

        let in_list = RlsPredicate::ColumnInList {
            column: "amount".into(),
            values: vec!["1".into(), "2".into()],
            column_id: 0,
        };
        assert!(!in_list.evaluate(&null_row, &ctx));
    }

    #[test]
    fn is_null_predicate_reads_absence() {
        let ctx = SessionContext::new("u");
        let present = make_row(&[("region", "eu")]);
        let missing = make_row(&[("id", "1")]);

        let is_null = RlsPredicate::ColumnIsNull {
            column: "region".into(),
            negated: false,
            column_id: 0,
        };
        assert!(is_null.evaluate(&missing, &ctx));
        assert!(!is_null.evaluate(&present, &ctx));

        let is_not_null = RlsPredicate::ColumnIsNull {
            column: "region".into(),
            negated: true,
            column_id: 0,
        };
        assert!(is_not_null.evaluate(&present, &ctx));
        assert!(!is_not_null.evaluate(&missing, &ctx));
    }

    #[test]
    fn in_list_and_text_comparison_use_the_right_order() {
        let ctx = SessionContext::new("u");
        let in_list = RlsPredicate::ColumnInList {
            column: "region".into(),
            values: vec!["eu".into(), "us".into()],
            column_id: 0,
        };
        assert!(in_list.evaluate(&make_row(&[("region", "eu")]), &ctx));
        assert!(!in_list.evaluate(&make_row(&[("region", "apac")]), &ctx));

        // Non-numeric operands keep lexical order, which is what text wants.
        let text_cmp = RlsPredicate::ColumnCmp {
            column: "region".into(),
            op: CmpOp::Lt,
            value: "m".into(),
            column_id: 0,
        };
        assert!(text_cmp.evaluate(&make_row(&[("region", "eu")]), &ctx));
        assert!(!text_cmp.evaluate(&make_row(&[("region", "us")]), &ctx));
    }

    #[test]
    fn rls_permissive_and_restrictive() {
        let mut engine = RlsEngine::new();
        engine.enable_rls("docs");

        // Permissive: user owns the doc
        engine.add_policy(RlsPolicy {
            name: "owner_access".into(),
            table: "docs".into(),
            command: PolicyCommand::Select,
            target_roles: vec![],
            predicate: RlsPredicate::ColumnEqUser {
                column: "owner".into(),
                column_id: 0,
            },
            check_predicate: None,
            permissive: true,
        });

        // Restrictive: doc must be published
        engine.add_policy(RlsPolicy {
            name: "published_only".into(),
            table: "docs".into(),
            command: PolicyCommand::Select,
            target_roles: vec![],
            predicate: RlsPredicate::ColumnEqStr {
                column: "status".into(),
                value: "published".into(),
                column_id: 0,
            },
            check_predicate: None,
            permissive: false,
        });

        let ctx = SessionContext::new("alice");

        // Alice's published doc: pass both
        let row1 = make_row(&[("owner", "alice"), ("status", "published")]);
        assert!(engine.check_row("docs", PolicyCommand::Select, &row1, &ctx));

        // Alice's draft doc: fails restrictive
        let row2 = make_row(&[("owner", "alice"), ("status", "draft")]);
        assert!(!engine.check_row("docs", PolicyCommand::Select, &row2, &ctx));

        // Bob's published doc: fails permissive
        let row3 = make_row(&[("owner", "bob"), ("status", "published")]);
        assert!(!engine.check_row("docs", PolicyCommand::Select, &row3, &ctx));
    }

    #[test]
    fn rls_filter_rows() {
        let mut engine = RlsEngine::new();
        engine.enable_rls("items");
        engine.add_policy(RlsPolicy {
            name: "tenant".into(),
            table: "items".into(),
            command: PolicyCommand::Select,
            target_roles: vec![],
            predicate: RlsPredicate::ColumnEqTenant {
                column: "tenant".into(),
                column_id: 0,
            },
            check_predicate: None,
            permissive: true,
        });

        let ctx = SessionContext::new("user1").with_tenant("t1");
        let rows = vec![
            make_row(&[("id", "1"), ("tenant", "t1")]),
            make_row(&[("id", "2"), ("tenant", "t2")]),
            make_row(&[("id", "3"), ("tenant", "t1")]),
            make_row(&[("id", "4"), ("tenant", "t3")]),
        ];

        let visible = engine.filter_rows("items", PolicyCommand::Select, &rows, &ctx);
        assert_eq!(visible, vec![0, 2]); // Only rows with tenant=t1
    }

    #[test]
    fn masking_email() {
        let rule = MaskingRule::EmailMask;
        assert_eq!(rule.apply("tyler@example.com"), "t****@example.com");
        assert_eq!(rule.apply("ab@test.io"), "a*@test.io");
    }

    #[test]
    fn masking_partial() {
        let rule = MaskingRule::Partial {
            show_first: 4,
            show_last: 4,
            mask_char: '*',
        };
        assert_eq!(rule.apply("1234567890123456"), "1234********3456");
    }

    #[test]
    fn masking_engine_applies_per_role() {
        let mut masking = MaskingEngine::new();
        masking.add_policy(MaskingPolicy {
            table: "users".into(),
            column: "email".into(),
            role: "analyst".into(),
            rule: MaskingRule::EmailMask,
            column_id: 0,
        });
        masking.add_policy(MaskingPolicy {
            table: "users".into(),
            column: "ssn".into(),
            role: "analyst".into(),
            rule: MaskingRule::Redact("***-**-****".into()),
            column_id: 0,
        });

        let analyst_ctx = SessionContext::new("bob").with_role("analyst");
        let admin_ctx = SessionContext::new("admin").with_role("admin");

        let row = make_row(&[
            ("name", "Tyler"),
            ("email", "tyler@example.com"),
            ("ssn", "123-45-6789"),
        ]);

        // Analyst sees masked data
        let masked = masking.mask_row("users", &row, &analyst_ctx);
        assert_eq!(masked["name"], "Tyler"); // No masking on name
        assert_eq!(masked["email"], "t****@example.com");
        assert_eq!(masked["ssn"], "***-**-****");

        // Admin sees raw data
        let unmasked = masking.mask_row("users", &row, &admin_ctx);
        assert_eq!(unmasked["email"], "tyler@example.com");
        assert_eq!(unmasked["ssn"], "123-45-6789");
    }

    #[test]
    fn audit_log_append_only() {
        let mut audit = AuditLog::new();

        audit.log(
            "alice",
            "SELECT",
            Some("users"),
            "SELECT * FROM users",
            10,
            true,
        );
        audit.log(
            "bob",
            "INSERT",
            Some("orders"),
            "INSERT INTO orders ...",
            1,
            true,
        );
        audit.log(
            "alice",
            "DELETE",
            Some("orders"),
            "DELETE FROM orders WHERE id=5",
            1,
            false,
        );

        assert_eq!(audit.len(), 3);

        let alice_entries = audit.entries_by_user("alice");
        assert_eq!(alice_entries.len(), 2);

        let orders_entries = audit.entries_by_table("orders");
        assert_eq!(orders_entries.len(), 2);

        // Verify immutability — entries are ordered by ID
        let all = audit.all_entries();
        assert_eq!(all[0].id, 1);
        assert_eq!(all[1].id, 2);
        assert_eq!(all[2].id, 3);
        assert!(!all[2].success); // The DELETE failed
    }

    #[test]
    fn security_manager_integration() {
        let mut mgr = SecurityManager::new();

        // Setup RLS
        mgr.rls.enable_rls("orders");
        mgr.rls.add_policy(RlsPolicy {
            name: "tenant_iso".into(),
            table: "orders".into(),
            command: PolicyCommand::All,
            target_roles: vec![],
            predicate: RlsPredicate::ColumnEqTenant {
                column: "org_id".into(),
                column_id: 0,
            },
            check_predicate: None,
            permissive: true,
        });

        // Setup masking
        mgr.masking.add_policy(MaskingPolicy {
            table: "orders".into(),
            column: "customer_email".into(),
            role: "support".into(),
            rule: MaskingRule::EmailMask,
            column_id: 0,
        });

        let ctx = SessionContext::new("agent")
            .with_role("support")
            .with_tenant("org_1");

        let row = make_row(&[
            ("id", "1"),
            ("org_id", "org_1"),
            ("customer_email", "john@example.com"),
        ]);

        // RLS check passes (same tenant)
        assert!(
            mgr.rls
                .check_row("orders", PolicyCommand::Select, &row, &ctx)
        );

        // Masking applies to support role
        let masked = mgr.masking.mask_row("orders", &row, &ctx);
        assert_eq!(masked["customer_email"], "j***@example.com");

        // Audit the access
        mgr.audit.log(
            &ctx.user,
            "SELECT",
            Some("orders"),
            "SELECT * FROM orders",
            1,
            true,
        );
        assert_eq!(mgr.audit.len(), 1);
    }

    // ── Per-tenant key isolation tests ──────────────────────────────

    #[test]
    fn tenant_key_manager_basic() {
        let mut mgr = TenantKeyManager::new();
        assert_eq!(mgr.tenant_count(), 0);
        let _id1 = mgr.register_tenant_key("t1", vec![1, 2, 3]);
        let _id2 = mgr.register_tenant_key("t2", vec![4, 5, 6]);
        assert_eq!(mgr.tenant_count(), 2);
        assert_eq!(mgr.get_active_key("t1").unwrap().1, &[1, 2, 3]);
        assert_eq!(mgr.get_active_key("t2").unwrap().1, &[4, 5, 6]);
        assert!(mgr.get_active_key("t3").is_none());
    }

    #[test]
    fn tenant_key_manager_default_fallback() {
        let mut mgr = TenantKeyManager::new();
        mgr.set_default_key(vec![10, 20, 30]);
        // Tenant without dedicated key should get default.
        let (_, key) = mgr.get_active_key("any_tenant").unwrap();
        assert_eq!(key, &[10, 20, 30]);
        // Tenant with dedicated key should get their own.
        mgr.register_tenant_key("special", vec![99, 88]);
        assert_eq!(mgr.get_active_key("special").unwrap().1, &[99, 88]);
    }

    #[test]
    fn tenant_key_rotation() {
        let mut mgr = TenantKeyManager::new();
        let id1 = mgr.register_tenant_key("t1", vec![1, 2, 3]);
        assert_eq!(mgr.key_history_count("t1"), 0);
        let id2 = mgr.rotate_key("t1", vec![4, 5, 6]).unwrap();
        assert_ne!(id1, id2);
        assert_eq!(mgr.key_history_count("t1"), 1);
        assert_eq!(mgr.get_active_key("t1").unwrap().1, &[4, 5, 6]);
        // Old key still retrievable by ID.
        assert_eq!(mgr.get_key_by_id(id1).unwrap(), &[1, 2, 3]);
    }

    #[test]
    fn tenant_key_rotation_nonexistent_fails() {
        let mut mgr = TenantKeyManager::new();
        assert!(mgr.rotate_key("missing", vec![1]).is_err());
    }

    #[test]
    fn tenant_key_revoke() {
        let mut mgr = TenantKeyManager::new();
        mgr.register_tenant_key("t1", vec![1, 2, 3]);
        mgr.rotate_key("t1", vec![4, 5, 6]).unwrap();
        assert!(mgr.revoke_tenant("t1"));
        assert!(mgr.get_active_key("t1").is_none());
        assert_eq!(mgr.tenant_count(), 0);
        assert!(!mgr.revoke_tenant("t1")); // Already revoked
    }

    #[test]
    fn tenant_key_get_by_id() {
        let mut mgr = TenantKeyManager::new();
        let default_id = mgr.set_default_key(vec![0, 0, 0]);
        let t1_id = mgr.register_tenant_key("t1", vec![1, 1, 1]);
        let t2_id = mgr.register_tenant_key("t2", vec![2, 2, 2]);
        assert_eq!(mgr.get_key_by_id(default_id).unwrap(), &[0, 0, 0]);
        assert_eq!(mgr.get_key_by_id(t1_id).unwrap(), &[1, 1, 1]);
        assert_eq!(mgr.get_key_by_id(t2_id).unwrap(), &[2, 2, 2]);
        assert!(mgr.get_key_by_id(999).is_none());
    }

    #[test]
    fn tenant_key_manager_list_tenants() {
        let mut mgr = TenantKeyManager::new();
        mgr.register_tenant_key("alpha", vec![1]);
        mgr.register_tenant_key("beta", vec![2]);
        mgr.register_tenant_key("gamma", vec![3]);
        let mut ids = mgr.tenant_ids();
        ids.sort();
        assert_eq!(ids, vec!["alpha", "beta", "gamma"]);
    }

    #[test]
    fn tenant_key_multiple_rotations() {
        let mut mgr = TenantKeyManager::new();
        mgr.register_tenant_key("t1", vec![1]);
        mgr.rotate_key("t1", vec![2]).unwrap();
        mgr.rotate_key("t1", vec![3]).unwrap();
        mgr.rotate_key("t1", vec![4]).unwrap();
        assert_eq!(mgr.key_history_count("t1"), 3);
        assert_eq!(mgr.get_active_key("t1").unwrap().1, &[4]);
    }

    // ── Key rotation tests ─────────────────────────────────────────

    #[test]
    fn key_rotation_create_key() {
        let mut mgr = KeyRotationManager::new();
        let id = mgr.create_key("AES-256-GCM", vec![1, 2, 3]);
        assert_eq!(id, 1);
        assert_eq!(mgr.key_count(), 1);
        let (kid, alg, mat) = mgr.active_key().unwrap();
        assert_eq!(kid, 1);
        assert_eq!(alg, "AES-256-GCM");
        assert_eq!(mat, &[1, 2, 3]);
    }

    #[test]
    fn key_rotation_begin_and_complete() {
        let mut mgr = KeyRotationManager::new();
        mgr.create_key("AES-256-GCM", vec![1, 2, 3]);
        let new_id = mgr.begin_rotation("AES-256-GCM", vec![4, 5, 6]).unwrap();
        assert_eq!(new_id, 2);
        assert!(matches!(
            mgr.rotation_state(),
            RotationState::InProgress { .. }
        ));

        mgr.update_progress(50);
        if let RotationState::InProgress { progress_pct, .. } = mgr.rotation_state() {
            assert_eq!(*progress_pct, 50);
        }

        mgr.complete_rotation(1000).unwrap();
        assert!(matches!(
            mgr.rotation_state(),
            RotationState::Completed { .. }
        ));
        assert_eq!(mgr.rotation_history().len(), 1);
        assert_eq!(mgr.rotation_history()[0].pages_re_encrypted, 1000);
        assert!(mgr.rotation_history()[0].completed_at_ms.is_some());
    }

    #[test]
    fn key_rotation_no_active_key_fails() {
        let mut mgr = KeyRotationManager::new();
        assert!(mgr.begin_rotation("AES-256-GCM", vec![1]).is_err());
    }

    #[test]
    fn key_rotation_double_begin_fails() {
        let mut mgr = KeyRotationManager::new();
        mgr.create_key("AES-256-GCM", vec![1]);
        mgr.begin_rotation("AES-256-GCM", vec![2]).unwrap();
        assert!(mgr.begin_rotation("AES-256-GCM", vec![3]).is_err());
    }

    #[test]
    fn key_rotation_cancel() {
        let mut mgr = KeyRotationManager::new();
        let old_id = mgr.create_key("AES-256-GCM", vec![1, 2, 3]);
        mgr.begin_rotation("AES-256-GCM", vec![4, 5, 6]).unwrap();
        mgr.cancel_rotation().unwrap();
        assert!(matches!(mgr.rotation_state(), RotationState::Idle));
        // Old key should be active again.
        let (kid, _, _) = mgr.active_key().unwrap();
        assert_eq!(kid, old_id);
        assert!(mgr.rotation_history().is_empty());
    }

    #[test]
    fn key_rotation_complete_without_begin_fails() {
        let mut mgr = KeyRotationManager::new();
        mgr.create_key("AES-256-GCM", vec![1]);
        assert!(mgr.complete_rotation(0).is_err());
    }

    #[test]
    fn key_rotation_get_key_by_id() {
        let mut mgr = KeyRotationManager::new();
        let id1 = mgr.create_key("AES-256-GCM", vec![1, 2, 3]);
        let id2 = mgr.create_key("AES-256-GCM", vec![4, 5, 6]);
        let (_, _, mat1) = mgr.get_key(id1).unwrap();
        assert_eq!(mat1, &[1, 2, 3]);
        let (_, _, mat2) = mgr.get_key(id2).unwrap();
        assert_eq!(mat2, &[4, 5, 6]);
        assert!(mgr.get_key(999).is_none());
    }

    #[test]
    fn key_rotation_multiple_rotations() {
        let mut mgr = KeyRotationManager::new();
        mgr.create_key("AES-256-GCM", vec![1]);
        mgr.begin_rotation("AES-256-GCM", vec![2]).unwrap();
        mgr.complete_rotation(100).unwrap();
        mgr.begin_rotation("AES-256-GCM", vec![3]).unwrap();
        mgr.complete_rotation(200).unwrap();
        assert_eq!(mgr.rotation_history().len(), 2);
        assert_eq!(mgr.key_count(), 3);
        let (_, _, mat) = mgr.active_key().unwrap();
        assert_eq!(mat, &[3]);
    }
}

#[cfg(test)]
mod masking_identity_tests {
    use super::*;

    fn mask(table: &str, column: &str) -> MaskingPolicy {
        MaskingPolicy {
            table: table.into(),
            column: column.into(),
            role: "analyst".into(),
            rule: MaskingRule::Redact("***".into()),
            column_id: 0,
        }
    }

    /// A renamed column must keep its mask. Losing it fails OPEN — the value is
    /// returned unmasked — which is the opposite direction from an RLS
    /// predicate losing its column, and the reason this is worth closing even
    /// while masking has no DDL surface yet.
    #[test]
    fn rename_column_keeps_the_mask_and_binds_the_id() {
        let mut engine = MaskingEngine::new();
        engine.add_policy(mask("people", "ssn"));

        assert!(engine.rename_column("people", 7, "ssn", "national_id"));
        let policy = &engine.all_policies()[0];
        assert_eq!(policy.column, "national_id");
        assert_eq!(
            policy.column_id, 7,
            "the id should be stamped while renaming"
        );

        // The escalation the id closes: recreate the old name. The mask must
        // stay on the column it was written for.
        assert!(!engine.rename_column("people", 9, "ssn", "decoy"));
        assert_eq!(engine.all_policies()[0].column, "national_id");
    }

    #[test]
    fn dropping_a_masked_column_removes_its_mask() {
        let mut engine = MaskingEngine::new();
        engine.add_policy(mask("people", "ssn"));
        assert_eq!(
            engine.masks_depending_on_column("people", 0, "ssn"),
            vec!["analyst".to_string()]
        );
        engine.drop_masks_for_column("people", 0, "ssn");
        assert!(engine.all_policies().is_empty());
    }

    /// A mask on another table must not be touched by this table's rename.
    #[test]
    fn rename_column_does_not_reach_other_tables() {
        let mut engine = MaskingEngine::new();
        engine.add_policy(mask("people", "ssn"));
        engine.add_policy(mask("staff", "ssn"));
        engine.rename_column("people", 7, "ssn", "national_id");
        let staff = engine
            .all_policies()
            .iter()
            .find(|p| p.table == "staff")
            .unwrap();
        assert_eq!(staff.column, "ssn");
    }
}
