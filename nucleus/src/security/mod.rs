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

// ============================================================================
// Session context
// ============================================================================

/// Session context for policy evaluation.
#[derive(Debug, Clone)]
pub struct SessionContext {
    pub user: String,
    pub roles: Vec<String>,
    pub tenant_id: Option<String>,
    pub properties: HashMap<String, String>,
}

impl SessionContext {
    pub fn new(user: &str) -> Self {
        Self {
            user: user.to_string(),
            roles: Vec::new(),
            tenant_id: None,
            properties: HashMap::new(),
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

    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|r| r == role)
    }
}

// ============================================================================
// Row-Level Security
// ============================================================================

/// The operation type a policy applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyCommand {
    Select,
    Insert,
    Update,
    Delete,
    All,
}

/// A predicate that can be evaluated against a row.
#[derive(Debug, Clone)]
pub enum RlsPredicate {
    /// Column must equal a constant string value.
    ColumnEqStr { column: String, value: String },
    /// Column must equal the session's tenant_id.
    ColumnEqTenant { column: String },
    /// Column must equal the session's user.
    ColumnEqUser { column: String },
    /// The session must have a specific role.
    HasRole { role: String },
    /// AND of two predicates.
    And(Box<RlsPredicate>, Box<RlsPredicate>),
    /// OR of two predicates.
    Or(Box<RlsPredicate>, Box<RlsPredicate>),
    /// Always true (permissive default).
    AlwaysTrue,
    /// Always false (restrictive default).
    AlwaysFalse,
}

impl RlsPredicate {
    /// Evaluate the predicate against a row (column_name → value map) and session context.
    pub fn evaluate(&self, row: &HashMap<String, String>, ctx: &SessionContext) -> bool {
        match self {
            RlsPredicate::ColumnEqStr { column, value } => {
                row.get(column) == Some(value)
            }
            RlsPredicate::ColumnEqTenant { column } => {
                if let Some(tenant) = &ctx.tenant_id {
                    row.get(column) == Some(tenant)
                } else {
                    false
                }
            }
            RlsPredicate::ColumnEqUser { column } => {
                row.get(column) == Some(&ctx.user)
            }
            RlsPredicate::HasRole { role } => ctx.has_role(role),
            RlsPredicate::And(a, b) => a.evaluate(row, ctx) && b.evaluate(row, ctx),
            RlsPredicate::Or(a, b) => a.evaluate(row, ctx) || b.evaluate(row, ctx),
            RlsPredicate::AlwaysTrue => true,
            RlsPredicate::AlwaysFalse => false,
        }
    }
}

/// A row-level security policy.
#[derive(Debug, Clone)]
pub struct RlsPolicy {
    pub name: String,
    pub table: String,
    pub command: PolicyCommand,
    /// Roles this policy applies to (empty = all roles).
    pub target_roles: Vec<String>,
    /// The predicate that must be true for a row to be visible/writable.
    pub predicate: RlsPredicate,
    /// Whether this is a permissive or restrictive policy.
    pub permissive: bool,
}

/// Row-level security engine.
pub struct RlsEngine {
    /// table_name → list of policies
    policies: HashMap<String, Vec<RlsPolicy>>,
    /// Tables with RLS enabled.
    enabled_tables: std::collections::HashSet<String>,
}

impl Default for RlsEngine {
    fn default() -> Self {
        Self::new()
    }
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

    /// Check if RLS is enabled on a table.
    pub fn is_enabled(&self, table: &str) -> bool {
        self.enabled_tables.contains(table)
    }

    /// Add a policy.
    pub fn add_policy(&mut self, policy: RlsPolicy) {
        self.policies
            .entry(policy.table.clone())
            .or_default()
            .push(policy);
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
        if ctx.has_role("superuser") {
            return true;
        }

        // If RLS is not enabled on this table, allow all
        if !self.is_enabled(table) {
            return true;
        }

        let policies = match self.policies.get(table) {
            Some(p) => p,
            None => return true, // No policies = allow all
        };

        // Filter applicable policies
        let applicable: Vec<&RlsPolicy> = policies
            .iter()
            .filter(|p| {
                // Command match
                (p.command == command || p.command == PolicyCommand::All)
                // Role match (empty = all roles)
                && (p.target_roles.is_empty()
                    || p.target_roles.iter().any(|r| ctx.has_role(r)))
            })
            .collect();

        if applicable.is_empty() {
            // No applicable policies with RLS enabled = deny
            return false;
        }

        // Permissive policies: at least one must pass
        let permissive: Vec<&&RlsPolicy> =
            applicable.iter().filter(|p| p.permissive).collect();
        let restrictive: Vec<&&RlsPolicy> =
            applicable.iter().filter(|p| !p.permissive).collect();

        // If there are permissive policies, at least one must allow
        let permissive_pass = if permissive.is_empty() {
            true // No permissive policies = pass by default
        } else {
            permissive.iter().any(|p| p.predicate.evaluate(row, ctx))
        };

        // All restrictive policies must allow
        let restrictive_pass = restrictive
            .iter()
            .all(|p| p.predicate.evaluate(row, ctx));

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
#[derive(Debug, Clone)]
pub enum MaskingRule {
    /// Full redaction: replace with a constant.
    Redact(String),
    /// Email masking: t***@example.com
    EmailMask,
    /// Partial mask: show first N and last M characters.
    Partial { show_first: usize, show_last: usize, mask_char: char },
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
#[derive(Debug, Clone)]
pub struct MaskingPolicy {
    pub table: String,
    pub column: String,
    pub role: String,
    pub rule: MaskingRule,
}

/// Data masking engine.
pub struct MaskingEngine {
    /// (table, column, role) → masking rule
    policies: Vec<MaskingPolicy>,
}

impl Default for MaskingEngine {
    fn default() -> Self {
        Self::new()
    }
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

    /// Get the masking rule for a specific table/column/role combination.
    pub fn get_rule(&self, table: &str, column: &str, ctx: &SessionContext) -> &MaskingRule {
        for policy in &self.policies {
            if policy.table == table
                && policy.column == column
                && ctx.has_role(&policy.role)
            {
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
            self.key_history.entry(tenant_id.to_string()).or_default().push(old.clone());
        }
        self.active_keys.insert(tenant_id.to_string(), (key_id, key_bytes));
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
        self.default_key.as_ref().map(|(id, bytes)| (*id, bytes.as_slice()))
    }

    /// Get a specific key by key_id (searches active + history across all tenants).
    pub fn get_key_by_id(&self, key_id: u32) -> Option<&[u8]> {
        // Check default key.
        if let Some((id, bytes)) = &self.default_key {
            if *id == key_id { return Some(bytes.as_slice()); }
        }
        // Check active keys.
        for (id, bytes) in self.active_keys.values() {
            if *id == key_id { return Some(bytes.as_slice()); }
        }
        // Check key history.
        for history in self.key_history.values() {
            for (id, bytes) in history {
                if *id == key_id { return Some(bytes.as_slice()); }
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
    InProgress { old_key_id: u32, new_key_id: u32, progress_pct: u8 },
    Completed { old_key_id: u32, new_key_id: u32 },
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
        self.keys.iter().find(|k| k.3).map(|k| (k.0, k.1.as_str(), k.2.as_slice()))
    }

    /// Look up a key by its ID.
    pub fn get_key(&self, key_id: u32) -> Option<(u32, &str, &[u8])> {
        self.keys.iter().find(|k| k.0 == key_id).map(|k| (k.0, k.1.as_str(), k.2.as_slice()))
    }

    /// Begin a key rotation: create a new key and set state to InProgress.
    pub fn begin_rotation(&mut self, algorithm: &str, new_material: Vec<u8>) -> Result<u32, String> {
        if matches!(&self.state, RotationState::InProgress { .. }) {
            return Err("rotation already in progress".into());
        }
        let old_key_id = match self.active_key() {
            Some((id, _, _)) => id,
            None => return Err("no active key to rotate from".into()),
        };
        let new_key_id = self.create_key(algorithm, new_material);
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        self.state = RotationState::InProgress { old_key_id, new_key_id, progress_pct: 0 };
        self.rotation_history.push(RotationRecord {
            old_key_id, new_key_id, started_at_ms: ts, completed_at_ms: None, pages_re_encrypted: 0,
        });
        Ok(new_key_id)
    }

    /// Update progress percentage of an in-progress rotation.
    pub fn update_progress(&mut self, pct: u8) {
        if let RotationState::InProgress { old_key_id, new_key_id, .. } = self.state {
            self.state = RotationState::InProgress { old_key_id, new_key_id, progress_pct: pct };
        }
    }

    /// Finalize the current rotation.
    pub fn complete_rotation(&mut self, pages_re_encrypted: u64) -> Result<(), String> {
        if let RotationState::InProgress { old_key_id, new_key_id, .. } = self.state {
            let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
            self.state = RotationState::Completed { old_key_id, new_key_id };
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
        if let RotationState::InProgress { old_key_id, new_key_id, .. } = self.state {
            // Reactivate old key, deactivate new.
            for k in &mut self.keys {
                k.3 = k.0 == old_key_id;
            }
            self.state = RotationState::Idle;
            // Remove last history record (incomplete).
            if let Some(last) = self.rotation_history.last() {
                if last.old_key_id == old_key_id && last.new_key_id == new_key_id {
                    self.rotation_history.pop();
                }
            }
            Ok(())
        } else {
            Err("no rotation in progress".into())
        }
    }

    pub fn rotation_state(&self) -> &RotationState { &self.state }
    pub fn rotation_history(&self) -> &[RotationRecord] { &self.rotation_history }
    pub fn key_count(&self) -> usize { self.keys.len() }
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
            },
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
            permissive: true,
        });

        let ctx = SessionContext::new("admin").with_role("superuser");
        let row = make_row(&[("id", "1")]);
        // Superuser bypasses RLS
        assert!(engine.check_row("orders", PolicyCommand::Select, &row, &ctx));
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
            },
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
            },
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
            },
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
        });
        masking.add_policy(MaskingPolicy {
            table: "users".into(),
            column: "ssn".into(),
            role: "analyst".into(),
            rule: MaskingRule::Redact("***-**-****".into()),
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

        audit.log("alice", "SELECT", Some("users"), "SELECT * FROM users", 10, true);
        audit.log("bob", "INSERT", Some("orders"), "INSERT INTO orders ...", 1, true);
        audit.log("alice", "DELETE", Some("orders"), "DELETE FROM orders WHERE id=5", 1, false);

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
            predicate: RlsPredicate::ColumnEqTenant { column: "org_id".into() },
            permissive: true,
        });

        // Setup masking
        mgr.masking.add_policy(MaskingPolicy {
            table: "orders".into(),
            column: "customer_email".into(),
            role: "support".into(),
            rule: MaskingRule::EmailMask,
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
        assert!(mgr.rls.check_row("orders", PolicyCommand::Select, &row, &ctx));

        // Masking applies to support role
        let masked = mgr.masking.mask_row("orders", &row, &ctx);
        assert_eq!(masked["customer_email"], "j***@example.com");

        // Audit the access
        mgr.audit.log(&ctx.user, "SELECT", Some("orders"), "SELECT * FROM orders", 1, true);
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
        assert!(matches!(mgr.rotation_state(), RotationState::InProgress { .. }));

        mgr.update_progress(50);
        if let RotationState::InProgress { progress_pct, .. } = mgr.rotation_state() {
            assert_eq!(*progress_pct, 50);
        }

        mgr.complete_rotation(1000).unwrap();
        assert!(matches!(mgr.rotation_state(), RotationState::Completed { .. }));
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
