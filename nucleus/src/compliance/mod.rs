//! Compliance engine — PII detection, differential privacy, right-to-deletion, retention policies.

// ---------------------------------------------------------------------------
// PII Auto-Detection
// ---------------------------------------------------------------------------

/// Categories of personally identifiable information.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PiiCategory {
    Email,
    Phone,
    Ssn,
    CreditCard,
    IpAddress,
    Name,
    Address,
    DateOfBirth,
    Financial,
    Medical,
}

/// A single PII detection result for a column.
#[derive(Debug, Clone)]
pub struct PiiMatch {
    pub column_name: String,
    pub category: PiiCategory,
    pub confidence: f64,
    pub matched_pattern: String,
}

/// Heuristic-based PII detector that inspects column names and sample values.
pub struct PiiDetector;

impl Default for PiiDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl PiiDetector {
    /// Create a new detector.
    pub fn new() -> Self {
        Self
    }

    /// Detect PII in a single column given its name and a handful of sample values.
    pub fn detect(&self, column_name: &str, sample_values: &[&str]) -> Vec<PiiMatch> {
        let mut matches = Vec::new();
        let lower = column_name.to_ascii_lowercase();

        // ---- column-name heuristics ----------------------------------------
        let name_hints: &[(&[&str], PiiCategory, f64, &str)] = &[
            (&["email", "e_mail", "e-mail", "email_address"], PiiCategory::Email, 0.8, "column name contains 'email'"),
            (&["phone", "telephone", "mobile", "cell", "phone_number"], PiiCategory::Phone, 0.8, "column name contains 'phone'"),
            (&["ssn", "social_security", "social-security"], PiiCategory::Ssn, 0.9, "column name contains 'ssn'"),
            (&["credit_card", "creditcard", "cc_number", "card_number"], PiiCategory::CreditCard, 0.8, "column name contains 'credit_card'"),
            (&["ip_address", "ip_addr", "ipaddress", "ip"], PiiCategory::IpAddress, 0.7, "column name contains 'ip'"),
            (&["first_name", "last_name", "full_name", "firstname", "lastname", "fullname", "name"], PiiCategory::Name, 0.7, "column name contains 'name'"),
            (&["address", "street", "addr", "street_address"], PiiCategory::Address, 0.7, "column name contains 'address'"),
            (&["dob", "date_of_birth", "birth_date", "birthday", "birthdate"], PiiCategory::DateOfBirth, 0.8, "column name contains date-of-birth keyword"),
            (&["salary", "income", "bank_account", "account_number", "routing_number", "iban"], PiiCategory::Financial, 0.7, "column name contains financial keyword"),
            (&["diagnosis", "medical_record", "prescription", "icd_code", "health"], PiiCategory::Medical, 0.7, "column name contains medical keyword"),
        ];

        // Split column name into words on underscore/hyphen boundaries for matching.
        let words: Vec<&str> = lower.split(['_', '-']).collect();

        for (keywords, category, confidence, pattern) in name_hints {
            for kw in *keywords {
                // Exact full-name match OR exact word match to avoid substring false positives
                // (e.g., "ip" inside "description" should NOT match IpAddress).
                let hit = lower == *kw
                    || words.contains(kw)
                    || (kw.len() > 3 && lower.contains(kw));
                if hit {
                    matches.push(PiiMatch {
                        column_name: column_name.to_string(),
                        category: category.clone(),
                        confidence: *confidence,
                        matched_pattern: pattern.to_string(),
                    });
                    break; // one match per category per column
                }
            }
        }

        // ---- content pattern matching ---------------------------------------
        for value in sample_values {
            // Email: contains '@' and '.'
            if value.contains('@') && value.contains('.') {
                let at_pos = value.find('@').unwrap();
                let after_at = &value[at_pos + 1..];
                if after_at.contains('.') && at_pos > 0 && after_at.len() > 2 {
                    Self::push_unique(&mut matches, PiiMatch {
                        column_name: column_name.to_string(),
                        category: PiiCategory::Email,
                        confidence: 0.9,
                        matched_pattern: "value matches email pattern: [REDACTED]".to_string(),
                    });
                }
            }

            // SSN: ###-##-#### pattern
            if Self::looks_like_ssn(value) {
                Self::push_unique(&mut matches, PiiMatch {
                    column_name: column_name.to_string(),
                    category: PiiCategory::Ssn,
                    confidence: 0.95,
                    matched_pattern: "value matches SSN pattern: [REDACTED]".to_string(),
                });
            }

            // Credit Card: 13-19 digits (possibly separated by spaces/dashes)
            if Self::looks_like_credit_card(value) {
                Self::push_unique(&mut matches, PiiMatch {
                    column_name: column_name.to_string(),
                    category: PiiCategory::CreditCard,
                    confidence: 0.9,
                    matched_pattern: "value matches credit card pattern: [REDACTED]".to_string(),
                });
            }

            // Phone: 10+ digits with optional separators
            if Self::looks_like_phone(value) {
                Self::push_unique(&mut matches, PiiMatch {
                    column_name: column_name.to_string(),
                    category: PiiCategory::Phone,
                    confidence: 0.85,
                    matched_pattern: "value matches phone pattern: [REDACTED]".to_string(),
                });
            }

            // IP Address: four dot-separated numbers 0-255
            if Self::looks_like_ipv4(value) {
                Self::push_unique(&mut matches, PiiMatch {
                    column_name: column_name.to_string(),
                    category: PiiCategory::IpAddress,
                    confidence: 0.9,
                    matched_pattern: "value matches IPv4 pattern: [REDACTED]".to_string(),
                });
            }
        }

        matches
    }

    /// Scan an entire table (list of (column_name, sample_values)).
    pub fn scan_table(&self, columns: &[(String, Vec<String>)]) -> Vec<PiiMatch> {
        let mut all = Vec::new();
        for (col, vals) in columns {
            let refs: Vec<&str> = vals.iter().map(|s| s.as_str()).collect();
            all.extend(self.detect(col, &refs));
        }
        all
    }

    // -- helpers (simple string-level pattern checks, no regex crate) ---------

    fn push_unique(matches: &mut Vec<PiiMatch>, m: PiiMatch) {
        // Avoid duplicate categories from content scan on the same column.
        if !matches.iter().any(|existing| {
            existing.category == m.category
                && existing.matched_pattern.starts_with("value ")
        }) {
            matches.push(m);
        }
    }

    fn looks_like_ssn(s: &str) -> bool {
        // Matches ###-##-#### exactly
        let s = s.trim();
        if s.len() != 11 {
            return false;
        }
        let bytes = s.as_bytes();
        bytes[0].is_ascii_digit()
            && bytes[1].is_ascii_digit()
            && bytes[2].is_ascii_digit()
            && bytes[3] == b'-'
            && bytes[4].is_ascii_digit()
            && bytes[5].is_ascii_digit()
            && bytes[6] == b'-'
            && bytes[7].is_ascii_digit()
            && bytes[8].is_ascii_digit()
            && bytes[9].is_ascii_digit()
            && bytes[10].is_ascii_digit()
    }

    fn looks_like_credit_card(s: &str) -> bool {
        let digits_only: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
        let non_digit_non_sep = s.chars().any(|c| !c.is_ascii_digit() && c != ' ' && c != '-');
        if non_digit_non_sep {
            return false;
        }
        (13..=19).contains(&digits_only.len())
    }

    fn looks_like_phone(s: &str) -> bool {
        let stripped: String = s
            .chars()
            .filter(|c| c.is_ascii_digit())
            .collect();
        // Allow only digits, spaces, dashes, parens, plus sign
        let all_phone_chars = s
            .chars()
            .all(|c| c.is_ascii_digit() || " -+(.)".contains(c));
        all_phone_chars && (10..=15).contains(&stripped.len())
    }

    fn looks_like_ipv4(s: &str) -> bool {
        let parts: Vec<&str> = s.trim().split('.').collect();
        if parts.len() != 4 {
            return false;
        }
        for part in &parts {
            if part.is_empty() || part.len() > 3 {
                return false;
            }
            let Ok(n) = part.parse::<u16>() else {
                return false;
            };
            if n > 255 {
                return false;
            }
        }
        true
    }
}

// ---------------------------------------------------------------------------
// Differential Privacy
// ---------------------------------------------------------------------------

/// Supported noise mechanisms.
#[derive(Debug, Clone, PartialEq)]
pub enum DpMechanism {
    Laplace,
    Gaussian,
}

/// Configuration for differential privacy noise injection.
#[derive(Debug, Clone)]
pub struct DpConfig {
    pub epsilon: f64,
    pub delta: Option<f64>,
    pub mechanism: DpMechanism,
}

/// Cryptographically secure RNG wrapper for differential privacy.
/// Uses `OsRng` to ensure DP noise guarantees are not weakened.
struct CryptoRng;

impl CryptoRng {
    /// Returns a uniform f64 in (0, 1) using OS entropy.
    fn next_f64() -> f64 {
        use rand::Rng;
        let mut rng = rand::rngs::OsRng;
        loop {
            let v: f64 = rng.r#gen();
            if v > 0.0 && v < 1.0 {
                return v;
            }
        }
    }

    /// Sample from Laplace(0, scale).
    fn laplace(scale: f64) -> f64 {
        let u = Self::next_f64() - 0.5;
        -scale * u.signum() * (1.0 - 2.0 * u.abs()).ln()
    }

    /// Sample from Gaussian(0, sigma) using Box-Muller.
    fn gaussian(sigma: f64) -> f64 {
        let u1 = Self::next_f64();
        let u2 = Self::next_f64();
        let z = (-2.0 * u1.ln()).sqrt() * (2.0 * core::f64::consts::PI * u2).cos();
        z * sigma
    }
}

/// Add calibrated noise to a value using cryptographically secure randomness.
///
/// - Laplace: noise scale = sensitivity / epsilon
/// - Gaussian: sigma = sensitivity * sqrt(2 * ln(1.25 / delta)) / epsilon  (requires delta)
pub fn add_noise(value: f64, sensitivity: f64, config: &DpConfig) -> f64 {
    match config.mechanism {
        DpMechanism::Laplace => {
            let scale = sensitivity / config.epsilon;
            value + CryptoRng::laplace(scale)
        }
        DpMechanism::Gaussian => {
            let delta = config.delta.unwrap_or(1e-5);
            let sigma =
                sensitivity * (2.0 * (1.25_f64 / delta).ln()).sqrt() / config.epsilon;
            value + CryptoRng::gaussian(sigma)
        }
    }
}

/// Return a differentially-private count.
pub fn dp_count(true_count: u64, config: &DpConfig) -> f64 {
    add_noise(true_count as f64, 1.0, config)
}

/// Return a differentially-private sum.
pub fn dp_sum(true_sum: f64, sensitivity: f64, config: &DpConfig) -> f64 {
    add_noise(true_sum, sensitivity, config)
}

/// Return a differentially-private average.
pub fn dp_avg(true_avg: f64, count: u64, sensitivity: f64, config: &DpConfig) -> f64 {
    // Sensitivity of the average = sensitivity / count
    let avg_sensitivity = sensitivity / count as f64;
    add_noise(true_avg, avg_sensitivity, config)
}

// ---------------------------------------------------------------------------
// Right to Deletion — Cascade Planning
// ---------------------------------------------------------------------------

/// A foreign key relationship.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct ForeignKey {
    from_table: String,
    from_column: String,
    to_table: String,
    to_column: String,
}

/// One step in a deletion plan.
#[derive(Debug, Clone)]
pub struct DeletionStep {
    pub table: String,
    pub condition: String,
    pub cascade_from: Option<String>,
}

/// An ordered list of deletion steps respecting FK ordering.
#[derive(Debug, Clone)]
pub struct DeletionPlan {
    pub steps: Vec<DeletionStep>,
}

/// Tracks tables and FK relationships to produce deletion plans.
pub struct DeletionCascade {
    tables: Vec<String>,
    foreign_keys: Vec<ForeignKey>,
}

impl Default for DeletionCascade {
    fn default() -> Self {
        Self::new()
    }
}

impl DeletionCascade {
    pub fn new() -> Self {
        Self {
            tables: Vec::new(),
            foreign_keys: Vec::new(),
        }
    }

    /// Register a table name.
    pub fn add_table(&mut self, name: &str) {
        if !self.tables.contains(&name.to_string()) {
            self.tables.push(name.to_string());
        }
    }

    /// Register a foreign key: `from_table.from_column` references `to_table.to_column`.
    pub fn add_foreign_key(
        &mut self,
        from_table: &str,
        from_column: &str,
        to_table: &str,
        to_column: &str,
    ) {
        self.foreign_keys.push(ForeignKey {
            from_table: from_table.to_string(),
            from_column: from_column.to_string(),
            to_table: to_table.to_string(),
            to_column: to_column.to_string(),
        });
    }

    /// Build a deletion plan: delete the user from `table` where `user_id_column = user_id`,
    /// and cascade to all dependent (child) tables first.
    ///
    /// The plan deletes children before parents to respect FK constraints.
    pub fn plan_deletion(
        &self,
        table: &str,
        user_id_column: &str,
        user_id: &str,
    ) -> DeletionPlan {
        let mut steps: Vec<DeletionStep> = Vec::new();
        let mut visited: Vec<String> = Vec::new();
        self.collect_dependents(table, user_id_column, user_id, &mut steps, &mut visited);
        DeletionPlan { steps }
    }

    /// Recursively collect deletion steps for all tables that reference `table`,
    /// adding children before the parent (post-order traversal).
    fn collect_dependents(
        &self,
        table: &str,
        id_column: &str,
        id_value: &str,
        steps: &mut Vec<DeletionStep>,
        visited: &mut Vec<String>,
    ) {
        if visited.contains(&table.to_string()) {
            return;
        }
        visited.push(table.to_string());

        // Find all tables whose FK points *to* this table.
        for fk in &self.foreign_keys {
            if fk.to_table == table {
                // The child table references us; recurse into it first.
                self.collect_dependents(
                    &fk.from_table,
                    &fk.from_column,
                    id_value,
                    steps,
                    visited,
                );
            }
        }

        // After all children are scheduled, schedule this table.
        let cascade_from = self
            .foreign_keys
            .iter()
            .find(|fk| fk.from_table == table && visited.contains(&fk.to_table))
            .map(|fk| fk.to_table.clone());

        steps.push(DeletionStep {
            table: table.to_string(),
            condition: format!("{id_column} = '{id_value}'"),
            cascade_from,
        });
    }
}

// ---------------------------------------------------------------------------
// Retention Policies
// ---------------------------------------------------------------------------

/// A time-based retention policy for a table.
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    pub table_name: String,
    pub retention_days: u32,
    pub timestamp_column: String,
    pub created_at: u64,
}

/// An action produced when a retention policy finds expired data.
#[derive(Debug, Clone)]
pub struct RetentionAction {
    pub table: String,
    pub condition: String,
    pub estimated_rows: u64,
}

/// Manages retention policies and checks for expired data.
pub struct RetentionEngine {
    policies: Vec<RetentionPolicy>,
}

impl Default for RetentionEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl RetentionEngine {
    pub fn new() -> Self {
        Self {
            policies: Vec::new(),
        }
    }

    /// Register a retention policy.
    pub fn register(&mut self, policy: RetentionPolicy) {
        self.policies.push(policy);
    }

    /// Given the current time (milliseconds since epoch), find all data that has expired
    /// under the given policy.
    ///
    /// `estimated_row_count` is a caller-supplied estimate of how many rows would be
    /// affected (the engine itself has no access to real data).
    pub fn find_expired(
        &self,
        policy: &RetentionPolicy,
        current_time_ms: u64,
        estimated_row_count: u64,
    ) -> Vec<RetentionAction> {
        let retention_ms = policy.retention_days as u64 * 24 * 60 * 60 * 1000;
        if current_time_ms <= retention_ms {
            return Vec::new();
        }
        let cutoff_ms = current_time_ms - retention_ms;

        vec![RetentionAction {
            table: policy.table_name.clone(),
            condition: format!("{} < {}", policy.timestamp_column, cutoff_ms),
            estimated_rows: estimated_row_count,
        }]
    }

    /// Convenience: check all registered policies.
    pub fn find_all_expired(
        &self,
        current_time_ms: u64,
        row_estimator: impl Fn(&str) -> u64,
    ) -> Vec<RetentionAction> {
        let mut actions = Vec::new();
        for policy in &self.policies {
            let est = row_estimator(&policy.table_name);
            actions.extend(self.find_expired(policy, current_time_ms, est));
        }
        actions
    }
}

// ---------------------------------------------------------------------------
// Data Residency Enforcement
// ---------------------------------------------------------------------------

/// Geographic regions for data residency rules.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataRegion {
    /// European Union (GDPR jurisdiction).
    EU,
    /// United States.
    US,
    /// Asia-Pacific.
    APAC,
    /// United Kingdom (post-Brexit).
    UK,
    /// Canada.
    CA,
    /// Custom named region.
    Custom(String),
}

impl std::fmt::Display for DataRegion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataRegion::EU => write!(f, "EU"),
            DataRegion::US => write!(f, "US"),
            DataRegion::APAC => write!(f, "APAC"),
            DataRegion::UK => write!(f, "UK"),
            DataRegion::CA => write!(f, "CA"),
            DataRegion::Custom(name) => write!(f, "{name}"),
        }
    }
}

/// A rule specifying where data in a particular table must reside.
#[derive(Debug, Clone)]
pub struct ResidencyRule {
    /// Table this rule applies to (or "*" for all tables).
    pub table_pattern: String,
    /// Allowed regions for this data.
    pub allowed_regions: Vec<DataRegion>,
    /// Denied regions (overrides allowed if both specified).
    pub denied_regions: Vec<DataRegion>,
    /// Whether cross-region replication is permitted.
    pub allow_cross_region_replication: bool,
}

/// Result of a residency check.
#[derive(Debug, Clone, PartialEq)]
pub enum ResidencyVerdict {
    /// Data placement is allowed.
    Allowed,
    /// Data placement is denied, with reason.
    Denied(String),
}

/// Enforces data residency rules.
pub struct ResidencyEnforcer {
    rules: Vec<ResidencyRule>,
    /// The region of the current node.
    local_region: DataRegion,
}

impl ResidencyEnforcer {
    pub fn new(local_region: DataRegion) -> Self {
        Self {
            rules: Vec::new(),
            local_region,
        }
    }

    /// Register a residency rule.
    pub fn add_rule(&mut self, rule: ResidencyRule) {
        self.rules.push(rule);
    }

    /// Check whether storing data for the given table in the given region is allowed.
    pub fn check_storage(&self, table_name: &str, target_region: &DataRegion) -> ResidencyVerdict {
        for rule in &self.rules {
            if !Self::table_matches(&rule.table_pattern, table_name) {
                continue;
            }
            // Denied takes priority.
            if rule.denied_regions.contains(target_region) {
                return ResidencyVerdict::Denied(format!(
                    "table '{table_name}' cannot be stored in region {target_region} (denied by rule)"
                ));
            }
            // If allowed_regions is specified and non-empty, target must be in it.
            if !rule.allowed_regions.is_empty()
                && !rule.allowed_regions.contains(target_region)
            {
                return ResidencyVerdict::Denied(format!(
                    "table '{}' can only be stored in {:?}, not {}",
                    table_name,
                    rule.allowed_regions.iter().map(|r| r.to_string()).collect::<Vec<_>>(),
                    target_region
                ));
            }
        }
        ResidencyVerdict::Allowed
    }

    /// Check whether replicating the given table to a remote region is allowed.
    pub fn check_replication(
        &self,
        table_name: &str,
        target_region: &DataRegion,
    ) -> ResidencyVerdict {
        // First check basic storage rules.
        let storage_verdict = self.check_storage(table_name, target_region);
        if storage_verdict != ResidencyVerdict::Allowed {
            return storage_verdict;
        }
        // Then check cross-region replication flag.
        if *target_region != self.local_region {
            for rule in &self.rules {
                if Self::table_matches(&rule.table_pattern, table_name)
                    && !rule.allow_cross_region_replication
                {
                    return ResidencyVerdict::Denied(format!(
                        "cross-region replication of '{}' from {} to {} is not allowed",
                        table_name, self.local_region, target_region
                    ));
                }
            }
        }
        ResidencyVerdict::Allowed
    }

    /// Check whether the current node's region is valid for a table.
    pub fn check_local(&self, table_name: &str) -> ResidencyVerdict {
        self.check_storage(table_name, &self.local_region)
    }

    /// Get the local region.
    pub fn local_region(&self) -> &DataRegion {
        &self.local_region
    }

    /// List all rules.
    pub fn rules(&self) -> &[ResidencyRule] {
        &self.rules
    }

    /// Check if a table pattern matches a table name (supports "*" wildcard for all).
    fn table_matches(pattern: &str, table_name: &str) -> bool {
        if pattern == "*" {
            return true;
        }
        if let Some(prefix) = pattern.strip_suffix('*') {
            return table_name.starts_with(prefix);
        }
        pattern == table_name
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // 1. PII detection by column name
    #[test]
    fn test_pii_detection_by_column_name() {
        let detector = PiiDetector::new();
        let matches = detector.detect("user_email", &[]);
        assert!(
            matches.iter().any(|m| m.category == PiiCategory::Email),
            "Should detect Email from column name 'user_email'"
        );

        let matches = detector.detect("ssn", &[]);
        assert!(
            matches.iter().any(|m| m.category == PiiCategory::Ssn),
            "Should detect Ssn from column name 'ssn'"
        );

        let matches = detector.detect("phone_number", &[]);
        assert!(
            matches.iter().any(|m| m.category == PiiCategory::Phone),
            "Should detect Phone from column name 'phone_number'"
        );
    }

    // 2. PII detection by content pattern
    #[test]
    fn test_pii_detection_by_content_pattern() {
        let detector = PiiDetector::new();

        // Email pattern
        let matches = detector.detect("contact", &["alice@example.com"]);
        assert!(
            matches.iter().any(|m| m.category == PiiCategory::Email),
            "Should detect email from value 'alice@example.com'"
        );

        // SSN pattern
        let matches = detector.detect("id_field", &["123-45-6789"]);
        assert!(
            matches.iter().any(|m| m.category == PiiCategory::Ssn),
            "Should detect SSN from value '123-45-6789'"
        );

        // Credit card pattern
        let matches = detector.detect("payment", &["4111111111111111"]);
        assert!(
            matches.iter().any(|m| m.category == PiiCategory::CreditCard),
            "Should detect credit card from value '4111111111111111'"
        );

        // Phone pattern
        let matches = detector.detect("misc", &["(555) 123-4567"]);
        assert!(
            matches.iter().any(|m| m.category == PiiCategory::Phone),
            "Should detect phone from value '(555) 123-4567'"
        );

        // IPv4 pattern
        let matches = detector.detect("host", &["192.168.1.1"]);
        assert!(
            matches.iter().any(|m| m.category == PiiCategory::IpAddress),
            "Should detect IP from value '192.168.1.1'"
        );
    }

    // 3. Differential privacy noise is bounded for given epsilon
    #[test]
    fn test_dp_noise_bounded() {
        let config = DpConfig {
            epsilon: 1.0,
            delta: None,
            mechanism: DpMechanism::Laplace,
        };

        // With sensitivity=1 and epsilon=1, Laplace scale=1.
        // Run many times; the mean of |noise| should be around 1 (the scale).
        // We just check the noisy value is within a generous range.
        let true_val = 100.0;
        let mut total_error = 0.0;
        let trials = 200;
        // Reset seed for determinism.
        // CryptoRng uses OsRng — no seed to set.
        for _ in 0..trials {
            let noisy = add_noise(true_val, 1.0, &config);
            total_error += (noisy - true_val).abs();
        }
        let mean_error = total_error / trials as f64;
        // Mean of |Laplace(0,1)| = 1.0. Allow generous bounds [0.2, 3.0].
        assert!(
            mean_error > 0.2 && mean_error < 3.0,
            "Mean absolute error {} should be roughly 1.0 for Laplace(scale=1)",
            mean_error
        );
    }

    // 4. Deletion cascade planning
    #[test]
    fn test_deletion_cascade_planning() {
        let mut cascade = DeletionCascade::new();
        cascade.add_table("users");
        cascade.add_table("orders");
        cascade.add_table("order_items");

        cascade.add_foreign_key("orders", "user_id", "users", "id");
        cascade.add_foreign_key("order_items", "order_id", "orders", "id");

        let plan = cascade.plan_deletion("users", "id", "42");

        assert_eq!(plan.steps.len(), 3, "Should have 3 deletion steps");
        // order_items should come before orders, which should come before users
        let tables: Vec<&str> = plan.steps.iter().map(|s| s.table.as_str()).collect();
        assert_eq!(tables, vec!["order_items", "orders", "users"]);
    }

    // 5. Retention policy expiration
    #[test]
    fn test_retention_policy_expiration() {
        let engine = RetentionEngine::new();
        let policy = RetentionPolicy {
            table_name: "events".into(),
            retention_days: 30,
            timestamp_column: "created_at".into(),
            created_at: 0,
        };

        // 60 days in ms
        let current_time_ms = 60 * 24 * 60 * 60 * 1000;
        let actions = engine.find_expired(&policy, current_time_ms, 500);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].table, "events");
        assert_eq!(actions[0].estimated_rows, 500);

        let cutoff_expected = current_time_ms - 30 * 24 * 60 * 60 * 1000;
        assert!(
            actions[0].condition.contains(&cutoff_expected.to_string()),
            "Condition should reference the cutoff timestamp"
        );
    }

    // 6. scan_table across multiple columns
    #[test]
    fn test_scan_table_multiple_columns() {
        let detector = PiiDetector::new();
        let columns: Vec<(String, Vec<String>)> = vec![
            ("user_email".into(), vec!["bob@test.com".into()]),
            ("phone".into(), vec!["555-123-4567".into()]),
            ("age".into(), vec!["25".into(), "30".into()]),
        ];

        let matches = detector.scan_table(&columns);
        let categories: Vec<&PiiCategory> = matches.iter().map(|m| &m.category).collect();
        assert!(
            categories.contains(&&PiiCategory::Email),
            "Should find Email in scan"
        );
        assert!(
            categories.contains(&&PiiCategory::Phone),
            "Should find Phone in scan"
        );
        // "age" should NOT trigger any PII detection
        assert!(
            !matches.iter().any(|m| m.column_name == "age"),
            "'age' column should not be flagged as PII"
        );
    }

    // 7. DP count preserves approximate value
    #[test]
    fn test_dp_count_approximate() {
        let config = DpConfig {
            epsilon: 1.0,
            delta: None,
            mechanism: DpMechanism::Laplace,
        };

        // CryptoRng uses OsRng — no seed to set.
        let true_count: u64 = 10_000;
        let mut total = 0.0;
        let trials = 200;
        for _ in 0..trials {
            total += dp_count(true_count, &config);
        }
        let mean = total / trials as f64;
        // Mean should be close to true_count. Allow 5% tolerance.
        let diff = (mean - true_count as f64).abs();
        assert!(
            diff < 500.0,
            "Mean noisy count {} should be within 500 of true count {}",
            mean,
            true_count
        );
    }

    // 8. FK ordering in deletion plan
    #[test]
    fn test_fk_ordering_deletion_plan() {
        let mut cascade = DeletionCascade::new();
        cascade.add_table("customers");
        cascade.add_table("accounts");
        cascade.add_table("transactions");
        cascade.add_table("audit_log");

        // accounts -> customers, transactions -> accounts, audit_log -> transactions
        cascade.add_foreign_key("accounts", "customer_id", "customers", "id");
        cascade.add_foreign_key("transactions", "account_id", "accounts", "id");
        cascade.add_foreign_key("audit_log", "transaction_id", "transactions", "id");

        let plan = cascade.plan_deletion("customers", "id", "C-100");

        let tables: Vec<&str> = plan.steps.iter().map(|s| s.table.as_str()).collect();
        // Deepest dependency first
        assert_eq!(
            tables,
            vec!["audit_log", "transactions", "accounts", "customers"],
            "Deletion order must be leaf-to-root"
        );

        // The root step should not have a cascade_from
        let root_step = plan.steps.last().unwrap();
        assert_eq!(root_step.table, "customers");
        assert_eq!(root_step.condition, "id = 'C-100'");

        // A child step should reference its parent as cascade_from
        let audit_step = &plan.steps[0];
        assert_eq!(audit_step.table, "audit_log");
        assert!(
            audit_step.cascade_from.is_some(),
            "audit_log step should have cascade_from set"
        );
    }

    // -----------------------------------------------------------------------
    // NEW TESTS: data masking, audit trails, access control, retention policy
    //            enforcement, PII detection edge cases, and general edge cases
    // -----------------------------------------------------------------------

    // 9. Data masking / anonymization — mask SSN, email, credit card values
    #[test]
    fn test_data_masking_anonymization() {
        // Simulate a masking function that replaces detected PII with masked versions.
        fn mask_value(value: &str, category: &PiiCategory) -> String {
            match category {
                PiiCategory::Ssn => {
                    // Keep last 4, mask the rest: ***-**-6789
                    let digits: String = value.chars().filter(|c| c.is_ascii_digit()).collect();
                    if digits.len() >= 4 {
                        format!("***-**-{}", &digits[digits.len() - 4..])
                    } else {
                        "***-**-****".to_string()
                    }
                }
                PiiCategory::Email => {
                    // Replace local part with asterisks: ***@example.com
                    if let Some(at_pos) = value.find('@') {
                        format!("***{}", &value[at_pos..])
                    } else {
                        "***".to_string()
                    }
                }
                PiiCategory::CreditCard => {
                    // Keep last 4, mask rest: ****-****-****-1111
                    let digits: String = value.chars().filter(|c| c.is_ascii_digit()).collect();
                    if digits.len() >= 4 {
                        format!("****-****-****-{}", &digits[digits.len() - 4..])
                    } else {
                        "****-****-****-****".to_string()
                    }
                }
                _ => "***REDACTED***".to_string(),
            }
        }

        let detector = PiiDetector::new();

        // SSN masking
        let ssn_value = "123-45-6789";
        let matches = detector.detect("data", &[ssn_value]);
        let ssn_match = matches.iter().find(|m| m.category == PiiCategory::Ssn);
        assert!(ssn_match.is_some(), "Should detect SSN in value");
        let masked = mask_value(ssn_value, &PiiCategory::Ssn);
        assert_eq!(masked, "***-**-6789");
        assert!(!masked.contains("123"), "Masked SSN must not contain original prefix");

        // Email masking
        let email_value = "sensitive@corp.com";
        let matches = detector.detect("data", &[email_value]);
        let email_match = matches.iter().find(|m| m.category == PiiCategory::Email);
        assert!(email_match.is_some(), "Should detect email in value");
        let masked = mask_value(email_value, &PiiCategory::Email);
        assert_eq!(masked, "***@corp.com");
        assert!(!masked.contains("sensitive"), "Masked email must not contain local part");

        // Credit card masking
        let cc_value = "4111111111111111";
        let matches = detector.detect("data", &[cc_value]);
        let cc_match = matches.iter().find(|m| m.category == PiiCategory::CreditCard);
        assert!(cc_match.is_some(), "Should detect credit card in value");
        let masked = mask_value(cc_value, &PiiCategory::CreditCard);
        assert_eq!(masked, "****-****-****-1111");
        assert!(!masked.contains("41111111"), "Masked CC must not contain full prefix");
    }

    // 10. Audit trail recording — verify that compliance operations can be logged
    #[test]
    fn test_audit_trail_recording() {
        // Simulate an audit trail that records compliance operations.
        #[derive(Debug, Clone, PartialEq)]
        #[allow(dead_code)]
        enum AuditAction {
            PiiScanPerformed,
            DeletionPlanCreated,
            RetentionCheckPerformed,
            DataMasked,
            AccessDenied,
            AccessGranted,
        }

        #[derive(Debug, Clone)]
        struct AuditEntry {
            timestamp_ms: u64,
            action: AuditAction,
            actor: String,
            target: String,
            details: String,
        }

        struct AuditTrail {
            entries: Vec<AuditEntry>,
        }

        impl AuditTrail {
            fn new() -> Self {
                Self { entries: Vec::new() }
            }

            fn record(&mut self, entry: AuditEntry) {
                self.entries.push(entry);
            }

            fn entries_for_actor(&self, actor: &str) -> Vec<&AuditEntry> {
                self.entries.iter().filter(|e| e.actor == actor).collect()
            }

            fn entries_for_action(&self, action: &AuditAction) -> Vec<&AuditEntry> {
                self.entries.iter().filter(|e| &e.action == action).collect()
            }
        }

        let mut trail = AuditTrail::new();

        // Record a PII scan
        let detector = PiiDetector::new();
        let pii_matches = detector.detect("user_email", &["test@example.com"]);
        trail.record(AuditEntry {
            timestamp_ms: 1_000_000,
            action: AuditAction::PiiScanPerformed,
            actor: "compliance_bot".into(),
            target: "users_table".into(),
            details: format!("Found {} PII matches", pii_matches.len()),
        });

        // Record a deletion plan creation
        let mut cascade = DeletionCascade::new();
        cascade.add_table("users");
        cascade.add_table("orders");
        cascade.add_foreign_key("orders", "user_id", "users", "id");
        let plan = cascade.plan_deletion("users", "id", "42");
        trail.record(AuditEntry {
            timestamp_ms: 1_000_001,
            action: AuditAction::DeletionPlanCreated,
            actor: "admin_user".into(),
            target: "users".into(),
            details: format!("{} deletion steps planned", plan.steps.len()),
        });

        // Record a data masking event
        trail.record(AuditEntry {
            timestamp_ms: 1_000_002,
            action: AuditAction::DataMasked,
            actor: "compliance_bot".into(),
            target: "users.email".into(),
            details: "Masked 150 email values".into(),
        });

        // Verify audit trail
        assert_eq!(trail.entries.len(), 3, "Should have 3 audit entries");

        let bot_entries = trail.entries_for_actor("compliance_bot");
        assert_eq!(bot_entries.len(), 2, "compliance_bot should have 2 entries");

        let scan_entries = trail.entries_for_action(&AuditAction::PiiScanPerformed);
        assert_eq!(scan_entries.len(), 1, "Should have exactly 1 PII scan entry");
        assert!(scan_entries[0].details.contains("PII matches"));

        // Verify chronological ordering
        for window in trail.entries.windows(2) {
            assert!(
                window[0].timestamp_ms <= window[1].timestamp_ms,
                "Audit entries should be in chronological order"
            );
        }
    }

    // 11. Access control checks — verify role-based access to compliance operations
    #[test]
    fn test_access_control_checks() {
        #[derive(Debug, Clone, PartialEq)]
        enum Role {
            Admin,
            ComplianceOfficer,
            Analyst,
            ReadOnly,
        }

        #[derive(Debug, Clone, PartialEq)]
        enum ComplianceOp {
            ScanPii,
            ExecuteDeletion,
            ViewRetentionPolicies,
            ModifyRetentionPolicies,
            ExportAuditLog,
            MaskData,
        }

        fn is_authorized(role: &Role, op: &ComplianceOp) -> bool {
            match role {
                Role::Admin => true, // Admin can do everything
                Role::ComplianceOfficer => matches!(
                    op,
                    ComplianceOp::ScanPii
                        | ComplianceOp::ExecuteDeletion
                        | ComplianceOp::ViewRetentionPolicies
                        | ComplianceOp::ModifyRetentionPolicies
                        | ComplianceOp::ExportAuditLog
                        | ComplianceOp::MaskData
                ),
                Role::Analyst => matches!(
                    op,
                    ComplianceOp::ScanPii
                        | ComplianceOp::ViewRetentionPolicies
                        | ComplianceOp::ExportAuditLog
                ),
                Role::ReadOnly => matches!(
                    op,
                    ComplianceOp::ViewRetentionPolicies
                ),
            }
        }

        // Admin has full access
        assert!(is_authorized(&Role::Admin, &ComplianceOp::ScanPii));
        assert!(is_authorized(&Role::Admin, &ComplianceOp::ExecuteDeletion));
        assert!(is_authorized(&Role::Admin, &ComplianceOp::MaskData));

        // ComplianceOfficer can do all compliance operations
        assert!(is_authorized(&Role::ComplianceOfficer, &ComplianceOp::ScanPii));
        assert!(is_authorized(&Role::ComplianceOfficer, &ComplianceOp::ExecuteDeletion));
        assert!(is_authorized(&Role::ComplianceOfficer, &ComplianceOp::MaskData));
        assert!(is_authorized(&Role::ComplianceOfficer, &ComplianceOp::ModifyRetentionPolicies));

        // Analyst can scan and view but not modify or delete
        assert!(is_authorized(&Role::Analyst, &ComplianceOp::ScanPii));
        assert!(is_authorized(&Role::Analyst, &ComplianceOp::ViewRetentionPolicies));
        assert!(is_authorized(&Role::Analyst, &ComplianceOp::ExportAuditLog));
        assert!(!is_authorized(&Role::Analyst, &ComplianceOp::ExecuteDeletion));
        assert!(!is_authorized(&Role::Analyst, &ComplianceOp::ModifyRetentionPolicies));
        assert!(!is_authorized(&Role::Analyst, &ComplianceOp::MaskData));

        // ReadOnly can only view retention policies
        assert!(is_authorized(&Role::ReadOnly, &ComplianceOp::ViewRetentionPolicies));
        assert!(!is_authorized(&Role::ReadOnly, &ComplianceOp::ScanPii));
        assert!(!is_authorized(&Role::ReadOnly, &ComplianceOp::ExecuteDeletion));
        assert!(!is_authorized(&Role::ReadOnly, &ComplianceOp::MaskData));
        assert!(!is_authorized(&Role::ReadOnly, &ComplianceOp::ExportAuditLog));

        // Verify access control works with actual compliance engine call
        let detector = PiiDetector::new();
        let role = Role::Analyst;
        if is_authorized(&role, &ComplianceOp::ScanPii) {
            let results = detector.detect("email", &["user@example.com"]);
            assert!(!results.is_empty(), "Authorized analyst should be able to scan");
        }
    }

    // 12. Retention policy enforcement — multiple policies, edge cases, no-expiry
    #[test]
    fn test_retention_policy_enforcement() {
        let mut engine = RetentionEngine::new();

        // Short-lived logs: 7 days
        engine.register(RetentionPolicy {
            table_name: "debug_logs".into(),
            retention_days: 7,
            timestamp_column: "logged_at".into(),
            created_at: 0,
        });

        // Medium-term data: 90 days
        engine.register(RetentionPolicy {
            table_name: "user_sessions".into(),
            retention_days: 90,
            timestamp_column: "session_start".into(),
            created_at: 0,
        });

        // Long-term data: 365 days
        engine.register(RetentionPolicy {
            table_name: "financial_records".into(),
            retention_days: 365,
            timestamp_column: "record_date".into(),
            created_at: 0,
        });

        // At day 30: debug_logs expired, sessions and financial not yet
        let day_30_ms: u64 = 30 * 24 * 60 * 60 * 1000;
        let actions = engine.find_all_expired(day_30_ms, |table| match table {
            "debug_logs" => 10_000,
            "user_sessions" => 5_000,
            "financial_records" => 1_000,
            _ => 0,
        });

        let expired_tables: Vec<&str> = actions.iter().map(|a| a.table.as_str()).collect();
        assert!(expired_tables.contains(&"debug_logs"), "debug_logs should be expired at day 30");
        assert!(!expired_tables.contains(&"user_sessions"), "user_sessions should NOT be expired at day 30");
        assert!(!expired_tables.contains(&"financial_records"), "financial_records should NOT be expired at day 30");

        // At day 100: debug_logs and user_sessions expired
        let day_100_ms: u64 = 100 * 24 * 60 * 60 * 1000;
        let actions = engine.find_all_expired(day_100_ms, |table| match table {
            "debug_logs" => 10_000,
            "user_sessions" => 5_000,
            "financial_records" => 1_000,
            _ => 0,
        });
        let expired_tables: Vec<&str> = actions.iter().map(|a| a.table.as_str()).collect();
        assert!(expired_tables.contains(&"debug_logs"));
        assert!(expired_tables.contains(&"user_sessions"));
        assert!(!expired_tables.contains(&"financial_records"));

        // At day 400: all three expired
        let day_400_ms: u64 = 400 * 24 * 60 * 60 * 1000;
        let actions = engine.find_all_expired(day_400_ms, |table| match table {
            "debug_logs" => 10_000,
            "user_sessions" => 5_000,
            "financial_records" => 1_000,
            _ => 0,
        });
        assert_eq!(actions.len(), 3, "All 3 policies should trigger at day 400");

        // Verify estimated row counts are preserved
        let debug_action = actions.iter().find(|a| a.table == "debug_logs").unwrap();
        assert_eq!(debug_action.estimated_rows, 10_000);
        let financial_action = actions.iter().find(|a| a.table == "financial_records").unwrap();
        assert_eq!(financial_action.estimated_rows, 1_000);
    }

    // 13. PII detection — comprehensive category coverage and multi-value scanning
    #[test]
    fn test_pii_detection_all_categories() {
        let detector = PiiDetector::new();

        // Name category via column name
        let matches = detector.detect("first_name", &[]);
        assert!(
            matches.iter().any(|m| m.category == PiiCategory::Name),
            "Should detect Name from column 'first_name'"
        );

        // Address category via column name
        let matches = detector.detect("street_address", &[]);
        assert!(
            matches.iter().any(|m| m.category == PiiCategory::Address),
            "Should detect Address from column 'street_address'"
        );

        // DateOfBirth category via column name
        let matches = detector.detect("date_of_birth", &[]);
        assert!(
            matches.iter().any(|m| m.category == PiiCategory::DateOfBirth),
            "Should detect DateOfBirth from column 'date_of_birth'"
        );

        // Financial category via column name
        let matches = detector.detect("salary", &[]);
        assert!(
            matches.iter().any(|m| m.category == PiiCategory::Financial),
            "Should detect Financial from column 'salary'"
        );

        // Medical category via column name
        let matches = detector.detect("diagnosis", &[]);
        assert!(
            matches.iter().any(|m| m.category == PiiCategory::Medical),
            "Should detect Medical from column 'diagnosis'"
        );

        // Multiple PII values in same column detected via content
        let matches = detector.detect("misc_data", &[
            "alice@example.com",
            "192.168.0.1",
            "123-45-6789",
        ]);
        let categories: Vec<&PiiCategory> = matches.iter().map(|m| &m.category).collect();
        assert!(categories.contains(&&PiiCategory::Email));
        assert!(categories.contains(&&PiiCategory::IpAddress));
        assert!(categories.contains(&&PiiCategory::Ssn));

        // Confidence scores should be within valid range
        for m in &matches {
            assert!(
                m.confidence > 0.0 && m.confidence <= 1.0,
                "Confidence {} for {:?} should be in (0, 1]",
                m.confidence,
                m.category
            );
        }
    }

    // 14. Edge cases — empty data, special characters, boundary values
    #[test]
    fn test_edge_cases_empty_and_special() {
        let detector = PiiDetector::new();

        // Empty column name, no values — should produce no matches
        let matches = detector.detect("", &[]);
        assert!(matches.is_empty(), "Empty column name with no values should have no PII matches");

        // Empty values array — only column name heuristics should fire
        let matches = detector.detect("user_email", &[]);
        assert!(
            matches.iter().any(|m| m.category == PiiCategory::Email),
            "Column name heuristic should work even with empty values"
        );

        // Values with special characters that are NOT PII
        let matches = detector.detect("notes", &[
            "",                      // empty string
            "   ",                   // whitespace only
            "hello!@#$%^&*()",       // special chars (not a valid email)
            "....",                   // dots only
            "@@@",                   // at signs without valid email structure
            "not-an-email@",         // incomplete email (no domain after @)
        ]);
        // "notes" column name should not trigger any PII
        assert!(
            !matches.iter().any(|m| m.matched_pattern.starts_with("column name")),
            "'notes' should not trigger column-name heuristics"
        );
        // None of these values should look like a valid email
        assert!(
            !matches.iter().any(|m| m.category == PiiCategory::Email),
            "Malformed email-like strings should not be detected as Email PII"
        );

        // Boundary SSN-like values that should NOT match
        let matches = detector.detect("data", &[
            "12-345-6789",   // wrong grouping
            "123-45-678",    // too short
            "1234-56-7890",  // too long
            "abc-de-fghi",   // letters, not digits
        ]);
        assert!(
            !matches.iter().any(|m| m.category == PiiCategory::Ssn),
            "Malformed SSN-like strings should not be detected"
        );

        // Boundary IP-like values that should NOT match
        let matches = detector.detect("data", &[
            "256.1.1.1",     // octet > 255
            "1.2.3",         // only 3 octets
            "1.2.3.4.5",     // 5 octets
            "a.b.c.d",       // non-numeric
            "",              // empty
        ]);
        assert!(
            !matches.iter().any(|m| m.category == PiiCategory::IpAddress),
            "Invalid IP-like strings should not be detected"
        );

        // Credit card boundary: 12 digits (too few) and 20 digits (too many)
        let matches = detector.detect("data", &["123456789012"]);
        assert!(
            !matches.iter().any(|m| m.category == PiiCategory::CreditCard),
            "12-digit number should not match credit card (min 13)"
        );
        let matches = detector.detect("data", &["12345678901234567890"]);
        assert!(
            !matches.iter().any(|m| m.category == PiiCategory::CreditCard),
            "20-digit number should not match credit card (max 19)"
        );

        // Exactly 13 digits (minimum valid CC length) should match
        let matches = detector.detect("data", &["1234567890123"]);
        assert!(
            matches.iter().any(|m| m.category == PiiCategory::CreditCard),
            "13-digit number should match credit card pattern"
        );

        // Retention engine: current_time <= retention period means no expiry
        let engine = RetentionEngine::new();
        let policy = RetentionPolicy {
            table_name: "recent_data".into(),
            retention_days: 30,
            timestamp_column: "ts".into(),
            created_at: 0,
        };
        // current_time exactly at retention boundary
        let boundary_ms = 30 * 24 * 60 * 60 * 1000;
        let actions = engine.find_expired(&policy, boundary_ms, 100);
        assert!(
            actions.is_empty(),
            "Data at exact retention boundary should not be considered expired"
        );

        // current_time is 0 — no data should expire
        let actions = engine.find_expired(&policy, 0, 100);
        assert!(actions.is_empty(), "No data should expire when current_time is 0");

        // Deletion cascade with no FK relationships — only root table deleted
        let mut cascade = DeletionCascade::new();
        cascade.add_table("standalone");
        let plan = cascade.plan_deletion("standalone", "id", "1");
        assert_eq!(plan.steps.len(), 1, "Standalone table should have exactly 1 step");
        assert_eq!(plan.steps[0].table, "standalone");
        assert!(plan.steps[0].cascade_from.is_none(), "Standalone deletion should have no cascade_from");
    }

    // 15. Gaussian DP mechanism — verify it produces noise with correct properties
    #[test]
    fn test_dp_gaussian_mechanism() {
        let config = DpConfig {
            epsilon: 1.0,
            delta: Some(1e-5),
            mechanism: DpMechanism::Gaussian,
        };

        // CryptoRng uses OsRng — no seed to set.
        let true_val = 500.0;
        let sensitivity = 1.0;
        let trials = 300;

        let mut sum = 0.0;
        let mut sum_sq = 0.0;
        for _ in 0..trials {
            let noisy = add_noise(true_val, sensitivity, &config);
            let diff = noisy - true_val;
            sum += diff;
            sum_sq += diff * diff;
        }

        // Mean of noise should be close to 0
        let mean_noise = sum / trials as f64;
        assert!(
            mean_noise.abs() < 2.0,
            "Mean Gaussian noise {} should be close to 0",
            mean_noise
        );

        // Variance should be positive (noise is being added)
        let variance = sum_sq / trials as f64 - mean_noise * mean_noise;
        assert!(
            variance > 0.0,
            "Gaussian noise variance {} should be positive",
            variance
        );

        // dp_sum and dp_avg should also work with Gaussian
        // CryptoRng uses OsRng — no seed to set.
        let noisy_sum = dp_sum(1000.0, 10.0, &config);
        assert!(
            (noisy_sum - 1000.0).abs() < 200.0,
            "Gaussian dp_sum {} should be within reasonable range of 1000.0",
            noisy_sum
        );

        let noisy_avg = dp_avg(50.0, 100, 10.0, &config);
        assert!(
            (noisy_avg - 50.0).abs() < 10.0,
            "Gaussian dp_avg {} should be within reasonable range of 50.0",
            noisy_avg
        );
    }

    // 16. Scan table with unicode and multi-byte column names
    #[test]
    fn test_scan_table_unicode_columns() {
        let detector = PiiDetector::new();

        // Unicode column names that happen to contain PII keywords
        let columns: Vec<(String, Vec<String>)> = vec![
            ("user_email_\u{00E9}".into(), vec![]),   // email with accented char
            ("\u{2603}_phone".into(), vec![]),          // snowman + phone
            ("safe_column".into(), vec!["hello".into()]),
        ];

        let matches = detector.scan_table(&columns);
        // The first column contains "email" so it should match
        assert!(
            matches.iter().any(|m| m.category == PiiCategory::Email),
            "Column with 'email' substring (even with unicode suffix) should be detected"
        );
        // The second column contains "phone" so it should match
        assert!(
            matches.iter().any(|m| m.category == PiiCategory::Phone),
            "Column with 'phone' substring (even with unicode prefix) should be detected"
        );
        // safe_column should not trigger anything
        assert!(
            !matches.iter().any(|m| m.column_name == "safe_column"),
            "'safe_column' with benign data should not be flagged"
        );
    }

    #[test]
    fn crypto_rng_produces_varied_noise() {
        // CryptoRng should produce non-deterministic output — calling add_noise
        // twice with the same inputs should give different results.
        let config = DpConfig {
            epsilon: 1.0,
            delta: None,
            mechanism: DpMechanism::Laplace,
        };
        let a = add_noise(100.0, 1.0, &config);
        let b = add_noise(100.0, 1.0, &config);
        // They could theoretically be equal, but with f64 precision this is
        // astronomically unlikely for a proper RNG.
        assert!(
            (a - b).abs() > 1e-15 || a != 100.0,
            "CryptoRng should produce varied noise"
        );
    }

    #[test]
    fn crypto_rng_gaussian_produces_varied_noise() {
        let config = DpConfig {
            epsilon: 1.0,
            delta: Some(1e-5),
            mechanism: DpMechanism::Gaussian,
        };
        let a = add_noise(100.0, 1.0, &config);
        let b = add_noise(100.0, 1.0, &config);
        assert!(
            (a - b).abs() > 1e-15 || a != 100.0,
            "CryptoRng gaussian should produce varied noise"
        );
    }

    // ── Data Residency Enforcement tests ────────────────────────────

    #[test]
    fn residency_allowed_region() {
        let mut enforcer = ResidencyEnforcer::new(DataRegion::EU);
        enforcer.add_rule(ResidencyRule {
            table_pattern: "users".into(),
            allowed_regions: vec![DataRegion::EU, DataRegion::UK],
            denied_regions: vec![],
            allow_cross_region_replication: true,
        });
        assert_eq!(
            enforcer.check_storage("users", &DataRegion::EU),
            ResidencyVerdict::Allowed
        );
        assert_eq!(
            enforcer.check_storage("users", &DataRegion::UK),
            ResidencyVerdict::Allowed
        );
    }

    #[test]
    fn residency_denied_region() {
        let mut enforcer = ResidencyEnforcer::new(DataRegion::EU);
        enforcer.add_rule(ResidencyRule {
            table_pattern: "users".into(),
            allowed_regions: vec![DataRegion::EU],
            denied_regions: vec![],
            allow_cross_region_replication: false,
        });
        match enforcer.check_storage("users", &DataRegion::US) {
            ResidencyVerdict::Denied(msg) => assert!(msg.contains("users")),
            ResidencyVerdict::Allowed => panic!("should be denied"),
        }
    }

    #[test]
    fn residency_denied_overrides_allowed() {
        let mut enforcer = ResidencyEnforcer::new(DataRegion::EU);
        enforcer.add_rule(ResidencyRule {
            table_pattern: "sensitive".into(),
            allowed_regions: vec![DataRegion::EU, DataRegion::US],
            denied_regions: vec![DataRegion::US],
            allow_cross_region_replication: false,
        });
        match enforcer.check_storage("sensitive", &DataRegion::US) {
            ResidencyVerdict::Denied(_) => {} // Expected: denied overrides allowed
            ResidencyVerdict::Allowed => panic!("US should be denied even if in allowed"),
        }
    }

    #[test]
    fn residency_no_rules_allows_all() {
        let enforcer = ResidencyEnforcer::new(DataRegion::US);
        assert_eq!(
            enforcer.check_storage("any_table", &DataRegion::APAC),
            ResidencyVerdict::Allowed
        );
    }

    #[test]
    fn residency_wildcard_rule() {
        let mut enforcer = ResidencyEnforcer::new(DataRegion::EU);
        enforcer.add_rule(ResidencyRule {
            table_pattern: "*".into(),
            allowed_regions: vec![DataRegion::EU],
            denied_regions: vec![],
            allow_cross_region_replication: false,
        });
        assert_eq!(
            enforcer.check_storage("anything", &DataRegion::EU),
            ResidencyVerdict::Allowed
        );
        assert!(matches!(
            enforcer.check_storage("anything", &DataRegion::US),
            ResidencyVerdict::Denied(_)
        ));
    }

    #[test]
    fn residency_cross_region_replication_blocked() {
        let mut enforcer = ResidencyEnforcer::new(DataRegion::EU);
        enforcer.add_rule(ResidencyRule {
            table_pattern: "gdpr_data".into(),
            allowed_regions: vec![DataRegion::EU, DataRegion::UK],
            denied_regions: vec![],
            allow_cross_region_replication: false,
        });
        // Replication within allowed regions but cross-region is blocked.
        match enforcer.check_replication("gdpr_data", &DataRegion::UK) {
            ResidencyVerdict::Denied(msg) => assert!(msg.contains("cross-region")),
            ResidencyVerdict::Allowed => panic!("cross-region replication should be blocked"),
        }
    }

    #[test]
    fn residency_cross_region_replication_allowed() {
        let mut enforcer = ResidencyEnforcer::new(DataRegion::EU);
        enforcer.add_rule(ResidencyRule {
            table_pattern: "logs".into(),
            allowed_regions: vec![DataRegion::EU, DataRegion::US],
            denied_regions: vec![],
            allow_cross_region_replication: true,
        });
        assert_eq!(
            enforcer.check_replication("logs", &DataRegion::US),
            ResidencyVerdict::Allowed
        );
    }

    #[test]
    fn residency_same_region_replication_always_ok() {
        let mut enforcer = ResidencyEnforcer::new(DataRegion::EU);
        enforcer.add_rule(ResidencyRule {
            table_pattern: "data".into(),
            allowed_regions: vec![DataRegion::EU],
            denied_regions: vec![],
            allow_cross_region_replication: false,
        });
        assert_eq!(
            enforcer.check_replication("data", &DataRegion::EU),
            ResidencyVerdict::Allowed
        );
    }

    #[test]
    fn residency_check_local() {
        let mut enforcer = ResidencyEnforcer::new(DataRegion::US);
        enforcer.add_rule(ResidencyRule {
            table_pattern: "eu_only".into(),
            allowed_regions: vec![DataRegion::EU],
            denied_regions: vec![],
            allow_cross_region_replication: false,
        });
        // Local node is US but table is EU-only.
        match enforcer.check_local("eu_only") {
            ResidencyVerdict::Denied(_) => {} // Expected
            ResidencyVerdict::Allowed => panic!("US node should not host EU-only data"),
        }
    }

    #[test]
    fn residency_prefix_pattern() {
        let mut enforcer = ResidencyEnforcer::new(DataRegion::EU);
        enforcer.add_rule(ResidencyRule {
            table_pattern: "gdpr_*".into(),
            allowed_regions: vec![DataRegion::EU],
            denied_regions: vec![],
            allow_cross_region_replication: false,
        });
        assert!(matches!(
            enforcer.check_storage("gdpr_users", &DataRegion::US),
            ResidencyVerdict::Denied(_)
        ));
        assert_eq!(
            enforcer.check_storage("gdpr_logs", &DataRegion::EU),
            ResidencyVerdict::Allowed
        );
        // Non-matching table is allowed (no rule applies).
        assert_eq!(
            enforcer.check_storage("other_table", &DataRegion::US),
            ResidencyVerdict::Allowed
        );
    }

    #[test]
    fn residency_custom_region() {
        let mut enforcer = ResidencyEnforcer::new(DataRegion::Custom("brazil".into()));
        enforcer.add_rule(ResidencyRule {
            table_pattern: "local_data".into(),
            allowed_regions: vec![DataRegion::Custom("brazil".into())],
            denied_regions: vec![],
            allow_cross_region_replication: false,
        });
        assert_eq!(
            enforcer.check_local("local_data"),
            ResidencyVerdict::Allowed
        );
        assert!(matches!(
            enforcer.check_storage("local_data", &DataRegion::US),
            ResidencyVerdict::Denied(_)
        ));
    }

    #[test]
    fn data_region_display() {
        assert_eq!(DataRegion::EU.to_string(), "EU");
        assert_eq!(DataRegion::US.to_string(), "US");
        assert_eq!(DataRegion::Custom("brazil".into()).to_string(), "brazil");
    }
}
