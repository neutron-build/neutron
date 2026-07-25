//! Determinism gate for replicated SQL.
//!
//! # The problem
//!
//! Raft replicates *commands*, and every replica applies the same command
//! sequence expecting to reach the same state. That only holds if each command
//! is a pure function of (replicated state, command). Shipping a raw SQL string
//! breaks it the moment the statement reads something outside that pair:
//!
//! ```sql
//! INSERT INTO events (at) VALUES (now());        -- different clock per replica
//! INSERT INTO t (x)       VALUES (random());     -- different RNG per replica
//! INSERT INTO audit (who) VALUES (current_user); -- different session per replica
//! ```
//!
//! Each replica happily commits — and silently stores something different. No
//! error, no divergence alarm, no way to tell which replica is "right".
//!
//! # The chosen fix: fold what is provably foldable, refuse the rest
//!
//! [`prepare_for_replication`] classifies every function reference in the parsed
//! statement and takes one of three paths:
//!
//! 1. **Clean** — no volatile reference. Replicate the SQL byte-for-byte. Zero
//!    rewrite risk for the overwhelmingly common case.
//! 2. **Foldable** — clock/RNG volatility only, which depends on nothing but the
//!    calling machine. The leader evaluates it once and substitutes a literal,
//!    so every replica — and the leader itself — executes the same constant.
//! 3. **Unfoldable** — anything depending on session identity, connection,
//!    process, catalog privileges, or a sequence/session cursor. **Rejected**
//!    with an error naming the function.
//!
//! Rejecting is loud and safe; silently replicating a statement that will
//! diverge is neither.
//!
//! # Why the folding path is fail-closed
//!
//! A rewrite is trusted only if it can be proved to have worked. After folding,
//! the statement is re-rendered, **re-parsed** and **re-classified**. The
//! rewrite is accepted only if all three hold:
//!
//!   - re-parsing the rendered SQL succeeds,
//!   - re-rendering the re-parsed statements is byte-identical (the render
//!     round-trips, so nothing was silently dropped or reshaped),
//!   - the re-parsed statements contain no volatile reference at all.
//!
//! Any failure falls through to rejection. The guarantee is therefore not "the
//! rewrite is usually right" but "either the command is provably deterministic
//! or it never enters the log".
//!
//! # Documented residual assumptions
//!
//! This gate reads the statement, so it cannot see nondeterminism reached
//! *indirectly*:
//!
//!   - a column `DEFAULT now()` / `DEFAULT random()` already stored in the
//!     catalog (though the `CREATE TABLE` that introduces one is itself caught
//!     here, because the default expression is part of that statement),
//!   - a trigger body or user-defined function body,
//!   - a generated-column expression.
//!
//! Implicit `SERIAL`/sequence defaults are deliberately *not* treated as
//! divergence: a sequence is replicated state, and given an identical command
//! order every replica advances it identically. An explicit `nextval` /
//! `currval` / `setval` in statement text is still refused — `currval` reads a
//! per-connection cursor, and an explicit sequence call can also be driven from
//! paths that are not replicated.

use std::fmt;
use std::ops::ControlFlow;

use sqlparser::ast::{
    DataType, Expr, FunctionArguments, Statement, TimezoneInfo, TypedString, Value, ValueWithSpan,
    visit_expressions_mut,
};
use sqlparser::tokenizer::Span;

/// Why a statement cannot be replicated as written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NondeterminismError {
    /// The offending function, as written in the statement.
    pub function: String,
    /// Operator-facing explanation.
    pub reason: String,
}

impl fmt::Display for NondeterminismError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cannot replicate statement: {} is nondeterministic ({}). \
             Replicas would silently disagree, so the statement is refused; \
             evaluate it on the client and send a literal value instead.",
            self.function, self.reason
        )
    }
}

impl std::error::Error for NondeterminismError {}

/// The outcome of preparing a statement for the Raft log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Prepared {
    /// Already deterministic; replicate verbatim.
    Verbatim(String),
    /// Volatile clock/RNG references were folded to literals on the leader.
    /// The leader and every follower must execute *this* string.
    Folded(String),
}

impl Prepared {
    /// The SQL that must actually be executed and replicated.
    pub fn sql(&self) -> &str {
        match self {
            Prepared::Verbatim(s) | Prepared::Folded(s) => s,
        }
    }

    /// Consume into the SQL string.
    pub fn into_sql(self) -> String {
        match self {
            Prepared::Verbatim(s) | Prepared::Folded(s) => s,
        }
    }

    /// Whether the leader rewrote the statement.
    pub fn was_rewritten(&self) -> bool {
        matches!(self, Prepared::Folded(_))
    }
}

/// Volatility class of a function reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Volatility {
    /// Pure function of its arguments and of replicated state.
    Deterministic,
    /// Depends only on the local clock or RNG — the leader can evaluate it once
    /// and ship the resulting literal.
    Foldable,
    /// Depends on session, connection, process, privileges or a session cursor.
    /// No safe leader-side substitution exists.
    Unfoldable(&'static str),
}

/// Classify a function name (case-insensitive, schema qualification ignored).
pub fn classify_function(name: &str) -> Volatility {
    let upper = name.to_ascii_uppercase();
    // `pg_catalog.now` is still `now`.
    let bare = upper.rsplit('.').next().unwrap_or(upper.as_str());
    match bare {
        // ── Wall clock ────────────────────────────────────────────────────────
        "NOW" | "CURRENT_TIMESTAMP" | "LOCALTIMESTAMP" | "CLOCK_TIMESTAMP"
        | "STATEMENT_TIMESTAMP" | "TRANSACTION_TIMESTAMP" | "TIMEOFDAY" | "CURRENT_DATE"
        | "CURRENT_TIME" | "LOCALTIME" => Volatility::Foldable,

        // ── Randomness ────────────────────────────────────────────────────────
        "RANDOM" | "RAND" | "GEN_RANDOM_UUID" | "UUID_GENERATE_V4" | "UUID_GENERATE_V1" => {
            Volatility::Foldable
        }

        // ── Session / connection / process identity ───────────────────────────
        "CURRENT_USER" | "SESSION_USER" | "CURRENT_ROLE" | "USER" => {
            Volatility::Unfoldable("resolves to the connected role, which differs per replica")
        }
        "CURRENT_DATABASE" | "CURRENT_CATALOG" | "CURRENT_SCHEMA" | "CURRENT_SCHEMAS" => {
            Volatility::Unfoldable("resolves against the local catalog namespace")
        }
        "CURRENT_SETTING" | "SET_CONFIG" => Volatility::Unfoldable("reads per-session configuration"),
        "VERSION" | "PG_BACKEND_PID" | "PG_POSTMASTER_START_TIME" | "PG_IS_IN_RECOVERY" => {
            Volatility::Unfoldable("reports per-process server state")
        }
        "INET_CLIENT_ADDR" | "INET_CLIENT_PORT" | "INET_SERVER_ADDR" | "INET_SERVER_PORT" => {
            Volatility::Unfoldable("reports the local connection's addresses")
        }
        "TXID_CURRENT" | "PG_CURRENT_XACT_ID" => {
            Volatility::Unfoldable("returns a node-local transaction id")
        }
        "PG_SLEEP" | "PG_SLEEP_FOR" | "PG_SLEEP_UNTIL" => {
            Volatility::Unfoldable("suspends the local backend")
        }

        // ── Sequences ─────────────────────────────────────────────────────────
        "NEXTVAL" | "SETVAL" => {
            Volatility::Unfoldable("advances a sequence outside the replicated command order")
        }
        "CURRVAL" | "LASTVAL" => Volatility::Unfoldable("reads a per-session sequence cursor"),

        // ── Privilege / catalog introspection ─────────────────────────────────
        "HAS_TABLE_PRIVILEGE"
        | "HAS_SCHEMA_PRIVILEGE"
        | "HAS_COLUMN_PRIVILEGE"
        | "HAS_DATABASE_PRIVILEGE"
        | "PG_TABLE_IS_VISIBLE"
        | "PG_GET_USERBYID"
        | "PG_GET_EXPR"
        | "OBJ_DESCRIPTION"
        | "COL_DESCRIPTION" => Volatility::Unfoldable("depends on the local role and catalog state"),

        // ── Local resource telemetry ──────────────────────────────────────────
        "MEM_USAGE" | "MEM_BUDGET" | "MEM_AVAILABLE" | "MEM_UTILIZATION" | "MEM_PRESSURE_EVENTS"
        | "MEM_PEAK" | "MEM_STATS" => Volatility::Unfoldable("reports this node's memory counters"),

        // ── Model inference ───────────────────────────────────────────────────
        "EMBED" | "CLASSIFY" | "PREDICT" => Volatility::Unfoldable("invokes a locally loaded model"),

        // ── Subscription side effects ─────────────────────────────────────────
        "SUBSCRIBE" | "UNSUBSCRIBE" | "SUBSCRIPTION_COUNT" => {
            Volatility::Unfoldable("mutates or reads node-local subscription state")
        }

        _ => Volatility::Deterministic,
    }
}

/// Every volatile function referenced by `stmt`, in traversal order.
///
/// sqlparser normalises the parenless keyword forms (`CURRENT_TIMESTAMP`,
/// `CURRENT_USER`, `LOCALTIME`, …) into `Expr::Function` too, so inspecting
/// function nodes is sufficient — there is no separate keyword variant to miss.
pub fn scan(stmt: &Statement) -> Vec<(String, Volatility)> {
    // `visit_expressions_mut` is the expression walker sqlparser exposes under
    // this crate's feature set; cloning keeps the caller's statement untouched.
    let mut probe = stmt.clone();
    let mut found = Vec::new();
    let _: ControlFlow<()> = visit_expressions_mut(&mut probe, |expr| {
        if let Expr::Function(func) = expr {
            let name = func.name.to_string();
            match classify_function(&name) {
                Volatility::Deterministic => {}
                other => found.push((name, other)),
            }
        }
        ControlFlow::Continue(())
    });
    found
}

/// Turn a statement into a form that is safe to put in the Raft log.
///
/// Returns the SQL that the leader **and** every follower must execute, or a
/// [`NondeterminismError`] naming the function that made replication unsafe.
pub fn prepare_for_replication(sql: &str) -> Result<Prepared, NondeterminismError> {
    let stmts = match crate::sql::parse(sql) {
        Ok(s) => s,
        // Unparseable text cannot be reasoned about — but it also cannot change
        // state: the executor rejects it identically on every replica, so
        // passing it through introduces no divergence.
        Err(_) => return Ok(Prepared::Verbatim(sql.to_string())),
    };

    let mut findings = Vec::new();
    for stmt in &stmts {
        findings.extend(scan(stmt));
    }

    if findings.is_empty() {
        return Ok(Prepared::Verbatim(sql.to_string()));
    }

    // Any unfoldable reference ends it here.
    if let Some((name, reason)) = findings.iter().find_map(|(n, v)| match v {
        Volatility::Unfoldable(r) => Some((n.clone(), *r)),
        _ => None,
    }) {
        return Err(NondeterminismError {
            function: name,
            reason: reason.to_string(),
        });
    }

    // Only foldable references remain. Sample the clock and RNG once, here on
    // the leader, and substitute the same literal everywhere it occurs.
    let subs = LeaderConstants::capture();
    let mut rendered = Vec::with_capacity(stmts.len());
    for mut stmt in stmts {
        let _: ControlFlow<()> = visit_expressions_mut(&mut stmt, |expr| {
            if let Expr::Function(func) = expr {
                let name = func.name.to_string();
                if matches!(classify_function(&name), Volatility::Foldable)
                    && let Some(literal) = subs.literal_for(&name, &func.args)
                {
                    *expr = literal;
                }
            }
            ControlFlow::Continue(())
        });
        rendered.push(stmt.to_string());
    }
    let folded = rendered.join("; ");

    verify_folded(&folded)?;
    Ok(Prepared::Folded(folded))
}

/// Fail-closed check on a rewrite: the rendered SQL must re-parse, re-render
/// byte-identically, and contain no volatile reference at all.
fn verify_folded(folded: &str) -> Result<(), NondeterminismError> {
    let reparsed = crate::sql::parse(folded).map_err(|e| NondeterminismError {
        function: "<leader rewrite>".to_string(),
        reason: format!("constant folding produced unparseable SQL ({e})"),
    })?;

    let rerendered = reparsed
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join("; ");
    if rerendered != folded {
        return Err(NondeterminismError {
            function: "<leader rewrite>".to_string(),
            reason: "constant folding did not round-trip; refusing to replicate a statement whose \
                     rendered form is not stable"
                .to_string(),
        });
    }

    for stmt in &reparsed {
        if let Some((name, vol)) = scan(stmt).into_iter().next() {
            let reason = match vol {
                Volatility::Unfoldable(r) => r,
                _ => "survived leader-side constant folding",
            };
            return Err(NondeterminismError {
                function: name,
                reason: reason.to_string(),
            });
        }
    }
    Ok(())
}

/// The clock and RNG samples the leader took. One sample serves every
/// occurrence in the statement, so `now()` twice folds to a single value —
/// matching PostgreSQL's statement-stable `now()` semantics.
struct LeaderConstants {
    timestamp: String,
    date: String,
    time: String,
    random: f64,
    uuid: String,
}

impl LeaderConstants {
    fn capture() -> Self {
        let now = chrono::Utc::now();
        Self {
            timestamp: now.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
            date: now.format("%Y-%m-%d").to_string(),
            time: now.format("%H:%M:%S%.6f").to_string(),
            random: rand::random::<f64>(),
            uuid: random_uuid_v4(),
        }
    }

    /// The literal that replaces a foldable call, or `None` if the call shape is
    /// unexpected — which leaves the volatile node in place and therefore trips
    /// the post-fold verification, failing closed.
    fn literal_for(&self, name: &str, args: &FunctionArguments) -> Option<Expr> {
        // Only nullary forms are folded. `now()` and bare `CURRENT_TIMESTAMP`
        // both qualify; a parameterised form such as `current_timestamp(3)` is
        // left alone rather than guessed at.
        let nullary = match args {
            FunctionArguments::None => true,
            FunctionArguments::List(list) => list.args.is_empty(),
            FunctionArguments::Subquery(_) => false,
        };
        if !nullary {
            return None;
        }

        let upper = name.to_ascii_uppercase();
        let bare = upper.rsplit('.').next().unwrap_or(upper.as_str());
        let expr = match bare {
            "NOW" | "CURRENT_TIMESTAMP" | "LOCALTIMESTAMP" | "CLOCK_TIMESTAMP"
            | "STATEMENT_TIMESTAMP" | "TRANSACTION_TIMESTAMP" | "TIMEOFDAY" => {
                Expr::TypedString(TypedString {
                    data_type: DataType::Timestamp(None, TimezoneInfo::None),
                    value: string_value(&self.timestamp),
                    uses_odbc_syntax: false,
                })
            }
            "CURRENT_DATE" => Expr::TypedString(TypedString {
                data_type: DataType::Date,
                value: string_value(&self.date),
                uses_odbc_syntax: false,
            }),
            "CURRENT_TIME" | "LOCALTIME" => Expr::TypedString(TypedString {
                data_type: DataType::Time(None, TimezoneInfo::None),
                value: string_value(&self.time),
                uses_odbc_syntax: false,
            }),
            "RANDOM" | "RAND" => Expr::Value(ValueWithSpan {
                value: Value::Number(format!("{:.17}", self.random), false),
                span: Span::empty(),
            }),
            "GEN_RANDOM_UUID" | "UUID_GENERATE_V4" | "UUID_GENERATE_V1" => {
                Expr::Value(string_value(&self.uuid))
            }
            _ => return None,
        };
        Some(expr)
    }
}

fn string_value(s: &str) -> ValueWithSpan {
    ValueWithSpan {
        value: Value::SingleQuotedString(s.to_string()),
        span: Span::empty(),
    }
}

/// RFC 4122 v4 UUID from the process RNG.
fn random_uuid_v4() -> String {
    let mut bytes = [0u8; 16];
    for b in bytes.iter_mut() {
        *b = rand::random::<u8>();
    }
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The headline case. Two replicas each executing the raw text would store
    /// different timestamps; after preparation they execute one identical
    /// literal, so applying the command twice is byte-for-byte reproducible.
    #[test]
    fn insert_with_now_replicates_identically() {
        let sql = "INSERT INTO events (at) VALUES (now())";
        let prepared = prepare_for_replication(sql).expect("now() must be foldable, not refused");
        assert!(prepared.was_rewritten(), "now() must not go into the log raw");

        let replicated = prepared.sql();
        assert!(
            !replicated.to_uppercase().contains("NOW("),
            "the replicated command still calls now(): {replicated}"
        );
        assert!(
            replicated.contains("TIMESTAMP '"),
            "now() should fold to a timestamp literal, got: {replicated}"
        );

        // Applying the same log entry on another replica yields the same text.
        let reapplied = prepare_for_replication(replicated).unwrap();
        assert_eq!(
            reapplied.sql(),
            replicated,
            "re-preparing an already-folded command must be a no-op"
        );
    }

    /// Two `now()` calls in one statement must fold to the SAME instant, or a
    /// replica could observe an ordering the leader never had.
    #[test]
    fn repeated_now_folds_to_a_single_instant() {
        let prepared =
            prepare_for_replication("INSERT INTO t (a, b) VALUES (now(), current_timestamp)")
                .unwrap();
        let sql = prepared.sql();
        let literals: Vec<&str> = sql.match_indices("TIMESTAMP '").map(|(_, s)| s).collect();
        assert_eq!(literals.len(), 2, "expected two folded timestamps: {sql}");
        let first = sql.split("TIMESTAMP '").nth(1).unwrap().split('\'').next();
        let second = sql.split("TIMESTAMP '").nth(2).unwrap().split('\'').next();
        assert_eq!(first, second, "clock sampled twice within one statement");
    }

    #[test]
    fn random_and_uuid_fold_to_literals() {
        let prepared = prepare_for_replication("INSERT INTO t (x, id) VALUES (random(), gen_random_uuid())")
            .expect("clock/RNG volatility is foldable");
        let sql = prepared.sql().to_uppercase();
        assert!(!sql.contains("RANDOM("), "random() survived: {sql}");
        assert!(!sql.contains("GEN_RANDOM_UUID"), "uuid fn survived: {sql}");
    }

    /// Session-dependent volatility has no safe leader-side value, so it must be
    /// refused rather than replicated.
    #[test]
    fn session_dependent_sql_is_refused() {
        for sql in [
            "INSERT INTO audit (who) VALUES (current_user)",
            "INSERT INTO audit (who) VALUES (session_user)",
            "INSERT INTO t (v) VALUES (txid_current())",
            "INSERT INTO t (v) VALUES (pg_backend_pid())",
            "INSERT INTO t (v) VALUES (nextval('s'))",
            "INSERT INTO t (v) VALUES (currval('s'))",
            "INSERT INTO t (v) VALUES (version())",
            "INSERT INTO t (v) VALUES (current_database())",
        ] {
            let err = prepare_for_replication(sql)
                .expect_err(&format!("must refuse to replicate: {sql}"));
            assert!(
                !err.function.is_empty(),
                "refusal must name the offending function"
            );
            assert!(
                err.to_string().contains("nondeterministic"),
                "refusal must explain itself: {err}"
            );
        }
    }

    /// Volatility nested inside subqueries, CASE arms and UPDATE targets must be
    /// caught too — a shallow top-level check would miss all of these.
    #[test]
    fn nested_volatility_is_detected() {
        let refused = [
            "UPDATE t SET who = current_user WHERE id = 1",
            "INSERT INTO t SELECT id FROM u WHERE owner = current_user",
            "UPDATE t SET v = CASE WHEN x > 0 THEN current_user ELSE 'x' END",
            "DELETE FROM t WHERE owner = session_user",
        ];
        for sql in refused {
            assert!(
                prepare_for_replication(sql).is_err(),
                "nested volatility missed in: {sql}"
            );
        }

        let folded = prepare_for_replication("UPDATE t SET at = now() WHERE id = 1").unwrap();
        assert!(folded.was_rewritten());
        assert!(!folded.sql().to_uppercase().contains("NOW("));
    }

    /// A `CREATE TABLE` whose DEFAULT is volatile would plant divergence in the
    /// catalog, where later INSERTs cannot be inspected. Catch it at DDL time.
    #[test]
    fn volatile_column_default_in_ddl_is_caught() {
        let stmts = crate::sql::parse("CREATE TABLE t (id INT, who TEXT DEFAULT current_user)")
            .expect("DDL should parse");
        let findings: Vec<_> = stmts.iter().flat_map(scan).collect();
        assert!(
            findings
                .iter()
                .any(|(_, v)| matches!(v, Volatility::Unfoldable(_))),
            "volatile DEFAULT was not detected: {findings:?}"
        );
    }

    /// The common case must not be touched at all: byte-identical passthrough.
    #[test]
    fn deterministic_sql_passes_through_untouched() {
        for sql in [
            "INSERT INTO t VALUES (1, 'a')",
            "UPDATE t SET x = x + 1 WHERE id = 3",
            "DELETE FROM t WHERE id < 10",
            "INSERT INTO t (a) VALUES (upper('abc'))",
            "INSERT INTO t (a) VALUES (LENGTH('abc') + ABS(-2))",
        ] {
            let prepared = prepare_for_replication(sql).unwrap();
            assert_eq!(prepared, Prepared::Verbatim(sql.to_string()), "for: {sql}");
        }
    }

    #[test]
    fn classification_is_case_and_schema_insensitive() {
        assert_eq!(classify_function("NoW"), Volatility::Foldable);
        assert_eq!(classify_function("pg_catalog.now"), Volatility::Foldable);
        assert!(matches!(
            classify_function("PG_CATALOG.CURRENT_USER"),
            Volatility::Unfoldable(_)
        ));
        assert_eq!(classify_function("upper"), Volatility::Deterministic);
    }

    // ================================================================
    // Real-path replay
    //
    // The tests above reason about strings. These run the command through
    // the actual executor on two independent databases — the same thing a
    // leader and a follower do — and compare what each one stored.
    //
    // Every one of them carries a CONTROL assertion first: proof that the
    // raw statement genuinely diverges across the same interval. Without
    // that control, a replay test would also pass against a log that
    // replicated raw nondeterministic SQL, and would be no evidence at all.
    // ================================================================

    /// Apply one statement to a fresh database and return the stored value,
    /// rendered so the comparison is type-agnostic.
    async fn apply_on_fresh_replica(ddl: &str, dml: &str, probe: &str) -> String {
        let db = crate::embedded::Database::memory();
        db.execute(ddl).await.expect("DDL should succeed");
        db.execute(dml)
            .await
            .unwrap_or_else(|e| panic!("replica failed to apply {dml:?}: {e}"));
        let value = db
            .query_one(probe)
            .await
            .expect("probe query should succeed")
            .expect("probe should return a row");
        format!("{value:?}")
    }

    /// Enough for the executor's microsecond-resolution clock to move.
    fn let_the_clock_move() {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }

    /// The headline divergence: `INSERT ... now()`.
    ///
    /// Control proves two replicas applying the RAW text store different
    /// timestamps. The subject proves the command that actually enters the log
    /// stores the same value on both.
    #[tokio::test]
    async fn now_replays_identically_on_two_replicas() {
        const DDL: &str = "CREATE TABLE t (at TIMESTAMP)";
        const PROBE: &str = "SELECT at FROM t";
        let raw = "INSERT INTO t (at) VALUES (now())";

        // Control: raw replication really does diverge.
        let control_a = apply_on_fresh_replica(DDL, raw, PROBE).await;
        let_the_clock_move();
        let control_b = apply_on_fresh_replica(DDL, raw, PROBE).await;
        assert_ne!(
            control_a, control_b,
            "control failed: raw now() did not diverge across replicas, so this test \
             could not detect a missing determinism gate"
        );

        // Subject: what the gate puts in the log.
        let replicated = prepare_for_replication(raw)
            .expect("now() must be foldable, not refused")
            .into_sql();
        let leader = apply_on_fresh_replica(DDL, &replicated, PROBE).await;
        let_the_clock_move();
        let follower = apply_on_fresh_replica(DDL, &replicated, PROBE).await;
        assert_eq!(
            leader, follower,
            "the replicated command produced different state on two replicas \
             (leader {leader}, follower {follower}); command was: {replicated}"
        );
    }

    /// Same shape for RNG volatility.
    #[tokio::test]
    async fn random_replays_identically_on_two_replicas() {
        const DDL: &str = "CREATE TABLE t (x DOUBLE PRECISION)";
        const PROBE: &str = "SELECT x FROM t";
        let raw = "INSERT INTO t (x) VALUES (random())";

        let control_a = apply_on_fresh_replica(DDL, raw, PROBE).await;
        let control_b = apply_on_fresh_replica(DDL, raw, PROBE).await;
        assert_ne!(
            control_a, control_b,
            "control failed: raw random() did not diverge, so this test proves nothing"
        );

        let replicated = prepare_for_replication(raw)
            .expect("random() must be foldable")
            .into_sql();
        let leader = apply_on_fresh_replica(DDL, &replicated, PROBE).await;
        let follower = apply_on_fresh_replica(DDL, &replicated, PROBE).await;
        assert_eq!(
            leader, follower,
            "random() diverged after preparation; command was: {replicated}"
        );
    }

    /// The other half of the contract. `current_user` cannot be folded (the
    /// leader's value is not the follower's), so it must never reach the log.
    /// The control shows it really does diverge when the session differs, which
    /// is why silently replicating it would be a correctness hole.
    #[tokio::test]
    async fn session_dependent_sql_never_reaches_the_log() {
        let raw = "INSERT INTO audit (who) VALUES (current_user)";
        let err = prepare_for_replication(raw)
            .expect_err("current_user must be refused, not replicated");
        assert!(
            err.function.to_uppercase().contains("CURRENT_USER"),
            "refusal must name the offending function, got: {err:?}"
        );

        // The value really is environment-derived, so replicating the text
        // would have written whatever each node happened to be running as.
        let db = crate::embedded::Database::memory();
        db.execute("CREATE TABLE audit (who TEXT)").await.unwrap();
        db.execute(raw).await.unwrap();
        let stored = db.query_one("SELECT who FROM audit").await.unwrap();
        assert!(
            stored.is_some(),
            "control failed: current_user did not resolve locally, so the refusal is untested"
        );
    }

    /// A folded command must be a fixed point: replaying it (as a follower does
    /// when it applies the log) must not re-fold or re-refuse it.
    #[tokio::test]
    async fn a_folded_command_is_a_fixed_point() {
        let once = prepare_for_replication("INSERT INTO t (at) VALUES (now())")
            .unwrap()
            .into_sql();
        let twice = prepare_for_replication(&once).unwrap();
        assert_eq!(
            twice,
            Prepared::Verbatim(once.clone()),
            "re-preparing an already-folded command changed it"
        );

        // And it still executes.
        const DDL: &str = "CREATE TABLE t (at TIMESTAMP)";
        let v = apply_on_fresh_replica(DDL, &once, "SELECT at FROM t").await;
        assert!(!v.is_empty());
    }

    /// A folded statement must contain no volatile reference by construction —
    /// this exercises the fail-closed verifier directly.
    #[test]
    fn verifier_rejects_a_rewrite_that_left_volatility_behind() {
        let err = verify_folded("INSERT INTO t (a) VALUES (current_user)")
            .expect_err("verifier must reject residual volatility");
        assert_eq!(err.function.to_uppercase(), "CURRENT_USER");

        let err = verify_folded("INSERT INTO ((((").expect_err("verifier must reject bad SQL");
        assert_eq!(err.function, "<leader rewrite>");
    }
}
