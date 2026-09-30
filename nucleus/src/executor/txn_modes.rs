//! Transaction access mode and isolation level (X08, N11).
//!
//! `BEGIN [ISOLATION LEVEL x] [READ ONLY]`, `SET TRANSACTION ...` and
//! `SET SESSION CHARACTERISTICS AS TRANSACTION ...` used to be parsed and
//! dropped: an INSERT inside `BEGIN READ ONLY` succeeded and every level
//! reported `read committed`. This module owns the state and the checks.
//!
//! * The reported level is the one the session asked for (the statement's,
//!   else `default_transaction_isolation`, else `read committed`). Whether the
//!   engine can honour it is decided by `require_isolation_level`, which
//!   refuses a level the engine cannot provide; a level the engine serves
//!   with something stronger (REPEATABLE READ under strict 2PL) is still
//!   reported as requested, as PostgreSQL allows ("a level may provide
//!   stronger guarantees than requested").
//! * READ ONLY is enforced per statement before it runs, with the same
//!   fail-closed write classification the degraded-server gate uses
//!   (`admission::statement_mutates`), extended to the shapes that parse as a
//!   query but write (data-modifying CTEs, `FOR UPDATE`, sequence and
//!   specialty-store functions).

#[cfg(feature = "server")]
use sqlparser::ast::Visit;
use sqlparser::ast::{self, Statement};
use std::sync::atomic::Ordering;

#[cfg(feature = "server")]
use super::admission::scalar_fn_mutates;
use super::admission::{statement_label, statement_mutates};
use super::{ExecError, ExecResult, Executor};

/// Modes named by a `BEGIN` / `SET TRANSACTION` statement. `None` = not named.
#[derive(Default, Debug, Clone)]
pub(super) struct TxnModes {
    pub isolation: Option<&'static str>,
    pub read_only: Option<bool>,
}

impl TxnModes {
    pub(super) fn from_ast(modes: &[ast::TransactionMode]) -> Self {
        let mut out = Self::default();
        for mode in modes {
            match mode {
                ast::TransactionMode::IsolationLevel(level) => {
                    out.isolation = Some(match level {
                        ast::TransactionIsolationLevel::ReadCommitted
                        | ast::TransactionIsolationLevel::ReadUncommitted => "read committed",
                        ast::TransactionIsolationLevel::RepeatableRead => "repeatable read",
                        ast::TransactionIsolationLevel::Serializable => "serializable",
                        ast::TransactionIsolationLevel::Snapshot => "snapshot",
                    });
                }
                ast::TransactionMode::AccessMode(ast::TransactionAccessMode::ReadOnly) => {
                    out.read_only = Some(true);
                }
                ast::TransactionMode::AccessMode(ast::TransactionAccessMode::ReadWrite) => {
                    out.read_only = Some(false);
                }
            }
        }
        out
    }
}

/// The level name PostgreSQL reports for a requested one.
fn report_label(level: &str) -> &'static str {
    match level {
        "serializable" => "serializable",
        "repeatable read" | "snapshot" => "repeatable read",
        _ => "read committed",
    }
}

fn setting_flag(value: &str) -> bool {
    matches!(
        value
            .trim_matches(|c| c == '\'' || c == '"')
            .to_lowercase()
            .as_str(),
        "on" | "true" | "yes" | "1"
    )
}

impl Executor {
    /// The session's `default_transaction_isolation`, reported form.
    fn default_isolation_label(&self) -> &'static str {
        let session = self.current_session();
        let value = session
            .settings
            .read()
            .get("default_transaction_isolation")
            .cloned();
        match value {
            Some(v) => report_label(
                v.trim_matches(|c| c == '\'' || c == '"')
                    .to_lowercase()
                    .as_str(),
            ),
            None => "read committed",
        }
    }

    fn default_read_only(&self) -> bool {
        let session = self.current_session();
        session
            .settings
            .read()
            .get("default_transaction_read_only")
            .is_some_and(|v| setting_flag(v))
    }

    /// Resolve the modes a new transaction runs with and hand the isolation
    /// level to the engine. Returns `(reported level, read only)`.
    pub(super) fn resolve_txn_modes(
        &self,
        requested: &TxnModes,
    ) -> Result<(&'static str, bool), ExecError> {
        let default = self.default_isolation_label();
        let asked = requested.isolation.unwrap_or(default);
        // MVCC resets its next-transaction level to snapshot after BEGIN.
        // Set even the default so reported read committed also has read
        // committed visibility on every new transaction.
        self.require_isolation_level(asked)?;
        let read_only = requested
            .read_only
            .unwrap_or_else(|| self.default_read_only());
        Ok((report_label(asked), read_only))
    }

    /// `transaction_isolation` / `transaction_read_only` as the session sees
    /// them right now, or `None` for any other setting.
    pub(super) fn transaction_mode_setting(&self, name: &str) -> Option<String> {
        let session = self.current_session();
        let active = session.txn_active.load(Ordering::SeqCst);
        match name {
            "transaction_isolation" => Some(if active {
                (*session.txn_isolation.lock()).to_string()
            } else {
                self.default_isolation_label().to_string()
            }),
            "transaction_read_only" => Some(
                if if active {
                    session.txn_read_only.load(Ordering::SeqCst)
                } else {
                    self.default_read_only()
                } {
                    "on"
                } else {
                    "off"
                }
                .to_string(),
            ),
            "default_transaction_isolation" => Some(self.default_isolation_label().to_string()),
            "default_transaction_read_only" => Some(
                if self.default_read_only() {
                    "on"
                } else {
                    "off"
                }
                .to_string(),
            ),
            _ => None,
        }
    }

    /// Whether statements of the current session must not write.
    fn in_read_only_txn(&self) -> bool {
        let session = self.current_session();
        if session.txn_active.load(Ordering::SeqCst) {
            session.txn_read_only.load(Ordering::SeqCst)
        } else {
            self.default_read_only()
        }
    }

    /// Refuse a write inside a READ ONLY transaction (SQLSTATE 25006).
    pub(super) fn check_read_only(&self, stmt: &Statement) -> Result<(), ExecError> {
        if !self.in_read_only_txn() {
            return Ok(());
        }
        match statement_write_label(stmt) {
            Some(label) => Err(ExecError::ReadOnly(format!(
                "cannot execute {label} in a read-only transaction"
            ))),
            None => Ok(()),
        }
    }

    /// `SET TRANSACTION ...` and `SET SESSION CHARACTERISTICS AS TRANSACTION ...`.
    pub(super) async fn execute_set_transaction(
        &self,
        modes: &[ast::TransactionMode],
        session_scope: bool,
    ) -> Result<ExecResult, ExecError> {
        let requested = TxnModes::from_ast(modes);
        let session = self.current_session();
        if session_scope {
            // Session characteristics are the defaults of every later BEGIN.
            if let Some(level) = requested.isolation {
                self.require_isolation_level(level)?;
                session.guc_note_setting("default_transaction_isolation", false);
                session
                    .settings
                    .write()
                    .insert("default_transaction_isolation".into(), level.into());
            }
            if let Some(ro) = requested.read_only {
                session.guc_note_setting("default_transaction_read_only", false);
                session.settings.write().insert(
                    "default_transaction_read_only".into(),
                    if ro { "on" } else { "off" }.into(),
                );
            }
            return Ok(ExecResult::Command {
                tag: "SET".into(),
                rows_affected: 0,
            });
        }
        if !session.txn_active.load(Ordering::SeqCst) {
            // PostgreSQL warns and does nothing outside a transaction block.
            tracing::warn!("SET TRANSACTION can only be used in transaction blocks");
            return Ok(ExecResult::Command {
                tag: "SET".into(),
                rows_affected: 0,
            });
        }
        let before_any_query = session.txn_stmts.load(Ordering::SeqCst) == 0;
        if let Some(ro) = requested.read_only {
            if ro {
                session.txn_read_only.store(true, Ordering::SeqCst);
            } else if session.txn_read_only.load(Ordering::SeqCst) {
                if !before_any_query {
                    return Err(ExecError::Runtime(
                        "transaction read-write mode must be set before any query".into(),
                    ));
                }
                session.txn_read_only.store(false, Ordering::SeqCst);
            }
        }
        if let Some(level) = requested.isolation {
            let label = report_label(level);
            if label != *session.txn_isolation.lock() {
                if !before_any_query {
                    return Err(ExecError::Runtime(
                        "SET TRANSACTION ISOLATION LEVEL must be called before any query".into(),
                    ));
                }
                // Nothing has run, so the engine transaction can be reopened
                // at the new level without losing anything.
                self.require_isolation_level(level)?;
                if self.storage.supports_mvcc() {
                    self.storage.abort_txn().await?;
                    self.storage.begin_txn().await?;
                }
                *session.txn_isolation.lock() = label;
            }
        }
        Ok(ExecResult::Command {
            tag: "SET".into(),
            rows_affected: 0,
        })
    }
}

/// Whether `stmt` writes, and the name to report it under. `None` for reads,
/// session state and transaction control.
fn statement_write_label(stmt: &Statement) -> Option<&'static str> {
    match stmt {
        // `COPY ... TO` reads; `ANALYZE` refreshes statistics only.
        Statement::Copy { to: true, .. } | Statement::Analyze(_) => None,
        Statement::Explain {
            analyze: true,
            statement,
            ..
        } => statement_write_label(statement),
        Statement::Query(_) => query_write_label(stmt),
        other if statement_mutates(other) => Some(statement_label(other)),
        _ => None,
    }
}

#[cfg(feature = "server")]
struct WriteFinder {
    label: Option<&'static str>,
}

#[cfg(feature = "server")]
impl sqlparser::ast::Visitor for WriteFinder {
    type Break = ();

    fn pre_visit_statement(&mut self, stmt: &Statement) -> std::ops::ControlFlow<Self::Break> {
        // A data-modifying CTE or subquery body.
        if !matches!(stmt, Statement::Query(_)) && statement_mutates(stmt) {
            self.label = Some(statement_label(stmt));
            return std::ops::ControlFlow::Break(());
        }
        std::ops::ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &ast::Query) -> std::ops::ControlFlow<Self::Break> {
        if !query.locks.is_empty() {
            self.label = Some("SELECT FOR UPDATE");
            return std::ops::ControlFlow::Break(());
        }
        std::ops::ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &ast::Expr) -> std::ops::ControlFlow<Self::Break> {
        if let ast::Expr::Function(func) = expr {
            let name = func.name.to_string().to_uppercase();
            let bare = name.rsplit('.').next().unwrap_or(&name);
            if matches!(bare, "NEXTVAL" | "SETVAL") {
                self.label = Some("nextval()");
                return std::ops::ControlFlow::Break(());
            }
            if scalar_fn_mutates(bare) {
                self.label = Some("a data-modifying function");
                return std::ops::ControlFlow::Break(());
            }
        }
        std::ops::ControlFlow::Continue(())
    }
}

#[cfg(feature = "server")]
fn query_write_label(stmt: &Statement) -> Option<&'static str> {
    let mut finder = WriteFinder { label: None };
    let _ = stmt.visit(&mut finder);
    finder.label
}

#[cfg(not(feature = "server"))]
fn query_write_label(_stmt: &Statement) -> Option<&'static str> {
    None
}
