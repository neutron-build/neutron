//! Administrative commands: SET/SHOW, GRANT/REVOKE, Cursors, LISTEN/NOTIFY.
//!
//! Extracted from `mod.rs` to reduce file size. All methods are `pub(super)` so
//! the main executor module can delegate to them.

use std::collections::HashMap;

use sqlparser::ast::{self, Visit};

use crate::fault::SubsystemHealth;
use crate::types::{DataType, Row, Value};

use super::helpers::{
    grantee_name, parse_grant_objects, parse_lock_timeout, parse_privileges, parse_time_zone,
};
use super::schema_types::{CursorDef, RoleDef, SeriesCursor};
use super::session::Session;
use super::types::ColMeta;
use super::{ExecError, ExecResult, Executor};

/// Rows a lazy FETCH produces and projects at a time. This bounds the working
/// set of a large FETCH and is the stride at which its result size is checked
/// against the cursor budgets.
const SERIES_CHUNK_ROWS: usize = 256;

/// Series values a lazy FETCH examines between polls of the session's cancel
/// flag, counting values a WHERE clause throws away, so a selective filter over
/// a long range stays cancellable.
const SERIES_CANCEL_POLL: u32 = 1024;

/// The identifier value of a (possibly quoted) object name, without the
/// delimiter quotes its Display rendering carries. Quotes delimit; they are
/// not part of the name.
pub(super) fn object_name_value(name: &ast::ObjectName) -> String {
    name.0
        .iter()
        .filter_map(|part| match part {
            ast::ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(".")
}

impl Executor {
    /// The principal an audit event should be attributed to: the effective
    /// role of the session running the statement.
    #[cfg(feature = "server")]
    pub(super) fn acting_principal(&self) -> String {
        self.current_session().session_context.read().user.clone()
    }

    pub(super) fn require_security_admin(&self, action: &str) -> Result<(), ExecError> {
        let session = self.current_session();
        let effective = session.session_context.read().user.clone();
        let roles = self.roles.try_read().map_err(|_| {
            ExecError::PermissionDenied(format!("cannot verify authority to {action}; retry"))
        })?;
        if roles.get(&effective).is_some_and(|r| r.is_superuser) {
            Ok(())
        } else {
            Err(ExecError::PermissionDenied(format!(
                "superuser authority is required to {action}"
            )))
        }
    }

    // ========================================================================
    // SET / SHOW
    // ========================================================================

    /// Push this session's `lock_timeout` setting to the storage engine, which
    /// applies it to this session's lock waits only. Called after every change
    /// to the setting, including the restores at COMMIT, ROLLBACK and
    /// ROLLBACK TO SAVEPOINT, so the engine never holds a value the session
    /// setting no longer has. Must run inside a statement (storage session
    /// scope); pool return and disconnect clear the entry by session id.
    pub(super) fn sync_lock_timeout(&self, session: &Session) {
        let ms = session
            .settings
            .read()
            .get("lock_timeout")
            .and_then(|v| parse_lock_timeout(v).ok());
        self.storage
            .set_session_lock_timeout_ms(super::unique_gate::gate_session_id(), ms);
    }

    /// Assume (or drop) a role. `local` ties the change to the open
    /// transaction; outside one, PostgreSQL warns and leaves the role alone.
    fn assign_role(session: &Session, local: bool, role: Option<String>) {
        if local && !session.guc_in_txn() {
            tracing::warn!("SET LOCAL ROLE can only be used in transaction blocks");
            return;
        }
        session.guc_note_role(local);
        *session.current_role.write() = role;
    }

    pub(super) fn execute_set(&self, set: ast::Set) -> Result<ExecResult, ExecError> {
        let session = self.current_session();
        match &set {
            ast::Set::SetRole {
                role_name,
                context_modifier,
            } => {
                let local = matches!(context_modifier, Some(ast::ContextModifier::Local));
                let Some(login_user) = session.authenticated_user.read().clone() else {
                    return Err(ExecError::PermissionDenied(
                        "session has no authenticated principal".into(),
                    ));
                };
                if let Some(role_name) = role_name {
                    let target = role_name.value.clone();
                    let roles = self.roles.try_read().map_err(|_| {
                        ExecError::PermissionDenied("role catalog is busy; retry SET ROLE".into())
                    })?;
                    let login = roles.get(&login_user).ok_or_else(|| {
                        ExecError::PermissionDenied("authenticated role no longer exists".into())
                    })?;
                    let mut reachable = login.member_of.clone();
                    let mut i = 0;
                    while i < reachable.len() {
                        let name = reachable[i].clone();
                        i += 1;
                        if let Some(role) = roles.get(&name) {
                            for parent in &role.member_of {
                                if !reachable.contains(parent) {
                                    reachable.push(parent.clone());
                                }
                            }
                        }
                    }
                    if !roles.contains_key(&target) {
                        return Err(ExecError::PermissionDenied(format!(
                            "role '{target}' does not exist"
                        )));
                    }
                    if !login.is_superuser && target != login_user && !reachable.contains(&target) {
                        return Err(ExecError::PermissionDenied(format!(
                            "permission denied to set role '{target}'"
                        )));
                    }
                    Self::assign_role(&session, local, Some(target));
                } else {
                    Self::assign_role(&session, local, None);
                }
                self.recompute_session_context(&session);
                return Ok(ExecResult::Command {
                    tag: "SET ROLE".into(),
                    rows_affected: 0,
                });
            }
            ast::Set::SetSessionAuthorization(param) => {
                let Some(login_user) = session.authenticated_user.read().clone() else {
                    return Err(ExecError::PermissionDenied(
                        "session has no authenticated principal".into(),
                    ));
                };
                let target = match &param.kind {
                    ast::SetSessionAuthorizationParamKind::Default => None,
                    ast::SetSessionAuthorizationParamKind::User(user) => Some(user.value.clone()),
                };
                if let Some(ref target) = target {
                    let roles = self.roles.try_read().map_err(|_| {
                        ExecError::PermissionDenied(
                            "role catalog is busy; retry SET SESSION AUTHORIZATION".into(),
                        )
                    })?;
                    let login = roles.get(&login_user).ok_or_else(|| {
                        ExecError::PermissionDenied("authenticated role no longer exists".into())
                    })?;
                    if !roles.contains_key(target) {
                        return Err(ExecError::PermissionDenied(format!(
                            "role '{target}' does not exist"
                        )));
                    }
                    if !login.is_superuser && target != &login_user {
                        return Err(ExecError::PermissionDenied(
                            "only a superuser may change session authorization".into(),
                        ));
                    }
                }
                Self::assign_role(
                    &session,
                    matches!(param.scope, ast::ContextModifier::Local),
                    target,
                );
                self.recompute_session_context(&session);
                return Ok(ExecResult::Command {
                    tag: "SET SESSION AUTHORIZATION".into(),
                    rows_affected: 0,
                });
            }
            // `SET [LOCAL] TIME ZONE x` is `SET [LOCAL] timezone = x`; it used
            // to fall through and store nothing.
            ast::Set::SetTimeZone { local, value } => {
                return self.execute_set(ast::Set::SingleAssignment {
                    scope: local.then_some(ast::ContextModifier::Local),
                    hivevar: false,
                    variable: ast::ObjectName::from(vec![ast::Ident::new("timezone")]),
                    values: vec![value.clone()],
                });
            }
            _ => {}
        }

        // Store SET values for SHOW to retrieve
        if let ast::Set::SingleAssignment {
            scope,
            variable,
            values,
            ..
        } = &set
        {
            let local = matches!(scope, Some(ast::ContextModifier::Local));
            let var_name = variable.to_string().to_lowercase();
            let val_str: Vec<String> = values.iter().map(|v| v.to_string()).collect();
            let mut val = val_str.join(", ");

            if matches!(var_name.as_str(), "session_authorization" | "role") {
                return Err(ExecError::PermissionDenied(format!(
                    "{var_name} must be changed with its dedicated, privilege-checked SET syntax"
                )));
            }
            if var_name == "nucleus.tenant_id" {
                return Err(ExecError::PermissionDenied(
                    "tenant identity can only be installed by a trusted authentication boundary"
                        .into(),
                ));
            }
            if var_name == "timezone" {
                val = if matches!(
                    val.trim().to_ascii_lowercase().as_str(),
                    "default" | "local"
                ) {
                    "UTC".to_string()
                } else {
                    parse_time_zone(&val)
                        .map_err(|_| {
                            ExecError::Runtime(format!(
                                "invalid value for parameter \"TimeZone\": \"{}\"",
                                val.trim().trim_matches(['\'', '"'])
                            ))
                        })?
                        .to_string()
                };
            }

            if matches!(
                var_name.as_str(),
                "transaction_isolation" | "transaction_read_only"
            ) {
                return Err(ExecError::Unsupported(
                    "SET transaction_isolation/transaction_read_only is not supported; use SET TRANSACTION ISOLATION LEVEL or SET TRANSACTION READ ONLY/READ WRITE".into(),
                ));
            }
            // Validate the default used by future transactions.
            if var_name == "default_transaction_isolation" {
                let level = val.trim_matches('\'').trim_matches('"').to_lowercase();
                // Same refusal as BEGIN ISOLATION LEVEL: a SET that reports
                // success and silently gives a weaker level is the same bug
                // through a different door.
                self.require_isolation_level(&level)?;
            }

            // `SET lock_timeout = '5s'` / `= 5000` — how long a SERIALIZABLE
            // transaction may block on a table lock before giving up. Same name
            // and same units (milliseconds) as PostgreSQL, 0 to disable.
            // Validated here; applied below, per session, with the setting
            // itself (`sync_lock_timeout`) so SET LOCAL and ROLLBACK scope it.
            if var_name == "lock_timeout" {
                parse_lock_timeout(&val)?;
            }

            // SET LOCAL is transaction-scoped: it is undone at COMMIT and
            // ROLLBACK (`Session::guc_commit` / `guc_rollback`). Outside a
            // transaction block PostgreSQL warns and does nothing.
            if local && !session.guc_in_txn() {
                tracing::warn!("SET LOCAL {var_name} can only be used in transaction blocks");
            } else {
                session.guc_note_setting(&var_name, local);
                session.settings.write().insert(var_name.clone(), val);
                if var_name == "lock_timeout" {
                    self.sync_lock_timeout(&session);
                }
            }
        }
        Ok(ExecResult::Command {
            tag: "SET".into(),
            rows_affected: 0,
        })
    }

    pub(super) async fn execute_show(
        &self,
        variable: Vec<ast::Ident>,
    ) -> Result<ExecResult, ExecError> {
        let var_name = variable
            .iter()
            .map(|i| i.value.clone())
            .collect::<Vec<_>>()
            .join(".");
        let var_lower = var_name.to_lowercase();

        // Handle SHOW ALL
        if var_lower == "all" {
            return self.execute_show_all();
        }

        // Check user-set values first. The value is cloned out so the
        // settings guard is released before any await below (the executor's
        // futures must stay Send).
        let sess = self.current_session();
        // The transaction's own mode, not whatever a SET stored under the name.
        if let Some(val) = self.transaction_mode_setting(&var_lower) {
            return Ok(ExecResult::Select {
                columns: vec![(var_name, DataType::Text)],
                rows: vec![vec![Value::Text(val)]],
            });
        }
        let user_val = sess.settings.read().get(&var_lower).cloned();
        if let Some(val) = user_val {
            return Ok(ExecResult::Select {
                columns: vec![(var_name, DataType::Text)],
                rows: vec![vec![Value::Text(val)]],
            });
        }

        // Handle special multi-word SHOW commands
        let var_upper = var_name.to_uppercase();
        match var_upper.as_str() {
            // The parsed-statement route (extended protocol / prepared SHOW)
            // lands here; the raw-text route lands in `execute`'s extension
            // arm. Both must answer identically or a client's Describe and
            // Execute disagree on the schema. The parsed form joins the
            // variable's words with dots (`SNAPSHOT.LEASE`).
            #[cfg(feature = "server")]
            "SNAPSHOT LEASE" | "SNAPSHOT.LEASE" => return self.execute_show_snapshot_lease(),
            "POOL_STATUS" | "POOL STATUS" => return self.show_pool_status(),
            "BUFFER_POOL" | "BUFFER POOL" => return self.show_buffer_pool(),
            "METRICS" => return self.show_metrics(),
            "INDEX_RECOMMENDATIONS" | "INDEX RECOMMENDATIONS" => {
                return self.show_index_recommendations();
            }
            "REPLICATION_STATUS" | "REPLICATION STATUS" => {
                return self.show_replication_status();
            }
            "SUBSYSTEM_HEALTH" | "SUBSYSTEM HEALTH" => {
                return self.show_subsystem_health();
            }
            "WAL_STATUS" | "WAL STATUS" => {
                return self.show_wal_status();
            }
            "TRANSACTIONS" => {
                return self.show_transactions().await;
            }
            "CACHE_STATS" | "CACHE STATS" => {
                return self.execute_cache_stats();
            }
            "CLUSTER_STATUS" | "CLUSTER STATUS" => {
                return self.show_cluster_status();
            }
            _ => {}
        }

        let value = match var_upper.as_str() {
            "SERVER_VERSION" => "16.0 (Nucleus)".to_string(),
            "SERVER_ENCODING" => "UTF8".to_string(),
            "CLIENT_ENCODING" => "UTF8".to_string(),
            "IS_SUPERUSER" => {
                let effective = sess.session_context.read().user.clone();
                if self
                    .roles
                    .try_read()
                    .ok()
                    .and_then(|roles| roles.get(&effective).map(|role| role.is_superuser))
                    .unwrap_or(false)
                {
                    "on".to_string()
                } else {
                    "off".to_string()
                }
            }
            "SESSION_AUTHORIZATION" => sess.authenticated_user.read().clone().unwrap_or_default(),
            "STANDARD_CONFORMING_STRINGS" => "on".to_string(),
            "TIMEZONE" => "UTC".to_string(),
            "DATESTYLE" => "ISO, MDY".to_string(),
            "INTEGER_DATETIMES" => "on".to_string(),
            "INTERVALSTYLE" => "postgres".to_string(),
            "SEARCH_PATH" => "\"$user\", public".to_string(),
            "MAX_CONNECTIONS" => "100".to_string(),
            "TRANSACTION_ISOLATION" => "read committed".to_string(),
            "DEFAULT_TRANSACTION_ISOLATION" => "read committed".to_string(),
            "LC_COLLATE" => "C".to_string(),
            "LC_CTYPE" => "en_US.UTF-8".to_string(),
            // Answered from the EFFECTIVE value -- session override over server
            // default -- not from whether this session happens to hold an
            // override. `SHOW synchronous_commit` returned "(not set)" on a
            // server whose default is `on`, so a client could not see the one
            // setting that decides whether a committed write survives a crash.
            // That is precisely why a durability report sat untriaged: the
            // setting "was never checked" because it could not be.
            "SYNCHRONOUS_COMMIT" => {
                if self.synchronous_commit_enabled() {
                    "on".to_string()
                } else {
                    "off".to_string()
                }
            }
            _ => "(not set)".to_string(),
        };

        Ok(ExecResult::Select {
            columns: vec![(var_name, DataType::Text)],
            rows: vec![vec![Value::Text(value)]],
        })
    }

    pub(super) async fn execute_show_tables(&self) -> Result<ExecResult, ExecError> {
        let names = self.catalog.table_names().await;
        let mut names_sorted = names;
        names_sorted.sort();
        let rows: Vec<Row> = names_sorted
            .into_iter()
            .map(|name| vec![Value::Text(name)])
            .collect();
        Ok(ExecResult::Select {
            columns: vec![("table_name".into(), DataType::Text)],
            rows,
        })
    }

    fn execute_show_all(&self) -> Result<ExecResult, ExecError> {
        // Return all settings as rows
        let sess = self.current_session();
        let settings = sess.settings.read();
        let mut rows = Vec::new();

        // Add default settings
        let effective = sess.session_context.read().user.clone();
        let is_superuser = self
            .roles
            .try_read()
            .ok()
            .and_then(|roles| roles.get(&effective).map(|role| role.is_superuser))
            .unwrap_or(false);
        let session_authorization = sess.authenticated_user.read().clone().unwrap_or_default();
        let defaults = vec![
            ("server_version", "16.0 (Nucleus)".to_string()),
            ("server_encoding", "UTF8".to_string()),
            ("client_encoding", "UTF8".to_string()),
            (
                "is_superuser",
                if is_superuser { "on" } else { "off" }.to_string(),
            ),
            ("session_authorization", session_authorization),
            ("standard_conforming_strings", "on".to_string()),
            ("timezone", "UTC".to_string()),
            ("datestyle", "ISO, MDY".to_string()),
            ("integer_datetimes", "on".to_string()),
            ("intervalstyle", "postgres".to_string()),
            ("search_path", "\"$user\", public".to_string()),
            ("max_connections", "100".to_string()),
            ("transaction_isolation", "read committed".to_string()),
            ("transaction_read_only", "off".to_string()),
            ("default_transaction_read_only", "off".to_string()),
            (
                "default_transaction_isolation",
                "read committed".to_string(),
            ),
            ("lc_collate", "en_US.UTF-8".to_string()),
            ("lc_ctype", "en_US.UTF-8".to_string()),
        ];

        for (name, value) in &defaults {
            // Check if user has overridden this setting
            let final_value = settings
                .get(*name)
                .map(|s| s.as_str())
                .unwrap_or(value.as_str());
            rows.push(vec![
                Value::Text(name.to_string()),
                Value::Text(final_value.to_string()),
                Value::Text("default".to_string()),
            ]);
        }

        // Add any user-set settings not in defaults
        for (name, value) in settings.iter() {
            if !defaults.iter().any(|(n, _)| n == name) {
                rows.push(vec![
                    Value::Text(name.clone()),
                    Value::Text(value.clone()),
                    Value::Text("user".to_string()),
                ]);
            }
        }

        Ok(ExecResult::Select {
            columns: vec![
                ("name".into(), DataType::Text),
                ("setting".into(), DataType::Text),
                ("description".into(), DataType::Text),
            ],
            rows,
        })
    }

    /// Display per-column statistics for a table collected by ANALYZE.
    /// Returns a result set with columns: column_name, distinct_count, null_count, min_value, max_value.
    pub(super) async fn show_table_stats(&self, table_name: &str) -> Result<ExecResult, ExecError> {
        // Verify the table exists
        let table_def = self
            .catalog
            .get_table(table_name)
            .await
            .ok_or_else(|| ExecError::TableNotFound(table_name.to_string()))?;

        let stats_opt = self.stats_store.get(table_name).await;
        let stats = match stats_opt {
            Some(s) => s,
            None => {
                return Err(ExecError::Unsupported(format!(
                    "no statistics available for table '{table_name}'; run ANALYZE {table_name} first"
                )));
            }
        };

        // Build rows in column definition order for deterministic output
        let mut result_rows: Vec<Row> = Vec::new();
        for col_def in &table_def.columns {
            let col_name = &col_def.name;
            if let Some(cs) = stats.column_stats.get(col_name) {
                // Compute null_count from null_fraction and row_count
                let null_count = (cs.null_fraction * stats.row_count as f64).round() as i64;
                result_rows.push(vec![
                    Value::Text(col_name.clone()),
                    Value::Int64(cs.distinct_count as i64),
                    Value::Int64(null_count),
                    match &cs.min_value {
                        Some(v) => Value::Text(v.clone()),
                        None => Value::Null,
                    },
                    match &cs.max_value {
                        Some(v) => Value::Text(v.clone()),
                        None => Value::Null,
                    },
                ]);
            }
        }

        Ok(ExecResult::Select {
            columns: vec![
                ("column_name".into(), DataType::Text),
                ("distinct_count".into(), DataType::Int64),
                ("null_count".into(), DataType::Int64),
                ("min_value".into(), DataType::Text),
                ("max_value".into(), DataType::Text),
            ],
            rows: result_rows,
        })
    }

    fn show_pool_status(&self) -> Result<ExecResult, ExecError> {
        let mvcc = self.storage.supports_mvcc();

        let mut rows = vec![
            vec![
                Value::Text("pool_mode".into()),
                Value::Text("session".into()),
            ],
            vec![
                Value::Text("mvcc_enabled".into()),
                Value::Text(mvcc.to_string()),
            ],
            vec![
                Value::Text("storage_engine".into()),
                Value::Text(
                    if mvcc {
                        "MvccStorageAdapter"
                    } else {
                        "MemoryEngine/DiskEngine"
                    }
                    .into(),
                ),
            ],
        ];

        // Report live connection pool stats if available
        #[cfg(feature = "server")]
        if let Some(ref pool) = self.conn_pool {
            let available = pool.available_permits();
            rows.push(vec![
                Value::Text("pool_available_permits".into()),
                Value::Text(available.to_string()),
            ]);
        } else {
            rows.push(vec![
                Value::Text("pool_status".into()),
                Value::Text("not wired".into()),
            ]);
        }
        #[cfg(not(feature = "server"))]
        rows.push(vec![
            Value::Text("pool_status".into()),
            Value::Text("not wired".into()),
        ]);

        Ok(ExecResult::Select {
            columns: vec![
                ("setting".into(), DataType::Text),
                ("value".into(), DataType::Text),
            ],
            rows,
        })
    }

    pub(super) fn show_cluster_status(&self) -> Result<ExecResult, ExecError> {
        #[cfg(feature = "server")]
        let rows = if let Some(ref cluster) = self.cluster {
            let status = cluster.read().status();
            let mode_str = match status.mode {
                crate::distributed::ClusterMode::Standalone => "standalone",
                crate::distributed::ClusterMode::PrimaryReplica => "primary-replica",
                crate::distributed::ClusterMode::MultiRaft => "multi-raft",
            };
            vec![
                vec![
                    Value::Text("node_id".into()),
                    Value::Text(format!("{:#x}", status.node_id)),
                ],
                vec![Value::Text("mode".into()), Value::Text(mode_str.into())],
                vec![
                    Value::Text("node_count".into()),
                    Value::Text(status.node_count.to_string()),
                ],
                vec![
                    Value::Text("shard_count".into()),
                    Value::Text(status.shard_count.to_string()),
                ],
                vec![
                    Value::Text("shards_led".into()),
                    Value::Text(status.shards_led.to_string()),
                ],
                vec![
                    Value::Text("epoch".into()),
                    Value::Text(status.epoch.to_string()),
                ],
                vec![
                    Value::Text("active_txns".into()),
                    Value::Text(status.active_txns.to_string()),
                ],
            ]
        } else {
            vec![
                vec![Value::Text("mode".into()), Value::Text("standalone".into())],
                vec![
                    Value::Text("cluster".into()),
                    Value::Text("not configured".into()),
                ],
            ]
        };
        #[cfg(not(feature = "server"))]
        let rows = vec![
            vec![Value::Text("mode".into()), Value::Text("standalone".into())],
            vec![
                Value::Text("cluster".into()),
                Value::Text("not configured".into()),
            ],
        ];

        Ok(ExecResult::Select {
            columns: vec![
                ("property".into(), DataType::Text),
                ("value".into(), DataType::Text),
            ],
            rows,
        })
    }

    pub(super) fn show_metrics(&self) -> Result<ExecResult, ExecError> {
        let metric_rows = self.metrics.as_rows();
        let rows: Vec<Row> = metric_rows
            .into_iter()
            .map(|(name, typ, val)| vec![Value::Text(name), Value::Text(typ), Value::Text(val)])
            .collect();
        Ok(ExecResult::Select {
            columns: vec![
                ("metric".into(), DataType::Text),
                ("type".into(), DataType::Text),
                ("value".into(), DataType::Text),
            ],
            rows,
        })
    }

    pub(super) fn show_buffer_pool(&self) -> Result<ExecResult, ExecError> {
        // Show buffer pool stats when running on DiskEngine.
        // Without direct access to the BufferPool from the executor, we report
        // that the stats are available via the storage engine's debug output.
        Ok(ExecResult::Select {
            columns: vec![
                ("metric".into(), DataType::Text),
                ("value".into(), DataType::Text),
            ],
            rows: vec![
                vec![
                    Value::Text("engine".into()),
                    Value::Text(
                        if self.storage.supports_mvcc() {
                            "mvcc"
                        } else {
                            "standard"
                        }
                        .into(),
                    ),
                ],
                vec![
                    Value::Text("supports_mvcc".into()),
                    Value::Text(self.storage.supports_mvcc().to_string()),
                ],
            ],
        })
    }

    pub(super) fn show_index_recommendations(&self) -> Result<ExecResult, ExecError> {
        let advisor = self.advisor.read();
        let recs = advisor.recommend();
        let rows: Vec<Row> = recs
            .iter()
            .map(|r| {
                vec![
                    Value::Text(r.table.clone()),
                    Value::Text(r.columns.join(", ")),
                    Value::Text(format!("{:?}", r.index_type)),
                    Value::Text(format!("{:.1}x", r.estimated_speedup)),
                    Value::Text(format!("{:?}", r.priority)),
                    Value::Text(r.reason.clone()),
                ]
            })
            .collect();
        Ok(ExecResult::Select {
            columns: vec![
                ("table".into(), DataType::Text),
                ("columns".into(), DataType::Text),
                ("index_type".into(), DataType::Text),
                ("speedup".into(), DataType::Text),
                ("priority".into(), DataType::Text),
                ("reason".into(), DataType::Text),
            ],
            rows,
        })
    }

    // The server-only live rows precede the always-present metrics rows. In the
    // core-only build cfg elimination makes this look like a trivial vec init,
    // but keeping one ordered construction avoids duplicating the result shape.
    #[allow(clippy::vec_init_then_push)]
    pub(super) fn show_replication_status(&self) -> Result<ExecResult, ExecError> {
        let mut result_rows: Vec<Row> = Vec::new();

        // If we have a live replication manager, show real status
        #[cfg(feature = "server")]
        if let Some(ref repl) = self.replication {
            let mgr = repl.read();
            let status = mgr.status();
            result_rows.push(vec![
                Value::Text("node_id".into()),
                Value::Text(status.node_id.to_string()),
            ]);
            result_rows.push(vec![
                Value::Text("role".into()),
                Value::Text(status.role.to_string()),
            ]);
            result_rows.push(vec![
                Value::Text("mode".into()),
                Value::Text(format!("{:?}", status.mode)),
            ]);
            result_rows.push(vec![
                Value::Text("wal_lsn".into()),
                Value::Text(status.wal_lsn.to_string()),
            ]);
            result_rows.push(vec![
                Value::Text("applied_lsn".into()),
                Value::Text(status.applied_lsn.to_string()),
            ]);
            result_rows.push(vec![
                Value::Text("replication_lag".into()),
                Value::Text(status.replication_lag.to_string()),
            ]);
            result_rows.push(vec![
                Value::Text("peer_connected".into()),
                Value::Text(status.peer_connected.to_string()),
            ]);
        }

        // Always include metrics-based counters
        result_rows.push(vec![
            Value::Text("replication_lag_bytes".into()),
            Value::Text(self.metrics.replication_lag_bytes.get().to_string()),
        ]);
        result_rows.push(vec![
            Value::Text("wal_bytes_written".into()),
            Value::Text(self.metrics.wal_bytes_written.get().to_string()),
        ]);
        result_rows.push(vec![
            Value::Text("wal_syncs".into()),
            Value::Text(self.metrics.wal_syncs.get().to_string()),
        ]);

        Ok(ExecResult::Select {
            columns: vec![
                ("metric".into(), DataType::Text),
                ("value".into(), DataType::Text),
            ],
            rows: result_rows,
        })
    }

    pub(super) fn show_subsystem_health(&self) -> Result<ExecResult, ExecError> {
        let health = self.subsystem_health();
        let rows: Vec<Row> = health
            .iter()
            .map(|(name, status)| {
                let status_str = match status {
                    SubsystemHealth::Healthy => "healthy",
                    SubsystemHealth::Degraded(_) => "degraded",
                    SubsystemHealth::Failed(_) => "failed",
                };
                vec![
                    Value::Text(name.clone()),
                    Value::Text(status_str.to_string()),
                ]
            })
            .collect();
        Ok(ExecResult::Select {
            columns: vec![
                ("subsystem".into(), DataType::Text),
                ("status".into(), DataType::Text),
            ],
            rows,
        })
    }

    /// WAL/checkpoint status from the storage engine, without needing a
    /// replication manager. This is the single-node answer to "why is the
    /// WAL growing": the current LSN, the checkpoint horizon below which
    /// segments are reclaimable, and the bytes actually on disk. The
    /// cumulative bytes/syncs counters come from the engine, not the metrics
    /// registry, so they are exact at statement time rather than up to one
    /// sync-interval stale.
    pub(super) fn show_wal_status(&self) -> Result<ExecResult, ExecError> {
        let current_lsn = self.storage.current_wal_lsn();
        let checkpoint_lsn = self.storage.wal_checkpoint_lsn();
        let size_bytes = self.storage.wal_size_bytes();
        let (bytes_written, syncs) = self.storage.wal_stats();
        let pair = |k: &str, v: u64| vec![Value::Text(k.to_string()), Value::Text(v.to_string())];
        Ok(ExecResult::Select {
            columns: vec![
                ("metric".into(), DataType::Text),
                ("value".into(), DataType::Text),
            ],
            rows: vec![
                pair("current_lsn", current_lsn),
                pair("checkpoint_lsn", checkpoint_lsn),
                pair("wal_size_bytes", size_bytes),
                pair("wal_bytes_written_total", bytes_written),
                pair("wal_syncs_total", syncs),
            ],
        })
    }

    /// Open-transaction state, one row per session with a transaction open,
    /// oldest first. The aggregate count exists as the
    /// `nucleus_open_transactions` gauge; this is the drill-down the incident
    /// runbook's "abandoned BEGIN pins the GC horizon" triage needs — which
    /// session, and how long it has been idle. There is no separate
    /// transaction-id allocation at the executor layer to expose: the
    /// engine's transaction identity IS the session (see `TxnState`), so
    /// session id + idle age is the honest identifier.
    pub(super) async fn show_transactions(&self) -> Result<ExecResult, ExecError> {
        let now = super::session::now_millis();
        // Snapshot (id, Arc<Session>) under the sync lock; read the tokio
        // txn locks afterwards so the sessions map is not held across an
        // await (same shape as the idle-in-transaction sweep).
        let candidates: Vec<(u64, std::sync::Arc<super::session::Session>)> = self
            .sessions
            .read()
            .iter()
            .map(|(id, s)| (*id, s.clone()))
            .collect();
        let mut open: Vec<(u64, bool, u64)> = Vec::new();
        for (id, session) in candidates {
            if session.txn_state.read().await.active {
                let executing = session.executing.load(std::sync::atomic::Ordering::Relaxed);
                let idle_ms = now.saturating_sub(
                    session
                        .last_activity_ms
                        .load(std::sync::atomic::Ordering::Relaxed),
                );
                open.push((id, executing, idle_ms));
            }
        }
        // Oldest idle first: that is the transaction pinning the MVCC
        // horizon, which is the one an operator is looking for.
        open.sort_by(|a, b| b.2.cmp(&a.2).then(a.0.cmp(&b.0)));
        let rows: Vec<Row> = open
            .into_iter()
            .map(|(id, executing, idle_ms)| {
                vec![
                    Value::Int64(id as i64),
                    Value::Bool(true),
                    Value::Bool(executing),
                    Value::Int64(idle_ms as i64),
                ]
            })
            .collect();
        Ok(ExecResult::Select {
            columns: vec![
                ("session_id".into(), DataType::Int64),
                ("transaction_active".into(), DataType::Bool),
                ("executing".into(), DataType::Bool),
                ("idle_ms".into(), DataType::Int64),
            ],
            rows,
        })
    }

    // ========================================================================
    // GRANT / REVOKE
    // ========================================================================

    pub(super) async fn execute_grant(
        &self,
        privileges: ast::Privileges,
        objects: Option<ast::GrantObjects>,
        grantees: Vec<ast::Grantee>,
    ) -> Result<ExecResult, ExecError> {
        self.require_security_admin("grant privileges")?;
        if objects.is_none()
            && let ast::Privileges::Actions(actions) = &privileges
            && actions
                .iter()
                .all(|action| matches!(action, ast::Action::Role { .. }))
        {
            let granted_roles: Vec<String> = actions
                .iter()
                .filter_map(|action| match action {
                    ast::Action::Role { role } => Some(object_name_value(role)),
                    _ => None,
                })
                .collect();
            let mut roles = self.roles.write().await;
            for granted in &granted_roles {
                if !roles.contains_key(granted) {
                    return Err(ExecError::Unsupported(format!(
                        "role '{granted}' does not exist"
                    )));
                }
            }
            for grantee in &grantees {
                let member = grantee_name(grantee);
                let role = roles.get_mut(&member).ok_or_else(|| {
                    ExecError::Unsupported(format!("role '{member}' does not exist"))
                })?;
                for granted in &granted_roles {
                    if !role.member_of.contains(granted) {
                        role.member_of.push(granted.clone());
                    }
                }
            }
            drop(roles);
            #[cfg(feature = "server")]
            for grantee in &grantees {
                self.audit(
                    crate::audit::AuditKind::PrivilegeGranted,
                    &grantee_name(grantee),
                    &format!(
                        "by {}; membership of [{}]",
                        self.acting_principal(),
                        granted_roles.join(",")
                    ),
                    None,
                );
            }
            return Ok(ExecResult::Command {
                tag: "GRANT ROLE".into(),
                rows_affected: 0,
            });
        }
        let privs = parse_privileges(&privileges);
        let object_names = objects
            .as_ref()
            .map(parse_grant_objects)
            .unwrap_or_else(|| vec!["*".to_string()]);
        let mut roles = self.roles.write().await;

        for grantee in &grantees {
            let role_name = grantee_name(grantee);
            let role = roles.entry(role_name.clone()).or_insert_with(|| RoleDef {
                name: role_name,
                password_hash: None,
                is_superuser: false,
                bypass_rls: false,
                can_login: false,
                valid_until: None,
                member_of: Vec::new(),
                privileges: HashMap::new(),
            });
            for obj in &object_names {
                let entry = role.privileges.entry(obj.clone()).or_insert_with(Vec::new);
                for p in &privs {
                    if !entry.contains(p) {
                        entry.push(p.clone());
                    }
                }
            }
        }

        drop(roles);
        #[cfg(feature = "server")]
        for grantee in &grantees {
            self.audit(
                crate::audit::AuditKind::PrivilegeGranted,
                &grantee_name(grantee),
                &format!(
                    "by {}; [{}] on [{}]",
                    self.acting_principal(),
                    privs
                        .iter()
                        .map(|p| format!("{p:?}"))
                        .collect::<Vec<_>>()
                        .join(","),
                    object_names.join(",")
                ),
                None,
            );
        }

        Ok(ExecResult::Command {
            tag: "GRANT".into(),
            rows_affected: 0,
        })
    }

    pub(super) async fn execute_revoke(
        &self,
        privileges: ast::Privileges,
        objects: Option<ast::GrantObjects>,
        grantees: Vec<ast::Grantee>,
    ) -> Result<ExecResult, ExecError> {
        self.require_security_admin("revoke privileges")?;
        if objects.is_none()
            && let ast::Privileges::Actions(actions) = &privileges
            && actions
                .iter()
                .all(|action| matches!(action, ast::Action::Role { .. }))
        {
            let revoked: Vec<String> = actions
                .iter()
                .filter_map(|action| match action {
                    ast::Action::Role { role } => Some(object_name_value(role)),
                    _ => None,
                })
                .collect();
            let mut roles = self.roles.write().await;
            for grantee in &grantees {
                let member = grantee_name(grantee);
                if let Some(role) = roles.get_mut(&member) {
                    role.member_of.retain(|parent| !revoked.contains(parent));
                }
            }
            drop(roles);
            #[cfg(feature = "server")]
            for grantee in &grantees {
                self.audit(
                    crate::audit::AuditKind::PrivilegeRevoked,
                    &grantee_name(grantee),
                    &format!(
                        "by {}; membership of [{}]",
                        self.acting_principal(),
                        revoked.join(",")
                    ),
                    None,
                );
            }
            return Ok(ExecResult::Command {
                tag: "REVOKE ROLE".into(),
                rows_affected: 0,
            });
        }
        let privs = parse_privileges(&privileges);
        let object_names = objects
            .as_ref()
            .map(parse_grant_objects)
            .unwrap_or_else(|| vec!["*".to_string()]);
        let mut roles = self.roles.write().await;

        for grantee in &grantees {
            let role_name = grantee_name(grantee);
            if let Some(role) = roles.get_mut(&role_name) {
                for obj in &object_names {
                    if let Some(entry) = role.privileges.get_mut(obj) {
                        entry.retain(|p| !privs.contains(p));
                    }
                }
            }
        }

        drop(roles);
        #[cfg(feature = "server")]
        for grantee in &grantees {
            self.audit(
                crate::audit::AuditKind::PrivilegeRevoked,
                &grantee_name(grantee),
                &format!(
                    "by {}; [{}] on [{}]",
                    self.acting_principal(),
                    privs
                        .iter()
                        .map(|p| format!("{p:?}"))
                        .collect::<Vec<_>>()
                        .join(","),
                    object_names.join(",")
                ),
                None,
            );
        }

        Ok(ExecResult::Command {
            tag: "REVOKE".into(),
            rows_affected: 0,
        })
    }

    pub(super) async fn execute_create_role(
        &self,
        create_role: ast::CreateRole,
    ) -> Result<ExecResult, ExecError> {
        self.require_security_admin("create roles")?;
        // Parse before taking the write lock, and before creating anything:
        // `VALID UNTIL 'not a timestamp'` must fail the statement rather than
        // create a role whose expiry silently did not apply.
        let valid_until = match create_role.valid_until {
            Some(ref expr) => parse_valid_until(expr)?,
            None => None,
        };
        let mut roles = self.roles.write().await;
        for name in &create_role.names {
            let role_name = object_name_value(name);
            // SEC-4, defence in depth. Authority is the bypass_rls attribute now,
            // so a role of this name confers nothing -- but policy TO-clauses
            // still address roles BY NAME, and a role called "superuser" is an
            // ambush: it reads as authority to every human looking at a catalog
            // dump. Reserve it so the ambiguity cannot be created in the first
            // place.
            if role_name.eq_ignore_ascii_case("superuser") {
                return Err(ExecError::PermissionDenied(
                    "role name 'superuser' is reserved; grant the SUPERUSER attribute instead"
                        .into(),
                ));
            }
            let mut role = RoleDef {
                name: role_name.clone(),
                password_hash: None,
                is_superuser: create_role.superuser.unwrap_or(false),
                bypass_rls: create_role.bypassrls.unwrap_or(false),
                can_login: create_role.login.unwrap_or(false),
                valid_until,
                member_of: create_role
                    .in_role
                    .iter()
                    .chain(create_role.in_group.iter())
                    .map(|r| r.value.clone())
                    .collect(),
                privileges: HashMap::new(),
            };
            if let Some(ref pwd) = create_role.password {
                match pwd {
                    ast::Password::Password(expr) => {
                        let raw = expr.to_string().trim_matches('\'').to_string();
                        role.password_hash = Some(super::store_password_literal(&raw));
                    }
                    ast::Password::NullPassword => {}
                }
            }
            #[cfg(feature = "server")]
            self.audit(
                crate::audit::AuditKind::RoleCreated,
                &role_name,
                &format!(
                    "by {}; login={} superuser={} password={} valid_until={}",
                    self.acting_principal(),
                    role.can_login,
                    role.is_superuser,
                    role.password_hash.is_some(),
                    valid_until.is_some(),
                ),
                None,
            );
            roles.insert(role_name, role);
        }
        Ok(ExecResult::Command {
            tag: "CREATE ROLE".into(),
            rows_affected: 0,
        })
    }

    pub(super) async fn execute_alter_role(
        &self,
        role_name: &str,
        operation: ast::AlterRoleOperation,
    ) -> Result<ExecResult, ExecError> {
        self.require_security_admin("alter roles")?;
        let mut roles = self.roles.write().await;
        let role = roles
            .get_mut(role_name)
            .ok_or_else(|| ExecError::Unsupported(format!("role '{role_name}' does not exist")))?;

        // What changed, for the audit record. Names of the options only —
        // never a password, not even its length.
        #[cfg_attr(not(feature = "server"), allow(unused_mut, unused_variables))]
        let mut changed: Vec<&'static str> = Vec::new();

        match operation {
            ast::AlterRoleOperation::WithOptions { options } => {
                for opt in &options {
                    match opt {
                        ast::RoleOption::SuperUser(v) => {
                            role.is_superuser = *v;
                            changed.push("superuser");
                        }
                        ast::RoleOption::BypassRLS(v) => {
                            role.bypass_rls = *v;
                            changed.push("bypassrls");
                        }
                        ast::RoleOption::Login(v) => {
                            role.can_login = *v;
                            changed.push("login");
                        }
                        ast::RoleOption::ValidUntil(expr) => {
                            role.valid_until = parse_valid_until(expr)?;
                            changed.push("valid_until");
                        }
                        ast::RoleOption::Password(pwd) => match pwd {
                            ast::Password::Password(expr) => {
                                let raw = expr.to_string().trim_matches('\'').to_string();
                                role.password_hash = Some(super::store_password_literal(&raw));
                                changed.push("password");
                            }
                            ast::Password::NullPassword => {
                                role.password_hash = None;
                                changed.push("password_cleared");
                            }
                        },
                        _ => {} // Ignore unsupported role options
                    }
                }
            }
            ast::AlterRoleOperation::RenameRole {
                role_name: new_name,
            } => {
                let new_name = new_name.value.clone();
                let mut role_data = roles.remove(role_name).unwrap();
                role_data.name = new_name.clone();
                roles.insert(new_name, role_data);
                changed.push("renamed");
            }
            _ => {} // Ignore unsupported alter operations
        }

        // Released before the audit write so a slow fsync cannot hold the role
        // catalog's write lock.
        drop(roles);
        #[cfg(feature = "server")]
        self.audit(
            crate::audit::AuditKind::RoleAltered,
            role_name,
            &format!(
                "by {}; changed=[{}]",
                self.acting_principal(),
                changed.join(",")
            ),
            None,
        );

        Ok(ExecResult::Command {
            tag: "ALTER ROLE".into(),
            rows_affected: 0,
        })
    }

    // ========================================================================
    // Cursors
    // ========================================================================

    /// DECLARE [BINARY] cursor [NO] SCROLL CURSOR [WITH | WITHOUT HOLD] FOR query.
    ///
    /// A cursor is one of two things, decided here:
    ///
    /// - **Lazy**, for a select over a single constant-argument
    ///   `generate_series(...)` (see [`Self::plan_series_cursor`]). Nothing is
    ///   executed at DECLARE: the cursor stores a few integers and the parsed
    ///   select list, and each FETCH produces only the rows it returns. The
    ///   source reads no table, so there is no snapshot to pin and the cursor
    ///   is insensitive trivially. It is forward only, because the producer
    ///   keeps no rows; an explicit `SCROLL` is never given one.
    /// - **Materialized**, for everything else: the query runs to completion
    ///   here and its rows are held in the session, a snapshot taken at
    ///   DECLARE. What bounds it:
    ///   - the per-cursor row budget, applied as a row limit on the query
    ///     itself so a sort-free scan stops early, and re-checked on the result;
    ///   - the per-cursor byte budget, checked on the result before it is
    ///     stored (the executor hands back a whole `Vec<Row>`, so bytes cannot
    ///     be enforced mid-flight).
    ///
    /// Both kinds share the per-session cursor count (checked before any work
    /// is done) and transaction lifetime: outside a transaction block only
    /// WITH HOLD is accepted, and every other cursor is closed at COMMIT or
    /// ROLLBACK.
    pub(super) async fn execute_declare_cursor(
        &self,
        stmt: &ast::Declare,
    ) -> Result<ExecResult, ExecError> {
        let cursor_name = stmt
            .names
            .first()
            .map(|n| n.value.clone())
            .unwrap_or_else(|| "unnamed".to_string());

        let query = stmt
            .for_query
            .as_ref()
            .ok_or_else(|| ExecError::Unsupported("DECLARE requires FOR query".into()))?;

        // Rows come back in the connection's normal result format; a BINARY
        // cursor would silently return text, so it is refused instead.
        if stmt.binary == Some(true) {
            return Err(ExecError::Unsupported(
                "DECLARE BINARY cursors are not supported".into(),
            ));
        }

        let hold = stmt.hold == Some(true);
        let no_scroll = stmt.scroll == Some(false);
        let sess = self.current_session();

        // PostgreSQL drops a non-holdable cursor at transaction end, so
        // outside a transaction block it could never be fetched.
        let in_txn = sess.txn_active.load(std::sync::atomic::Ordering::SeqCst);
        if !in_txn && !hold {
            return Err(ExecError::Runtime(
                "DECLARE CURSOR can only be used in transaction blocks".into(),
            ));
        }

        // Per-session cursor limit: cursors materialize their whole row set
        // and live until CLOSE, transaction end or disconnect — the most
        // memory-dense per-session object there is. Replacement of an
        // existing name does not grow the map and is always allowed. Refused
        // with the `too_many_cursors` wording the wire codec maps to SQLSTATE
        // 54000 (program_limit_exceeded). Checked before the query runs so a
        // refused DECLARE costs nothing.
        {
            let cursors = sess.cursors.read().await;
            let limit = self
                .max_cursors_per_session
                .load(std::sync::atomic::Ordering::Acquire);
            if !cursors.contains_key(&cursor_name) && cursors.len() >= limit {
                return Err(ExecError::Unsupported(format!(
                    "too_many_cursors: session already has {} open cursors (limit \
                     {limit}); CLOSE one before declaring another, or raise \
                     limits.max_cursors_per_session",
                    cursors.len()
                )));
            }
        }

        let max_rows = self
            .max_cursor_rows
            .load(std::sync::atomic::Ordering::Acquire);
        let max_bytes = self
            .max_cursor_bytes
            .load(std::sync::atomic::Ordering::Acquire);

        // The lazy path. An explicit SCROLL promises backward movement, which
        // a producer that keeps no rows cannot give, so it is materialized.
        if stmt.scroll != Some(true)
            && let Some((series, columns)) = self.plan_series_cursor(query)
        {
            let seq = sess
                .cursor_seq
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            sess.cursors.write().await.insert(
                cursor_name.clone(),
                CursorDef {
                    name: cursor_name,
                    rows: Vec::new(),
                    columns,
                    position: 0,
                    // Forward only by construction: this is what makes the
                    // shared FETCH check refuse a backward movement.
                    no_scroll: true,
                    hold,
                    opened_in_txn: in_txn,
                    bytes: 0,
                    seq,
                    lazy: Some(series),
                },
            );
            return Ok(ExecResult::Command {
                tag: "DECLARE CURSOR".into(),
                rows_affected: 0,
            });
        }

        // The executor materializes a table function whole before any LIMIT
        // applies, so the row cap below cannot stop it. Refuse a series that
        // is already over the budget instead of building it.
        self.refuse_oversized_series(query, max_rows)?;

        let mut capped: ast::Query = (**query).clone();
        cap_cursor_query(&mut capped, max_rows);
        let result = self.execute_query(capped).await?;
        match result {
            ExecResult::Select { columns, rows } => {
                if rows.len() > max_rows {
                    return Err(ExecError::Unsupported(format!(
                        "too_many_cursor_rows: DECLARE would materialize more than {max_rows} \
                         rows (limit {max_rows}); add a LIMIT, page with a keyset query, or \
                         raise limits.max_cursor_rows"
                    )));
                }
                let mut bytes = 0usize;
                for row in &rows {
                    bytes = bytes.saturating_add(cursor_row_bytes(row));
                    if bytes > max_bytes {
                        return Err(ExecError::Unsupported(format!(
                            "too_many_cursor_bytes: DECLARE would materialize more than \
                             {max_bytes} bytes (limit {max_bytes}); add a LIMIT, select fewer \
                             columns, or raise limits.max_cursor_bytes"
                        )));
                    }
                }
                let seq = sess
                    .cursor_seq
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let mut cursors = sess.cursors.write().await;
                cursors.insert(
                    cursor_name.clone(),
                    CursorDef {
                        name: cursor_name,
                        rows,
                        columns,
                        position: 0,
                        no_scroll,
                        hold,
                        opened_in_txn: in_txn,
                        bytes,
                        seq,
                        lazy: None,
                    },
                );
                Ok(ExecResult::Command {
                    tag: "DECLARE CURSOR".into(),
                    rows_affected: 0,
                })
            }
            _ => Err(ExecError::Unsupported(
                "DECLARE cursor query must be SELECT".into(),
            )),
        }
    }

    pub(super) async fn execute_fetch_cursor(
        &self,
        cursor_name: &str,
        direction: &ast::FetchDirection,
    ) -> Result<ExecResult, ExecError> {
        // Parsed before the cursor is looked up so a malformed count is
        // reported as such even for a cursor that does not exist.
        let movement = cursor_move(direction)?;

        let sess = self.current_session();
        let mut cursors = sess.cursors.write().await;
        let cursor = cursors.get_mut(cursor_name).ok_or_else(|| {
            ExecError::Runtime(format!("cursor \"{cursor_name}\" does not exist"))
        })?;

        let Some(mut series) = cursor.lazy.clone() else {
            let fetched = apply_cursor_move(cursor, movement)?;
            return Ok(ExecResult::Select {
                columns: cursor.columns.clone(),
                rows: fetched,
            });
        };

        // A lazy cursor: advance a copy of the producer and keep it only if
        // the whole FETCH succeeded. `cursor` is not used past this point.
        let position = cursor.position;
        let columns = cursor.columns.clone();
        if !moves_forward_only(&movement, position) {
            return Err(scan_forward_error());
        }
        let max_rows = self
            .max_cursor_rows
            .load(std::sync::atomic::Ordering::Acquire);
        let max_bytes = self
            .max_cursor_bytes
            .load(std::sync::atomic::Ordering::Acquire);
        match self.advance_series(&mut series, movement, max_rows, max_bytes) {
            Ok(rows) => {
                if let Some(cursor) = cursors.get_mut(cursor_name) {
                    cursor.position = series.position();
                    cursor.lazy = Some(series);
                }
                Ok(ExecResult::Select { columns, rows })
            }
            // Over budget: nothing was consumed, the cursor stays where it was.
            Err(SeriesFetchError::Refused(err)) => Err(err),
            // Cancelled or failed mid-stream: the producer's position is no
            // longer one the client can reason about, so the cursor is closed
            // and its state released with it.
            Err(SeriesFetchError::Failed(err)) => {
                cursors.remove(cursor_name);
                Err(err)
            }
        }
    }

    pub(super) async fn execute_close_cursor(
        &self,
        cursor: ast::CloseCursor,
    ) -> Result<ExecResult, ExecError> {
        let sess = self.current_session();
        match cursor {
            ast::CloseCursor::Specific { name } => {
                if sess.cursors.write().await.remove(&name.value).is_none() {
                    return Err(ExecError::Runtime(format!(
                        "cursor \"{}\" does not exist",
                        name.value
                    )));
                }
            }
            ast::CloseCursor::All => {
                sess.cursors.write().await.clear();
            }
        }
        Ok(ExecResult::Command {
            tag: "CLOSE".into(),
            rows_affected: 0,
        })
    }

    // ========================================================================
    // LISTEN / NOTIFY
    // ========================================================================

    pub(super) async fn execute_notify(
        &self,
        channel: &str,
        payload: Option<&str>,
    ) -> Result<ExecResult, ExecError> {
        let msg = payload.unwrap_or("").to_string();

        // Local delivery via the async hub.
        {
            let mut pubsub = self.pubsub.write().await;
            pubsub.publish(channel, msg.clone());
        }

        // Distributed delivery: also publish via the router (which queues remote messages)
        // and forward to all cluster peers if a replicator is present.
        {
            let mut router = self.dist_pubsub.write();
            router.publish(channel, msg.clone());
        }
        #[cfg(feature = "server")]
        {
            let maybe_rep = self.raft_replicator.read().clone();
            if let Some(replicator) = maybe_rep {
                replicator.broadcast_pubsub(channel, &msg).await;
            }
        }

        Ok(ExecResult::Command {
            tag: "NOTIFY".into(),
            rows_affected: 0,
        })
    }

    pub(super) async fn execute_listen(&self, channel: &str) -> Result<ExecResult, ExecError> {
        // Subscribe on the local async hub.
        {
            let mut pubsub = self.pubsub.write().await;
            let _ = pubsub.subscribe(channel);
        }

        // Gossip to peers: tell them this node now subscribes to `channel`.
        #[cfg(feature = "server")]
        {
            let snapshot = self.dist_pubsub.read().local_subscription_snapshot();
            let maybe_rep = self.raft_replicator.read().clone();
            if let Some(replicator) = maybe_rep {
                replicator.broadcast_gossip(snapshot).await;
            }
        }

        Ok(ExecResult::Command {
            tag: "LISTEN".into(),
            rows_affected: 0,
        })
    }

    pub(super) async fn execute_unlisten(&self, channel: &str) -> Result<ExecResult, ExecError> {
        // Unsubscribing is handled by dropping the receiver; we just acknowledge
        let _ = channel;
        Ok(ExecResult::Command {
            tag: "UNLISTEN".into(),
            rows_affected: 0,
        })
    }
}

/// How a lazy FETCH failed. A refusal leaves the cursor exactly where it was;
/// any other failure (cancellation, an evaluation error) closes it.
enum SeriesFetchError {
    Refused(ExecError),
    Failed(ExecError),
}

/// Stops at the first construct a lazy cursor will not evaluate at FETCH
/// time: a function call (it could write, read a table or a store, or differ
/// between calls), a subquery, or an unbound parameter. What is left is pure
/// arithmetic, comparison and CASE over the series value and literals, whose
/// result depends on nothing but the row it is evaluated against.
struct NotLazyPure;

impl ast::Visitor for NotLazyPure {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &ast::Expr) -> std::ops::ControlFlow<()> {
        match expr {
            ast::Expr::Function(_)
            | ast::Expr::Subquery(_)
            | ast::Expr::Exists { .. }
            | ast::Expr::InSubquery { .. } => std::ops::ControlFlow::Break(()),
            ast::Expr::Value(v) if matches!(&v.value, ast::Value::Placeholder(_)) => {
                std::ops::ControlFlow::Break(())
            }
            _ => std::ops::ControlFlow::Continue(()),
        }
    }
}

fn lazy_pure(expr: &ast::Expr) -> bool {
    expr.visit(&mut NotLazyPure).is_continue()
}

fn scan_forward_error() -> ExecError {
    ExecError::Runtime(
        "cursor can only scan forward; declare it with SCROLL option to enable backward scan"
            .into(),
    )
}

impl Executor {
    /// The lazy producer for a cursor query, or `None` when the query is not
    /// of the one shape that can be produced without a snapshot:
    ///
    /// ```text
    /// SELECT <select list> FROM generate_series(a, b [, step]) [[AS] t[(c)]]
    ///   [WHERE <predicate>] [LIMIT n] [OFFSET m]
    /// ```
    ///
    /// with constant integer arguments and a select list and predicate built
    /// from arithmetic, comparison, CASE and literals only (no function call,
    /// subquery or parameter). No ORDER BY, DISTINCT, GROUP BY, HAVING,
    /// window, join, CTE, set operation or locking clause: each of those needs
    /// the whole input before it can emit a row, or reads something a
    /// snapshot would have to pin. Anything outside the shape returns `None`
    /// and is materialized under the budgets instead, which is also where its
    /// errors are reported.
    ///
    /// The column types come from evaluating the select list once on the
    /// first series value (or from its static type when that evaluation fails
    /// or the series is empty), the way a materialized cursor types its
    /// columns from its first row. That is the only evaluation DECLARE does.
    fn plan_series_cursor(
        &self,
        query: &ast::Query,
    ) -> Option<(SeriesCursor, Vec<(String, DataType)>)> {
        if query.with.is_some()
            || query.order_by.is_some()
            || query.fetch.is_some()
            || !query.locks.is_empty()
            || query.for_clause.is_some()
            || query.settings.is_some()
            || query.format_clause.is_some()
            || !query.pipe_operators.is_empty()
        {
            return None;
        }
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            return None;
        };
        if select.optimizer_hint.is_some()
            || select.distinct.is_some()
            || select.select_modifiers.is_some()
            || select.top.is_some()
            || select.exclude.is_some()
            || select.into.is_some()
            || !select.lateral_views.is_empty()
            || select.prewhere.is_some()
            || !select.connect_by.is_empty()
            || !select.cluster_by.is_empty()
            || !select.distribute_by.is_empty()
            || !select.sort_by.is_empty()
            || select.having.is_some()
            || !select.named_window.is_empty()
            || select.qualify.is_some()
            || select.value_table_mode.is_some()
            || !matches!(
                &select.group_by,
                ast::GroupByExpr::Expressions(exprs, modifiers)
                    if exprs.is_empty() && modifiers.is_empty()
            )
        {
            return None;
        }

        if select.from.len() != 1 || !select.from[0].joins.is_empty() {
            return None;
        }
        let ast::TableFactor::Table {
            name,
            alias,
            args: Some(fn_args),
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
        } = &select.from[0].relation
        else {
            return None;
        };
        if !with_hints.is_empty()
            || version.is_some()
            || *with_ordinality
            || !partitions.is_empty()
            || json_path.is_some()
            || sample.is_some()
            || !index_hints.is_empty()
            || fn_args.settings.is_some()
            || fn_args.args.len() < 2
            || fn_args.args.len() > 3
        {
            return None;
        }
        let table_name = crate::sql::object_name_key(name);
        if !table_name.eq_ignore_ascii_case("generate_series") {
            return None;
        }

        // The label and column name the materialized table function gives its
        // single output column.
        let label = alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| table_name.clone());
        let column_name = match alias {
            Some(a) => {
                if a.columns.len() > 1 || a.columns.iter().any(|c| c.data_type.is_some()) {
                    return None;
                }
                a.columns.first().map(|c| c.name.value.clone())
            }
            None => None,
        };

        let mut bounds: Vec<i64> = Vec::with_capacity(3);
        for arg in &fn_args.args {
            let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) = arg else {
                return None;
            };
            if !lazy_pure(expr) {
                return None;
            }
            match self.eval_const_expr(expr).ok()? {
                Value::Int32(n) => bounds.push(i64::from(n)),
                Value::Int64(n) => bounds.push(n),
                _ => return None,
            }
        }
        let step = bounds.get(2).copied().unwrap_or(1);
        if step == 0 {
            return None;
        }

        let (limit_expr, offset_expr): (Option<&ast::Expr>, Option<&ast::Expr>) =
            match &query.limit_clause {
                None => (None, None),
                Some(ast::LimitClause::LimitOffset {
                    limit,
                    offset,
                    limit_by,
                }) => {
                    if !limit_by.is_empty() {
                        return None;
                    }
                    (limit.as_ref(), offset.as_ref().map(|o| &o.value))
                }
                Some(ast::LimitClause::OffsetCommaLimit { offset, limit }) => {
                    (Some(limit), Some(offset))
                }
            };
        let mut remaining: Option<usize> = None;
        if let Some(expr) = limit_expr {
            if !lazy_pure(expr) {
                return None;
            }
            remaining = self.expr_to_usize(expr).ok()?;
        }
        let mut skip = 0usize;
        if let Some(expr) = offset_expr {
            if !lazy_pure(expr) {
                return None;
            }
            skip = self.expr_to_usize(expr).ok()?.unwrap_or(0);
        }

        for item in &select.projection {
            let pure = match item {
                ast::SelectItem::UnnamedExpr(expr) => lazy_pure(expr),
                ast::SelectItem::ExprWithAlias { expr, .. } => lazy_pure(expr),
                ast::SelectItem::Wildcard(_) => true,
                ast::SelectItem::QualifiedWildcard(..) => false,
            };
            if !pure {
                return None;
            }
        }
        if let Some(predicate) = &select.selection
            && !lazy_pure(predicate)
        {
            return None;
        }

        let series = SeriesCursor {
            next: bounds[0],
            stop: bounds[1],
            step,
            finished: false,
            col_meta: vec![ColMeta {
                table: Some(label.clone()),
                name: column_name.unwrap_or(label),
                dtype: DataType::Int64,
            }],
            selection: select.selection.clone(),
            projection: select.projection.clone(),
            skip,
            remaining,
            emitted: 0,
            after_last: false,
            current: None,
        };
        let projected = match series.probe_row() {
            Some(probe) => self
                .project_columns(
                    &series.projection,
                    &series.col_meta,
                    std::slice::from_ref(&probe),
                )
                .or_else(|_| self.project_columns(&series.projection, &series.col_meta, &[])),
            None => self.project_columns(&series.projection, &series.col_meta, &[]),
        };
        let (columns, _) = projected.ok()?;
        Some((series, columns))
    }

    /// Refuse a `generate_series` call in a materialized cursor's top-level
    /// FROM whose length already exceeds the row budget. The table function is
    /// built whole before LIMIT or the cursor cap can act, so without this a
    /// `SCROLL` cursor (or an `ORDER BY`) over a huge range would allocate
    /// the entire series before the budget check could refuse it. Only the
    /// top-level FROM is inspected; a series nested in a subquery is bounded
    /// by the executor's ordinary query memory budget like any other query.
    fn refuse_oversized_series(
        &self,
        query: &ast::Query,
        max_rows: usize,
    ) -> Result<(), ExecError> {
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            return Ok(());
        };
        for from in &select.from {
            let factors =
                std::iter::once(&from.relation).chain(from.joins.iter().map(|j| &j.relation));
            for factor in factors {
                let ast::TableFactor::Table {
                    name,
                    args: Some(fn_args),
                    ..
                } = factor
                else {
                    continue;
                };
                if !crate::sql::object_name_key(name).eq_ignore_ascii_case("generate_series") {
                    continue;
                }
                let mut bounds: Vec<i128> = Vec::with_capacity(3);
                for arg in &fn_args.args {
                    if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) = arg {
                        match self.eval_const_expr(expr) {
                            Ok(Value::Int32(n)) => bounds.push(i128::from(n)),
                            Ok(Value::Int64(n)) => bounds.push(i128::from(n)),
                            _ => {}
                        }
                    }
                }
                if bounds.len() != fn_args.args.len() || bounds.len() < 2 || bounds.len() > 3 {
                    continue;
                }
                let step = bounds.get(2).copied().unwrap_or(1);
                if step == 0 {
                    continue;
                }
                let span = if step > 0 {
                    bounds[1] - bounds[0]
                } else {
                    bounds[0] - bounds[1]
                };
                let count = if span < 0 { 0 } else { span / step.abs() + 1 };
                if count > max_rows as i128 {
                    return Err(ExecError::Unsupported(format!(
                        "too_many_cursor_rows: DECLARE would materialize a generate_series of \
                         {count} rows (limit {max_rows}); a cursor over a bare \
                         generate_series (no SCROLL, ORDER BY or aggregate) is produced \
                         lazily instead, or raise limits.max_cursor_rows"
                    )));
                }
            }
        }
        Ok(())
    }

    /// The next source row of a lazy cursor that passes the WHERE clause, after
    /// the query's OFFSET and within its LIMIT; `None` once either runs out.
    /// Polls the cancel flag every [`SERIES_CANCEL_POLL`] values examined.
    fn series_next_row(
        &self,
        series: &mut SeriesCursor,
        polled: &mut u32,
    ) -> Result<Option<Row>, ExecError> {
        loop {
            if series.remaining == Some(0) {
                return Ok(None);
            }
            let Some(value) = series.next_value() else {
                return Ok(None);
            };
            *polled = polled.wrapping_add(1);
            if (*polled).is_multiple_of(SERIES_CANCEL_POLL) {
                self.check_cancelled()?;
            }
            let row: Row = vec![Value::Int64(value)];
            if let Some(predicate) = series.selection.as_ref()
                && !self.eval_where(predicate, &row, &series.col_meta)?
            {
                continue;
            }
            if series.skip > 0 {
                series.skip -= 1;
                continue;
            }
            if let Some(remaining) = series.remaining.as_mut() {
                *remaining -= 1;
            }
            return Ok(Some(row));
        }
    }

    /// Carry out one forward FETCH movement on a lazy cursor's producer and
    /// return the rows it fetched, updating `series` to the new position.
    ///
    /// Memory is bounded independent of the series length: rows are produced
    /// and projected [`SERIES_CHUNK_ROWS`] at a time and the result is checked
    /// against the row and byte budgets after every chunk, so a FETCH that
    /// would return more than `max_cursor_rows` rows or `max_cursor_bytes`
    /// bytes is refused rather than built. `series` is a working copy: the
    /// caller keeps it only on `Ok`.
    fn advance_series(
        &self,
        series: &mut SeriesCursor,
        movement: CursorMove,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Vec<Row>, SeriesFetchError> {
        use SeriesFetchError::{Failed, Refused};

        // A zero count re-fetches the row the cursor is on; nothing is run.
        if matches!(
            movement,
            CursorMove::Forward(0) | CursorMove::Backward(0) | CursorMove::Relative(0)
        ) {
            return Ok(series.current.iter().cloned().collect());
        }
        self.check_cancelled().map_err(Failed)?;
        if series.after_last {
            return Ok(Vec::new());
        }

        // (rows to discard, rows to return). The caller has already refused
        // every movement that is not strictly forward.
        let (discard, take): (usize, usize) = match movement {
            CursorMove::Forward(k) => (0, k),
            CursorMove::ForwardAll => (0, usize::MAX),
            CursorMove::Absolute(t) => {
                let target = usize::try_from(t).unwrap_or(usize::MAX);
                (target.saturating_sub(series.emitted).saturating_sub(1), 1)
            }
            CursorMove::Relative(d) => {
                let ahead = usize::try_from(d).unwrap_or(usize::MAX);
                (ahead.saturating_sub(1), 1)
            }
            CursorMove::Backward(_) | CursorMove::BackwardAll => {
                return Err(Refused(scan_forward_error()));
            }
        };
        if take != usize::MAX && take > max_rows {
            return Err(Refused(ExecError::Unsupported(format!(
                "too_many_cursor_rows: FETCH would return {take} rows (limit {max_rows}); \
                 fetch fewer rows at a time or raise limits.max_cursor_rows"
            ))));
        }

        let mut polled: u32 = 0;
        for _ in 0..discard {
            match self.series_next_row(series, &mut polled).map_err(Failed)? {
                Some(_) => series.emitted += 1,
                None => {
                    series.after_last = true;
                    series.current = None;
                    return Ok(Vec::new());
                }
            }
        }

        let mut out: Vec<Row> = Vec::new();
        let mut bytes = 0usize;
        while out.len() < take {
            let want = (take - out.len()).min(SERIES_CHUNK_ROWS);
            let mut source: Vec<Row> = Vec::with_capacity(want);
            while source.len() < want {
                match self.series_next_row(series, &mut polled).map_err(Failed)? {
                    Some(row) => source.push(row),
                    None => break,
                }
            }
            let exhausted = source.len() < want;
            if !source.is_empty() {
                let (_, projected) = self
                    .project_columns(&series.projection, &series.col_meta, &source)
                    .map_err(Failed)?;
                if out.len().saturating_add(projected.len()) > max_rows {
                    return Err(Refused(ExecError::Unsupported(format!(
                        "too_many_cursor_rows: FETCH would return more than {max_rows} rows \
                         (limit {max_rows}); fetch fewer rows at a time or raise \
                         limits.max_cursor_rows"
                    ))));
                }
                for row in &projected {
                    bytes = bytes.saturating_add(cursor_row_bytes(row));
                }
                if bytes > max_bytes {
                    return Err(Refused(ExecError::Unsupported(format!(
                        "too_many_cursor_bytes: FETCH would return more than {max_bytes} \
                         bytes (limit {max_bytes}); fetch fewer rows at a time, select fewer \
                         columns, or raise limits.max_cursor_bytes"
                    ))));
                }
                series.emitted += projected.len();
                out.extend(projected);
            }
            if exhausted {
                series.after_last = true;
                break;
            }
        }
        series.current = if series.after_last {
            None
        } else {
            out.last().cloned()
        };
        Ok(out)
    }
}

/// `VALID UNTIL <expr>` as UTC microseconds, or `None` for no expiry.
///
/// PostgreSQL takes a timestamptz here; `NULL` and `'infinity'` both mean "no
/// expiry". A value that does not parse is an error rather than a silently
/// dropped clause — the whole defect this replaced was a guarantee-carrying
/// clause being accepted and discarded.
fn parse_valid_until(expr: &ast::Expr) -> Result<Option<i64>, ExecError> {
    let raw = expr.to_string();
    let literal = raw.trim().trim_matches('\'').trim();
    if literal.is_empty() || literal.eq_ignore_ascii_case("null") {
        return Ok(None);
    }
    if literal.eq_ignore_ascii_case("infinity") {
        return Ok(None);
    }
    crate::types::parse_timestamptz(literal)
        .map(Some)
        .map_err(|e| ExecError::Unsupported(format!("VALID UNTIL {raw}: {e}")))
}

/// Estimated heap bytes a materialized row holds. Uses the same estimator as
/// KV memory accounting (`Value::approx_heap_size`), which counts arrays and
/// JSONB by content rather than by a flat guess.
fn cursor_row_bytes(row: &Row) -> usize {
    row.iter()
        .map(Value::approx_heap_size)
        .sum::<usize>()
        .saturating_add(std::mem::size_of::<Row>())
}

/// The literal row count of a LIMIT expression: `Some(n)` for a number,
/// `None` for anything else (parameter, expression, NULL, ALL).
fn literal_limit(expr: &ast::Expr) -> Option<usize> {
    match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::Number(n, _) => n.parse::<usize>().ok(),
            _ => None,
        },
        _ => None,
    }
}

/// `expr` unless it is absent, NULL, or a literal above `cap`, in which case
/// the cap. A non-literal limit is left alone: the post-materialization row
/// check still refuses it, it just cannot stop the scan early.
fn capped_limit(limit: Option<ast::Expr>, cap: usize) -> ast::Expr {
    let cap_expr = || ast::Expr::Value(ast::Value::Number(cap.to_string(), false).into());
    match limit {
        None => cap_expr(),
        Some(expr) => {
            if matches!(&expr, ast::Expr::Value(v) if matches!(v.value, ast::Value::Null)) {
                return cap_expr();
            }
            match literal_limit(&expr) {
                Some(n) if n > cap => cap_expr(),
                _ => expr,
            }
        }
    }
}

/// Bound a cursor's query to `max_rows + 1` rows so materialization stops one
/// row past the budget (the extra row is what proves the budget was exceeded).
/// A smaller user LIMIT is kept. Queries with a ClickHouse `LIMIT ... BY`, a
/// `FETCH FIRST` clause or row locks are left alone; the result check still
/// applies to them.
fn cap_cursor_query(query: &mut ast::Query, max_rows: usize) {
    if query.fetch.is_some() || !query.locks.is_empty() {
        return;
    }
    let cap = max_rows.saturating_add(1);
    query.limit_clause = Some(match query.limit_clause.take() {
        None => ast::LimitClause::LimitOffset {
            limit: Some(capped_limit(None, cap)),
            offset: None,
            limit_by: Vec::new(),
        },
        Some(ast::LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) => {
            if limit_by.is_empty() {
                ast::LimitClause::LimitOffset {
                    limit: Some(capped_limit(limit, cap)),
                    offset,
                    limit_by,
                }
            } else {
                ast::LimitClause::LimitOffset {
                    limit,
                    offset,
                    limit_by,
                }
            }
        }
        Some(ast::LimitClause::OffsetCommaLimit { offset, limit }) => {
            ast::LimitClause::OffsetCommaLimit {
                offset,
                limit: capped_limit(Some(limit), cap),
            }
        }
    });
}

/// A FETCH direction reduced to the six movements a materialized cursor has.
#[derive(Debug, Clone, Copy, PartialEq)]
enum CursorMove {
    Forward(usize),
    ForwardAll,
    Backward(usize),
    BackwardAll,
    Absolute(i64),
    Relative(i64),
}

fn invalid_fetch_count(text: &str) -> ExecError {
    ExecError::Runtime(format!(
        "invalid FETCH count \"{text}\": expected an integer that fits in 64 bits"
    ))
}

fn parse_fetch_count(value: &ast::Value) -> Result<i64, ExecError> {
    match value {
        ast::Value::Number(n, _) => n.parse::<i64>().map_err(|_| invalid_fetch_count(n)),
        other => Err(invalid_fetch_count(&other.to_string())),
    }
}

/// A signed count: a negative count reverses the direction (PostgreSQL).
fn signed_move(n: i64) -> CursorMove {
    let count = usize::try_from(n.unsigned_abs()).unwrap_or(usize::MAX);
    if n >= 0 {
        CursorMove::Forward(count)
    } else {
        CursorMove::Backward(count)
    }
}

fn cursor_move(direction: &ast::FetchDirection) -> Result<CursorMove, ExecError> {
    use ast::FetchDirection as D;
    Ok(match direction {
        D::Count { limit } => signed_move(parse_fetch_count(limit)?),
        D::Next => CursorMove::Forward(1),
        D::Prior => CursorMove::Backward(1),
        D::First => CursorMove::Absolute(1),
        D::Last => CursorMove::Absolute(-1),
        D::Absolute { limit } => CursorMove::Absolute(parse_fetch_count(limit)?),
        D::Relative { limit } => CursorMove::Relative(parse_fetch_count(limit)?),
        D::All | D::ForwardAll => CursorMove::ForwardAll,
        D::Forward { limit: None } => CursorMove::Forward(1),
        D::Forward { limit: Some(limit) } => signed_move(parse_fetch_count(limit)?),
        D::Backward { limit: None } => CursorMove::Backward(1),
        D::Backward { limit: Some(limit) } => match signed_move(parse_fetch_count(limit)?) {
            CursorMove::Forward(n) => CursorMove::Backward(n),
            CursorMove::Backward(n) => CursorMove::Forward(n),
            other => other,
        },
        D::BackwardAll => CursorMove::BackwardAll,
        #[allow(unreachable_patterns)]
        other => {
            return Err(ExecError::Unsupported(format!(
                "FETCH direction {other:?} is not supported"
            )));
        }
    })
}

/// Whether a movement only ever goes strictly forward from `pos`. The only
/// movement allowed to stay put is a zero count (re-fetch the current row).
/// Stricter than PostgreSQL in one edge: `FETCH ABSOLUTE k` to a row at or
/// behind the current position is refused rather than rewinding.
fn moves_forward_only(movement: &CursorMove, pos: usize) -> bool {
    match *movement {
        CursorMove::Forward(_) | CursorMove::ForwardAll => true,
        CursorMove::Backward(_) | CursorMove::BackwardAll => false,
        CursorMove::Absolute(t) => t > 0 && usize::try_from(t).unwrap_or(usize::MAX) > pos,
        CursorMove::Relative(d) => d >= 0,
    }
}

fn row_at(rows: &[Row], pos: usize) -> Vec<Row> {
    if pos >= 1 && pos <= rows.len() {
        vec![rows[pos - 1].clone()]
    } else {
        Vec::new()
    }
}

/// Move the cursor and return the rows the movement fetched. `position` is
/// PostgreSQL's: 0 before the first row, `1..=n` on a row, `n + 1` after the
/// last. Nothing is changed when the movement is refused.
fn apply_cursor_move(cursor: &mut CursorDef, movement: CursorMove) -> Result<Vec<Row>, ExecError> {
    let n = cursor.rows.len();
    let pos = cursor.position;
    if cursor.no_scroll && !moves_forward_only(&movement, pos) {
        return Err(scan_forward_error());
    }
    let (fetched, new_pos): (Vec<Row>, usize) = match movement {
        CursorMove::Forward(0) | CursorMove::Backward(0) | CursorMove::Relative(0) => {
            (row_at(&cursor.rows, pos), pos)
        }
        CursorMove::Forward(k) => {
            let start = pos.min(n);
            let end = start.saturating_add(k).min(n);
            let new_pos = if start.saturating_add(k) > n {
                n + 1
            } else {
                start + k
            };
            (cursor.rows[start..end].to_vec(), new_pos)
        }
        CursorMove::ForwardAll => {
            let start = pos.min(n);
            (cursor.rows[start..].to_vec(), n + 1)
        }
        CursorMove::Backward(k) => {
            let take = k.min(pos.saturating_sub(1));
            let out: Vec<Row> = (1..=take)
                .map(|i| cursor.rows[pos - i - 1].clone())
                .collect();
            (out, pos.saturating_sub(k))
        }
        CursorMove::BackwardAll => {
            let take = pos.saturating_sub(1);
            let out: Vec<Row> = (1..=take)
                .map(|i| cursor.rows[pos - i - 1].clone())
                .collect();
            (out, 0)
        }
        CursorMove::Absolute(t) => {
            if t > 0 {
                let target = usize::try_from(t).unwrap_or(usize::MAX);
                if target <= n {
                    (row_at(&cursor.rows, target), target)
                } else {
                    (Vec::new(), n + 1)
                }
            } else if t == 0 {
                (Vec::new(), 0)
            } else {
                let back = usize::try_from(t.unsigned_abs()).unwrap_or(usize::MAX);
                if back <= n {
                    let target = n - back + 1;
                    (row_at(&cursor.rows, target), target)
                } else {
                    (Vec::new(), 0)
                }
            }
        }
        CursorMove::Relative(d) => {
            if d > 0 {
                let target = pos.saturating_add(usize::try_from(d).unwrap_or(usize::MAX));
                if target <= n {
                    (row_at(&cursor.rows, target), target)
                } else {
                    (Vec::new(), n + 1)
                }
            } else {
                let back = usize::try_from(d.unsigned_abs()).unwrap_or(usize::MAX);
                if pos > back {
                    (row_at(&cursor.rows, pos - back), pos - back)
                } else {
                    (Vec::new(), 0)
                }
            }
        }
    };
    cursor.position = new_pos;
    Ok(fetched)
}

#[cfg(test)]
mod cursor_tests {
    use super::*;

    fn capped(sql: &str, max_rows: usize) -> String {
        let mut stmts = crate::sql::parse(sql).expect("parse");
        match stmts.remove(0) {
            ast::Statement::Query(mut query) => {
                cap_cursor_query(&mut query, max_rows);
                query.to_string()
            }
            other => panic!("expected a query, got {other}"),
        }
    }

    fn cursor_over(n: i32, no_scroll: bool) -> CursorDef {
        CursorDef {
            name: "c".into(),
            rows: (1..=n).map(|i| vec![Value::Int32(i)]).collect(),
            columns: vec![("id".into(), DataType::Int32)],
            position: 0,
            no_scroll,
            hold: false,
            opened_in_txn: true,
            bytes: 0,
            seq: 0,
            lazy: None,
        }
    }

    fn ids(rows: &[Row]) -> Vec<i32> {
        rows.iter()
            .map(|r| match &r[0] {
                Value::Int32(n) => *n,
                other => panic!("expected Int32, got {other:?}"),
            })
            .collect()
    }

    /// The row cap rides on the query as `max_rows + 1`; a smaller user LIMIT
    /// survives, a larger or missing one is replaced, OFFSET and ORDER BY are
    /// kept, and a row-locking query is left alone.
    #[test]
    fn cap_cursor_query_bounds_the_scan_without_changing_smaller_limits() {
        assert_eq!(capped("SELECT id FROM t", 3), "SELECT id FROM t LIMIT 4");
        assert_eq!(
            capped("SELECT id FROM t ORDER BY id", 3),
            "SELECT id FROM t ORDER BY id LIMIT 4"
        );
        assert_eq!(
            capped("SELECT id FROM t LIMIT 2", 3),
            "SELECT id FROM t LIMIT 2"
        );
        assert_eq!(
            capped("SELECT id FROM t LIMIT 4", 3),
            "SELECT id FROM t LIMIT 4"
        );
        assert_eq!(
            capped("SELECT id FROM t LIMIT 100", 3),
            "SELECT id FROM t LIMIT 4"
        );
        assert_eq!(
            capped("SELECT id FROM t LIMIT 100 OFFSET 5", 3),
            "SELECT id FROM t LIMIT 4 OFFSET 5"
        );
        assert!(!capped("SELECT id FROM t FOR UPDATE", 3).contains("LIMIT"));
    }

    #[test]
    fn a_zero_count_refetches_the_current_row_and_empty_cursors_stay_empty() {
        let mut cursor = cursor_over(3, false);
        assert!(
            apply_cursor_move(&mut cursor, CursorMove::Forward(0))
                .unwrap()
                .is_empty()
        );
        apply_cursor_move(&mut cursor, CursorMove::Forward(2)).unwrap();
        let again = apply_cursor_move(&mut cursor, CursorMove::Forward(0)).unwrap();
        assert_eq!(ids(&again), vec![2]);
        assert_eq!(cursor.position, 2);

        let mut empty = cursor_over(0, false);
        assert!(
            apply_cursor_move(&mut empty, CursorMove::Forward(1))
                .unwrap()
                .is_empty()
        );
        assert_eq!(empty.position, 1);
        assert!(
            apply_cursor_move(&mut empty, CursorMove::Absolute(-1))
                .unwrap()
                .is_empty()
        );
        assert_eq!(empty.position, 0);
    }

    /// Counts far beyond the row set must saturate, not overflow or panic.
    #[test]
    fn huge_counts_saturate() {
        let mut cursor = cursor_over(3, false);
        let all = apply_cursor_move(&mut cursor, CursorMove::Forward(usize::MAX)).unwrap();
        assert_eq!(ids(&all), vec![1, 2, 3]);
        assert_eq!(cursor.position, 4);
        let back = apply_cursor_move(&mut cursor, CursorMove::Backward(usize::MAX)).unwrap();
        assert_eq!(ids(&back), vec![3, 2, 1]);
        assert_eq!(cursor.position, 0);
        assert!(
            apply_cursor_move(&mut cursor, CursorMove::Relative(i64::MAX))
                .unwrap()
                .is_empty()
        );
        assert_eq!(cursor.position, 4);
        assert!(
            apply_cursor_move(&mut cursor, CursorMove::Absolute(i64::MIN))
                .unwrap()
                .is_empty()
        );
        assert_eq!(cursor.position, 0);
    }

    /// A refused NO SCROLL fetch leaves rows and position untouched.
    #[test]
    fn no_scroll_refusal_leaves_the_cursor_untouched() {
        let mut cursor = cursor_over(3, true);
        apply_cursor_move(&mut cursor, CursorMove::Forward(2)).unwrap();
        let err = apply_cursor_move(&mut cursor, CursorMove::Backward(1)).unwrap_err();
        assert!(err.to_string().contains("cursor can only scan forward"));
        assert_eq!(cursor.position, 2);
        assert_eq!(cursor.rows.len(), 3);
    }

    fn series(start: i64, stop: i64, step: i64) -> SeriesCursor {
        SeriesCursor {
            next: start,
            stop,
            step,
            finished: false,
            col_meta: Vec::new(),
            selection: None,
            projection: Vec::new(),
            skip: 0,
            remaining: None,
            emitted: 0,
            after_last: false,
            current: None,
        }
    }

    fn drain(series: &mut SeriesCursor) -> Vec<i64> {
        std::iter::from_fn(|| series.next_value()).collect()
    }

    /// The producer yields exactly what `generate_series` builds, both ways.
    #[test]
    fn series_values_follow_generate_series_in_both_directions() {
        assert_eq!(drain(&mut series(1, 5, 1)), vec![1, 2, 3, 4, 5]);
        assert_eq!(drain(&mut series(0, 20, 5)), vec![0, 5, 10, 15, 20]);
        assert_eq!(drain(&mut series(10, 1, -3)), vec![10, 7, 4, 1]);
        assert!(drain(&mut series(5, 1, 1)).is_empty());
        assert!(drain(&mut series(1, 5, -1)).is_empty());
    }

    /// A series that reaches the end of the integer range stops there; the
    /// next value is not computed by wrapping around.
    #[test]
    fn a_series_at_the_integer_limit_stops_instead_of_wrapping() {
        assert_eq!(
            drain(&mut series(i64::MAX - 1, i64::MAX, 1)),
            vec![i64::MAX - 1, i64::MAX]
        );
        assert_eq!(
            drain(&mut series(i64::MIN + 1, i64::MIN, -1)),
            vec![i64::MIN + 1, i64::MIN]
        );
        assert_eq!(
            drain(&mut series(i64::MAX - 5, i64::MAX, 4)),
            vec![i64::MAX - 5, i64::MAX - 1]
        );
    }

    /// Position numbering matches the materialized cursor's, and the probe
    /// row is the first value without consuming it.
    #[test]
    fn series_position_and_probe() {
        let mut cursor = series(3, 4, 1);
        assert_eq!(cursor.position(), 0);
        assert_eq!(cursor.probe_row(), Some(vec![Value::Int64(3)]));
        assert_eq!(cursor.next_value(), Some(3));
        cursor.emitted = 1;
        assert_eq!(cursor.position(), 1);
        cursor.after_last = true;
        assert_eq!(cursor.position(), 2);
        assert_eq!(series(5, 1, 1).probe_row(), None);
    }
}
