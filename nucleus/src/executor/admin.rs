//! Administrative commands: SET/SHOW, GRANT/REVOKE, Cursors, LISTEN/NOTIFY.
//!
//! Extracted from `mod.rs` to reduce file size. All methods are `pub(super)` so
//! the main executor module can delegate to them.

use std::collections::HashMap;

use sqlparser::ast;

use crate::fault::SubsystemHealth;
use crate::types::{DataType, Row, Value};

use super::schema_types::{CursorDef, RoleDef};
use super::{ExecError, ExecResult, Executor};
use super::helpers::{grantee_name, parse_grant_objects, parse_privileges};

impl Executor {
    // ========================================================================
    // SET / SHOW
    // ========================================================================

    pub(super) fn execute_set(
        &self,
        set: ast::Set,
    ) -> Result<ExecResult, ExecError> {
        // Store SET values for SHOW to retrieve
        if let ast::Set::SingleAssignment { variable, values, .. } = &set {
            let var_name = variable.to_string().to_lowercase();
            let val_str: Vec<String> = values.iter().map(|v| v.to_string()).collect();
            let val = val_str.join(", ");

            // Handle SET TRANSACTION ISOLATION LEVEL
            if var_name == "transaction_isolation" || var_name == "default_transaction_isolation" {
                let level = val.trim_matches('\'').trim_matches('"').to_lowercase();
                self.storage.set_next_isolation_level(&level);
            }

            self.current_session().settings.write().insert(var_name, val);
        }
        Ok(ExecResult::Command {
            tag: "SET".into(),
            rows_affected: 0,
        })
    }

    pub(super) fn execute_show(&self, variable: Vec<ast::Ident>) -> Result<ExecResult, ExecError> {
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

        // Check user-set values first
        let sess = self.current_session();
        let settings = sess.settings.read();
        if let Some(val) = settings.get(&var_lower) {
            return Ok(ExecResult::Select {
                columns: vec![(var_name, DataType::Text)],
                rows: vec![vec![Value::Text(val.clone())]],
            });
        }
        drop(settings);

        // Handle special multi-word SHOW commands
        let var_upper = var_name.to_uppercase();
        match var_upper.as_str() {
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
            "IS_SUPERUSER" => "on".to_string(),
            "SESSION_AUTHORIZATION" => "nucleus".to_string(),
            "STANDARD_CONFORMING_STRINGS" => "on".to_string(),
            "TIMEZONE" => "UTC".to_string(),
            "DATESTYLE" => "ISO, MDY".to_string(),
            "INTEGER_DATETIMES" => "on".to_string(),
            "INTERVALSTYLE" => "postgres".to_string(),
            "SEARCH_PATH" => "\"$user\", public".to_string(),
            "MAX_CONNECTIONS" => "100".to_string(),
            "TRANSACTION_ISOLATION" => "read committed".to_string(),
            "DEFAULT_TRANSACTION_ISOLATION" => "read committed".to_string(),
            "LC_COLLATE" => "en_US.UTF-8".to_string(),
            "LC_CTYPE" => "en_US.UTF-8".to_string(),
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
        let defaults = vec![
            ("server_version", "16.0 (Nucleus)"),
            ("server_encoding", "UTF8"),
            ("client_encoding", "UTF8"),
            ("is_superuser", "on"),
            ("session_authorization", "nucleus"),
            ("standard_conforming_strings", "on"),
            ("timezone", "UTC"),
            ("datestyle", "ISO, MDY"),
            ("integer_datetimes", "on"),
            ("intervalstyle", "postgres"),
            ("search_path", "\"$user\", public"),
            ("max_connections", "100"),
            ("transaction_isolation", "read committed"),
            ("default_transaction_isolation", "read committed"),
            ("lc_collate", "en_US.UTF-8"),
            ("lc_ctype", "en_US.UTF-8"),
        ];

        for (name, value) in &defaults {
            // Check if user has overridden this setting
            let final_value = settings.get(*name).map(|s| s.as_str()).unwrap_or(*value);
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
        let table_def = self.catalog.get_table(table_name).await
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
            vec![Value::Text("pool_mode".into()), Value::Text("session".into())],
            vec![Value::Text("mvcc_enabled".into()), Value::Text(mvcc.to_string())],
            vec![Value::Text("storage_engine".into()), Value::Text(
                if mvcc { "MvccStorageAdapter" } else { "MemoryEngine/DiskEngine" }.into(),
            )],
        ];

        // Report live connection pool stats if available
        if let Some(ref pool) = self.conn_pool {
            let available = pool.available_permits();
            rows.push(vec![Value::Text("pool_available_permits".into()), Value::Text(available.to_string())]);
        } else {
            rows.push(vec![Value::Text("pool_status".into()), Value::Text("not wired".into())]);
        }

        Ok(ExecResult::Select {
            columns: vec![
                ("setting".into(), DataType::Text),
                ("value".into(), DataType::Text),
            ],
            rows,
        })
    }

    pub(super) fn show_cluster_status(&self) -> Result<ExecResult, ExecError> {
        let rows = if let Some(ref cluster) = self.cluster {
            let status = cluster.read().status();
            let mode_str = match status.mode {
                crate::distributed::ClusterMode::Standalone => "standalone",
                crate::distributed::ClusterMode::PrimaryReplica => "primary-replica",
                crate::distributed::ClusterMode::MultiRaft => "multi-raft",
            };
            vec![
                vec![Value::Text("node_id".into()), Value::Text(format!("{:#x}", status.node_id))],
                vec![Value::Text("mode".into()), Value::Text(mode_str.into())],
                vec![Value::Text("node_count".into()), Value::Text(status.node_count.to_string())],
                vec![Value::Text("shard_count".into()), Value::Text(status.shard_count.to_string())],
                vec![Value::Text("shards_led".into()), Value::Text(status.shards_led.to_string())],
                vec![Value::Text("epoch".into()), Value::Text(status.epoch.to_string())],
                vec![Value::Text("active_txns".into()), Value::Text(status.active_txns.to_string())],
            ]
        } else {
            vec![
                vec![Value::Text("mode".into()), Value::Text("standalone".into())],
                vec![Value::Text("cluster".into()), Value::Text("not configured".into())],
            ]
        };

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
            .map(|(name, typ, val)| {
                vec![
                    Value::Text(name),
                    Value::Text(typ),
                    Value::Text(val),
                ]
            })
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
                vec![Value::Text("engine".into()), Value::Text(if self.storage.supports_mvcc() { "mvcc" } else { "standard" }.into())],
                vec![Value::Text("supports_mvcc".into()), Value::Text(self.storage.supports_mvcc().to_string())],
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

    pub(super) fn show_replication_status(&self) -> Result<ExecResult, ExecError> {
        let mut result_rows: Vec<Row> = Vec::new();

        // If we have a live replication manager, show real status
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

    // ========================================================================
    // GRANT / REVOKE
    // ========================================================================

    pub(super) async fn execute_grant(
        &self,
        privileges: ast::Privileges,
        objects: Option<ast::GrantObjects>,
        grantees: Vec<ast::Grantee>,
    ) -> Result<ExecResult, ExecError> {
        let privs = parse_privileges(&privileges);
        let object_names = objects.as_ref().map(parse_grant_objects).unwrap_or_else(|| vec!["*".to_string()]);
        let mut roles = self.roles.write().await;

        for grantee in &grantees {
            let role_name = grantee_name(grantee);
            let role = roles.entry(role_name.clone()).or_insert_with(|| RoleDef {
                name: role_name,
                password_hash: None,
                is_superuser: false,
                can_login: false,
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
        let privs = parse_privileges(&privileges);
        let object_names = objects.as_ref().map(parse_grant_objects).unwrap_or_else(|| vec!["*".to_string()]);
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

        Ok(ExecResult::Command {
            tag: "REVOKE".into(),
            rows_affected: 0,
        })
    }

    pub(super) async fn execute_create_role(
        &self,
        create_role: ast::CreateRole,
    ) -> Result<ExecResult, ExecError> {
        let mut roles = self.roles.write().await;
        for name in &create_role.names {
            let role_name = name.to_string();
            let mut role = RoleDef {
                name: role_name.clone(),
                password_hash: None,
                is_superuser: create_role.superuser.unwrap_or(false),
                can_login: create_role.login.unwrap_or(false),
                privileges: HashMap::new(),
            };
            if let Some(ref pwd) = create_role.password {
                match pwd {
                    ast::Password::Password(expr) => {
                        let raw = expr.to_string().trim_matches('\'').to_string();
                        role.password_hash = Some(blake3::hash(raw.as_bytes()).to_hex().to_string());
                    }
                    ast::Password::NullPassword => {}
                }
            }
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
        let mut roles = self.roles.write().await;
        let role = roles.get_mut(role_name).ok_or_else(|| {
            ExecError::Unsupported(format!("role '{role_name}' does not exist"))
        })?;

        match operation {
            ast::AlterRoleOperation::WithOptions { options } => {
                for opt in &options {
                    match opt {
                        ast::RoleOption::SuperUser(v) => role.is_superuser = *v,
                        ast::RoleOption::Login(v) => role.can_login = *v,
                        ast::RoleOption::Password(pwd) => match pwd {
                            ast::Password::Password(expr) => {
                                let raw = expr.to_string().trim_matches('\'').to_string();
                                role.password_hash = Some(blake3::hash(raw.as_bytes()).to_hex().to_string());
                            }
                            ast::Password::NullPassword => {
                                role.password_hash = None;
                            }
                        },
                        _ => {} // Ignore unsupported role options
                    }
                }
            }
            ast::AlterRoleOperation::RenameRole { role_name: new_name } => {
                let new_name = new_name.value.clone();
                let mut role_data = roles.remove(role_name).unwrap();
                role_data.name = new_name.clone();
                roles.insert(new_name, role_data);
            }
            _ => {} // Ignore unsupported alter operations
        }

        Ok(ExecResult::Command {
            tag: "ALTER ROLE".into(),
            rows_affected: 0,
        })
    }

    // ========================================================================
    // Cursors
    // ========================================================================

    pub(super) async fn execute_declare_cursor(
        &self,
        stmt: &ast::Declare,
    ) -> Result<ExecResult, ExecError> {
        let cursor_name = stmt.names.first()
            .map(|n| n.value.clone())
            .unwrap_or_else(|| "unnamed".to_string());

        let query = stmt.for_query.as_ref()
            .ok_or_else(|| ExecError::Unsupported("DECLARE requires FOR query".into()))?;

        let result = self.execute_query(*query.clone()).await?;
        match result {
            ExecResult::Select { columns, rows } => {
                let sess = self.current_session();
                let mut cursors = sess.cursors.write().await;
                cursors.insert(cursor_name.clone(), CursorDef {
                    name: cursor_name,
                    rows,
                    columns,
                    position: 0,
                });
                Ok(ExecResult::Command {
                    tag: "DECLARE CURSOR".into(),
                    rows_affected: 0,
                })
            }
            _ => Err(ExecError::Unsupported("DECLARE cursor query must be SELECT".into())),
        }
    }

    pub(super) async fn execute_fetch_cursor(
        &self,
        cursor_name: &str,
        direction: &ast::FetchDirection,
    ) -> Result<ExecResult, ExecError> {
        let count = match direction {
            ast::FetchDirection::Count { limit: ast::Value::Number(n, _) } => {
                n.parse::<usize>().unwrap_or(1)
            }
            ast::FetchDirection::Next | ast::FetchDirection::Forward { .. } => 1,
            ast::FetchDirection::All | ast::FetchDirection::ForwardAll => usize::MAX,
            ast::FetchDirection::First => 1,
            _ => 1,
        };

        let sess = self.current_session();
        let mut cursors = sess.cursors.write().await;
        let cursor = cursors.get_mut(cursor_name)
            .ok_or_else(|| ExecError::Unsupported(format!("cursor '{cursor_name}' not found")))?;

        let start = cursor.position;
        let end = start.saturating_add(count).min(cursor.rows.len());
        let fetched: Vec<Row> = cursor.rows[start..end].to_vec();
        cursor.position = end;

        Ok(ExecResult::Select {
            columns: cursor.columns.clone(),
            rows: fetched,
        })
    }

    pub(super) async fn execute_close_cursor(
        &self,
        cursor: ast::CloseCursor,
    ) -> Result<ExecResult, ExecError> {
        let sess = self.current_session();
        match cursor {
            ast::CloseCursor::Specific { name } => {
                sess.cursors.write().await.remove(&name.value);
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
        let maybe_rep = self.raft_replicator.read().clone();
        if let Some(replicator) = maybe_rep {
            replicator.broadcast_pubsub(channel, &msg).await;
        }

        Ok(ExecResult::Command {
            tag: "NOTIFY".into(),
            rows_affected: 0,
        })
    }

    pub(super) async fn execute_listen(
        &self,
        channel: &str,
    ) -> Result<ExecResult, ExecError> {
        // Subscribe on the local async hub.
        {
            let mut pubsub = self.pubsub.write().await;
            let _ = pubsub.subscribe(channel);
        }

        // Gossip to peers: tell them this node now subscribes to `channel`.
        let snapshot = self.dist_pubsub.read().local_subscription_snapshot();
        let maybe_rep = self.raft_replicator.read().clone();
        if let Some(replicator) = maybe_rep {
            replicator.broadcast_gossip(snapshot).await;
        }

        Ok(ExecResult::Command {
            tag: "LISTEN".into(),
            rows_affected: 0,
        })
    }

    pub(super) async fn execute_unlisten(
        &self,
        channel: &str,
    ) -> Result<ExecResult, ExecError> {
        // Unsubscribing is handled by dropping the receiver; we just acknowledge
        let _ = channel;
        Ok(ExecResult::Command {
            tag: "UNLISTEN".into(),
            rows_affected: 0,
        })
    }
}
