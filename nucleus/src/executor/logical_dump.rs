//! Logical (SQL-text) backup — T2.1 v2.
//!
//! Unlike the physical byte-copy in `crate::backup` (which is tied to the
//! on-disk page format and must be taken against a stopped instance), a logical
//! dump emits portable SQL — `CREATE TABLE` / `CREATE INDEX` / `INSERT` — that
//! **replays through the executor**, so it survives on-disk-format and
//! schema-version changes and cannot reintroduce corrupt rows (every INSERT is
//! re-checked against constraints, per T0.1). Restore into a fresh instance is
//! just running the script.
//!
//! Consistency: each table is scanned under its own MVCC snapshot, so the dump
//! is per-table consistent. For a whole-DB point-in-time image, take it against
//! a quiesced instance (as with physical backup).
//!
//! # What the script carries (M4)
//!
//! Schemas, extensions, enum types, roles (including SCRAM verifiers and role
//! memberships), tables, rows, sequences **and their current position**, secondary
//! indexes, views, materialized views, stored functions, triggers, table
//! privileges, RLS policies, and row-security enablement — emitted in
//! dependency-correct order (tables are topologically sorted by foreign key so a
//! child never precedes its parent).
//!
//! Sequence position matters for correctness, not tidiness: restoring a SERIAL
//! table without replaying `setval` restarts the counter at 1 and the next insert
//! collides with an existing primary key.
//!
//! # Documented gaps (deliberate, not silent)
//!
//! - The bootstrap `nucleus` superuser is NOT emitted: a restore must not
//!   overwrite the target's own administrative credential.
//! - Column **masking** policies have a SQL DDL surface (`CREATE MASKING
//!   POLICY`) but this dump does not emit it yet, so a SQL-text dump cannot
//!   reconstruct them.
//!   [`Executor::logical_dump_gaps`] reports them so a caller can fail loudly
//!   rather than restore a database that silently unmasks columns.
//! - Non-relational model state (KV, graph, streams, blobs, …) lives outside the
//!   SQL catalog and is likewise reported rather than emitted.

use super::ExecError;
use super::helpers::value_to_text_string_impl;
use super::schema_types::{
    FunctionDef, FunctionKind, MaterializedViewDef, Privilege, RoleDef, SequenceDef, TriggerDef,
    TriggerEvent, TriggerTiming, ViewDef,
};
use crate::catalog::{FkAction, IndexDef, TableConstraint, TableDef};
use crate::security::{PolicyCommand, RlsPolicy, RlsPredicate};
use crate::types::{DataType, Value};
use std::collections::{HashMap, HashSet};

/// Open a persistent (disk-backed) executor at `data_dir` for one-shot logical
/// maintenance (dump/restore from the CLI). Unlike `embedded::Database::open`,
/// which reconstructs table definitions from storage metadata alone (dropping
/// constraints), this loads `catalog.json`, so PRIMARY KEY / UNIQUE / CHECK / FK
/// survive into a dump. Replays the WAL on open. Creates the directory if
/// missing (so restore can target a fresh location).
#[cfg(feature = "server")]
pub async fn open_persistent_executor(
    data_dir: &std::path::Path,
) -> Result<std::sync::Arc<super::Executor>, ExecError> {
    use crate::storage::buffered_engine::BufferedDiskEngine;
    use crate::storage::persistence::CatalogPersistence;
    use crate::storage::{DiskEngine, StorageEngine};
    use std::sync::Arc;

    std::fs::create_dir_all(data_dir).map_err(|e| {
        ExecError::Runtime(format!("create data dir '{}': {e}", data_dir.display()))
    })?;
    let catalog = Arc::new(crate::catalog::Catalog::new());
    let catalog_path = data_dir.join("catalog.json");
    // Best-effort: a fresh target directory has no catalog yet.
    let _ = CatalogPersistence::new(&catalog_path)
        .load_catalog(&catalog)
        .await;
    let db_path = data_dir.join("nucleus.db");
    let engine = Arc::new(DiskEngine::open(&db_path, catalog.clone()).map_err(ExecError::Storage)?);
    let storage: Arc<dyn StorageEngine> = Arc::new(BufferedDiskEngine::new(engine));
    let ex = Arc::new(super::Executor::new_with_persistence(
        catalog,
        storage,
        Some(catalog_path),
        Some(data_dir),
    ));
    // Roles, RLS policies, views, sequences, and functions live in meta.json,
    // NOT in catalog.json. Without this the executor opens with an empty
    // security/metadata catalog and the dump silently emits only tables+rows —
    // which is precisely the data-only export this work exists to fix. The lib
    // tests build executors that already hold this state in memory, so they
    // pass either way; only the CLI path exposes the gap.
    ex.load_meta().await;
    // B-tree indexes live only in the engine's in-memory registry, so a
    // freshly opened directory has none of them however many the catalog
    // lists. Every caller of this helper — the CLI's dump/restore paths and
    // the wire tests — otherwise runs with every index missing and every
    // indexed query scanning.
    ex.rebuild_persistent_indexes().await;
    Ok(ex)
}

/// Wrap `s` as a single-quoted SQL string literal, doubling embedded quotes.
fn quote_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Render a `Value` as a SQL literal for an INSERT. Numbers/booleans are bare;
/// everything else is a quoted literal that the target column coerces on insert
/// (the same coercion an ordinary INSERT applies), except vectors which use the
/// `VECTOR('[...]')` constructor the parser expects.
pub(super) fn value_to_sql_literal(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        Value::Int32(i) => i.to_string(),
        Value::Int64(i) => i.to_string(),
        Value::Float64(f) => {
            if f.is_finite() {
                f.to_string()
            } else if f.is_nan() {
                "'NaN'::double precision".to_string()
            } else if *f > 0.0 {
                "'Infinity'::double precision".to_string()
            } else {
                "'-Infinity'::double precision".to_string()
            }
        }
        Value::Numeric(n) => n.clone(),
        Value::Vector(vec) => format!(
            "VECTOR('[{}]')",
            vec.iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ),
        // Text, Bytea, Timestamp, Date, TimestampTz, Uuid, Jsonb, Array, Interval:
        // quote their canonical text form and let column-type coercion parse it.
        other => quote_str(&value_to_text_string_impl(other)),
    }
}

fn fk_action_sql(a: &FkAction) -> &'static str {
    match a {
        FkAction::NoAction => "NO ACTION",
        FkAction::Restrict => "RESTRICT",
        FkAction::Cascade => "CASCADE",
        FkAction::SetNull => "SET NULL",
        FkAction::SetDefault => "SET DEFAULT",
    }
}

fn render_constraint(c: &TableConstraint) -> String {
    match c {
        TableConstraint::PrimaryKey { columns, .. } => {
            format!("PRIMARY KEY ({})", columns.join(", "))
        }
        TableConstraint::Unique { columns, .. } => {
            format!("UNIQUE ({})", columns.join(", "))
        }
        TableConstraint::Check { expr, .. } => format!("CHECK ({expr})"),
        TableConstraint::ForeignKey {
            columns,
            ref_table,
            ref_columns,
            on_delete,
            on_update,
            ..
        } => format!(
            "FOREIGN KEY ({}) REFERENCES {} ({}) ON DELETE {} ON UPDATE {}",
            columns.join(", "),
            ref_table,
            ref_columns.join(", "),
            fk_action_sql(on_delete),
            fk_action_sql(on_update),
        ),
    }
}

/// Render `CREATE TABLE` DDL for a table definition.
pub(super) fn render_create_table(def: &TableDef) -> String {
    let mut items: Vec<String> = def
        .columns
        .iter()
        .map(|c| {
            let mut s = format!("{} {}", c.name, c.data_type);
            if !c.nullable {
                s.push_str(" NOT NULL");
            }
            if let Some(d) = &c.default_expr {
                s.push_str(&format!(" DEFAULT {d}"));
            }
            s
        })
        .collect();
    for con in &def.constraints {
        items.push(render_constraint(con));
    }
    format!("CREATE TABLE {} (\n  {}\n);", def.name, items.join(",\n  "))
}

/// Render `CREATE INDEX` DDL. Encrypted indexes (BTree + an `encryption_mode`
/// option) round-trip as `USING encrypted`; vector/GIN/etc. use their type.
pub(super) fn render_create_index(idx: &IndexDef) -> String {
    use crate::catalog::IndexType;
    let using = if idx.options.contains_key("encryption_mode") {
        Some("encrypted".to_string())
    } else {
        match &idx.index_type {
            IndexType::BTree => None,
            other => Some(other.to_string().to_lowercase()),
        }
    };
    let unique = if idx.unique { "UNIQUE " } else { "" };
    match using {
        Some(u) => format!(
            "CREATE {unique}INDEX {} ON {} USING {u} ({});",
            idx.name,
            idx.table_name,
            idx.columns.join(", ")
        ),
        None => format!(
            "CREATE {unique}INDEX {} ON {} ({});",
            idx.name,
            idx.table_name,
            idx.columns.join(", ")
        ),
    }
}

/// Render an `INSERT` for one row against a table definition.
pub(super) fn render_insert(def: &TableDef, row: &[Value]) -> String {
    let cols: Vec<&str> = def.columns.iter().map(|c| c.name.as_str()).collect();
    let vals: Vec<String> = row.iter().map(value_to_sql_literal).collect();
    format!(
        "INSERT INTO {} ({}) VALUES ({});",
        def.name,
        cols.join(", "),
        vals.join(", ")
    )
}

/// The bootstrap superuser. Never emitted into a dump: replaying a foreign
/// administrative credential into a live instance is a privilege-escalation
/// footgun, and dropping the target's own password would lock the operator out.
const BOOTSTRAP_ROLE: &str = "nucleus";
/// Seeded by every fresh executor; re-creating them would fail or be a no-op.
const BOOTSTRAP_SCHEMA: &str = "public";
const BOOTSTRAP_EXTENSION: &str = "plpgsql";

/// Render an identifier list for `TO a, b` style clauses.
fn join_names(names: &[String]) -> String {
    names.join(", ")
}

/// Order `tables` so that a table always follows every table it references by
/// foreign key. `CREATE TABLE` rejects an FK whose target does not exist yet, and
/// an `INSERT` of a child row before its parent violates the constraint — so a
/// dump emitted in `HashMap` order restores only by luck. Ties break by name, so
/// the same database always dumps to the same bytes (diffable backups).
///
/// A cycle (mutually-referencing tables) cannot be expressed as a linear script;
/// the remaining tables are appended in name order so the restore fails loudly on
/// the FK rather than silently omitting them.
fn topo_sort_tables(tables: &[std::sync::Arc<TableDef>]) -> Vec<std::sync::Arc<TableDef>> {
    let mut pending: Vec<std::sync::Arc<TableDef>> = tables.to_vec();
    pending.sort_by(|a, b| a.name.cmp(&b.name));

    let mut emitted: HashSet<String> = HashSet::new();
    let mut ordered: Vec<std::sync::Arc<TableDef>> = Vec::with_capacity(pending.len());
    loop {
        let mut progressed = false;
        let mut still_pending = Vec::new();
        for def in pending {
            let ready = def.constraints.iter().all(|c| match c {
                TableConstraint::ForeignKey { ref_table, .. } => {
                    // A self-reference is satisfied by the table's own CREATE.
                    ref_table.eq_ignore_ascii_case(&def.name) || emitted.contains(ref_table)
                }
                _ => true,
            });
            if ready {
                emitted.insert(def.name.clone());
                ordered.push(def);
                progressed = true;
            } else {
                still_pending.push(def);
            }
        }
        pending = still_pending;
        if pending.is_empty() || !progressed {
            ordered.extend(pending);
            break;
        }
    }
    ordered
}

/// Render `CREATE ROLE`. The stored SCRAM verifier is emitted verbatim; the
/// executor stores an already-encoded verifier as-is instead of re-hashing it
/// (see `super::store_password_literal`), so credentials survive a round trip.
/// Memberships are NOT emitted here — they become `GRANT` statements once every
/// role exists.
pub(super) fn render_create_role(role: &RoleDef) -> String {
    let mut opts = vec![
        if role.can_login { "LOGIN" } else { "NOLOGIN" }.to_string(),
        if role.is_superuser {
            "SUPERUSER"
        } else {
            "NOSUPERUSER"
        }
        .to_string(),
        if role.bypass_rls {
            "BYPASSRLS"
        } else {
            "NOBYPASSRLS"
        }
        .to_string(),
    ];
    if let Some(hash) = &role.password_hash {
        opts.push(format!("PASSWORD {}", quote_str(hash)));
    }
    // A dump that drops the expiry restores a password that no longer expires,
    // which is the failure direction that matters.
    if let Some(us) = role.valid_until {
        opts.push(format!(
            "VALID UNTIL {}",
            quote_str(&crate::types::Value::Timestamp(us).to_string())
        ));
    }
    format!("CREATE ROLE {} WITH {};", role.name, opts.join(" "))
}

fn privilege_sql(p: &Privilege) -> &'static str {
    match p {
        Privilege::Select => "SELECT",
        Privilege::Insert => "INSERT",
        Privilege::Update => "UPDATE",
        Privilege::Delete => "DELETE",
        Privilege::All => "ALL",
        Privilege::Create => "CREATE",
        Privilege::Drop => "DROP",
        Privilege::Usage => "USAGE",
    }
}

/// Render the `GRANT … ON <object> TO <role>` statements a role's privilege map
/// implies. The wildcard object `*` (a cluster-wide grant) round-trips through
/// `GRANT … ON ALL TABLES TO role`, and `<schema>.*` through
/// `GRANT … ON ALL TABLES IN SCHEMA <schema> TO role`.
fn render_grants(role: &RoleDef) -> Vec<String> {
    let mut objects: Vec<(&String, &Vec<Privilege>)> = role.privileges.iter().collect();
    objects.sort_by(|a, b| a.0.cmp(b.0));
    objects
        .into_iter()
        .filter(|(_, privs)| !privs.is_empty())
        .map(|(object, privs)| {
            let list = privs
                .iter()
                .map(|p| privilege_sql(p).to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let target = match object.as_str() {
                "*" => "ALL TABLES".to_string(),
                other => match other.strip_suffix(".*") {
                    Some(schema) => format!("ALL TABLES IN SCHEMA {schema}"),
                    None => other.to_string(),
                },
            };
            format!("GRANT {list} ON {target} TO {};", role.name)
        })
        .collect()
}

/// Render an RLS predicate back into the SQL subset `compile_rls_predicate`
/// accepts, so a dumped policy recompiles to the identical predicate.
fn render_rls_predicate(p: &RlsPredicate) -> String {
    match p {
        RlsPredicate::ColumnEqStr { column, value, .. } => {
            format!("{column} = {}", quote_str(value))
        }
        RlsPredicate::ColumnEqTenant { column, .. } => {
            format!("{column} = current_setting('nucleus.tenant_id')")
        }
        RlsPredicate::ColumnEqUser { column, .. } => format!("{column} = CURRENT_USER"),
        // Every constant is re-emitted quoted. The compiler reads a quoted
        // number back as its digits, which is exactly what the row map holds,
        // so `amount > 100` round-trips to the same comparison — while a value
        // that merely looks numeric stays intact instead of being mangled.
        RlsPredicate::ColumnCmp {
            column, op, value, ..
        } => format!("{column} {} {}", op.as_sql(), quote_str(value)),
        RlsPredicate::ColumnInList { column, values, .. } => {
            let rendered: Vec<String> = values.iter().map(|v| quote_str(v)).collect();
            format!("{column} IN ({})", rendered.join(", "))
        }
        RlsPredicate::ColumnIsNull {
            column, negated, ..
        } => {
            if *negated {
                format!("{column} IS NOT NULL")
            } else {
                format!("{column} IS NULL")
            }
        }
        RlsPredicate::HasRole { role } => format!("has_role({})", quote_str(role)),
        RlsPredicate::And(a, b) => format!(
            "({} AND {})",
            render_rls_predicate(a),
            render_rls_predicate(b)
        ),
        RlsPredicate::Or(a, b) => format!(
            "({} OR {})",
            render_rls_predicate(a),
            render_rls_predicate(b)
        ),
        RlsPredicate::Not(inner) => format!("NOT ({})", render_rls_predicate(inner)),
        RlsPredicate::AlwaysTrue => "true".to_string(),
        RlsPredicate::AlwaysFalse => "false".to_string(),
    }
}

/// Render `CREATE POLICY`. `USING` is invalid for INSERT policies and
/// `WITH CHECK` is invalid for SELECT policies, matching the DDL validation the
/// executor applies — so the emitted statement is accepted on replay.
fn render_create_policy(policy: &RlsPolicy) -> String {
    let command = match policy.command {
        PolicyCommand::All => "ALL",
        PolicyCommand::Select => "SELECT",
        PolicyCommand::Insert => "INSERT",
        PolicyCommand::Update => "UPDATE",
        PolicyCommand::Delete => "DELETE",
    };
    let kind = if policy.permissive {
        "PERMISSIVE"
    } else {
        "RESTRICTIVE"
    };
    let mut sql = format!(
        "CREATE POLICY {} ON {} AS {kind} FOR {command}",
        policy.name, policy.table
    );
    if !policy.target_roles.is_empty() {
        sql.push_str(&format!(" TO {}", join_names(&policy.target_roles)));
    }
    if policy.command != PolicyCommand::Insert {
        sql.push_str(&format!(
            " USING ({})",
            render_rls_predicate(&policy.predicate)
        ));
    }
    if policy.command != PolicyCommand::Select {
        // An INSERT policy keeps its check in `predicate` when no explicit
        // WITH CHECK was given, because USING is not a legal INSERT clause.
        let check = policy.check_predicate.as_ref().or({
            if policy.command == PolicyCommand::Insert {
                Some(&policy.predicate)
            } else {
                None
            }
        });
        if let Some(check) = check {
            sql.push_str(&format!(" WITH CHECK ({})", render_rls_predicate(check)));
        }
    }
    sql.push(';');
    sql
}

/// Render `CREATE SEQUENCE` with the source's exact parameters. Emitted AFTER
/// the tables so it overwrites the implicit sequence `CREATE TABLE … SERIAL`
/// creates, which would otherwise reset a customised increment/min/max.
fn render_create_sequence(name: &str, seq: &SequenceDef) -> String {
    format!(
        "CREATE SEQUENCE {name} INCREMENT BY {} MINVALUE {} MAXVALUE {} START WITH {};",
        seq.increment, seq.min_value, seq.max_value, seq.start
    )
}

/// Restore a sequence's position. Without this a restored SERIAL restarts at 1
/// and immediately collides with existing primary keys.
fn render_setval(name: &str, seq: &SequenceDef) -> String {
    format!("SELECT setval('{name}', {});", seq.current)
}

fn render_create_view(view: &ViewDef) -> String {
    if view.columns.is_empty() {
        format!("CREATE VIEW {} AS {};", view.name, view.sql)
    } else {
        format!(
            "CREATE VIEW {} ({}) AS {};",
            view.name,
            join_names(&view.columns),
            view.sql
        )
    }
}

fn render_create_matview(mv: &MaterializedViewDef) -> String {
    format!("CREATE MATERIALIZED VIEW {} AS {};", mv.name, mv.sql)
}

/// Render `CREATE FUNCTION`. The body is dollar-quoted with a tag the emitter
/// controls, so a body containing `;` or quotes survives statement splitting.
fn render_create_function(f: &FunctionDef) -> String {
    let params = f
        .params
        .iter()
        .map(|(n, t)| format!("{n} {t}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = format!("CREATE FUNCTION {}({params})", f.name);
    match (&f.kind, &f.return_type) {
        (FunctionKind::Function, Some(rt)) => sql.push_str(&format!(" RETURNS {rt}")),
        // A function with no declared return type still needs one to parse.
        (FunctionKind::Function, None) => sql.push_str(&format!(" RETURNS {}", DataType::Text)),
        (FunctionKind::Procedure, _) => {}
    }
    sql.push_str(&format!(
        " LANGUAGE SQL AS {}{}{};",
        DOLLAR_TAG, f.body, DOLLAR_TAG
    ));
    sql
}

fn render_create_trigger(t: &TriggerDef) -> String {
    let timing = match t.timing {
        TriggerTiming::Before => "BEFORE",
        TriggerTiming::After => "AFTER",
        TriggerTiming::InsteadOf => "INSTEAD OF",
    };
    let events = t
        .events
        .iter()
        .map(|e| match e {
            TriggerEvent::Insert => "INSERT",
            TriggerEvent::Update => "UPDATE",
            TriggerEvent::Delete => "DELETE",
        })
        .collect::<Vec<_>>()
        .join(" OR ");
    let for_each = if t.for_each_row {
        " FOR EACH ROW"
    } else {
        " FOR EACH STATEMENT"
    };
    format!(
        "CREATE TRIGGER {} {timing} {events} ON {}{for_each} EXECUTE FUNCTION {}();",
        t.name, t.table_name, t.body
    )
}

/// Dollar-quote tag used for function bodies. Chosen to be implausible inside a
/// user body; [`split_sql_statements`] treats its contents as opaque.
const DOLLAR_TAG: &str = "$nucleus_dump$";

/// Something a logical dump provably cannot express, reported so a caller can
/// refuse a restore instead of silently losing it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DumpGap {
    pub kind: &'static str,
    pub detail: String,
}

impl super::Executor {
    /// Objects present in this database that [`dump_logical`](Self::dump_logical)
    /// cannot represent as SQL text. Empty means the dump is complete.
    ///
    /// This exists so "the dump is lossy" is a value a program can branch on,
    /// not a sentence in a doc comment.
    pub fn logical_dump_gaps(&self) -> Vec<DumpGap> {
        let mut gaps = Vec::new();
        let security = self.security.read();
        for policy in security.masking.all_policies() {
            gaps.push(DumpGap {
                kind: "masking_policy",
                detail: format!(
                    "column mask on {}.{} for role {} is not emitted by the dump \
                     (CREATE MASKING POLICY exists; dump support pending)",
                    policy.table, policy.column, policy.role
                ),
            });
        }
        gaps
    }

    /// Produce a portable SQL script that reconstructs this database when
    /// replayed through [`restore_logical`](Self::restore_logical): schemas,
    /// extensions, enum types, roles, tables, rows, sequences and their current
    /// position, indexes, views, materialized views, functions, triggers,
    /// privileges, RLS policies, and row-security enablement.
    ///
    /// Statements are emitted in dependency-correct order and every collection is
    /// name-sorted, so the same database always produces the same script.
    pub async fn dump_logical(&self) -> Result<String, ExecError> {
        let mut out = String::new();
        out.push_str("-- Nucleus logical dump (portable SQL, replayable through the executor)\n");

        // ── 1. Namespaces, extensions, and user-defined types ───────────────
        let mut schemas: Vec<String> = self
            .schemas
            .read()
            .await
            .iter()
            .filter(|s| s.as_str() != BOOTSTRAP_SCHEMA)
            .cloned()
            .collect();
        schemas.sort();
        for schema in &schemas {
            out.push_str(&format!("CREATE SCHEMA {schema};\n"));
        }

        let mut extensions: Vec<(String, String, String)> = self
            .extensions
            .read()
            .iter()
            .filter(|(name, _)| name.as_str() != BOOTSTRAP_EXTENSION)
            .map(|(_, e)| (e.name.clone(), e.schema.clone(), e.version.clone()))
            .collect();
        extensions.sort();
        for (name, schema, version) in &extensions {
            out.push_str(&format!(
                "CREATE EXTENSION IF NOT EXISTS {name} WITH SCHEMA {schema} VERSION '{version}';\n"
            ));
        }

        let mut enum_types = self.catalog.list_enum_types().await;
        enum_types.sort();
        for name in &enum_types {
            if let Some(labels) = self.catalog.get_enum_type(name).await {
                let rendered = labels
                    .iter()
                    .map(|l| quote_str(l))
                    .collect::<Vec<_>>()
                    .join(", ");
                out.push_str(&format!("CREATE TYPE {name} AS ENUM ({rendered});\n"));
            }
        }

        // ── 2. Roles, before anything that can reference them ───────────────
        // Policies name roles in TO clauses and CREATE POLICY rejects an unknown
        // role, so roles must exist first.
        let role_snapshot: Vec<RoleDef> = {
            let roles = self.roles.read().await;
            let mut v: Vec<RoleDef> = roles
                .values()
                .filter(|r| r.name != BOOTSTRAP_ROLE)
                .cloned()
                .collect();
            v.sort_by(|a, b| a.name.cmp(&b.name));
            v
        };
        if !role_snapshot.is_empty() {
            out.push('\n');
        }
        for role in &role_snapshot {
            out.push_str(&render_create_role(role));
            out.push('\n');
        }
        // Memberships come after every CREATE ROLE so the parent always exists.
        for role in &role_snapshot {
            for parent in &role.member_of {
                // `GRANT ROLE x TO y`, not `GRANT x TO y`: the latter sends the
                // parser down the privilege path and fails on the role name.
                if parent != BOOTSTRAP_ROLE {
                    out.push_str(&format!("GRANT ROLE {parent} TO {};\n", role.name));
                }
            }
        }

        // ── 3. Tables, in foreign-key dependency order ──────────────────────
        let tables = topo_sort_tables(&self.catalog.list_tables().await);
        if !tables.is_empty() {
            out.push('\n');
        }
        for def in &tables {
            out.push_str(&render_create_table(def));
            out.push('\n');
        }

        // ── 4. Sequences, AFTER the tables so an explicit definition wins over
        //       the implicit one `CREATE TABLE … SERIAL` just installed ───────
        let sequences: Vec<(String, SequenceDef)> = {
            let seqs = self.sequences.read();
            let mut v: Vec<(String, SequenceDef)> = seqs
                .iter()
                .map(|(name, mu)| (name.clone(), mu.lock().clone()))
                .collect();
            v.sort_by(|a, b| a.0.cmp(&b.0));
            v
        };
        if !sequences.is_empty() {
            out.push('\n');
        }
        for (name, seq) in &sequences {
            out.push_str(&render_create_sequence(name, seq));
            out.push('\n');
        }

        // ── 5. Data, in the same dependency order (a child row must not
        //       precede its parent) ────────────────────────────────────────
        for def in &tables {
            let rows = self.storage_for(&def.name).scan(&def.name).await?;
            if !rows.is_empty() {
                out.push('\n');
            }
            for row in &rows {
                out.push_str(&render_insert(def, row));
                out.push('\n');
            }
        }

        // ── 6. Sequence positions, after the data ───────────────────────────
        // Restoring the definition without the position restarts a SERIAL at 1,
        // so the first insert into the restored table collides with an existing
        // primary key. This is the difference between a restored database that
        // is writable and one that is not.
        if !sequences.is_empty() {
            out.push('\n');
        }
        for (name, seq) in &sequences {
            out.push_str(&render_setval(name, seq));
            out.push('\n');
        }

        // ── 7. Indexes, so the bulk load above isn't slowed by maintenance ──
        //    Skip indexes that merely back a PRIMARY KEY / UNIQUE constraint —
        //    the CREATE TABLE above already recreates those, so re-emitting them
        //    would fail with "index already exists".
        let mut wrote_index_header = false;
        for def in &tables {
            let constraint_cols: Vec<&[String]> = def
                .constraints
                .iter()
                .filter_map(|c| match c {
                    TableConstraint::PrimaryKey { columns, .. }
                    | TableConstraint::Unique { columns, .. } => Some(columns.as_slice()),
                    _ => None,
                })
                .collect();
            let mut indexes = self.catalog.get_indexes(&def.name).await;
            indexes.sort_by(|a, b| a.name.cmp(&b.name));
            for idx in indexes {
                if constraint_cols
                    .iter()
                    .any(|cols| *cols == idx.columns.as_slice())
                {
                    continue;
                }
                if !wrote_index_header {
                    out.push('\n');
                    wrote_index_header = true;
                }
                out.push_str(&render_create_index(&idx));
                out.push('\n');
            }
        }

        // ── 8. Views, then materialized views (which execute their query at
        //       creation time and so must follow the data) ──────────────────
        let views: Vec<ViewDef> = {
            let views = self.views.read().await;
            let mut v: Vec<ViewDef> = views.values().cloned().collect();
            v.sort_by(|a, b| a.name.cmp(&b.name));
            v
        };
        if !views.is_empty() {
            out.push('\n');
        }
        for view in &views {
            out.push_str(&render_create_view(view));
            out.push('\n');
        }

        let matviews: Vec<MaterializedViewDef> = {
            let mvs = self.materialized_views.read().await;
            let mut v: Vec<MaterializedViewDef> = mvs.values().cloned().collect();
            v.sort_by(|a, b| a.name.cmp(&b.name));
            order_matviews(v)
        };
        if !matviews.is_empty() {
            out.push('\n');
        }
        for mv in &matviews {
            out.push_str(&render_create_matview(mv));
            out.push('\n');
        }

        // ── 9. Functions, then triggers that may call them ──────────────────
        let functions: Vec<FunctionDef> = {
            let fns = self.functions.read();
            let mut v: Vec<FunctionDef> = fns.values().cloned().collect();
            v.sort_by(|a, b| a.name.cmp(&b.name));
            v
        };
        if !functions.is_empty() {
            out.push('\n');
        }
        for f in &functions {
            out.push_str(&render_create_function(f));
            out.push('\n');
        }

        let triggers: Vec<TriggerDef> = {
            let mut v = self.triggers.read().await.clone();
            v.sort_by(|a, b| a.name.cmp(&b.name));
            v
        };
        if !triggers.is_empty() {
            out.push('\n');
        }
        for t in &triggers {
            out.push_str(&render_create_trigger(t));
            out.push('\n');
        }

        // ── 10. Privileges, once every role and object exists ───────────────
        let mut wrote_grant_header = false;
        for role in &role_snapshot {
            for stmt in render_grants(role) {
                if !wrote_grant_header {
                    out.push('\n');
                    wrote_grant_header = true;
                }
                out.push_str(&stmt);
                out.push('\n');
            }
        }

        // ── 11. Row-level security LAST ─────────────────────────────────────
        // Emitting the security boundary after the data means the bulk load
        // above is never filtered by a half-restored policy set, and a restore
        // that dies partway leaves the table unreachable rather than exposed.
        let (mut rls_tables, mut policies) = {
            let security = self.security.read();
            let tables = security.rls.enabled_tables();
            let policies: Vec<RlsPolicy> =
                security.rls.all_policies().into_iter().cloned().collect();
            (tables, policies)
        };
        rls_tables.sort();
        policies.sort_by(|a, b| (&a.table, &a.name).cmp(&(&b.table, &b.name)));
        if !policies.is_empty() || !rls_tables.is_empty() {
            out.push('\n');
        }
        for policy in &policies {
            out.push_str(&render_create_policy(policy));
            out.push('\n');
        }
        for table in &rls_tables {
            out.push_str(&format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;\n"));
        }

        Ok(out)
    }

    /// Replay a logical dump produced by [`dump_logical`] into this instance.
    /// Statements run through the normal executor, so constraints are enforced
    /// and corrupt rows can't be reintroduced.
    pub async fn restore_logical(&self, script: &str) -> Result<(), ExecError> {
        for stmt in split_sql_statements(script) {
            let trimmed = stmt.trim();
            if trimmed.is_empty() {
                continue;
            }
            self.execute(trimmed).await?;
        }
        Ok(())
    }
}

/// Order materialized views so one built on top of another follows it. A
/// `CREATE MATERIALIZED VIEW` executes its query immediately, so an MV over
/// another MV fails if the dependency has not been created yet. Input is
/// expected name-sorted; ties keep that order, so the output is deterministic.
fn order_matviews(views: Vec<MaterializedViewDef>) -> Vec<MaterializedViewDef> {
    let names: HashSet<&str> = views.iter().map(|v| v.name.as_str()).collect();
    // Only dependencies that are themselves materialized views constrain order.
    let deps: HashMap<String, Vec<String>> = views
        .iter()
        .map(|v| {
            let d = v
                .source_tables
                .iter()
                .filter(|t| t.as_str() != v.name && names.contains(t.as_str()))
                .cloned()
                .collect();
            (v.name.clone(), d)
        })
        .collect();

    let mut pending = views;
    let mut emitted: HashSet<String> = HashSet::new();
    let mut ordered = Vec::with_capacity(pending.len());
    loop {
        let mut progressed = false;
        let mut still_pending = Vec::new();
        for mv in pending {
            let ready = deps[&mv.name].iter().all(|d| emitted.contains(d));
            if ready {
                emitted.insert(mv.name.clone());
                ordered.push(mv);
                progressed = true;
            } else {
                still_pending.push(mv);
            }
        }
        pending = still_pending;
        if pending.is_empty() || !progressed {
            ordered.extend(pending);
            break;
        }
    }
    ordered
}

/// Read a dollar-quote tag (`$$` or `$ident$`) starting at `chars[i]`, if there
/// is one. PostgreSQL requires the tag body to start with a letter or
/// underscore, which is what keeps a positional parameter like `$1` from being
/// mistaken for an opening tag.
fn dollar_tag_at(chars: &[char], i: usize) -> Option<String> {
    if chars.get(i) != Some(&'$') {
        return None;
    }
    let mut j = i + 1;
    if chars.get(j) == Some(&'$') {
        return Some("$$".to_string());
    }
    if !matches!(chars.get(j), Some(c) if c.is_ascii_alphabetic() || *c == '_') {
        return None;
    }
    while matches!(chars.get(j), Some(c) if c.is_ascii_alphanumeric() || *c == '_') {
        j += 1;
    }
    if chars.get(j) == Some(&'$') {
        Some(chars[i..=j].iter().collect())
    } else {
        None
    }
}

/// Split a SQL script into statements on top-level `;`.
///
/// Respects single-quoted string literals (including doubled `''`), `--` line
/// comments, and dollar-quoted bodies — a `;` inside any of those must not split
/// the statement, which is what lets a dumped function body carry SQL of its own.
fn split_sql_statements(script: &str) -> Vec<String> {
    let chars: Vec<char> = script.chars().collect();
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut i = 0;
    let mut in_str = false;
    let mut open_tag: Option<String> = None;

    while i < chars.len() {
        // Inside a dollar-quoted body everything is opaque until the closing tag.
        if let Some(tag) = &open_tag {
            let tag_chars: Vec<char> = tag.chars().collect();
            if chars[i..].starts_with(tag_chars.as_slice()) {
                cur.push_str(tag);
                i += tag_chars.len();
                open_tag = None;
            } else {
                cur.push(chars[i]);
                i += 1;
            }
            continue;
        }
        let c = chars[i];
        if in_str {
            cur.push(c);
            i += 1;
            if c == '\'' {
                // Doubled '' is an escaped quote, not a terminator.
                if chars.get(i) == Some(&'\'') {
                    cur.push('\'');
                    i += 1;
                } else {
                    in_str = false;
                }
            }
            continue;
        }
        match c {
            '\'' => {
                in_str = true;
                cur.push(c);
                i += 1;
            }
            '-' if chars.get(i + 1) == Some(&'-') => {
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
            }
            '$' if dollar_tag_at(&chars, i).is_some() => {
                let tag = dollar_tag_at(&chars, i).expect("checked above");
                cur.push_str(&tag);
                i += tag.chars().count();
                open_tag = Some(tag);
            }
            ';' => {
                out.push(std::mem::take(&mut cur));
                i += 1;
            }
            _ => {
                cur.push(c);
                i += 1;
            }
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur);
    }
    out
}

// ============================================================================
// Online physical backup, driven by the LIVE server
// ============================================================================

impl super::Executor {
    /// Take an online physical backup of this RUNNING instance into `output`.
    ///
    /// This is the `pg_basebackup` shape: the backup is coordinated by the
    /// process that owns the data directory, not by an external command trying
    /// to read files out from under it. An outside process holds no lock, sees
    /// no LSN, and cannot pin WAL retention, so it can only ever produce a torn
    /// copy of a live database — which is why the CLI refuses one. Routing
    /// through the live engine is what makes an online snapshot of a serving
    /// database possible at all.
    ///
    /// Returns the manifest describing the snapshot.
    #[cfg(feature = "server")]
    pub async fn backup_online_to(
        &self,
        output: &std::path::Path,
        force: bool,
    ) -> Result<crate::backup::BackupManifest, ExecError> {
        self.require_security_admin("take a physical backup")?;

        let data_dir = self.data_dir.clone().ok_or_else(|| {
            ExecError::Unsupported(
                "this instance has no data directory (in-memory or embedded); \
                 physical backup requires disk-backed storage"
                    .into(),
            )
        })?;

        // Flush catalog + metadata first so the snapshot carries the schema,
        // roles, and policies that match its rows. Without this the copied
        // catalog.json/meta.json could trail the data file by an unbounded
        // amount, and a restore would come up with a schema that does not
        // describe what it restored.
        let _ = self.persist_catalog().await;

        let coord = self.storage.as_backup_coordinator().ok_or_else(|| {
            ExecError::Unsupported(
                "the active storage engine has no physical snapshot (only the \
                     disk engine does); use a logical dump instead"
                    .into(),
            )
        })?;

        crate::backup::backup_online(&data_dir, output, force, env!("CARGO_PKG_VERSION"), coord)
            .map_err(|e| ExecError::Runtime(format!("online backup failed: {e}")))
    }
}
