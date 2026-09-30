//! pg_catalog emulation for schema introspection.
//!
//! One consistent view of the engine's objects (tables, views, materialized
//! views, sequences, indexes, constraints, enum types) rendered as the
//! PostgreSQL catalog relations that ORMs, drivers and the Neutron CLI's
//! `schema pull` / `schema check` / `migrate generate --mode live` read:
//! `pg_class`, `pg_attribute`, `pg_attrdef`, `pg_constraint`, `pg_index`,
//! `pg_depend`, `pg_inherits`, `pg_trigger`, `pg_rewrite`, `pg_type`,
//! `pg_enum` and `pg_opclass`, plus the deparse functions that read them
//! (`pg_get_expr`, `pg_get_constraintdef`, `pg_get_indexdef`,
//! `pg_get_viewdef`, `format_type`).
//!
//! Object identifiers are NOT positions in a hash map: they are derived from
//! the object's kind and name, so an OID handed out by `pg_class` stays valid
//! while unrelated DDL runs (an introspector reads the relation list, then
//! queries each relation by OID). Uniqueness inside one snapshot is enforced
//! by probing, in name order, so a collision can shift only the later name.
//!
//! What is faithful and what is not:
//! * Types come from the column's declared type where the catalog kept one
//!   (`varchar(20)`, `numeric(10,2)`, `smallint`, `real`, `json`), else from
//!   the engine's `DataType`. See [`crate::catalog::DeclaredType`].
//! * Constraint and default expressions are re-deparsed into PostgreSQL's
//!   spelling for the common node kinds; anything the deparser does not
//!   recognise is returned as the stored text rather than guessed at.
//! * Generated and identity columns are reported through
//!   [`column_generation`], the single seam other streams fill in.
//! * There is one namespace (`public`), no inheritance or partitioning, no
//!   rules, no extension-owned objects and no non-internal triggers beyond
//!   the executor's own trigger list.

use std::collections::HashMap;
use std::sync::Arc;

use sqlparser::ast::{self, Expr};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use super::types::ColMeta;
use super::{ExecError, Executor};
use crate::catalog::{DeclaredType, FkAction, IndexDef, IndexType, TableConstraint, TableDef};
use crate::types::{DataType, Row, Value};

/// `public`.
const PUBLIC_NSP: i32 = 2200;
/// First OID handed out to user objects, as in PostgreSQL.
const FIRST_USER_OID: i64 = 16384;
/// OIDs are folded into 30 bits so they stay positive `int4`.
const OID_SPAN: u64 = 1 << 30;
/// Type OID of pgvector's `vector`, as exposed by `pg_type_info`.
const VECTOR_OID: i32 = 16385;

/// Access-method OIDs beyond the ones `pg_am` already lists.
const AM_HNSW: i32 = 9001;
const AM_IVFFLAT: i32 = 9002;
const AM_FTS: i32 = 9003;
const AM_RTREE: i32 = 9004;

// ============================================================================
// Object identifiers
// ============================================================================

fn fnv1a(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Assigns unique, name-derived OIDs. Keys are `"<kind>:<name>"`.
#[derive(Default)]
struct OidMap {
    by_key: HashMap<String, i32>,
    by_oid: HashMap<i32, String>,
}

impl OidMap {
    fn assign(&mut self, key: String) -> i32 {
        if let Some(o) = self.by_key.get(&key) {
            return *o;
        }
        let mut n = fnv1a(&key) % OID_SPAN;
        loop {
            let oid = (FIRST_USER_OID as u64 + n) as i32;
            if !self.by_oid.contains_key(&oid) {
                self.by_oid.insert(oid, key.clone());
                self.by_key.insert(key, oid);
                return oid;
            }
            n = (n + 1) % OID_SPAN;
        }
    }
}

// ============================================================================
// Types
// ============================================================================

/// `(oid, typname, typlen, typcategory, array oid)` for the built-in types the
/// engine can hand out. Array types are listed separately by `array_of`.
const BUILTIN_TYPES: &[(i32, &str, i32, &str, i32)] = &[
    (16, "bool", 1, "B", 1000),
    (17, "bytea", -1, "U", 1001),
    (19, "name", 64, "S", 1003),
    (20, "int8", 8, "N", 1016),
    (21, "int2", 2, "N", 1005),
    (23, "int4", 4, "N", 1007),
    (25, "text", -1, "S", 1009),
    (26, "oid", 4, "N", 1028),
    (114, "json", -1, "U", 199),
    (700, "float4", 4, "N", 1021),
    (701, "float8", 8, "N", 1022),
    (1042, "bpchar", -1, "S", 1014),
    (1043, "varchar", -1, "S", 1015),
    (1082, "date", 4, "D", 1182),
    (1083, "time", 8, "D", 1183),
    (1114, "timestamp", 8, "D", 1115),
    (1184, "timestamptz", 8, "D", 1185),
    (1186, "interval", 16, "T", 1187),
    (1700, "numeric", -1, "N", 1231),
    (2950, "uuid", 16, "U", 2951),
    (3802, "jsonb", -1, "U", 3807),
];

fn builtin_by_name(name: &str) -> Option<&'static (i32, &'static str, i32, &'static str, i32)> {
    BUILTIN_TYPES.iter().find(|t| t.1 == name)
}

fn builtin_by_oid(oid: i32) -> Option<&'static (i32, &'static str, i32, &'static str, i32)> {
    BUILTIN_TYPES.iter().find(|t| t.0 == oid)
}

fn builtin_by_array_oid(oid: i32) -> Option<&'static (i32, &'static str, i32, &'static str, i32)> {
    BUILTIN_TYPES.iter().find(|t| t.4 == oid)
}

/// The base type name a `DataType` stores under when there is no declaration.
fn default_typname(dt: &DataType) -> &'static str {
    match dt {
        DataType::Bool => "bool",
        DataType::Int32 => "int4",
        DataType::Int64 => "int8",
        DataType::Float64 => "float8",
        DataType::Text => "text",
        DataType::Jsonb => "jsonb",
        DataType::Date => "date",
        DataType::Timestamp => "timestamp",
        DataType::TimestampTz => "timestamptz",
        DataType::Numeric => "numeric",
        DataType::Uuid => "uuid",
        DataType::Bytea => "bytea",
        DataType::Interval => "interval",
        DataType::Vector(_) => "vector",
        DataType::Array(_) | DataType::UserDefined(_) => "text",
    }
}

/// Whether a recorded declaration still describes a column of type `dt`.
/// A declaration recorded before an ALTER COLUMN TYPE the catalog did not
/// mirror must be ignored, never trusted.
fn declaration_fits(dt: &DataType, typname: &str) -> bool {
    match dt {
        DataType::Array(inner) => typname
            .strip_prefix('_')
            .is_some_and(|name| declaration_fits(inner, name)),
        DataType::Int32 => matches!(typname, "int2" | "int4"),
        DataType::Float64 => matches!(typname, "float4" | "float8"),
        DataType::Text => matches!(typname, "text" | "varchar" | "bpchar"),
        DataType::Jsonb => matches!(typname, "json" | "jsonb"),
        DataType::Numeric => typname == "numeric",
        DataType::Timestamp => typname == "timestamp",
        DataType::TimestampTz => typname == "timestamptz",
        DataType::Interval => typname == "interval",
        _ => false,
    }
}

/// Derive the declaration to record for a column written with SQL type `dt`,
/// or `None` when the engine's `DataType` already says everything.
pub(super) fn declared_type_of(dt: &ast::DataType) -> Option<DeclaredType> {
    use ast::{CharacterLength, DataType as D, ExactNumberInfo};
    let d = |typname: &str, typmod: i32| {
        Some(DeclaredType {
            typname: typname.to_string(),
            typmod,
        })
    };
    let char_len = |len: &Option<CharacterLength>| match len {
        Some(CharacterLength::IntegerLength { length, .. }) => i32::try_from(*length + 4).ok(),
        _ => None,
    };
    match dt {
        D::Array(element) => {
            let inner = match element {
                ast::ArrayElemTypeDef::AngleBracket(inner)
                | ast::ArrayElemTypeDef::SquareBracket(inner, _)
                | ast::ArrayElemTypeDef::Parenthesis(inner) => inner,
                ast::ArrayElemTypeDef::None => return None,
            };
            let declaration = declared_type_of(inner)?;
            d(&format!("_{}", declaration.typname), declaration.typmod)
        }
        D::SmallInt(_) | D::Int2(_) | D::TinyInt(_) => d("int2", -1),
        D::Real | D::Float4 => d("float4", -1),
        D::Float(ExactNumberInfo::Precision(p)) if *p <= 24 => d("float4", -1),
        D::Varchar(len) | D::CharVarying(len) | D::CharacterVarying(len) => {
            d("varchar", char_len(len).unwrap_or(-1))
        }
        D::Char(len) | D::Character(len) => d("bpchar", char_len(len).unwrap_or(5)),
        D::JSON => d("json", -1),
        D::Numeric(info) | D::Decimal(info) | D::Dec(info) => match info {
            ExactNumberInfo::None => None,
            ExactNumberInfo::Precision(p) => d("numeric", ((*p as i32) << 16) + 4),
            ExactNumberInfo::PrecisionAndScale(p, s) => {
                d("numeric", (((*p as i32) << 16) | (*s as i32 & 0xffff)) + 4)
            }
        },
        D::Timestamp(Some(p), _) => {
            let tz = matches!(
                dt,
                D::Timestamp(_, ast::TimezoneInfo::WithTimeZone | ast::TimezoneInfo::Tz)
            );
            d(if tz { "timestamptz" } else { "timestamp" }, *p as i32)
        }
        D::Custom(name, _) => {
            let first = name.0.first()?.as_ident()?.value.to_lowercase();
            (first == "smallserial" || first == "serial2").then(|| DeclaredType {
                typname: "int2".into(),
                typmod: -1,
            })
        }
        _ => None,
    }
}

/// Everything the catalog reports about one column's type.
#[derive(Clone, Debug)]
struct ColType {
    oid: i32,
    typname: String,
    typmod: i32,
    len: i32,
    /// `Some(element type oid)` for an array column.
    array_elem: Option<i32>,
}

// ============================================================================
// Snapshot
// ============================================================================

/// One index as `pg_class`/`pg_index` see it.
pub(super) struct IndexEntry {
    pub oid: i32,
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub kind: IndexType,
    /// Name of the PRIMARY KEY / UNIQUE constraint this index backs.
    pub backs_constraint: Option<String>,
    pub primary: bool,
    pub def: Option<Arc<IndexDef>>,
}

/// A view or materialized view, as far as the catalog needs it.
pub(super) struct ViewEntry {
    pub name: String,
    pub sql: String,
    pub materialized: bool,
}

/// One consistent picture of the engine's objects.
pub(super) struct PgSnapshot {
    pub tables: Vec<Arc<TableDef>>,
    pub views: Vec<ViewEntry>,
    pub sequences: Vec<String>,
    pub indexes: Vec<IndexEntry>,
    pub enums: Vec<(String, Vec<String>)>,
    pub triggers: Vec<(String, String)>,
    oids: OidMap,
    declared: HashMap<(String, u32), DeclaredType>,
}

impl PgSnapshot {
    pub fn table_oid(&self, name: &str) -> Option<i32> {
        self.oids.by_key.get(&format!("r:{name}")).copied()
    }

    fn view_oid(&self, v: &ViewEntry) -> i32 {
        let kind = if v.materialized { 'm' } else { 'v' };
        self.oids.by_key[&format!("{kind}:{}", v.name)]
    }

    fn sequence_oid(&self, name: &str) -> i32 {
        self.oids.by_key[&format!("S:{name}")]
    }

    fn enum_oid(&self, name: &str) -> Option<i32> {
        self.oids.by_key.get(&format!("t:{name}")).copied()
    }

    fn enum_array_oid(&self, name: &str) -> Option<i32> {
        self.oids.by_key.get(&format!("_t:{name}")).copied()
    }

    fn constraint_oid(&self, table: &str, name: &str) -> i32 {
        self.oids.by_key[&format!("c:{table}:{name}")]
    }

    fn table(&self, name: &str) -> Option<&Arc<TableDef>> {
        self.tables.iter().find(|t| t.name == name)
    }

    /// Relation name for any relation OID (table, view, sequence, index).
    pub fn relation_name(&self, oid: i32) -> Option<String> {
        let key = self.oids.by_oid.get(&oid)?;
        let (kind, name) = key.split_once(':')?;
        matches!(kind, "r" | "v" | "m" | "S" | "i").then(|| name.to_string())
    }

    /// Any relation OID by name (tables, views, materialized views,
    /// sequences, indexes).
    pub fn relation_oid(&self, name: &str) -> Option<i32> {
        ["r", "v", "m", "S", "i"]
            .iter()
            .find_map(|k| self.oids.by_key.get(&format!("{k}:{name}")).copied())
    }

    fn column_type(&self, table: &TableDef, col: &crate::catalog::ColumnDef) -> ColType {
        let declared = self
            .declared
            .get(&(table.name.clone(), col.id))
            .filter(|d| declaration_fits(&col.data_type, &d.typname));
        match &col.data_type {
            DataType::Array(inner) => {
                if let Some(d) = declared
                    && let Some(builtin) = d.typname.strip_prefix('_').and_then(builtin_by_name)
                {
                    return ColType {
                        oid: builtin.4,
                        typname: d.typname.clone(),
                        typmod: d.typmod,
                        len: -1,
                        array_elem: Some(builtin.0),
                    };
                }
                let elem = self.scalar_type_oid(inner);
                ColType {
                    oid: self.array_oid_of_elem(inner, elem),
                    typname: format!("_{}", self.scalar_typname(inner)),
                    typmod: -1,
                    len: -1,
                    array_elem: Some(elem),
                }
            }
            DataType::UserDefined(name) => match self.enum_oid(name) {
                Some(oid) => ColType {
                    oid,
                    typname: name.clone(),
                    typmod: -1,
                    len: 4,
                    array_elem: None,
                },
                // Not a registered enum (a type this engine only names):
                // report text, which is what the engine stores.
                None => self.builtin_col("text", -1),
            },
            DataType::Vector(dim) => ColType {
                oid: VECTOR_OID,
                typname: "vector".into(),
                typmod: *dim as i32,
                len: -1,
                array_elem: None,
            },
            other => match declared {
                Some(d) => self.builtin_col(&d.typname, d.typmod),
                None => self.builtin_col(default_typname(other), -1),
            },
        }
    }

    fn builtin_col(&self, typname: &str, typmod: i32) -> ColType {
        let (oid, name, len, ..) = builtin_by_name(typname).expect("builtin type name");
        ColType {
            oid: *oid,
            typname: (*name).to_string(),
            typmod,
            len: *len,
            array_elem: None,
        }
    }

    fn scalar_typname(&self, dt: &DataType) -> String {
        match dt {
            DataType::UserDefined(n) if self.enum_oid(n).is_some() => n.clone(),
            other => default_typname(other).to_string(),
        }
    }

    fn scalar_type_oid(&self, dt: &DataType) -> i32 {
        match dt {
            DataType::UserDefined(n) => self.enum_oid(n).unwrap_or(25),
            DataType::Vector(_) => VECTOR_OID,
            other => builtin_by_name(default_typname(other)).map_or(25, |t| t.0),
        }
    }

    fn array_oid_of_elem(&self, inner: &DataType, elem_oid: i32) -> i32 {
        match inner {
            DataType::UserDefined(n) => self.enum_array_oid(n).unwrap_or(1009),
            DataType::Vector(_) => 1009,
            _ => builtin_by_oid(elem_oid).map_or(1009, |t| t.4),
        }
    }

    /// Declared or default default-expression text for one column, deparsed.
    fn default_text(&self, table: &TableDef, col: &crate::catalog::ColumnDef) -> Option<String> {
        match &col.generation {
            Some(crate::catalog::ColumnGeneration::Stored(expr)) => return Some(expr.clone()),
            Some(
                crate::catalog::ColumnGeneration::IdentityAlways
                | crate::catalog::ColumnGeneration::IdentityByDefault,
            ) => return None,
            None => {}
        }
        let raw = col.default_expr.as_ref()?;
        let ty = self.column_type(table, col);
        Some(deparse_default(raw, &ty, &col.data_type))
    }

    /// Sequence owned by a serial column, if the default is `nextval('seq')`.
    fn owned_sequence(&self, col: &crate::catalog::ColumnDef) -> Option<String> {
        let raw = col.default_expr.as_ref()?;
        let name = nextval_target(raw)?;
        self.sequences.iter().find(|s| **s == name).cloned()
    }
}

/// The sequence named by `nextval('seq')` / `nextval('seq'::regclass)`.
fn nextval_target(default_expr: &str) -> Option<String> {
    let lower = default_expr.trim();
    let rest = lower
        .strip_prefix("nextval(")
        .or_else(|| lower.strip_prefix("NEXTVAL("))?;
    let rest = rest.strip_suffix(')')?;
    let start = rest.find('\'')? + 1;
    let end = rest[start..].find('\'')? + start;
    let name = rest[start..end].trim_matches('"');
    Some(name.strip_prefix("public.").unwrap_or(name).to_string())
}

impl Executor {
    /// Build the snapshot every pg_catalog relation and deparse function
    /// reads. Cheap (linear in the number of objects); built per query.
    pub(super) async fn pg_snapshot(&self) -> PgSnapshot {
        let mut tables = self.catalog.list_tables().await;
        tables.sort_by(|a, b| a.name.cmp(&b.name));
        let mut real_indexes = self.catalog.get_all_indexes().await;
        real_indexes.sort_by(|a, b| a.name.cmp(&b.name));

        let mut views: Vec<ViewEntry> = self
            .views
            .read()
            .await
            .values()
            .map(|v| ViewEntry {
                name: v.name.clone(),
                sql: v.sql.clone(),
                materialized: false,
            })
            .collect();
        views.extend(
            self.materialized_views
                .read()
                .await
                .values()
                .map(|v| ViewEntry {
                    name: v.name.clone(),
                    sql: v.sql.clone(),
                    materialized: true,
                }),
        );
        views.sort_by(|a, b| a.name.cmp(&b.name));

        let mut sequences: Vec<String> = self.sequences.read().keys().cloned().collect();
        sequences.sort();

        let mut enums: Vec<(String, Vec<String>)> = Vec::new();
        let mut enum_names = self.catalog.list_enum_types().await;
        enum_names.sort();
        for n in enum_names {
            if let Some(labels) = self.catalog.get_enum_type(&n).await {
                enums.push((n, labels));
            }
        }

        let triggers: Vec<(String, String)> = self
            .triggers
            .read()
            .await
            .iter()
            .map(|t| (t.name.clone(), t.table_name.clone()))
            .collect();

        let mut oids = OidMap::default();
        for t in &tables {
            oids.assign(format!("r:{}", t.name));
        }
        for v in &views {
            oids.assign(format!(
                "{}:{}",
                if v.materialized { 'm' } else { 'v' },
                v.name
            ));
        }
        for s in &sequences {
            oids.assign(format!("S:{s}"));
        }

        // Index entries: every IndexDef, plus the index PostgreSQL would have
        // built for each PRIMARY KEY / UNIQUE constraint the engine enforces
        // without one.
        let mut indexes: Vec<IndexEntry> = Vec::new();
        let mut claimed: std::collections::HashSet<String> = std::collections::HashSet::new();
        for t in &tables {
            for con in &t.constraints {
                let (name, cols, primary) = match con {
                    TableConstraint::PrimaryKey { name, columns } => (name, columns, true),
                    TableConstraint::Unique { name, columns } => (name, columns, false),
                    _ => continue,
                };
                let cname = name.clone().unwrap_or_else(|| {
                    if primary {
                        format!("{}_pkey", t.name)
                    } else {
                        format!("{}_{}_key", t.name, cols.join("_"))
                    }
                });
                let backing = real_indexes
                    .iter()
                    .find(|i| {
                        i.table_name == t.name && i.name == cname && !claimed.contains(&i.name)
                    })
                    .cloned();
                match backing {
                    Some(ix) => {
                        claimed.insert(ix.name.clone());
                        indexes.push(IndexEntry {
                            oid: 0,
                            name: ix.name.clone(),
                            table: t.name.clone(),
                            columns: ix.columns.clone(),
                            unique: true,
                            kind: ix.index_type.clone(),
                            backs_constraint: Some(cname),
                            primary,
                            def: Some(ix),
                        });
                    }
                    None => indexes.push(IndexEntry {
                        oid: 0,
                        name: cname.clone(),
                        table: t.name.clone(),
                        columns: cols.clone(),
                        unique: true,
                        kind: IndexType::BTree,
                        backs_constraint: Some(cname),
                        primary,
                        def: None,
                    }),
                }
            }
        }
        for ix in &real_indexes {
            if claimed.contains(&ix.name) {
                continue;
            }
            indexes.push(IndexEntry {
                oid: 0,
                name: ix.name.clone(),
                table: ix.table_name.clone(),
                columns: ix.columns.clone(),
                unique: ix.unique,
                kind: ix.index_type.clone(),
                backs_constraint: None,
                primary: false,
                def: Some(ix.clone()),
            });
        }
        indexes.sort_by(|a, b| a.name.cmp(&b.name));
        for ix in &mut indexes {
            ix.oid = oids.assign(format!("i:{}", ix.name));
        }
        for (n, _) in &enums {
            oids.assign(format!("t:{n}"));
            oids.assign(format!("_t:{n}"));
        }
        for t in &tables {
            for (name, _) in constraint_names(t) {
                oids.assign(format!("c:{}:{name}", t.name));
            }
        }

        let declared = self
            .catalog
            .declared_types_snapshot()
            .into_iter()
            .map(|(t, id, d)| ((t, id), d))
            .collect();

        PgSnapshot {
            tables,
            views,
            sequences,
            indexes,
            enums,
            triggers,
            oids,
            declared,
        }
    }
}

fn constraint_names(t: &TableDef) -> Vec<(String, &TableConstraint)> {
    let mut out = Vec::new();
    for (i, c) in t.constraints.iter().enumerate() {
        let name = match c {
            TableConstraint::PrimaryKey { name, .. } => {
                name.clone().unwrap_or_else(|| format!("{}_pkey", t.name))
            }
            TableConstraint::Unique { name, columns } => name
                .clone()
                .unwrap_or_else(|| format!("{}_{}_key", t.name, columns.join("_"))),
            TableConstraint::Check { name, .. } => name
                .clone()
                .unwrap_or_else(|| format!("{}_check_{}", t.name, i + 1)),
            TableConstraint::ForeignKey { name, columns, .. } => name
                .clone()
                .unwrap_or_else(|| format!("{}_{}_fkey", t.name, columns.join("_"))),
        };
        out.push((name, c));
    }
    out
}

// ============================================================================
// Generated and identity columns: the seam
// ============================================================================

/// `(attidentity, attgenerated)` for one column: `("a" | "d" | "", "s" | "")`.
///
/// Serial columns retain ordinary defaults; identities and stored generated
/// columns report their actual generation metadata.
fn column_generation(
    _table: &TableDef,
    col: &crate::catalog::ColumnDef,
) -> (&'static str, &'static str) {
    (col.attidentity(), col.attgenerated())
}

// ============================================================================
// Column helpers
// ============================================================================

fn meta(label: &str, cols: &[(&str, DataType)]) -> Vec<ColMeta> {
    cols.iter()
        .map(|(n, dt)| ColMeta {
            table: Some(label.into()),
            name: (*n).into(),
            dtype: dt.clone(),
        })
        .collect()
}

fn text(s: &str) -> Value {
    Value::Text(s.to_string())
}

fn int_array(v: impl IntoIterator<Item = i32>) -> Value {
    Value::Array(v.into_iter().map(Value::Int32).collect())
}

/// A space-separated `int2vector`/`oidvector` spelling.
fn vector_text(v: impl IntoIterator<Item = i32>) -> Value {
    Value::Text(
        v.into_iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(" "),
    )
}

fn fk_code(a: &FkAction) -> &'static str {
    match a {
        FkAction::NoAction => "a",
        FkAction::Restrict => "r",
        FkAction::Cascade => "c",
        FkAction::SetNull => "n",
        FkAction::SetDefault => "d",
    }
}

fn relam_of(kind: &IndexType) -> i32 {
    match kind {
        IndexType::BTree => 403,
        IndexType::Hash => 405,
        IndexType::Gist => 783,
        IndexType::Gin => 2742,
        IndexType::Hnsw => AM_HNSW,
        IndexType::IvfFlat => AM_IVFFLAT,
        IndexType::Fts => AM_FTS,
        IndexType::Rtree => AM_RTREE,
    }
}

fn amname_of(kind: &IndexType) -> &'static str {
    match kind {
        IndexType::BTree => "btree",
        IndexType::Hash => "hash",
        IndexType::Gist => "gist",
        IndexType::Gin => "gin",
        IndexType::Hnsw => "hnsw",
        IndexType::IvfFlat => "ivfflat",
        IndexType::Fts => "fts",
        IndexType::Rtree => "rtree",
    }
}

/// Position (1-based) of a plain column name, 0 for an expression.
fn attnum_of(table: &TableDef, col: &str) -> i32 {
    table
        .columns
        .iter()
        .position(|c| c.name == col)
        .map_or(0, |p| p as i32 + 1)
}

fn is_plain_column(table: &TableDef, part: &str) -> bool {
    table.columns.iter().any(|c| c.name == part)
}

// ============================================================================
// Virtual relations
// ============================================================================

impl Executor {
    /// The catalog relations this module owns. `None` = not one of them.
    pub(super) async fn load_pg_catalog_table(
        &self,
        name: &str,
        label: &str,
    ) -> Result<Option<(Vec<ColMeta>, Vec<Row>)>, ExecError> {
        let bare = name.strip_prefix("pg_catalog.").unwrap_or(name);
        let out = match bare {
            "pg_class" => {
                let snap = self.pg_snapshot().await;
                let rls = {
                    let sec = self.security.read();
                    sec.rls
                        .enabled_tables()
                        .into_iter()
                        .collect::<std::collections::HashSet<String>>()
                };
                pg_class_rel(label, &snap, &rls)
            }
            "pg_attribute" => pg_attribute_rel(label, &self.pg_snapshot().await),
            "pg_attrdef" => pg_attrdef_rel(label, &self.pg_snapshot().await),
            "pg_constraint" => pg_constraint_rel(label, &self.pg_snapshot().await),
            "pg_index" => pg_index_rel(label, &self.pg_snapshot().await),
            "pg_depend" => pg_depend_rel(label, &self.pg_snapshot().await),
            "pg_type" => pg_type_rel(label, &self.pg_snapshot().await),
            "pg_enum" => pg_enum_rel(label, &self.pg_snapshot().await),
            "pg_opclass" => pg_opclass_rel(label),
            "pg_trigger" => pg_trigger_rel(label, &self.pg_snapshot().await),
            "pg_inherits" => (
                meta(
                    label,
                    &[
                        ("inhrelid", DataType::Int32),
                        ("inhparent", DataType::Int32),
                        ("inhseqno", DataType::Int32),
                        ("inhdetachpending", DataType::Bool),
                    ],
                ),
                Vec::new(),
            ),
            "pg_rewrite" => (
                meta(
                    label,
                    &[
                        ("oid", DataType::Int32),
                        ("rulename", DataType::Text),
                        ("ev_class", DataType::Int32),
                        ("ev_type", DataType::Text),
                        ("ev_enabled", DataType::Text),
                        ("is_instead", DataType::Bool),
                    ],
                ),
                Vec::new(),
            ),
            _ => return Ok(None),
        };
        Ok(Some(out))
    }
}

fn pg_class_rel(
    label: &str,
    snap: &PgSnapshot,
    rls: &std::collections::HashSet<String>,
) -> (Vec<ColMeta>, Vec<Row>) {
    let cols = meta(
        label,
        &[
            ("oid", DataType::Int32),
            ("relname", DataType::Text),
            ("relnamespace", DataType::Int32),
            ("relkind", DataType::Text),
            ("reltuples", DataType::Float64),
            ("relowner", DataType::Int32),
            ("relam", DataType::Int32),
            ("relchecks", DataType::Int32),
            ("relhasindex", DataType::Bool),
            ("relhasrules", DataType::Bool),
            ("relhastriggers", DataType::Bool),
            ("relrowsecurity", DataType::Bool),
            ("relforcerowsecurity", DataType::Bool),
            ("relispartition", DataType::Bool),
            ("reltablespace", DataType::Int32),
            ("reloftype", DataType::Int32),
            ("relpersistence", DataType::Text),
            ("relreplident", DataType::Text),
            ("reltoastrelid", DataType::Int32),
            ("relhassubclass", DataType::Bool),
            ("reloptions", DataType::Array(Box::new(DataType::Text))),
            ("relnatts", DataType::Int32),
            ("reltype", DataType::Int32),
            ("relfilenode", DataType::Int32),
        ],
    );
    let mut rows: Vec<Row> = Vec::new();
    for t in &snap.tables {
        let oid = snap.table_oid(&t.name).unwrap_or(0);
        let has_index = snap.indexes.iter().any(|ix| ix.table == t.name);
        let checks = t
            .constraints
            .iter()
            .filter(|c| matches!(c, TableConstraint::Check { .. }))
            .count();
        let has_trigger = snap.triggers.iter().any(|(_, tbl)| *tbl == t.name);
        rows.push(vec![
            Value::Int32(oid),
            text(&t.name),
            Value::Int32(PUBLIC_NSP),
            text("r"),
            Value::Float64(-1.0),
            Value::Int32(10),
            Value::Int32(2),
            Value::Int32(checks as i32),
            Value::Bool(has_index),
            Value::Bool(false),
            Value::Bool(has_trigger),
            Value::Bool(rls.contains(&t.name)),
            Value::Bool(false),
            Value::Bool(false),
            Value::Int32(0),
            Value::Int32(0),
            text("p"),
            text("d"),
            Value::Int32(0),
            Value::Bool(false),
            Value::Null,
            Value::Int32(t.columns.len() as i32),
            Value::Int32(0),
            Value::Int32(oid),
        ]);
    }
    for v in &snap.views {
        let oid = snap.view_oid(v);
        rows.push(vec![
            Value::Int32(oid),
            text(&v.name),
            Value::Int32(PUBLIC_NSP),
            text(if v.materialized { "m" } else { "v" }),
            Value::Float64(-1.0),
            Value::Int32(10),
            Value::Int32(if v.materialized { 2 } else { 0 }),
            Value::Int32(0),
            Value::Bool(false),
            Value::Bool(!v.materialized),
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(false),
            Value::Int32(0),
            Value::Int32(0),
            text("p"),
            text("n"),
            Value::Int32(0),
            Value::Bool(false),
            Value::Null,
            Value::Int32(0),
            Value::Int32(0),
            Value::Int32(oid),
        ]);
    }
    for s in &snap.sequences {
        let oid = snap.sequence_oid(s);
        rows.push(vec![
            Value::Int32(oid),
            text(s),
            Value::Int32(PUBLIC_NSP),
            text("S"),
            Value::Float64(1.0),
            Value::Int32(10),
            Value::Int32(0),
            Value::Int32(0),
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(false),
            Value::Int32(0),
            Value::Int32(0),
            text("p"),
            text("n"),
            Value::Int32(0),
            Value::Bool(false),
            Value::Null,
            Value::Int32(3),
            Value::Int32(0),
            Value::Int32(oid),
        ]);
    }
    for ix in &snap.indexes {
        rows.push(vec![
            Value::Int32(ix.oid),
            text(&ix.name),
            Value::Int32(PUBLIC_NSP),
            text("i"),
            Value::Float64(0.0),
            Value::Int32(10),
            Value::Int32(relam_of(&ix.kind)),
            Value::Int32(0),
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(false),
            Value::Int32(0),
            Value::Int32(0),
            text("p"),
            text("n"),
            Value::Int32(0),
            Value::Bool(false),
            index_reloptions(ix),
            Value::Int32(ix.columns.len() as i32),
            Value::Int32(0),
            Value::Int32(ix.oid),
        ]);
    }
    (cols, rows)
}

/// `reloptions` of an index: the integer access-method parameters an HNSW /
/// IVFFlat index was built with, as `key=value`. NULL when there are none.
fn index_reloptions(ix: &IndexEntry) -> Value {
    let Some(def) = &ix.def else {
        return Value::Null;
    };
    let mut opts: Vec<String> = Vec::new();
    for key in ["m", "ef_construction", "lists"] {
        if let Some(v) = def.options.get(key)
            && v.parse::<i64>().is_ok()
        {
            opts.push(format!("{key}={v}"));
        }
    }
    if opts.is_empty() {
        Value::Null
    } else {
        Value::Array(opts.into_iter().map(Value::Text).collect())
    }
}

fn pg_attribute_rel(label: &str, snap: &PgSnapshot) -> (Vec<ColMeta>, Vec<Row>) {
    let cols = meta(
        label,
        &[
            ("attrelid", DataType::Int32),
            ("attname", DataType::Text),
            ("atttypid", DataType::Int32),
            ("attnum", DataType::Int32),
            ("attnotnull", DataType::Bool),
            ("atttypmod", DataType::Int32),
            ("atthasdef", DataType::Bool),
            ("attcollation", DataType::Int32),
            ("attidentity", DataType::Text),
            ("attgenerated", DataType::Text),
            ("attisdropped", DataType::Bool),
            ("attndims", DataType::Int32),
            ("attlen", DataType::Int32),
        ],
    );
    let mut rows = Vec::new();
    for t in &snap.tables {
        let rel = snap.table_oid(&t.name).unwrap_or(0);
        for (i, c) in t.columns.iter().enumerate() {
            let ty = snap.column_type(t, c);
            let (identity, generated) = column_generation(t, c);
            rows.push(vec![
                Value::Int32(rel),
                text(&c.name),
                Value::Int32(ty.oid),
                Value::Int32(i as i32 + 1),
                Value::Bool(!c.nullable),
                Value::Int32(ty.typmod),
                Value::Bool(snap.default_text(t, c).is_some()),
                Value::Int32(0),
                text(identity),
                text(generated),
                Value::Bool(false),
                Value::Int32(i32::from(ty.array_elem.is_some())),
                Value::Int32(ty.len),
            ]);
        }
    }
    (cols, rows)
}

fn pg_attrdef_rel(label: &str, snap: &PgSnapshot) -> (Vec<ColMeta>, Vec<Row>) {
    let cols = meta(
        label,
        &[
            ("oid", DataType::Int32),
            ("adrelid", DataType::Int32),
            ("adnum", DataType::Int32),
            ("adbin", DataType::Text),
        ],
    );
    let mut oids = OidMap::default();
    let mut rows = Vec::new();
    for t in &snap.tables {
        let rel = snap.table_oid(&t.name).unwrap_or(0);
        for (i, c) in t.columns.iter().enumerate() {
            if let Some(expr) = snap.default_text(t, c) {
                rows.push(vec![
                    Value::Int32(oids.assign(format!("d:{}:{}", t.name, c.name))),
                    Value::Int32(rel),
                    Value::Int32(i as i32 + 1),
                    Value::Text(expr),
                ]);
            }
        }
    }
    (cols, rows)
}

fn pg_constraint_rel(label: &str, snap: &PgSnapshot) -> (Vec<ColMeta>, Vec<Row>) {
    let int_arr = DataType::Array(Box::new(DataType::Int32));
    let cols = meta(
        label,
        &[
            ("oid", DataType::Int32),
            ("conname", DataType::Text),
            ("connamespace", DataType::Int32),
            ("conrelid", DataType::Int32),
            ("contypid", DataType::Int32),
            ("conindid", DataType::Int32),
            ("confrelid", DataType::Int32),
            ("contype", DataType::Text),
            ("condeferrable", DataType::Bool),
            ("condeferred", DataType::Bool),
            ("convalidated", DataType::Bool),
            ("conkey", int_arr.clone()),
            ("confkey", int_arr),
            ("confupdtype", DataType::Text),
            ("confdeltype", DataType::Text),
            ("confmatchtype", DataType::Text),
            ("conbin", DataType::Text),
        ],
    );
    let mut rows = Vec::new();
    for t in &snap.tables {
        let rel = snap.table_oid(&t.name).unwrap_or(0);
        for (name, con) in constraint_names(t) {
            let oid = snap.constraint_oid(&t.name, &name);
            let backing = snap
                .indexes
                .iter()
                .find(|ix| ix.table == t.name && ix.backs_constraint.as_deref() == Some(&name))
                .map_or(0, |ix| ix.oid);
            let mut row = vec![
                Value::Int32(oid),
                text(&name),
                Value::Int32(PUBLIC_NSP),
                Value::Int32(rel),
                Value::Int32(0),
                Value::Int32(backing),
            ];
            match con {
                TableConstraint::PrimaryKey { columns, .. }
                | TableConstraint::Unique { columns, .. } => {
                    let kind = if matches!(con, TableConstraint::PrimaryKey { .. }) {
                        "p"
                    } else {
                        "u"
                    };
                    row.extend([
                        Value::Int32(0),
                        text(kind),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(true),
                        int_array(columns.iter().map(|c| attnum_of(t, c))),
                        Value::Null,
                        text(" "),
                        text(" "),
                        text(" "),
                        Value::Null,
                    ]);
                }
                TableConstraint::Check { expr, .. } => {
                    let referenced: Vec<i32> = t
                        .columns
                        .iter()
                        .enumerate()
                        .filter(|(_, c)| expr_mentions(expr, &c.name))
                        .map(|(i, _)| i as i32 + 1)
                        .collect();
                    row.extend([
                        Value::Int32(0),
                        text("c"),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(true),
                        int_array(referenced),
                        Value::Null,
                        text(" "),
                        text(" "),
                        text(" "),
                        Value::Text(deparse_check(expr, t, snap)),
                    ]);
                }
                TableConstraint::ForeignKey {
                    columns,
                    ref_table,
                    ref_columns,
                    on_delete,
                    on_update,
                    ..
                } => {
                    let target = snap.table(ref_table);
                    let ref_oid = snap.table_oid(ref_table).unwrap_or(0);
                    let ref_keys: Vec<i32> = ref_columns
                        .iter()
                        .map(|c| target.map_or(0, |rt| attnum_of(rt, c)))
                        .collect();
                    row.extend([
                        Value::Int32(ref_oid),
                        text("f"),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(true),
                        int_array(columns.iter().map(|c| attnum_of(t, c))),
                        int_array(ref_keys),
                        text(fk_code(on_update)),
                        text(fk_code(on_delete)),
                        text("s"),
                        Value::Null,
                    ]);
                }
            }
            rows.push(row);
        }
    }
    (cols, rows)
}

/// Whether a check expression mentions a column as an identifier.
fn expr_mentions(expr: &str, column: &str) -> bool {
    use sqlparser::ast::{Visit, Visitor};
    use std::ops::ControlFlow;
    struct ColumnVisitor<'a> {
        column: &'a str,
        found: bool,
    }
    impl Visitor for ColumnVisitor<'_> {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &ast::Expr) -> ControlFlow<()> {
            let identifier = match expr {
                ast::Expr::Identifier(id) => Some(id),
                ast::Expr::CompoundIdentifier(ids) => ids.last(),
                _ => None,
            };
            if let Some(id) = identifier {
                self.found |= if id.quote_style.is_some() {
                    id.value == self.column
                } else {
                    id.value.eq_ignore_ascii_case(self.column)
                };
            }
            ControlFlow::Continue(())
        }
    }
    let Ok(statements) = sqlparser::parser::Parser::parse_sql(
        &sqlparser::dialect::PostgreSqlDialect {},
        &format!("SELECT {expr}"),
    ) else {
        return false;
    };
    let mut visitor = ColumnVisitor {
        column,
        found: false,
    };
    let _ = statements.visit(&mut visitor);
    visitor.found
}

fn pg_index_rel(label: &str, snap: &PgSnapshot) -> (Vec<ColMeta>, Vec<Row>) {
    let cols = meta(
        label,
        &[
            ("indexrelid", DataType::Int32),
            ("indrelid", DataType::Int32),
            ("indnatts", DataType::Int32),
            ("indnkeyatts", DataType::Int32),
            ("indisunique", DataType::Bool),
            ("indisprimary", DataType::Bool),
            ("indkey", DataType::Text),
            ("indisclustered", DataType::Bool),
            ("indisvalid", DataType::Bool),
            ("indisready", DataType::Bool),
            ("indislive", DataType::Bool),
            ("indisreplident", DataType::Bool),
            ("indoption", DataType::Text),
            ("indcollation", DataType::Text),
            ("indclass", DataType::Text),
            ("indexprs", DataType::Text),
            ("indpred", DataType::Text),
            ("indnullsnotdistinct", DataType::Bool),
        ],
    );
    let mut rows = Vec::new();
    for ix in &snap.indexes {
        let Some(t) = snap.table(&ix.table) else {
            continue;
        };
        let keys: Vec<i32> = ix.columns.iter().map(|c| attnum_of(t, c)).collect();
        let exprs: Vec<String> = ix
            .columns
            .iter()
            .filter(|c| !is_plain_column(t, c))
            .cloned()
            .collect();
        let classes: Vec<i32> = ix
            .columns
            .iter()
            .map(|c| opclass_oid_for(t, c, ix, snap))
            .collect();
        rows.push(vec![
            Value::Int32(ix.oid),
            Value::Int32(snap.table_oid(&ix.table).unwrap_or(0)),
            Value::Int32(ix.columns.len() as i32),
            Value::Int32(ix.columns.len() as i32),
            Value::Bool(ix.unique),
            Value::Bool(ix.primary),
            vector_text(keys),
            Value::Bool(false),
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(false),
            vector_text(ix.columns.iter().map(|_| 0)),
            vector_text(ix.columns.iter().map(|_| 0)),
            vector_text(classes),
            if exprs.is_empty() {
                Value::Null
            } else {
                Value::Text(exprs.join(", "))
            },
            Value::Null,
            Value::Bool(false),
        ]);
    }
    (cols, rows)
}

// ----------------------------------------------------------------------------
// Operator classes
// ----------------------------------------------------------------------------

/// `(oid, method, name, default)` for the operator classes the catalog names.
const OPCLASSES: &[(i32, i32, &str, bool)] = &[
    (10001, 403, "bool_ops", true),
    (10002, 403, "bytea_ops", true),
    (10003, 403, "int8_ops", true),
    (10004, 403, "int2_ops", true),
    (10005, 403, "int4_ops", true),
    (10006, 403, "text_ops", true),
    (10007, 403, "float4_ops", true),
    (10008, 403, "float8_ops", true),
    (10009, 403, "varchar_ops", false),
    (10010, 403, "bpchar_ops", true),
    (10011, 403, "date_ops", true),
    (10012, 403, "timestamp_ops", true),
    (10013, 403, "timestamptz_ops", true),
    (10014, 403, "interval_ops", true),
    (10015, 403, "numeric_ops", true),
    (10016, 403, "uuid_ops", true),
    (10017, 403, "jsonb_ops", false),
    (10018, 403, "time_ops", true),
    (10019, 405, "text_ops", true),
    (10020, 405, "int4_ops", true),
    (10021, 405, "int8_ops", true),
    (10022, 405, "uuid_ops", true),
    (10023, 2742, "jsonb_ops", true),
    (10024, 2742, "array_ops", true),
    (10025, 783, "range_ops", true),
    (10026, AM_HNSW, "vector_l2_ops", true),
    (10027, AM_HNSW, "vector_cosine_ops", false),
    (10028, AM_HNSW, "vector_ip_ops", false),
    (10029, AM_IVFFLAT, "vector_l2_ops", true),
    (10030, AM_IVFFLAT, "vector_cosine_ops", false),
    (10031, AM_IVFFLAT, "vector_ip_ops", false),
    (10032, AM_FTS, "text_ops", true),
];

fn opclass_named(method: i32, name: &str) -> i32 {
    OPCLASSES
        .iter()
        .find(|o| o.1 == method && o.2 == name)
        .map_or(0, |o| o.0)
}

/// The operator class one index key part uses.
fn opclass_oid_for(t: &TableDef, part: &str, ix: &IndexEntry, snap: &PgSnapshot) -> i32 {
    let method = relam_of(&ix.kind);
    match ix.kind {
        IndexType::Hnsw | IndexType::IvfFlat => {
            let metric = ix
                .def
                .as_ref()
                .and_then(|d| {
                    d.options
                        .get("metric")
                        .or_else(|| d.options.get("distance"))
                })
                .map(|m| m.to_lowercase())
                .unwrap_or_default();
            let name = match metric.as_str() {
                "cosine" | "vector_cosine_ops" => "vector_cosine_ops",
                "ip" | "inner_product" | "vector_ip_ops" => "vector_ip_ops",
                _ => "vector_l2_ops",
            };
            opclass_named(method, name)
        }
        IndexType::Fts => opclass_named(method, "text_ops"),
        IndexType::Gin => {
            let is_array = t
                .columns
                .iter()
                .find(|c| c.name == part)
                .is_some_and(|c| matches!(c.data_type, DataType::Array(_)));
            opclass_named(method, if is_array { "array_ops" } else { "jsonb_ops" })
        }
        IndexType::Gist | IndexType::Rtree => opclass_named(783, "range_ops"),
        IndexType::BTree | IndexType::Hash => {
            let typname = t
                .columns
                .iter()
                .find(|c| c.name == part)
                .map(|c| snap.column_type(t, c).typname)
                .unwrap_or_else(|| "text".into());
            // varchar shares text's operator class in PostgreSQL.
            let typname = if typname == "varchar" {
                "text".into()
            } else {
                typname
            };
            let name = format!("{typname}_ops");
            let found = opclass_named(method, &name);
            if found != 0 {
                found
            } else {
                opclass_named(method, "text_ops")
            }
        }
    }
}

fn pg_opclass_rel(label: &str) -> (Vec<ColMeta>, Vec<Row>) {
    let cols = meta(
        label,
        &[
            ("oid", DataType::Int32),
            ("opcmethod", DataType::Int32),
            ("opcname", DataType::Text),
            ("opcnamespace", DataType::Int32),
            ("opcdefault", DataType::Bool),
        ],
    );
    let rows = OPCLASSES
        .iter()
        .map(|(oid, method, name, default)| {
            vec![
                Value::Int32(*oid),
                Value::Int32(*method),
                text(name),
                Value::Int32(11),
                Value::Bool(*default),
            ]
        })
        .collect();
    (cols, rows)
}

fn pg_depend_rel(label: &str, snap: &PgSnapshot) -> (Vec<ColMeta>, Vec<Row>) {
    let cols = meta(
        label,
        &[
            ("classid", DataType::Int32),
            ("objid", DataType::Int32),
            ("objsubid", DataType::Int32),
            ("refclassid", DataType::Int32),
            ("refobjid", DataType::Int32),
            ("refobjsubid", DataType::Int32),
            ("deptype", DataType::Text),
        ],
    );
    const PG_CLASS: i32 = 1259;
    let mut rows = Vec::new();
    // A serial column owns its sequence: auto ('a') dependency of the
    // sequence on the table column.
    for t in &snap.tables {
        for (i, c) in t.columns.iter().enumerate() {
            if let Some(seq) = snap.owned_sequence(c) {
                rows.push(vec![
                    Value::Int32(PG_CLASS),
                    Value::Int32(snap.sequence_oid(&seq)),
                    Value::Int32(0),
                    Value::Int32(PG_CLASS),
                    Value::Int32(snap.table_oid(&t.name).unwrap_or(0)),
                    Value::Int32(i as i32 + 1),
                    text(if c.attidentity().is_empty() { "a" } else { "i" }),
                ]);
            }
        }
    }
    (cols, rows)
}

fn pg_trigger_rel(label: &str, snap: &PgSnapshot) -> (Vec<ColMeta>, Vec<Row>) {
    let cols = meta(
        label,
        &[
            ("oid", DataType::Int32),
            ("tgrelid", DataType::Int32),
            ("tgname", DataType::Text),
            ("tgenabled", DataType::Text),
            ("tgisinternal", DataType::Bool),
        ],
    );
    let mut oids = OidMap::default();
    let rows = snap
        .triggers
        .iter()
        .map(|(name, table)| {
            vec![
                Value::Int32(oids.assign(format!("g:{table}:{name}"))),
                Value::Int32(snap.table_oid(table).unwrap_or(0)),
                text(name),
                text("O"),
                Value::Bool(false),
            ]
        })
        .collect();
    (cols, rows)
}

fn pg_enum_rel(label: &str, snap: &PgSnapshot) -> (Vec<ColMeta>, Vec<Row>) {
    let cols = meta(
        label,
        &[
            ("oid", DataType::Int32),
            ("enumtypid", DataType::Int32),
            ("enumsortorder", DataType::Float64),
            ("enumlabel", DataType::Text),
        ],
    );
    let mut oids = OidMap::default();
    let mut rows = Vec::new();
    for (name, labels) in &snap.enums {
        let type_oid = snap.enum_oid(name).unwrap_or(0);
        for (i, l) in labels.iter().enumerate() {
            rows.push(vec![
                Value::Int32(oids.assign(format!("e:{name}:{l}"))),
                Value::Int32(type_oid),
                Value::Float64(i as f64 + 1.0),
                text(l),
            ]);
        }
    }
    (cols, rows)
}

fn pg_type_rel(label: &str, snap: &PgSnapshot) -> (Vec<ColMeta>, Vec<Row>) {
    use super::helpers::pg_type_io_names;
    let cols = meta(
        label,
        &[
            ("oid", DataType::Int32),
            ("typname", DataType::Text),
            ("typnamespace", DataType::Int32),
            ("typlen", DataType::Int32),
            ("typtype", DataType::Text),
            ("typcategory", DataType::Text),
            ("typcollation", DataType::Int32),
            ("typnotnull", DataType::Bool),
            ("typbasetype", DataType::Int32),
            ("typtypmod", DataType::Int32),
            ("typrelid", DataType::Int32),
            ("typelem", DataType::Int32),
            ("typarray", DataType::Int32),
            ("typdelim", DataType::Text),
            ("typinput", DataType::Text),
            ("typoutput", DataType::Text),
            ("typreceive", DataType::Text),
            ("typsend", DataType::Text),
        ],
    );
    let row = |oid: i32,
               name: &str,
               nsp: i32,
               len: i32,
               ttype: &str,
               cat: &str,
               elem: i32,
               array: i32,
               io_name: &str| {
        let (tin, tout, trecv, tsend) = pg_type_io_names(io_name);
        vec![
            Value::Int32(oid),
            text(name),
            Value::Int32(nsp),
            Value::Int32(len),
            text(ttype),
            text(cat),
            Value::Int32(0),
            Value::Bool(false),
            Value::Int32(0),
            Value::Int32(-1),
            Value::Int32(0),
            Value::Int32(elem),
            Value::Int32(array),
            text(","),
            text(&tin),
            text(&tout),
            text(&trecv),
            text(&tsend),
        ]
    };
    let mut rows = Vec::new();
    for (oid, name, len, cat, array) in BUILTIN_TYPES {
        rows.push(row(*oid, name, 11, *len, "b", cat, 0, *array, name));
        rows.push(row(
            *array,
            &format!("_{name}"),
            11,
            -1,
            "b",
            "A",
            *oid,
            0,
            "array",
        ));
    }
    rows.push(row(
        VECTOR_OID, "vector", PUBLIC_NSP, -1, "b", "U", 0, 0, "vector",
    ));
    for (name, _) in &snap.enums {
        let oid = snap.enum_oid(name).unwrap_or(0);
        let array = snap.enum_array_oid(name).unwrap_or(0);
        rows.push(row(oid, name, PUBLIC_NSP, 4, "e", "E", 0, array, "enum"));
        rows.push(row(
            array,
            &format!("_{name}"),
            PUBLIC_NSP,
            -1,
            "b",
            "A",
            oid,
            0,
            "array",
        ));
    }
    (cols, rows)
}

// ============================================================================
// Deparse: expressions
// ============================================================================

fn parse_expr(sql: &str) -> Option<Expr> {
    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect).try_with_sql(sql).ok()?;
    let expr = parser.parse_expr().ok()?;
    parser
        .peek_token_ref()
        .token
        .eq(&sqlparser::tokenizer::Token::EOF)
        .then_some(expr)
}

/// Spelling of a type in a `::type` suffix, PostgreSQL style.
fn cast_type_name(ty: &ColType) -> String {
    format_type_text(ty.oid, ty.typmod, None)
}

/// Deparse a column default the way `pg_get_expr(adbin, adrelid)` prints it.
fn deparse_default(raw: &str, ty: &ColType, dt: &DataType) -> String {
    let Some(expr) = parse_expr(raw) else {
        return raw.to_string();
    };
    let ctx = DeparseCtx {
        table: None,
        snap: None,
        literal_type: Some((ty.clone(), dt.clone())),
    };
    ctx.top(&expr).unwrap_or_else(|| raw.to_string())
}

/// Deparse a CHECK expression the way `pg_get_expr(conbin, conrelid)` does.
fn deparse_check(raw: &str, table: &TableDef, snap: &PgSnapshot) -> String {
    let Some(expr) = parse_expr(raw) else {
        return raw.to_string();
    };
    let ctx = DeparseCtx {
        table: Some(table),
        snap: Some(snap),
        literal_type: None,
    };
    ctx.top(&expr).unwrap_or_else(|| raw.to_string())
}

struct DeparseCtx<'a> {
    table: Option<&'a TableDef>,
    snap: Option<&'a PgSnapshot>,
    /// For defaults: the column type that types a bare literal.
    literal_type: Option<(ColType, DataType)>,
}

/// The type a sub-expression evaluates to, as far as the deparser needs it to
/// decide where PostgreSQL prints a cast.
#[derive(Clone, Debug, PartialEq)]
enum Ty {
    Text,
    Varchar,
    Bpchar,
    Other,
}

impl DeparseCtx<'_> {
    fn column_ty(&self, name: &str) -> Option<(Ty, ColType)> {
        let table = self.table?;
        let snap = self.snap?;
        let col = table.columns.iter().find(|c| c.name == name)?;
        let ct = snap.column_type(table, col);
        let ty = match ct.typname.as_str() {
            "text" => Ty::Text,
            "varchar" => Ty::Varchar,
            "bpchar" => Ty::Bpchar,
            _ => Ty::Other,
        };
        Some((ty, ct))
    }

    /// Top level of a stored expression. A CHECK is always shown wrapped in
    /// one pair of parentheses by `pg_get_constraintdef`, but
    /// `pg_get_expr(conbin)` prints the expression as-is.
    fn top(&self, e: &Expr) -> Option<String> {
        match self.literal_type.clone() {
            Some((ty, dt)) => self.default_expr(e, &ty, &dt),
            None => self.expr(e),
        }
    }

    fn default_expr(&self, e: &Expr, ty: &ColType, dt: &DataType) -> Option<String> {
        match e {
            Expr::Nested(inner) => self.default_expr(inner, ty, dt),
            Expr::Value(v) => self.default_value(&v.value, ty, dt, false),
            Expr::UnaryOp {
                op: ast::UnaryOperator::Minus,
                expr,
            } => match expr.as_ref() {
                Expr::Value(v) => self.default_value(&v.value, ty, dt, true),
                _ => None,
            },
            Expr::Cast {
                expr, data_type, ..
            } => {
                let inner = match expr.as_ref() {
                    Expr::Value(v) => match &v.value {
                        ast::Value::SingleQuotedString(s) => quote_literal(s),
                        _ => return None,
                    },
                    _ => return None,
                };
                Some(format!("{inner}::{}", type_spelling(data_type)?))
            }
            Expr::Function(f) => self.function(f, Some((ty, dt))),
            _ => self.expr(e),
        }
    }

    fn default_value(
        &self,
        v: &ast::Value,
        ty: &ColType,
        dt: &DataType,
        negative: bool,
    ) -> Option<String> {
        match v {
            ast::Value::Number(n, _) => {
                let n = if negative { format!("-{n}") } else { n.clone() };
                if negative || is_float_literal(&n) && !matches!(dt, DataType::Numeric) {
                    // PostgreSQL stores a negative or fractional constant as a
                    // typed string literal.
                    Some(format!("'{n}'::{}", cast_type_name(ty)))
                } else {
                    Some(n)
                }
            }
            ast::Value::SingleQuotedString(s) => {
                Some(format!("{}::{}", quote_literal(s), cast_type_name(ty)))
            }
            ast::Value::Boolean(b) => Some(if *b { "true" } else { "false" }.into()),
            ast::Value::Null => Some("NULL::".to_string() + &cast_type_name(ty)),
            _ => None,
        }
    }

    fn function(&self, f: &ast::Function, typed: Option<(&ColType, &DataType)>) -> Option<String> {
        let name = f.name.to_string();
        let lower = name.to_lowercase();
        let args = match &f.args {
            ast::FunctionArguments::List(l) => l
                .args
                .iter()
                .map(|a| match a {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e.clone()),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?,
            ast::FunctionArguments::None => Vec::new(),
            _ => return None,
        };
        if lower == "nextval" {
            return match args.first()? {
                Expr::Value(v) => match &v.value {
                    ast::Value::SingleQuotedString(s) => {
                        Some(format!("nextval({}::regclass)", quote_literal(s)))
                    }
                    _ => None,
                },
                Expr::Cast {
                    expr, data_type, ..
                } if type_spelling(data_type).as_deref() == Some("regclass") => match expr.as_ref()
                {
                    Expr::Value(v) => match &v.value {
                        ast::Value::SingleQuotedString(s) => {
                            Some(format!("nextval({}::regclass)", quote_literal(s)))
                        }
                        _ => None,
                    },
                    _ => None,
                },
                _ => None,
            };
        }
        let rendered = args
            .iter()
            .map(|a| match typed {
                Some((ty, dt)) if matches!(a, Expr::Value(_)) => self.default_expr(a, ty, dt),
                _ => self.expr(a),
            })
            .collect::<Option<Vec<_>>>()?;
        match lower.as_str() {
            "current_timestamp" | "current_date" | "current_time" | "localtimestamp"
            | "localtime"
                if rendered.is_empty() =>
            {
                Some(lower.to_uppercase())
            }
            _ => Some(format!("{lower}({})", rendered.join(", "))),
        }
    }

    /// Render `e` as PostgreSQL prints a boolean/scalar expression tree:
    /// every binary operation in its own parentheses.
    fn expr(&self, e: &Expr) -> Option<String> {
        match e {
            Expr::Nested(inner) => self.expr(inner),
            Expr::Identifier(id) => Some(quote_ident(&id.value)),
            Expr::CompoundIdentifier(parts) => Some(
                parts
                    .iter()
                    .map(|p| quote_ident(&p.value))
                    .collect::<Vec<_>>()
                    .join("."),
            ),
            Expr::Value(v) => match &v.value {
                ast::Value::Number(n, _) => Some(n.clone()),
                ast::Value::SingleQuotedString(s) => Some(format!("{}::text", quote_literal(s))),
                ast::Value::Boolean(b) => Some(if *b { "true" } else { "false" }.into()),
                ast::Value::Null => Some("NULL".into()),
                _ => None,
            },
            Expr::UnaryOp { op, expr } => match op {
                ast::UnaryOperator::Not => Some(format!("(NOT {})", self.expr(expr)?)),
                ast::UnaryOperator::Minus => Some(format!("(- {})", self.expr(expr)?)),
                ast::UnaryOperator::Plus => self.expr(expr),
                _ => None,
            },
            Expr::IsNull(inner) => Some(format!("({} IS NULL)", self.operand(inner)?)),
            Expr::IsNotNull(inner) => Some(format!("({} IS NOT NULL)", self.operand(inner)?)),
            Expr::IsTrue(inner) => Some(format!("({} IS TRUE)", self.operand(inner)?)),
            Expr::IsFalse(inner) => Some(format!("({} IS FALSE)", self.operand(inner)?)),
            Expr::BinaryOp { left, op, right } => self.binary(left, op, right),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let (l, lo, hi) = (self.expr(expr)?, self.expr(low)?, self.expr(high)?);
                Some(if *negated {
                    format!("(({l} < {lo}) OR ({l} > {hi}))")
                } else {
                    format!("(({l} >= {lo}) AND ({l} <= {hi}))")
                })
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let (col_ty, _) = self.expr_ty(expr);
                let lhs = self.operand_typed(expr, &col_ty)?;
                let items = list
                    .iter()
                    .map(|i| self.literal_for(i, &col_ty))
                    .collect::<Option<Vec<_>>>()?;
                Some(if *negated {
                    format!("({lhs} <> ALL (ARRAY[{}]))", items.join(", "))
                } else {
                    format!("({lhs} = ANY (ARRAY[{}]))", items.join(", "))
                })
            }
            Expr::Like {
                negated,
                expr,
                pattern,
                escape_char: None,
                ..
            } => {
                let (col_ty, _) = self.expr_ty(expr);
                let lhs = self.operand_typed(expr, &col_ty)?;
                let pat = self.literal_for(pattern, &col_ty)?;
                Some(format!(
                    "({lhs} {} {pat})",
                    if *negated { "!~~" } else { "~~" }
                ))
            }
            Expr::ILike {
                negated,
                expr,
                pattern,
                escape_char: None,
                ..
            } => {
                let (col_ty, _) = self.expr_ty(expr);
                let lhs = self.operand_typed(expr, &col_ty)?;
                let pat = self.literal_for(pattern, &col_ty)?;
                Some(format!(
                    "({lhs} {} {pat})",
                    if *negated { "!~~*" } else { "~~*" }
                ))
            }
            Expr::Cast {
                expr, data_type, ..
            } => Some(format!(
                "({})::{}",
                self.expr(expr)?,
                type_spelling(data_type)?
            )),
            Expr::Function(f) => self.function(f, None),
            _ => None,
        }
    }

    /// An operand of a comparison: identifiers stay bare, everything else is
    /// already parenthesised by `expr`.
    fn operand(&self, e: &Expr) -> Option<String> {
        self.expr(e)
    }

    fn expr_ty(&self, e: &Expr) -> (Ty, Option<ColType>) {
        match e {
            Expr::Nested(inner) => self.expr_ty(inner),
            Expr::Identifier(id) => match self.column_ty(&id.value) {
                Some((t, c)) => (t, Some(c)),
                None => (Ty::Other, None),
            },
            _ => (Ty::Other, None),
        }
    }

    /// A column reference compared against text: varchar/bpchar columns are
    /// implicitly cast to text.
    fn operand_typed(&self, e: &Expr, ty: &Ty) -> Option<String> {
        let s = self.expr(e)?;
        Some(match ty {
            Ty::Varchar | Ty::Bpchar => format!("({s})::text"),
            _ => s,
        })
    }

    /// A string literal compared against a column of type `ty`.
    fn literal_for(&self, e: &Expr, ty: &Ty) -> Option<String> {
        match e {
            Expr::Value(v) => match &v.value {
                ast::Value::SingleQuotedString(s) => Some(format!("{}::text", quote_literal(s))),
                ast::Value::Number(n, _) => Some(n.clone()),
                ast::Value::Boolean(b) => Some(if *b { "true" } else { "false" }.into()),
                _ => None,
            },
            other => {
                let _ = ty;
                self.expr(other)
            }
        }
    }

    fn binary(&self, l: &Expr, op: &ast::BinaryOperator, r: &Expr) -> Option<String> {
        use ast::BinaryOperator as B;
        if matches!(op, B::And | B::Or) {
            // PostgreSQL flattens a chain of the same boolean operator.
            let word = if matches!(op, B::And) { "AND" } else { "OR" };
            let mut parts = Vec::new();
            self.flatten(l, op, &mut parts)?;
            self.flatten(r, op, &mut parts)?;
            return Some(format!("({})", parts.join(&format!(" {word} "))));
        }
        let sym = match op {
            B::Eq => "=",
            B::NotEq => "<>",
            B::Lt => "<",
            B::LtEq => "<=",
            B::Gt => ">",
            B::GtEq => ">=",
            B::Plus => "+",
            B::Minus => "-",
            B::Multiply => "*",
            B::Divide => "/",
            B::Modulo => "%",
            B::StringConcat => "||",
            _ => return None,
        };
        let comparison = matches!(op, B::Eq | B::NotEq | B::Lt | B::LtEq | B::Gt | B::GtEq);
        let (lt, _) = self.expr_ty(l);
        let (rt, _) = self.expr_ty(r);
        let text_side = if comparison || matches!(op, B::StringConcat) {
            if matches!(lt, Ty::Text | Ty::Varchar | Ty::Bpchar)
                || matches!(rt, Ty::Text | Ty::Varchar | Ty::Bpchar)
            {
                Ty::Text
            } else {
                Ty::Other
            }
        } else {
            Ty::Other
        };
        let render = |e: &Expr, ty: &Ty| -> Option<String> {
            if text_side == Ty::Text {
                match (e, ty) {
                    (Expr::Value(_), _) => self.literal_for(e, &Ty::Text),
                    (_, Ty::Varchar | Ty::Bpchar) => self.operand_typed(e, ty),
                    _ => self.expr(e),
                }
            } else {
                self.expr(e)
            }
        };
        Some(format!("({} {sym} {})", render(l, &lt)?, render(r, &rt)?))
    }

    fn flatten(&self, e: &Expr, op: &ast::BinaryOperator, out: &mut Vec<String>) -> Option<()> {
        match e {
            Expr::Nested(inner) => self.flatten(inner, op, out),
            Expr::BinaryOp { left, op: o, right } if o == op => {
                self.flatten(left, op, out)?;
                self.flatten(right, op, out)
            }
            other => {
                out.push(self.expr(other)?);
                Some(())
            }
        }
    }
}

fn is_float_literal(n: &str) -> bool {
    n.contains('.') || n.contains('e') || n.contains('E')
}

/// `quote_literal`: single quotes doubled.
fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// PostgreSQL's identifier quoting.
pub(super) fn quote_ident(s: &str) -> String {
    let plain = !s.is_empty()
        && s.chars()
            .next()
            .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && !is_reserved_word(s);
    if plain {
        s.to_string()
    } else {
        format!("\"{}\"", s.replace('"', "\"\""))
    }
}

fn is_reserved_word(s: &str) -> bool {
    const RESERVED: &[&str] = &[
        "all",
        "analyse",
        "analyze",
        "and",
        "any",
        "array",
        "as",
        "asc",
        "asymmetric",
        "both",
        "case",
        "cast",
        "check",
        "collate",
        "column",
        "constraint",
        "create",
        "current_catalog",
        "current_date",
        "current_role",
        "current_time",
        "current_timestamp",
        "current_user",
        "default",
        "deferrable",
        "desc",
        "distinct",
        "do",
        "else",
        "end",
        "except",
        "false",
        "fetch",
        "for",
        "foreign",
        "from",
        "grant",
        "group",
        "having",
        "in",
        "initially",
        "intersect",
        "into",
        "lateral",
        "leading",
        "limit",
        "localtime",
        "localtimestamp",
        "not",
        "null",
        "offset",
        "on",
        "only",
        "or",
        "order",
        "placing",
        "primary",
        "references",
        "returning",
        "select",
        "session_user",
        "some",
        "symmetric",
        "table",
        "then",
        "to",
        "trailing",
        "true",
        "union",
        "unique",
        "user",
        "using",
        "variadic",
        "when",
        "where",
        "window",
        "with",
    ];
    RESERVED.contains(&s)
}

/// The spelling PostgreSQL uses after `::` for a parsed SQL type.
fn type_spelling(dt: &ast::DataType) -> Option<String> {
    use ast::DataType as D;
    Some(match dt {
        D::Text => "text".into(),
        D::Int(_) | D::Integer(_) | D::Int4(_) => "integer".into(),
        D::BigInt(_) | D::Int8(_) => "bigint".into(),
        D::SmallInt(_) | D::Int2(_) => "smallint".into(),
        D::Boolean | D::Bool => "boolean".into(),
        D::Real | D::Float4 => "real".into(),
        D::Double(_) | D::DoublePrecision | D::Float8 => "double precision".into(),
        D::JSONB => "jsonb".into(),
        D::JSON => "json".into(),
        D::Date => "date".into(),
        D::Uuid => "uuid".into(),
        D::Bytea => "bytea".into(),
        D::Regclass => "regclass".into(),
        D::Numeric(_) | D::Decimal(_) => {
            let d = declared_type_of(dt)?;
            format_type_text(1700, d.typmod, None)
        }
        D::Varchar(_) | D::CharVarying(_) | D::CharacterVarying(_) => {
            let d = declared_type_of(dt)?;
            format_type_text(1043, d.typmod, None)
        }
        D::Timestamp(_, tz) => {
            if matches!(tz, ast::TimezoneInfo::WithTimeZone | ast::TimezoneInfo::Tz) {
                "timestamp with time zone".into()
            } else {
                "timestamp without time zone".into()
            }
        }
        D::Custom(name, _) => name.0.last()?.as_ident()?.value.to_lowercase(),
        _ => return None,
    })
}

// ============================================================================
// Deparse: types
// ============================================================================

/// `format_type(oid, typmod)`. `names` resolves user-defined type OIDs.
pub(super) fn format_type_text(oid: i32, typmod: i32, names: Option<&PgSnapshot>) -> String {
    if let Some(t) = builtin_by_array_oid(oid) {
        return format!("{}[]", format_type_text(t.0, typmod, names));
    }
    match oid {
        16 => "boolean".into(),
        17 => "bytea".into(),
        19 => "name".into(),
        20 => "bigint".into(),
        21 => "smallint".into(),
        23 => "integer".into(),
        25 => "text".into(),
        26 => "oid".into(),
        114 => "json".into(),
        700 => "real".into(),
        701 => "double precision".into(),
        1042 if typmod >= 5 => format!("character({})", typmod - 4),
        1042 => "character".into(),
        1043 if typmod >= 5 => format!("character varying({})", typmod - 4),
        1043 => "character varying".into(),
        1082 => "date".into(),
        1083 => "time without time zone".into(),
        1114 if typmod >= 0 => format!("timestamp({typmod}) without time zone"),
        1114 => "timestamp without time zone".into(),
        1184 if typmod >= 0 => format!("timestamp({typmod}) with time zone"),
        1184 => "timestamp with time zone".into(),
        1186 => "interval".into(),
        1700 if typmod >= 4 => {
            let m = typmod - 4;
            format!("numeric({},{})", (m >> 16) & 0xffff, m & 0xffff)
        }
        1700 => "numeric".into(),
        2950 => "uuid".into(),
        3802 => "jsonb".into(),
        VECTOR_OID if typmod > 0 => format!("vector({typmod})"),
        VECTOR_OID => "vector".into(),
        other => match names.and_then(|s| s.oids.by_oid.get(&other)) {
            Some(key) => match key.split_once(':') {
                Some(("t", n)) => quote_ident(n),
                Some(("_t", n)) => format!("{}[]", quote_ident(n)),
                _ => "-".into(),
            },
            None => "-".into(),
        },
    }
}

// ============================================================================
// Deparse: constraints, indexes, views
// ============================================================================

fn constraint_def(t: &TableDef, con: &TableConstraint, snap: &PgSnapshot) -> String {
    let list = |cols: &[String]| {
        cols.iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ")
    };
    match con {
        TableConstraint::PrimaryKey { columns, .. } => format!("PRIMARY KEY ({})", list(columns)),
        TableConstraint::Unique { columns, .. } => format!("UNIQUE ({})", list(columns)),
        TableConstraint::Check { expr, .. } => {
            let d = deparse_check(expr, t, snap);
            if d.starts_with('(') {
                format!("CHECK ({d})")
            } else {
                format!("CHECK (({d}))")
            }
        }
        TableConstraint::ForeignKey {
            columns,
            ref_table,
            ref_columns,
            on_delete,
            on_update,
            ..
        } => {
            let mut out = format!(
                "FOREIGN KEY ({}) REFERENCES {}({})",
                list(columns),
                quote_ident(ref_table),
                list(ref_columns)
            );
            let action = |a: &FkAction| match a {
                FkAction::NoAction => None,
                FkAction::Restrict => Some("RESTRICT"),
                FkAction::Cascade => Some("CASCADE"),
                FkAction::SetNull => Some("SET NULL"),
                FkAction::SetDefault => Some("SET DEFAULT"),
            };
            if let Some(a) = action(on_update) {
                out.push_str(&format!(" ON UPDATE {a}"));
            }
            if let Some(a) = action(on_delete) {
                out.push_str(&format!(" ON DELETE {a}"));
            }
            out
        }
    }
}

fn index_def(ix: &IndexEntry, snap: &PgSnapshot) -> String {
    let parts: Vec<String> = ix
        .columns
        .iter()
        .map(|c| {
            let plain = snap.table(&ix.table).is_some_and(|t| is_plain_column(t, c));
            if plain { quote_ident(c) } else { c.clone() }
        })
        .collect();
    let mut out = format!(
        "CREATE {}INDEX {} ON public.{} USING {} ({})",
        if ix.unique { "UNIQUE " } else { "" },
        quote_ident(&ix.name),
        quote_ident(&ix.table),
        amname_of(&ix.kind),
        parts.join(", ")
    );
    if let Value::Array(opts) = index_reloptions(ix) {
        let list: Vec<String> = opts.iter().map(|o| o.to_string()).collect();
        out.push_str(&format!(" WITH ({})", list.join(", ")));
    }
    out
}

impl Executor {
    /// `pg_get_indexdef(oid)`; NULL for an OID that is not an index.
    pub(super) fn pg_get_indexdef_by_oid(&self, snap: &PgSnapshot, oid: i32) -> Value {
        snap.indexes
            .iter()
            .find(|ix| ix.oid == oid)
            .map_or(Value::Null, |ix| Value::Text(index_def(ix, snap)))
    }

    /// `pg_get_constraintdef(oid)`; NULL for an OID that is not a constraint.
    pub(super) fn pg_get_constraintdef_by_oid(&self, snap: &PgSnapshot, oid: i32) -> Value {
        for t in &snap.tables {
            for (name, con) in constraint_names(t) {
                if snap.constraint_oid(&t.name, &name) == oid {
                    return Value::Text(constraint_def(t, con, snap));
                }
            }
        }
        Value::Null
    }

    /// `pg_get_viewdef(oid)`: the stored SELECT, re-rendered by the parser.
    /// This is not PostgreSQL's multi-line deparse; it is a valid statement
    /// that means the same thing.
    pub(super) fn pg_get_viewdef_by_oid(&self, snap: &PgSnapshot, oid: i32) -> Value {
        snap.views
            .iter()
            .find(|v| snap.view_oid(v) == oid)
            .map_or(Value::Null, |v| Value::Text(render_view_sql(&v.sql)))
    }

    /// `format_type(oid, typmod)` with user-defined types resolved.
    pub(super) fn format_type_with_snapshot(
        &self,
        snap: &PgSnapshot,
        oid: i32,
        typmod: i32,
    ) -> String {
        format_type_text(oid, typmod, Some(snap))
    }
}

fn render_view_sql(sql: &str) -> String {
    let dialect = PostgreSqlDialect {};
    match Parser::parse_sql(&dialect, sql) {
        Ok(stmts) if stmts.len() == 1 => format!("{};", stmts[0]),
        _ => sql.to_string(),
    }
}

/// The `information_schema.columns` facets that depend on the column's type.
pub(super) struct InfoSchemaType {
    pub data_type: String,
    pub udt_name: String,
    pub udt_schema: &'static str,
    pub char_len: Value,
    pub num_precision: Value,
    pub num_precision_radix: Value,
    pub num_scale: Value,
    pub datetime_precision: Value,
    pub default: Value,
}

impl PgSnapshot {
    /// The facts `information_schema.columns` reports for one column, in
    /// PostgreSQL's spelling (`character varying`, `integer`, `ARRAY`, ...).
    pub(super) fn info_schema_type(
        &self,
        table: &TableDef,
        col: &crate::catalog::ColumnDef,
    ) -> InfoSchemaType {
        let ty = self.column_type(table, col);
        let int = |n: i32| Value::Int32(n);
        let mut out = InfoSchemaType {
            data_type: String::new(),
            udt_name: ty.typname.clone(),
            udt_schema: "pg_catalog",
            char_len: Value::Null,
            num_precision: Value::Null,
            num_precision_radix: Value::Null,
            num_scale: Value::Null,
            datetime_precision: Value::Null,
            default: if col.generation.is_some() {
                Value::Null
            } else {
                self.default_text(table, col)
                    .map_or(Value::Null, Value::Text)
            },
        };
        if ty.array_elem.is_some() {
            out.data_type = "ARRAY".into();
            return out;
        }
        match ty.typname.as_str() {
            "int2" => {
                out.num_precision = int(16);
                out.num_precision_radix = int(2);
                out.num_scale = int(0);
            }
            "int4" => {
                out.num_precision = int(32);
                out.num_precision_radix = int(2);
                out.num_scale = int(0);
            }
            "int8" => {
                out.num_precision = int(64);
                out.num_precision_radix = int(2);
                out.num_scale = int(0);
            }
            "float4" => {
                out.num_precision = int(24);
                out.num_precision_radix = int(2);
            }
            "float8" => {
                out.num_precision = int(53);
                out.num_precision_radix = int(2);
            }
            "numeric" => {
                out.num_precision_radix = int(10);
                if ty.typmod >= 4 {
                    let m = ty.typmod - 4;
                    out.num_precision = int((m >> 16) & 0xffff);
                    out.num_scale = int(m & 0xffff);
                }
            }
            "varchar" | "bpchar" if ty.typmod >= 5 => out.char_len = int(ty.typmod - 4),
            "timestamp" | "timestamptz" => {
                out.datetime_precision = int(if ty.typmod >= 0 { ty.typmod } else { 6 });
            }
            "date" => out.datetime_precision = int(0),
            "interval" | "time" => out.datetime_precision = int(6),
            _ => {}
        }
        if ty.oid == VECTOR_OID || self.enum_oid(&ty.typname).is_some_and(|o| o == ty.oid) {
            out.data_type = "USER-DEFINED".into();
            out.udt_schema = "public";
        } else {
            out.data_type = format_type_text(ty.oid, -1, None);
            // information_schema spells the type without its modifier and
            // without the `(p)` of a timestamp.
        }
        out
    }
}

impl Executor {
    /// Replace relation OIDs among `args` with relation names, so the
    /// privilege functions can accept `has_table_privilege(c.oid, 'SELECT')`.
    pub(super) fn relation_oids_to_names(&self, mut args: Vec<Value>) -> Vec<Value> {
        let is_oid = |v: &Value| match v {
            Value::Int32(n) => i64::from(*n) >= FIRST_USER_OID,
            Value::Int64(n) => *n >= FIRST_USER_OID && *n < FIRST_USER_OID + OID_SPAN as i64,
            _ => false,
        };
        if !args.iter().any(is_oid) {
            return args;
        }
        let snap = super::session::sync_block_on(self.pg_snapshot());
        for a in &mut args {
            if is_oid(a) {
                let oid = match a {
                    Value::Int32(n) => *n,
                    Value::Int64(n) => *n as i32,
                    _ => continue,
                };
                if let Some(name) = snap.relation_name(oid) {
                    *a = Value::Text(name);
                }
            }
        }
        args
    }

    /// `pg_get_serial_sequence('table', 'column')`.
    pub(super) fn pg_get_serial_sequence(&self, table: &str, column: &str) -> Value {
        let snap = super::session::sync_block_on(self.pg_snapshot());
        let table = table.trim_matches('"');
        let table = table
            .strip_prefix("public.")
            .unwrap_or(table)
            .trim_matches('"');
        let Some(t) = snap.table(table) else {
            return Value::Null;
        };
        t.columns
            .iter()
            .find(|c| c.name == column)
            .and_then(|c| snap.owned_sequence(c))
            .map_or(Value::Null, |s| {
                Value::Text(format!("public.{}", quote_ident(&s)))
            })
    }

    /// `'name'::regclass`: any relation by name, `None` when it does not exist.
    pub(super) fn regclass_by_name(&self, name: &str) -> Option<i32> {
        let bare = name.replace('"', "");
        let bare = bare.strip_prefix("public.").unwrap_or(&bare);
        let snap = super::session::sync_block_on(self.pg_snapshot());
        snap.relation_oid(bare)
    }

    /// `oid::regclass` rendered as the relation name.
    pub(super) fn regclass_by_oid(&self, oid: i32) -> Option<String> {
        let snap = super::session::sync_block_on(self.pg_snapshot());
        snap.relation_name(oid).map(|n| quote_ident(&n))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ty(typname: &str, typmod: i32) -> ColType {
        let (oid, name, len, ..) = builtin_by_name(typname).unwrap();
        ColType {
            oid: *oid,
            typname: (*name).into(),
            typmod,
            len: *len,
            array_elem: None,
        }
    }

    #[test]
    fn oids_are_name_derived_and_unique() {
        let mut a = OidMap::default();
        let x = a.assign("r:users".into());
        let mut b = OidMap::default();
        // Unrelated objects assigned first must not move `users`.
        b.assign("r:zzz".into());
        b.assign("v:aaa".into());
        assert_eq!(b.assign("r:users".into()), x);
        assert!(i64::from(x) >= FIRST_USER_OID);
    }

    #[test]
    fn declared_types_cover_the_lossy_cases() {
        let dialect = PostgreSqlDialect {};
        let decl = |sql: &str| {
            let stmts = Parser::parse_sql(&dialect, &format!("CREATE TABLE t (c {sql})")).unwrap();
            let ast::Statement::CreateTable(ct) = &stmts[0] else {
                panic!()
            };
            declared_type_of(&ct.columns[0].data_type)
        };
        assert_eq!(decl("varchar(20)").unwrap().typmod, 24);
        assert_eq!(decl("numeric(10,2)").unwrap().typmod, 655_366);
        assert_eq!(decl("smallint").unwrap().typname, "int2");
        assert_eq!(decl("real").unwrap().typname, "float4");
        assert_eq!(decl("json").unwrap().typname, "json");
        assert_eq!(decl("char(3)").unwrap().typmod, 7);
        assert!(decl("integer").is_none());
        assert!(decl("text").is_none());
        assert!(decl("numeric").is_none());
    }

    #[test]
    fn format_type_renders_typmods() {
        assert_eq!(format_type_text(1043, 24, None), "character varying(20)");
        assert_eq!(format_type_text(1700, 655_366, None), "numeric(10,2)");
        assert_eq!(format_type_text(1009, -1, None), "text[]");
        assert_eq!(format_type_text(1007, -1, None), "integer[]");
        assert_eq!(format_type_text(1042, 7, None), "character(3)");
    }

    #[test]
    fn check_and_default_deparse_like_postgres() {
        let t = TableDef {
            name: "t".into(),
            columns: vec![
                crate::catalog::ColumnDef {
                    name: "qty".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                    default_expr: None,
                    id: 1,
                    analyzer: None,
                    generation: None,
                    max_len: None,
                },
                crate::catalog::ColumnDef {
                    name: "status".into(),
                    data_type: DataType::Text,
                    nullable: true,
                    default_expr: None,
                    id: 2,
                    analyzer: None,
                    generation: None,
                    max_len: None,
                },
            ],
            constraints: vec![],
            append_only: false,
            epoch: 0,
        };
        let snap = PgSnapshot {
            tables: vec![Arc::new(t.clone())],
            views: vec![],
            sequences: vec![],
            indexes: vec![],
            enums: vec![],
            triggers: vec![],
            oids: OidMap::default(),
            declared: HashMap::new(),
        };
        assert_eq!(deparse_check("qty > 0", &t, &snap), "(qty > 0)");
        assert_eq!(
            deparse_check("qty >= 0 AND qty < 100", &t, &snap),
            "((qty >= 0) AND (qty < 100))"
        );
        assert_eq!(
            deparse_check("status IN ('a', 'b')", &t, &snap),
            "(status = ANY (ARRAY['a'::text, 'b'::text]))"
        );
        assert_eq!(
            deparse_check("qty BETWEEN 1 AND 5", &t, &snap),
            "((qty >= 1) AND (qty <= 5))"
        );
        assert_eq!(
            deparse_default("nextval('t_id_seq')", &ty("int8", -1), &DataType::Int64),
            "nextval('t_id_seq'::regclass)"
        );
        assert_eq!(
            deparse_default("'x'", &ty("text", -1), &DataType::Text),
            "'x'::text"
        );
        assert_eq!(deparse_default("0", &ty("int4", -1), &DataType::Int32), "0");
        assert_eq!(
            deparse_default("-1", &ty("int4", -1), &DataType::Int32),
            "'-1'::integer"
        );
        assert_eq!(
            deparse_default("now()", &ty("timestamptz", -1), &DataType::TimestampTz),
            "now()"
        );
        assert_eq!(
            deparse_default("true", &ty("bool", -1), &DataType::Bool),
            "true"
        );
    }
}
