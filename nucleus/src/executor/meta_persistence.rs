//! Persistence for executor-level metadata: views, sequences, triggers, roles, functions.
//!
//! Saves to `meta.json` alongside `catalog.json`.  Uses atomic write (tmp + rename)
//! to prevent corruption on power loss.  All deserialization errors are soft-logged
//! and the missing entry is skipped, so a partial file never blocks startup.

use std::collections::HashMap;
use std::io::Write as IoWrite;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::types::DataType;
use super::schema_types::{
    FunctionDef, FunctionKind, FunctionLanguage, MaterializedViewDef, Privilege, RoleDef,
    SequenceDef, TriggerDef, TriggerEvent, TriggerTiming, ViewDef,
};

// ── DataType helpers ─────────────────────────────────────────────────────────

fn dtype_to_str(dt: &DataType) -> String {
    match dt {
        DataType::Bool => "Bool".into(),
        DataType::Int32 => "Int32".into(),
        DataType::Int64 => "Int64".into(),
        DataType::Float64 => "Float64".into(),
        DataType::Text => "Text".into(),
        DataType::Jsonb => "Jsonb".into(),
        DataType::Date => "Date".into(),
        DataType::Timestamp => "Timestamp".into(),
        DataType::TimestampTz => "TimestampTz".into(),
        DataType::Numeric => "Numeric".into(),
        DataType::Uuid => "Uuid".into(),
        DataType::Bytea => "Bytea".into(),
        DataType::Interval => "Interval".into(),
        DataType::Array(inner) => format!("Array({})", dtype_to_str(inner)),
        DataType::Vector(dim) => format!("Vector({dim})"),
        DataType::UserDefined(name) => format!("UserDefined({name})"),
    }
}

fn str_to_dtype(s: &str) -> Option<DataType> {
    match s {
        "Bool" => Some(DataType::Bool),
        "Int32" => Some(DataType::Int32),
        "Int64" => Some(DataType::Int64),
        "Float64" => Some(DataType::Float64),
        "Text" => Some(DataType::Text),
        "Jsonb" => Some(DataType::Jsonb),
        "Date" => Some(DataType::Date),
        "Timestamp" => Some(DataType::Timestamp),
        "TimestampTz" => Some(DataType::TimestampTz),
        "Numeric" => Some(DataType::Numeric),
        "Uuid" => Some(DataType::Uuid),
        "Bytea" => Some(DataType::Bytea),
        "Interval" => Some(DataType::Interval),
        other if other.starts_with("Array(") && other.ends_with(')') => {
            let inner = &other[6..other.len() - 1];
            Some(DataType::Array(Box::new(str_to_dtype(inner)?)))
        }
        other if other.starts_with("Vector(") && other.ends_with(')') => {
            let dim: usize = other[7..other.len() - 1].parse().ok()?;
            Some(DataType::Vector(dim))
        }
        other if other.starts_with("UserDefined(") && other.ends_with(')') => {
            let name = &other[12..other.len() - 1];
            Some(DataType::UserDefined(name.to_string()))
        }
        _ => None,
    }
}

// ── Serializable structs ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ViewSer {
    name: String,
    sql: String,
    columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MatViewSer {
    name: String,
    sql: String,
    /// Column (name, type_str) pairs — rows are NOT persisted, user must REFRESH.
    columns: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SequenceSer {
    name: String,
    current: i64,
    increment: i64,
    min_value: i64,
    max_value: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TriggerSer {
    name: String,
    table_name: String,
    /// "Before" | "After" | "InsteadOf"
    timing: String,
    /// ["Insert", "Update", "Delete"]
    events: Vec<String>,
    for_each_row: bool,
    body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RoleSer {
    name: String,
    #[serde(default)]
    password_hash: Option<String>,
    is_superuser: bool,
    can_login: bool,
    /// table → ["Select", "Insert", ...]
    privileges: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FunctionSer {
    name: String,
    /// "Function" | "Procedure"
    kind: String,
    params: Vec<(String, String)>,
    return_type: Option<String>,
    body: String,
    /// "Sql"
    language: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct MetaSnapshot {
    #[serde(default)]
    views: Vec<ViewSer>,
    #[serde(default)]
    materialized_views: Vec<MatViewSer>,
    #[serde(default)]
    sequences: Vec<SequenceSer>,
    #[serde(default)]
    triggers: Vec<TriggerSer>,
    #[serde(default)]
    roles: Vec<RoleSer>,
    #[serde(default)]
    functions: Vec<FunctionSer>,
}

// ── Conversion helpers ───────────────────────────────────────────────────────

fn privilege_to_str(p: &Privilege) -> &'static str {
    match p {
        Privilege::Select => "Select",
        Privilege::Insert => "Insert",
        Privilege::Update => "Update",
        Privilege::Delete => "Delete",
        Privilege::All => "All",
        Privilege::Create => "Create",
        Privilege::Drop => "Drop",
        Privilege::Usage => "Usage",
    }
}

fn str_to_privilege(s: &str) -> Option<Privilege> {
    match s {
        "Select" => Some(Privilege::Select),
        "Insert" => Some(Privilege::Insert),
        "Update" => Some(Privilege::Update),
        "Delete" => Some(Privilege::Delete),
        "All" => Some(Privilege::All),
        "Create" => Some(Privilege::Create),
        "Drop" => Some(Privilege::Drop),
        "Usage" => Some(Privilege::Usage),
        _ => None,
    }
}

// ── Public API ───────────────────────────────────────────────────────────────

/// Handles saving and loading executor metadata to/from `meta.json`.
pub struct MetaPersistence {
    path: PathBuf,
}

impl MetaPersistence {
    pub fn new(path: &Path) -> Self {
        Self { path: path.to_path_buf() }
    }

    /// Derive the meta.json path from the catalog.json path (same directory).
    pub fn alongside_catalog(catalog_path: &Path) -> Self {
        let dir = catalog_path.parent().unwrap_or(Path::new("."));
        Self::new(&dir.join("meta.json"))
    }

    // ── Save ─────────────────────────────────────────────────────────────────

    pub fn save(
        &self,
        views: &HashMap<String, ViewDef>,
        mat_views: &HashMap<String, MaterializedViewDef>,
        sequences: &HashMap<String, parking_lot::Mutex<SequenceDef>>,
        triggers: &[TriggerDef],
        roles: &HashMap<String, RoleDef>,
        functions: &HashMap<String, FunctionDef>,
    ) -> Result<(), String> {
        let snapshot = MetaSnapshot {
            views: views.values().map(|v| ViewSer {
                name: v.name.clone(),
                sql: v.sql.clone(),
                columns: v.columns.clone(),
            }).collect(),

            materialized_views: mat_views.values().map(|mv| MatViewSer {
                name: mv.name.clone(),
                sql: mv.sql.clone(),
                columns: mv.columns.iter()
                    .map(|(n, dt)| (n.clone(), dtype_to_str(dt)))
                    .collect(),
            }).collect(),

            sequences: sequences.iter().map(|(name, mu)| {
                let seq = mu.lock();
                SequenceSer {
                    name: name.clone(),
                    current: seq.current,
                    increment: seq.increment,
                    min_value: seq.min_value,
                    max_value: seq.max_value,
                }
            }).collect(),

            triggers: triggers.iter().map(|t| TriggerSer {
                name: t.name.clone(),
                table_name: t.table_name.clone(),
                timing: match t.timing {
                    TriggerTiming::Before => "Before",
                    TriggerTiming::After => "After",
                    TriggerTiming::InsteadOf => "InsteadOf",
                }.into(),
                events: t.events.iter().map(|e| match e {
                    TriggerEvent::Insert => "Insert",
                    TriggerEvent::Update => "Update",
                    TriggerEvent::Delete => "Delete",
                }.into()).collect(),
                for_each_row: t.for_each_row,
                body: t.body.clone(),
            }).collect(),

            roles: roles.values().map(|r| RoleSer {
                name: r.name.clone(),
                password_hash: r.password_hash.clone(),
                is_superuser: r.is_superuser,
                can_login: r.can_login,
                privileges: r.privileges.iter().map(|(table, privs)| {
                    (table.clone(), privs.iter().map(|p| privilege_to_str(p).to_string()).collect())
                }).collect(),
            }).collect(),

            functions: functions.values().map(|f| FunctionSer {
                name: f.name.clone(),
                kind: match f.kind {
                    FunctionKind::Function => "Function",
                    FunctionKind::Procedure => "Procedure",
                }.into(),
                params: f.params.iter().map(|(n, dt)| (n.clone(), dtype_to_str(dt))).collect(),
                return_type: f.return_type.as_ref().map(dtype_to_str),
                body: f.body.clone(),
                language: match f.language {
                    FunctionLanguage::Sql => "Sql",
                }.into(),
            }).collect(),
        };

        let json = serde_json::to_string_pretty(&snapshot)
            .map_err(|e| format!("meta serialize: {e}"))?;

        let tmp = self.path.with_extension("json.tmp");
        {
            let mut f = std::fs::File::create(&tmp)
                .map_err(|e| format!("meta write tmp: {e}"))?;
            f.write_all(json.as_bytes()).map_err(|e| format!("meta write: {e}"))?;
            f.sync_all().map_err(|e| format!("meta fsync: {e}"))?;
        }
        std::fs::rename(&tmp, &self.path).map_err(|e| format!("meta rename: {e}"))?;
        Ok(())
    }

    // ── Load ─────────────────────────────────────────────────────────────────

    /// Load persisted metadata. Returns empty maps if the file doesn't exist.
    pub fn load(&self) -> LoadedMeta {
        if !self.path.exists() {
            return LoadedMeta::default();
        }
        let json = match std::fs::read_to_string(&self.path) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("meta.json read error: {e}");
                return LoadedMeta::default();
            }
        };
        let snap: MetaSnapshot = match serde_json::from_str(&json) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("meta.json parse error: {e}");
                return LoadedMeta::default();
            }
        };

        let mut meta = LoadedMeta::default();

        for v in snap.views {
            meta.views.insert(v.name.clone(), ViewDef {
                name: v.name,
                sql: v.sql,
                columns: v.columns,
            });
        }

        for mv in snap.materialized_views {
            let columns: Vec<(String, DataType)> = mv.columns.into_iter()
                .filter_map(|(n, t)| str_to_dtype(&t).map(|dt| (n, dt)))
                .collect();
            meta.materialized_views.insert(mv.name.clone(), MaterializedViewDef {
                name: mv.name,
                sql: mv.sql,
                columns,
                rows: vec![],   // rows are not persisted; user must REFRESH
            });
        }

        for s in snap.sequences {
            let seq = SequenceDef {
                current: s.current,
                increment: s.increment,
                min_value: s.min_value,
                max_value: s.max_value,
            };
            meta.sequences.insert(s.name, parking_lot::Mutex::new(seq));
        }

        for t in snap.triggers {
            let timing = match t.timing.as_str() {
                "Before" => TriggerTiming::Before,
                "InsteadOf" => TriggerTiming::InsteadOf,
                _ => TriggerTiming::After,
            };
            let events: Vec<TriggerEvent> = t.events.iter().filter_map(|e| match e.as_str() {
                "Insert" => Some(TriggerEvent::Insert),
                "Update" => Some(TriggerEvent::Update),
                "Delete" => Some(TriggerEvent::Delete),
                _ => None,
            }).collect();
            meta.triggers.push(TriggerDef {
                name: t.name,
                table_name: t.table_name,
                timing,
                events,
                for_each_row: t.for_each_row,
                body: t.body,
            });
        }

        for r in snap.roles {
            let privileges: HashMap<String, Vec<Privilege>> = r.privileges.into_iter()
                .map(|(table, privs)| {
                    (table, privs.iter().filter_map(|p| str_to_privilege(p)).collect())
                }).collect();
            meta.roles.insert(r.name.clone(), RoleDef {
                name: r.name,
                password_hash: r.password_hash,
                is_superuser: r.is_superuser,
                can_login: r.can_login,
                privileges,
            });
        }

        for f in snap.functions {
            let params: Vec<(String, DataType)> = f.params.into_iter()
                .filter_map(|(n, t)| str_to_dtype(&t).map(|dt| (n, dt)))
                .collect();
            let return_type = f.return_type.as_deref().and_then(str_to_dtype);
            let kind = match f.kind.as_str() {
                "Procedure" => FunctionKind::Procedure,
                _ => FunctionKind::Function,
            };
            let language = FunctionLanguage::Sql;
            let _ = f.language.as_str(); // reserved for future language variants
            meta.functions.insert(f.name.clone(), FunctionDef {
                name: f.name,
                kind,
                params,
                return_type,
                body: f.body,
                language,
            });
        }

        meta
    }
}

/// Deserialized executor metadata ready to be installed into the executor's maps.
#[derive(Default)]
pub struct LoadedMeta {
    pub views: HashMap<String, ViewDef>,
    pub materialized_views: HashMap<String, MaterializedViewDef>,
    pub sequences: HashMap<String, parking_lot::Mutex<SequenceDef>>,
    pub triggers: Vec<TriggerDef>,
    pub roles: HashMap<String, RoleDef>,
    pub functions: HashMap<String, FunctionDef>,
}
