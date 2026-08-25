//! Source type-name mapping. PostgreSQL reports two type names per column
//! (`information_schema.columns.data_type` plus the low-level `udt_name`);
//! SQLite reports a free-form declared type. Each mapping records whether it
//! is lossless and, when it is not, exactly why.

use crate::types::DataType;

#[derive(Debug, Clone, PartialEq)]
pub struct MappedType {
    pub data_type: DataType,
    pub lossless: bool,
    pub note: Option<String>,
}

fn clean(t: DataType) -> MappedType {
    MappedType {
        data_type: t,
        lossless: true,
        note: None,
    }
}

fn lossy(t: DataType, note: &str) -> MappedType {
    MappedType {
        data_type: t,
        lossless: false,
        note: Some(note.to_string()),
    }
}

/// Map a PostgreSQL column type to a Nucleus type.
///
/// `data_type` is `information_schema.columns.data_type`; `udt_name` is the
/// pg_catalog name (`int4`, `_text`, …). Types with no Nucleus equivalent map
/// to TEXT and are flagged lossy — the value text is preserved, its semantics
/// are not.
pub fn map_pg_type(data_type: &str, udt_name: &str) -> MappedType {
    let dt = normalize(data_type);
    match dt.as_str() {
        "boolean" | "bool" => clean(DataType::Bool),
        "smallint" => clean(DataType::Int32),
        "integer" | "int" | "int4" => clean(DataType::Int32),
        "bigint" | "int8" => clean(DataType::Int64),
        "real" => clean(DataType::Float64),
        "double precision" => clean(DataType::Float64),
        "numeric" | "decimal" => clean(DataType::Numeric),
        "text" => clean(DataType::Text),
        "character varying" | "varchar" | "character" | "bpchar" | "char" | "name" => {
            clean(DataType::Text)
        }
        "json" | "jsonb" => clean(DataType::Jsonb),
        "date" => clean(DataType::Date),
        "timestamp without time zone" | "timestamp" => clean(DataType::Timestamp),
        "timestamp with time zone" | "timestamptz" => clean(DataType::TimestampTz),
        "time without time zone" | "time with time zone" | "time" | "timetz" => lossy(
            DataType::Text,
            "PostgreSQL TIME has no Nucleus type; stored as text",
        ),
        "interval" => clean(DataType::Interval),
        "uuid" => clean(DataType::Uuid),
        "bytea" => clean(DataType::Bytea),
        "array" => {
            let inner_udt = udt_name.strip_prefix('_').unwrap_or(udt_name);
            let inner = pg_udt_to_type(inner_udt);
            match inner {
                Some(t) => MappedType {
                    data_type: DataType::Array(Box::new(t)),
                    lossless: true,
                    note: None,
                },
                None => lossy(
                    DataType::Text,
                    &format!("array of {inner_udt} has no Nucleus element type; stored as text"),
                ),
            }
        }
        "user-defined" => lossy(
            DataType::Text,
            &format!("PostgreSQL type '{udt_name}' has no Nucleus equivalent; stored as text"),
        ),
        other => lossy(
            DataType::Text,
            &format!("PostgreSQL type '{other}' has no Nucleus equivalent; stored as text"),
        ),
    }
}

fn normalize(data_type: &str) -> String {
    let dt = data_type.trim().to_ascii_lowercase();
    // sqlparser renders sized types ("VARCHAR(80)", "NUMERIC(10,2)"); the
    // size does not change the target type.
    match dt.find('(') {
        Some(i) => dt[..i].trim().to_string(),
        None => dt,
    }
}

fn pg_udt_to_type(udt: &str) -> Option<DataType> {
    Some(match udt {
        "bool" => DataType::Bool,
        "int2" | "int4" => DataType::Int32,
        "int8" => DataType::Int64,
        "float4" | "float8" => DataType::Float64,
        "numeric" => DataType::Numeric,
        "text" | "varchar" | "bpchar" | "name" | "char" => DataType::Text,
        "json" | "jsonb" => DataType::Jsonb,
        "date" => DataType::Date,
        "timestamp" => DataType::Timestamp,
        "timestamptz" => DataType::TimestampTz,
        "interval" => DataType::Interval,
        "uuid" => DataType::Uuid,
        "bytea" => DataType::Bytea,
        _ => return None,
    })
}

/// Map a SQLite column's declared type using SQLite's affinity rules, then
/// refine common semantic names. SQLite's typing is dynamic — the declared
/// type constrains affinity, not stored values — so anything whose meaning
/// depends on more than affinity is flagged lossy with a note.
pub fn map_sqlite_type(declared: &str) -> MappedType {
    let d = declared.trim().to_ascii_uppercase();
    if d.is_empty() {
        return lossy(
            DataType::Bytea,
            "SQLite BLOB (no declared type); stored as bytea",
        );
    }
    if d.contains("INT") {
        return clean(DataType::Int64);
    }
    if d.contains("CHAR") || d.contains("CLOB") || d.contains("TEXT") {
        return clean(DataType::Text);
    }
    if d.contains("BLOB") {
        return clean(DataType::Bytea);
    }
    if d.contains("REAL") || d.contains("FLOA") || d.contains("DOUB") {
        return clean(DataType::Float64);
    }
    match d.as_str() {
        "BOOLEAN" | "BOOL" => lossy(
            DataType::Bool,
            "SQLite BOOLEAN is numeric affinity; only 0/1 values survive as booleans",
        ),
        "DATE" => lossy(
            DataType::Date,
            "SQLite DATE is free-form text; parsed as a date",
        ),
        "DATETIME" | "TIMESTAMP" => lossy(
            DataType::Timestamp,
            "SQLite DATETIME is free-form text; parsed as a timestamp",
        ),
        "JSON" | "JSONB" => lossy(
            DataType::Jsonb,
            "SQLite has no JSON type; text is parsed as JSONB",
        ),
        "UUID" => lossy(
            DataType::Uuid,
            "SQLite has no UUID type; text is parsed as a UUID",
        ),
        _ => lossy(
            DataType::Numeric,
            "SQLite NUMERIC affinity stores mixed types; non-numeric values become text",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlparser_style_names_map_for_sql_text_source() {
        // The SQL-text source feeds sqlparser's rendered type names through
        // the PostgreSQL mapper; Nucleus's own dialect shares the names.
        let m = map_pg_type("BIGINT", "int8");
        assert_eq!(m.data_type, DataType::Int64);
        let m = map_pg_type("DOUBLE PRECISION", "float8");
        assert_eq!(m.data_type, DataType::Float64);
    }
}
