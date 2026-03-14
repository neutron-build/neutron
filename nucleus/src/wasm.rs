//! WebAssembly bindings for Nucleus embedded database.
//!
//! Provides a JavaScript-friendly API for running Nucleus in the browser
//! or any WASM runtime. Only memory-based storage modes are supported.
//!
//! ```js
//! import init, { NucleusWasm } from 'nucleus-wasm';
//!
//! await init();
//! const db = NucleusWasm.memory();
//! await db.execute("CREATE TABLE users (id INT NOT NULL, name TEXT)");
//! const result = await db.query("SELECT * FROM users");
//! console.log(result); // { columns: ["id", "name"], rows: [[1, "Alice"]] }
//! ```

use wasm_bindgen::prelude::*;
use serde::Serialize;
use std::sync::Arc;

use crate::catalog::Catalog;
use crate::executor::{ExecResult, Executor};
use crate::storage::{MemoryEngine, MvccStorageAdapter, StorageEngine};
use crate::types::Value;

/// A Nucleus database instance running in WebAssembly.
///
/// Only in-memory storage modes are available (no filesystem access).
/// All query operations return JavaScript Promises.
#[wasm_bindgen]
pub struct NucleusWasm {
    executor: Arc<Executor>,
    _catalog: Arc<Catalog>,
    _storage: Arc<dyn StorageEngine>,
}

#[wasm_bindgen]
impl NucleusWasm {
    /// Create an in-memory database (simple HashMap storage, fastest).
    #[wasm_bindgen(js_name = "memory")]
    pub fn memory() -> Self {
        let catalog = Arc::new(Catalog::new());
        let storage: Arc<dyn StorageEngine> = Arc::new(MemoryEngine::new());
        let executor = Arc::new(Executor::new(catalog.clone(), storage.clone()));
        Self {
            executor,
            _catalog: catalog,
            _storage: storage,
        }
    }

    /// Create an in-memory database with MVCC (snapshot isolation for concurrent transactions).
    #[wasm_bindgen(js_name = "mvcc")]
    pub fn mvcc() -> Self {
        let catalog = Arc::new(Catalog::new());
        let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
        let executor = Arc::new(Executor::new(catalog.clone(), storage.clone()));
        Self {
            executor,
            _catalog: catalog,
            _storage: storage,
        }
    }

    /// Execute one or more SQL statements (DDL, DML, or queries).
    /// Returns a JSON-compatible JavaScript value with the results.
    #[wasm_bindgen(js_name = "execute")]
    pub async fn execute(&self, sql: &str) -> Result<JsValue, JsValue> {
        let results = self
            .executor
            .execute(sql)
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        let js_results: Vec<JsResultRepr> = results.into_iter().map(exec_result_to_js).collect();

        serde_wasm_bindgen::to_value(&js_results)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Execute a query and return just the rows (convenience for SELECT).
    /// Returns a JavaScript object: `{ columns: string[], rows: any[][] }`.
    #[wasm_bindgen(js_name = "query")]
    pub async fn query(&self, sql: &str) -> Result<JsValue, JsValue> {
        let results = self
            .executor
            .execute(sql)
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        // Find the last SELECT result (skip DDL/DML results that may precede it).
        for result in results.into_iter().rev() {
            if let ExecResult::Select { columns, rows } = result {
                let query_result = JsQueryResult {
                    columns: columns.into_iter().map(|(name, _dt)| name).collect(),
                    rows: rows
                        .into_iter()
                        .map(|row| row.into_iter().map(value_to_js).collect())
                        .collect(),
                };
                return serde_wasm_bindgen::to_value(&query_result)
                    .map_err(|e| JsValue::from_str(&e.to_string()));
            }
        }

        // No SELECT found -- return empty result.
        let empty = JsQueryResult {
            columns: vec![],
            rows: vec![],
        };
        serde_wasm_bindgen::to_value(&empty).map_err(|e| JsValue::from_str(&e.to_string()))
    }
}

// ============================================================================
// Serialization helpers
// ============================================================================

/// Intermediate representation for a single execution result, serialized to JS.
#[derive(Serialize)]
struct JsResultRepr {
    kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    columns: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rows: Option<Vec<Vec<JsVal>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    affected: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

/// Query-only result shape: `{ columns, rows }`.
#[derive(Serialize)]
struct JsQueryResult {
    columns: Vec<String>,
    rows: Vec<Vec<JsVal>>,
}

/// A JSON-friendly representation of a Nucleus [`Value`].
#[derive(Serialize)]
#[serde(untagged)]
enum JsVal {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
}

fn exec_result_to_js(r: ExecResult) -> JsResultRepr {
    match r {
        ExecResult::Select { columns, rows } => JsResultRepr {
            kind: "select".into(),
            columns: Some(columns.into_iter().map(|(name, _dt)| name).collect()),
            rows: Some(
                rows.into_iter()
                    .map(|row| row.into_iter().map(value_to_js).collect())
                    .collect(),
            ),
            affected: None,
            message: None,
        },
        ExecResult::Command { tag, rows_affected } => JsResultRepr {
            kind: "command".into(),
            columns: None,
            rows: None,
            affected: Some(rows_affected),
            message: Some(tag),
        },
        ExecResult::CopyOut { data, row_count } => JsResultRepr {
            kind: "copy_out".into(),
            columns: None,
            rows: None,
            affected: Some(row_count),
            message: Some(data),
        },
    }
}

fn value_to_js(v: Value) -> JsVal {
    match v {
        Value::Null => JsVal::Null,
        Value::Bool(b) => JsVal::Bool(b),
        Value::Int32(i) => JsVal::Int(i as i64),
        Value::Int64(i) => JsVal::Int(i),
        Value::Float64(f) => JsVal::Float(f),
        Value::Text(s) => JsVal::Text(s),
        Value::Jsonb(j) => JsVal::Text(j.to_string()),
        Value::Date(days) => JsVal::Int(days as i64),
        Value::Timestamp(us) => JsVal::Int(us),
        Value::TimestampTz(us) => JsVal::Int(us),
        Value::Numeric(s) => JsVal::Text(s),
        Value::Uuid(bytes) => JsVal::Text(format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            bytes[0], bytes[1], bytes[2], bytes[3],
            bytes[4], bytes[5], bytes[6], bytes[7],
            bytes[8], bytes[9], bytes[10], bytes[11],
            bytes[12], bytes[13], bytes[14], bytes[15],
        )),
        Value::Bytea(b) => {
            use base64::Engine;
            JsVal::Text(base64::engine::general_purpose::STANDARD.encode(&b))
        }
        Value::Array(arr) => JsVal::Text(format!("{:?}", arr)),
        Value::Vector(v) => JsVal::Text(format!("{:?}", v)),
        Value::Interval {
            months,
            days,
            microseconds,
        } => JsVal::Text(format!(
            "{{\"months\":{},\"days\":{},\"microseconds\":{}}}",
            months, days, microseconds,
        )),
    }
}
