use std::path::PathBuf;
use std::sync::OnceLock;

/// Manages the embedded Nucleus database lifecycle for desktop apps.
///
/// Nucleus runs in-process — zero IPC overhead, microsecond queries.
/// Data is stored per-platform:
/// - Windows: `%APPDATA%\com.neutron.{app}\nucleus\`
/// - macOS: `~/Library/Application Support/com.neutron.{app}/nucleus/`
/// - Linux: `~/.local/share/com.neutron.{app}/nucleus/`
pub struct NucleusState {
    data_dir: PathBuf,
    #[cfg(feature = "nucleus-embedded")]
    db: OnceLock<nucleus::embedded::Database>,
    #[cfg(not(feature = "nucleus-embedded"))]
    _initialized: OnceLock<()>,
}

impl NucleusState {
    /// Create a new Nucleus state with the given data directory.
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            #[cfg(feature = "nucleus-embedded")]
            db: OnceLock::new(),
            #[cfg(not(feature = "nucleus-embedded"))]
            _initialized: OnceLock::new(),
        }
    }

    /// Get the data directory path.
    pub fn data_dir(&self) -> &PathBuf {
        &self.data_dir
    }

    /// Initialize the database, creating the data directory if needed.
    pub fn initialize(&self) -> Result<(), NucleusError> {
        std::fs::create_dir_all(&self.data_dir).map_err(|e| NucleusError::Io(e.to_string()))?;

        #[cfg(feature = "nucleus-embedded")]
        {
            if self.db.get().is_some() {
                return Ok(());
            }
            let database = nucleus::embedded::Database::open(&self.data_dir)
                .map_err(|e| NucleusError::Database(e.to_string()))?;
            let _ = self.db.set(database);
            tracing::info!(dir = %self.data_dir.display(), "Nucleus embedded database opened");
        }
        #[cfg(not(feature = "nucleus-embedded"))]
        {
            let _ = self._initialized.set(());
            tracing::info!(dir = %self.data_dir.display(), "Nucleus data directory ready");
        }

        Ok(())
    }

    /// Check if the database has been initialized.
    pub fn is_initialized(&self) -> bool {
        #[cfg(feature = "nucleus-embedded")]
        { self.db.get().is_some() }
        #[cfg(not(feature = "nucleus-embedded"))]
        { self._initialized.get().is_some() }
    }

    /// Get a reference to the embedded database.
    #[cfg(feature = "nucleus-embedded")]
    pub fn db(&self) -> Result<&nucleus::embedded::Database, NucleusError> {
        self.db.get().ok_or(NucleusError::NotInitialized)
    }

    /// Execute a SQL statement and return JSON-serializable results.
    #[cfg(feature = "nucleus-embedded")]
    pub async fn query(&self, sql: &str) -> Result<NucleusQueryResult, NucleusError> {
        let db = self.db()?;
        let results = db.execute(sql).await
            .map_err(|e| NucleusError::Query(e.to_string()))?;

        let mut columns = Vec::new();
        let mut rows = Vec::new();
        let mut rows_affected = 0usize;

        for res in results {
            match res {
                nucleus::executor::ExecResult::Select { columns: cols, rows: rs } => {
                    columns = cols.into_iter().map(|(name, _)| name).collect();
                    rows = rs.into_iter().map(|row| {
                        row.into_iter().map(value_to_json).collect()
                    }).collect();
                }
                nucleus::executor::ExecResult::Command { rows_affected: n, .. } => {
                    rows_affected += n;
                }
                _ => {}
            }
        }

        Ok(NucleusQueryResult { columns, rows, rows_affected })
    }
}

/// JSON-serializable query result.
#[derive(Debug, serde::Serialize)]
pub struct NucleusQueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub rows_affected: usize,
}

/// Convert a Nucleus Value to a JSON value.
#[cfg(feature = "nucleus-embedded")]
fn value_to_json(v: nucleus::types::Value) -> serde_json::Value {
    use nucleus::types::Value;
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(b),
        Value::Int32(i) => serde_json::json!(i),
        Value::Int64(i) => serde_json::json!(i),
        Value::Float64(f) => serde_json::json!(f),
        Value::Text(s) => serde_json::Value::String(s),
        Value::Bytea(b) => serde_json::Value::String(format!("<blob:{} bytes>", b.len())),
        Value::Jsonb(v) => v,
        Value::Date(d) => serde_json::json!(d),
        Value::Timestamp(ts) => serde_json::json!(ts),
        Value::TimestampTz(ts) => serde_json::json!(ts),
        Value::Numeric(s) => serde_json::Value::String(s),
        Value::Uuid(u) => serde_json::Value::String(format!("{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}", u[0],u[1],u[2],u[3],u[4],u[5],u[6],u[7],u[8],u[9],u[10],u[11],u[12],u[13],u[14],u[15])),
        Value::Array(arr) => serde_json::Value::Array(arr.into_iter().map(value_to_json).collect()),
        Value::Vector(v) => serde_json::json!(v),
        Value::Interval { months, days, microseconds } => serde_json::json!({ "months": months, "days": days, "microseconds": microseconds }),
    }
}

/// Get the platform-appropriate data directory for a given app identifier.
pub fn platform_data_dir(app_id: &str) -> PathBuf {
    let base = dirs::data_dir().expect("no platform data directory available");
    base.join(format!("com.neutron.{app_id}")).join("nucleus")
}

#[derive(Debug, thiserror::Error)]
pub enum NucleusError {
    #[error("IO error: {0}")]
    Io(String),

    #[error("lock poisoned")]
    LockPoisoned,

    #[error("not initialized")]
    NotInitialized,

    #[error("database error: {0}")]
    Database(String),

    #[error("query error: {0}")]
    Query(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nucleus_state_lifecycle() {
        let dir = std::env::temp_dir().join("neutron-test-nucleus");
        let state = NucleusState::new(dir.clone());
        assert!(!state.is_initialized());
        state.initialize().unwrap();
        assert!(state.is_initialized());
        // Second init is a no-op
        state.initialize().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_platform_data_dir() {
        let dir = platform_data_dir("myapp");
        assert!(dir.to_string_lossy().contains("com.neutron.myapp"));
    }
}
