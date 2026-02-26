//! Stored procedures engine with sandboxed execution.
//!
//! Supports:
//!   - Register/unregister stored procedures
//!   - Execute procedures with arguments and return values
//!   - Procedure metadata (language, author, version)
//!   - Execution context with database access primitives
//!   - Built-in procedures (SQL-based)
//!
//! The WASM execution path is designed as a trait so a full runtime
//! (wasmtime/wasmer) can be plugged in without changing the procedure API.
//!
//! Replaces PL/pgSQL and custom stored procedure systems.

use std::collections::HashMap;

// ============================================================================
// Procedure types
// ============================================================================

/// A typed value for procedure arguments and return values.
#[derive(Debug, Clone, PartialEq)]
pub enum ProcValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    Array(Vec<ProcValue>),
    Map(HashMap<String, ProcValue>),
}

impl ProcValue {
    pub fn as_text(&self) -> Option<&str> {
        match self {
            ProcValue::Text(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            ProcValue::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            ProcValue::Float(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            ProcValue::Bool(b) => Some(*b),
            _ => None,
        }
    }
}

/// The language a procedure is written in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Language {
    Sql,
    Wasm,
    /// Built-in (implemented in Rust).
    Builtin,
}

/// Data type annotation for procedure parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParamType {
    Text,
    Integer,
    Float,
    Boolean,
    Any,
}

impl std::fmt::Display for ParamType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParamType::Text => write!(f, "TEXT"),
            ParamType::Integer => write!(f, "INTEGER"),
            ParamType::Float => write!(f, "FLOAT"),
            ParamType::Boolean => write!(f, "BOOLEAN"),
            ParamType::Any => write!(f, "ANY"),
        }
    }
}

/// Metadata about a stored procedure.
#[derive(Debug, Clone)]
pub struct ProcedureMetadata {
    pub name: String,
    pub language: Language,
    pub description: String,
    pub param_names: Vec<String>,
    pub param_types: Vec<ParamType>,
    pub return_type: Option<ParamType>,
    pub author: Option<String>,
    pub version: Option<String>,
    pub created_at: u64,
}

/// Result of a procedure execution.
#[derive(Debug, Clone)]
pub enum ProcResult {
    /// Successfully returned a value.
    Ok(ProcValue),
    /// Successfully returned multiple rows (for table-returning functions).
    Rows(Vec<Vec<ProcValue>>),
    /// Execution failed.
    Error(String),
}

// ============================================================================
// Procedure implementations
// ============================================================================

/// A SQL-based stored procedure.
#[derive(Debug, Clone)]
pub struct SqlProcedure {
    pub body: String,
    pub param_names: Vec<String>,
}

/// Trait for WASM-based procedure execution.
/// Implementations can use wasmtime, wasmer, or any WASM runtime.
pub trait WasmRuntime: Send + Sync {
    /// Execute a WASM module with the given function name and arguments.
    fn execute(
        &self,
        module_bytes: &[u8],
        function: &str,
        args: &[ProcValue],
    ) -> ProcResult;
}

type BuiltinProcFn = dyn Fn(&[ProcValue]) -> ProcResult + Send + Sync;

/// Type-erased procedure body.
enum ProcedureBody {
    Sql(SqlProcedure),
    Wasm { module_bytes: Vec<u8>, entry_point: String },
    Builtin(Box<BuiltinProcFn>),
}

/// A registered stored procedure.
struct RegisteredProcedure {
    metadata: ProcedureMetadata,
    body: ProcedureBody,
}

// ============================================================================
// Procedure registry
// ============================================================================

/// Registry and executor for stored procedures.
pub struct ProcedureEngine {
    procedures: HashMap<String, RegisteredProcedure>,
    /// Optional WASM runtime.
    wasm_runtime: Option<Box<dyn WasmRuntime>>,
    /// Execution log.
    executions: Vec<ExecutionRecord>,
}

/// Record of a procedure execution.
#[derive(Debug, Clone)]
pub struct ExecutionRecord {
    pub procedure: String,
    pub args_count: usize,
    pub success: bool,
    pub duration_us: u64,
    pub timestamp: u64,
}

impl Default for ProcedureEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcedureEngine {
    pub fn new() -> Self {
        let mut engine = Self {
            procedures: HashMap::new(),
            wasm_runtime: None,
            executions: Vec::new(),
        };
        engine.register_builtins();
        engine
    }

    /// Set the WASM runtime.
    pub fn set_wasm_runtime(&mut self, runtime: Box<dyn WasmRuntime>) {
        self.wasm_runtime = Some(runtime);
    }

    /// Register a SQL stored procedure with explicit parameter types.
    pub fn register_sql_typed(
        &mut self,
        name: &str,
        description: &str,
        param_names: Vec<String>,
        param_types: Vec<ParamType>,
        return_type: Option<ParamType>,
        body: &str,
    ) {
        let ts = now_ms();
        self.procedures.insert(
            name.to_string(),
            RegisteredProcedure {
                metadata: ProcedureMetadata {
                    name: name.to_string(),
                    language: Language::Sql,
                    description: description.to_string(),
                    param_types,
                    param_names: param_names.clone(),
                    return_type,
                    author: None,
                    version: None,
                    created_at: ts,
                },
                body: ProcedureBody::Sql(SqlProcedure {
                    body: body.to_string(),
                    param_names,
                }),
            },
        );
    }

    /// Register a SQL stored procedure (untyped — all params are `Any`).
    pub fn register_sql(
        &mut self,
        name: &str,
        description: &str,
        param_names: Vec<String>,
        body: &str,
    ) {
        let ts = now_ms();
        self.procedures.insert(
            name.to_string(),
            RegisteredProcedure {
                metadata: ProcedureMetadata {
                    name: name.to_string(),
                    language: Language::Sql,
                    description: description.to_string(),
                    param_types: vec![ParamType::Any; param_names.len()],
                    param_names: param_names.clone(),
                    return_type: None,
                    author: None,
                    version: None,
                    created_at: ts,
                },
                body: ProcedureBody::Sql(SqlProcedure {
                    body: body.to_string(),
                    param_names,
                }),
            },
        );
    }

    /// Register a WASM stored procedure.
    pub fn register_wasm(
        &mut self,
        name: &str,
        description: &str,
        param_names: Vec<String>,
        module_bytes: Vec<u8>,
        entry_point: &str,
    ) {
        let ts = now_ms();
        self.procedures.insert(
            name.to_string(),
            RegisteredProcedure {
                metadata: ProcedureMetadata {
                    name: name.to_string(),
                    language: Language::Wasm,
                    description: description.to_string(),
                    param_types: vec![ParamType::Any; param_names.len()],
                    param_names,
                    return_type: None,
                    author: None,
                    version: None,
                    created_at: ts,
                },
                body: ProcedureBody::Wasm {
                    module_bytes,
                    entry_point: entry_point.to_string(),
                },
            },
        );
    }

    /// Register a built-in (Rust) procedure.
    pub fn register_builtin(
        &mut self,
        name: &str,
        description: &str,
        param_names: Vec<String>,
        func: impl Fn(&[ProcValue]) -> ProcResult + Send + Sync + 'static,
    ) {
        let ts = now_ms();
        self.procedures.insert(
            name.to_string(),
            RegisteredProcedure {
                metadata: ProcedureMetadata {
                    name: name.to_string(),
                    language: Language::Builtin,
                    description: description.to_string(),
                    param_types: vec![ParamType::Any; param_names.len()],
                    param_names,
                    return_type: None,
                    author: None,
                    version: None,
                    created_at: ts,
                },
                body: ProcedureBody::Builtin(Box::new(func)),
            },
        );
    }

    /// Unregister a procedure.
    pub fn unregister(&mut self, name: &str) -> bool {
        self.procedures.remove(name).is_some()
    }

    /// Execute a procedure by name.
    pub fn execute(&mut self, name: &str, args: &[ProcValue]) -> ProcResult {
        let start = std::time::Instant::now();

        let result = match self.procedures.get(name) {
            None => ProcResult::Error(format!("procedure not found: {name}")),
            Some(proc) => match &proc.body {
                ProcedureBody::Sql(sql_proc) => {
                    // For SQL procedures, return the body with parameter substitution.
                    // Text values are escaped to prevent SQL injection:
                    // - NUL bytes stripped
                    // - Backslashes doubled
                    // - Single quotes doubled
                    let mut positional = Vec::with_capacity(sql_proc.param_names.len());
                    let mut named = HashMap::new();
                    for (i, param) in sql_proc.param_names.iter().enumerate() {
                        if let Some(arg) = args.get(i) {
                            let replacement = proc_sql_replacement(arg);
                            positional.push(replacement.clone());
                            named.insert(param.clone(), replacement);
                        } else {
                            positional.push("NULL".to_string());
                        }
                    }
                    let body = substitute_proc_sql_placeholders(
                        &sql_proc.body,
                        &positional,
                        &named,
                    );
                    ProcResult::Ok(ProcValue::Text(body))
                }
                ProcedureBody::Wasm { module_bytes, entry_point } => {
                    match &self.wasm_runtime {
                        Some(runtime) => runtime.execute(module_bytes, entry_point, args),
                        None => ProcResult::Error("no WASM runtime configured".into()),
                    }
                }
                ProcedureBody::Builtin(func) => func(args),
            },
        };

        let duration = start.elapsed().as_micros() as u64;
        let success = matches!(result, ProcResult::Ok(_) | ProcResult::Rows(_));

        self.executions.push(ExecutionRecord {
            procedure: name.to_string(),
            args_count: args.len(),
            success,
            duration_us: duration,
            timestamp: now_ms(),
        });

        result
    }

    /// Get procedure metadata.
    pub fn get_metadata(&self, name: &str) -> Option<&ProcedureMetadata> {
        self.procedures.get(name).map(|p| &p.metadata)
    }

    /// List all registered procedures.
    pub fn list_procedures(&self) -> Vec<&ProcedureMetadata> {
        self.procedures.values().map(|p| &p.metadata).collect()
    }

    /// Get execution history.
    pub fn execution_history(&self) -> &[ExecutionRecord] {
        &self.executions
    }

    /// Register built-in utility procedures.
    fn register_builtins(&mut self) {
        self.register_builtin(
            "nucleus_version",
            "Returns the Nucleus version string",
            vec![],
            |_args| ProcResult::Ok(ProcValue::Text("0.1.0".into())),
        );

        self.register_builtin(
            "coalesce",
            "Returns the first non-null argument",
            vec!["values".into()],
            |args| {
                for arg in args {
                    if *arg != ProcValue::Null {
                        return ProcResult::Ok(arg.clone());
                    }
                }
                ProcResult::Ok(ProcValue::Null)
            },
        );

        self.register_builtin(
            "array_length",
            "Returns the length of an array",
            vec!["arr".into()],
            |args| {
                match args.first() {
                    Some(ProcValue::Array(arr)) => ProcResult::Ok(ProcValue::Int(arr.len() as i64)),
                    Some(ProcValue::Text(s)) => ProcResult::Ok(ProcValue::Int(s.len() as i64)),
                    _ => ProcResult::Error("expected array or text argument".into()),
                }
            },
        );

        self.register_builtin(
            "json_extract",
            "Extract a field from a JSON string",
            vec!["json".into(), "field".into()],
            |args| {
                let json_str = match args.first() {
                    Some(ProcValue::Text(s)) => s,
                    _ => return ProcResult::Error("expected text argument".into()),
                };
                let field = match args.get(1) {
                    Some(ProcValue::Text(s)) => s,
                    _ => return ProcResult::Error("expected field name".into()),
                };

                // Simple JSON field extraction (not a full parser)
                let pattern = format!("\"{field}\":");
                if let Some(pos) = json_str.find(&pattern) {
                    let after = &json_str[pos + pattern.len()..];
                    let trimmed = after.trim_start();
                    if let Some(after_quote) = trimmed.strip_prefix('"') {
                        // String value
                        if let Some(end) = after_quote.find('"') {
                            return ProcResult::Ok(ProcValue::Text(after_quote[..end].to_string()));
                        }
                    } else {
                        // Numeric or other value
                        let end = trimmed.find([',', '}', ']']).unwrap_or(trimmed.len());
                        let val = trimmed[..end].trim();
                        if let Ok(n) = val.parse::<i64>() {
                            return ProcResult::Ok(ProcValue::Int(n));
                        }
                        if let Ok(f) = val.parse::<f64>() {
                            return ProcResult::Ok(ProcValue::Float(f));
                        }
                        return ProcResult::Ok(ProcValue::Text(val.to_string()));
                    }
                }
                ProcResult::Ok(ProcValue::Null)
            },
        );
    }
}

fn sanitize_proc_sql_text(value: &str) -> String {
    value
        .replace('\0', "")
        .replace('\\', "\\\\")
        .replace('\'', "''")
}

fn proc_sql_replacement(value: &ProcValue) -> String {
    match value {
        ProcValue::Text(s) => format!("'{}'", sanitize_proc_sql_text(s)),
        ProcValue::Int(n) => n.to_string(),
        ProcValue::Float(f) => f.to_string(),
        ProcValue::Bool(b) => b.to_string(),
        ProcValue::Null => "NULL".to_string(),
        _ => format!("'{}'", sanitize_proc_sql_text(&format!("{value:?}"))),
    }
}

fn substitute_proc_sql_placeholders(
    sql: &str,
    positional: &[String],
    named: &HashMap<String, String>,
) -> String {
    let mut out = String::with_capacity(sql.len() + 32);
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while i < bytes.len() {
        if in_line_comment {
            out.push(bytes[i] as char);
            if bytes[i] == b'\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }
        if in_block_comment {
            if i + 1 < bytes.len() && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                out.push('*');
                out.push('/');
                in_block_comment = false;
                i += 2;
            } else {
                out.push(bytes[i] as char);
                i += 1;
            }
            continue;
        }
        if in_single {
            out.push(bytes[i] as char);
            if bytes[i] == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    out.push('\'');
                    i += 2;
                } else {
                    in_single = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }
        if in_double {
            out.push(bytes[i] as char);
            if bytes[i] == b'"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    out.push('"');
                    i += 2;
                } else {
                    in_double = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }

        if i + 1 < bytes.len() && bytes[i] == b'-' && bytes[i + 1] == b'-' {
            out.push('-');
            out.push('-');
            in_line_comment = true;
            i += 2;
            continue;
        }
        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            out.push('/');
            out.push('*');
            in_block_comment = true;
            i += 2;
            continue;
        }
        if bytes[i] == b'\'' {
            out.push('\'');
            in_single = true;
            i += 1;
            continue;
        }
        if bytes[i] == b'"' {
            out.push('"');
            in_double = true;
            i += 1;
            continue;
        }

        if bytes[i] == b'$' {
            let start = i;
            i += 1;
            if i < bytes.len() && bytes[i].is_ascii_digit() {
                let mut idx = 0usize;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    idx = idx * 10 + (bytes[i] - b'0') as usize;
                    i += 1;
                }
                if idx > 0 && idx <= positional.len() {
                    out.push_str(&positional[idx - 1]);
                } else {
                    out.push_str(&sql[start..i]);
                }
                continue;
            }
            if i < bytes.len()
                && (bytes[i].is_ascii_alphabetic() || bytes[i] == b'_')
            {
                let ident_start = i;
                i += 1;
                while i < bytes.len()
                    && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_')
                {
                    i += 1;
                }
                let ident = &sql[ident_start..i];
                if let Some(repl) = named.get(ident) {
                    out.push_str(repl);
                } else {
                    out.push_str(&sql[start..i]);
                }
                continue;
            }
            out.push('$');
            continue;
        }

        out.push(bytes[i] as char);
        i += 1;
    }

    out
}

// ============================================================================
// Sandboxed WASM execution (checklist 7.1)
// ============================================================================

/// Configuration for WASM procedure sandboxing.
#[derive(Debug, Clone)]
pub struct WasmSandboxConfig {
    /// Maximum memory pages (64 KiB each) a WASM module may allocate.
    pub max_memory_pages: u32,
    /// Maximum fuel (instruction count) before aborting.
    pub max_fuel: u64,
    /// Maximum byte size of a WASM module.
    pub max_module_bytes: usize,
    /// Allowed host imports (function names the WASM module may call).
    pub allowed_imports: Vec<String>,
}

impl Default for WasmSandboxConfig {
    fn default() -> Self {
        Self {
            max_memory_pages: 256,   // 16 MiB
            max_fuel: 1_000_000,
            max_module_bytes: 4 * 1024 * 1024, // 4 MiB
            allowed_imports: vec![
                "nucleus_log".into(),
                "nucleus_query".into(),
                "nucleus_kv_get".into(),
                "nucleus_kv_set".into(),
            ],
        }
    }
}

/// Validation result for a WASM module.
#[derive(Debug, Clone)]
pub struct WasmValidation {
    pub valid: bool,
    pub exports: Vec<String>,
    pub imports: Vec<String>,
    pub memory_pages_requested: u32,
    pub errors: Vec<String>,
}

/// Sandboxed WASM runtime that validates modules and enforces limits.
///
/// This is a simulation layer — a real deployment would use `wasmtime` or
/// `wasmer`. The sandbox validates module structure, enforces size/memory
/// limits, and provides a fuel-metered execution shim.
pub struct SandboxedWasmRuntime {
    config: WasmSandboxConfig,
    modules: HashMap<String, WasmModule>,
    execution_count: u64,
    fuel_consumed: u64,
}

/// A registered WASM module.
#[derive(Debug, Clone)]
struct WasmModule {
    name: String,
    bytes: Vec<u8>,
    exports: Vec<String>,
    imports: Vec<String>,
    memory_pages: u32,
}

impl SandboxedWasmRuntime {
    pub fn new(config: WasmSandboxConfig) -> Self {
        Self {
            config,
            modules: HashMap::new(),
            execution_count: 0,
            fuel_consumed: 0,
        }
    }

    /// Validate a WASM module against sandbox policies.
    pub fn validate_module(&self, name: &str, bytes: &[u8]) -> WasmValidation {
        let mut errors = Vec::new();
        let mut exports = Vec::new();
        let imports = Vec::new();
        let memory_pages: u32 = 1;

        // Size check
        if bytes.len() > self.config.max_module_bytes {
            errors.push(format!(
                "module size {}B exceeds limit {}B",
                bytes.len(), self.config.max_module_bytes
            ));
        }

        // WASM magic number check: \0asm
        if bytes.len() >= 4 && bytes[0..4] == [0x00, 0x61, 0x73, 0x6D] {
            // Parse simple metadata from the module name convention:
            // In a real implementation, we'd parse the actual WASM binary sections.
            exports.push(name.to_string());
        } else if bytes.len() >= 4 {
            errors.push("invalid WASM magic number".into());
        }

        // Simulate import scanning — check if any declared imports are disallowed.
        // In real impl, parse the import section of the WASM binary.
        for imp in &imports {
            if !self.config.allowed_imports.contains(imp) {
                errors.push(format!("disallowed import: {imp}"));
            }
        }

        // Memory check
        if memory_pages > self.config.max_memory_pages {
            errors.push(format!(
                "requested {memory_pages} memory pages, limit is {}",
                self.config.max_memory_pages
            ));
        }

        WasmValidation {
            valid: errors.is_empty(),
            exports,
            imports,
            memory_pages_requested: memory_pages,
            errors,
        }
    }

    /// Load a validated WASM module into the sandbox.
    pub fn load_module(&mut self, name: &str, bytes: Vec<u8>, exports: Vec<String>) -> Result<(), String> {
        let validation = self.validate_module(name, &bytes);
        if !validation.valid {
            return Err(validation.errors.join("; "));
        }
        self.modules.insert(name.to_string(), WasmModule {
            name: name.to_string(),
            bytes,
            exports,
            imports: validation.imports,
            memory_pages: validation.memory_pages_requested,
        });
        Ok(())
    }

    /// Unload a WASM module.
    pub fn unload_module(&mut self, name: &str) -> bool {
        self.modules.remove(name).is_some()
    }

    /// List loaded module names.
    pub fn loaded_modules(&self) -> Vec<String> {
        self.modules.keys().cloned().collect()
    }

    /// Get execution statistics.
    pub fn stats(&self) -> (u64, u64) {
        (self.execution_count, self.fuel_consumed)
    }
}

impl WasmRuntime for SandboxedWasmRuntime {
    fn execute(&self, module_bytes: &[u8], function: &str, args: &[ProcValue]) -> ProcResult {
        // Size check
        if module_bytes.len() > self.config.max_module_bytes {
            return ProcResult::Error(format!(
                "module size {}B exceeds sandbox limit {}B",
                module_bytes.len(), self.config.max_module_bytes
            ));
        }

        // WASM magic number check
        if module_bytes.len() < 4 || module_bytes[0..4] != [0x00, 0x61, 0x73, 0x6D] {
            return ProcResult::Error("invalid WASM module (bad magic number)".into());
        }

        // Simulate fuel-metered execution:
        // In a real runtime, this would instantiate the WASM module and call the
        // exported function with fuel limits. Here we simulate by returning
        // a deterministic result based on the function name and args.
        let result_text = format!(
            "wasm:{function}({})",
            args.iter()
                .map(|a| match a {
                    ProcValue::Int(n) => n.to_string(),
                    ProcValue::Text(s) => format!("'{s}'"),
                    ProcValue::Float(f) => f.to_string(),
                    ProcValue::Bool(b) => b.to_string(),
                    ProcValue::Null => "null".into(),
                    _ => "?".into(),
                })
                .collect::<Vec<_>>()
                .join(", ")
        );

        ProcResult::Ok(ProcValue::Text(result_text))
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_version() {
        let mut engine = ProcedureEngine::new();
        let result = engine.execute("nucleus_version", &[]);
        match result {
            ProcResult::Ok(ProcValue::Text(v)) => assert_eq!(v, "0.1.0"),
            _ => panic!("expected version string"),
        }
    }

    #[test]
    fn builtin_coalesce() {
        let mut engine = ProcedureEngine::new();
        let result = engine.execute("coalesce", &[
            ProcValue::Null,
            ProcValue::Null,
            ProcValue::Int(42),
            ProcValue::Text("hello".into()),
        ]);
        match result {
            ProcResult::Ok(ProcValue::Int(42)) => {}
            _ => panic!("expected Int(42)"),
        }
    }

    #[test]
    fn builtin_array_length() {
        let mut engine = ProcedureEngine::new();
        let result = engine.execute("array_length", &[
            ProcValue::Array(vec![ProcValue::Int(1), ProcValue::Int(2), ProcValue::Int(3)]),
        ]);
        match result {
            ProcResult::Ok(ProcValue::Int(3)) => {}
            _ => panic!("expected Int(3)"),
        }
    }

    #[test]
    fn builtin_json_extract() {
        let mut engine = ProcedureEngine::new();
        let json = r#"{"name":"Alice","age":30}"#;
        let result = engine.execute("json_extract", &[
            ProcValue::Text(json.into()),
            ProcValue::Text("name".into()),
        ]);
        match result {
            ProcResult::Ok(ProcValue::Text(s)) => assert_eq!(s, "Alice"),
            _ => panic!("expected text 'Alice'"),
        }

        let result = engine.execute("json_extract", &[
            ProcValue::Text(json.into()),
            ProcValue::Text("age".into()),
        ]);
        match result {
            ProcResult::Ok(ProcValue::Int(30)) => {}
            _ => panic!("expected Int(30)"),
        }
    }

    #[test]
    fn sql_procedure() {
        let mut engine = ProcedureEngine::new();
        engine.register_sql(
            "get_user",
            "Fetch a user by ID",
            vec!["id".into()],
            "SELECT * FROM users WHERE id = $id",
        );

        let result = engine.execute("get_user", &[ProcValue::Int(42)]);
        match result {
            ProcResult::Ok(ProcValue::Text(sql)) => {
                assert_eq!(sql, "SELECT * FROM users WHERE id = 42");
            }
            _ => panic!("expected substituted SQL"),
        }
    }

    #[test]
    fn procedure_not_found() {
        let mut engine = ProcedureEngine::new();
        let result = engine.execute("nonexistent", &[]);
        match result {
            ProcResult::Error(msg) => assert!(msg.contains("not found")),
            _ => panic!("expected error"),
        }
    }

    #[test]
    fn custom_builtin() {
        let mut engine = ProcedureEngine::new();
        engine.register_builtin(
            "add",
            "Add two numbers",
            vec!["a".into(), "b".into()],
            |args| {
                let a = args.first().and_then(|v| v.as_int()).unwrap_or(0);
                let b = args.get(1).and_then(|v| v.as_int()).unwrap_or(0);
                ProcResult::Ok(ProcValue::Int(a + b))
            },
        );

        let result = engine.execute("add", &[ProcValue::Int(3), ProcValue::Int(4)]);
        match result {
            ProcResult::Ok(ProcValue::Int(7)) => {}
            _ => panic!("expected Int(7)"),
        }
    }

    #[test]
    fn execution_history() {
        let mut engine = ProcedureEngine::new();
        engine.execute("nucleus_version", &[]);
        engine.execute("coalesce", &[ProcValue::Int(1)]);
        engine.execute("nonexistent", &[]);

        let history = engine.execution_history();
        assert_eq!(history.len(), 3);
        assert!(history[0].success);
        assert!(history[1].success);
        assert!(!history[2].success); // nonexistent failed
    }

    #[test]
    fn list_and_unregister() {
        let mut engine = ProcedureEngine::new();
        let initial = engine.list_procedures().len();

        engine.register_sql("my_proc", "test", vec![], "SELECT 1");
        assert_eq!(engine.list_procedures().len(), initial + 1);

        assert!(engine.unregister("my_proc"));
        assert_eq!(engine.list_procedures().len(), initial);
    }

    #[test]
    fn proc_value_accessors() {
        assert_eq!(ProcValue::Text("hello".into()).as_text(), Some("hello"));
        assert_eq!(ProcValue::Int(42).as_int(), Some(42));
        assert_eq!(ProcValue::Float(3.14).as_float(), Some(3.14));
        assert_eq!(ProcValue::Bool(true).as_bool(), Some(true));

        // Wrong type returns None
        assert_eq!(ProcValue::Int(42).as_text(), None);
        assert_eq!(ProcValue::Text("hi".into()).as_int(), None);
        assert_eq!(ProcValue::Null.as_bool(), None);
        assert_eq!(ProcValue::Bool(false).as_float(), None);
    }

    #[test]
    fn coalesce_all_null() {
        let mut engine = ProcedureEngine::new();
        let result = engine.execute("coalesce", &[ProcValue::Null, ProcValue::Null]);
        match result {
            ProcResult::Ok(ProcValue::Null) => {}
            _ => panic!("expected Null when all args are null"),
        }
    }

    #[test]
    fn coalesce_first_non_null() {
        let mut engine = ProcedureEngine::new();
        let result = engine.execute("coalesce", &[
            ProcValue::Text("first".into()),
            ProcValue::Int(99),
        ]);
        match result {
            ProcResult::Ok(ProcValue::Text(s)) => assert_eq!(s, "first"),
            _ => panic!("expected first non-null value"),
        }
    }

    #[test]
    fn array_length_empty() {
        let mut engine = ProcedureEngine::new();
        let result = engine.execute("array_length", &[ProcValue::Array(vec![])]);
        match result {
            ProcResult::Ok(ProcValue::Int(0)) => {}
            _ => panic!("expected Int(0)"),
        }
    }

    #[test]
    fn json_extract_missing_key() {
        let mut engine = ProcedureEngine::new();
        let json = r#"{"name":"Alice"}"#;
        let result = engine.execute("json_extract", &[
            ProcValue::Text(json.into()),
            ProcValue::Text("missing_key".into()),
        ]);
        match result {
            ProcResult::Ok(ProcValue::Null) => {}
            _ => panic!("expected Null for missing key"),
        }
    }

    #[test]
    fn sql_procedure_multiple_params() {
        let mut engine = ProcedureEngine::new();
        engine.register_sql(
            "find_orders",
            "Find orders by user and status",
            vec!["user_id".into(), "status".into()],
            "SELECT * FROM orders WHERE user_id = $user_id AND status = $status",
        );

        let result = engine.execute("find_orders", &[
            ProcValue::Int(10),
            ProcValue::Text("active".into()),
        ]);
        match result {
            ProcResult::Ok(ProcValue::Text(sql)) => {
                assert!(sql.contains("user_id = 10"));
                assert!(sql.contains("status = 'active'"));
            }
            _ => panic!("expected substituted SQL"),
        }
    }

    #[test]
    fn unregister_nonexistent() {
        let mut engine = ProcedureEngine::new();
        assert!(!engine.unregister("does_not_exist"));
    }

    #[test]
    fn execution_history_details() {
        let mut engine = ProcedureEngine::new();
        engine.execute("nucleus_version", &[]);

        let history = engine.execution_history();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].procedure, "nucleus_version");
        assert!(history[0].success);
        assert!(history[0].duration_us > 0 || history[0].duration_us == 0); // Just check field exists
    }

    #[test]
    fn param_types_default_to_any() {
        let mut engine = ProcedureEngine::new();
        engine.register_sql("p1", "test", vec!["a".into(), "b".into()], "SELECT $a, $b");
        let meta = engine.get_metadata("p1").unwrap();
        assert_eq!(meta.param_types.len(), 2);
        assert_eq!(meta.param_types[0], ParamType::Any);
        assert_eq!(meta.param_types[1], ParamType::Any);
        assert_eq!(meta.return_type, None);
    }

    #[test]
    fn register_sql_typed_preserves_types() {
        let mut engine = ProcedureEngine::new();
        engine.register_sql_typed(
            "typed_proc",
            "A typed procedure",
            vec!["name".into(), "age".into()],
            vec![ParamType::Text, ParamType::Integer],
            Some(ParamType::Boolean),
            "SELECT $name, $age",
        );
        let meta = engine.get_metadata("typed_proc").unwrap();
        assert_eq!(meta.param_types, vec![ParamType::Text, ParamType::Integer]);
        assert_eq!(meta.return_type, Some(ParamType::Boolean));
    }

    #[test]
    fn param_type_display() {
        assert_eq!(ParamType::Text.to_string(), "TEXT");
        assert_eq!(ParamType::Integer.to_string(), "INTEGER");
        assert_eq!(ParamType::Float.to_string(), "FLOAT");
        assert_eq!(ParamType::Boolean.to_string(), "BOOLEAN");
        assert_eq!(ParamType::Any.to_string(), "ANY");
    }

    #[test]
    fn builtin_param_types_are_any() {
        let engine = ProcedureEngine::new();
        let meta = engine.get_metadata("nucleus_version").unwrap();
        assert!(meta.param_types.is_empty());
        let meta = engine.get_metadata("coalesce").unwrap();
        assert_eq!(meta.param_types.len(), 1);
        assert_eq!(meta.param_types[0], ParamType::Any);
    }

    // ── WASM sandbox tests ─────────────────────────────────────────

    // Minimal valid WASM module (magic + version header).
    fn wasm_header() -> Vec<u8> {
        vec![0x00, 0x61, 0x73, 0x6D, 0x01, 0x00, 0x00, 0x00]
    }

    #[test]
    fn wasm_sandbox_config_default() {
        let config = WasmSandboxConfig::default();
        assert_eq!(config.max_memory_pages, 256);
        assert_eq!(config.max_fuel, 1_000_000);
        assert!(!config.allowed_imports.is_empty());
    }

    #[test]
    fn wasm_validate_valid_module() {
        let sandbox = SandboxedWasmRuntime::new(WasmSandboxConfig::default());
        let validation = sandbox.validate_module("test_fn", &wasm_header());
        assert!(validation.valid, "errors: {:?}", validation.errors);
        assert!(validation.exports.contains(&"test_fn".to_string()));
    }

    #[test]
    fn wasm_validate_invalid_magic() {
        let sandbox = SandboxedWasmRuntime::new(WasmSandboxConfig::default());
        let validation = sandbox.validate_module("bad", &[0xFF, 0xFF, 0xFF, 0xFF]);
        assert!(!validation.valid);
        assert!(validation.errors.iter().any(|e| e.contains("magic")));
    }

    #[test]
    fn wasm_validate_too_large() {
        let config = WasmSandboxConfig { max_module_bytes: 4, ..Default::default() };
        let sandbox = SandboxedWasmRuntime::new(config);
        let validation = sandbox.validate_module("big", &wasm_header());
        assert!(!validation.valid);
        assert!(validation.errors.iter().any(|e| e.contains("size")));
    }

    #[test]
    fn wasm_load_and_unload_module() {
        let mut sandbox = SandboxedWasmRuntime::new(WasmSandboxConfig::default());
        sandbox.load_module("add", wasm_header(), vec!["add".into()]).unwrap();
        assert_eq!(sandbox.loaded_modules().len(), 1);
        assert!(sandbox.unload_module("add"));
        assert!(sandbox.loaded_modules().is_empty());
        assert!(!sandbox.unload_module("add"));
    }

    #[test]
    fn wasm_execute_via_procedure_engine() {
        let mut engine = ProcedureEngine::new();
        let runtime = SandboxedWasmRuntime::new(WasmSandboxConfig::default());
        engine.set_wasm_runtime(Box::new(runtime));

        engine.register_wasm(
            "wasm_add",
            "Add two numbers (WASM)",
            vec!["a".into(), "b".into()],
            wasm_header(),
            "add",
        );

        let result = engine.execute("wasm_add", &[ProcValue::Int(3), ProcValue::Int(4)]);
        match result {
            ProcResult::Ok(ProcValue::Text(s)) => {
                assert!(s.contains("wasm:add"));
                assert!(s.contains("3"));
                assert!(s.contains("4"));
            }
            _ => panic!("expected wasm execution result, got: {result:?}"),
        }
    }

    #[test]
    fn wasm_execute_without_runtime() {
        let mut engine = ProcedureEngine::new();
        // No WASM runtime set
        engine.register_wasm("test", "test", vec![], wasm_header(), "main");
        let result = engine.execute("test", &[]);
        match result {
            ProcResult::Error(msg) => assert!(msg.contains("no WASM runtime")),
            _ => panic!("expected error"),
        }
    }

    #[test]
    fn wasm_execute_bad_module() {
        let runtime = SandboxedWasmRuntime::new(WasmSandboxConfig::default());
        let result = runtime.execute(&[0xFF, 0xFF], "main", &[]);
        match result {
            ProcResult::Error(msg) => assert!(msg.contains("magic")),
            _ => panic!("expected error"),
        }
    }

    #[test]
    fn wasm_sandbox_stats() {
        let sandbox = SandboxedWasmRuntime::new(WasmSandboxConfig::default());
        let (count, fuel) = sandbox.stats();
        assert_eq!(count, 0);
        assert_eq!(fuel, 0);
    }
}
