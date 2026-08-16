//! Rust executor for the Nucleus live data-model conformance spec.
//!
//! Reads ../../spec.json, runs every case against a live engine through the
//! real in-repo Rust client (`neutron-nucleus`), and prints one JSON result
//! document to stdout. It asserts nothing a mock could assert: only that a call
//! reaches the engine, is accepted over the wire, and comes back with the right
//! value.
//!
//! ```text
//! NEUTRON_TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:55432/postgres \
//!     cargo run --quiet
//! ```
//!
//! Exit codes: 0 all cases behaved as the spec says, 1 otherwise. An `xfail`
//! case that PASSES is a failure — otherwise a fix lands and the note
//! explaining why the case is expected to fail quietly becomes a lie.
//!
//! Everything on stdout is the report. Diagnostics go to stderr, because the
//! orchestrator parses stdout.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::time::Duration;

use neutron_nucleus::{NucleusClient, NucleusConfig, SslMode};
use rand::Rng;
use serde::Deserialize;
use serde_json::{json, Value};

/// The instant the spec's time-series millisecond offsets are measured from:
/// 2026-08-11T12:00:00Z. Fixed so the cases are deterministic and comparable
/// across SDKs.
const TS_BASE_MS: i64 = 1_786_795_200_000;

/// Bounds a single op. A hang is a finding, but a hung run reports nothing at
/// all, so it is turned into a failure with a name attached.
const STEP_TIMEOUT: Duration = Duration::from_secs(60);

// ── spec types ───────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct Step {
    op: String,
    #[serde(default)]
    args: Vec<Value>,
    #[serde(default)]
    bind: Option<String>,
    #[serde(default)]
    expect: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct XFail {
    #[serde(default)]
    reason: String,
    #[serde(default)]
    sdks: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct SpecCase {
    id: String,
    #[serde(default)]
    model: String,
    #[serde(default)]
    xfail: Option<XFail>,
    steps: Vec<Step>,
}

#[derive(Debug, Deserialize)]
struct Spec {
    #[serde(rename = "specVersion", default)]
    spec_version: u32,
    cases: Vec<SpecCase>,
}

#[derive(Debug, Deserialize)]
struct Unsupported {
    #[serde(default)]
    cases: HashMap<String, String>,
}

/// An op the Rust SDK has no surface for. Undeclared it is a failure; declared
/// in unsupported.json with a reason it is `unsupported`.
#[derive(Debug)]
struct NoMapping(String);

type StepResult = Result<Value, StepError>;

#[derive(Debug)]
enum StepError {
    Unsupported(NoMapping),
    Failed(String),
}

impl StepError {
    fn failed(msg: impl Into<String>) -> Self {
        StepError::Failed(msg.into())
    }
}

// ── argument resolution ──────────────────────────────────────────────────────

/// "@name" is a per-case unique fixture (stable within a case, unique across
/// runs); "$name" is a value bound by an earlier step; anything else is a
/// literal.
fn resolve(
    v: &Value,
    fixtures: &mut HashMap<String, String>,
    bound: &HashMap<String, Value>,
) -> Result<Value, String> {
    match v {
        Value::String(s) => {
            if let Some(name) = s.strip_prefix('$') {
                return bound
                    .get(name)
                    .cloned()
                    .ok_or_else(|| format!("step references ${name} before it was bound"));
            }
            Ok(Value::String(expand_fixtures(s, fixtures)))
        }
        Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for i in items {
                out.push(resolve(i, fixtures, bound)?);
            }
            Ok(Value::Array(out))
        }
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, val) in map {
                out.insert(k.clone(), resolve(val, fixtures, bound)?);
            }
            Ok(Value::Object(out))
        }
        other => Ok(other.clone()),
    }
}

fn expand_fixtures(s: &str, fixtures: &mut HashMap<String, String>) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == '@' {
            let start = i + 1;
            let mut end = start;
            while end < bytes.len()
                && (bytes[end].is_ascii_alphanumeric() || bytes[end] == '_')
                && !(end == start && bytes[end].is_ascii_digit())
            {
                end += 1;
            }
            if end > start {
                let name: String = bytes[start..end].iter().collect();
                let val = fixtures
                    .entry(name.clone())
                    .or_insert_with(|| format!("{name}_{}", rand_hex(5)))
                    .clone();
                out.push_str(&val);
                i = end;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    out
}

fn rand_hex(n: usize) -> String {
    let mut rng = rand::thread_rng();
    (0..n).map(|_| format!("{:02x}", rng.gen::<u8>())).collect()
}

// ── expectations ─────────────────────────────────────────────────────────────

/// Applies the spec's expectation vocabulary to one result. Semantics match the
/// Python executor exactly, including that `nonEmpty` is truthiness.
fn check(result: &Value, expect: &Value) -> Result<(), String> {
    let mut actual = result.clone();

    if let Some(k) = expect.get("key") {
        let key = k.as_str().unwrap_or_default();
        if actual.is_null() {
            return Err(format!("expected a map with key {key}, got null"));
        }
        let obj = actual
            .as_object()
            .ok_or_else(|| format!("expected a map with key {key}, got {}", show(&actual)))?;
        actual = obj
            .get(key)
            .cloned()
            .ok_or_else(|| format!("key {key} is absent from {}", show(&actual)))?;
    }

    if let Some(i) = expect.get("index").and_then(|v| v.as_i64()) {
        let list = actual
            .as_array()
            .ok_or_else(|| format!("expected a list to index, got {}", show(&actual)))?;
        actual = list
            .get(i as usize)
            .cloned()
            .ok_or_else(|| format!("index {i} out of range for {} elements", list.len()))?;
    }

    if expect.get("jsonDecode").and_then(|v| v.as_bool()) == Some(true) {
        if let Some(s) = actual.as_str() {
            actual = serde_json::from_str(s)
                .map_err(|e| format!("jsonDecode failed on {s:?}: {e}"))?;
        }
    }

    if expect.get("notNull").and_then(|v| v.as_bool()) == Some(true) && actual.is_null() {
        return Err("expected a value, got null".into());
    }

    if expect.get("isNull").and_then(|v| v.as_bool()) == Some(true) && !actual.is_null() {
        return Err(format!("expected null, got {}", show(&actual)));
    }

    if expect.get("nonEmpty").and_then(|v| v.as_bool()) == Some(true) && !truthy(&actual) {
        return Err(format!(
            "expected a non-empty collection, got {}",
            show(&actual)
        ));
    }

    if let Some(want) = expect.get("length").and_then(|v| v.as_i64()) {
        let n = length_of(&actual)?;
        if n as i64 != want {
            return Err(format!(
                "expected {want} elements, got {n}: {}",
                show(&actual)
            ));
        }
    }

    if let Some(want) = expect.get("type").and_then(|v| v.as_str()) {
        check_type(&actual, want)?;
    }

    if let Some(want) = expect.get("equals") {
        if !json_equal(&actual, want) {
            return Err(format!(
                "expected {}, got {}",
                show(want),
                show(&actual)
            ));
        }
    }

    Ok(())
}

fn check_type(actual: &Value, want: &str) -> Result<(), String> {
    let ok = match want {
        "list" => actual.is_array(),
        "map" => actual.is_object(),
        "string" => actual.is_str(),
        "int" => actual.is_i64() || actual.is_u64(),
        "float" => actual.is_number(),
        "bool" => actual.is_boolean(),
        // The wire carries bytes as base64 text; a decoder that produced a byte
        // array would still be a string here, so both read as `bytes`.
        "bytes" => actual.is_str() || actual.is_array(),
        other => return Err(format!("unknown expectation type {other:?}")),
    };
    if ok {
        Ok(())
    } else {
        Err(format!("expected {want}, got {}", show(actual)))
    }
}

fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(true),
    }
}

fn length_of(v: &Value) -> Result<usize, String> {
    match v {
        Value::Array(a) => Ok(a.len()),
        Value::Object(o) => Ok(o.len()),
        Value::String(s) => Ok(s.chars().count()),
        other => Err(format!("expected a collection, got {}", show(other))),
    }
}

/// Floats compare loosely; everything else exactly. Integers and floats compare
/// across the boundary, because which side of the wire produced a whole number
/// is not what any of these cases is testing.
fn json_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => match (x.as_f64(), y.as_f64()) {
            (Some(p), Some(q)) => (p - q).abs() < 1e-9,
            _ => x == y,
        },
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| json_equal(p, q))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .all(|(k, v)| y.get(k).map(|w| json_equal(v, w)).unwrap_or(false))
        }
        _ => a == b,
    }
}

fn show(v: &Value) -> String {
    let s = v.to_string();
    if s.len() > 200 {
        format!("{}…", &s[..200])
    } else {
        s
    }
}

trait IsStr {
    fn is_str(&self) -> bool;
}
impl IsStr for Value {
    fn is_str(&self) -> bool {
        self.is_string()
    }
}

// ── main ─────────────────────────────────────────────────────────────────────

fn here() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn spec_path() -> PathBuf {
    here().join("../../spec.json")
}

/// Parse a postgres URL into the client's config. The client has no `from_url`,
/// so the executor does it rather than inventing one on the SDK's behalf.
fn config_from_url(url: &str) -> Result<NucleusConfig, String> {
    let rest = url
        .strip_prefix("postgresql://")
        .or_else(|| url.strip_prefix("postgres://"))
        .ok_or("URL must start with postgres:// or postgresql://")?;

    let (authority, dbname) = match rest.split_once('/') {
        Some((a, d)) => (a, d.split('?').next().unwrap_or("postgres")),
        None => (rest, "postgres"),
    };

    let (userinfo, hostport) = match authority.rsplit_once('@') {
        Some((u, h)) => (Some(u), h),
        None => (None, authority),
    };

    let (user, password) = match userinfo {
        Some(ui) => match ui.split_once(':') {
            Some((u, p)) => (u.to_string(), p.to_string()),
            None => (ui.to_string(), String::new()),
        },
        None => ("postgres".to_string(), String::new()),
    };

    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) => (
            h.to_string(),
            p.parse::<u16>().map_err(|_| format!("bad port in {url}"))?,
        ),
        None => (hostport.to_string(), 5432),
    };

    // Left on the DEFAULT sslmode (Prefer) on purpose: this executor should
    // connect the way a real user's first program does, so a regression in the
    // default path fails the suite rather than hiding behind an override.
    //
    // That default could not connect at all until 2026-08-16. Nucleus answers
    // SSLRequest with 'S' and presents a self-signed certificate; the client's
    // rustls verifier correctly rejects it, and `Prefer` did not then retry in
    // plaintext the way libpq's `prefer` does — so it behaved exactly like
    // `require` and every Rust user's first connection failed on a mode whose
    // own documentation says "otherwise plaintext". Fixed in the client; this
    // executor is the regression test.
    Ok(NucleusConfig::new(host, port, dbname.to_string())
        .user(user)
        .password(password)
        .sslmode(SslMode::Prefer))
}

#[tokio::main]
async fn main() {
    std::process::exit(run().await);
}

async fn run() -> i32 {
    let url = match std::env::var("NEUTRON_TEST_DATABASE_URL") {
        Ok(u) if !u.is_empty() => u,
        _ => {
            eprintln!(
                "::error::NEUTRON_TEST_DATABASE_URL is not set. This suite is only \
                 meaningful against a live engine; refusing to report a green run \
                 for zero executed cases."
            );
            return 1;
        }
    };

    let spec: Spec = match std::fs::read(spec_path()).map(|b| serde_json::from_slice(&b)) {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            eprintln!("::error::cannot parse spec.json: {e}");
            return 1;
        }
        Err(e) => {
            eprintln!("::error::cannot read {}: {e}", spec_path().display());
            return 1;
        }
    };

    let unsupported: Unsupported = match std::fs::read(here().join("unsupported.json")) {
        Ok(b) => match serde_json::from_slice(&b) {
            Ok(u) => u,
            Err(e) => {
                eprintln!("::error::cannot parse unsupported.json: {e}");
                return 1;
            }
        },
        Err(_) => Unsupported {
            cases: HashMap::new(),
        },
    };

    let config = match config_from_url(&url) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("::error::{e}");
            return 1;
        }
    };

    let client = match NucleusClient::connect(config).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("::error::cannot connect to {url}: {e}");
            return 1;
        }
    };

    let mut results = Vec::new();
    for case in &spec.cases {
        let outcome = run_case(case, &client, &url).await;

        // An xfail may be scoped to named SDKs: some engine defects are only
        // reachable through one driver.
        let xfail_applies = match &case.xfail {
            Some(x) => match &x.sdks {
                Some(list) => list.iter().any(|s| s == "rust"),
                None => true,
            },
            None => false,
        };

        let (status, detail) = match outcome {
            Ok(()) if xfail_applies => (
                "xpass",
                "case is marked xfail but passed — the underlying bug is fixed \
                 and the xfail note is now false"
                    .to_string(),
            ),
            Ok(()) => ("pass", String::new()),
            Err(StepError::Unsupported(NoMapping(op))) => {
                match unsupported.cases.get(&case.id) {
                    Some(reason) => ("unsupported", reason.clone()),
                    None => (
                        "fail",
                        format!(
                            "no Rust mapping for op {op}, and the case is not declared \
                             unsupported in unsupported.json"
                        ),
                    ),
                }
            }
            Err(StepError::Failed(msg)) if xfail_applies => (
                "xfail",
                format!(
                    "{} ({msg})",
                    case.xfail.as_ref().map(|x| x.reason.as_str()).unwrap_or("")
                ),
            ),
            Err(StepError::Failed(msg)) => ("fail", msg),
        };

        if status == "fail" || status == "xpass" {
            eprintln!("::error::{}: {status} — {detail}", case.id);
        }

        let mut entry = serde_json::Map::new();
        entry.insert("id".into(), json!(case.id));
        entry.insert("model".into(), json!(case.model));
        entry.insert("status".into(), json!(status));
        if !detail.is_empty() {
            entry.insert("detail".into(), json!(detail));
        }
        results.push(Value::Object(entry));
    }

    let failed = results
        .iter()
        .filter(|r| {
            matches!(
                r.get("status").and_then(|s| s.as_str()),
                Some("fail") | Some("xpass")
            )
        })
        .count();

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "sdk": "rust",
            "specVersion": spec.spec_version,
            "cases": results
        }))
        .unwrap()
    );

    let mut summary = String::new();
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for r in &results {
        *counts
            .entry(r.get("status").and_then(|s| s.as_str()).unwrap_or("?"))
            .or_default() += 1;
    }
    let mut keys: Vec<_> = counts.keys().copied().collect();
    keys.sort_unstable();
    for k in keys {
        let _ = write!(summary, "{k}={} ", counts[k]);
    }
    eprintln!("rust: {summary}");

    if failed > 0 {
        1
    } else {
        0
    }
}

async fn run_case(case: &SpecCase, client: &NucleusClient, url: &str) -> Result<(), StepError> {
    let mut fixtures: HashMap<String, String> = HashMap::new();
    let mut bound: HashMap<String, Value> = HashMap::new();

    for (i, step) in case.steps.iter().enumerate() {
        let mut args = Vec::with_capacity(step.args.len());
        for a in &step.args {
            args.push(
                resolve(a, &mut fixtures, &bound)
                    .map_err(|e| StepError::failed(format!("step {i} ({}): {e}", step.op)))?,
            );
        }

        let call = ops::call(client, url, &step.op, &args);
        let result = match tokio::time::timeout(STEP_TIMEOUT, call).await {
            Err(_) => {
                return Err(StepError::failed(format!(
                    "step {i} ({}): timed out after {}s",
                    step.op,
                    STEP_TIMEOUT.as_secs()
                )))
            }
            Ok(r) => r,
        };

        let value = match result {
            Ok(v) => v,
            Err(StepError::Unsupported(u)) => return Err(StepError::Unsupported(u)),
            Err(StepError::Failed(msg)) => {
                return Err(StepError::failed(format!("step {i} ({}): {msg}", step.op)))
            }
        };

        if let Some(name) = &step.bind {
            bound.insert(name.clone(), value.clone());
        }
        if let Some(expect) = &step.expect {
            check(&value, expect)
                .map_err(|e| StepError::failed(format!("step {i} ({}): {e}", step.op)))?;
        }
    }
    Ok(())
}

mod ops;
