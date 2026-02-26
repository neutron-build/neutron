use std::fs;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime};

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "neutron", about = "Neutron Rust framework CLI", version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the development server with auto-restart on file changes
    Dev {
        /// Port to listen on
        #[arg(short, long, default_value_t = 3000)]
        port: u16,
    },
    /// Create a new Neutron project
    New {
        /// Project name (also used as directory name)
        name: String,
        /// Project template: api (default), grpc, graphql, jobs, full
        #[arg(short, long, default_value = "api")]
        template: String,
    },
    /// Build a release binary with size reporting
    Build {
        /// Additional cargo build flags (e.g. --features "jemalloc")
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Run cargo check (faster than a full build)
    Check,
    /// Print all registered routes extracted from source comments
    Routes,
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Dev { port }          => cmd_dev(port),
        Commands::New { name, template } => cmd_new(&name, &template),
        Commands::Build { args }        => cmd_build(args),
        Commands::Check                 => cmd_check(),
        Commands::Routes                => cmd_routes(),
    }
}

// ===========================================================================
// neutron new
// ===========================================================================

fn cmd_new(name: &str, template: &str) {
    // Validate project name
    if name.is_empty() {
        eprintln!("Error: project name cannot be empty");
        std::process::exit(1);
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        eprintln!("Error: project name must contain only alphanumeric characters, hyphens, or underscores");
        std::process::exit(1);
    }

    let project_dir = Path::new(name);
    if project_dir.exists() {
        eprintln!("Error: directory '{name}' already exists");
        std::process::exit(1);
    }

    let tpl = match template {
        "api" | "grpc" | "graphql" | "jobs" | "full" => template,
        other => {
            eprintln!("Error: unknown template '{other}'. Choices: api, grpc, graphql, jobs, full");
            std::process::exit(1);
        }
    };

    println!("Creating new Neutron project: {name}  (template: {tpl})");

    // Create directory structure
    let src_subdir = match tpl {
        "grpc" | "rpc" => "src/services",
        _ => "src/routes",
    };
    fs::create_dir_all(project_dir.join(src_subdir)).unwrap_or_else(|e| {
        eprintln!("Error creating directories: {e}");
        std::process::exit(1);
    });

    let extra_deps = match tpl {
        "grpc"    => "neutron-grpc = { path = \"../neutron/crates/neutron-grpc\" }\n",
        "graphql" => "neutron-graphql = { path = \"../neutron/crates/neutron-graphql\" }\n",
        "jobs"    => "neutron-jobs = { path = \"../neutron/crates/neutron-jobs\" }\n",
        "full"    => concat!(
            "neutron-grpc     = { path = \"../neutron/crates/neutron-grpc\" }\n",
            "neutron-graphql  = { path = \"../neutron/crates/neutron-graphql\" }\n",
            "neutron-jobs     = { path = \"../neutron/crates/neutron-jobs\" }\n",
        ),
        _ => "",
    };

    let cargo_toml = format!(
        "[package]\nname = \"{name}\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n\
         [dependencies]\n\
         # path dep until neutron is published on crates.io\n\
         neutron = {{ path = \"../neutron/crates/neutron\", features = [\"full\"] }}\n\
         {extra_deps}\
         tokio = {{ version = \"1\", features = [\"full\"] }}\n\
         serde = {{ version = \"1\", features = [\"derive\"] }}\n\
         serde_json = \"1\"\n\
         tracing-subscriber = {{ version = \"0.3\", features = [\"env-filter\"] }}\n"
    );

    let main_rs = match tpl {
        "grpc" => TEMPLATE_GRPC_MAIN.replace("{name}", name),
        "graphql" => TEMPLATE_GRAPHQL_MAIN.replace("{name}", name),
        "jobs" => TEMPLATE_JOBS_MAIN.replace("{name}", name),
        _ => TEMPLATE_API_MAIN.replace("{name}", name),
    };

    let routes_file = match tpl {
        "grpc"    => ("src/services/mod.rs", TEMPLATE_GRPC_SERVICE),
        "graphql" => ("src/routes/mod.rs",   TEMPLATE_GRAPHQL_ROUTES),
        "jobs"    => ("src/routes/mod.rs",   TEMPLATE_JOBS_ROUTES),
        _         => ("src/routes/mod.rs",   TEMPLATE_API_ROUTES),
    };

    let gitignore = "/target\n";

    let files: &[(&str, &str)] = &[
        ("Cargo.toml",      &cargo_toml),
        ("src/main.rs",     &main_rs),
        (routes_file.0,     routes_file.1),
        (".gitignore",       gitignore),
    ];

    for (path, content) in files {
        let full_path = project_dir.join(path);
        fs::write(&full_path, content).unwrap_or_else(|e| {
            eprintln!("Error writing {path}: {e}");
            std::process::exit(1);
        });
        println!("  Created {name}/{path}");
    }

    println!();
    println!("Next steps:");
    println!("  cd {name}");
    println!("  neutron dev          # start with auto-reload");
    println!("  neutron build        # release binary");
    println!("  neutron routes       # list registered routes");
    println!();
    println!("Then visit http://localhost:3000");
}

// ---------------------------------------------------------------------------
// Templates
// ---------------------------------------------------------------------------

const TEMPLATE_API_MAIN: &str = r#"use neutron::prelude::*;

mod routes;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "info".into()))
        .init();

    let router = Router::new()
        .middleware(Logger)
        .middleware(RequestId::new())
        .middleware(Helmet::default())
        .middleware(Cors::new().allow_any_origin().allow_any_method())
        .get("/",       || async { "Hello from Neutron!" })
        .get("/health", || async { Json(serde_json::json!({ "status": "ok" })) })
        .nest("/api", routes::api_router());

    Neutron::new().router(router).listen("0.0.0.0:3000".parse()?).await?;
    Ok(())
}
"#;

const TEMPLATE_API_ROUTES: &str = r#"use neutron::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Item { id: u64, name: String }

pub fn api_router() -> Router {
    Router::new()
        .get("/items",     list_items)
        .get("/items/:id", get_item)
        .post("/items",    create_item)
}

async fn list_items() -> Json<Vec<Item>> {
    Json(vec![
        Item { id: 1, name: "First".into() },
        Item { id: 2, name: "Second".into() },
    ])
}

async fn get_item(Path(id): Path<u64>) -> impl IntoResponse {
    Json(Item { id, name: format!("Item {id}") })
}

async fn create_item(Json(item): Json<Item>) -> impl IntoResponse {
    (StatusCode::CREATED, Json(item))
}
"#;

const TEMPLATE_GRPC_MAIN: &str = r#"use neutron::prelude::*;

mod services;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let router = Router::new()
        .get("/health", || async { "ok" })
        .nest("/", services::grpc_router().into_router());

    Neutron::new().router(router).listen("0.0.0.0:3000".parse()?).await?;
    Ok(())
}
"#;

const TEMPLATE_GRPC_SERVICE: &str = r#"use neutron_grpc::{GrpcRequest, GrpcResponse, GrpcRouter};

pub fn grpc_router() -> GrpcRouter {
    GrpcRouter::new()
        .method("/example.Example/Ping", ping)
}

async fn ping(GrpcRequest(payload): GrpcRequest) -> GrpcResponse {
    GrpcResponse::ok(payload) // echo back
}
"#;

const TEMPLATE_GRAPHQL_MAIN: &str = r#"use neutron::prelude::*;
use neutron_graphql::{graphql_handler, ExecutableSchema, GraphQlRequest, GraphQlResponse};
use std::sync::Arc;
use std::pin::Pin;
use std::future::Future;

mod routes;

struct MySchema;

impl ExecutableSchema for MySchema {
    fn execute(
        self: Arc<Self>,
        req: GraphQlRequest,
    ) -> Pin<Box<dyn Future<Output = GraphQlResponse> + Send + 'static>> {
        Box::pin(async move {
            GraphQlResponse::ok(serde_json::json!({ "echo": req.query }))
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let handler = graphql_handler(MySchema);
    let router  = Router::new()
        .get("/graphql",  handler.clone())
        .post("/graphql", handler);

    Neutron::new().router(router).listen("0.0.0.0:3000".parse()?).await?;
    Ok(())
}
"#;

const TEMPLATE_GRAPHQL_ROUTES: &str = r#"// GraphQL schema and resolvers go here.
// See src/main.rs for the schema registration.
"#;

const TEMPLATE_JOBS_MAIN: &str = r#"use neutron::prelude::*;
use neutron_jobs::{Job, JobQueue, JobResult, JobWorker};
use std::sync::Arc;
use serde::{Deserialize, Serialize};

mod routes;

#[derive(Serialize, Deserialize)]
struct SendEmail { to: String, subject: String }

async fn send_email(Job(email): Job<SendEmail>) -> JobResult {
    println!("Sending email to {} — {}", email.to, email.subject);
    JobResult::Ok
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let queue = Arc::new(JobQueue::new());

    // Enqueue a test job on startup.
    queue.enqueue("send_email", serde_json::to_vec(&SendEmail {
        to: "user@example.com".into(),
        subject: "Welcome!".into(),
    }).unwrap());

    let worker = JobWorker::new(Arc::clone(&queue))
        .job("send_email", send_email)
        .concurrency(4);

    tokio::spawn(worker.run());

    let router = Router::new()
        .get("/health", || async { "ok" });

    Neutron::new().router(router).listen("0.0.0.0:3000".parse()?).await?;
    Ok(())
}
"#;

const TEMPLATE_JOBS_ROUTES: &str = r#"// HTTP routes can enqueue jobs via State<Arc<JobQueue>>.
// See src/main.rs for the worker setup.
"#;

// ===========================================================================
// neutron build
// ===========================================================================

fn cmd_build(extra_args: Vec<String>) {
    if !Path::new("Cargo.toml").exists() {
        eprintln!("Error: no Cargo.toml found. Run from your project root.");
        std::process::exit(1);
    }

    println!("Building release binary...\n");

    let mut args = vec!["build", "--release"];
    let extra: Vec<&str> = extra_args.iter().map(String::as_str).collect();
    args.extend_from_slice(&extra);

    let status = Command::new("cargo")
        .args(&args)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .unwrap_or_else(|e| {
            eprintln!("Failed to run `cargo build`: {e}");
            std::process::exit(1);
        });

    if !status.success() {
        eprintln!("\nBuild failed.");
        std::process::exit(1);
    }

    // Report binary size.
    if let Ok(meta) = std::fs::metadata(binary_path()) {
        let bytes = meta.len();
        println!("\nBinary size: {} ({:.1} MB)", bytes, bytes as f64 / 1_048_576.0);
        println!("Location:    {}", binary_path().display());
    }
}

fn binary_path() -> std::path::PathBuf {
    // Read package name from Cargo.toml (naive search — no TOML parser dep).
    let toml = std::fs::read_to_string("Cargo.toml").unwrap_or_default();
    let name = toml
        .lines()
        .find(|l| l.trim_start().starts_with("name"))
        .and_then(|l| l.split('"').nth(1))
        .unwrap_or("app")
        .to_string();

    let exe = if cfg!(windows) { format!("{name}.exe") } else { name };
    std::path::Path::new("target/release").join(exe)
}

// ===========================================================================
// neutron check
// ===========================================================================

fn cmd_check() {
    if !Path::new("Cargo.toml").exists() {
        eprintln!("Error: no Cargo.toml found. Run from your project root.");
        std::process::exit(1);
    }

    println!("Checking project...\n");

    let status = Command::new("cargo")
        .args(["check"])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .unwrap_or_else(|e| {
            eprintln!("Failed to run `cargo check`: {e}");
            std::process::exit(1);
        });

    if !status.success() {
        std::process::exit(1);
    }
}

// ===========================================================================
// neutron routes
// ===========================================================================

/// Extract and print routes by scanning `src/` for router registration calls.
///
/// Looks for patterns like `.get("...", ...)`, `.post("...", ...)`, etc. in
/// source files and prints a table.  This is a heuristic scan — it does not
/// execute the binary — so it works offline and instantly.
fn cmd_routes() {
    if !Path::new("src").is_dir() {
        eprintln!("Error: no src/ directory found. Run from your project root.");
        std::process::exit(1);
    }

    let methods = ["get", "post", "put", "patch", "delete", "head", "options"];
    let files   = collect_mtimes("src");

    let mut routes: Vec<(String, String, String)> = Vec::new(); // (method, path, file)

    for (file_path, _) in &files {
        if !file_path.ends_with(".rs") {
            continue;
        }
        let Ok(src) = std::fs::read_to_string(file_path) else { continue };
        for line in src.lines() {
            let trimmed = line.trim();
            for method in methods {
                let needle = format!(".{method}(\"");
                if let Some(rest) = trimmed.strip_prefix(&needle) {
                    if let Some(end) = rest.find('"') {
                        let route_path = &rest[..end];
                        routes.push((
                            method.to_uppercase(),
                            route_path.to_string(),
                            file_path.clone(),
                        ));
                    }
                }
            }
        }
    }

    if routes.is_empty() {
        println!("No routes found in src/  (scan looks for .get(\"...\"), .post(\"...\"), etc.)");
        return;
    }

    routes.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));

    let method_w = routes.iter().map(|(m, _, _)| m.len()).max().unwrap_or(6).max(6);
    let path_w   = routes.iter().map(|(_, p, _)| p.len()).max().unwrap_or(4).max(4);

    println!("\n{:<method_w$}  {:<path_w$}  FILE", "METHOD", "PATH");
    println!("{}", "-".repeat(method_w + path_w + 20));
    for (method, path, file) in &routes {
        println!("{:<method_w$}  {:<path_w$}  {file}", method, path);
    }
    println!();
    println!("{} route(s) found.", routes.len());
}

// ===========================================================================
// neutron dev
// ===========================================================================

fn cmd_dev(port: u16) {
    // Verify we're in a Cargo project
    if !Path::new("Cargo.toml").exists() {
        eprintln!("Error: no Cargo.toml found in current directory");
        eprintln!("Run this command from your project root, or use `neutron new` first.");
        std::process::exit(1);
    }

    println!("Neutron dev server starting (port {port})...");
    println!("Watching src/ for changes. Press Ctrl+C to stop.\n");

    let mut child = spawn_cargo(port);
    let mut last_snapshot = collect_snapshot();

    loop {
        std::thread::sleep(Duration::from_millis(500));

        let current = collect_snapshot();
        if current != last_snapshot {
            println!("\n--- File change detected, restarting... ---\n");
            kill_child(&mut child);
            child = spawn_cargo(port);
            last_snapshot = current;
        }

        // Check if child exited (compilation error, etc.)
        match child.try_wait() {
            Ok(Some(status)) => {
                if !status.success() {
                    eprintln!("Process exited with {status}. Waiting for file changes...");
                    // Wait for a change before retrying
                    loop {
                        std::thread::sleep(Duration::from_millis(500));
                        let current = collect_snapshot();
                        if current != last_snapshot {
                            println!("\n--- File change detected, restarting... ---\n");
                            child = spawn_cargo(port);
                            last_snapshot = current;
                            break;
                        }
                    }
                }
            }
            Ok(None) => {} // still running
            Err(e) => {
                eprintln!("Error checking process status: {e}");
                break;
            }
        }
    }
}

fn spawn_cargo(port: u16) -> Child {
    Command::new("cargo")
        .args(["run"])
        .env("PORT", port.to_string())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap_or_else(|e| {
            eprintln!("Failed to run `cargo run`: {e}");
            std::process::exit(1);
        })
}

fn kill_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// Collect a snapshot of file modification times for all watched paths.
///
/// Watches: `src/` directory (recursively), `Cargo.toml`, and `Cargo.lock`.
fn collect_snapshot() -> Vec<(String, SystemTime)> {
    let mut result = collect_mtimes("src");
    // Also watch root config files
    for file in &["Cargo.toml", "Cargo.lock"] {
        if let Ok(meta) = fs::metadata(file) {
            if let Ok(mtime) = meta.modified() {
                result.push((file.to_string(), mtime));
            }
        }
    }
    result.sort_by(|a, b| a.0.cmp(&b.0));
    result
}

/// Collect modification times of all files under a directory.
/// Returns a sorted Vec of (path, mtime) for comparison.
fn collect_mtimes(dir: &str) -> Vec<(String, SystemTime)> {
    let mut result = Vec::new();
    if let Ok(entries) = walk_dir(Path::new(dir)) {
        for path in entries {
            if let Ok(meta) = fs::metadata(&path) {
                if let Ok(mtime) = meta.modified() {
                    result.push((path, mtime));
                }
            }
        }
    }
    result.sort_by(|a, b| a.0.cmp(&b.0));
    result
}

/// Recursively list all files under a directory.
fn walk_dir(dir: &Path) -> std::io::Result<Vec<String>> {
    let mut files = Vec::new();
    if !dir.is_dir() {
        return Ok(files);
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            files.extend(walk_dir(&path)?);
        } else {
            files.push(path.to_string_lossy().into_owned());
        }
    }
    Ok(files)
}
