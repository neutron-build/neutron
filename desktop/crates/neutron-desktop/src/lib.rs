mod bridge;
mod ipc;
mod nucleus_state;
mod window;

pub use bridge::{create_protocol_handler, Request, Response};
pub use nucleus_state::{NucleusError, NucleusState};
#[cfg(feature = "nucleus-embedded")]
pub use nucleus_state::NucleusQueryResult;
pub use window::WindowConfig;

use tauri::Manager;
use std::sync::Arc;

/// Builder for configuring and launching a Neutron Desktop application.
///
/// Routes frontend `fetch()` calls through the `neutron://` protocol to the
/// Rust middleware pipeline — no open TCP port, no network attack surface.
pub struct NeutronDesktopBuilder {
    title: String,
    width: f64,
    height: f64,
    nucleus_enabled: bool,
    nucleus_data_dir: Option<std::path::PathBuf>,
    routes: Vec<Route>,
    plugins: Vec<Box<dyn FnOnce(tauri::Builder<tauri::Wry>) -> tauri::Builder<tauri::Wry>>>,
}

struct Route {
    method: http::Method,
    path: String,
    handler: Box<dyn Fn(bridge::Request) -> bridge::Response + Send + Sync>,
}

impl NeutronDesktopBuilder {
    pub fn new() -> Self {
        Self {
            title: "Neutron App".to_string(),
            width: 1200.0,
            height: 800.0,
            nucleus_enabled: false,
            nucleus_data_dir: None,
            routes: Vec::new(),
            plugins: Vec::new(),
        }
    }

    /// Enable embedded Nucleus database (in-process, zero IPC overhead).
    pub fn nucleus_embedded(mut self) -> Self {
        self.nucleus_enabled = true;
        self
    }

    /// Set custom data directory for Nucleus storage.
    pub fn nucleus_data_dir(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.nucleus_data_dir = Some(path.into());
        self
    }

    /// Configure the main window.
    pub fn window(mut self, f: impl FnOnce(&mut WindowConfig)) -> Self {
        let mut config = WindowConfig {
            title: self.title.clone(),
            width: self.width,
            height: self.height,
            resizable: true,
            decorations: true,
            transparent: false,
            fullscreen: false,
            min_width: None,
            min_height: None,
        };
        f(&mut config);
        self.title = config.title;
        self.width = config.width;
        self.height = config.height;
        self
    }

    /// Register a GET route handler.
    pub fn get(
        mut self,
        path: &str,
        handler: impl Fn(bridge::Request) -> bridge::Response + Send + Sync + 'static,
    ) -> Self {
        self.routes.push(Route {
            method: http::Method::GET,
            path: path.to_string(),
            handler: Box::new(handler),
        });
        self
    }

    /// Register a POST route handler.
    pub fn post(
        mut self,
        path: &str,
        handler: impl Fn(bridge::Request) -> bridge::Response + Send + Sync + 'static,
    ) -> Self {
        self.routes.push(Route {
            method: http::Method::POST,
            path: path.to_string(),
            handler: Box::new(handler),
        });
        self
    }

    /// Register a PUT route handler.
    pub fn put(
        mut self,
        path: &str,
        handler: impl Fn(bridge::Request) -> bridge::Response + Send + Sync + 'static,
    ) -> Self {
        self.routes.push(Route {
            method: http::Method::PUT,
            path: path.to_string(),
            handler: Box::new(handler),
        });
        self
    }

    /// Register a DELETE route handler.
    pub fn delete(
        mut self,
        path: &str,
        handler: impl Fn(bridge::Request) -> bridge::Response + Send + Sync + 'static,
    ) -> Self {
        self.routes.push(Route {
            method: http::Method::DELETE,
            path: path.to_string(),
            handler: Box::new(handler),
        });
        self
    }

    /// Add a Tauri plugin.
    pub fn plugin<P: tauri::plugin::Plugin<tauri::Wry> + 'static>(mut self, plugin: P) -> Self {
        self.plugins.push(Box::new(move |builder| {
            builder.plugin(plugin)
        }));
        self
    }

    /// Build and run the Tauri application.
    ///
    /// The caller must provide a `tauri::Context` (typically via `tauri::generate_context!()`
    /// in the binary crate that has a `tauri.conf.json`).
    pub fn run(mut self, context: tauri::Context) -> Result<(), Box<dyn std::error::Error>> {
        // When Nucleus is enabled, register built-in API routes
        #[cfg(feature = "nucleus-embedded")]
        if self.nucleus_enabled {
            self = self.register_nucleus_routes();
        }

        let router = bridge::Router::new(self.routes);
        let title = self.title.clone();
        let width = self.width;
        let height = self.height;
        let nucleus_enabled = self.nucleus_enabled;
        let data_dir = self.nucleus_data_dir.clone();

        let mut builder = tauri::Builder::default()
            .register_asynchronous_uri_scheme_protocol("neutron", move |_ctx, request, responder| {
                let response = router.handle(request);
                responder.respond(response);
            });

        // Apply registered plugins
        for apply_plugin in self.plugins {
            builder = apply_plugin(builder);
        }

        builder.setup(move |app| {
                // Initialize Nucleus if enabled
                if nucleus_enabled {
                    let dir = data_dir.unwrap_or_else(|| {
                        let base = dirs::data_dir().expect("no data directory available");
                        base.join("com.neutron.app").join("nucleus")
                    });
                    let state = NucleusState::new(dir);
                    state.initialize().map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
                    let state = Arc::new(state);
                    #[cfg(feature = "nucleus-embedded")]
                    set_nucleus_global(state.clone());
                    app.manage(state);
                    tracing::info!("Nucleus embedded database initialized");
                }

                // Create main window
                let _window = tauri::WebviewWindowBuilder::new(
                    app,
                    "main",
                    tauri::WebviewUrl::App("index.html".into()),
                )
                .title(&title)
                .inner_size(width, height)
                .build()?;

                tracing::info!("Neutron Desktop started");
                Ok(())
            })
            .run(context)?;

        Ok(())
    }

    /// Register built-in Nucleus API routes for the protocol bridge.
    #[cfg(feature = "nucleus-embedded")]
    fn register_nucleus_routes(self) -> Self {
        self
            .get("/api/nucleus/health", |_req| {
                Response::json(&serde_json::json!({
                    "status": "ok",
                    "nucleus": true,
                    "version": env!("CARGO_PKG_VERSION"),
                    "engine": "embedded",
                }))
            })
            .post("/api/nucleus/query", |req| {
                // Parse SQL from request body
                #[derive(serde::Deserialize)]
                struct QueryRequest {
                    sql: String,
                }

                let body: QueryRequest = match req.json() {
                    Ok(b) => b,
                    Err(e) => return Response::error(400, "Bad Request", &format!("Invalid JSON: {e}")),
                };

                // Execute synchronously using a runtime handle
                // (protocol handlers run in a sync context)
                let rt = match tokio::runtime::Handle::try_current() {
                    Ok(h) => h,
                    Err(_) => return Response::error(500, "Internal Error", "No Tokio runtime"),
                };

                // We need access to the NucleusState, which is managed by Tauri.
                // Since protocol handlers don't have app state, we use a global.
                match NUCLEUS_DB.get() {
                    Some(state) => {
                        match rt.block_on(state.query(&body.sql)) {
                            Ok(result) => Response::json(&result),
                            Err(e) => Response::error(400, "Query Error", &e.to_string()),
                        }
                    }
                    None => Response::error(503, "Not Ready", "Database not initialized"),
                }
            })
    }
}

/// Global reference to the Nucleus state for protocol handler access.
#[cfg(feature = "nucleus-embedded")]
static NUCLEUS_DB: std::sync::OnceLock<Arc<NucleusState>> = std::sync::OnceLock::new();

/// Set the global Nucleus state reference (called during app setup).
#[cfg(feature = "nucleus-embedded")]
pub fn set_nucleus_global(state: Arc<NucleusState>) {
    let _ = NUCLEUS_DB.set(state);
}

impl Default for NeutronDesktopBuilder {
    fn default() -> Self {
        Self::new()
    }
}
