mod bridge;
mod dev_server;
mod ipc;
mod migration;
pub use migration::migrate_legacy_storage;
mod nucleus_state;
mod window;

pub use bridge::{Request, Response, create_protocol_handler};
pub use dev_server::{dev_port, is_dev_mode};
// Re-exported so callers of the `ipc_handler!` macro can name `IpcCommand`,
// and so the TypeScript codegen helpers are reachable from outside the crate.
pub use ipc::{IpcCommand, TypeScriptBinding, collect_bindings};
#[cfg(feature = "nucleus-embedded")]
pub use nucleus_state::NucleusQueryResult;
pub use nucleus_state::{NucleusError, NucleusState, platform_data_dir};
pub use window::WindowConfig;

use std::sync::Arc;
use tauri::Manager;

/// A deferred `tauri::Builder` transformation used to register a plugin.
type PluginRegistration = Box<dyn FnOnce(tauri::Builder<tauri::Wry>) -> tauri::Builder<tauri::Wry>>;

/// Builder for configuring and launching a Neutron Desktop application.
///
/// Routes frontend `fetch()` calls through the `neutron://` protocol to the
/// explicit synchronous handlers. Applications own authorization/middleware.
/// Debug TCP dispatch is separately opt-in and capability authenticated.
pub struct NeutronDesktopBuilder {
    window_config: WindowConfig,
    nucleus_enabled: bool,
    nucleus_data_dir: Option<std::path::PathBuf>,
    routes: Vec<Route>,
    plugins: Vec<PluginRegistration>,
}

struct Route {
    method: http::Method,
    path: String,
    handler: Box<dyn Fn(bridge::Request) -> bridge::Response + Send + Sync>,
}

impl NeutronDesktopBuilder {
    pub fn new() -> Self {
        Self {
            window_config: WindowConfig::default(),
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
        f(&mut self.window_config);
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
        self.plugins
            .push(Box::new(move |builder| builder.plugin(plugin)));
        self
    }

    /// Build and run the Tauri application.
    ///
    /// The caller must provide a `tauri::Context` (typically via `tauri::generate_context!()`
    /// in the binary crate that has a `tauri.conf.json`).
    pub fn run(mut self, context: tauri::Context) -> Result<(), Box<dyn std::error::Error>> {
        self.window_config.validate()?;
        // When Nucleus is enabled, register built-in API routes
        #[cfg(feature = "nucleus-embedded")]
        if self.nucleus_enabled {
            self = self.register_nucleus_routes();
        }

        let router = bridge::Router::new(self.routes);
        let router = Arc::new(router);

        // --- Dev-mode TCP server (strictly additive, never affects prod) ---
        let dev_mode = dev_server::is_dev_mode();
        let dev_port_val = dev_server::dev_port();

        let dev_access = if dev_mode {
            Some(dev_server::DevAccess::new(dev_port_val)?)
        } else {
            None
        };
        if let Some(access) = &dev_access {
            dev_server::spawn_dev_server(Arc::clone(&router), dev_port_val, access.clone())?;
        } else {
            println!(
                "Desktop running in PRODUCTION MODE \u{2014} neutron:// protocol active, no TCP port"
            );
        }

        let window_config = self.window_config;

        let nucleus_enabled = self.nucleus_enabled;
        let data_dir = self.nucleus_data_dir.clone();

        let protocol_router = Arc::clone(&router);
        let mut builder = tauri::Builder::default().register_asynchronous_uri_scheme_protocol(
            "neutron",
            move |_ctx, request, responder| {
                let response = protocol_router.handle(request);
                responder.respond(response);
            },
        );

        // Apply registered plugins
        for apply_plugin in self.plugins {
            builder = apply_plugin(builder);
        }

        builder.setup(move |app| {
                // Initialize Nucleus if enabled
                if nucleus_enabled {
                    let dir = match data_dir {
                        Some(dir) => dir,
                        None => app.path().app_data_dir()?.join("nucleus"),
                    };
                    let state = NucleusState::new(dir);
                    state.initialize().map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
                    let state = Arc::new(state);
                    #[cfg(feature = "nucleus-embedded")]
                    set_nucleus_global(state.clone());
                    app.manage(state);
                    tracing::info!("Nucleus embedded database initialized");
                }

                // Create main window
                let window = tauri::WebviewWindowBuilder::new(
                    app,
                    "main",
                    tauri::WebviewUrl::App("index.html".into()),
                )
                .title(&window_config.title)
                .inner_size(window_config.width, window_config.height)
                .resizable(window_config.resizable)
                .decorations(window_config.decorations)
                .transparent(window_config.transparent)
                .fullscreen(window_config.fullscreen);
                let window = if let Some(access) = &dev_access {
                    let js = format!(
                        "window.__NEUTRON_DEV_MODE__ = true; window.__NEUTRON_DEV_PORT__ = {}; window.__NEUTRON_DEV_TOKEN__ = {};",
                        dev_port_val, serde_json::to_string(&access.token)?,
                    );
                    window.initialization_script(&js)
                } else { window };
                let _window = match (window_config.min_width, window_config.min_height) {
                    (Some(w), Some(h)) => window.min_inner_size(w, h),
                    _ => window,
                }.build()?;

                tracing::info!(
                    mode = if dev_mode { "dev" } else { "production" },
                    "Neutron Desktop started"
                );
                Ok(())
            })
            .run(context)?;

        Ok(())
    }

    /// Register built-in Nucleus API routes for the protocol bridge.
    #[cfg(feature = "nucleus-embedded")]
    fn register_nucleus_routes(self) -> Self {
        self.get("/api/nucleus/health", |_req| {
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
                Err(e) => {
                    return Response::error(400, "Bad Request", &format!("Invalid JSON: {e}"));
                }
            };

            // Execute synchronously using a runtime handle
            // (protocol handlers run in a sync context)
            let rt = tauri::async_runtime::handle();

            // We need access to the NucleusState, which is managed by Tauri.
            // Since protocol handlers don't have app state, we use a global.
            match NUCLEUS_DB.get() {
                Some(state) => match rt.block_on(state.query(&body.sql)) {
                    Ok(result) => Response::json(&result),
                    Err(e) => Response::error(400, "Query Error", &e.to_string()),
                },
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
