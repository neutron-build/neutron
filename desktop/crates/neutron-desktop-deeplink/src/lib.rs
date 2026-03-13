use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use tauri::plugin::TauriPlugin;
use tauri::{Manager, Wry};

struct DeepLinkState {
    scheme: String,
    initial_url: Mutex<Option<String>>,
    history: Mutex<Vec<String>>,
}

pub fn init(scheme: &str) -> TauriPlugin<Wry> {
    let scheme = scheme.to_string();
    tauri::plugin::Builder::new("neutron-deeplink")
        .setup(move |app, _api| {
            app.manage(DeepLinkState {
                scheme: scheme.clone(),
                initial_url: Mutex::new(None),
                history: Mutex::new(Vec::new()),
            });

            // On macOS/Windows, the OS delivers the URL via command-line args
            // when the app is cold-launched via a deep link.
            let args: Vec<String> = std::env::args().collect();
            if let Some(url_arg) = args.iter().find(|a| a.contains("://")) {
                if let Ok(mut initial) = app.state::<DeepLinkState>().initial_url.lock() {
                    *initial = Some(url_arg.clone());
                }
                tracing::info!(url = %url_arg, "Deep link from launch args");
            }

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            get_initial_url,
            get_scheme,
            get_history,
            push_url,
        ])
        .build()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeepLink {
    pub url: String,
    pub scheme: String,
    pub host: Option<String>,
    pub path: String,
    pub query: std::collections::HashMap<String, String>,
    pub fragment: Option<String>,
}

impl DeepLink {
    pub fn parse(raw: &str) -> Option<Self> {
        let parsed = url::Url::parse(raw).ok()?;
        let query = parsed.query_pairs().into_owned().collect();
        Some(Self {
            url: raw.to_string(),
            scheme: parsed.scheme().to_string(),
            host: parsed.host_str().map(|s| s.to_string()),
            path: parsed.path().to_string(),
            query,
            fragment: parsed.fragment().map(|s| s.to_string()),
        })
    }
}

#[tauri::command]
async fn get_initial_url(
    state: tauri::State<'_, DeepLinkState>,
) -> Result<Option<DeepLink>, String> {
    let guard = state.initial_url.lock().map_err(|e| e.to_string())?;
    Ok(guard.as_ref().and_then(|url| DeepLink::parse(url)))
}

#[tauri::command]
async fn get_scheme(state: tauri::State<'_, DeepLinkState>) -> Result<String, String> {
    Ok(state.scheme.clone())
}

#[tauri::command]
async fn get_history(state: tauri::State<'_, DeepLinkState>) -> Result<Vec<DeepLink>, String> {
    let guard = state.history.lock().map_err(|e| e.to_string())?;
    Ok(guard.iter().filter_map(|url| DeepLink::parse(url)).collect())
}

/// Push a URL into the deep link history (called from native event handlers).
#[tauri::command]
async fn push_url(
    url: String,
    state: tauri::State<'_, DeepLinkState>,
) -> Result<Option<DeepLink>, String> {
    let parsed = DeepLink::parse(&url);
    state.history.lock().map_err(|e| e.to_string())?.push(url);
    Ok(parsed)
}
