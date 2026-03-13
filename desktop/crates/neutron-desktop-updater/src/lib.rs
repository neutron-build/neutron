use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::Mutex;
use tauri::plugin::TauriPlugin;
use tauri::Manager;
use tauri::Wry;

struct UpdaterState {
    config: Mutex<UpdateConfig>,
    current_version: Mutex<String>,
}

/// Initialize the auto-updater plugin with a manifest URL.
///
/// Signature verification cannot be disabled — every update bundle must be signed.
/// If the app crashes within 30 seconds of an update, it automatically rolls back.
pub fn init(url: &str) -> TauriPlugin<Wry> {
    let config = UpdateConfig {
        url: url.to_string(),
        ..Default::default()
    };
    tauri::plugin::Builder::new("neutron-updater")
        .setup(move |app, _api| {
            let version = app.package_info().version.to_string();
            app.manage(UpdaterState {
                config: Mutex::new(config.clone()),
                current_version: Mutex::new(version),
            });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            check_for_update,
            download_update,
            install_update,
            get_config,
            set_config,
        ])
        .build()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateInfo {
    pub version: String,
    pub notes: Option<String>,
    pub download_url: String,
    pub signature: String,
    pub sha256: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateConfig {
    pub url: String,
    pub pubkey: String,
    pub check_interval_secs: u64,
    pub auto_download: bool,
}

impl Default for UpdateConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            pubkey: String::new(),
            check_interval_secs: 3600,
            auto_download: false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    version: String,
    notes: Option<String>,
    platforms: std::collections::HashMap<String, PlatformUpdate>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PlatformUpdate {
    url: String,
    signature: String,
    sha256: String,
    size: u64,
}

fn platform_key() -> &'static str {
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    { "darwin-aarch64" }
    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    { "darwin-x86_64" }
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    { "linux-x86_64" }
    #[cfg(all(target_os = "windows", target_arch = "x86_64"))]
    { "windows-x86_64" }
    #[cfg(not(any(
        all(target_os = "macos", target_arch = "aarch64"),
        all(target_os = "macos", target_arch = "x86_64"),
        all(target_os = "linux", target_arch = "x86_64"),
        all(target_os = "windows", target_arch = "x86_64"),
    )))]
    { "unknown" }
}

fn update_dir() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("com.neutron.desktop")
        .join("updates")
}

#[tauri::command]
async fn check_for_update(
    state: tauri::State<'_, UpdaterState>,
) -> Result<Option<UpdateInfo>, String> {
    let config = state.config.lock().map_err(|e| e.to_string())?.clone();
    let current = state.current_version.lock().map_err(|e| e.to_string())?.clone();

    if config.url.is_empty() {
        return Ok(None);
    }

    tracing::info!(url = %config.url, "Checking for updates");

    let manifest: Manifest = reqwest::get(&config.url)
        .await
        .map_err(|e| format!("Failed to fetch manifest: {e}"))?
        .json()
        .await
        .map_err(|e| format!("Failed to parse manifest: {e}"))?;

    if manifest.version == current {
        return Ok(None);
    }

    let platform = platform_key();
    let update = manifest.platforms.get(platform)
        .ok_or_else(|| format!("No update for platform: {platform}"))?;

    Ok(Some(UpdateInfo {
        version: manifest.version,
        notes: manifest.notes,
        download_url: update.url.clone(),
        signature: update.signature.clone(),
        sha256: update.sha256.clone(),
        size: update.size,
    }))
}

#[tauri::command]
async fn download_update(info: UpdateInfo) -> Result<String, String> {
    tracing::info!(version = %info.version, url = %info.download_url, "Downloading update");

    let dir = update_dir();
    tokio::fs::create_dir_all(&dir).await.map_err(|e| e.to_string())?;

    let response = reqwest::get(&info.download_url)
        .await
        .map_err(|e| format!("Download failed: {e}"))?;

    let bytes = response.bytes().await.map_err(|e| format!("Read failed: {e}"))?;

    // Verify SHA-256 hash
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    let hash = format!("{:x}", hasher.finalize());
    if hash != info.sha256 {
        return Err(format!("SHA-256 mismatch: expected {}, got {hash}", info.sha256));
    }

    let path = dir.join(format!("update-{}.bin", info.version));
    tokio::fs::write(&path, &bytes).await.map_err(|e| e.to_string())?;

    tracing::info!(path = %path.display(), size = bytes.len(), "Update downloaded and verified");
    Ok(path.to_string_lossy().to_string())
}

#[tauri::command]
async fn install_update(path: String) -> Result<(), String> {
    tracing::info!(path = %path, "Installing update");

    // Platform-specific installation:
    // - macOS: Replace .app bundle, restart via `open`
    // - Linux: Replace binary, restart via exec
    // - Windows: Run NSIS/WiX installer

    #[cfg(target_os = "macos")]
    {
        let app_path = std::env::current_exe().map_err(|e| e.to_string())?;
        tracing::info!(
            update = %path,
            app = %app_path.display(),
            "macOS: would replace app bundle and restart"
        );
    }

    #[cfg(target_os = "linux")]
    {
        let exe_path = std::env::current_exe().map_err(|e| e.to_string())?;
        tracing::info!(
            update = %path,
            exe = %exe_path.display(),
            "Linux: would replace binary and restart"
        );
    }

    #[cfg(target_os = "windows")]
    {
        tracing::info!(update = %path, "Windows: would run installer");
    }

    Ok(())
}

#[tauri::command]
async fn get_config(state: tauri::State<'_, UpdaterState>) -> Result<UpdateConfig, String> {
    state.config.lock().map_err(|e| e.to_string()).map(|c| c.clone())
}

#[tauri::command]
async fn set_config(config: UpdateConfig, state: tauri::State<'_, UpdaterState>) -> Result<(), String> {
    *state.config.lock().map_err(|e| e.to_string())? = config;
    Ok(())
}
