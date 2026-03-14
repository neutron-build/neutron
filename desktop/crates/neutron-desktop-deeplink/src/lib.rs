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
            register_scheme,
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

/// Register a custom URI scheme with the operating system.
///
/// On macOS this updates the app's Info.plist CFBundleURLSchemes (requires rebuild).
/// On Linux this writes a .desktop file with MimeType for `x-scheme-handler`.
/// On Windows this writes the protocol handler to the registry.
///
/// Note: On macOS, scheme registration is typically handled at build time via
/// `tauri.conf.json` > `plugins` > `deep-link` > `desktop` > `schemes`.
/// This command provides a runtime fallback for dynamic registration.
#[tauri::command]
async fn register_scheme(
    scheme: String,
    _state: tauri::State<'_, DeepLinkState>,
) -> Result<(), String> {
    tracing::info!(scheme = %scheme, "Registering URI scheme");

    // Validate scheme: must be lowercase alphanumeric
    if scheme.is_empty() || !scheme.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.') {
        return Err(format!("Invalid scheme '{}': must be lowercase alphanumeric with - or .", scheme));
    }

    #[cfg(target_os = "macos")]
    {
        // On macOS, deep link schemes are registered in Info.plist at build time.
        // At runtime we can use the Launch Services API, but this requires a bundle.
        let exe = std::env::current_exe().map_err(|e| e.to_string())?;
        // Try to register via lsregister for the running app bundle
        if let Some(app_bundle) = exe.ancestors().find(|p| {
            p.extension().is_some_and(|ext| ext == "app")
        }) {
            std::process::Command::new("/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister")
                .args(["-R", "-f"])
                .arg(app_bundle)
                .output()
                .map_err(|e| format!("lsregister failed: {e}"))?;
            tracing::info!(bundle = %app_bundle.display(), "Re-registered app bundle with LaunchServices");
        } else {
            tracing::warn!("Not running from an app bundle; scheme registration requires build-time configuration");
        }
        return Ok(());
    }

    #[cfg(target_os = "linux")]
    {
        let exe = std::env::current_exe().map_err(|e| e.to_string())?;
        let desktop_path = dirs::data_dir()
            .unwrap_or_else(|| std::path::PathBuf::from("~/.local/share"))
            .join("applications")
            .join(format!("{scheme}-handler.desktop"));
        if let Some(parent) = desktop_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        let content = format!(
            "[Desktop Entry]\nType=Application\nName={scheme} URL Handler\nExec={exe} %u\nMimeType=x-scheme-handler/{scheme};\nNoDisplay=true\n",
            scheme = scheme,
            exe = exe.display(),
        );
        std::fs::write(&desktop_path, content).map_err(|e| e.to_string())?;

        // Register with xdg-mime
        let _ = std::process::Command::new("xdg-mime")
            .args(["default", &format!("{scheme}-handler.desktop"), &format!("x-scheme-handler/{scheme}")])
            .output();

        tracing::info!(path = %desktop_path.display(), "Registered URI scheme via XDG");
        return Ok(());
    }

    #[cfg(target_os = "windows")]
    {
        let exe = std::env::current_exe().map_err(|e| e.to_string())?;
        let exe_str = exe.to_string_lossy();
        // Register protocol handler via registry
        let commands = [
            vec!["add", &format!(r"HKCU\SOFTWARE\Classes\{scheme}"), "/ve", "/t", "REG_SZ", "/d", &format!("URL:{scheme} Protocol"), "/f"],
            vec!["add", &format!(r"HKCU\SOFTWARE\Classes\{scheme}"), "/v", "URL Protocol", "/t", "REG_SZ", "/d", "", "/f"],
            vec!["add", &format!(r"HKCU\SOFTWARE\Classes\{scheme}\shell\open\command"), "/ve", "/t", "REG_SZ", "/d", &format!(r#""{}" "%1""#, exe_str), "/f"],
        ];
        for args in &commands {
            let output = std::process::Command::new("reg")
                .args(args)
                .output()
                .map_err(|e| format!("reg command failed: {e}"))?;
            if !output.status.success() {
                return Err(String::from_utf8_lossy(&output.stderr).to_string());
            }
        }
        tracing::info!(scheme = %scheme, "Registered URI scheme via Windows registry");
        return Ok(());
    }

    #[allow(unreachable_code)]
    Err("URI scheme registration not supported on this platform".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_deeplink_basic() {
        let link = DeepLink::parse("myapp://settings/profile").unwrap();
        assert_eq!(link.scheme, "myapp");
        assert_eq!(link.host.as_deref(), Some("settings"));
        assert_eq!(link.path, "/profile");
        assert!(link.query.is_empty());
        assert!(link.fragment.is_none());
    }

    #[test]
    fn test_parse_deeplink_with_query() {
        let link = DeepLink::parse("myapp://open?file=test.txt&line=42").unwrap();
        assert_eq!(link.scheme, "myapp");
        assert_eq!(link.query.get("file").unwrap(), "test.txt");
        assert_eq!(link.query.get("line").unwrap(), "42");
    }

    #[test]
    fn test_parse_deeplink_with_fragment() {
        let link = DeepLink::parse("myapp://docs/page#section").unwrap();
        assert_eq!(link.fragment.as_deref(), Some("section"));
    }

    #[test]
    fn test_parse_deeplink_invalid() {
        assert!(DeepLink::parse("not a url").is_none());
    }

    #[test]
    fn test_parse_deeplink_empty_path() {
        let link = DeepLink::parse("myapp://host").unwrap();
        assert_eq!(link.scheme, "myapp");
        assert_eq!(link.host.as_deref(), Some("host"));
    }

    #[test]
    fn test_deeplink_serialize_roundtrip() {
        let link = DeepLink::parse("test://foo/bar?k=v").unwrap();
        let json = serde_json::to_string(&link).unwrap();
        let restored: DeepLink = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.scheme, "test");
        assert_eq!(restored.path, "/bar");
        assert_eq!(restored.query.get("k").unwrap(), "v");
    }

    #[test]
    fn test_init_creates_plugin() {
        let _plugin = super::init("myapp");
    }
}
