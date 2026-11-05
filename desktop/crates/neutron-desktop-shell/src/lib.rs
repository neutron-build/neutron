use tauri::plugin::TauriPlugin;
use tauri::Wry;

pub fn init() -> TauriPlugin<Wry> {
    tauri::plugin::Builder::new("neutron-shell")
        .invoke_handler(tauri::generate_handler![open_url, reveal_in_finder])
        .build()
}

/// URL schemes `open_url` hands to the OS. Anything else (file:, custom
/// protocol handlers, javascript:, ...) could launch local programs or files.
const ALLOWED_URL_SCHEMES: &[&str] = &["https", "mailto"];

fn check_url(url: &str) -> Result<url::Url, String> {
    let parsed = url::Url::parse(url).map_err(|e| format!("invalid URL: {e}"))?;
    if !ALLOWED_URL_SCHEMES.contains(&parsed.scheme()) {
        return Err(format!(
            "URL scheme not allowed: {} (allowed: https, mailto)",
            parsed.scheme()
        ));
    }
    Ok(parsed)
}

#[tauri::command]
async fn open_url(url: String) -> Result<(), String> {
    let parsed = check_url(&url)?;
    open::that(parsed.as_str()).map_err(|e| format!("failed to open URL: {e}"))
}

/// Reveal a file or folder in the platform file manager (Finder, Explorer, Nautilus).
#[tauri::command]
async fn reveal_in_finder(path: String) -> Result<(), String> {
    #[cfg(target_os = "macos")]
    {
        std::process::Command::new("open")
            .args(["-R", &path])
            .spawn()
            .map_err(|e| format!("failed to reveal in Finder: {e}"))?;
        return Ok(());
    }

    #[cfg(target_os = "windows")]
    {
        std::process::Command::new("explorer")
            .args(["/select,", &path])
            .spawn()
            .map_err(|e| format!("failed to reveal in Explorer: {e}"))?;
        return Ok(());
    }

    #[cfg(target_os = "linux")]
    {
        // Try dbus method first (works with most file managers), fallback to xdg-open on parent
        let p = std::path::Path::new(&path);
        let dir = if p.is_dir() { p } else { p.parent().unwrap_or(p) };
        std::process::Command::new("xdg-open")
            .arg(dir.as_os_str())
            .spawn()
            .map_err(|e| format!("failed to reveal in file manager: {e}"))?;
        return Ok(());
    }

    #[allow(unreachable_code)]
    Err("reveal_in_finder not supported on this platform".to_string())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_init_creates_plugin() {
        // Verify the plugin builder doesn't panic
        let _plugin = super::init();
    }

    #[test]
    fn open_url_allows_only_https_and_mailto() {
        assert!(super::check_url("https://neutron.build/docs").is_ok());
        assert!(super::check_url("mailto:hello@example.com").is_ok());
        for bad in [
            "http://example.com",
            "file:///etc/passwd",
            "javascript:alert(1)",
            "smb://host/share",
            "/Applications/Calculator.app",
            "not a url",
        ] {
            assert!(super::check_url(bad).is_err(), "{bad} must be refused");
        }
    }
}
