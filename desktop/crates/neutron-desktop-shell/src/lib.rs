use tauri::plugin::TauriPlugin;
use tauri::Wry;

pub fn init() -> TauriPlugin<Wry> {
    tauri::plugin::Builder::new("neutron-shell")
        .invoke_handler(tauri::generate_handler![open_url, open_path, reveal_in_finder])
        .build()
}

#[tauri::command]
async fn open_url(url: String) -> Result<(), String> {
    open::that(&url).map_err(|e| format!("failed to open URL: {e}"))
}

#[tauri::command]
async fn open_path(path: String) -> Result<(), String> {
    open::that(&path).map_err(|e| format!("failed to open path: {e}"))
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
}
