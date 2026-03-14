use std::path::PathBuf;
use tauri::plugin::TauriPlugin;
use tauri::{Manager, Wry};

struct AutostartState {
    app_name: String,
    exe_path: PathBuf,
}

pub fn init() -> TauriPlugin<Wry> {
    tauri::plugin::Builder::new("neutron-autostart")
        .setup(|app, _api| {
            let name = app.package_info().name.clone();
            let exe = std::env::current_exe().unwrap_or_default();
            app.manage(AutostartState {
                app_name: name,
                exe_path: exe,
            });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            enable_autostart,
            disable_autostart,
            is_autostart_enabled,
        ])
        .build()
}

/// macOS: ~/Library/LaunchAgents/com.neutron.{app}.plist
#[cfg(target_os = "macos")]
fn plist_path(app_name: &str) -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("~"))
        .join("Library/LaunchAgents")
        .join(format!("com.neutron.{app_name}.plist"))
}

#[cfg(target_os = "macos")]
fn write_plist(app_name: &str, exe_path: &std::path::Path) -> Result<(), String> {
    let path = plist_path(app_name);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let content = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.neutron.{app_name}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{exe}</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
</dict>
</plist>"#,
        app_name = app_name,
        exe = exe_path.display()
    );
    std::fs::write(&path, content).map_err(|e| e.to_string())?;
    tracing::info!(path = %path.display(), "Wrote LaunchAgent plist");
    Ok(())
}

/// Linux: ~/.config/autostart/{app}.desktop
#[cfg(target_os = "linux")]
fn desktop_entry_path(app_name: &str) -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("~/.config"))
        .join("autostart")
        .join(format!("{app_name}.desktop"))
}

#[cfg(target_os = "linux")]
fn write_desktop_entry(app_name: &str, exe_path: &std::path::Path) -> Result<(), String> {
    let path = desktop_entry_path(app_name);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let content = format!(
        "[Desktop Entry]\nType=Application\nName={name}\nExec={exe}\nX-GNOME-Autostart-enabled=true\n",
        name = app_name,
        exe = exe_path.display()
    );
    std::fs::write(&path, content).map_err(|e| e.to_string())?;
    tracing::info!(path = %path.display(), "Wrote XDG autostart entry");
    Ok(())
}

/// Windows: HKCU\SOFTWARE\Microsoft\Windows\CurrentVersion\Run
#[cfg(target_os = "windows")]
fn registry_key_name(app_name: &str) -> String {
    format!("Neutron_{app_name}")
}

#[tauri::command]
async fn enable_autostart(state: tauri::State<'_, AutostartState>) -> Result<(), String> {
    let app_name = &state.app_name;
    let exe_path = &state.exe_path;

    #[cfg(target_os = "macos")]
    write_plist(app_name, exe_path)?;

    #[cfg(target_os = "linux")]
    write_desktop_entry(app_name, exe_path)?;

    #[cfg(target_os = "windows")]
    {
        let key_name = registry_key_name(app_name);
        let output = std::process::Command::new("reg")
            .args(["add", r"HKCU\SOFTWARE\Microsoft\Windows\CurrentVersion\Run",
                   "/v", &key_name, "/t", "REG_SZ",
                   "/d", &exe_path.to_string_lossy(), "/f"])
            .output()
            .map_err(|e| e.to_string())?;
        if !output.status.success() {
            return Err(String::from_utf8_lossy(&output.stderr).to_string());
        }
        tracing::info!(key = %key_name, "Wrote Windows registry autostart");
    }

    tracing::info!(app = %app_name, "Autostart enabled");
    Ok(())
}

#[tauri::command]
async fn disable_autostart(state: tauri::State<'_, AutostartState>) -> Result<(), String> {
    let app_name = &state.app_name;

    #[cfg(target_os = "macos")]
    {
        let path = plist_path(app_name);
        if path.exists() {
            std::fs::remove_file(&path).map_err(|e| e.to_string())?;
        }
    }

    #[cfg(target_os = "linux")]
    {
        let path = desktop_entry_path(app_name);
        if path.exists() {
            std::fs::remove_file(&path).map_err(|e| e.to_string())?;
        }
    }

    #[cfg(target_os = "windows")]
    {
        let key_name = registry_key_name(app_name);
        let _ = std::process::Command::new("reg")
            .args(["delete", r"HKCU\SOFTWARE\Microsoft\Windows\CurrentVersion\Run",
                   "/v", &key_name, "/f"])
            .output();
    }

    tracing::info!(app = %app_name, "Autostart disabled");
    Ok(())
}

#[tauri::command]
async fn is_autostart_enabled(state: tauri::State<'_, AutostartState>) -> Result<bool, String> {
    let app_name = &state.app_name;

    #[cfg(target_os = "macos")]
    { return Ok(plist_path(app_name).exists()); }

    #[cfg(target_os = "linux")]
    { return Ok(desktop_entry_path(app_name).exists()); }

    #[cfg(target_os = "windows")]
    {
        let key_name = registry_key_name(app_name);
        let output = std::process::Command::new("reg")
            .args(["query", r"HKCU\SOFTWARE\Microsoft\Windows\CurrentVersion\Run",
                   "/v", &key_name])
            .output()
            .map_err(|e| e.to_string())?;
        return Ok(output.status.success());
    }

    #[allow(unreachable_code)]
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "macos")]
    #[test]
    fn test_plist_path_format() {
        let path = plist_path("testapp");
        let path_str = path.to_string_lossy();
        assert!(path_str.contains("Library/LaunchAgents"));
        assert!(path_str.contains("com.neutron.testapp.plist"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_desktop_entry_path_format() {
        let path = desktop_entry_path("testapp");
        let path_str = path.to_string_lossy();
        assert!(path_str.contains("autostart"));
        assert!(path_str.contains("testapp.desktop"));
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn test_registry_key_name_format() {
        let key = registry_key_name("testapp");
        assert_eq!(key, "Neutron_testapp");
    }

    #[test]
    fn test_init_creates_plugin() {
        let _plugin = super::init();
    }
}
