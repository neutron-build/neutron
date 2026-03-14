use serde::{Deserialize, Serialize};
use tauri::plugin::TauriPlugin;
use tauri::Wry;

pub fn init() -> TauriPlugin<Wry> {
    tauri::plugin::Builder::new("neutron-window-state")
        .invoke_handler(tauri::generate_handler![
            save_window_state,
            load_window_state,
        ])
        .build()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WindowState {
    pub x: i32,
    pub y: i32,
    pub width: u32,
    pub height: u32,
    pub maximized: bool,
    pub fullscreen: bool,
}

#[tauri::command]
async fn save_window_state(label: String, state: WindowState) -> Result<(), String> {
    let path = state_file_path(&label);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let json = serde_json::to_string_pretty(&state).map_err(|e| e.to_string())?;
    std::fs::write(&path, json).map_err(|e| e.to_string())?;
    Ok(())
}

#[tauri::command]
async fn load_window_state(label: String) -> Result<Option<WindowState>, String> {
    let path = state_file_path(&label);
    if !path.exists() {
        return Ok(None);
    }
    let json = std::fs::read_to_string(&path).map_err(|e| e.to_string())?;
    let state: WindowState = serde_json::from_str(&json).map_err(|e| e.to_string())?;
    Ok(Some(state))
}

fn state_file_path(label: &str) -> std::path::PathBuf {
    let base = dirs::data_dir().expect("no data directory");
    base.join("com.neutron.desktop")
        .join("window-state")
        .join(format!("{label}.json"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_window_state_serialize() {
        let state = WindowState {
            x: 100,
            y: 200,
            width: 1920,
            height: 1080,
            maximized: false,
            fullscreen: false,
        };
        let json = serde_json::to_string(&state).unwrap();
        assert!(json.contains("1920"));
        assert!(json.contains("1080"));
    }

    #[test]
    fn test_window_state_deserialize() {
        let json = r#"{"x":50,"y":50,"width":800,"height":600,"maximized":true,"fullscreen":false}"#;
        let state: WindowState = serde_json::from_str(json).unwrap();
        assert_eq!(state.x, 50);
        assert_eq!(state.y, 50);
        assert_eq!(state.width, 800);
        assert_eq!(state.height, 600);
        assert!(state.maximized);
        assert!(!state.fullscreen);
    }

    #[test]
    fn test_window_state_roundtrip() {
        let state = WindowState {
            x: -10,
            y: 0,
            width: 3840,
            height: 2160,
            maximized: true,
            fullscreen: true,
        };
        let json = serde_json::to_string_pretty(&state).unwrap();
        let restored: WindowState = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.x, state.x);
        assert_eq!(restored.y, state.y);
        assert_eq!(restored.width, state.width);
        assert_eq!(restored.height, state.height);
        assert_eq!(restored.maximized, state.maximized);
        assert_eq!(restored.fullscreen, state.fullscreen);
    }

    #[test]
    fn test_state_file_path_format() {
        let path = state_file_path("main");
        let path_str = path.to_string_lossy();
        assert!(path_str.contains("com.neutron.desktop"));
        assert!(path_str.contains("window-state"));
        assert!(path_str.ends_with("main.json"));
    }

    #[test]
    fn test_init_creates_plugin() {
        let _plugin = super::init();
    }
}
