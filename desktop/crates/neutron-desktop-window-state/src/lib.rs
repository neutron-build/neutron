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
