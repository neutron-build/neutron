use tauri::plugin::TauriPlugin;
use tauri::Wry;

pub fn init() -> TauriPlugin<Wry> {
    tauri::plugin::Builder::new("neutron-shell")
        .invoke_handler(tauri::generate_handler![open_url, open_path])
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
