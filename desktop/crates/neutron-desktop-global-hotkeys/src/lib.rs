use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;
use tauri::plugin::TauriPlugin;
use tauri::{Emitter, Manager, Wry};
use tauri_plugin_global_shortcut::{GlobalShortcutExt, Shortcut};

struct HotkeyState {
    registered: Mutex<HashMap<String, String>>,
}

pub fn init() -> TauriPlugin<Wry> {
    tauri::plugin::Builder::new("neutron-global-hotkeys")
        .setup(|app, _api| {
            app.manage(HotkeyState {
                registered: Mutex::new(HashMap::new()),
            });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            register_hotkey,
            unregister_hotkey,
            unregister_all,
            list_hotkeys,
            is_registered,
        ])
        .build()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hotkey {
    pub id: String,
    pub accelerator: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HotkeyEvent {
    id: String,
    accelerator: String,
}

fn parse_shortcut(accel: &str) -> Result<Shortcut, String> {
    accel.parse::<Shortcut>().map_err(|e| format!("Invalid shortcut '{accel}': {e}"))
}

#[tauri::command]
async fn register_hotkey(
    app: tauri::AppHandle,
    id: String,
    accelerator: String,
    state: tauri::State<'_, HotkeyState>,
) -> Result<(), String> {
    let shortcut = parse_shortcut(&accelerator)?;
    let hotkey_id = id.clone();
    let hotkey_accel = accelerator.clone();
    let app_clone = app.clone();

    app.global_shortcut().on_shortcut(
        shortcut,
        move |_app, _shortcut, _event| {
            let _ = app_clone.emit("neutron-hotkey", HotkeyEvent {
                id: hotkey_id.clone(),
                accelerator: hotkey_accel.clone(),
            });
        },
    ).map_err(|e| e.to_string())?;

    state.registered.lock().map_err(|e| e.to_string())?
        .insert(id.clone(), accelerator.clone());

    tracing::info!(id = %id, accelerator = %accelerator, "Registered global hotkey");
    Ok(())
}

#[tauri::command]
async fn unregister_hotkey(
    app: tauri::AppHandle,
    id: String,
    state: tauri::State<'_, HotkeyState>,
) -> Result<(), String> {
    let mut registered = state.registered.lock().map_err(|e| e.to_string())?;
    if let Some(accelerator) = registered.remove(&id) {
        let shortcut = parse_shortcut(&accelerator)?;
        app.global_shortcut().unregister(shortcut).map_err(|e| e.to_string())?;
        tracing::info!(id = %id, "Unregistered global hotkey");
    }
    Ok(())
}

#[tauri::command]
async fn unregister_all(
    app: tauri::AppHandle,
    state: tauri::State<'_, HotkeyState>,
) -> Result<(), String> {
    app.global_shortcut().unregister_all().map_err(|e| e.to_string())?;
    state.registered.lock().map_err(|e| e.to_string())?.clear();
    tracing::info!("Unregistered all global hotkeys");
    Ok(())
}

#[tauri::command]
async fn list_hotkeys(state: tauri::State<'_, HotkeyState>) -> Result<Vec<Hotkey>, String> {
    let registered = state.registered.lock().map_err(|e| e.to_string())?;
    Ok(registered.iter().map(|(id, accel)| Hotkey {
        id: id.clone(),
        accelerator: accel.clone(),
    }).collect())
}

#[tauri::command]
async fn is_registered(
    app: tauri::AppHandle,
    accelerator: String,
) -> Result<bool, String> {
    let shortcut = parse_shortcut(&accelerator)?;
    Ok(app.global_shortcut().is_registered(shortcut))
}
