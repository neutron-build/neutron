use std::sync::Mutex;
use tauri::plugin::TauriPlugin;
use tauri::Manager;
use tauri::Wry;

struct ClipboardState(Mutex<Option<arboard::Clipboard>>);

pub fn init() -> TauriPlugin<Wry> {
    tauri::plugin::Builder::new("neutron-clipboard")
        .setup(|app, _api| {
            let clipboard = arboard::Clipboard::new().ok();
            app.manage(ClipboardState(Mutex::new(clipboard)));
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            read_text,
            write_text,
            read_image,
            write_image,
            has_text,
            clear,
        ])
        .build()
}

#[tauri::command]
async fn read_text(state: tauri::State<'_, ClipboardState>) -> Result<String, String> {
    let mut guard = state.0.lock().map_err(|e| e.to_string())?;
    let cb = guard.as_mut().ok_or("clipboard not available")?;
    cb.get_text().map_err(|e| e.to_string())
}

#[tauri::command]
async fn write_text(text: String, state: tauri::State<'_, ClipboardState>) -> Result<(), String> {
    let mut guard = state.0.lock().map_err(|e| e.to_string())?;
    let cb = guard.as_mut().ok_or("clipboard not available")?;
    cb.set_text(&text).map_err(|e| e.to_string())?;
    tracing::debug!(len = text.len(), "Wrote text to clipboard");
    Ok(())
}

#[tauri::command]
async fn read_image(state: tauri::State<'_, ClipboardState>) -> Result<Vec<u8>, String> {
    let mut guard = state.0.lock().map_err(|e| e.to_string())?;
    let cb = guard.as_mut().ok_or("clipboard not available")?;
    let img = cb.get_image().map_err(|e| e.to_string())?;
    Ok(img.bytes.into_owned())
}

#[tauri::command]
async fn write_image(
    width: usize,
    height: usize,
    rgba: Vec<u8>,
    state: tauri::State<'_, ClipboardState>,
) -> Result<(), String> {
    let mut guard = state.0.lock().map_err(|e| e.to_string())?;
    let cb = guard.as_mut().ok_or("clipboard not available")?;
    let img = arboard::ImageData {
        width,
        height,
        bytes: rgba.into(),
    };
    cb.set_image(img).map_err(|e| e.to_string())?;
    tracing::debug!(width, height, "Wrote image to clipboard");
    Ok(())
}

#[tauri::command]
async fn has_text(state: tauri::State<'_, ClipboardState>) -> Result<bool, String> {
    let mut guard = state.0.lock().map_err(|e| e.to_string())?;
    let cb = guard.as_mut().ok_or("clipboard not available")?;
    Ok(cb.get_text().is_ok())
}

#[tauri::command]
async fn clear(state: tauri::State<'_, ClipboardState>) -> Result<(), String> {
    let mut guard = state.0.lock().map_err(|e| e.to_string())?;
    let cb = guard.as_mut().ok_or("clipboard not available")?;
    cb.clear().map_err(|e| e.to_string())
}
