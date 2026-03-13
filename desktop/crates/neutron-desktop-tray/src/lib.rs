use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use tauri::plugin::TauriPlugin;
use tauri::tray::{TrayIconBuilder, MouseButton, MouseButtonState};
use tauri::menu::{MenuBuilder, MenuItemBuilder, CheckMenuItemBuilder, SubmenuBuilder};
use tauri::{AppHandle, Manager, Wry};

struct TrayState {
    icon_id: Mutex<Option<String>>,
}

pub fn init() -> TauriPlugin<Wry> {
    tauri::plugin::Builder::new("neutron-tray")
        .setup(|app, _api| {
            app.manage(TrayState {
                icon_id: Mutex::new(None),
            });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            create_tray,
            set_tray_menu,
            set_tray_tooltip,
            destroy_tray,
        ])
        .build()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrayMenuItem {
    pub id: String,
    pub label: String,
    pub enabled: bool,
    pub checked: Option<bool>,
    pub accelerator: Option<String>,
    pub submenu: Option<Vec<TrayMenuItem>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TrayConfig {
    pub tooltip: Option<String>,
    pub menu: Option<Vec<TrayMenuItem>>,
}

#[tauri::command]
async fn create_tray(
    app: AppHandle,
    config: TrayConfig,
    state: tauri::State<'_, TrayState>,
) -> Result<String, String> {
    let id = format!("neutron-tray-{}", std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis());

    let mut builder = TrayIconBuilder::with_id(&id);

    if let Some(tooltip) = &config.tooltip {
        builder = builder.tooltip(tooltip);
    }

    if let Some(items) = &config.menu {
        let menu = build_menu(&app, items)?;
        builder = builder.menu(&menu);
    }

    builder = builder.on_tray_icon_event(|_tray, event| {
        match event {
            tauri::tray::TrayIconEvent::Click { button: MouseButton::Left, button_state: MouseButtonState::Up, .. } => {
                tracing::debug!("Tray icon left-clicked");
            }
            _ => {}
        }
    });

    builder.build(&app).map_err(|e| e.to_string())?;

    *state.icon_id.lock().map_err(|e| e.to_string())? = Some(id.clone());
    tracing::info!(id = %id, "Created system tray");
    Ok(id)
}

fn build_menu(app: &AppHandle, items: &[TrayMenuItem]) -> Result<tauri::menu::Menu<Wry>, String> {
    let mut menu = MenuBuilder::new(app);
    for item in items {
        if let Some(ref sub_items) = item.submenu {
            let mut sub = SubmenuBuilder::new(app, &item.label);
            for sub_item in sub_items {
                if let Some(true) = sub_item.checked {
                    sub = sub.item(&CheckMenuItemBuilder::new(&sub_item.label)
                        .id(&sub_item.id)
                        .enabled(sub_item.enabled)
                        .checked(true)
                        .build(app).map_err(|e| e.to_string())?);
                } else {
                    sub = sub.item(&MenuItemBuilder::new(&sub_item.label)
                        .id(&sub_item.id)
                        .enabled(sub_item.enabled)
                        .build(app).map_err(|e| e.to_string())?);
                }
            }
            menu = menu.item(&sub.build().map_err(|e| e.to_string())?);
        } else if let Some(true) = item.checked {
            menu = menu.item(&CheckMenuItemBuilder::new(&item.label)
                .id(&item.id)
                .enabled(item.enabled)
                .checked(true)
                .build(app).map_err(|e| e.to_string())?);
        } else {
            menu = menu.item(&MenuItemBuilder::new(&item.label)
                .id(&item.id)
                .enabled(item.enabled)
                .build(app).map_err(|e| e.to_string())?);
        }
    }
    menu.build().map_err(|e| e.to_string())
}

#[tauri::command]
async fn set_tray_menu(
    app: AppHandle,
    items: Vec<TrayMenuItem>,
    state: tauri::State<'_, TrayState>,
) -> Result<(), String> {
    let id = state.icon_id.lock().map_err(|e| e.to_string())?;
    if let Some(id) = id.as_ref() {
        let menu = build_menu(&app, &items)?;
        if let Some(tray) = app.tray_by_id(id) {
            tray.set_menu(Some(menu)).map_err(|e| e.to_string())?;
        }
    }
    tracing::info!(count = items.len(), "Updated tray menu");
    Ok(())
}

#[tauri::command]
async fn set_tray_tooltip(
    app: AppHandle,
    tooltip: String,
    state: tauri::State<'_, TrayState>,
) -> Result<(), String> {
    let id = state.icon_id.lock().map_err(|e| e.to_string())?;
    if let Some(id) = id.as_ref() {
        if let Some(tray) = app.tray_by_id(id) {
            tray.set_tooltip(Some(&tooltip)).map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

#[tauri::command]
async fn destroy_tray(
    app: AppHandle,
    state: tauri::State<'_, TrayState>,
) -> Result<(), String> {
    let mut id = state.icon_id.lock().map_err(|e| e.to_string())?;
    if let Some(tray_id) = id.take() {
        if let Some(tray) = app.tray_by_id(&tray_id) {
            drop(tray);
        }
        tracing::info!(id = %tray_id, "Destroyed system tray");
    }
    Ok(())
}
