use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tauri::Wry;
use tauri::plugin::TauriPlugin;
use tauri_plugin_dialog::DialogExt;

/// Initialize the filesystem plugin.
pub fn init() -> TauriPlugin<Wry> {
    tauri::plugin::Builder::new("neutron-fs")
        .setup(|app, _| {
            app.plugin(tauri_plugin_dialog::init())?;
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            read_file,
            write_file,
            read_dir,
            create_dir,
            remove_file,
            remove_dir,
            exists,
            show_open_dialog,
            show_save_dialog,
        ])
        .build()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileFilter {
    pub name: String,
    pub extensions: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DirEntry {
    pub name: String,
    pub path: String,
    pub is_dir: bool,
    pub size: u64,
}

#[tauri::command]
async fn read_file(path: String) -> Result<Vec<u8>, String> {
    tokio::fs::read(&path)
        .await
        .map_err(|e| format!("read_file failed: {e}"))
}

#[tauri::command]
async fn write_file(path: String, contents: Vec<u8>) -> Result<(), String> {
    if let Some(parent) = PathBuf::from(&path).parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| format!("create parent dir: {e}"))?;
    }
    tokio::fs::write(&path, contents)
        .await
        .map_err(|e| format!("write_file failed: {e}"))
}

#[tauri::command]
async fn read_dir(path: String) -> Result<Vec<DirEntry>, String> {
    let mut entries = Vec::new();
    let mut dir = tokio::fs::read_dir(&path)
        .await
        .map_err(|e| format!("read_dir failed: {e}"))?;

    while let Ok(Some(entry)) = dir.next_entry().await {
        let metadata = entry
            .metadata()
            .await
            .unwrap_or_else(|_| std::fs::metadata(entry.path()).expect("metadata"));
        entries.push(DirEntry {
            name: entry.file_name().to_string_lossy().to_string(),
            path: entry.path().to_string_lossy().to_string(),
            is_dir: metadata.is_dir(),
            size: metadata.len(),
        });
    }
    Ok(entries)
}

#[tauri::command]
async fn create_dir(path: String, recursive: bool) -> Result<(), String> {
    if recursive {
        tokio::fs::create_dir_all(&path).await
    } else {
        tokio::fs::create_dir(&path).await
    }
    .map_err(|e| format!("create_dir failed: {e}"))
}

#[tauri::command]
async fn remove_file(path: String) -> Result<(), String> {
    tokio::fs::remove_file(&path)
        .await
        .map_err(|e| format!("remove_file failed: {e}"))
}

#[tauri::command]
async fn remove_dir(path: String, recursive: bool) -> Result<(), String> {
    if recursive {
        tokio::fs::remove_dir_all(&path).await
    } else {
        tokio::fs::remove_dir(&path).await
    }
    .map_err(|e| format!("remove_dir failed: {e}"))
}

#[tauri::command]
async fn exists(path: String) -> Result<bool, String> {
    Ok(tokio::fs::try_exists(&path).await.unwrap_or(false))
}

#[tauri::command]
async fn show_open_dialog(
    app: tauri::AppHandle,
    filters: Option<Vec<FileFilter>>,
    multiple: Option<bool>,
    directory: Option<bool>,
) -> Result<Vec<String>, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let mut dialog = app.dialog().file();
        for filter in filters.unwrap_or_default() {
            dialog = dialog.add_filter(
                filter.name,
                &filter
                    .extensions
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
            );
        }
        let files = match (directory.unwrap_or(false), multiple.unwrap_or(false)) {
            (true, true) => dialog.blocking_pick_folders(),
            (true, false) => dialog.blocking_pick_folder().map(|p| vec![p]),
            (false, true) => dialog.blocking_pick_files(),
            (false, false) => dialog.blocking_pick_file().map(|p| vec![p]),
        };
        files
            .unwrap_or_default()
            .into_iter()
            .map(|p| {
                p.into_path()
                    .map(|p| p.to_string_lossy().into_owned())
                    .map_err(|e| e.to_string())
            })
            .collect()
    })
    .await
    .map_err(|e| e.to_string())?
}

#[tauri::command]
async fn show_save_dialog(
    app: tauri::AppHandle,
    default_path: Option<String>,
    filters: Option<Vec<FileFilter>>,
) -> Result<Option<String>, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let mut dialog = app.dialog().file();
        if let Some(path) = default_path {
            let path = std::path::PathBuf::from(path);
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                dialog = dialog.set_directory(parent);
            }
            if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
                dialog = dialog.set_file_name(name);
            } else {
                return Err("Save path requires a UTF-8 file name".into());
            }
        }
        for filter in filters.unwrap_or_default() {
            dialog = dialog.add_filter(
                filter.name,
                &filter
                    .extensions
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
            );
        }
        dialog
            .blocking_save_file()
            .map(|p| {
                p.into_path()
                    .map(|p| p.to_string_lossy().into_owned())
                    .map_err(|e| e.to_string())
            })
            .transpose()
    })
    .await
    .map_err(|e| e.to_string())?
}
