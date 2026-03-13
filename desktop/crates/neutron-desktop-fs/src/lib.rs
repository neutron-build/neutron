use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tauri::plugin::TauriPlugin;
use tauri::Wry;

/// Initialize the filesystem plugin.
pub fn init() -> TauriPlugin<Wry> {
    tauri::plugin::Builder::new("neutron-fs")
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
        let metadata = entry.metadata().await.unwrap_or_else(|_| {
            std::fs::metadata(entry.path()).expect("metadata")
        });
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
    Ok(tokio::fs::try_exists(&path)
        .await
        .unwrap_or(false))
}

#[tauri::command]
async fn show_open_dialog(
    _filters: Option<Vec<FileFilter>>,
    _multiple: Option<bool>,
    _directory: Option<bool>,
) -> Result<Vec<String>, String> {
    // Tauri's dialog API handles the native file picker
    Ok(Vec::new())
}

#[tauri::command]
async fn show_save_dialog(
    _default_path: Option<String>,
    _filters: Option<Vec<FileFilter>>,
) -> Result<Option<String>, String> {
    Ok(None)
}
