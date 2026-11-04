use serde::{Deserialize, Serialize};
use std::path::{Component, Path, PathBuf};
use tauri::plugin::TauriPlugin;
use tauri::{AppHandle, Manager, Wry};
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

/// The app-owned directory a file command operates in. Every path argument
/// is relative to one of these; absolute paths and `..` are refused, so the
/// WebView can never reach the rest of the file system through this plugin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BaseDir {
    AppData,
    AppLocalData,
    AppConfig,
    AppCache,
    AppLog,
}

fn base_dir_path(app: &AppHandle, base: BaseDir) -> Result<PathBuf, String> {
    let paths = app.path();
    match base {
        BaseDir::AppData => paths.app_data_dir(),
        BaseDir::AppLocalData => paths.app_local_data_dir(),
        BaseDir::AppConfig => paths.app_config_dir(),
        BaseDir::AppCache => paths.app_cache_dir(),
        BaseDir::AppLog => paths.app_log_dir(),
    }
    .map_err(|e| format!("resolve base dir: {e}"))
}

/// Resolve `rel` inside `root`, refusing absolute paths, `..`, and anything
/// whose existing portion resolves (through symlinks) outside `root`. The
/// root is created if missing. An empty `rel` is the root itself.
pub fn resolve_in(root: &Path, rel: &str) -> Result<PathBuf, String> {
    let rel_path = Path::new(rel);
    for component in rel_path.components() {
        if !matches!(component, Component::Normal(_)) {
            return Err(format!(
                "path must be relative to the base directory without '..': {rel}"
            ));
        }
    }
    std::fs::create_dir_all(root).map_err(|e| format!("create base dir: {e}"))?;
    let root = root
        .canonicalize()
        .map_err(|e| format!("resolve base dir: {e}"))?;
    let candidate = root.join(rel_path);

    // Canonicalize the deepest existing ancestor so a symlink inside the base
    // directory cannot point the operation elsewhere.
    let mut existing = candidate.as_path();
    while std::fs::symlink_metadata(existing).is_err() {
        existing = existing
            .parent()
            .ok_or_else(|| format!("path escapes the base directory: {rel}"))?;
    }
    let resolved = existing
        .canonicalize()
        .map_err(|e| format!("resolve path: {e}"))?;
    if !resolved.starts_with(&root) {
        return Err(format!("path escapes the base directory: {rel}"));
    }
    Ok(candidate)
}

fn resolve(app: &AppHandle, base: BaseDir, path: &str) -> Result<PathBuf, String> {
    resolve_in(&base_dir_path(app, base)?, path)
}

fn resolve_non_root(app: &AppHandle, base: BaseDir, path: &str) -> Result<PathBuf, String> {
    if path.is_empty() {
        return Err("path must name an entry inside the base directory".into());
    }
    resolve(app, base, path)
}

#[tauri::command]
async fn read_file(app: AppHandle, base: BaseDir, path: String) -> Result<Vec<u8>, String> {
    let full = resolve_non_root(&app, base, &path)?;
    tokio::fs::read(&full)
        .await
        .map_err(|e| format!("read_file failed: {e}"))
}

#[tauri::command]
async fn write_file(
    app: AppHandle,
    base: BaseDir,
    path: String,
    contents: Vec<u8>,
) -> Result<(), String> {
    let full = resolve_non_root(&app, base, &path)?;
    if let Some(parent) = full.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| format!("create parent dir: {e}"))?;
    }
    tokio::fs::write(&full, contents)
        .await
        .map_err(|e| format!("write_file failed: {e}"))
}

/// Lists a directory. Each entry's `path` is relative to the base directory,
/// ready to pass back to the other commands.
#[tauri::command]
async fn read_dir(app: AppHandle, base: BaseDir, path: String) -> Result<Vec<DirEntry>, String> {
    let full = resolve(&app, base, &path)?;
    let mut entries = Vec::new();
    let mut dir = tokio::fs::read_dir(&full)
        .await
        .map_err(|e| format!("read_dir failed: {e}"))?;

    while let Ok(Some(entry)) = dir.next_entry().await {
        let metadata = entry
            .metadata()
            .await
            .unwrap_or_else(|_| std::fs::metadata(entry.path()).expect("metadata"));
        let name = entry.file_name().to_string_lossy().to_string();
        entries.push(DirEntry {
            path: Path::new(&path).join(&name).to_string_lossy().to_string(),
            name,
            is_dir: metadata.is_dir(),
            size: metadata.len(),
        });
    }
    Ok(entries)
}

#[tauri::command]
async fn create_dir(
    app: AppHandle,
    base: BaseDir,
    path: String,
    recursive: bool,
) -> Result<(), String> {
    let full = resolve_non_root(&app, base, &path)?;
    if recursive {
        tokio::fs::create_dir_all(&full).await
    } else {
        tokio::fs::create_dir(&full).await
    }
    .map_err(|e| format!("create_dir failed: {e}"))
}

#[tauri::command]
async fn remove_file(app: AppHandle, base: BaseDir, path: String) -> Result<(), String> {
    let full = resolve_non_root(&app, base, &path)?;
    tokio::fs::remove_file(&full)
        .await
        .map_err(|e| format!("remove_file failed: {e}"))
}

#[tauri::command]
async fn remove_dir(
    app: AppHandle,
    base: BaseDir,
    path: String,
    recursive: bool,
) -> Result<(), String> {
    let full = resolve_non_root(&app, base, &path)?;
    if recursive {
        tokio::fs::remove_dir_all(&full).await
    } else {
        tokio::fs::remove_dir(&full).await
    }
    .map_err(|e| format!("remove_dir failed: {e}"))
}

#[tauri::command]
async fn exists(app: AppHandle, base: BaseDir, path: String) -> Result<bool, String> {
    let full = resolve(&app, base, &path)?;
    Ok(tokio::fs::try_exists(&full).await.unwrap_or(false))
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

#[cfg(test)]
mod tests {
    use super::resolve_in;

    fn temp_root(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "neutron-fs-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn relative_paths_resolve_inside_the_root() {
        let root = temp_root("ok");
        let canon = root.canonicalize().unwrap();
        assert_eq!(resolve_in(&root, "a/b.txt").unwrap(), canon.join("a/b.txt"));
        assert_eq!(resolve_in(&root, "").unwrap(), canon);
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn absolute_and_parent_paths_are_refused() {
        let root = temp_root("escape");
        for bad in ["/etc/passwd", "../outside", "a/../../outside", "./a"] {
            assert!(resolve_in(&root, bad).is_err(), "{bad} must be refused");
        }
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_out_of_the_root_are_refused() {
        let root = temp_root("link");
        let outside = temp_root("outside");
        std::os::unix::fs::symlink(&outside, root.join("out")).unwrap();
        assert!(resolve_in(&root, "out/secret.txt").is_err());
        assert!(resolve_in(&root, "out").is_err());
        std::os::unix::fs::symlink(outside.join("missing"), root.join("dangling")).unwrap();
        assert!(resolve_in(&root, "dangling").is_err());
        std::fs::remove_dir_all(&root).unwrap();
        std::fs::remove_dir_all(&outside).unwrap();
    }

    #[test]
    fn missing_root_is_created() {
        let root = temp_root("create").join("nested");
        assert!(resolve_in(&root, "x").is_ok());
        assert!(root.is_dir());
        std::fs::remove_dir_all(root.parent().unwrap()).unwrap();
    }
}
