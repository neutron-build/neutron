//! Signed metadata and payload verification, followed by Tauri's supported installer.
//! No automatic rollback is claimed. Provision trust in Rust, never via IPC/update input.
mod persistence;
use base64::Engine;
use minisign_verify::{PublicKey, Signature};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{path::{Path, PathBuf}, sync::Mutex};
use tauri::{Manager, Wry, plugin::TauriPlugin};
use tauri_plugin_updater::{Update, UpdaterExt};
const MAX_PAYLOAD: u64 = 128 * 1024 * 1024;

struct UpdaterState {
    config: UpdateConfig,
    channel: String,
    selected: Mutex<Option<(Update, UpdateInfo)>>,
    metadata_signature: Mutex<Option<String>>,
    install_lock: tokio::sync::Mutex<()>,
}

/// Legacy initialization fails closed until a trust key is supplied via init_secure.
pub fn init(url: &str) -> TauriPlugin<Wry> {
    init_secure(url, "", "stable")
}

/// Key uses Tauri's base64-encoded minisign public-key file format.
pub fn init_secure(url: &str, pubkey: &str, channel: &str) -> TauriPlugin<Wry> {
    let config = UpdateConfig {
        url: url.into(),
        pubkey: pubkey.into(),
        ..Default::default()
    };
    let channel = channel.to_string();
    tauri::plugin::Builder::new("neutron-updater")
        .setup(move |app, _| {
            app.plugin(
                tauri_plugin_updater::Builder::new()
                    .pubkey(config.pubkey.clone())
                    .build(),
            )?;
            app.manage(UpdaterState {
                config: config.clone(),
                channel: channel.clone(),
                selected: Mutex::new(None),
                metadata_signature: Mutex::new(None),
                install_lock: tokio::sync::Mutex::new(()),
            });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            check_for_update,
            recover_update,
            download_update,
            install_update,
            verify_signature,
            get_config,
            set_config
        ])
        .build()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UpdateInfo {
    pub version: String,
    pub notes: Option<String>,
    pub download_url: String,
    pub signature: String,
    pub sha256: String,
    pub size: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateConfig {
    pub url: String,
    pub pubkey: String,
    pub check_interval_secs: u64,
    pub auto_download: bool,
}
impl Default for UpdateConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            pubkey: String::new(),
            check_interval_secs: 3600,
            auto_download: false,
        }
    }
}
#[derive(Serialize)]
struct SignedMetadata<'a> {
    version: &'a str,
    channel: &'a str,
    download_url: &'a str,
    signature: &'a str,
    sha256: &'a str,
    size: u64,
}
fn verify(key: &str, data: &[u8], signature: &str) -> Result<(), String> {
    if key.is_empty() || signature.is_empty() {
        return Err("Trusted key and signature are required".into());
    }
    let decode = |text: &str| -> Result<String, String> {
        String::from_utf8(
            base64::engine::general_purpose::STANDARD
                .decode(text)
                .map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())
    };
    let key = PublicKey::decode(&decode(key)?).map_err(|e| e.to_string())?;
    let signature = Signature::decode(&decode(signature)?).map_err(|e| e.to_string())?;
    key.verify(data, &signature, false)
        .map_err(|e| e.to_string())
}
fn validate_info(info: &UpdateInfo, current: &str) -> Result<(), String> {
    if semver::Version::parse(&info.version).map_err(|e| e.to_string())?
        <= semver::Version::parse(current).map_err(|e| e.to_string())?
        || info.size == 0
        || info.size > MAX_PAYLOAD
        || info.sha256.len() != 64
        || !info
            .sha256
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        || info.signature.is_empty()
        || url::Url::parse(&info.download_url)
            .map_err(|e| e.to_string())?
            .scheme()
            != "https"
    {
        return Err("Invalid update version, size, hash, signature or transport".into());
    }
    Ok(())
}
fn stage_path(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    Ok(app
        .path()
        .app_local_data_dir()
        .map_err(|e| e.to_string())?
        .join("updates")
        .join("verified-update.bin"))
}
fn stage_payload(app: &tauri::AppHandle, info: &UpdateInfo) -> Result<PathBuf, String> {
    // Digest-addressed generations: replacing a selection never overwrites the
    // prior record's payload before the new record is durably published.
    Ok(stage_path(app)?.with_file_name(format!("verified-{}.bin", info.sha256)))
}
#[tauri::command]
async fn check_for_update(
    app: tauri::AppHandle,
    state: tauri::State<'_, UpdaterState>,
) -> Result<Option<UpdateInfo>, String> {
    let _guard = state.install_lock.lock().await;
    *state.selected.lock().map_err(|e| e.to_string())? = None;
    *state.metadata_signature.lock().map_err(|e| e.to_string())? = None;
    if state.config.pubkey.is_empty() {
        return Err("Updater requires a Rust-provisioned trust key".into());
    }
    let endpoint = url::Url::parse(&state.config.url).map_err(|e| e.to_string())?;
    if endpoint.scheme() != "https" {
        return Err("Update endpoint must use HTTPS".into());
    }
    let updater = app
        .updater_builder()
        .endpoints(vec![endpoint])
        .map_err(|e| e.to_string())?
        .pubkey(state.config.pubkey.clone())
        .timeout(std::time::Duration::from_secs(30))
        .version_comparator(|current, release| release.version > current)
        .build()
        .map_err(|e| e.to_string())?;
    let Some(update) = updater.check().await.map_err(|e| e.to_string())? else {
        return Ok(None);
    };
    // Dynamic Tauri manifests must add these authenticated metadata fields.
    let raw = &update.raw_json;
    let channel = raw
        .get("channel")
        .and_then(|v| v.as_str())
        .ok_or("Missing signed channel")?;
    if channel != state.channel {
        return Err("Update channel mismatch".into());
    }
    let info = UpdateInfo {
        version: update.version.clone(),
        notes: update.body.clone(),
        download_url: update.download_url.to_string(),
        signature: update.signature.clone(),
        sha256: raw
            .get("sha256")
            .and_then(|v| v.as_str())
            .ok_or("Missing signed hash")?
            .into(),
        size: raw
            .get("size")
            .and_then(|v| v.as_u64())
            .ok_or("Missing signed size")?,
    };
    validate_info(&info, &update.current_version)?;
    let metadata = serde_json::to_vec(&SignedMetadata {
        version: &info.version,
        channel,
        download_url: &info.download_url,
        signature: &info.signature,
        sha256: &info.sha256,
        size: info.size,
    })
    .map_err(|e| e.to_string())?;
    verify(
        &state.config.pubkey,
        &metadata,
        raw.get("metadataSignature")
            .and_then(|v| v.as_str())
            .ok_or("Missing metadata signature")?,
    )?;
    *state.metadata_signature.lock().map_err(|e| e.to_string())? = Some(
        raw.get("metadataSignature")
            .and_then(|v| v.as_str())
            .ok_or("Missing metadata signature")?
            .into(),
    );
    *state.selected.lock().map_err(|e| e.to_string())? = Some((update, info.clone()));
    Ok(Some(info))
}
#[tauri::command]
async fn download_update(
    app: tauri::AppHandle,
    info: UpdateInfo,
    state: tauri::State<'_, UpdaterState>,
) -> Result<String, String> {
    let _guard = state.install_lock.lock().await;
    let selected = state
        .selected
        .lock()
        .map_err(|e| e.to_string())?
        .clone()
        .ok_or("No authenticated update selected")?;
    if selected.1 != info {
        return Err("Update input differs from authenticated selection".into());
    }
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(120))
        .build()
        .map_err(|e| e.to_string())?;
    let mut response = client
        .get(&info.download_url)
        .send()
        .await
        .map_err(|e| e.to_string())?
        .error_for_status()
        .map_err(|e| e.to_string())?;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|e| e.to_string())? {
        if bytes.len() as u64 + chunk.len() as u64 > info.size {
            return Err("Update exceeds authenticated size".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.len() as u64 != info.size || format!("{:x}", Sha256::digest(&bytes)) != info.sha256 {
        return Err("Payload size/hash mismatch".into());
    }
    verify(&state.config.pubkey, &bytes, &info.signature)?;
    let path = stage_payload(&app, &info)?;
    persistence::publish(&path, &bytes)?;
    let record = StageRecord {
        info: info.clone(),
        channel: state.channel.clone(),
        metadata_signature: state
            .metadata_signature
            .lock()
            .map_err(|e| e.to_string())?
            .clone()
            .ok_or("Missing authenticated metadata")?,
        phase: "ready".into(),
    };
    persistence::publish(
        &stage_path(&app)?.with_extension("json"),
        &serde_json::to_vec(&record).map_err(|e| e.to_string())?,
    )?;
    Ok(path.to_string_lossy().into_owned())
}
#[derive(Serialize, Deserialize)]
struct StageRecord {
    info: UpdateInfo,
    channel: String,
    metadata_signature: String,
    phase: String,
}
fn read_record(app: &tauri::AppHandle, state: &UpdaterState) -> Result<StageRecord, String> {
    let path = stage_path(app)?;
    persistence::private_directory(path.parent().ok_or("Missing stage parent")?)?;
    let record: StageRecord =
        serde_json::from_slice(&read_stage(&path.with_extension("json"), 1024 * 1024)?)
            .map_err(|e| e.to_string())?;
    if record.channel != state.channel || !matches!(record.phase.as_str(), "ready" | "installing") {
        return Err("Invalid persisted stage policy".into());
    }
    validate_info(&record.info, &app.package_info().version.to_string())?;
    verify(
        &state.config.pubkey,
        &serde_json::to_vec(&SignedMetadata {
            version: &record.info.version,
            channel: &record.channel,
            download_url: &record.info.download_url,
            signature: &record.info.signature,
            sha256: &record.info.sha256,
            size: record.info.size,
        })
        .map_err(|e| e.to_string())?,
        &record.metadata_signature,
    )?;
    Ok(record)
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecoveredUpdate {
    pub info: UpdateInfo,
    pub path: String,
    pub interrupted_install: bool,
}
/// Restart recovery reauthenticates through Tauri before returning the durable
/// selection. No installer launches at startup, especially after interruption.
#[tauri::command]
async fn recover_update(
    app: tauri::AppHandle,
    state: tauri::State<'_, UpdaterState>,
) -> Result<RecoveredUpdate, String> {
    let record = read_record(&app, &state)?;
    let selected = check_for_update(app.clone(), state.clone())
        .await?
        .ok_or("Persisted release no longer available; explicit fresh selection required")?;
    if selected != record.info {
        return Err("Persisted release differs from current authenticated selection".into());
    }
    let bytes = read_stage(&stage_payload(&app, &selected)?, selected.size)?;
    if bytes.len() as u64 != selected.size
        || format!("{:x}", Sha256::digest(&bytes)) != selected.sha256
    {
        return Err("Persisted stage corrupt".into());
    }
    verify(&state.config.pubkey, &bytes, &selected.signature)?;
    Ok(RecoveredUpdate {
        path: stage_payload(&app, &selected)?
            .to_string_lossy()
            .into_owned(),
        info: selected,
        interrupted_install: record.phase == "installing",
    })
}
fn read_stage(path: &std::path::Path, limit: u64) -> Result<Vec<u8>, String> {
    use std::io::Read;
    let mut bytes = Vec::new();
    // Bound the read itself: a concurrent file replacement/growth cannot bypass metadata checks.
    let metadata = std::fs::symlink_metadata(path).map_err(|e| e.to_string())?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err("Stage is not a regular private file".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.mode() & 0o077 != 0 {
            return Err("Stage permissions are not owner-only".into());
        }
    }
    std::fs::File::open(path)
        .map_err(|e| e.to_string())?
        .take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    if bytes.len() as u64 > limit {
        return Err("Stage exceeds size limit".into());
    }
    Ok(bytes)
}
#[tauri::command]
async fn install_update(
    app: tauri::AppHandle,
    path: String,
    state: tauri::State<'_, UpdaterState>,
) -> Result<(), String> {
    let _guard = state.install_lock.lock().await;
    let record = read_record(&app, &state)?;
    let expected = stage_payload(&app, &record.info)?;
    if Path::new(&path) != expected.as_path() {
        return Err("Only the app-owned authenticated stage may be installed".into());
    }
    let (update, info) = state
        .selected
        .lock()
        .map_err(|e| e.to_string())?
        .clone()
        .ok_or("No authenticated update selected")?;
    validate_info(&info, &app.package_info().version.to_string())?;
    let bytes = read_stage(&expected, info.size)?;
    if bytes.len() as u64 != info.size || format!("{:x}", Sha256::digest(&bytes)) != info.sha256 {
        return Err("Staged payload was modified".into());
    }
    verify(&state.config.pubkey, &bytes, &info.signature)?;
    let mut record = read_record(&app, &state)?;
    if record.info != info {
        return Err("Durable selection differs from authenticated update".into());
    }
    record.phase = "installing".into();
    persistence::publish(
        &stage_path(&app)?.with_extension("json"),
        &serde_json::to_vec(&record).map_err(|e| e.to_string())?,
    )?;
    // Tauri performs the real platform installation (and reports unsupported bundles).
    update.install(bytes).map_err(|e| e.to_string())?;
    persistence::remove(&stage_path(&app)?.with_extension("json"))?;
    persistence::remove(&expected)?;
    *state.selected.lock().map_err(|e| e.to_string())? = None;
    Ok(())
}
#[tauri::command]
async fn verify_signature(
    app: tauri::AppHandle,
    path: String,
    signature: String,
    state: tauri::State<'_, UpdaterState>,
) -> Result<bool, String> {
    let record = read_record(&app, &state)?;
    let expected = stage_payload(&app, &record.info)?;
    if Path::new(&path) != expected.as_path() {
        return Err("Only app-owned stage can be verified".into());
    }
    let metadata = std::fs::metadata(&expected).map_err(|e| e.to_string())?;
    if metadata.len() > MAX_PAYLOAD {
        return Err("Stage exceeds size limit".into());
    }
    verify(
        &state.config.pubkey,
        &read_stage(&expected, MAX_PAYLOAD)?,
        &signature,
    )?;
    Ok(true)
}
#[tauri::command]
async fn get_config(state: tauri::State<'_, UpdaterState>) -> Result<UpdateConfig, String> {
    Ok(state.config.clone())
}
#[tauri::command]
async fn set_config(_config: UpdateConfig) -> Result<(), String> {
    Err("Updater trust/config is immutable; provision through init_secure in Rust".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_trust_is_rejected() {
        assert!(verify("", b"hello", "").is_err());
        assert!(verify("garbage", b"hello", "garbage").is_err());
    }
    #[test]
    fn rollback_and_limits_rejected() {
        let mut info = UpdateInfo {
            version: "1.0.0".into(),
            notes: None,
            download_url: "https://example.com/a".into(),
            signature: "signature".into(),
            sha256: "a".repeat(64),
            size: 1,
        };
        assert!(validate_info(&info, "1.0.0").is_err());
        info.version = "1.0.1".into();
        assert!(validate_info(&info, "1.0.0").is_ok());
        info.size = MAX_PAYLOAD + 1;
        assert!(validate_info(&info, "1.0.0").is_err());
    }
}
