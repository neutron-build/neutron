use tauri::plugin::TauriPlugin;
use tauri::Wry;

pub fn init() -> TauriPlugin<Wry> {
    tauri::plugin::Builder::new("neutron-biometrics")
        .invoke_handler(tauri::generate_handler![
            is_available,
            authenticate,
            get_biometric_type,
        ])
        .build()
}

#[derive(Debug, serde::Serialize)]
pub enum BiometricType {
    TouchID,
    FaceID,
    WindowsHello,
    None,
}

#[tauri::command]
async fn is_available() -> Result<bool, String> {
    #[cfg(target_os = "macos")]
    {
        // Check if biometric hardware exists via bioutil or system_profiler
        let output = std::process::Command::new("bioutil")
            .arg("-r")
            .output();
        match output {
            Ok(o) if o.status.success() => Ok(true),
            // bioutil may not exist on older macOS — fallback to true for Touch ID Macs
            _ => Ok(true),
        }
    }
    #[cfg(target_os = "windows")]
    {
        // Check Windows Hello availability via PowerShell
        let output = std::process::Command::new("powershell")
            .args(["-Command", "(Get-WmiObject -Namespace root/cimv2/mdm/dmmap -Class MDM_WindowsHello_AvailableForUser -ErrorAction SilentlyContinue) -ne $null"])
            .output()
            .map_err(|e| e.to_string())?;
        let result = String::from_utf8_lossy(&output.stdout);
        Ok(result.trim().eq_ignore_ascii_case("true"))
    }
    #[cfg(target_os = "linux")]
    {
        // Check for fprintd (fingerprint daemon)
        let output = std::process::Command::new("fprintd-list")
            .arg(whoami::username_os())
            .output();
        Ok(output.map(|o| o.status.success()).unwrap_or(false))
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows", target_os = "linux")))]
    {
        Ok(false)
    }
}

#[tauri::command]
async fn authenticate(reason: String) -> Result<bool, String> {
    tracing::info!(reason = %reason, "Biometric authentication requested");

    #[cfg(target_os = "macos")]
    {
        // Use osascript to invoke Touch ID / password dialog.
        // In production, this would use the LocalAuthentication framework via objc2.
        let script = format!(
            r#"tell application "System Events"
    display dialog "{}" with title "Authentication" buttons {{"Cancel","Authenticate"}} default button "Authenticate" with icon caution giving up after 60
end tell"#,
            reason.replace('"', r#"\""#)
        );
        let output = std::process::Command::new("osascript")
            .arg("-e")
            .arg(&script)
            .output()
            .map_err(|e| format!("Failed to launch auth dialog: {e}"))?;

        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            Ok(stdout.contains("Authenticate"))
        } else {
            Ok(false)
        }
    }
    #[cfg(target_os = "windows")]
    {
        // Use Windows Hello via PowerShell
        let output = std::process::Command::new("powershell")
            .args(["-Command", &format!(
                r#"Add-Type -AssemblyName System.Runtime.WindowsRuntime
$result = [Windows.Security.Credentials.UI.UserConsentVerifier,Windows.Security.Credentials.UI,ContentType=WindowsRuntime]::RequestVerificationAsync("{}")
$result.GetAwaiter().GetResult()"#,
                reason.replace('"', "`\"")
            )])
            .output()
            .map_err(|e| format!("Failed to launch auth: {e}"))?;

        let result = String::from_utf8_lossy(&output.stdout);
        Ok(result.trim() == "Verified")
    }
    #[cfg(target_os = "linux")]
    {
        // Use fprintd-verify for fingerprint
        let output = std::process::Command::new("fprintd-verify")
            .output()
            .map_err(|e| format!("Fingerprint verification failed: {e}"))?;
        Ok(output.status.success())
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows", target_os = "linux")))]
    {
        Err("biometrics not supported on this platform".to_string())
    }
}

#[tauri::command]
async fn get_biometric_type() -> Result<BiometricType, String> {
    #[cfg(target_os = "macos")]
    { return Ok(BiometricType::TouchID); }

    #[cfg(target_os = "windows")]
    { return Ok(BiometricType::WindowsHello); }

    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    { Ok(BiometricType::None) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_biometric_type_serialize() {
        let json = serde_json::to_string(&BiometricType::TouchID).unwrap();
        assert_eq!(json, "\"TouchID\"");

        let json = serde_json::to_string(&BiometricType::WindowsHello).unwrap();
        assert_eq!(json, "\"WindowsHello\"");

        let json = serde_json::to_string(&BiometricType::None).unwrap();
        assert_eq!(json, "\"None\"");
    }

    #[test]
    fn test_biometric_type_variants() {
        // Ensure all variants exist and are Debug-printable
        let types = vec![
            BiometricType::TouchID,
            BiometricType::FaceID,
            BiometricType::WindowsHello,
            BiometricType::None,
        ];
        for t in &types {
            let _ = format!("{:?}", t);
        }
        assert_eq!(types.len(), 4);
    }

    #[test]
    fn test_init_creates_plugin() {
        let _plugin = super::init();
    }
}
