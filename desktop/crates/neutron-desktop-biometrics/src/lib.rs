use tauri::{plugin::TauriPlugin, Wry};

pub fn init() -> TauriPlugin<Wry> {
    tauri::plugin::Builder::new("neutron-biometrics")
        .invoke_handler(tauri::generate_handler![
            is_available,
            authenticate,
            get_biometric_type
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

#[cfg(target_os = "macos")]
mod macos {
    use block2::RcBlock;
    use objc2::runtime::Bool;
    use objc2_foundation::{NSError, NSString};
    use objc2_local_authentication::{LAContext, LAPolicy};
    use std::sync::mpsc;
    use std::time::Duration;

    pub fn available() -> bool {
        let context = unsafe { LAContext::new() };
        unsafe {
            context
                .canEvaluatePolicy_error(LAPolicy::DeviceOwnerAuthenticationWithBiometrics)
                .is_ok()
        }
    }

    // Run on Tauri's blocking pool, never block the application event loop.
    pub fn authenticate(reason: &str) -> Result<bool, String> {
        let context = unsafe { LAContext::new() };
        unsafe {
            context.canEvaluatePolicy_error(LAPolicy::DeviceOwnerAuthenticationWithBiometrics)
        }
        .map_err(|error| format!("Biometric authentication unavailable: {error}"))?;
        let (sender, receiver) = mpsc::channel();
        let reply = RcBlock::new(move |success: Bool, error: *mut NSError| {
            // OS success and absence of error are both required.
            let _ = sender.send(success.as_bool() && error.is_null());
        });
        unsafe {
            context.evaluatePolicy_localizedReason_reply(
                LAPolicy::DeviceOwnerAuthenticationWithBiometrics,
                &NSString::from_str(reason),
                &reply,
            );
        }
        let result = receiver.recv_timeout(Duration::from_secs(60));
        unsafe { context.invalidate() };
        result.map_err(|_| "Biometric authentication timed out or callback failed".to_string())
    }
}

#[tauri::command]
async fn is_available() -> Result<bool, String> {
    #[cfg(target_os = "macos")]
    {
        Ok(macos::available())
    }
    #[cfg(not(target_os = "macos"))]
    {
        Ok(false)
    }
}

#[tauri::command]
async fn authenticate(reason: String) -> Result<bool, String> {
    if reason.trim().is_empty() {
        return Err("Authentication reason is required".into());
    }
    #[cfg(target_os = "macos")]
    {
        tauri::async_runtime::spawn_blocking(move || macos::authenticate(&reason))
            .await
            .map_err(|e| e.to_string())?
    }
    #[cfg(not(target_os = "macos"))]
    {
        Err("Native biometric verification is unsupported on this platform".into())
    }
}

#[tauri::command]
async fn get_biometric_type() -> Result<BiometricType, String> {
    if !is_available().await? {
        return Ok(BiometricType::None);
    }
    #[cfg(target_os = "macos")]
    {
        Ok(BiometricType::TouchID)
    }
    #[cfg(not(target_os = "macos"))]
    {
        Ok(BiometricType::None)
    }
}
