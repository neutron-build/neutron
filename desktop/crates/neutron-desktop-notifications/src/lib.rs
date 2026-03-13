use serde::{Deserialize, Serialize};
use tauri::plugin::TauriPlugin;
use tauri::Wry;

pub fn init() -> TauriPlugin<Wry> {
    tauri::plugin::Builder::new("neutron-notifications")
        .invoke_handler(tauri::generate_handler![
            send_notification,
            request_permission,
            is_permission_granted,
            schedule_notification,
            cancel_notification,
        ])
        .build()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Notification {
    pub title: String,
    pub body: Option<String>,
    pub icon: Option<String>,
    pub sound: Option<String>,
    pub timeout: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ScheduledNotification {
    pub id: String,
    pub notification: Notification,
    pub delay_ms: u64,
}

#[tauri::command]
async fn send_notification(notification: Notification) -> Result<(), String> {
    tracing::info!(title = %notification.title, "Sending OS notification");

    let mut n = notify_rust::Notification::new();
    n.summary(&notification.title);

    if let Some(body) = &notification.body {
        n.body(body);
    }
    if let Some(icon) = &notification.icon {
        n.icon(icon);
    }
    if let Some(timeout) = notification.timeout {
        n.timeout(notify_rust::Timeout::Milliseconds(timeout));
    }

    n.show().map_err(|e| e.to_string())?;
    Ok(())
}

#[tauri::command]
async fn schedule_notification(scheduled: ScheduledNotification) -> Result<(), String> {
    let delay = scheduled.delay_ms;
    let notification = scheduled.notification;
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        let mut n = notify_rust::Notification::new();
        n.summary(&notification.title);
        if let Some(body) = &notification.body {
            n.body(body);
        }
        let _ = n.show();
    });
    Ok(())
}

#[tauri::command]
async fn cancel_notification(_id: String) -> Result<(), String> {
    // notify-rust doesn't support cancellation by ID on all platforms.
    // On macOS, this would require UNUserNotificationCenter via objc.
    tracing::debug!(id = %_id, "Cancel notification (platform-specific)");
    Ok(())
}

#[tauri::command]
async fn request_permission() -> Result<bool, String> {
    // Desktop platforms generally don't require explicit permission.
    // macOS 10.14+ does, but notify-rust handles it transparently.
    Ok(true)
}

#[tauri::command]
async fn is_permission_granted() -> Result<bool, String> {
    Ok(true)
}
