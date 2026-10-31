use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tauri::plugin::TauriPlugin;
use tauri::{Manager, Wry};
#[derive(Default, Clone)]
struct Scheduled(Arc<Mutex<HashMap<String, tokio::task::AbortHandle>>>);

pub fn init() -> TauriPlugin<Wry> {
    tauri::plugin::Builder::new("neutron-notifications")
        .setup(|app, _| {
            app.manage(Scheduled::default());
            Ok(())
        })
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
    send(&notification)
}

fn send(notification: &Notification) -> Result<(), String> {
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
async fn schedule_notification(
    scheduled: ScheduledNotification,
    state: tauri::State<'_, Scheduled>,
) -> Result<(), String> {
    if scheduled.id.is_empty() {
        return Err("Notification ID is required".into());
    }
    let state = state.inner().clone();
    let mut pending = state.0.lock().map_err(|e| e.to_string())?;
    if pending.contains_key(&scheduled.id) {
        return Err("Notification ID is already scheduled".into());
    }
    let shared = state.clone();
    let id = scheduled.id.clone();
    let task = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(scheduled.delay_ms)).await;
        // Cancellation and dispatch are serialized. Cancellation winning this lock prevents send.
        if let Ok(mut pending) = shared.0.lock()
            && pending.remove(&id).is_some()
            && let Err(error) = send(&scheduled.notification)
        {
            tracing::warn!(%error, "Scheduled notification failed");
        }
    });
    pending.insert(scheduled.id, task.abort_handle());
    Ok(())
}

#[tauri::command]
async fn cancel_notification(id: String, state: tauri::State<'_, Scheduled>) -> Result<(), String> {
    let handle = state
        .inner()
        .0
        .lock()
        .map_err(|e| e.to_string())?
        .remove(&id)
        .ok_or_else(|| "Notification is unknown or already dispatched".to_string())?;
    handle.abort();
    Ok(())
}

#[tauri::command]
async fn request_permission() -> Result<bool, String> {
    Err("This notification backend cannot request OS authorization; configure native notification permission integration".into())
}

#[tauri::command]
async fn is_permission_granted() -> Result<bool, String> {
    Err("This notification backend cannot query OS authorization".into())
}
