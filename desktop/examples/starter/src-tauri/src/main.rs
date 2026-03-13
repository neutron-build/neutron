// Prevents additional console window on Windows in release
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use neutron_desktop::{NeutronDesktopBuilder, Response};

fn main() {
    NeutronDesktopBuilder::new()
        .window(|w| {
            w.title = "Neutron Starter".to_string();
            w.width = 1200.0;
            w.height = 800.0;
        })
        .get("/api/hello", |_req| {
            Response::json(&serde_json::json!({
                "message": "Hello from Neutron Desktop!"
            }))
        })
        .run(tauri::generate_context!())
        .expect("error while running application");
}
