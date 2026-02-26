# Neutron Desktop

Cross-platform desktop apps using Tauri 2.0 + Preact + Neutron Rust. ~10MB bundles, ~30MB idle memory, full Nucleus database embedded in-process.

## Philosophy

Light core, modular OS integrations. The core shell is Tauri 2.0 (window management, WebView, distribution). Everything else — file system, notifications, tray, auto-update — is an opt-in module. You never pay for what you don't use.

## Stack

| Layer | Technology |
|-------|-----------|
| Frontend | Preact + @preact/signals + Neutron Router |
| Builder | Tauri 2.0 (wry → system WebView) |
| Backend | Neutron Rust (full middleware pipeline) |
| Database | Nucleus (embedded, in-process) |
| Bundler | Vite |

## vs Electron

| | Neutron Desktop | Electron |
|-|----------------|---------|
| Bundle size | ~10MB | ~150MB |
| Idle memory | ~30-40MB | ~150-300MB |
| Backend | Rust (Neutron) | Node.js |
| Security | Sandboxed + Rust | Full Node access |
| Rendering | System WebView | Bundled Chromium |
| Local DB | Nucleus embedded | Manual setup |

## How the Backend Works

The frontend makes standard `fetch()` calls — identical to Neutron TS web apps. A `neutron://` custom protocol intercepts them and routes through the full Neutron Rust middleware pipeline (auth, logging, rate limiting, etc.). No open TCP port. No attack surface. Zero frontend code changes when moving a route between web and desktop.

```
fetch("neutron://localhost/api/users")
    → Tauri custom protocol handler
    → Neutron Rust Router (full middleware pipeline)
    → Handler → Nucleus query
    → Response
```

Direct `invoke()` calls are reserved only for native OS operations (window management, file dialogs, tray) where typed bindings are auto-generated at build time.

## Component Sharing with Neutron TS

The same Preact component works on web (Neutron TS) and desktop if it follows two rules:
1. No direct I/O — all data via props or signals
2. No platform APIs in component body — use `PlatformContext` instead

```tsx
// Works on web AND desktop — zero changes
export function UserList({ users }: { users: User[] }) {
  return (
    <ul>
      {users.map(u => <li key={u.id}>{u.name}</li>)}
    </ul>
  )
}
```

Platform differences (navigation, file system, notifications) are injected via `PlatformContext` at the app root.

## Nucleus Embedding

Nucleus runs in-process — no TCP socket, no IPC overhead, ~0ms query latency vs network. Data lives in the platform app data directory:
- Windows: `%APPDATA%\com.neutron.{app}\nucleus\`
- macOS: `~/Library/Application Support/com.neutron.{app}/nucleus/`
- Linux: `~/.local/share/com.neutron.{app}/nucleus/`

SQL migrations are embedded at compile time and run at startup.

## App Setup

```rust
// src-tauri/src/lib.rs
use neutron_desktop::NeutronDesktopBuilder;
use neutron::Router;

pub fn run() {
    let router = Router::new()
        .get("/api/users", handlers::list_users)
        .post("/api/users", handlers::create_user);

    NeutronDesktopBuilder::new()
        .router(router)
        .nucleus_embedded()
        .plugin(neutron_desktop_fs::init())
        .plugin(neutron_desktop_tray::init())
        .plugin(neutron_desktop_updater::init("https://releases.example.com/latest.json"))
        .window(|w| w.title("My App").size(1200, 800))
        .run()
        .expect("failed to run");
}
```

## Module System

Core is minimal. OS integrations are opt-in:

| Crate | JS Package | Provides |
|-------|-----------|---------|
| `neutron-desktop` | `@neutron/desktop` | Core: window, bridge, Nucleus |
| `neutron-desktop-fs` | `@neutron/desktop-fs` | File system, drag-and-drop, watch |
| `neutron-desktop-notifications` | `@neutron/desktop-notifications` | OS notifications |
| `neutron-desktop-tray` | `@neutron/desktop-tray` | System tray icon + menu |
| `neutron-desktop-updater` | `@neutron/desktop-updater` | Auto-update with signature verification |
| `neutron-desktop-shell` | `@neutron/desktop-shell` | Open files/URLs with OS default app |
| `neutron-desktop-clipboard` | `@neutron/desktop-clipboard` | Clipboard read/write |
| `neutron-desktop-global-hotkeys` | `@neutron/desktop-global-hotkeys` | System-wide keyboard shortcuts |
| `neutron-desktop-autostart` | `@neutron/desktop-autostart` | Launch at OS startup |
| `neutron-desktop-window-state` | `@neutron/desktop-window-state` | Persist window size/position |
| `neutron-desktop-deeplink` | `@neutron/desktop-deeplink` | Custom URI scheme deep links |
| `neutron-desktop-biometrics` | `@neutron/desktop-biometrics` | TouchID / Windows Hello |

## Auto-Update

Built-in signature verification (cannot be disabled — correct default). Non-blocking UI by default. Rollback if app crashes on first launch after update.

## Distribution

| Platform | Format | Signing |
|---------|--------|---------|
| Windows | `.msi`, `.exe` | EV cert via Azure Key Vault HSM |
| macOS | `.dmg` (universal) | Developer ID + notarization |
| Linux | `.AppImage`, `.deb`, `.rpm` | SHA256 checksums |

## Dev Mode

```bash
neutron desktop dev      # Vite HMR + cargo-watch, instant frontend reload
neutron desktop build    # production build for current platform
neutron desktop release  # tag, build, sign, upload, update manifest
```

## File Structure

```
desktop/
├── apps/example/               # Reference app
│   ├── src/                    # Preact frontend
│   │   ├── routes/             # File-based routes (same format as Neutron TS)
│   │   ├── platform/desktop.ts # PlatformContext desktop implementation
│   │   └── main.tsx
│   └── src-tauri/
│       ├── src/
│       │   ├── lib.rs          # NeutronDesktopBuilder setup
│       │   ├── bridge.rs       # neutron:// protocol bridge
│       │   ├── nucleus_state.rs
│       │   ├── migrations.rs
│       │   └── commands/       # Native OS commands only
│       ├── migrations/
│       └── tauri.conf.json
├── crates/
│   ├── neutron-desktop/        # Core crate
│   ├── neutron-desktop-fs/
│   ├── neutron-desktop-tray/
│   └── ...                     # One crate per module
├── packages/
│   ├── neutron-shared/         # Shared Preact components (web + desktop)
│   └── @neutron/desktop*/      # TypeScript bindings per module
└── Cargo.toml
```

## Key Design Decisions

- **`neutron://` protocol, not `localhost`** — no open TCP port, no firewall alerts, no port conflicts
- **Nucleus in-process** — zero IPC overhead, `Arc<Mutex<Client>>` handles concurrent access
- **Signals over `useState`** — desktop apps run for hours; signals don't leak across navigations
- **File-based routes identical to Neutron TS** — move route files between web and desktop with zero changes
- **No CSP relaxation** — strict by default; `neutron://` and `tauri://` explicitly allowed
- **Auto-generated native bindings** — no magic strings in `invoke()` calls

## Status

Planned — not yet implemented.
