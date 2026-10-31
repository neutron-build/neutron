# Neutron Desktop

Cross-platform desktop apps using Tauri 2.0 + Preact + Neutron Rust. Full Nucleus database embedded in-process. Bundle size (~10MB) and idle memory (~30MB) are **design targets, not measured results** — no build-size measurement exists in the repo yet.

## Philosophy

Light core, modular OS integrations. The core shell is Tauri 2.0 (window management, WebView, distribution). Everything else — file system, notifications, tray, auto-update — is an opt-in module. You never pay for what you don't use.

## Stack

| Layer | Technology |
|-------|-----------|
| Frontend | Preact + @preact/signals + Neutron Router |
| Builder | Tauri 2.0 (wry → system WebView) |
| Backend | Synchronous typed desktop bridge handlers |
| Database | Nucleus (embedded, in-process) |
| Bundler | Vite |

## vs Electron

The size and memory rows are the design targets above, not measurements;
Electron's figures are its commonly cited ballpark.

| | Neutron Desktop (target) | Electron (typical) |
|-|----------------|---------|
| Bundle size | ~10MB | ~150MB |
| Idle memory | ~30-40MB | ~150-300MB |
| Backend | Rust (Neutron) | Node.js |
| Security | Sandboxed + Rust | Full Node access |
| Rendering | System WebView | Bundled Chromium |
| Local DB | Nucleus embedded | Manual setup |

## How the Backend Works

The frontend makes standard `fetch()` calls — identical to Neutron TS web apps. A `neutron://` custom protocol dispatches typed desktop bridge handlers. Applications supply authorization and middleware in those handlers. Production uses no development TCP listener; WebView content and IPC permissions still require an application security policy.

```
fetch("neutron://localhost/api/users")
    → Tauri custom protocol handler
    → Desktop bridge router
    → Handler → Nucleus query
    → Response
```

Direct `invoke()` calls are reserved only for native OS operations (window management, file dialogs, tray) through the hand-written TypeScript adapters in this package.

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
use neutron_desktop::Response;

pub fn run() {
    NeutronDesktopBuilder::new()
        .get("/api/users", |_request| Response::json(&vec!["example"]))
        .nucleus_embedded()
        .plugin(neutron_desktop_fs::init())
        .plugin(neutron_desktop_tray::init())
        .window(|w| w.title("My App").size(1200, 800))
        .run(tauri::generate_context!())
        .expect("failed to run");
}
```

## Module System

Core is minimal. OS integrations are opt-in (12 plugin crates; one shared TypeScript package, `@neutron/desktop`):

| Crate | Provides |
|-------|----------|
| `neutron-desktop` | Core: window, `neutron://` bridge, dev server, embedded Nucleus |
| `neutron-desktop-fs` | File system |
| `neutron-desktop-notifications` | OS notifications |
| `neutron-desktop-tray` | System tray icon + menu |
| `neutron-desktop-updater` | Auto-update with signature verification |
| `neutron-desktop-shell` | Open files/URLs with OS default app |
| `neutron-desktop-clipboard` | Clipboard read/write |
| `neutron-desktop-global-hotkeys` | System-wide keyboard shortcuts |
| `neutron-desktop-autostart` | Launch at OS startup |
| `neutron-desktop-window-state` | Persist window size/position |
| `neutron-desktop-deeplink` | Custom URI scheme deep links |
| `neutron-desktop-biometrics` | macOS Touch ID; other platforms unsupported |

## Auto-Update

Initialize with `init_secure(endpoint, trusted_minisign_key, channel)` in Rust. Metadata and payload signatures are mandatory. Verified stages and signed metadata persist across restart. An interrupted installer is not automatically retried: the next process must reauthenticate the selection and explicitly retry through Tauri. Desktop automatic crash rollback is not implemented.

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
├── examples/starter/           # Reference app
│   ├── src/                    # Preact frontend
│   └── src-tauri/
├── crates/
│   ├── neutron-desktop/        # Core crate (window, bridge, dev server, Nucleus state)
│   ├── neutron-desktop-fs/
│   ├── neutron-desktop-tray/
│   └── ...                     # One crate per module (12 plugins)
├── packages/desktop/           # @neutron/desktop — TypeScript package
├── scripts/                    # icon generation, macOS/Windows signing
└── Cargo.toml
```

## Key Design Decisions

- **`neutron://` protocol, not `localhost`** — no open TCP port, no firewall alerts, no port conflicts
- **Nucleus in-process** — zero IPC overhead, `Arc<Mutex<Client>>` handles concurrent access
- **Signals over `useState`** — desktop apps run for hours; signals don't leak across navigations
- **Explicit desktop handlers** — register typed bridge routes in the Rust builder
- **Application CSP ownership** — the starter currently has `csp: null`; provision and verify a policy for the shipped WebView content
- **Explicit native adapters** — TypeScript wrappers call the Tauri plugin commands

## Status

Implemented and tested in-tree: `desktop.yml` runs `cargo test --workspace`
(including an embedded-Nucleus lifecycle test) and the TypeScript package's
tests; `desktop-release.yml` owns the signed release path. Platform surface
still evolving — see the note on size targets above.

Desktop storage defaults come from Tauri's configured application identifier.
Legacy shared directories are not adopted automatically. Stop the legacy app
and explicitly call `migrate_legacy_storage(source, destination)` to copy a
selected store to an unused destination; it preserves the source and a backup,
refuses links and conflicts, and requires the caller to keep both stores offline
throughout migration. This helper can migrate either the old Nucleus directory
or the old window-state directory.

The updater requires a Rust-provisioned trusted key and HTTPS endpoint through
`init_secure`. Its dynamic response must include signed `channel`, `size`,
`sha256` and `metadataSignature` fields. The metadata signature authenticates
fixed-order JSON fields `version`, `channel`, `download_url`, `signature`,
`sha256`, `size`; payload signatures use Tauri's Minisign encoding. Downloads
are bounded, verified and staged in app-local storage; installation delegates
to Tauri's supported installer. Legacy `init` and IPC trust mutation fail closed.
Private stage files and signed selection records survive restart. Call
`recover_update` to reacquire the same release through Tauri and reverify its
stage; offline recovery cannot reconstruct a Tauri installer handle and rejects.
An `installing` record never auto-retries: only a new explicit install request
can do so after recovery. Automatic desktop crash rollback is not implemented.
Owner-only durable staging currently supports Unix; other storage backends
reject until their ACL and directory-sync implementation is provided.

File selection delegates to Tauri's native dialog plugin. Notification delivery
uses the OS backend; scheduling has cancellable IDs, while permission queries
return an explicit unsupported error because this backend has no permission API.
Animated native platform behavior and signed installer execution need OS tests.
