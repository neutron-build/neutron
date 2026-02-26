# Neutron Mobile Preview

On-device preview app for Neutron Native. Scan a QR code, see your app live on device instantly — the Expo Go for Neutron. Written in Go. 5-6MB installed vs Expo Go's 50-80MB.

## Philosophy

Same idea as Expo Go. Better execution. No SDK version locking, no paid cloud builds, no 50MB binary. All common device APIs compiled in from day one.

## Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| Language | Go (core logic) + Swift/Kotlin (platform shell) | gomobile is experimental and incompatible with Android NDK 24+ |
| JS Runtime | QuickJS-NG 0.9.0 | ~800KB, ES2023+, deterministic, no JIT warmup — 2,693 commits, actively maintained |
| Layout | Yoga 3 | Flexbox → absolute UIKit/Android View positions |
| Renderer | Go → CGO → UIKit / Android Views | Real native views, not a WebView |
| Bundler | esbuild (delta mode) | 10-100x faster than Metro cold start |
| Hot reload | WebSocket + delta bundles | <400ms end-to-end |

## vs Expo Go

| | Neutron Preview | Expo Go |
|-|----------------|---------|
| Installed size | ~5-6 MB | ~50-80 MB |
| SDK locking | None | Breaks on each SDK update |
| Custom native modules | All compiled in | Requires paid EAS Build |
| JS runtime | Preact 3KB | React 42KB |
| Bundler cold start | ~500ms (esbuild) | 20-60s (Metro) |
| Reconnect on sleep | Auto (exponential backoff) | Often requires re-scan |

## What We Took From Expo Go

- **QR code → instant connect** — the best DX innovation in mobile tooling. Kept exactly.
- **Fast Refresh state preservation** — stateful hot reload, not full reload. Preact signals survive module re-evaluation naturally.
- **Shake-to-debug menu** — universally understood. Kept.
- **Delta bundling** — only changed modules sent over the wire. A 200-line change sends ~500 bytes, not 500KB.
- **LAN + tunnel fallback** — local network default, ngrok-style tunnel for restrictive networks.

## What We Fixed

- **No SDK version concept** — the preview app is a runtime that executes arbitrary JS bundles. The bundle declares its own Preact version. Nothing locks.
- **All device APIs compiled in** — camera, location, sensors, notifications, filesystem, haptics, network info, secure storage, clipboard, audio. No "custom native module" wall.
- **Offline mode** — last bundle cached to disk. App shows cached version if dev server is unreachable.
- **Auto-reconnect** — exponential backoff on disconnect, no re-scan required after sleep or network switch.

## JS Runtime: QuickJS-NG

Three candidates evaluated:

- **V8** — eliminated. 6-8MB stripped ARM64, engineered for JIT on large-memory machines. Violates light-core philosophy.
- **Hermes** — rejected for this use case. Hermes's advantage is AOT `.hbc` compilation at build time. In a hot reload app, the bundle arrives fresh every change — you cannot pre-compile it. Hermes's headline feature vanishes.
- **QuickJS-NG** — chosen. ~800KB static ARM64, ES2023+, MIT licensed, Go CGO bindings via `github.com/buke/quickjs-go`. Interpreter-only = no JIT warmup pauses, deterministic frame times. Cold start single-digit milliseconds.

## Architecture

```
neutron dev --mobile
    └── esbuild watcher starts
         └── QR code printed to terminal
              └── Neutron Preview app scans QR
                   └── WebSocket connection established (LAN or tunnel)
                        └── Full bundle sent on connect
                             └── QuickJS-NG evaluates bundle
                                  └── Preact renders component tree
                                       └── HostConfig translates to native view commands
                                            └── UIKit (iOS) / Android View system

On file change:
    └── esbuild rebuilds changed module (~5ms)
         └── module_delta sent over WebSocket
              └── QuickJS-NG re-evaluates module
                   └── Preact Fast Refresh swaps component subtree
                        └── Signals preserve state automatically
                             └── <400ms end-to-end
```

## Native Rendering

Go owns the JS engine and module system. Native views are rendered via platform-native wrappers — Go is not used for native UI calls directly (gomobile is experimental and incompatible with Android NDK 24+):

- **iOS**: Go (logic) → Swift → UIKit
- **Android**: Go (logic) → Kotlin → Android View system

Go exposes a C-compatible ABI; the thin Swift/Kotlin shells call it over FFI. Yoga 3 computes flexbox layout, producing absolute pixel positions. Those positions become `UIView.frame` on iOS and `View.layout()` bounds on Android. Same approach as React Native Fabric, without the framework overhead.

## Device API Module System

All APIs compiled in, none initialized until first access. Each module implements:

```
Module.Initialize()   — called on first JS property access, triggers OS permission request
Module.JSBindings()   — exposes API to QuickJS
Module.Permissions()  — declares required OS permissions
```

Permission dialogs appear at the point of use, not all at launch.

## Hot Reload Protocol

Custom JSON over WebSocket. No Metro dependency.

The bundle is cached to disk keyed by `{serverIP}:{port}:{hash}`. On reconnect, the app sends its known hash. The server sends only deltas since last connection, or `full_reload` if the hash is stale.

## Binary Size Budget

| Component | Size |
|-----------|------|
| Go runtime | ~3.0 MB |
| QuickJS-NG | ~0.8 MB |
| Preact + renderer (JS) | ~0.06 MB |
| Yoga 3 | ~0.3 MB |
| Device modules | ~0.25 MB |
| Platform shell | ~0.5 MB |
| **Total installed** | **~5-6 MB** |

## Availability

- iOS — App Store
- Android — Google Play

## Why Not gomobile

gomobile (`golang.org/x/mobile`) is **experimental** — the Go team explicitly marks it "use at your own risk" with no end-user support. Critically, it is incompatible with modern Android NDK 24+ due to outdated assumptions about the SDK/NDK configuration. The type support is also limited to a small subset of Go types, causing friction with any non-trivial data structures.

Instead: Go core logic is compiled to a C-compatible `.so` / `.dylib` that Swift (iOS) and Kotlin (Android) call over FFI. This is the same model used by SQLite, LevelDB, and other embedded C libraries — well-understood, NDK-compatible, zero experimental risk.

## File Structure

```
mobile-preview/
├── main.go                     # Entry point — boot QuickJS, connect to dev server
├── internal/
│   ├── bridge/                 # QuickJS-NG CGO layer
│   ├── renderer/               # Platform-split: ios.go / android.go
│   ├── layout/                 # Yoga 3 bindings (CGO → C++)
│   ├── modules/                # Device APIs
│   │   ├── camera/
│   │   ├── location/
│   │   ├── notifications/
│   │   ├── haptics/
│   │   └── storage/
│   └── hotreload/              # WebSocket client + delta bundler
├── js/
│   └── renderer/               # Preact HostConfig + Fast Refresh runtime
├── platform/
│   ├── ios/                    # Swift wrapper: loads Go .dylib over FFI, passes view commands to UIKit
│   └── android/                # Kotlin wrapper: loads Go .so over JNI/FFI, passes commands to Android Views
└── go.mod
```

## Status

Planned — not yet implemented.
