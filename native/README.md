# Neutron Native

Mobile apps for iOS and Android with React Native 0.76 (New Architecture /
Fabric), plus web from the same component code via a `preact/compat` alias.
There is **no custom renderer**: on native, React Native's built-in Fabric
renderer creates the views; on web builds the bundler maps `react` to
`preact/compat`, so the same components run on Preact's ~3KB runtime.

## Packages

| Package | What it is |
|---|---|
| `@neutron-build/native` | Core: components, router, navigation, device APIs, gestures, animation, accessibility, virtualized lists, signals, OTA, TurboModules |
| `@neutron-build/native-styling` | NeutronWind — build-time `className` → `StyleSheet.create` (Babel plugin + Rspack loader, generated Tailwind-style token map) |
| `@neutron-build/native-cli` | `new`, `dev`, `run`, `build` commands |

Device APIs (camera, location, notifications, biometrics, haptics, clipboard,
sensors, net-info, async-storage, permissions) ship inside the core package
under `src/device/` — not as separate npm packages.

## Component compatibility

Components are written against React-compatible APIs. On native they render
through Fabric to real UIKit/Android views (not a WebView); on web the same
code compiles to Preact. Platform-only primitives are separated so shared code
stays portable.

## Status

Implemented and tested in-tree (`jest`, per-package `__tests__`); CI
(`native.yml`) installs, builds, and tests on every change to `native/**`.
Still evolving — it is the youngest of the TypeScript packages, so expect API
churn. Example app: `examples/hello-world` (React Native 0.76).

---

*This file replaced a pre-implementation design document (2026-08-19). That
document described a `preact-reconciler` → Fabric JSI bridge ("The Bridge",
"no custom C++ bridge"), Hermes V1 defaults, React Native 0.82+, ten separate
`@neutron-build/native-*` module packages, and `neutron release native` — none
of which match the shipped code, which uses RN 0.76's own renderer with a web
preact/compat alias, in-package device modules, and a four-command CLI. It
ended with "Status: Planned — not yet implemented". Found by the S97 claims
audit.*
