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

OTA requires a complete `NativeOTAAdapter` with native boot tracking and a
trusted public key. Without that adapter the client reports an unsupported
error before fetching or staging. The adapter owns atomic durable staging,
signature/hash verification, pending update publication, native early-boot
crash counting, healthy launch confirmation and rollback to the last good
bundle. JavaScript cannot count crashes that occur before it starts. The core package supplies orchestration and a typed public RN adapter.
`modules/neutron-ota` supplies Swift/CryptoKit and Kotlin/Android storage,
cryptography and pre-JS boot selection source. Hosts must autolink/codegen it,
provision trust and use its native selected loader before JS. These modules
remain subject to compilation and installed-app/device acceptance; see
`modules/neutron-ota/README.md`. Startup publishes failures to its error state. Update methods are async and
can reject; callers must handle rejection and observe the error state.

File-route components may be passed directly, or explicitly as
`{kind: 'component', component: Screen}`. Lazy routes use
`{kind: 'lazy', load: () => import('./Screen'), loading, errorFallback}`. Discovery
never invokes a function to determine its shape. Lazy loading occurs during
React rendering within Suspense and an error boundary.

`useAnimatedStyle` requires `react-native-reanimated`. If unavailable, it throws
an explicit unsupported error; the limited Animated helpers do not supply a
shared-value style subscription. `ReactNavigationRoot` and `NativeStack` delegate native UI to React Navigation 7;
`useStackActions()` targets the current screen's scoped siblings. The public
container ref projects native back/gesture state into router subscriptions,
and replace/forward update the same native root. Installed RN gesture/transition
acceptance remains required. The smaller `Stack`/`Tabs`/`Drawer` implementations
retain synchronous scoped JS rendering without claiming native transitions.
