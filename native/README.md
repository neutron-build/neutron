# Neutron Native

iOS and Android apps using Preact. Same components as your Neutron web app, rendering to real native UIKit and Android Views — not a WebView.

## Philosophy

Light core, real native rendering. Preact (3KB) instead of React (42KB). Re.Pack with Rspack instead of Metro. File-based routing identical to Neutron web. One config file instead of five. The same component that runs on the web renders to a native button on iOS.

## Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| Renderer | preact-reconciler → React Native Fabric (JSI) | Production bridge — don't rebuild what Meta spent a decade on |
| JS engine | Hermes V1 (mandatory, default in RN 0.84) | +10–15% TTI, +2.5% iOS / +7.6% Android startup vs Hermes 0.x |
| Bundler | Re.Pack 5 (Rspack) | 5× faster builds than webpack, Module Federation v2 for OTA updates |
| Navigation | File-based router → React Navigation native stack | Real `UINavigationController` / `FragmentTransaction` |
| Styling | NeutronWind (Tailwind → `StyleSheet.create`) | `className` on web, same `className` on native, zero runtime overhead |
| Native APIs | TurboModules (`@neutron/native-*`) | Typed, opt-in, no bundle weight if unused |

## The Bridge

React Native's Fabric + JSI infrastructure is the substrate. `preact-reconciler` maps Preact's virtual DOM operations to Fabric's `UIManager` JSI calls. The `HostConfig` translates:

- `createInstance` → `UIManager.createView`
- `commitUpdate` → `UIManager.updateView`
- `appendChild` → `UIManager.manageChildren`

Preact re-renders produce the same native view operations as React Native would. No custom C++ bridge. No reinventing what Facebook already verified at scale.

## NeutronWind — Styling

A Re.Pack loader transform converts `className` at build time:

```tsx
// What you write
<View className="p-4 bg-white rounded-xl" />

// What gets compiled
<View style={_styles.c0} />
const _styles = StyleSheet.create({ c0: { padding: 16, backgroundColor: '#ffffff', borderRadius: 12 } })
```

Zero runtime overhead. Same `className` prop works on web (emits CSS) and native (emits StyleSheet). Components that only use Tier 1 primitives and `className` run identically on both platforms.

## Component Tiers

**Tier 1 — Universal** (same code on web and native):

```tsx
import { View, Text, Image, Pressable, TextInput } from 'neutron'
// .web.tsx → <div>, <img>, onClick
// .native.tsx → View, Image, onPress → UIKit/Android Views
```

**Tier 2 — Shared business logic** — any component built from Tier 1 primitives works on both platforms with zero changes:

```tsx
// components/UserCard.tsx — one file, all platforms
export function UserCard({ user, onPress }) {
  return (
    <Pressable onPress={onPress}>
      <View className="p-4 flex-row gap-3">
        <Image src={user.avatar} className="w-10 h-10 rounded-full" />
        <View>
          <Text className="font-semibold">{user.name}</Text>
          <Text className="text-gray-500">{user.email}</Text>
        </View>
      </View>
    </Pressable>
  )
}
// On web: renders to <div>, <img>, etc.
// On native: renders to View, Image, native text
```

**Tier 3 — Native-only** (`import from 'neutron/native'`):

`FlatList`, `ScrollView`, `Modal`, `KeyboardAvoidingView`, `StatusBar` — using these in shared code is a TypeScript error.

## File-Based Routing

Same `app/` convention as Neutron web:

```
app/
├── _layout.tsx        # Root layout — Tab or Stack navigator
├── index.tsx          # / → Home screen
├── users/
│   ├── _layout.tsx    # Stack navigator for users section
│   ├── index.tsx      # /users → User list
│   └── [id].tsx       # /users/123 → User detail
```

Deep linking is automatic from the file structure. React Navigation's `createNativeStackNavigator` provides the real `UINavigationController` transitions and Android back-stack — native gestures, native animations.

## What We Took From Each Framework

| Framework | What we adopted |
|-----------|----------------|
| React Native | Fabric + JSI infrastructure (production-verified bridge), Hermes V1 AOT, TurboModules API |
| Expo | QR-code dev flow (via Neutron Preview), file-based router pattern, Config Plugins concept |
| NativeWind | `className` → `StyleSheet.create` transform — same styling API across platforms |
| Re.Pack 5 | Rspack bundler (5× faster than webpack), Module Federation v2 for self-hosted OTA updates |

## What We Avoided

| Framework | What we avoided |
|-----------|----------------|
| React | 42KB runtime — Preact is 3KB |
| Metro | 20-60s cold start, weak tree-shaking |
| Expo SDK monolith | SDK version locking that breaks preview apps |
| Paid EAS | Cloud builds required for any native feature — use Neutron Preview instead |
| Flutter | Dart (no web code sharing), custom canvas widgets (not native views) |
| Capacitor | WebView shell — animation, scroll physics, input don't feel native |

## Configuration

One file replaces `metro.config.js`, `babel.config.js`, `app.json`, and `eas.json`:

```ts
// neutron.config.ts
export default defineConfig({
  name: 'My App',
  bundleId: 'com.example.myapp',
  icon: './assets/icon.png',
  splash: { image: './assets/splash.png', backgroundColor: '#fff' },
  plugins: [
    '@neutron/native-camera',
    '@neutron/native-location',
  ],
  ota: {
    url: 'https://updates.example.com',
    signingKey: process.env.OTA_SIGNING_KEY,
  },
})
```

## OTA Updates

Self-hosted, no EAS. Module Federation enables chunk-level granularity — a 10MB app can push a 50KB changed chunk instead of a full bundle. Bundles are signed; apps verify before applying.

## Native API Modules

All opt-in, zero bundle weight if unused:

| Package | Provides |
|---------|---------|
| `@neutron/native-camera` | Camera, photo library, QR scanner |
| `@neutron/native-location` | GPS, geofencing |
| `@neutron/native-notifications` | Push + local notifications |
| `@neutron/native-biometrics` | Face ID, Touch ID, Fingerprint |
| `@neutron/native-haptics` | Taptic Engine / vibration |
| `@neutron/native-storage` | Secure keychain/keystore |
| `@neutron/native-sensors` | Accelerometer, gyroscope, barometer |
| `@neutron/native-clipboard` | Clipboard read/write |
| `@neutron/native-audio` | Playback, recording |
| `@neutron/native-share` | OS share sheet |

## Development Flow

```bash
neutron dev --mobile      # start dev server, print QR code
                          # scan with Neutron Preview app on device
                          # changes appear in <400ms, state preserved

neutron build native      # production build (Xcode + Gradle)
neutron release native    # build, sign, submit to App Store / Play Store
```

No Xcode or Android Studio required during development — only needed for final production builds.

## React Native New Architecture

Neutron Native targets **React Native 0.82+**, where the New Architecture (Fabric renderer + TurboModules) is mandatory — `newArchEnabled=false` is silently ignored. As of early 2026, ~83% of EAS-built projects already run it. There is no legacy bridge fallback to maintain.

All `@neutron/native-*` modules are TurboModules from day one — no legacy bridge wrappers, no JSI polyfills.

## Platform Support

| Platform | Min Version |
|----------|------------|
| iOS | 14.0+ |
| Android | 7.0+ (API 24) |

## File Structure

```
native/
├── packages/
│   ├── neutron-native/           # Core renderer
│   │   ├── src/
│   │   │   ├── host-config.ts    # preact-reconciler HostConfig → Fabric
│   │   │   ├── components/       # Tier 1 universal components
│   │   │   │   ├── View.native.tsx
│   │   │   │   ├── View.web.tsx
│   │   │   │   ├── Text.native.tsx
│   │   │   │   ├── Text.web.tsx
│   │   │   │   ├── Image.native.tsx
│   │   │   │   ├── Image.web.tsx
│   │   │   │   ├── Pressable.native.tsx
│   │   │   │   └── TextInput.native.tsx
│   │   │   ├── router/           # File-based router for native
│   │   │   └── index.ts
│   │   └── package.json
│   ├── neutron-native-styling/   # NeutronWind — className → StyleSheet
│   └── neutron-native-cli/       # CLI: dev, build, release
├── modules/                      # @neutron/native-* packages
│   ├── camera/
│   ├── location/
│   └── notifications/
└── examples/
    └── demo-app/                 # Reference app
```

## Status

Planned — not yet implemented.
