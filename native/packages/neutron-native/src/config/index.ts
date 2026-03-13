/**
 * neutron.config.ts schema — defineConfig + full type definitions.
 *
 * Import from '@neutron/native/config' in the project root neutron.config.ts.
 * This replaces metro.config.js, babel.config.js, app.json, eas.json.
 */

// ─── Permission descriptions ───────────────────────────────────────────────

export interface NeutronPermissions {
  /** iOS NSCameraUsageDescription / Android CAMERA */
  camera?: string
  /** iOS NSLocationWhenInUseUsageDescription / Android ACCESS_FINE_LOCATION */
  location?: string
  /** iOS NSMicrophoneUsageDescription / Android RECORD_AUDIO */
  microphone?: string
  /** iOS NSContactsUsageDescription / Android READ_CONTACTS */
  contacts?: string
  /** iOS NSCalendarsUsageDescription / Android READ_CALENDAR */
  calendars?: string
  /** iOS NSPhotoLibraryUsageDescription / Android READ_MEDIA_IMAGES */
  photoLibrary?: string
  /** iOS NSFaceIDUsageDescription */
  faceID?: string
  /** Android POST_NOTIFICATIONS */
  notifications?: string
  /** Android BLUETOOTH_CONNECT */
  bluetooth?: string
  /** Additional platform-specific permissions */
  [key: string]: string | undefined
}

// ─── Plugin configuration ──────────────────────────────────────────────────

/** A plugin is either a package name string or a [name, options] tuple */
export type NeutronPlugin =
  | string
  | [name: string, options: Record<string, unknown>]

// ─── OTA update configuration ──────────────────────────────────────────────

export interface NeutronOTAConfig {
  /** URL of the OTA update endpoint (serves bundle metadata + chunk diffs) */
  endpoint: string
  /** Ed25519 public key for bundle signature verification (PEM or base64) */
  publicKey?: string
  /** Check interval in seconds (default: 3600) */
  checkInterval?: number
  /** Apply updates on next launch vs. immediately (default: 'next-launch') */
  updateStrategy?: 'next-launch' | 'immediate'
  /** Channel name for staged rollouts (e.g. 'production', 'beta', 'canary') */
  channel?: string
}

// ─── Platform-specific overrides ─────────────────────────────────────────

export interface NeutronIOSConfig {
  /** iOS deployment target (default: '15.0') */
  deploymentTarget?: string
  /** Additional Info.plist entries */
  infoPlist?: Record<string, unknown>
  /** Additional CocoaPods to add to the Podfile */
  extraPods?: Array<{ name: string; version?: string; options?: string }>
  /** Build configurations beyond Debug/Release */
  buildConfigurations?: string[]
  /** Capabilities (e.g. push notifications, associated domains) */
  capabilities?: Record<string, boolean | Record<string, unknown>>
}

export interface NeutronAndroidConfig {
  /** Android min SDK version (default: 24) */
  minSdkVersion?: number
  /** Android target SDK version (default: 34) */
  targetSdkVersion?: number
  /** Android compile SDK version (default: 34) */
  compileSdkVersion?: number
  /** Additional android/app/build.gradle entries */
  gradle?: Record<string, unknown>
  /** Additional AndroidManifest.xml attributes */
  manifest?: Record<string, unknown>
  /** Signing config for release builds */
  signing?: {
    storeFile: string
    storePassword: string
    keyAlias: string
    keyPassword: string
  }
}

// ─── Rspack/Re.Pack bundler overrides ────────────────────────────────────

export interface NeutronBundlerConfig {
  /**
   * Additional Rspack resolve aliases (merged with Preact compat defaults).
   * Neutron always sets: react → preact/compat, react-dom → preact/compat.
   */
  alias?: Record<string, string>
  /**
   * Additional Rspack module rules (appended after the built-in rules).
   * Use this for SVG loaders, custom asset types, etc.
   */
  rules?: unknown[]
  /**
   * Additional Rspack plugins.
   */
  plugins?: unknown[]
  /**
   * Enable Module Federation for micro-frontend native bundles.
   * Also enables OTA chunk-level updates.
   */
  moduleFederation?: {
    name: string
    exposes?: Record<string, string>
    remotes?: Record<string, string>
    shared?: Record<string, unknown>
  }
  /**
   * Entry file override (default: './index.js').
   */
  entry?: string
}

// ─── Root config ──────────────────────────────────────────────────────────

export interface NeutronConfig {
  // ── Identity ──────────────────────────────────────────────────────────

  /** Human-readable app name shown on device home screen */
  name: string

  /** Bundle ID / application ID */
  bundleId:
    | string
    | {
        ios: string
        android: string
      }

  /** Semantic version string (e.g. '1.0.0') */
  version: string

  /** Build number — incremented for each App Store / Play Store submission */
  buildNumber?: number

  // ── Assets ────────────────────────────────────────────────────────────

  /** Path to app icon (1024×1024 PNG, no transparency on iOS) */
  icon?: string

  /**
   * Path to splash/launch screen image.
   * Neutron generates all platform splash variants from this single asset.
   */
  splash?: {
    image: string
    /** Background color shown while image loads (hex) */
    backgroundColor?: string
    /** 'contain' keeps aspect ratio; 'cover' fills screen (default: 'contain') */
    resizeMode?: 'contain' | 'cover' | 'native'
  }

  /**
   * Custom fonts to bundle.
   * Pass an array of .ttf / .otf paths — Neutron auto-links them on iOS and
   * adds them to assets on Android.
   */
  fonts?: string[]

  // ── Permissions ───────────────────────────────────────────────────────

  /**
   * Permission usage descriptions.
   * On iOS these become Info.plist entries.
   * On Android Neutron adds the corresponding uses-permission tags.
   */
  permissions?: NeutronPermissions

  // ── Plugins ───────────────────────────────────────────────────────────

  /**
   * Config plugins — applied during `neutron prebuild` to modify the generated
   * native iOS/Android projects. Each plugin may add native code, CocoaPods,
   * Gradle dependencies, etc.
   *
   * String form: '@neutron/native-camera'
   * Tuple form:  ['@neutron/native-notifications', { mode: 'production' }]
   */
  plugins?: NeutronPlugin[]

  // ── OTA updates ──────────────────────────────────────────────────────

  /**
   * Over-the-air update configuration.
   * When set, `@neutron/ota` is automatically included and the update check
   * is wired into app startup.
   */
  ota?: NeutronOTAConfig

  // ── Deep linking ─────────────────────────────────────────────────────

  /**
   * Deep link URL scheme (e.g. 'myapp').
   * The router uses the file structure to generate link handlers automatically.
   * Set this if you need a custom scheme other than the bundle ID.
   */
  scheme?: string

  /**
   * Associated domains for universal links (iOS) / App Links (Android).
   * Example: ['applinks:myapp.com']
   */
  associatedDomains?: string[]

  // ── Platform overrides ────────────────────────────────────────────────

  /** iOS-specific configuration */
  ios?: NeutronIOSConfig

  /** Android-specific configuration */
  android?: NeutronAndroidConfig

  // ── Bundler ──────────────────────────────────────────────────────────

  /**
   * Rspack/Re.Pack bundler overrides.
   * Neutron provides a complete default config — only override what you need.
   */
  bundler?: NeutronBundlerConfig

  // ── Development ───────────────────────────────────────────────────────

  /**
   * Development-only settings. Ignored in production builds.
   */
  dev?: {
    /** Dev server port (default: 8081) */
    port?: number
    /** Dev server host (default: 'localhost') */
    host?: string
    /** Enable Flipper integration (default: false) */
    flipper?: boolean
  }
}

// ─── defineConfig ─────────────────────────────────────────────────────────

/**
 * Define a Neutron Native project configuration with full TypeScript inference.
 *
 * @example
 * ```ts
 * // neutron.config.ts
 * import { defineConfig } from '@neutron/native/config'
 *
 * export default defineConfig({
 *   name: 'My App',
 *   bundleId: 'com.example.myapp',
 *   version: '1.0.0',
 * })
 * ```
 */
export function defineConfig(config: NeutronConfig): NeutronConfig {
  return config
}

/**
 * Load and resolve a neutron.config.ts file.
 * Used internally by the CLI — not intended for app code.
 */
export async function loadConfig(configPath: string): Promise<NeutronConfig> {
  // Dynamic import handles both .ts (via ts-node/tsx/jiti) and compiled .js
  const mod = await import(configPath) as { default?: NeutronConfig } | NeutronConfig
  const config = 'default' in mod ? (mod as { default: NeutronConfig }).default : mod as NeutronConfig
  if (!config || typeof config !== 'object') {
    throw new Error(`neutron.config.ts must export a default NeutronConfig object`)
  }
  if (!config.name) throw new Error('neutron.config.ts: "name" is required')
  if (!config.bundleId) throw new Error('neutron.config.ts: "bundleId" is required')
  if (!config.version) throw new Error('neutron.config.ts: "version" is required')
  return config
}
