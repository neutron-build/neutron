/**
 * Runtime capability detection — replaces feature flags and build-time conditionals
 * for APIs that may or may not be available depending on RN version / OS.
 */

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const g = globalThis as any

export const Capabilities = {
  /** True when running under Hermes V1 (RN 0.82+) */
  hermes: Boolean(g.__hermes__),

  /** True when Fabric (new architecture) is enabled */
  fabric: Boolean(g.nativeFabricUIManager ?? g.__fbRCTBridgeEnabled === false),

  /** True when JSI is available (always true on Hermes + Fabric) */
  jsi: Boolean(g.nativeCallSyncHook),

  /** True when running in development mode */
  dev: Boolean(g.__DEV__) || process.env.NODE_ENV === 'development',

  /** True when running in a test environment (Jest) */
  test: process.env.NODE_ENV === 'test',

  /** Turbo Modules / native modules via JSI */
  turboModules: Boolean(g.__turboModuleProxy),
}
