/**
 * Runtime capability detection — replaces feature flags and build-time conditionals
 * for APIs that may or may not be available depending on RN version / OS.
 *
 * These probes are DIAGNOSTIC (NF-NR-15): they describe what appears to be
 * present in the runtime, and must never be used to authorize an operation
 * on their own — validate actual module availability at each capability
 * boundary (the lazy loader try/catch pattern) before relying on an API.
 */

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const g = globalThis as any

export const Capabilities = {
  /** True when running under the Hermes engine. The documented runtime
   * marker is global.HermesInternal (NOT the undocumented __hermes__). */
  hermes: Boolean(g.HermesInternal),

  /** Diagnostic: the Fabric runtime object appears to be present. */
  fabric: Boolean(g.nativeFabricUIManager),

  /** Diagnostic: a synchronous JSI hook appears to be present. */
  jsi: Boolean(g.nativeCallSyncHook ?? g.HermesInternal),

  /** True when running in development mode */
  dev: Boolean(g.__DEV__) || process.env.NODE_ENV === 'development',

  /** True when running in a test environment (Jest) */
  test: process.env.NODE_ENV === 'test',

  /** Diagnostic: the TurboModules proxy appears to be present. */
  turboModules: Boolean(g.__turboModuleProxy),
}
