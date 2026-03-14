/**
 * TurboModule Registry — manages lazy loading of native modules via JSI.
 *
 * Modules are registered by name and instantiated on first access.
 * The registry acts as a singleton — there is exactly one per JS runtime.
 *
 * Resolution order:
 *   1. Cache (already resolved)
 *   2. Native JSI registry (__turboModuleProxy)
 *   3. Web implementation (when running in a browser)
 *   4. JS-side factory (registered via registerModule — stubs)
 *
 * Usage:
 *   import { getModule } from '@neutron/native/turbomodule'
 *   const camera = getModule<CameraModule>('NeutronCamera')
 */

import type { TurboModule } from './types.js'

// ─── Platform detection ─────────────────────────────────────────────────────

/**
 * Detect whether we are running in a web browser (not React Native).
 *
 * Uses feature detection rather than user-agent sniffing:
 *   - `window` and `document` exist (DOM environment)
 *   - `navigator.product` is NOT 'ReactNative' (excludes RN WebView)
 */
export function isWeb(): boolean {
  return typeof window !== 'undefined'
    && typeof document !== 'undefined'
    && !(typeof navigator !== 'undefined' && navigator.product === 'ReactNative')
}

// ─── JSI global access ──────────────────────────────────────────────────────

interface TurboModuleRegistry {
  get(name: string): TurboModule | null
  getEnforcing(name: string): TurboModule
}

function getNativeRegistry(): TurboModuleRegistry | null {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const g = globalThis as any
  return g.__turboModuleProxy ?? g.TurboModuleRegistry ?? null
}

// ─── Module cache ────────────────────────────────────────────────────────────

const _cache = new Map<string, TurboModule>()
const _factories = new Map<string, () => TurboModule>()
const _webImplementations = new Map<string, () => TurboModule>()

/**
 * Register a JS-side factory for a TurboModule.
 * Used by each device module to register its spec + fallback implementation.
 */
export function registerModule<T extends TurboModule>(
  name: string,
  factory: () => T,
): void {
  _factories.set(name, factory)
}

/**
 * Register a web implementation for a TurboModule.
 *
 * Web implementations use browser APIs (getUserMedia, Geolocation,
 * Vibration, Clipboard, Notification, WebAuthn, localStorage, etc.)
 * and are only resolved when `isWeb()` returns true.
 *
 * The web factory is preferred over the JS stub but comes after
 * native JSI — so on React Native it is never used.
 */
export function registerWebModule<T extends TurboModule>(
  name: string,
  factory: () => T,
): void {
  _webImplementations.set(name, factory)
}

/**
 * Get a TurboModule by name. Resolution order:
 * 1. Cache (already resolved)
 * 2. Native JSI registry (__turboModuleProxy)
 * 3. Web implementation (browser APIs — only when isWeb() is true)
 * 4. JS-side factory (registered via registerModule — stubs)
 *
 * Returns null if not found anywhere.
 */
export function getModule<T extends TurboModule>(name: string): T | null {
  // 1. Cache hit
  const cached = _cache.get(name)
  if (cached) return cached as T

  // 2. Try native JSI
  const registry = getNativeRegistry()
  if (registry) {
    const native = registry.get(name)
    if (native) {
      _cache.set(name, native)
      return native as T
    }
  }

  // 3. Web implementation (browser APIs)
  if (isWeb()) {
    const webFactory = _webImplementations.get(name)
    if (webFactory) {
      const mod = webFactory() as T
      _cache.set(name, mod)
      return mod
    }
  }

  // 4. JS factory (stub / polyfill)
  const factory = _factories.get(name)
  if (factory) {
    const mod = factory() as T
    _cache.set(name, mod)
    return mod
  }

  return null
}

/**
 * Get a TurboModule, throwing if not available.
 */
export function requireModule<T extends TurboModule>(name: string): T {
  const mod = getModule<T>(name)
  if (!mod) {
    throw new Error(
      `[neutron-native] TurboModule "${name}" is not available. ` +
      `Ensure the native module is linked and the app was rebuilt.`
    )
  }
  return mod
}

/**
 * Check if a TurboModule is available (native, web, or JS stub).
 */
export function hasModule(name: string): boolean {
  if (_cache.has(name)) return true

  const registry = getNativeRegistry()
  if (registry?.get(name)) return true

  if (isWeb() && _webImplementations.has(name)) return true

  return _factories.has(name)
}

/**
 * List all registered module names (native, web, and JS).
 */
export function listModules(): string[] {
  const names = new Set<string>(_factories.keys())
  for (const name of _webImplementations.keys()) {
    names.add(name)
  }
  // Native registry doesn't expose listing — only add cached native modules
  for (const name of _cache.keys()) {
    names.add(name)
  }
  return Array.from(names)
}

/**
 * Clear the module cache. Used in tests and hot reload.
 */
export function clearCache(): void {
  _cache.clear()
}
