/**
 * AsyncStorage TurboModule — persistent key-value storage.
 *
 * iOS: UserDefaults (small) / filesystem (large)
 * Android: SharedPreferences (small) / filesystem (large)
 *
 * Replaces @react-native-async-storage/async-storage with a TurboModule.
 */

import type { TurboModule, ModuleMethod } from '../types.js'
import { getModule, registerModule } from '../registry.js'

export interface AsyncStorageModule extends TurboModule {
  moduleName: 'NeutronAsyncStorage'

  /** Get a value by key. Returns null if not found. */
  getItem(key: string): Promise<string | null>

  /** Set a key-value pair. */
  setItem(key: string, value: string): Promise<void>

  /** Remove a key. */
  removeItem(key: string): Promise<void>

  /** Get multiple values by keys. Returns [key, value | null][] */
  multiGet(keys: string[]): Promise<[string, string | null][]>

  /** Set multiple key-value pairs. */
  multiSet(pairs: [string, string][]): Promise<void>

  /** Remove multiple keys. */
  multiRemove(keys: string[]): Promise<void>

  /** Get all keys. */
  getAllKeys(): Promise<string[]>

  /** Clear all stored data. */
  clear(): Promise<void>
}

const METHODS: readonly ModuleMethod[] = [
  { name: 'getItem', kind: 'async' },
  { name: 'setItem', kind: 'async' },
  { name: 'removeItem', kind: 'async' },
  { name: 'multiGet', kind: 'async' },
  { name: 'multiSet', kind: 'async' },
  { name: 'multiRemove', kind: 'async' },
  { name: 'getAllKeys', kind: 'async' },
  { name: 'clear', kind: 'async' },
] as const

// In-memory fallback for when native module isn't linked
const _memStore = new Map<string, string>()

registerModule<AsyncStorageModule>('NeutronAsyncStorage', () => ({
  moduleName: 'NeutronAsyncStorage',
  methods: METHODS,
  async getItem(key) { return _memStore.get(key) ?? null },
  async setItem(key, value) { _memStore.set(key, value) },
  async removeItem(key) { _memStore.delete(key) },
  async multiGet(keys) { return keys.map(k => [k, _memStore.get(k) ?? null] as [string, string | null]) },
  async multiSet(pairs) { for (const [k, v] of pairs) _memStore.set(k, v) },
  async multiRemove(keys) { for (const k of keys) _memStore.delete(k) },
  async getAllKeys() { return Array.from(_memStore.keys()) },
  async clear() { _memStore.clear() },
}))

/**
 * Hook to access the AsyncStorage TurboModule.
 *
 * @example
 * ```tsx
 * const storage = useAsyncStorage()
 * await storage.setItem('user.token', 'abc123')
 * const token = await storage.getItem('user.token')
 * ```
 */
export function useAsyncStorage(): AsyncStorageModule {
  const mod = getModule<AsyncStorageModule>('NeutronAsyncStorage')
  if (!mod) throw new Error('[neutron-native] NeutronAsyncStorage module not available')
  return mod
}
