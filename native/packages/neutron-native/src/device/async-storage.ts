/**
 * AsyncStorage — persistent key-value storage wrapping
 * @react-native-async-storage/async-storage and expo-secure-store.
 *
 * Peer dependencies (install one):
 *   - @react-native-async-storage/async-storage (community standard)
 *   - expo-secure-store (Expo — for encrypted storage)
 *
 * Falls back to an in-memory Map when no native module is linked (useful in tests).
 *
 * @module @neutron/native/device/async-storage
 */

// ─── Lazy module loaders ────────────────────────────────────────────────────

/* eslint-disable @typescript-eslint/no-explicit-any */
let _asyncStorage: any = undefined
let _expoSecureStore: any = undefined

function getAsyncStorage(): any {
  if (_asyncStorage === undefined) {
    try {
      const mod = require('@react-native-async-storage/async-storage')
      _asyncStorage = mod.default ?? mod
    } catch {
      _asyncStorage = null
    }
  }
  return _asyncStorage
}

function getExpoSecureStore(): any {
  if (_expoSecureStore === undefined) {
    try { _expoSecureStore = require('expo-secure-store') } catch { _expoSecureStore = null }
  }
  return _expoSecureStore
}
/* eslint-enable @typescript-eslint/no-explicit-any */

// ─── In-memory fallback ─────────────────────────────────────────────────────

const _memStore = new Map<string, string>()

// ─── Public API ─────────────────────────────────────────────────────────────

/**
 * Get a value by key from persistent storage.
 *
 * @param key - The storage key.
 * @returns The stored value, or null if the key does not exist.
 *
 * @example
 * ```ts
 * import { getItem } from '@neutron/native/device/async-storage'
 * const token = await getItem('auth.token')
 * ```
 */
export async function getItem(key: string): Promise<string | null> {
  const storage = getAsyncStorage()
  if (storage) {
    return storage.getItem(key)
  }

  // Expo SecureStore can be used as an alternative (encrypted, but limited to ~2KB per value)
  const secureStore = getExpoSecureStore()
  if (secureStore) {
    return secureStore.getItemAsync(key)
  }

  // In-memory fallback
  return _memStore.get(key) ?? null
}

/**
 * Set a key-value pair in persistent storage.
 *
 * @param key - The storage key.
 * @param value - The string value to store.
 *
 * @example
 * ```ts
 * import { setItem } from '@neutron/native/device/async-storage'
 * await setItem('auth.token', 'abc123')
 * ```
 */
export async function setItem(key: string, value: string): Promise<void> {
  const storage = getAsyncStorage()
  if (storage) {
    await storage.setItem(key, value)
    return
  }

  const secureStore = getExpoSecureStore()
  if (secureStore) {
    await secureStore.setItemAsync(key, value)
    return
  }

  _memStore.set(key, value)
}

/**
 * Remove a key from persistent storage.
 *
 * @param key - The storage key to remove.
 *
 * @example
 * ```ts
 * import { removeItem } from '@neutron/native/device/async-storage'
 * await removeItem('auth.token')
 * ```
 */
export async function removeItem(key: string): Promise<void> {
  const storage = getAsyncStorage()
  if (storage) {
    await storage.removeItem(key)
    return
  }

  const secureStore = getExpoSecureStore()
  if (secureStore) {
    await secureStore.deleteItemAsync(key)
    return
  }

  _memStore.delete(key)
}

/**
 * Get all storage keys.
 *
 * @returns Array of all stored keys.
 *
 * @example
 * ```ts
 * import { getAllKeys } from '@neutron/native/device/async-storage'
 * const keys = await getAllKeys()
 * console.log('Stored keys:', keys)
 * ```
 */
export async function getAllKeys(): Promise<string[]> {
  const storage = getAsyncStorage()
  if (storage) {
    const keys = await storage.getAllKeys()
    return Array.isArray(keys) ? keys : []
  }

  // expo-secure-store does not support listing keys
  // Return in-memory keys as fallback
  return Array.from(_memStore.keys())
}

/**
 * Clear all data from persistent storage.
 *
 * Use with caution — this removes all keys and values.
 *
 * @example
 * ```ts
 * import { clear } from '@neutron/native/device/async-storage'
 * await clear()
 * ```
 */
export async function clear(): Promise<void> {
  const storage = getAsyncStorage()
  if (storage) {
    await storage.clear()
    return
  }

  // expo-secure-store does not support clear; remove known keys from memory
  _memStore.clear()
}

/**
 * Get multiple values by keys in a single batch operation.
 *
 * @param keys - Array of storage keys.
 * @returns Array of [key, value] pairs. Value is null if key does not exist.
 *
 * @example
 * ```ts
 * import { multiGet } from '@neutron/native/device/async-storage'
 * const results = await multiGet(['user.name', 'user.email'])
 * results.forEach(([key, value]) => console.log(key, value))
 * ```
 */
export async function multiGet(
  keys: string[],
): Promise<[string, string | null][]> {
  const storage = getAsyncStorage()
  if (storage) {
    const results = await storage.multiGet(keys)
    return results as [string, string | null][]
  }

  // Fall back to individual getItem calls
  const results: [string, string | null][] = []
  for (const key of keys) {
    results.push([key, await getItem(key)])
  }
  return results
}

/**
 * Set multiple key-value pairs in a single batch operation.
 *
 * @param pairs - Array of [key, value] pairs to set.
 *
 * @example
 * ```ts
 * import { multiSet } from '@neutron/native/device/async-storage'
 * await multiSet([
 *   ['user.name', 'Alice'],
 *   ['user.email', 'alice@example.com'],
 * ])
 * ```
 */
export async function multiSet(pairs: [string, string][]): Promise<void> {
  const storage = getAsyncStorage()
  if (storage) {
    await storage.multiSet(pairs)
    return
  }

  // Fall back to individual setItem calls
  for (const [key, value] of pairs) {
    await setItem(key, value)
  }
}

/**
 * Remove multiple keys in a single batch operation.
 *
 * @param keys - Array of storage keys to remove.
 *
 * @example
 * ```ts
 * import { multiRemove } from '@neutron/native/device/async-storage'
 * await multiRemove(['user.name', 'user.email'])
 * ```
 */
export async function multiRemove(keys: string[]): Promise<void> {
  const storage = getAsyncStorage()
  if (storage) {
    await storage.multiRemove(keys)
    return
  }

  for (const key of keys) {
    await removeItem(key)
  }
}

/**
 * Merge a value with an existing value for a key.
 *
 * Both values must be valid JSON strings. The merge performs a shallow merge
 * of the parsed objects.
 *
 * @param key - The storage key.
 * @param value - JSON string to merge with the existing value.
 *
 * @example
 * ```ts
 * import { mergeItem } from '@neutron/native/device/async-storage'
 * await setItem('settings', JSON.stringify({ theme: 'dark' }))
 * await mergeItem('settings', JSON.stringify({ fontSize: 16 }))
 * // Result: { theme: 'dark', fontSize: 16 }
 * ```
 */
export async function mergeItem(key: string, value: string): Promise<void> {
  const storage = getAsyncStorage()
  if (storage?.mergeItem) {
    await storage.mergeItem(key, value)
    return
  }

  // Manual merge fallback
  const existing = await getItem(key)
  if (existing) {
    try {
      const merged = { ...JSON.parse(existing), ...JSON.parse(value) }
      await setItem(key, JSON.stringify(merged))
    } catch {
      // If either value is not valid JSON, overwrite
      await setItem(key, value)
    }
  } else {
    await setItem(key, value)
  }
}
