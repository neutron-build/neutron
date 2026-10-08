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
 * @module @neutron-build/native/device/async-storage
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
 * expo-secure-store cannot enumerate or bulk-clear its keys. Every key this
 * module writes through that backend is therefore ALSO recorded in a durable
 * app-owned index (stored in the backend itself), so listing and clearing
 * target exactly what we wrote (NF-NR-07). `clear()` never resolves success
 * unless every indexed persistent key was actually removed.
 */
const SECURE_INDEX_KEY = '__neutron_storage_index__'

async function readSecureIndex(secureStore: any): Promise<string[]> {
  try {
    const raw = await secureStore.getItemAsync(SECURE_INDEX_KEY)
    const parsed = raw ? JSON.parse(raw) : []
    return Array.isArray(parsed) ? parsed.filter(k => typeof k === 'string') : []
  } catch {
    return []
  }
}

async function writeSecureIndex(secureStore: any, keys: string[]): Promise<void> {
  await secureStore.setItemAsync(SECURE_INDEX_KEY, JSON.stringify(keys.sort()))
}

/**
 * Merge externally-known keys into the durable index (explicit legacy
 * migration for values written before the index existed).
 */
export async function importLegacySecureKeys(keys: string[]): Promise<void> {
  const secureStore = getExpoSecureStore()
  if (!secureStore) return
  const index = new Set(await readSecureIndex(secureStore))
  for (const key of keys) index.add(key)
  await writeSecureIndex(secureStore, Array.from(index))
}

/** Thrown by clear() when some persistent keys could not be removed. */
export class IncompleteClearError extends Error {
  constructor(public readonly remainingKeys: string[]) {
    super(`clear() could not remove ${remainingKeys.length} persistent key(s): ${remainingKeys.join(', ')}`)
    this.name = 'IncompleteClearError'
  }
}

/**
 * Get a value by key from persistent storage.
 *
 * @param key - The storage key.
 * @returns The stored value, or null if the key does not exist.
 *
 * @example
 * ```ts
 * import { getItem } from '@neutron-build/native/device/async-storage'
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
 * @example
 * ```ts
 * import { setItem } from '@neutron-build/native/device/async-storage'
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
    // Failure-safe index update: a failed update after a successful write
    // leaves an unindexed (orphan) value — a leak, never data loss.
    const index = new Set(await readSecureIndex(secureStore))
    if (!index.has(key)) {
      index.add(key)
      await writeSecureIndex(secureStore, Array.from(index))
    }
    return
  }

  _memStore.set(key, value)
}

/**
 * Remove a key from persistent storage.
 *
 * @example
 * ```ts
 * import { removeItem } from '@neutron-build/native/device/async-storage'
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
    const index = new Set(await readSecureIndex(secureStore))
    if (index.delete(key)) {
      await writeSecureIndex(secureStore, Array.from(index))
    }
    return
  }

  _memStore.delete(key)
}

/**
 * Get all storage keys.
 *
 * With the SecureStore backend this returns the durable app-owned index —
 * exactly the keys this module wrote (plus any merged via
 * `importLegacySecureKeys`).
 *
 * @example
 * ```ts
 * import { getAllKeys } from '@neutron-build/native/device/async-storage'
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

  const secureStore = getExpoSecureStore()
  if (secureStore) {
    return readSecureIndex(secureStore)
  }

  return Array.from(_memStore.keys())
}

/**
 * Clear all data from persistent storage.
 *
 * Use with caution — this removes all keys and values. With the SecureStore
 * backend, every indexed persistent key is deleted individually; if any
 * deletion fails the call REJECTS with the remaining keys instead of
 * claiming success (NF-NR-07).
 *
 * @example
 * ```ts
 * import { clear } from '@neutron-build/native/device/async-storage'
 * await clear()
 * ```
 */
export async function clear(): Promise<void> {
  const storage = getAsyncStorage()
  if (storage) {
    await storage.clear()
    return
  }

  const secureStore = getExpoSecureStore()
  if (secureStore) {
    const index = await readSecureIndex(secureStore)
    const remaining: string[] = []
    for (const key of index) {
      try {
        await secureStore.deleteItemAsync(key)
      } catch {
        remaining.push(key)
      }
    }
    if (remaining.length > 0) {
      // Keep the failed keys indexed so a retry targets exactly them.
      await writeSecureIndex(secureStore, remaining)
      throw new IncompleteClearError(remaining)
    }
    await writeSecureIndex(secureStore, [])
    return
  }

  _memStore.clear()
}
/**
 * Get multiple values for a set of keys.
 *
 * @example
 * ```ts
 * import { multiGet } from '@neutron-build/native/device/async-storage'
 * const pairs = await multiGet(['user.name', 'user.email'])
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
 * import { multiSet } from '@neutron-build/native/device/async-storage'
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

  // Fall back to individual setItem calls (index-aware per key)
  for (const [key, value] of pairs) {
    await setItem(key, value)
  }
}

/**
 * Remove multiple keys in a single batch operation.
 *
 * @example
 * ```ts
 * import { multiRemove } from '@neutron-build/native/device/async-storage'
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
 * @example
 * ```ts
 * import { mergeItem } from '@neutron-build/native/device/async-storage'
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

  // Manual merge fallback (setItem keeps the SecureStore index accurate)
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
