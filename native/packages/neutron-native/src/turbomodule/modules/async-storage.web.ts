/**
 * AsyncStorage TurboModule — web implementation using localStorage.
 *
 * Uses window.localStorage for persistent key-value storage on web.
 * All keys are prefixed with `__neutron_` to avoid collisions with
 * other scripts sharing the same origin.
 *
 * Falls back to an in-memory Map when localStorage is unavailable
 * (e.g. private browsing in some older browsers).
 *
 * Browser support: All modern browsers (IE 8+)
 */

import type { AsyncStorageModule } from './async-storage.js'
import type { ModuleMethod } from '../types.js'
import { registerWebModule } from '../registry.js'

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

const KEY_PREFIX = '__neutron_'

/** Check if localStorage is available and writable */
function hasLocalStorage(): boolean {
  try {
    const testKey = '__neutron_test__'
    localStorage.setItem(testKey, '1')
    localStorage.removeItem(testKey)
    return true
  } catch {
    return false
  }
}

/** In-memory fallback when localStorage is not available */
const _memStore = new Map<string, string>()

const _useLocalStorage = typeof window !== 'undefined' && hasLocalStorage()

function prefixedKey(key: string): string {
  return KEY_PREFIX + key
}

const WEB_ASYNC_STORAGE: AsyncStorageModule = {
  moduleName: 'NeutronAsyncStorage',
  methods: METHODS,

  async getItem(key: string): Promise<string | null> {
    if (_useLocalStorage) {
      return localStorage.getItem(prefixedKey(key))
    }
    return _memStore.get(key) ?? null
  },

  async setItem(key: string, value: string): Promise<void> {
    if (_useLocalStorage) {
      localStorage.setItem(prefixedKey(key), value)
    } else {
      _memStore.set(key, value)
    }
  },

  async removeItem(key: string): Promise<void> {
    if (_useLocalStorage) {
      localStorage.removeItem(prefixedKey(key))
    } else {
      _memStore.delete(key)
    }
  },

  async multiGet(keys: string[]): Promise<[string, string | null][]> {
    return keys.map((key) => {
      if (_useLocalStorage) {
        return [key, localStorage.getItem(prefixedKey(key))] as [string, string | null]
      }
      return [key, _memStore.get(key) ?? null] as [string, string | null]
    })
  },

  async multiSet(pairs: [string, string][]): Promise<void> {
    for (const [key, value] of pairs) {
      if (_useLocalStorage) {
        localStorage.setItem(prefixedKey(key), value)
      } else {
        _memStore.set(key, value)
      }
    }
  },

  async multiRemove(keys: string[]): Promise<void> {
    for (const key of keys) {
      if (_useLocalStorage) {
        localStorage.removeItem(prefixedKey(key))
      } else {
        _memStore.delete(key)
      }
    }
  },

  async getAllKeys(): Promise<string[]> {
    if (_useLocalStorage) {
      const keys: string[] = []
      for (let i = 0; i < localStorage.length; i++) {
        const key = localStorage.key(i)
        if (key && key.startsWith(KEY_PREFIX)) {
          keys.push(key.slice(KEY_PREFIX.length))
        }
      }
      return keys
    }
    return Array.from(_memStore.keys())
  },

  async clear(): Promise<void> {
    if (_useLocalStorage) {
      // Only clear keys with our prefix
      const keysToRemove: string[] = []
      for (let i = 0; i < localStorage.length; i++) {
        const key = localStorage.key(i)
        if (key && key.startsWith(KEY_PREFIX)) {
          keysToRemove.push(key)
        }
      }
      for (const key of keysToRemove) {
        localStorage.removeItem(key)
      }
    } else {
      _memStore.clear()
    }
  },
}

// ─── Register web implementation ─────────────────────────────────────────────

registerWebModule('NeutronAsyncStorage', () => WEB_ASYNC_STORAGE)
