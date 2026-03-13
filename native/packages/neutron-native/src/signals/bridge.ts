/**
 * Signals bridge — connects @preact/signals-core state to native async storage
 * and cross-component event buses.
 *
 * Uses @preact/signals-core (framework-agnostic). For React component
 * reactivity, users add @preact/signals-react. For Preact (web builds),
 * @preact/signals provides the integration automatically.
 *
 * This enables patterns like:
 *   const user = persistedSignal('user', null)  // auto-synced to AsyncStorage
 *   const theme = persistedSignal('theme', 'light')
 */

import { signal } from '@preact/signals-core'
import type { Signal } from '@preact/signals-core'

// ─── AsyncStorage shim ────────────────────────────────────────────────────────

interface AsyncStorageInterface {
  getItem(key: string): Promise<string | null>
  setItem(key: string, value: string): Promise<void>
  removeItem(key: string): Promise<void>
}

function _getStorage(): AsyncStorageInterface | null {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const g = globalThis as any
  return g.AsyncStorage ?? g.__neutronStorage ?? null
}

// ─── Persisted signal ─────────────────────────────────────────────────────────

/**
 * Create a signal that automatically persists to AsyncStorage.
 * The value is restored from storage on first mount.
 *
 * @example
 * const token = persistedSignal('auth.token', null as string | null)
 * token.value = 'abc123'  // auto-saved
 */
export function persistedSignal<T>(key: string, initialValue: T): Signal<T> {
  const s = signal<T>(initialValue)
  const storage = _getStorage()

  if (storage) {
    // Restore from storage asynchronously
    storage.getItem(key).then(raw => {
      if (raw != null) {
        try {
          s.value = JSON.parse(raw) as T
        } catch {
          // Stored value is not valid JSON — use raw string
          s.value = raw as unknown as T
        }
      }
    })

    // Persist on every change using a lightweight subscription
    let _prev = JSON.stringify(initialValue)
    const unsubscribe = s.subscribe(val => {
      const serialized = JSON.stringify(val)
      if (serialized !== _prev) {
        _prev = serialized
        if (val == null) {
          storage.removeItem(key)
        } else {
          storage.setItem(key, serialized)
        }
      }
    })

    // Attach cleanup to the signal (caller responsible for calling if needed)
    ;(s as Signal<T> & { dispose?: () => void }).dispose = unsubscribe
  }

  return s
}

// ─── Event bus ────────────────────────────────────────────────────────────────

type Listener<T> = (payload: T) => void

/**
 * Lightweight cross-component event bus backed by signals.
 * Useful for imperative events (like "scroll to top", "show toast") that
 * don't need persistent state.
 *
 * @example
 * const scrollToTop = createEventBus<void>()
 * // Publisher: scrollToTop.emit()
 * // Subscriber: scrollToTop.on(handler)
 */
export function createEventBus<T = void>() {
  const listeners = new Set<Listener<T>>()

  return {
    emit(payload: T): void {
      for (const listener of listeners) {
        try { listener(payload) } catch { /* prevent one bad listener from breaking others */ }
      }
    },
    on(listener: Listener<T>): () => void {
      listeners.add(listener)
      return () => listeners.delete(listener)
    },
    clear(): void {
      listeners.clear()
    },
  }
}
