/**
 * OTA hooks for React Native components.
 */

import { signal, computed } from '@preact/signals-core'
import { useSyncExternalStore } from 'react'
import type { OTAState, NativeOTAConfig } from './types.js'
import { OTAClient } from './client.js'

// ─── Global OTA client singleton ─────────────────────────────────────────────

let _client: OTAClient | null = null
let _clientGeneration = 0

const _state = signal<OTAState>({
  status: 'up-to-date',
  currentUpdateId: null,
  availableUpdate: null,
  downloadProgress: 0,
  error: null,
  isFirstLaunchAfterUpdate: false,
  consecutiveCrashes: 0,
})

/** React listeners for useOTA's useSyncExternalStore subscription (NF-NR-16). */
const _reactListeners = new Set<() => void>()
function _emitToReact(): void {
  for (const listener of _reactListeners) listener()
}
function _subscribeReact(listener: () => void): () => void {
  _reactListeners.add(listener)
  return () => { _reactListeners.delete(listener) }
}

/**
 * Initialize the OTA client. Call once at app startup.
 *
 * @example
 * ```tsx
 * import { initOTA } from '@neutron-build/native/ota'
 *
 * initOTA({
 *   endpoint: 'https://updates.myapp.com',
 *   publicKey: 'base64...',
 *   checkInterval: 3600,
 *   updateStrategy: 'next-launch',
 *   channel: 'production',
 * })
 * ```
 */
export function initOTA(config: NativeOTAConfig): OTAClient {
  if (_client) {
    _client.stop()
  }

  // Fence stale callbacks: a replaced client's in-flight subscription must
  // never write state for the new one (NF-NR-16).
  const generation = ++_clientGeneration
  _client = new OTAClient(config)
  const unsubscribe = _client.subscribe((newState) => {
    if (generation !== _clientGeneration) {
      unsubscribe()
      return
    }
    _state.value = newState
    _emitToReact()
  })
  _client.start()

  return _client
}

/**
 * Dispose the global OTA client (stops checks, fences its callbacks and
 * detaches React subscriptions). The app may call initOTA again afterwards.
 */
export function disposeOTA(): void {
  _clientGeneration++
  if (_client) {
    _client.stop()
    _client = null
  }
}

/**
 * Hook to access OTA update state and actions. SUBSCRIBES to the client's
 * state (useSyncExternalStore) so components re-render on every change
 * (NF-NR-16); throws when used before initOTA.
 *
 * @example
 * ```tsx
 * function UpdateBanner() {
 *   const { status, availableUpdate, downloadProgress, checkForUpdate, downloadAndApply } = useOTA()
 *
 *   if (status === 'available' && availableUpdate) {
 *     return (
 *       <Pressable onPress={downloadAndApply}>
 *         <Text>Update to v{availableUpdate.version}</Text>
 *       </Pressable>
 *     )
 *   }
 *
 *   if (status === 'downloading') {
 *     return <Text>Downloading... {Math.round(downloadProgress * 100)}%</Text>
 *   }
 *
 *   return null
 * }
 * ```
 */
export function useOTA() {
  const state = useSyncExternalStore(_subscribeReact, () => _state.value)

  return {
    ...state,

    /** Check for updates manually */
    async checkForUpdate() {
      return _client?.checkForUpdate() ?? null
    },

    /** Download and apply the available update */
    async downloadAndApply() {
      return _client?.downloadAndApply() ?? false
    },

    /** Roll back to the original bundle */
    async rollback() {
      return _client?.rollback()
    },

    /** Mark the current launch as successful (clears crash counter) */
    markSuccessfulLaunch() {
      _client?.markSuccessfulLaunch()
    },
  }
}

/** Whether an update is available (computed signal for fine-grained reactivity) */
export const isUpdateAvailable = computed(() => _state.value.status === 'available')

/** Whether an update is being downloaded */
export const isDownloading = computed(() => _state.value.status === 'downloading')

/** Download progress (0-1) */
export const downloadProgress = computed(() => _state.value.downloadProgress)
