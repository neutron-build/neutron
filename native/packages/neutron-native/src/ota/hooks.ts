/**
 * OTA hooks for React Native components.
 */

import { signal, computed } from '@preact/signals-core'
import type { OTAState, NativeOTAConfig } from './types.js'
import { OTAClient } from './client.js'

// ─── Global OTA client singleton ─────────────────────────────────────────────

let _client: OTAClient | null = null

const _state = signal<OTAState>({
  status: 'up-to-date',
  currentUpdateId: null,
  availableUpdate: null,
  downloadProgress: 0,
  error: null,
  isFirstLaunchAfterUpdate: false,
  consecutiveCrashes: 0,
})

/**
 * Initialize the OTA client. Call once at app startup.
 *
 * @example
 * ```tsx
 * import { initOTA } from '@neutron/native/ota'
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

  _client = new OTAClient(config)
  _client.subscribe((newState) => {
    _state.value = newState
  })
  _client.start()

  return _client
}

/**
 * Hook to access OTA update state and actions.
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
  const state = _state.value

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
