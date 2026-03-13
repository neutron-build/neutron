/**
 * NetInfo TurboModule — network connectivity state.
 *
 * iOS: NWPathMonitor (Network framework)
 * Android: ConnectivityManager
 */

import type { TurboModule, ModuleMethod, NetInfoState, NativeSubscription, NativeEventCallback } from '../types.js'
import { getModule, registerModule } from '../registry.js'

export interface NetInfoModule extends TurboModule {
  moduleName: 'NeutronNetInfo'

  /** Get current network state */
  fetch(): Promise<NetInfoState>

  /** Subscribe to network state changes */
  addEventListener(callback: NativeEventCallback<NetInfoState>): NativeSubscription

  /** Check if currently connected to the internet */
  isConnected(): Promise<boolean>
}

const METHODS: readonly ModuleMethod[] = [
  { name: 'fetch', kind: 'async' },
  { name: 'addEventListener', kind: 'sync' },
  { name: 'isConnected', kind: 'async' },
] as const

registerModule<NetInfoModule>('NeutronNetInfo', () => ({
  moduleName: 'NeutronNetInfo',
  methods: METHODS,
  async fetch() {
    return { isConnected: false, isInternetReachable: null, type: 'unknown' as const, details: null }
  },
  addEventListener() { return { remove() {} } },
  async isConnected() { return false },
}))

/**
 * Hook to access the NetInfo TurboModule.
 *
 * @example
 * ```tsx
 * const netInfo = useNetInfo()
 * const state = await netInfo.fetch()
 * console.log(state.type, state.isConnected)
 *
 * const sub = netInfo.addEventListener((state) => {
 *   console.log('Network changed:', state.type)
 * })
 * // later: sub.remove()
 * ```
 */
export function useNetInfo(): NetInfoModule {
  const mod = getModule<NetInfoModule>('NeutronNetInfo')
  if (!mod) throw new Error('[neutron-native] NeutronNetInfo module not available')
  return mod
}
