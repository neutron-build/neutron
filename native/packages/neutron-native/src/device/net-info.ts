/**
 * NetInfo — network connectivity state wrapping @react-native-community/netinfo.
 *
 * Peer dependencies:
 *   - @react-native-community/netinfo (standard for both Expo and bare RN)
 *
 * Falls back to `navigator.onLine` on web and provides a stub when no
 * native module is available.
 *
 * @module @neutron/native/device/net-info
 */

// ─── Types ──────────────────────────────────────────────────────────────────

/** Network connection type */
export type ConnectionType =
  | 'wifi'
  | 'cellular'
  | 'ethernet'
  | 'bluetooth'
  | 'vpn'
  | 'wimax'
  | 'none'
  | 'unknown'
  | 'other'

/** Cellular generation */
export type CellularGeneration = '2g' | '3g' | '4g' | '5g' | null

/** Network state snapshot */
export interface NetInfoState {
  /** Whether the device has an active network connection */
  isConnected: boolean
  /** Whether the internet is reachable (null if not yet determined) */
  isInternetReachable: boolean | null
  /** The type of network connection */
  type: ConnectionType
  /** Detailed connection information */
  details: NetInfoDetails | null
}

/** Connection details */
export interface NetInfoDetails {
  /** Whether the connection is expensive (e.g., metered cellular) */
  isConnectionExpensive?: boolean
  /** WiFi SSID (requires location permission on Android) */
  ssid?: string
  /** WiFi BSSID */
  bssid?: string
  /** WiFi signal strength (dBm) */
  strength?: number
  /** WiFi IP address */
  ipAddress?: string
  /** WiFi subnet mask */
  subnet?: string
  /** WiFi frequency in MHz */
  frequency?: number
  /** Cellular generation */
  cellularGeneration?: CellularGeneration
  /** Cellular carrier name */
  carrier?: string
}

/** Handle to unsubscribe from network state changes */
export interface NetInfoSubscription {
  /** Stop listening for network state changes */
  remove(): void
}

/** Configuration for network state fetching */
export interface NetInfoConfiguration {
  /** URL to use for internet reachability checks */
  reachabilityUrl?: string
  /** Timeout for reachability checks in milliseconds */
  reachabilityRequestTimeout?: number
  /** Interval between reachability checks in milliseconds */
  reachabilityLongTimeout?: number
  /** Short interval for reachability checks after connectivity changes */
  reachabilityShortTimeout?: number
}

// ─── Lazy module loader ─────────────────────────────────────────────────────

/* eslint-disable @typescript-eslint/no-explicit-any */
let _netInfo: any = undefined

function getNetInfo(): any {
  if (_netInfo === undefined) {
    try {
      const mod = require('@react-native-community/netinfo')
      _netInfo = mod.default ?? mod
    } catch {
      _netInfo = null
    }
  }
  return _netInfo
}
/* eslint-enable @typescript-eslint/no-explicit-any */

// ─── Helpers ────────────────────────────────────────────────────────────────

function normalizeState(state: any): NetInfoState { // eslint-disable-line @typescript-eslint/no-explicit-any
  const type = (state.type ?? 'unknown').toLowerCase() as ConnectionType
  return {
    isConnected: state.isConnected ?? false,
    isInternetReachable: state.isInternetReachable ?? null,
    type,
    details: state.details
      ? {
          isConnectionExpensive: state.details.isConnectionExpensive,
          ssid: state.details.ssid,
          bssid: state.details.bssid,
          strength: state.details.strength,
          ipAddress: state.details.ipAddress,
          subnet: state.details.subnet,
          frequency: state.details.frequency,
          cellularGeneration: state.details.cellularGeneration ?? null,
          carrier: state.details.carrier,
        }
      : null,
  }
}

function webFallbackState(): NetInfoState {
  if (typeof navigator !== 'undefined' && 'onLine' in navigator) {
    return {
      isConnected: navigator.onLine,
      isInternetReachable: navigator.onLine,
      type: 'unknown',
      details: null,
    }
  }
  return {
    isConnected: false,
    isInternetReachable: null,
    type: 'unknown',
    details: null,
  }
}

// ─── Public API ─────────────────────────────────────────────────────────────

/**
 * Fetch the current network state.
 *
 * @returns The current network connectivity state.
 *
 * @example
 * ```ts
 * import { fetch } from '@neutron/native/device/net-info'
 * const state = await fetch()
 * console.log(`Connected: ${state.isConnected}, Type: ${state.type}`)
 * ```
 */
export async function fetch(): Promise<NetInfoState> {
  const netInfo = getNetInfo()
  if (netInfo) {
    const state = await netInfo.fetch()
    return normalizeState(state)
  }

  return webFallbackState()
}

/**
 * Fetch the network state for a specific interface type.
 *
 * @param interfaceType - The interface type to query (e.g., 'wifi', 'cellular').
 * @returns The network state for the requested interface.
 */
export async function fetchInterface(
  interfaceType: ConnectionType,
): Promise<NetInfoState> {
  const netInfo = getNetInfo()
  if (netInfo?.fetch) {
    const state = await netInfo.fetch(interfaceType)
    return normalizeState(state)
  }

  return webFallbackState()
}

/**
 * Subscribe to network state changes.
 *
 * The callback is called immediately with the current state, and then
 * again whenever the network state changes.
 *
 * @param callback - Called with the new network state on each change.
 * @returns A subscription handle; call `.remove()` to stop listening.
 *
 * @example
 * ```ts
 * import { addEventListener } from '@neutron/native/device/net-info'
 * const sub = addEventListener((state) => {
 *   if (!state.isConnected) {
 *     console.log('Lost connection!')
 *   }
 * })
 * // later: sub.remove()
 * ```
 */
export function addEventListener(
  callback: (state: NetInfoState) => void,
): NetInfoSubscription {
  const netInfo = getNetInfo()
  if (netInfo) {
    const unsub = netInfo.addEventListener(
      (state: any) => callback(normalizeState(state)), // eslint-disable-line @typescript-eslint/no-explicit-any
    )
    // @react-native-community/netinfo returns an unsubscribe function
    return {
      remove: typeof unsub === 'function' ? unsub : () => unsub?.(),
    }
  }

  // Web fallback: use online/offline events
  if (typeof window !== 'undefined') {
    const handler = () => callback(webFallbackState())
    window.addEventListener('online', handler)
    window.addEventListener('offline', handler)

    // Fire immediately with current state
    callback(webFallbackState())

    return {
      remove() {
        window.removeEventListener('online', handler)
        window.removeEventListener('offline', handler)
      },
    }
  }

  return { remove() {} }
}

/**
 * Check if the device is currently connected to the internet.
 *
 * Convenience wrapper around `fetch()` for simple connectivity checks.
 *
 * @returns true if the device has an active network connection.
 *
 * @example
 * ```ts
 * import { isConnected } from '@neutron/native/device/net-info'
 * if (await isConnected()) {
 *   await syncData()
 * }
 * ```
 */
export async function isConnected(): Promise<boolean> {
  const state = await fetch()
  return state.isConnected
}

/**
 * Configure the network info module.
 *
 * @param config - Configuration options.
 */
export function configure(config: NetInfoConfiguration): void {
  const netInfo = getNetInfo()
  if (netInfo?.configure) {
    netInfo.configure({
      reachabilityUrl: config.reachabilityUrl,
      reachabilityRequestTimeout: config.reachabilityRequestTimeout,
      reachabilityLongTimeout: config.reachabilityLongTimeout,
      reachabilityShortTimeout: config.reachabilityShortTimeout,
    })
  }
}

/**
 * Refresh the current network state.
 *
 * Forces a fresh network state check, bypassing any cached state.
 *
 * @returns The refreshed network state.
 */
export async function refresh(): Promise<NetInfoState> {
  const netInfo = getNetInfo()
  if (netInfo?.refresh) {
    const state = await netInfo.refresh()
    return normalizeState(state)
  }
  return fetch()
}
