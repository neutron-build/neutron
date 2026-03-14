/**
 * NetInfo TurboModule — web implementation using the Network Information API
 * and navigator.onLine.
 *
 * Uses navigator.connection (Network Information API) where available for
 * connection type details, with navigator.onLine as the baseline.
 *
 * Browser support:
 *   - navigator.onLine: All modern browsers
 *   - navigator.connection: Chrome 61+, Edge 79+ (not Firefox, not Safari)
 */

import type { NetInfoModule } from './net-info.js'
import type { NetInfoState, NativeSubscription, NativeEventCallback, ModuleMethod } from '../types.js'
import { registerWebModule } from '../registry.js'

const METHODS: readonly ModuleMethod[] = [
  { name: 'fetch', kind: 'async' },
  { name: 'addEventListener', kind: 'sync' },
  { name: 'isConnected', kind: 'async' },
] as const

/** Build a NetInfoState from the current browser state */
function getCurrentState(): NetInfoState {
  const isConnected = typeof navigator !== 'undefined' ? navigator.onLine : false

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const connection = typeof navigator !== 'undefined' ? (navigator as any).connection : undefined

  let type: NetInfoState['type'] = isConnected ? 'unknown' : 'none'
  let details: NetInfoState['details'] = null

  if (connection) {
    // Map effectiveType to our type enum
    const effectiveType: string = connection.effectiveType ?? ''
    if (connection.type === 'wifi') {
      type = 'wifi'
    } else if (connection.type === 'cellular') {
      type = 'cellular'
      const genMap: Record<string, '2g' | '3g' | '4g' | '5g'> = {
        'slow-2g': '2g',
        '2g': '2g',
        '3g': '3g',
        '4g': '4g',
      }
      details = { cellularGeneration: genMap[effectiveType] }
    } else if (connection.type === 'ethernet') {
      type = 'ethernet'
    } else if (connection.type === 'bluetooth') {
      type = 'bluetooth'
    } else if (connection.type === 'none') {
      type = 'none'
    } else if (isConnected) {
      // effectiveType may give us a hint even without connection.type
      if (effectiveType === '4g') type = 'wifi' // Best guess for high-speed
      else if (effectiveType === '3g' || effectiveType === '2g' || effectiveType === 'slow-2g') {
        type = 'cellular'
        const genMap: Record<string, '2g' | '3g' | '4g'> = {
          'slow-2g': '2g',
          '2g': '2g',
          '3g': '3g',
        }
        details = { cellularGeneration: genMap[effectiveType] }
      }
    }
  }

  return {
    isConnected,
    isInternetReachable: isConnected ? true : null,
    type,
    details,
  }
}

const WEB_NET_INFO: NetInfoModule = {
  moduleName: 'NeutronNetInfo',
  methods: METHODS,

  async fetch(): Promise<NetInfoState> {
    return getCurrentState()
  },

  addEventListener(callback: NativeEventCallback<NetInfoState>): NativeSubscription {
    const handler = () => callback(getCurrentState())

    // Listen for online/offline events
    window.addEventListener('online', handler)
    window.addEventListener('offline', handler)

    // Also listen for connection change events if available
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const connection = typeof navigator !== 'undefined' ? (navigator as any).connection : undefined
    if (connection?.addEventListener) {
      connection.addEventListener('change', handler)
    }

    return {
      remove() {
        window.removeEventListener('online', handler)
        window.removeEventListener('offline', handler)
        if (connection?.removeEventListener) {
          connection.removeEventListener('change', handler)
        }
      },
    }
  },

  async isConnected(): Promise<boolean> {
    return typeof navigator !== 'undefined' ? navigator.onLine : false
  },
}

// ─── Register web implementation ─────────────────────────────────────────────

registerWebModule('NeutronNetInfo', () => WEB_NET_INFO)
