/**
 * Location TurboModule — GPS / network location.
 *
 * iOS: CoreLocation (CLLocationManager)
 * Android: FusedLocationProviderClient (Google Play Services)
 */

import type { TurboModule, ModuleMethod, Coordinate, NativeResult, NativeSubscription, NativeEventCallback } from '../types.js'
import { getModule, registerModule } from '../registry.js'

export interface LocationOptions {
  /** Desired accuracy level */
  accuracy?: 'best' | 'nearest-ten-meters' | 'hundred-meters' | 'kilometer' | 'three-kilometers'
  /** Minimum distance (meters) between updates */
  distanceFilter?: number
  /** Timeout for getCurrentPosition in ms (default: 15000) */
  timeout?: number
  /** Accept cached position if younger than this (ms) */
  maximumAge?: number
  /** Request background location updates (requires permission) */
  enableBackground?: boolean
}

export interface LocationModule extends TurboModule {
  moduleName: 'NeutronLocation'

  /** Get current position (one-shot) */
  getCurrentPosition(options?: LocationOptions): Promise<NativeResult<Coordinate>>

  /** Start watching position changes — returns subscription handle */
  watchPosition(
    callback: NativeEventCallback<Coordinate>,
    options?: LocationOptions,
  ): NativeSubscription

  /** Check if location services are enabled at OS level */
  isServicesEnabled(): Promise<boolean>

  /** Check current permission status */
  checkPermission(): Promise<'granted' | 'denied' | 'not-determined' | 'when-in-use'>

  /** Request location permission */
  requestPermission(level: 'when-in-use' | 'always'): Promise<'granted' | 'denied'>
}

const METHODS: readonly ModuleMethod[] = [
  { name: 'getCurrentPosition', kind: 'async' },
  { name: 'watchPosition', kind: 'sync' },
  { name: 'isServicesEnabled', kind: 'async' },
  { name: 'checkPermission', kind: 'async' },
  { name: 'requestPermission', kind: 'async' },
] as const

registerModule<LocationModule>('NeutronLocation', () => ({
  moduleName: 'NeutronLocation',
  methods: METHODS,
  async getCurrentPosition() {
    return { ok: false, error: { code: 'UNAVAILABLE', message: 'Location module not linked' } }
  },
  watchPosition() { return { remove() {} } },
  async isServicesEnabled() { return false },
  async checkPermission() { return 'denied' as const },
  async requestPermission() { return 'denied' as const },
}))

/**
 * Hook to access the Location TurboModule.
 *
 * @example
 * ```tsx
 * const location = useLocation()
 * const pos = await location.getCurrentPosition({ accuracy: 'best' })
 * if (pos.ok) console.log(pos.value.latitude, pos.value.longitude)
 * ```
 */
export function useLocation(): LocationModule {
  const mod = getModule<LocationModule>('NeutronLocation')
  if (!mod) throw new Error('[neutron-native] NeutronLocation module not available')
  return mod
}
