/**
 * Location — geolocation wrapping expo-location and @react-native-community/geolocation.
 *
 * Peer dependencies (install one):
 *   - expo-location (Expo managed/bare)
 *   - @react-native-community/geolocation (bare React Native)
 *
 * All functions are async and handle missing dependencies gracefully.
 *
 * @module @neutron/native/device/location
 */

// ─── Types ──────────────────────────────────────────────────────────────────

/** A geographic coordinate with metadata */
export interface LocationCoordinate {
  /** Latitude in degrees */
  latitude: number
  /** Longitude in degrees */
  longitude: number
  /** Altitude in meters (null if unavailable) */
  altitude: number | null
  /** Horizontal accuracy in meters */
  accuracy: number
  /** Vertical accuracy in meters (null if unavailable) */
  altitudeAccuracy: number | null
  /** Heading in degrees from true north (null if unavailable) */
  heading: number | null
  /** Speed in m/s (null if unavailable) */
  speed: number | null
  /** Unix timestamp in milliseconds */
  timestamp: number
}

/** Options for location requests */
export interface LocationOptions {
  /** Desired accuracy (default: 'balanced') */
  accuracy?: 'lowest' | 'low' | 'balanced' | 'high' | 'best' | 'bestForNavigation'
  /** Minimum distance in meters between updates for watchPosition (default: 0) */
  distanceFilter?: number
  /** Timeout in milliseconds for getCurrentPosition (default: 15000) */
  timeout?: number
  /** Accept a cached position no older than this many milliseconds */
  maximumAge?: number
}

/** Options for watchPosition */
export interface WatchOptions extends LocationOptions {
  /** Request background location updates (requires 'always' permission) */
  enableBackground?: boolean
  /** Minimum time interval between updates in milliseconds (Android only) */
  fastestInterval?: number
}

/** Location permission status */
export type LocationPermissionStatus =
  | 'granted'
  | 'denied'
  | 'undetermined'
  | 'when-in-use'
  | 'always'

/** Handle to cancel a position watch subscription */
export interface LocationSubscription {
  /** Stop watching for position updates */
  remove(): void
}

// ─── Lazy module loaders ────────────────────────────────────────────────────

/* eslint-disable @typescript-eslint/no-explicit-any */
let _expoLocation: any = undefined
let _communityGeo: any = undefined

function getExpoLocation(): any {
  if (_expoLocation === undefined) {
    try { _expoLocation = require('expo-location') } catch { _expoLocation = null }
  }
  return _expoLocation
}

function getCommunityGeo(): any {
  if (_communityGeo === undefined) {
    try { _communityGeo = require('@react-native-community/geolocation') } catch { _communityGeo = null }
  }
  return _communityGeo
}

function assertAvailable(): void {
  if (!getExpoLocation() && !getCommunityGeo()) {
    throw new Error(
      '[neutron-native/device/location] No location package found. ' +
      'Install one of: expo-location, @react-native-community/geolocation'
    )
  }
}
/* eslint-enable @typescript-eslint/no-explicit-any */

// ─── Helpers ────────────────────────────────────────────────────────────────

function expoAccuracy(accuracy: LocationOptions['accuracy']): number {
  const expo = getExpoLocation()
  if (!expo) return 4 // Accuracy.Balanced fallback
  const map: Record<string, number> = {
    lowest: expo.Accuracy?.Lowest ?? 1,
    low: expo.Accuracy?.Low ?? 2,
    balanced: expo.Accuracy?.Balanced ?? 4,
    high: expo.Accuracy?.High ?? 5,
    best: expo.Accuracy?.Highest ?? 6,
    bestForNavigation: expo.Accuracy?.BestForNavigation ?? 6,
  }
  return map[accuracy ?? 'balanced'] ?? 4
}

function normalizePosition(pos: any): LocationCoordinate { // eslint-disable-line @typescript-eslint/no-explicit-any
  const c = pos.coords ?? pos
  return {
    latitude: c.latitude,
    longitude: c.longitude,
    altitude: c.altitude ?? null,
    accuracy: c.accuracy ?? 0,
    altitudeAccuracy: c.altitudeAccuracy ?? null,
    heading: c.heading ?? null,
    speed: c.speed ?? null,
    timestamp: pos.timestamp ?? Date.now(),
  }
}

// ─── Public API ─────────────────────────────────────────────────────────────

/**
 * Request location permission from the user.
 *
 * @param level - Permission level to request (default: 'when-in-use').
 * @returns The resulting permission status.
 *
 * @example
 * ```ts
 * import { requestLocationPermission } from '@neutron/native/device/location'
 * const status = await requestLocationPermission('when-in-use')
 * ```
 */
export async function requestLocationPermission(
  level: 'when-in-use' | 'always' = 'when-in-use',
): Promise<LocationPermissionStatus> {
  const expo = getExpoLocation()
  if (expo) {
    if (level === 'always') {
      // Expo requires foreground first, then background
      const fg = await expo.requestForegroundPermissionsAsync()
      if (fg.status !== 'granted') return 'denied'
      const bg = await expo.requestBackgroundPermissionsAsync()
      return bg.status === 'granted' ? 'always' : 'when-in-use'
    }
    const { status } = await expo.requestForegroundPermissionsAsync()
    if (status === 'granted') return 'when-in-use'
    return 'denied'
  }

  const geo = getCommunityGeo()
  if (geo) {
    // @react-native-community/geolocation uses PermissionsAndroid on Android
    // and Info.plist on iOS; requestAuthorization triggers the prompt
    await geo.requestAuthorization?.(level === 'always' ? 'always' : 'whenInUse')
    return 'granted' // best effort — actual status depends on user action
  }

  throw new Error(
    '[neutron-native/device/location] No location package found. ' +
    'Install one of: expo-location, @react-native-community/geolocation'
  )
}

/**
 * Check the current location permission status without prompting.
 *
 * @returns The current permission status.
 */
export async function getLocationPermissionStatus(): Promise<LocationPermissionStatus> {
  const expo = getExpoLocation()
  if (expo) {
    const fg = await expo.getForegroundPermissionsAsync()
    if (fg.status !== 'granted') {
      return fg.status === 'denied' ? 'denied' : 'undetermined'
    }
    const bg = await expo.getBackgroundPermissionsAsync()
    return bg.status === 'granted' ? 'always' : 'when-in-use'
  }

  // community geolocation doesn't expose a standalone permission check
  return 'undetermined'
}

/**
 * Get the device's current position (one-shot).
 *
 * @param options - Location request options.
 * @returns The current geographic coordinate.
 *
 * @example
 * ```ts
 * import { getCurrentPosition } from '@neutron/native/device/location'
 * const pos = await getCurrentPosition({ accuracy: 'high' })
 * console.log(pos.latitude, pos.longitude)
 * ```
 */
export async function getCurrentPosition(
  options: LocationOptions = {},
): Promise<LocationCoordinate> {
  assertAvailable()

  const expo = getExpoLocation()
  if (expo) {
    const result = await expo.getCurrentPositionAsync({
      accuracy: expoAccuracy(options.accuracy),
      maximumAge: options.maximumAge,
    })
    return normalizePosition(result)
  }

  const geo = getCommunityGeo()
  if (geo) {
    return new Promise<LocationCoordinate>((resolve, reject) => {
      geo.getCurrentPosition(
        (pos: any) => resolve(normalizePosition(pos)), // eslint-disable-line @typescript-eslint/no-explicit-any
        (err: any) => reject(new Error(`[neutron-native/device/location] ${err.message}`)), // eslint-disable-line @typescript-eslint/no-explicit-any
        {
          enableHighAccuracy: (options.accuracy === 'high' || options.accuracy === 'best' || options.accuracy === 'bestForNavigation'),
          timeout: options.timeout ?? 15000,
          maximumAge: options.maximumAge ?? 0,
        },
      )
    })
  }

  throw new Error('[neutron-native/device/location] No location provider available')
}

/**
 * Watch for continuous position updates.
 *
 * @param callback - Called with each new position.
 * @param options - Watch configuration.
 * @returns A subscription handle; call `.remove()` to stop watching.
 *
 * @example
 * ```ts
 * import { watchPosition } from '@neutron/native/device/location'
 * const sub = await watchPosition((pos) => {
 *   console.log('Moved to', pos.latitude, pos.longitude)
 * }, { accuracy: 'high', distanceFilter: 10 })
 * // later: sub.remove()
 * ```
 */
export async function watchPosition(
  callback: (position: LocationCoordinate) => void,
  options: WatchOptions = {},
): Promise<LocationSubscription> {
  assertAvailable()

  const expo = getExpoLocation()
  if (expo) {
    const sub = await expo.watchPositionAsync(
      {
        accuracy: expoAccuracy(options.accuracy),
        distanceInterval: options.distanceFilter ?? 0,
        timeInterval: options.fastestInterval,
        mayShowUserSettingsDialog: true,
      },
      (loc: any) => callback(normalizePosition(loc)), // eslint-disable-line @typescript-eslint/no-explicit-any
    )
    return { remove: () => sub.remove() }
  }

  const geo = getCommunityGeo()
  if (geo) {
    const watchId = geo.watchPosition(
      (pos: any) => callback(normalizePosition(pos)), // eslint-disable-line @typescript-eslint/no-explicit-any
      undefined,
      {
        enableHighAccuracy: (options.accuracy === 'high' || options.accuracy === 'best' || options.accuracy === 'bestForNavigation'),
        distanceFilter: options.distanceFilter ?? 0,
        interval: options.fastestInterval,
      },
    )
    return { remove: () => geo.clearWatch(watchId) }
  }

  throw new Error('[neutron-native/device/location] No location provider available')
}

/**
 * Check if location services are enabled at the OS level.
 *
 * @returns true if location services are turned on.
 */
export async function isLocationServicesEnabled(): Promise<boolean> {
  const expo = getExpoLocation()
  if (expo) {
    return expo.hasServicesEnabledAsync()
  }

  // community geolocation doesn't have a direct check;
  // attempt a quick getCurrentPosition with a short timeout
  const geo = getCommunityGeo()
  if (geo) {
    return new Promise<boolean>((resolve) => {
      geo.getCurrentPosition(
        () => resolve(true),
        () => resolve(false),
        { timeout: 2000, maximumAge: Infinity },
      )
    })
  }

  return false
}
