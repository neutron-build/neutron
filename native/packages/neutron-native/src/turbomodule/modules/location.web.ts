/**
 * Location TurboModule — web implementation using the Geolocation API.
 *
 * Uses navigator.geolocation (W3C Geolocation API) which is supported
 * in all modern browsers. Requires HTTPS in production.
 *
 * Browser support: Chrome 5+, Firefox 3.5+, Safari 5+, Edge 12+
 */

import type { LocationModule, LocationOptions } from './location.js'
import type { Coordinate, NativeResult, NativeSubscription, NativeEventCallback, ModuleMethod } from '../types.js'
import { registerWebModule } from '../registry.js'

const METHODS: readonly ModuleMethod[] = [
  { name: 'getCurrentPosition', kind: 'async' },
  { name: 'watchPosition', kind: 'sync' },
  { name: 'isServicesEnabled', kind: 'async' },
  { name: 'checkPermission', kind: 'async' },
  { name: 'requestPermission', kind: 'async' },
] as const

const WEB_LOCATION: LocationModule = {
  moduleName: 'NeutronLocation',
  methods: METHODS,

  async getCurrentPosition(options?: LocationOptions): Promise<NativeResult<Coordinate>> {
    if (typeof navigator === 'undefined' || !navigator.geolocation) {
      return { ok: false, error: { code: 'UNAVAILABLE', message: 'Geolocation API not supported' } }
    }

    const enableHighAccuracy = options?.accuracy === 'best'
      || options?.accuracy === 'nearest-ten-meters'

    return new Promise((resolve) => {
      navigator.geolocation.getCurrentPosition(
        (pos) => {
          resolve({
            ok: true,
            value: {
              latitude: pos.coords.latitude,
              longitude: pos.coords.longitude,
              altitude: pos.coords.altitude,
              accuracy: pos.coords.accuracy,
              altitudeAccuracy: pos.coords.altitudeAccuracy,
              heading: pos.coords.heading,
              speed: pos.coords.speed,
              timestamp: pos.timestamp,
            },
          })
        },
        (err) => {
          const code = err.code === 1 ? 'PERMISSION_DENIED'
            : err.code === 2 ? 'UNAVAILABLE'
            : 'UNAVAILABLE'
          resolve({ ok: false, error: { code, message: err.message } })
        },
        {
          enableHighAccuracy,
          timeout: options?.timeout ?? 15000,
          maximumAge: options?.maximumAge ?? 0,
        },
      )
    })
  },

  watchPosition(
    callback: NativeEventCallback<Coordinate>,
    options?: LocationOptions,
  ): NativeSubscription {
    if (typeof navigator === 'undefined' || !navigator.geolocation) {
      return { remove() {} }
    }

    const enableHighAccuracy = options?.accuracy === 'best'
      || options?.accuracy === 'nearest-ten-meters'

    const watchId = navigator.geolocation.watchPosition(
      (pos) => {
        callback({
          latitude: pos.coords.latitude,
          longitude: pos.coords.longitude,
          altitude: pos.coords.altitude,
          accuracy: pos.coords.accuracy,
          altitudeAccuracy: pos.coords.altitudeAccuracy,
          heading: pos.coords.heading,
          speed: pos.coords.speed,
          timestamp: pos.timestamp,
        })
      },
      undefined,
      {
        enableHighAccuracy,
        timeout: options?.timeout,
        maximumAge: options?.maximumAge,
      },
    )

    return { remove: () => navigator.geolocation.clearWatch(watchId) }
  },

  async isServicesEnabled(): Promise<boolean> {
    return typeof navigator !== 'undefined' && 'geolocation' in navigator
  },

  async checkPermission(): Promise<'granted' | 'denied' | 'not-determined' | 'when-in-use'> {
    try {
      const result = await navigator.permissions.query({ name: 'geolocation' })
      if (result.state === 'granted') return 'granted'
      if (result.state === 'denied') return 'denied'
      return 'not-determined'
    } catch {
      return 'not-determined'
    }
  },

  async requestPermission(): Promise<'granted' | 'denied'> {
    if (typeof navigator === 'undefined' || !navigator.geolocation) {
      return 'denied'
    }

    // The web Geolocation API requests permission implicitly on first use.
    // Trigger a fast getCurrentPosition to prompt the user.
    return new Promise((resolve) => {
      navigator.geolocation.getCurrentPosition(
        () => resolve('granted'),
        () => resolve('denied'),
        { timeout: 10000, maximumAge: Infinity },
      )
    })
  },
}

// ─── Register web implementation ─────────────────────────────────────────────

registerWebModule('NeutronLocation', () => WEB_LOCATION)
