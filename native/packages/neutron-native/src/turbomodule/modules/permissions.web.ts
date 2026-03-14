/**
 * Permissions TurboModule — web implementation using the Permissions API.
 *
 * Uses navigator.permissions.query() to check permission status for
 * camera, geolocation, microphone, and notifications.
 *
 * Browser support: Chrome 43+, Firefox 46+, Edge 79+, Safari 16+
 */

import type { PermissionsModule, PermissionResult } from './permissions.js'
import type { PermissionStatus, PermissionName, ModuleMethod } from '../types.js'
import { registerWebModule } from '../registry.js'

const METHODS: readonly ModuleMethod[] = [
  { name: 'check', kind: 'async' },
  { name: 'request', kind: 'async' },
  { name: 'checkMultiple', kind: 'async' },
  { name: 'requestMultiple', kind: 'async' },
  { name: 'openSettings', kind: 'async' },
] as const

/**
 * Map Neutron permission names to web Permissions API names.
 * Not all permissions have web equivalents.
 */
const PERMISSION_MAP: Partial<Record<PermissionName, string>> = {
  'camera': 'camera',
  'location': 'geolocation',
  'location-always': 'geolocation',
  'microphone': 'microphone',
  'notifications': 'notifications',
}

/** Convert a PermissionState to our PermissionStatus */
function mapState(state: globalThis.PermissionState): PermissionStatus {
  if (state === 'granted') return 'granted'
  if (state === 'denied') return 'denied'
  return 'denied' // 'prompt' maps to denied since user hasn't acted yet
}

/** Check a single permission via the Permissions API */
async function checkSingle(permission: PermissionName): Promise<PermissionStatus> {
  const webName = PERMISSION_MAP[permission]
  if (!webName) {
    // This permission has no web equivalent
    return 'unavailable'
  }

  if (typeof navigator === 'undefined' || !navigator.permissions?.query) {
    return 'unavailable'
  }

  // Special case: notifications have their own API
  if (permission === 'notifications') {
    if (typeof window !== 'undefined' && 'Notification' in window) {
      if (Notification.permission === 'granted') return 'granted'
      if (Notification.permission === 'denied') return 'denied'
      return 'denied' // 'default' = not yet asked
    }
    return 'unavailable'
  }

  try {
    const result = await navigator.permissions.query({ name: webName as globalThis.PermissionName })
    return mapState(result.state)
  } catch {
    return 'unavailable'
  }
}

/** Request a single permission by triggering the relevant browser API */
async function requestSingle(permission: PermissionName): Promise<PermissionStatus> {
  // The web Permissions API does not have a generic request() method.
  // Permissions are requested implicitly when using the related API.
  // We trigger a minimal use of each API to prompt the user.

  if (permission === 'camera' || permission === 'microphone') {
    if (typeof navigator === 'undefined' || !navigator.mediaDevices?.getUserMedia) {
      return 'unavailable'
    }
    try {
      const constraints = permission === 'camera' ? { video: true } : { audio: true }
      const stream = await navigator.mediaDevices.getUserMedia(constraints)
      stream.getTracks().forEach((t) => t.stop())
      return 'granted'
    } catch (err: unknown) {
      const error = err instanceof Error ? err : new Error(String(err))
      return error.name === 'NotAllowedError' ? 'denied' : 'unavailable'
    }
  }

  if (permission === 'location' || permission === 'location-always') {
    if (typeof navigator === 'undefined' || !navigator.geolocation) {
      return 'unavailable'
    }
    return new Promise<PermissionStatus>((resolve) => {
      navigator.geolocation.getCurrentPosition(
        () => resolve('granted'),
        (err) => resolve(err.code === 1 ? 'denied' : 'unavailable'),
        { timeout: 10000, maximumAge: Infinity },
      )
    })
  }

  if (permission === 'notifications') {
    if (typeof window === 'undefined' || !('Notification' in window)) {
      return 'unavailable'
    }
    const result = await Notification.requestPermission()
    return result === 'granted' ? 'granted' : 'denied'
  }

  // Permissions not available on web
  return 'unavailable'
}

const WEB_PERMISSIONS: PermissionsModule = {
  moduleName: 'NeutronPermissions',
  methods: METHODS,

  async check(permission: PermissionName): Promise<PermissionStatus> {
    return checkSingle(permission)
  },

  async request(permission: PermissionName): Promise<PermissionStatus> {
    return requestSingle(permission)
  },

  async checkMultiple(permissions: PermissionName[]): Promise<Record<PermissionName, PermissionStatus>> {
    const result: Record<string, PermissionStatus> = {}
    await Promise.all(
      permissions.map(async (p) => {
        result[p] = await checkSingle(p)
      }),
    )
    return result as Record<PermissionName, PermissionStatus>
  },

  async requestMultiple(permissions: PermissionName[]): Promise<Record<PermissionName, PermissionStatus>> {
    const result: Record<string, PermissionStatus> = {}
    // Request sequentially to avoid multiple permission prompts overlapping
    for (const p of permissions) {
      result[p] = await requestSingle(p)
    }
    return result as Record<PermissionName, PermissionStatus>
  },

  async openSettings(): Promise<void> {
    // There is no web API to open browser or OS settings.
    // Log a helpful message for developers.
    console.warn(
      '[neutron-native] openSettings() is not available on web. ' +
      'Guide your users to open their browser settings to manage permissions.'
    )
  },
}

// ─── Register web implementation ─────────────────────────────────────────────

registerWebModule('NeutronPermissions', () => WEB_PERMISSIONS)
