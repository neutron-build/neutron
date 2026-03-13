/**
 * Permissions TurboModule — unified permission management.
 *
 * iOS: Info.plist keys + runtime permission requests
 * Android: AndroidManifest.xml + ActivityCompat.requestPermissions
 *
 * Wraps platform-specific permission APIs into a single interface.
 */

import type { TurboModule, ModuleMethod, PermissionStatus, PermissionName } from '../types.js'
import { getModule, registerModule } from '../registry.js'

export interface PermissionResult {
  permission: PermissionName
  status: PermissionStatus
}

export interface PermissionsModule extends TurboModule {
  moduleName: 'NeutronPermissions'

  /** Check the status of a single permission */
  check(permission: PermissionName): Promise<PermissionStatus>

  /** Request a single permission. Returns the resulting status. */
  request(permission: PermissionName): Promise<PermissionStatus>

  /** Check multiple permissions at once */
  checkMultiple(permissions: PermissionName[]): Promise<Record<PermissionName, PermissionStatus>>

  /** Request multiple permissions at once */
  requestMultiple(permissions: PermissionName[]): Promise<Record<PermissionName, PermissionStatus>>

  /** Open the app's settings page (for when permissions are blocked) */
  openSettings(): Promise<void>
}

const METHODS: readonly ModuleMethod[] = [
  { name: 'check', kind: 'async' },
  { name: 'request', kind: 'async' },
  { name: 'checkMultiple', kind: 'async' },
  { name: 'requestMultiple', kind: 'async' },
  { name: 'openSettings', kind: 'async' },
] as const

registerModule<PermissionsModule>('NeutronPermissions', () => ({
  moduleName: 'NeutronPermissions',
  methods: METHODS,
  async check() { return 'unavailable' as const },
  async request() { return 'unavailable' as const },
  async checkMultiple(perms) {
    const result: Record<string, PermissionStatus> = {}
    for (const p of perms) result[p] = 'unavailable'
    return result as Record<PermissionName, PermissionStatus>
  },
  async requestMultiple(perms) {
    const result: Record<string, PermissionStatus> = {}
    for (const p of perms) result[p] = 'unavailable'
    return result as Record<PermissionName, PermissionStatus>
  },
  async openSettings() {},
}))

/**
 * Hook to access the Permissions TurboModule.
 *
 * @example
 * ```tsx
 * const perms = usePermissions()
 * const status = await perms.check('camera')
 * if (status === 'denied') {
 *   const result = await perms.request('camera')
 *   if (result === 'blocked') await perms.openSettings()
 * }
 * ```
 */
export function usePermissions(): PermissionsModule {
  const mod = getModule<PermissionsModule>('NeutronPermissions')
  if (!mod) throw new Error('[neutron-native] NeutronPermissions module not available')
  return mod
}
