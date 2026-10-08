/**
 * Permissions — unified permission system wrapping react-native-permissions
 * and expo modules.
 *
 * Peer dependencies (install one):
 *   - react-native-permissions (bare React Native — comprehensive)
 *   - expo modules (expo-camera, expo-location, etc. — each handles its own permissions)
 *
 * @module @neutron-build/native/device/permissions
 */

// ─── Types ──────────────────────────────────────────────────────────────────

/** Permission status across platforms */
export enum PermissionStatus {
  /** Permission has been granted */
  GRANTED = 'granted',
  /** Permission has been denied (can be requested again) */
  DENIED = 'denied',
  /** Permission has been blocked by the user (must open Settings) */
  BLOCKED = 'blocked',
  /** Permission is not available on this device/OS */
  UNAVAILABLE = 'unavailable',
  /** Limited access granted (iOS 14+ photo library, contacts) */
  LIMITED = 'limited',
  /** Permission has not been requested yet */
  UNDETERMINED = 'undetermined',
}

/** Permission name — platform-agnostic identifiers */
export type PermissionName =
  | 'camera'
  | 'microphone'
  | 'location'
  | 'location-always'
  | 'photo-library'
  | 'contacts'
  | 'calendar'
  | 'reminders'
  | 'notifications'
  | 'bluetooth'
  | 'face-id'
  | 'media-library'
  | 'motion'
  | 'speech-recognition'
  | 'storage'

/** Result of checking or requesting a permission */
export interface PermissionResult {
  /** The permission that was checked or requested */
  permission: PermissionName
  /** The resulting status */
  status: PermissionStatus
}

// ─── Lazy module loaders ────────────────────────────────────────────────────

/* eslint-disable @typescript-eslint/no-explicit-any */
let _rnPermissions: any = undefined

function getRNPermissions(): any {
  if (_rnPermissions === undefined) {
    try {
      const mod = require('react-native-permissions')
      // Community library ships both default- and named-export shapes.
      _rnPermissions = mod?.default ?? mod ?? null
    } catch { _rnPermissions = null }
  }
  return _rnPermissions
}

/**
 * Notifications have no PERMISSIONS constant: react-native-permissions
 * routes them through checkNotifications/requestNotifications on every
 * platform (iOS included), and on Android ≤12 POST_NOTIFICATIONS does not
 * exist as a runtime permission at all. Resolving them through the
 * constant map disabled the valid iOS path (NF-NR-09).
 */
async function checkSpecialPermission(name: PermissionName): Promise<PermissionStatus | null> {
  if (name !== 'notifications') return null
  const rnp = getRNPermissions()
  if (typeof rnp?.checkNotifications === 'function') {
    const { status } = await rnp.checkNotifications()
    return normalizeStatus(status)
  }
  return null
}

async function requestSpecialPermission(name: PermissionName): Promise<PermissionStatus | null> {
  if (name !== 'notifications') return null
  const rnp = getRNPermissions()
  if (typeof rnp?.requestNotifications === 'function') {
    const { status } = await rnp.requestNotifications(['alert', 'badge', 'sound'])
    return normalizeStatus(status)
  }
  return null
}

function getPlatformOS(): 'ios' | 'android' | 'web' {
  try {
    const rn = require('react-native')
    return (rn.Platform?.OS ?? 'web') as 'ios' | 'android' | 'web'
  } catch {
    return 'web'
  }
}
/* eslint-enable @typescript-eslint/no-explicit-any */

// ─── Permission Mapping ─────────────────────────────────────────────────────

/**
 * Map our platform-agnostic permission names to react-native-permissions
 * platform-specific PERMISSIONS constants.
 */
function mapPermission(name: PermissionName): string | null {
  const rnp = getRNPermissions()
  if (!rnp?.PERMISSIONS) return null

  const os = getPlatformOS()
  const P = rnp.PERMISSIONS

  if (os === 'ios') {
    const iosMap: Record<PermissionName, string | undefined> = {
      'camera': P.IOS?.CAMERA,
      'microphone': P.IOS?.MICROPHONE,
      'location': P.IOS?.LOCATION_WHEN_IN_USE,
      'location-always': P.IOS?.LOCATION_ALWAYS,
      'photo-library': P.IOS?.PHOTO_LIBRARY,
      'contacts': P.IOS?.CONTACTS,
      'calendar': P.IOS?.CALENDARS,
      'reminders': P.IOS?.REMINDERS,
      'notifications': undefined, // handled by expo-notifications or firebase
      'bluetooth': P.IOS?.BLUETOOTH,
      'face-id': P.IOS?.FACE_ID,
      'media-library': P.IOS?.MEDIA_LIBRARY,
      'motion': P.IOS?.MOTION,
      'speech-recognition': P.IOS?.SPEECH_RECOGNITION,
      'storage': undefined, // not applicable on iOS
    }
    return iosMap[name] ?? null
  }

  if (os === 'android') {
    const androidMap: Record<PermissionName, string | undefined> = {
      'camera': P.ANDROID?.CAMERA,
      'microphone': P.ANDROID?.RECORD_AUDIO,
      'location': P.ANDROID?.ACCESS_FINE_LOCATION,
      'location-always': P.ANDROID?.ACCESS_BACKGROUND_LOCATION,
      'photo-library': P.ANDROID?.READ_MEDIA_IMAGES ?? P.ANDROID?.READ_EXTERNAL_STORAGE,
      'contacts': P.ANDROID?.READ_CONTACTS,
      'calendar': P.ANDROID?.READ_CALENDAR,
      'reminders': undefined, // not applicable on Android
      'notifications': P.ANDROID?.POST_NOTIFICATIONS,
      'bluetooth': P.ANDROID?.BLUETOOTH_CONNECT ?? P.ANDROID?.BLUETOOTH,
      'face-id': undefined, // not applicable on Android (use biometrics module)
      'media-library': P.ANDROID?.READ_MEDIA_AUDIO ?? P.ANDROID?.READ_EXTERNAL_STORAGE,
      'motion': P.ANDROID?.ACTIVITY_RECOGNITION,
      'speech-recognition': P.ANDROID?.RECORD_AUDIO,
      'storage': P.ANDROID?.READ_EXTERNAL_STORAGE,
    }
    return androidMap[name] ?? null
  }

  return null
}

/**
 * Normalize react-native-permissions status strings to our PermissionStatus enum.
 */
function normalizeStatus(status: string): PermissionStatus {
  switch (status) {
    case 'granted': return PermissionStatus.GRANTED
    case 'denied': return PermissionStatus.DENIED
    case 'blocked': return PermissionStatus.BLOCKED
    case 'unavailable': return PermissionStatus.UNAVAILABLE
    case 'limited': return PermissionStatus.LIMITED
    default: return PermissionStatus.UNDETERMINED
  }
}

// ─── Expo fallback helpers ──────────────────────────────────────────────────

async function expoCheck(name: PermissionName): Promise<PermissionStatus> {
  /* eslint-disable @typescript-eslint/no-explicit-any */
  try {
    let mod: any = null
    switch (name) {
      case 'camera':
        mod = require('expo-camera')
        const camResult = await mod.Camera.getCameraPermissionsAsync()
        return normalizeStatus(camResult.status)
      case 'microphone':
        mod = require('expo-camera')
        const micResult = await mod.Camera.getMicrophonePermissionsAsync()
        return normalizeStatus(micResult.status)
      case 'location':
      case 'location-always':
        mod = require('expo-location')
        if (name === 'location-always') {
          const bgResult = await mod.getBackgroundPermissionsAsync()
          return normalizeStatus(bgResult.status)
        }
        const fgResult = await mod.getForegroundPermissionsAsync()
        return normalizeStatus(fgResult.status)
      case 'photo-library':
      case 'media-library':
        mod = require('expo-media-library')
        const mlResult = await mod.getPermissionsAsync()
        return normalizeStatus(mlResult.status)
      case 'contacts':
        mod = require('expo-contacts')
        const ctResult = await mod.getPermissionsAsync()
        return normalizeStatus(ctResult.status)
      case 'calendar':
        mod = require('expo-calendar')
        const calResult = await mod.getCalendarPermissionsAsync()
        return normalizeStatus(calResult.status)
      case 'notifications':
        mod = require('expo-notifications')
        const nResult = await mod.getPermissionsAsync()
        return normalizeStatus(nResult.status)
      default:
        return PermissionStatus.UNAVAILABLE
    }
  } catch {
    return PermissionStatus.UNAVAILABLE
  }
  /* eslint-enable @typescript-eslint/no-explicit-any */
}

async function expoRequest(name: PermissionName): Promise<PermissionStatus> {
  /* eslint-disable @typescript-eslint/no-explicit-any */
  try {
    let mod: any = null
    switch (name) {
      case 'camera':
        mod = require('expo-camera')
        const camResult = await mod.Camera.requestCameraPermissionsAsync()
        return normalizeStatus(camResult.status)
      case 'microphone':
        mod = require('expo-camera')
        const micResult = await mod.Camera.requestMicrophonePermissionsAsync()
        return normalizeStatus(micResult.status)
      case 'location':
        mod = require('expo-location')
        const fgResult = await mod.requestForegroundPermissionsAsync()
        return normalizeStatus(fgResult.status)
      case 'location-always':
        mod = require('expo-location')
        await mod.requestForegroundPermissionsAsync()
        const bgResult = await mod.requestBackgroundPermissionsAsync()
        return normalizeStatus(bgResult.status)
      case 'photo-library':
      case 'media-library':
        mod = require('expo-media-library')
        const mlResult = await mod.requestPermissionsAsync()
        return normalizeStatus(mlResult.status)
      case 'contacts':
        mod = require('expo-contacts')
        const ctResult = await mod.requestPermissionsAsync()
        return normalizeStatus(ctResult.status)
      case 'calendar':
        mod = require('expo-calendar')
        const calResult = await mod.requestCalendarPermissionsAsync()
        return normalizeStatus(calResult.status)
      case 'notifications':
        mod = require('expo-notifications')
        const nResult = await mod.requestPermissionsAsync()
        return normalizeStatus(nResult.status)
      default:
        return PermissionStatus.UNAVAILABLE
    }
  } catch {
    return PermissionStatus.UNAVAILABLE
  }
  /* eslint-enable @typescript-eslint/no-explicit-any */
}

// ─── Public API ─────────────────────────────────────────────────────────────

/**
 * Check the status of a single permission without prompting the user.
 *
 * @param permission - The permission to check.
 * @returns The current status of the permission.
 *
 * @example
 * ```ts
 * import { check, PermissionStatus } from '@neutron-build/native/device/permissions'
 * const status = await check('camera')
 * if (status === PermissionStatus.GRANTED) {
 *   // Camera is available
 * }
 * ```
 */
export async function check(permission: PermissionName): Promise<PermissionStatus> {
  const rnp = getRNPermissions()
  if (rnp) {
    // Notifications (and any future constant-less permission) resolve
    // through their dedicated provider APIs first (NF-NR-09).
    const special = await checkSpecialPermission(permission)
    if (special !== null) return special
    const mapped = mapPermission(permission)
    if (!mapped) return PermissionStatus.UNAVAILABLE
    const status = await rnp.check(mapped)
    return normalizeStatus(status)
  }

  // Expo fallback: check using individual expo modules
  return expoCheck(permission)
}

/**
 * Request a single permission from the user.
 *
 * If the permission has already been granted, this returns immediately.
 * If the permission has been blocked, the user must go to Settings.
 *
 * @param permission - The permission to request.
 * @returns The resulting status after the user responds.
 *
 * @example
 * ```ts
 * import { request, PermissionStatus } from '@neutron-build/native/device/permissions'
 * const status = await request('camera')
 * if (status === PermissionStatus.BLOCKED) {
 *   await openSettings()
 * }
 * ```
 */
export async function request(permission: PermissionName): Promise<PermissionStatus> {
  const rnp = getRNPermissions()
  if (rnp) {
    const special = await requestSpecialPermission(permission)
    if (special !== null) return special
    const mapped = mapPermission(permission)
    if (!mapped) return PermissionStatus.UNAVAILABLE
    const status = await rnp.request(mapped)
    return normalizeStatus(status)
  }

  return expoRequest(permission)
}

/**
 * Check multiple permissions at once.
 *
 * @param permissions - Array of permissions to check.
 * @returns Record mapping each permission to its status.
 *
 * @example
 * ```ts
 * import { checkMultiple } from '@neutron-build/native/device/permissions'
 * const statuses = await checkMultiple(['camera', 'microphone', 'location'])
 * ```
 */
export async function checkMultiple(
  permissions: PermissionName[],
): Promise<Record<PermissionName, PermissionStatus>> {
  const rnp = getRNPermissions()
  if (rnp) {
    // The same special-path resolution as the single-permission check,
    // shared across both batch shapes (NF-NR-09).
    const out: Partial<Record<PermissionName, PermissionStatus>> = {}
    const nativeNames: string[] = []
    const nativeMapped: Array<{ name: PermissionName; native: string }> = []
    for (const p of permissions) {
      const special = await checkSpecialPermission(p)
      if (special !== null) { out[p] = special; continue }
      const native = mapPermission(p)
      if (native !== null) { nativeNames.push(native); nativeMapped.push({ name: p, native }) }
      else out[p] = PermissionStatus.UNAVAILABLE
    }

    const result = nativeNames.length > 0
      ? await rnp.checkMultiple(nativeNames)
      : {}

    for (const { name, native } of nativeMapped) {
      out[name] = normalizeStatus(result[native] ?? 'unavailable')
    }
    return out as Record<PermissionName, PermissionStatus>
  }

  // Expo fallback: check each individually
  const out: Partial<Record<PermissionName, PermissionStatus>> = {}
  for (const p of permissions) {
    out[p] = await expoCheck(p)
  }
  return out as Record<PermissionName, PermissionStatus>
}

/**
 * Request multiple permissions at once.
 *
 * On Android, this shows a single system dialog for all permissions.
 * On iOS, each permission is requested sequentially.
 *
 * @param permissions - Array of permissions to request.
 * @returns Record mapping each permission to its resulting status.
 *
 * @example
 * ```ts
 * import { requestMultiple } from '@neutron-build/native/device/permissions'
 * const statuses = await requestMultiple(['camera', 'microphone'])
 * ```
 */
export async function requestMultiple(
  permissions: PermissionName[],
): Promise<Record<PermissionName, PermissionStatus>> {
  const rnp = getRNPermissions()
  if (rnp) {
    // Shared special-path resolution (NF-NR-09): notifications resolve
    // through their dedicated provider APIs on every platform.
    const out: Partial<Record<PermissionName, PermissionStatus>> = {}
    const nativeNames: string[] = []
    const nativeMapped: Array<{ name: PermissionName; native: string }> = []
    for (const p of permissions) {
      const special = await requestSpecialPermission(p)
      if (special !== null) { out[p] = special; continue }
      const native = mapPermission(p)
      if (native !== null) { nativeNames.push(native); nativeMapped.push({ name: p, native }) }
      else out[p] = PermissionStatus.UNAVAILABLE
    }

    const result = nativeNames.length > 0
      ? await rnp.requestMultiple(nativeNames)
      : {}

    for (const { name, native } of nativeMapped) {
      out[name] = normalizeStatus(result[native] ?? 'unavailable')
    }
    return out as Record<PermissionName, PermissionStatus>
  }

  // Expo fallback: request each individually
  const out: Partial<Record<PermissionName, PermissionStatus>> = {}
  for (const p of permissions) {
    out[p] = await expoRequest(p)
  }
  return out as Record<PermissionName, PermissionStatus>
}

/**
 * Open the app's system settings page.
 *
 * This is useful when a permission is blocked and the user needs to
 * manually grant it from the system Settings app.
 *
 * @example
 * ```ts
 * import { openSettings } from '@neutron-build/native/device/permissions'
 * const status = await request('camera')
 * if (status === PermissionStatus.BLOCKED) {
 *   // Show a dialog explaining why the permission is needed,
 *   // then open Settings
 *   await openSettings()
 * }
 * ```
 */
export async function openSettings(): Promise<void> {
  const rnp = getRNPermissions()
  if (rnp?.openSettings) {
    await rnp.openSettings()
    return
  }

  // Fallback: use Linking to open settings
  try {
    const { Linking } = require('react-native')
    await Linking.openSettings()
  } catch {
    throw new Error(
      '[neutron-native/device/permissions] Cannot open settings. ' +
      'Install react-native-permissions for full support.'
    )
  }
}

/**
 * Check if a specific notification setting is enabled (iOS only).
 *
 * @returns An object with notification setting details.
 */
export async function checkNotifications(): Promise<{
  status: PermissionStatus
  settings: {
    alert?: boolean
    badge?: boolean
    sound?: boolean
    lockScreen?: boolean
    notificationCenter?: boolean
    criticalAlert?: boolean
  }
}> {
  const rnp = getRNPermissions()
  if (rnp?.checkNotifications) {
    const result = await rnp.checkNotifications()
    return {
      status: normalizeStatus(result.status),
      settings: result.settings ?? {},
    }
  }

  // Expo fallback
  const notifStatus = await expoCheck('notifications')
  return {
    status: notifStatus,
    settings: {},
  }
}
