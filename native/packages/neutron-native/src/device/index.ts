/**
 * '@neutron/native/device' — unified device API modules.
 *
 * Each module wraps popular React Native / Expo community packages behind
 * a consistent async API with runtime package detection, graceful fallbacks,
 * and full TypeScript types.
 *
 * Peer dependencies are lazily loaded via `require()` — if a package is not
 * installed, the module throws a clear error message telling you what to install.
 *
 * @example
 * ```ts
 * import { camera, location, haptics } from '@neutron/native/device'
 *
 * const photo = await camera.takePicture({ quality: 0.9 })
 * const pos = await location.getCurrentPosition({ accuracy: 'high' })
 * haptics.impact('medium')
 * ```
 *
 * Or import individual modules:
 * ```ts
 * import { takePicture } from '@neutron/native/device/camera'
 * import { getCurrentPosition } from '@neutron/native/device/location'
 * ```
 *
 * @module @neutron/native/device
 */

// ─── Camera ─────────────────────────────────────────────────────────────────
export * as camera from './camera.js'
export type {
  CaptureOptions,
  GalleryOptions,
  CameraResult,
  CameraPermissionStatus,
} from './camera.js'

// ─── Location ───────────────────────────────────────────────────────────────
export * as location from './location.js'
export type {
  LocationCoordinate,
  LocationOptions,
  WatchOptions,
  LocationPermissionStatus,
  LocationSubscription,
} from './location.js'

// ─── Notifications ──────────────────────────────────────────────────────────
export * as notifications from './notifications.js'
export type {
  NotificationContent,
  NotificationTrigger,
  ReceivedNotification,
  NotificationPermissionStatus,
  NotificationSubscription,
} from './notifications.js'

// ─── Biometrics ─────────────────────────────────────────────────────────────
export * as biometrics from './biometrics.js'
export type {
  BiometryType,
  AuthenticateOptions,
  AuthenticateResult,
  BiometricAvailability,
} from './biometrics.js'

// ─── Haptics ────────────────────────────────────────────────────────────────
export * as haptics from './haptics.js'
export type {
  ImpactStyle,
  NotificationType,
} from './haptics.js'

// ─── Clipboard ──────────────────────────────────────────────────────────────
export * as clipboard from './clipboard.js'

// ─── Async Storage ──────────────────────────────────────────────────────────
export * as asyncStorage from './async-storage.js'

// ─── Network Info ───────────────────────────────────────────────────────────
export * as netInfo from './net-info.js'
export type {
  ConnectionType,
  CellularGeneration,
  NetInfoState,
  NetInfoDetails,
  NetInfoSubscription,
  NetInfoConfiguration,
} from './net-info.js'

// ─── Device Info ────────────────────────────────────────────────────────────
export * as deviceInfo from './device-info.js'
export type {
  DeviceSnapshot,
} from './device-info.js'

// ─── Permissions ────────────────────────────────────────────────────────────
export * as permissions from './permissions.js'
export {
  PermissionStatus,
} from './permissions.js'
export type {
  PermissionName,
  PermissionResult,
} from './permissions.js'
