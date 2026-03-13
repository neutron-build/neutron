/**
 * '@neutron/native/turbomodule' — TurboModule registry + all device modules.
 */

// ─── Registry ────────────────────────────────────────────────────────────────
export {
  registerModule,
  getModule,
  requireModule,
  hasModule,
  listModules,
  clearCache,
} from './registry.js'

// ─── Types ───────────────────────────────────────────────────────────────────
export type {
  TurboModule,
  ModuleMethod,
  NativeResult,
  NativeSubscription,
  NativeEventCallback,
  PlatformGuard,
  JSIValue,
  Coordinate,
  CaptureResult,
  BiometricResult,
  DeviceInfoSnapshot,
  NetInfoState,
  HapticStyle,
  PermissionStatus,
  PermissionName,
  NotificationPayload,
} from './types.js'

// ─── Device Modules ──────────────────────────────────────────────────────────
export { useCamera, type CameraModule } from './modules/camera.js'
export { useLocation, type LocationModule } from './modules/location.js'
export { useNotifications, type NotificationsModule } from './modules/notifications.js'
export { useBiometrics, type BiometricsModule } from './modules/biometrics.js'
export { useHaptics, type HapticsModule } from './modules/haptics.js'
export { useClipboard, type ClipboardModule } from './modules/clipboard.js'
export { useAsyncStorage, type AsyncStorageModule } from './modules/async-storage.js'
export { useNetInfo, type NetInfoModule } from './modules/net-info.js'
export { useDeviceInfo, type DeviceInfoModule } from './modules/device-info.js'
export { usePermissions, type PermissionsModule } from './modules/permissions.js'
