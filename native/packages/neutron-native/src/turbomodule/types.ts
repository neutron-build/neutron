/**
 * TurboModule types — JSI bridge between JS and native (iOS/Android) code.
 *
 * TurboModules are lazily loaded native modules accessed through the JSI
 * (JavaScript Interface) instead of the old bridge. This gives synchronous
 * access to native APIs with zero serialization overhead.
 */

/** Serializable types that can cross the JSI boundary */
export type JSIValue =
  | null
  | boolean
  | number
  | string
  | JSIValue[]
  | { [key: string]: JSIValue }

/** A TurboModule method descriptor */
export interface ModuleMethod {
  name: string
  /** 'sync' runs on JS thread; 'async' returns a Promise */
  kind: 'sync' | 'async'
}

/** Base interface every TurboModule must implement */
export interface TurboModule {
  /** Unique module name — must match the native registration */
  readonly moduleName: string
  /** Method descriptors for codegen / introspection */
  readonly methods: readonly ModuleMethod[]
}

/** Result wrapper for async native calls */
export interface NativeResult<T> {
  ok: boolean
  value?: T
  error?: { code: string; message: string }
}

/** Platform-aware module — some methods may only exist on iOS or Android */
export type PlatformGuard<T> = T & {
  /** Check if this module is available on the current platform */
  isAvailable(): boolean
}

/** Event subscription handle */
export interface NativeSubscription {
  remove(): void
}

/** Callback for native event listeners */
export type NativeEventCallback<T = unknown> = (event: T) => void

/** Location coordinate */
export interface Coordinate {
  latitude: number
  longitude: number
  altitude: number | null
  accuracy: number
  altitudeAccuracy: number | null
  heading: number | null
  speed: number | null
  timestamp: number
}

/** Camera capture result */
export interface CaptureResult {
  uri: string
  width: number
  height: number
  fileSize: number
  type: 'photo' | 'video'
  duration?: number
}

/** Biometric authentication result */
export interface BiometricResult {
  success: boolean
  error?: string
  biometryType?: 'FaceID' | 'TouchID' | 'Fingerprint' | 'Iris'
}

/** Device info snapshot */
export interface DeviceInfoSnapshot {
  brand: string
  model: string
  deviceId: string
  systemName: string
  systemVersion: string
  appVersion: string
  buildNumber: string
  bundleId: string
  isTablet: boolean
  isEmulator: boolean
  totalMemory: number
  usedMemory: number
}

/** Network state */
export interface NetInfoState {
  isConnected: boolean
  isInternetReachable: boolean | null
  type: 'wifi' | 'cellular' | 'ethernet' | 'bluetooth' | 'vpn' | 'none' | 'unknown'
  details: {
    ssid?: string
    strength?: number
    cellularGeneration?: '2g' | '3g' | '4g' | '5g'
  } | null
}

/** Haptic feedback style */
export type HapticStyle =
  | 'light'
  | 'medium'
  | 'heavy'
  | 'success'
  | 'warning'
  | 'error'
  | 'selection'

/** Permission status */
export type PermissionStatus =
  | 'granted'
  | 'denied'
  | 'blocked'
  | 'unavailable'
  | 'limited'

/** Permission name — maps to platform-specific identifiers */
export type PermissionName =
  | 'camera'
  | 'location'
  | 'location-always'
  | 'microphone'
  | 'contacts'
  | 'calendar'
  | 'photo-library'
  | 'notifications'
  | 'bluetooth'
  | 'face-id'

/** Notification payload */
export interface NotificationPayload {
  title: string
  body?: string
  data?: Record<string, string>
  badge?: number
  sound?: string
  categoryId?: string
  threadId?: string
}
