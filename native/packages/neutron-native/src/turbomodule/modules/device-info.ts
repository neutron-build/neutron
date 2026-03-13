/**
 * DeviceInfo TurboModule — device hardware and software information.
 *
 * iOS: UIDevice, ProcessInfo
 * Android: Build, ActivityManager
 */

import type { TurboModule, ModuleMethod, DeviceInfoSnapshot } from '../types.js'
import { getModule, registerModule } from '../registry.js'

export interface DeviceInfoModule extends TurboModule {
  moduleName: 'NeutronDeviceInfo'

  /** Get full device info snapshot (cached after first call) */
  getInfo(): DeviceInfoSnapshot

  /** Get device unique identifier (vendor ID on iOS, ANDROID_ID on Android) */
  getDeviceId(): string

  /** Get app version string (e.g. '1.0.0') */
  getVersion(): string

  /** Get build number string */
  getBuildNumber(): string

  /** Get bundle ID / application ID */
  getBundleId(): string

  /** Check if running on a physical device vs simulator/emulator */
  isEmulator(): boolean

  /** Check if the device is a tablet */
  isTablet(): boolean

  /** Get total device RAM in bytes */
  getTotalMemory(): number

  /** Get battery level (0-1) or -1 if unavailable */
  getBatteryLevel(): Promise<number>

  /** Check if device is in low power mode */
  isLowPowerMode(): Promise<boolean>

  /** Get device locale (e.g. 'en-US') */
  getLocale(): string

  /** Get device timezone (e.g. 'America/New_York') */
  getTimezone(): string
}

const METHODS: readonly ModuleMethod[] = [
  { name: 'getInfo', kind: 'sync' },
  { name: 'getDeviceId', kind: 'sync' },
  { name: 'getVersion', kind: 'sync' },
  { name: 'getBuildNumber', kind: 'sync' },
  { name: 'getBundleId', kind: 'sync' },
  { name: 'isEmulator', kind: 'sync' },
  { name: 'isTablet', kind: 'sync' },
  { name: 'getTotalMemory', kind: 'sync' },
  { name: 'getBatteryLevel', kind: 'async' },
  { name: 'isLowPowerMode', kind: 'async' },
  { name: 'getLocale', kind: 'sync' },
  { name: 'getTimezone', kind: 'sync' },
] as const

registerModule<DeviceInfoModule>('NeutronDeviceInfo', () => ({
  moduleName: 'NeutronDeviceInfo',
  methods: METHODS,
  getInfo() {
    return {
      brand: 'unknown', model: 'unknown', deviceId: 'unknown',
      systemName: 'unknown', systemVersion: '0', appVersion: '0.0.0',
      buildNumber: '0', bundleId: 'com.unknown', isTablet: false,
      isEmulator: true, totalMemory: 0, usedMemory: 0,
    }
  },
  getDeviceId() { return 'unknown' },
  getVersion() { return '0.0.0' },
  getBuildNumber() { return '0' },
  getBundleId() { return 'com.unknown' },
  isEmulator() { return true },
  isTablet() { return false },
  getTotalMemory() { return 0 },
  async getBatteryLevel() { return -1 },
  async isLowPowerMode() { return false },
  getLocale() { return 'en-US' },
  getTimezone() { return 'UTC' },
}))

/**
 * Hook to access the DeviceInfo TurboModule.
 *
 * @example
 * ```tsx
 * const device = useDeviceInfo()
 * console.log(device.getInfo())
 * console.log(`Running on ${device.isEmulator() ? 'emulator' : 'device'}`)
 * ```
 */
export function useDeviceInfo(): DeviceInfoModule {
  const mod = getModule<DeviceInfoModule>('NeutronDeviceInfo')
  if (!mod) throw new Error('[neutron-native] NeutronDeviceInfo module not available')
  return mod
}
