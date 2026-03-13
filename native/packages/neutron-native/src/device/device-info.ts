/**
 * DeviceInfo — device metadata wrapping react-native-device-info and expo-device.
 *
 * Peer dependencies (install one):
 *   - react-native-device-info (bare React Native — comprehensive)
 *   - expo-device (Expo managed/bare)
 *   - expo-application (Expo — for app-level info)
 *
 * @module @neutron/native/device/device-info
 */

// ─── Types ──────────────────────────────────────────────────────────────────

/** Complete device info snapshot */
export interface DeviceSnapshot {
  /** Device brand (e.g. 'Apple', 'Samsung') */
  brand: string
  /** Device model identifier (e.g. 'iPhone 15 Pro', 'Pixel 8') */
  model: string
  /** Device model ID / hardware string (e.g. 'iPhone16,2') */
  modelId: string
  /** OS name (e.g. 'iOS', 'Android') */
  systemName: string
  /** OS version string (e.g. '18.0', '15') */
  systemVersion: string
  /** App version (e.g. '1.0.0') */
  appVersion: string
  /** App build number (e.g. '42') */
  buildNumber: string
  /** Bundle ID / application ID (e.g. 'com.myapp') */
  bundleId: string
  /** Whether the device is a tablet */
  isTablet: boolean
  /** Whether running on an emulator/simulator */
  isEmulator: boolean
  /** Device locale (e.g. 'en-US') */
  locale: string
  /** Device timezone (e.g. 'America/New_York') */
  timezone: string
}

// ─── Lazy module loaders ────────────────────────────────────────────────────

/* eslint-disable @typescript-eslint/no-explicit-any */
let _rnDeviceInfo: any = undefined
let _expoDevice: any = undefined
let _expoApplication: any = undefined

function getRNDeviceInfo(): any {
  if (_rnDeviceInfo === undefined) {
    try {
      const mod = require('react-native-device-info')
      _rnDeviceInfo = mod.default ?? mod
    } catch {
      _rnDeviceInfo = null
    }
  }
  return _rnDeviceInfo
}

function getExpoDevice(): any {
  if (_expoDevice === undefined) {
    try { _expoDevice = require('expo-device') } catch { _expoDevice = null }
  }
  return _expoDevice
}

function getExpoApplication(): any {
  if (_expoApplication === undefined) {
    try { _expoApplication = require('expo-application') } catch { _expoApplication = null }
  }
  return _expoApplication
}

function getPlatform(): any {
  try { return require('react-native').Platform } catch { return null }
}
/* eslint-enable @typescript-eslint/no-explicit-any */

// ─── Public API ─────────────────────────────────────────────────────────────

/**
 * Get the unique device identifier.
 *
 * On iOS, this is the IDFV (vendor ID). On Android, this is ANDROID_ID.
 * Note: The ID may change on factory reset or app reinstall.
 *
 * @returns The device identifier string.
 *
 * @example
 * ```ts
 * import { getDeviceId } from '@neutron/native/device/device-info'
 * const id = await getDeviceId()
 * ```
 */
export async function getDeviceId(): Promise<string> {
  const rndi = getRNDeviceInfo()
  if (rndi) {
    return rndi.getUniqueId()
  }

  const expoDevice = getExpoDevice()
  if (expoDevice) {
    // expo-device doesn't provide a unique ID directly, but modelId is stable
    return expoDevice.modelId ?? expoDevice.osBuildId ?? 'unknown'
  }

  return 'unknown'
}

/**
 * Get the device model name.
 *
 * @returns Human-readable model name (e.g. 'iPhone 15 Pro', 'Pixel 8').
 *
 * @example
 * ```ts
 * import { getModel } from '@neutron/native/device/device-info'
 * const model = await getModel()
 * console.log(`Running on ${model}`)
 * ```
 */
export async function getModel(): Promise<string> {
  const rndi = getRNDeviceInfo()
  if (rndi) {
    return rndi.getModel()
  }

  const expoDevice = getExpoDevice()
  if (expoDevice) {
    return expoDevice.modelName ?? 'unknown'
  }

  return 'unknown'
}

/**
 * Get the device brand (manufacturer).
 *
 * @returns Brand name (e.g. 'Apple', 'Samsung', 'Google').
 */
export async function getBrand(): Promise<string> {
  const rndi = getRNDeviceInfo()
  if (rndi) {
    return rndi.getBrand()
  }

  const expoDevice = getExpoDevice()
  if (expoDevice) {
    return expoDevice.brand ?? expoDevice.manufacturer ?? 'unknown'
  }

  return 'unknown'
}

/**
 * Get the operating system name.
 *
 * @returns OS name (e.g. 'iOS', 'Android', 'iPadOS').
 */
export async function getSystemName(): Promise<string> {
  const rndi = getRNDeviceInfo()
  if (rndi) {
    return rndi.getSystemName()
  }

  const expoDevice = getExpoDevice()
  if (expoDevice) {
    return expoDevice.osName ?? getPlatform()?.OS ?? 'unknown'
  }

  const platform = getPlatform()
  if (platform) {
    return platform.OS === 'ios' ? 'iOS' : platform.OS === 'android' ? 'Android' : platform.OS
  }

  return 'unknown'
}

/**
 * Get the operating system version.
 *
 * @returns OS version string (e.g. '18.0', '15').
 */
export async function getSystemVersion(): Promise<string> {
  const rndi = getRNDeviceInfo()
  if (rndi) {
    return rndi.getSystemVersion()
  }

  const expoDevice = getExpoDevice()
  if (expoDevice) {
    return expoDevice.osVersion ?? String(getPlatform()?.Version ?? '0')
  }

  const platform = getPlatform()
  if (platform) {
    return String(platform.Version ?? '0')
  }

  return '0'
}

/**
 * Check if the device is a tablet.
 *
 * @returns true if the device is classified as a tablet.
 *
 * @example
 * ```ts
 * import { isTablet } from '@neutron/native/device/device-info'
 * if (await isTablet()) {
 *   // Use tablet-optimized layout
 * }
 * ```
 */
export async function isTablet(): Promise<boolean> {
  const rndi = getRNDeviceInfo()
  if (rndi) {
    return rndi.isTablet()
  }

  const expoDevice = getExpoDevice()
  if (expoDevice) {
    // expo-device: DeviceType.TABLET === 2
    return expoDevice.deviceType === 2
  }

  return false
}

/**
 * Check if the app is running on an emulator/simulator.
 *
 * @returns true if running on an emulator or simulator.
 */
export async function isEmulator(): Promise<boolean> {
  const rndi = getRNDeviceInfo()
  if (rndi) {
    return rndi.isEmulator()
  }

  const expoDevice = getExpoDevice()
  if (expoDevice) {
    return !expoDevice.isDevice
  }

  return false
}

/**
 * Get the app version string.
 *
 * @returns App version (e.g. '1.0.0').
 */
export async function getAppVersion(): Promise<string> {
  const rndi = getRNDeviceInfo()
  if (rndi) {
    return rndi.getVersion()
  }

  const expoApp = getExpoApplication()
  if (expoApp) {
    return expoApp.nativeApplicationVersion ?? '0.0.0'
  }

  return '0.0.0'
}

/**
 * Get the app build number.
 *
 * @returns Build number string (e.g. '42').
 */
export async function getBuildNumber(): Promise<string> {
  const rndi = getRNDeviceInfo()
  if (rndi) {
    return rndi.getBuildNumber()
  }

  const expoApp = getExpoApplication()
  if (expoApp) {
    return expoApp.nativeBuildVersion ?? '0'
  }

  return '0'
}

/**
 * Get the app bundle ID / application ID.
 *
 * @returns Bundle identifier (e.g. 'com.myapp').
 */
export async function getBundleId(): Promise<string> {
  const rndi = getRNDeviceInfo()
  if (rndi) {
    return rndi.getBundleId()
  }

  const expoApp = getExpoApplication()
  if (expoApp) {
    return expoApp.applicationId ?? 'com.unknown'
  }

  return 'com.unknown'
}

/**
 * Get the current battery level.
 *
 * @returns Battery level from 0 to 1, or -1 if unavailable.
 */
export async function getBatteryLevel(): Promise<number> {
  const rndi = getRNDeviceInfo()
  if (rndi) {
    return rndi.getBatteryLevel()
  }

  let expoBattery: any // eslint-disable-line @typescript-eslint/no-explicit-any
  try { expoBattery = require('expo-battery') } catch { /* empty */ }

  if (expoBattery) {
    return expoBattery.getBatteryLevelAsync()
  }

  return -1
}

/**
 * Check if the device is in low power mode / battery saver.
 *
 * @returns true if low power mode is enabled.
 */
export async function isLowPowerMode(): Promise<boolean> {
  const rndi = getRNDeviceInfo()
  if (rndi) {
    return rndi.getPowerState().then((s: any) => s.lowPowerMode ?? false) // eslint-disable-line @typescript-eslint/no-explicit-any
  }

  let expoBattery: any // eslint-disable-line @typescript-eslint/no-explicit-any
  try { expoBattery = require('expo-battery') } catch { /* empty */ }

  if (expoBattery) {
    return expoBattery.isLowPowerModeEnabledAsync()
  }

  return false
}

/**
 * Get the total device RAM in bytes.
 *
 * @returns Total memory in bytes, or 0 if unavailable.
 */
export async function getTotalMemory(): Promise<number> {
  const rndi = getRNDeviceInfo()
  if (rndi) {
    return rndi.getTotalMemory()
  }

  const expoDevice = getExpoDevice()
  if (expoDevice) {
    return expoDevice.totalMemory ?? 0
  }

  return 0
}

/**
 * Get the device locale string.
 *
 * @returns Locale identifier (e.g. 'en-US').
 */
export function getLocale(): string {
  const rndi = getRNDeviceInfo()
  if (rndi?.getDeviceLocale) {
    return rndi.getDeviceLocale()
  }

  let expoLocalization: any // eslint-disable-line @typescript-eslint/no-explicit-any
  try { expoLocalization = require('expo-localization') } catch { /* empty */ }

  if (expoLocalization) {
    // Expo SDK 50+: getLocales() returns array
    const locales = expoLocalization.getLocales?.()
    if (Array.isArray(locales) && locales.length > 0) {
      return locales[0].languageTag ?? 'en-US'
    }
    return expoLocalization.locale ?? 'en-US'
  }

  return 'en-US'
}

/**
 * Get the device timezone.
 *
 * @returns IANA timezone identifier (e.g. 'America/New_York').
 */
export function getTimezone(): string {
  const rndi = getRNDeviceInfo()
  if (rndi?.getTimezone) {
    return rndi.getTimezone()
  }

  let expoLocalization: any // eslint-disable-line @typescript-eslint/no-explicit-any
  try { expoLocalization = require('expo-localization') } catch { /* empty */ }

  if (expoLocalization) {
    return expoLocalization.timezone ?? Intl.DateTimeFormat().resolvedOptions().timeZone ?? 'UTC'
  }

  // Fallback to Intl API (works on Hermes and V8)
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone
  } catch {
    return 'UTC'
  }
}

/**
 * Get a complete device info snapshot.
 *
 * @returns A snapshot of all device metadata.
 *
 * @example
 * ```ts
 * import { getSnapshot } from '@neutron/native/device/device-info'
 * const info = await getSnapshot()
 * console.log(`${info.brand} ${info.model} running ${info.systemName} ${info.systemVersion}`)
 * ```
 */
export async function getSnapshot(): Promise<DeviceSnapshot> {
  const [brand, model, systemName, systemVersion, appVersion, buildNumber, bundleId, tablet, emulator] =
    await Promise.all([
      getBrand(),
      getModel(),
      getSystemName(),
      getSystemVersion(),
      getAppVersion(),
      getBuildNumber(),
      getBundleId(),
      isTablet(),
      isEmulator(),
    ])

  const rndi = getRNDeviceInfo()
  const modelId = rndi?.getDeviceId?.() ?? ''

  return {
    brand,
    model,
    modelId,
    systemName,
    systemVersion,
    appVersion,
    buildNumber,
    bundleId,
    isTablet: tablet,
    isEmulator: emulator,
    locale: getLocale(),
    timezone: getTimezone(),
  }
}
