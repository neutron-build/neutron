/**
 * DeviceInfo TurboModule — web implementation using browser APIs.
 *
 * Uses navigator.userAgent, navigator.language, screen dimensions,
 * and the Battery Status API where available.
 *
 * Browser support: All modern browsers (some properties are approximate)
 */

import type { DeviceInfoModule } from './device-info.js'
import type { DeviceInfoSnapshot, ModuleMethod } from '../types.js'
import { registerWebModule } from '../registry.js'

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

/** Parse a rough browser name from the user agent */
function detectBrowser(): string {
  if (typeof navigator === 'undefined') return 'unknown'
  const ua = navigator.userAgent
  if (ua.includes('Firefox/')) return 'Firefox'
  if (ua.includes('Edg/')) return 'Edge'
  if (ua.includes('Chrome/')) return 'Chrome'
  if (ua.includes('Safari/') && !ua.includes('Chrome')) return 'Safari'
  if (ua.includes('Opera') || ua.includes('OPR/')) return 'Opera'
  return 'unknown'
}

/** Parse the browser version from the user agent */
function detectBrowserVersion(): string {
  if (typeof navigator === 'undefined') return '0'
  const ua = navigator.userAgent
  // Try common patterns
  const patterns = [
    /Firefox\/([\d.]+)/,
    /Edg\/([\d.]+)/,
    /Chrome\/([\d.]+)/,
    /Version\/([\d.]+).*Safari/,
    /OPR\/([\d.]+)/,
  ]
  for (const pattern of patterns) {
    const match = ua.match(pattern)
    if (match) return match[1]
  }
  return '0'
}

/** Detect operating system from user agent */
function detectOS(): { name: string; version: string } {
  if (typeof navigator === 'undefined') return { name: 'unknown', version: '0' }
  const ua = navigator.userAgent
  if (ua.includes('Windows')) {
    const match = ua.match(/Windows NT ([\d.]+)/)
    return { name: 'Windows', version: match?.[1] ?? '0' }
  }
  if (ua.includes('Mac OS X')) {
    const match = ua.match(/Mac OS X ([\d_.]+)/)
    return { name: 'macOS', version: (match?.[1] ?? '0').replace(/_/g, '.') }
  }
  if (ua.includes('Linux')) return { name: 'Linux', version: '0' }
  if (ua.includes('CrOS')) return { name: 'ChromeOS', version: '0' }
  return { name: 'unknown', version: '0' }
}

const WEB_DEVICE_INFO: DeviceInfoModule = {
  moduleName: 'NeutronDeviceInfo',
  methods: METHODS,

  getInfo(): DeviceInfoSnapshot {
    const os = detectOS()
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const deviceMemory = typeof navigator !== 'undefined' ? (navigator as any).deviceMemory : undefined
    return {
      brand: detectBrowser(),
      model: typeof navigator !== 'undefined' ? navigator.platform ?? 'web' : 'web',
      deviceId: 'web',
      systemName: os.name,
      systemVersion: os.version,
      appVersion: detectBrowserVersion(),
      buildNumber: '0',
      bundleId: typeof window !== 'undefined' ? window.location.hostname : 'localhost',
      isTablet: typeof screen !== 'undefined' && Math.min(screen.width, screen.height) >= 600,
      isEmulator: false,
      totalMemory: deviceMemory ? deviceMemory * 1024 * 1024 * 1024 : 0,
      usedMemory: 0,
    }
  },

  getDeviceId(): string {
    return 'web'
  },

  getVersion(): string {
    return detectBrowserVersion()
  },

  getBuildNumber(): string {
    return '0'
  },

  getBundleId(): string {
    return typeof window !== 'undefined' ? window.location.hostname : 'localhost'
  },

  isEmulator(): boolean {
    return false
  },

  isTablet(): boolean {
    return typeof screen !== 'undefined' && Math.min(screen.width, screen.height) >= 600
  },

  getTotalMemory(): number {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const deviceMemory = typeof navigator !== 'undefined' ? (navigator as any).deviceMemory : undefined
    return deviceMemory ? deviceMemory * 1024 * 1024 * 1024 : 0
  },

  async getBatteryLevel(): Promise<number> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const nav = navigator as any
    if (typeof nav.getBattery === 'function') {
      try {
        const battery = await nav.getBattery()
        return battery.level // 0.0 - 1.0
      } catch {
        return -1
      }
    }
    return -1
  },

  async isLowPowerMode(): Promise<boolean> {
    // Web does not expose a low power mode API.
    // Check battery level as a best-effort heuristic.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const nav = navigator as any
    if (typeof nav.getBattery === 'function') {
      try {
        const battery = await nav.getBattery()
        return battery.level <= 0.15 && !battery.charging
      } catch {
        return false
      }
    }
    return false
  },

  getLocale(): string {
    if (typeof navigator !== 'undefined') {
      return navigator.language ?? 'en-US'
    }
    return 'en-US'
  },

  getTimezone(): string {
    try {
      return Intl.DateTimeFormat().resolvedOptions().timeZone
    } catch {
      return 'UTC'
    }
  },
}

// ─── Register web implementation ─────────────────────────────────────────────

registerWebModule('NeutronDeviceInfo', () => WEB_DEVICE_INFO)
