/**
 * Platform — runtime detection utilities.
 *
 * Mirrors React Native's Platform API but adds Neutron-specific helpers.
 * Detection never invents a platform: when the environment cannot be
 * identified, OS is 'unknown' and isNative is false (NF-NR-15).
 */

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const g = globalThis as any

export type OS = 'ios' | 'android' | 'web' | 'macos' | 'windows' | 'unknown'

function _detect(): OS {
  // React Native injects Platform with every supported host OS
  // (ios, android, macos, windows, web) — trust it first.
  if (g.Platform?.OS) return g.Platform.OS as OS

  // Browser / web
  if (typeof document !== 'undefined' && typeof window !== 'undefined') return 'web'

  // Hermes engine without the RN Platform global: the documented runtime
  // marker is global.HermesInternal. The ENGINE does not identify the HOST
  // OS — an Android guess here was invented (NF-NR-15). A user agent, when
  // one exists at all, is the only honest signal left.
  if (g.HermesInternal) {
    const userAgent: string = g.navigator?.userAgent ?? ''
    if (/iPhone|iPad|iPod/.test(userAgent)) return 'ios'
    if (/Android/.test(userAgent)) return 'android'
    return 'unknown'
  }

  return 'unknown'
}

export const Platform = {
  OS: _detect(),
  get isIOS(): boolean { return this.OS === 'ios' },
  get isAndroid(): boolean { return this.OS === 'android' },
  get isWeb(): boolean { return this.OS === 'web' },
  get isNative(): boolean { return this.OS === 'ios' || this.OS === 'android' || this.OS === 'macos' || this.OS === 'windows' },

  /**
   * Select a value based on platform.
   * @example Platform.select({ ios: 'SF Pro', android: 'Roboto', default: 'sans-serif' })
   */
  select<T>(specifics: Partial<Record<OS | 'default', T>>): T {
    return (specifics[this.OS] ?? specifics.default) as T
  },

  /** OS version string, e.g. '18.0' on iOS */
  Version: (g.Platform?.Version ?? 0) as string | number,
} as const
