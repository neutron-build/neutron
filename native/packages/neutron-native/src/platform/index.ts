/**
 * Platform — runtime detection utilities.
 *
 * Mirrors React Native's Platform API but adds Neutron-specific helpers.
 */

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const g = globalThis as any

export type OS = 'ios' | 'android' | 'web' | 'macos' | 'windows'

function _detect(): OS {
  // React Native environment
  if (g.Platform?.OS) return g.Platform.OS as OS
  // Hermes V1 injects __hermes__
  if (g.__hermes__) {
    const userAgent = g.navigator?.userAgent ?? ''
    if (userAgent.includes('iPhone') || userAgent.includes('iPad')) return 'ios'
    return 'android'
  }
  // Browser / web
  if (typeof document !== 'undefined' && typeof window !== 'undefined') return 'web'
  return 'android' // safe fallback
}

export const Platform = {
  OS: _detect(),
  get isIOS(): boolean { return this.OS === 'ios' },
  get isAndroid(): boolean { return this.OS === 'android' },
  get isWeb(): boolean { return this.OS === 'web' },
  get isNative(): boolean { return this.OS === 'ios' || this.OS === 'android' },

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
