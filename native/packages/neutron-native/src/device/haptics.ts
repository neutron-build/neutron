/**
 * Haptics — vibration and tactile feedback wrapping expo-haptics
 * and React Native's Vibration API.
 *
 * Peer dependencies (install one for best experience):
 *   - expo-haptics (Expo managed/bare — iOS Taptic Engine + Android vibration)
 *   - react-native (Vibration API is built-in — basic fallback)
 *
 * @module @neutron/native/device/haptics
 */

// ─── Types ──────────────────────────────────────────────────────────────────

/** Impact feedback intensity */
export type ImpactStyle = 'light' | 'medium' | 'heavy' | 'soft' | 'rigid'

/** Notification feedback type */
export type NotificationType = 'success' | 'warning' | 'error'

// ─── Lazy module loaders ────────────────────────────────────────────────────

/* eslint-disable @typescript-eslint/no-explicit-any */
let _expoHaptics: any = undefined
let _rnVibration: any = undefined

function getExpoHaptics(): any {
  if (_expoHaptics === undefined) {
    try { _expoHaptics = require('expo-haptics') } catch { _expoHaptics = null }
  }
  return _expoHaptics
}

function getVibration(): any {
  if (_rnVibration === undefined) {
    try {
      const rn = require('react-native')
      _rnVibration = rn.Vibration ?? null
    } catch {
      _rnVibration = null
    }
  }
  return _rnVibration
}

function getPlatformOS(): string {
  try {
    const rn = require('react-native')
    return rn.Platform?.OS ?? 'unknown'
  } catch {
    return 'unknown'
  }
}
/* eslint-enable @typescript-eslint/no-explicit-any */

// ─── Vibration duration mapping ─────────────────────────────────────────────

const IMPACT_DURATIONS: Record<ImpactStyle, number> = {
  light: 10,
  medium: 20,
  heavy: 30,
  soft: 5,
  rigid: 25,
}

const NOTIFICATION_PATTERNS: Record<NotificationType, number[]> = {
  success: [0, 20, 60, 20],
  warning: [0, 25, 40, 25, 40, 25],
  error: [0, 30, 40, 30, 40, 30, 40, 30],
}

// ─── Public API ─────────────────────────────────────────────────────────────

/**
 * Trigger an impact feedback vibration.
 *
 * On iOS with expo-haptics, this uses the Taptic Engine for precise haptic
 * feedback. On Android or without expo-haptics, falls back to Vibration API.
 *
 * @param style - Impact intensity (default: 'medium').
 *
 * @example
 * ```ts
 * import { impact } from '@neutron/native/device/haptics'
 * impact('light')   // Subtle tap
 * impact('heavy')   // Strong thump
 * ```
 */
export async function impact(style: ImpactStyle = 'medium'): Promise<void> {
  const expo = getExpoHaptics()
  if (expo) {
    const styleMap: Record<ImpactStyle, any> = { // eslint-disable-line @typescript-eslint/no-explicit-any
      light: expo.ImpactFeedbackStyle?.Light ?? 'light',
      medium: expo.ImpactFeedbackStyle?.Medium ?? 'medium',
      heavy: expo.ImpactFeedbackStyle?.Heavy ?? 'heavy',
      soft: expo.ImpactFeedbackStyle?.Soft ?? expo.ImpactFeedbackStyle?.Light ?? 'light',
      rigid: expo.ImpactFeedbackStyle?.Rigid ?? expo.ImpactFeedbackStyle?.Heavy ?? 'heavy',
    }
    await expo.impactAsync(styleMap[style])
    return
  }

  const vibration = getVibration()
  if (vibration) {
    vibration.vibrate(IMPACT_DURATIONS[style] ?? 20)
    return
  }
}

/**
 * Trigger a notification-type haptic feedback.
 *
 * On iOS, maps to UINotificationFeedbackGenerator for distinct success,
 * warning, and error haptic patterns.
 *
 * @param type - Notification type (default: 'success').
 *
 * @example
 * ```ts
 * import { notification } from '@neutron/native/device/haptics'
 * notification('success')  // Payment complete
 * notification('error')    // Validation failed
 * ```
 */
export async function notification(type: NotificationType = 'success'): Promise<void> {
  const expo = getExpoHaptics()
  if (expo) {
    const typeMap: Record<NotificationType, any> = { // eslint-disable-line @typescript-eslint/no-explicit-any
      success: expo.NotificationFeedbackType?.Success ?? 'success',
      warning: expo.NotificationFeedbackType?.Warning ?? 'warning',
      error: expo.NotificationFeedbackType?.Error ?? 'error',
    }
    await expo.notificationAsync(typeMap[type])
    return
  }

  const vibration = getVibration()
  if (vibration) {
    const pattern = NOTIFICATION_PATTERNS[type]
    if (pattern) {
      // On Android, Vibration.vibrate with an array plays a pattern
      // On iOS, Vibration.vibrate ignores patterns — best effort
      if (getPlatformOS() === 'android') {
        vibration.vibrate(pattern)
      } else {
        vibration.vibrate(25)
      }
    }
    return
  }
}

/**
 * Trigger a selection change haptic (light tap).
 *
 * Ideal for picker scrolling, toggle switches, and segment controls.
 *
 * @example
 * ```ts
 * import { selection } from '@neutron/native/device/haptics'
 * selection()  // Picker wheel tick
 * ```
 */
export async function selection(): Promise<void> {
  const expo = getExpoHaptics()
  if (expo) {
    await expo.selectionAsync()
    return
  }

  const vibration = getVibration()
  if (vibration) {
    vibration.vibrate(5)
    return
  }
}

/**
 * Trigger a raw vibration for a specified duration.
 *
 * @param durationMs - Vibration duration in milliseconds (default: 400).
 *   On iOS, duration is ignored by the system (always a fixed pulse).
 *
 * @example
 * ```ts
 * import { vibrate } from '@neutron/native/device/haptics'
 * vibrate(200)  // 200ms buzz
 * ```
 */
export function vibrate(durationMs: number = 400): void {
  const vibration = getVibration()
  if (vibration) {
    vibration.vibrate(durationMs)
    return
  }
}

/**
 * Trigger a vibration pattern (alternating wait/vibrate durations).
 *
 * @param pattern - Array of milliseconds [wait, vibrate, wait, vibrate, ...].
 * @param repeat - Whether to loop the pattern (default: false).
 *
 * @example
 * ```ts
 * import { vibratePattern } from '@neutron/native/device/haptics'
 * vibratePattern([0, 100, 50, 100, 50, 200])  // SOS-style
 * ```
 */
export function vibratePattern(pattern: number[], repeat: boolean = false): void {
  const vibration = getVibration()
  if (vibration) {
    vibration.vibrate(pattern, repeat)
    return
  }
}

/**
 * Cancel any ongoing vibration.
 */
export function cancel(): void {
  const vibration = getVibration()
  if (vibration) {
    vibration.cancel()
  }
}

/**
 * Check if haptic feedback is available on this device.
 *
 * Returns true if expo-haptics is installed or if the React Native Vibration
 * API is accessible. Note: iOS simulators do not support haptics.
 *
 * @returns true if haptic/vibration APIs are available.
 */
export function isAvailable(): boolean {
  return !!(getExpoHaptics() || getVibration())
}
