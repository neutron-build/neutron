/**
 * Haptics TurboModule — tactile feedback.
 *
 * iOS: UIImpactFeedbackGenerator, UINotificationFeedbackGenerator, UISelectionFeedbackGenerator
 * Android: Vibrator / VibrationEffect (API 26+)
 */

import type { TurboModule, ModuleMethod, HapticStyle } from '../types.js'
import { getModule, registerModule } from '../registry.js'

export interface HapticsModule extends TurboModule {
  moduleName: 'NeutronHaptics'

  /** Trigger a haptic feedback pattern */
  impact(style: HapticStyle): void

  /** Trigger a notification-style feedback (success/warning/error) */
  notification(type: 'success' | 'warning' | 'error'): void

  /** Trigger a selection change feedback (light tap) */
  selection(): void

  /** Vibrate for a specified duration in ms (Android primarily) */
  vibrate(duration?: number): void

  /** Check if haptic feedback is supported on this device */
  isAvailable(): boolean
}

const METHODS: readonly ModuleMethod[] = [
  { name: 'impact', kind: 'sync' },
  { name: 'notification', kind: 'sync' },
  { name: 'selection', kind: 'sync' },
  { name: 'vibrate', kind: 'sync' },
  { name: 'isAvailable', kind: 'sync' },
] as const

registerModule<HapticsModule>('NeutronHaptics', () => ({
  moduleName: 'NeutronHaptics',
  methods: METHODS,
  impact() {},
  notification() {},
  selection() {},
  vibrate() {},
  isAvailable() { return false },
}))

/**
 * Hook to access the Haptics TurboModule.
 *
 * @example
 * ```tsx
 * const haptics = useHaptics()
 * haptics.impact('medium')       // Button press feedback
 * haptics.notification('success') // Success confirmation
 * haptics.selection()             // Picker / toggle change
 * ```
 */
export function useHaptics(): HapticsModule {
  const mod = getModule<HapticsModule>('NeutronHaptics')
  if (!mod) throw new Error('[neutron-native] NeutronHaptics module not available')
  return mod
}
