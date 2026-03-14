/**
 * Haptics TurboModule — web implementation using the Vibration API.
 *
 * Uses navigator.vibrate() (W3C Vibration API). Note that iOS Safari
 * does not support the Vibration API, so calls are no-ops on iOS web.
 *
 * Browser support: Chrome 32+, Firefox 16+, Edge 79+ (not Safari)
 */

import type { HapticsModule } from './haptics.js'
import type { HapticStyle, ModuleMethod } from '../types.js'
import { registerWebModule } from '../registry.js'

const METHODS: readonly ModuleMethod[] = [
  { name: 'impact', kind: 'sync' },
  { name: 'notification', kind: 'sync' },
  { name: 'selection', kind: 'sync' },
  { name: 'vibrate', kind: 'sync' },
  { name: 'isAvailable', kind: 'sync' },
] as const

/** Duration mapping for impact styles (milliseconds) */
const IMPACT_DURATIONS: Record<string, number> = {
  light: 10,
  medium: 20,
  heavy: 40,
  success: 15,
  warning: 25,
  error: 35,
  selection: 5,
}

/** Vibration patterns for notification types */
const NOTIFICATION_PATTERNS: Record<string, number | number[]> = {
  success: [0, 15],
  warning: [0, 25, 50, 25],
  error: [0, 40, 80, 40],
}

const WEB_HAPTICS: HapticsModule = {
  moduleName: 'NeutronHaptics',
  methods: METHODS,

  impact(style: HapticStyle): void {
    const duration = IMPACT_DURATIONS[style] ?? 20
    navigator.vibrate?.(duration)
  },

  notification(type: 'success' | 'warning' | 'error'): void {
    const pattern = NOTIFICATION_PATTERNS[type] ?? 20
    navigator.vibrate?.(pattern)
  },

  selection(): void {
    navigator.vibrate?.(5)
  },

  vibrate(duration?: number): void {
    navigator.vibrate?.(duration ?? 400)
  },

  isAvailable(): boolean {
    return typeof navigator !== 'undefined' && typeof navigator.vibrate === 'function'
  },
}

// ─── Register web implementation ─────────────────────────────────────────────

registerWebModule('NeutronHaptics', () => WEB_HAPTICS)
