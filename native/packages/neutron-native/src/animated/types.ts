/**
 * Animation system types.
 */

import type { NativeStyleProp } from '../types.js'

/** A value shared between JS and UI threads via JSI */
export interface SharedValue<T = number> {
  /** Current value — reading on JS thread, writing triggers UI update */
  value: T
  /** Add a listener for value changes */
  addListener(id: number, callback: (value: T) => void): void
  /** Remove a value change listener */
  removeListener(id: number): void
}

/** Timing animation configuration */
export interface TimingConfig {
  /** Target duration in milliseconds (default: 300) */
  duration?: number
  /** Easing function (default: Easing.inOut(Easing.quad)) */
  easing?: (t: number) => number
}

/** Spring physics configuration */
export interface SpringConfig {
  /** Damping ratio (default: 10) */
  damping?: number
  /** Mass (default: 1) */
  mass?: number
  /** Stiffness (default: 100) */
  stiffness?: number
  /** Initial velocity */
  velocity?: number
  /** Overdamping clamping — stop at toValue instead of overshooting */
  overshootClamping?: boolean
  /** Settle threshold — animation finishes when |velocity| < this (default: 0.01) */
  restDisplacementThreshold?: number
  /** Velocity threshold (default: 0.01) */
  restSpeedThreshold?: number
}

/** Decay animation configuration */
export interface DecayConfig {
  /** Deceleration rate (default: 0.998, 0 = stop instantly, 1 = never stop) */
  deceleration?: number
  /** Initial velocity (required) */
  velocity: number
  /** Optional bounds clamping */
  clamp?: [number, number]
}

/** Union of all animation configs */
export type AnimationConfig = TimingConfig | SpringConfig | DecayConfig

/** A worklet function that returns animated styles from shared values */
export type AnimatedStyleWorklet = () => NativeStyleProp

/** Animation completion callback */
export type AnimationCallback = (finished: boolean, current?: number) => void

/** Extrapolation mode for interpolate() */
export type ExtrapolationType = 'extend' | 'clamp' | 'identity'
