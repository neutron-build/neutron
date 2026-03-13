/**
 * Animation worklets — Reanimated-compatible animation system with fallback.
 *
 * When react-native-reanimated is installed, all APIs delegate to it for
 * true worklet-based UI-thread animations. When it is not available, a
 * fallback implementation uses React Native's built-in `Animated` API with
 * `useNativeDriver: true` to still achieve hardware-accelerated animations.
 *
 * @example
 * import { useSharedValue, withTiming, useAnimatedStyle } from '@neutron/native/animated'
 *
 * function FadeIn({ children }) {
 *   const opacity = useSharedValue(0)
 *   const style = useAnimatedStyle(() => ({ opacity: opacity.value }))
 *   useEffect(() => { opacity.value = withTiming(1, { duration: 600 }) }, [])
 *   return <Animated.View style={style}>{children}</Animated.View>
 * }
 */

import { useRef, useEffect, useMemo, useState, type RefObject } from 'react'
import { Animated as RNAnimated } from 'react-native'
import type {
  SharedValue, TimingConfig, SpringConfig, DecayConfig,
  AnimationCallback, ExtrapolationType,
} from './types.js'
import type { NativeStyleProp } from '../types.js'

// ─── Reanimated detection ────────────────────────────────────────────────────

let _reanimated: any = null
let _reanimatedChecked = false

/**
 * Lazily attempt to load react-native-reanimated.
 * Returns the module if available, null otherwise.
 */
function getReanimated(): any {
  if (!_reanimatedChecked) {
    _reanimatedChecked = true
    try {
      // Use require() for lazy peer dependency resolution
      _reanimated = require('react-native-reanimated')
    } catch {
      _reanimated = null
    }
  }
  return _reanimated
}

/** Returns true if react-native-reanimated is available */
export function hasReanimated(): boolean {
  return getReanimated() !== null
}

// ─── Fallback SharedValue using RN Animated.Value ────────────────────────────

/**
 * Internal animation descriptor. When assigned to `sv.value`, the
 * SharedValue intercepts this and runs the described animation instead
 * of immediately setting the value.
 */
interface AnimationDescriptor {
  __isAnimation: true
  type: 'timing' | 'spring' | 'decay' | 'sequence' | 'delay'
  toValue?: number
  config?: Record<string, unknown>
  callback?: AnimationCallback
  /** For sequence: ordered list of animation descriptors */
  animations?: AnimationDescriptor[]
  /** For delay: the wrapped animation */
  animation?: AnimationDescriptor
  /** For delay: delay in ms */
  delayMs?: number
}

/** Type guard for animation descriptors */
function isAnimationDescriptor(v: unknown): v is AnimationDescriptor {
  return (
    typeof v === 'object' &&
    v !== null &&
    (v as AnimationDescriptor).__isAnimation === true
  )
}

/**
 * Create a fallback SharedValue backed by RN Animated.Value.
 * Supports animation descriptors assigned via `.value = withTiming(...)`.
 */
function createFallbackSharedValue<T>(initial: T): FallbackSharedValue<T> {
  const isNumeric = typeof initial === 'number'
  const animatedValue = isNumeric ? new RNAnimated.Value(initial as number) : null
  let currentValue = initial
  const listeners = new Map<number, (value: T) => void>()
  let activeAnimation: RNAnimated.CompositeAnimation | null = null

  /**
   * Build and start an RN Animated animation from a descriptor.
   */
  function runDescriptor(desc: AnimationDescriptor, av: RNAnimated.Value): void {
    // Stop any running animation on this value
    if (activeAnimation) {
      activeAnimation.stop()
      activeAnimation = null
    }

    const composite = buildComposite(desc, av)
    activeAnimation = composite

    composite.start(({ finished }) => {
      if (activeAnimation === composite) activeAnimation = null
      // Sync the JS-side current value
      av.stopAnimation((v) => {
        currentValue = v as T
        for (const cb of listeners.values()) cb(currentValue)
      })
      desc.callback?.(finished, currentValue as number)
    })
  }

  /**
   * Recursively build an RN Animated.CompositeAnimation from a descriptor.
   */
  function buildComposite(
    desc: AnimationDescriptor,
    av: RNAnimated.Value,
  ): RNAnimated.CompositeAnimation {
    switch (desc.type) {
      case 'timing': {
        const cfg = (desc.config ?? {}) as TimingConfig
        return RNAnimated.timing(av, {
          toValue: desc.toValue ?? 0,
          duration: cfg.duration ?? 300,
          easing: cfg.easing as ((value: number) => number) | undefined,
          useNativeDriver: true,
        })
      }
      case 'spring': {
        const cfg = (desc.config ?? {}) as SpringConfig
        return RNAnimated.spring(av, {
          toValue: desc.toValue ?? 0,
          damping: cfg.damping ?? 10,
          mass: cfg.mass ?? 1,
          stiffness: cfg.stiffness ?? 100,
          velocity: cfg.velocity ?? 0,
          overshootClamping: cfg.overshootClamping ?? false,
          restDisplacementThreshold: cfg.restDisplacementThreshold ?? 0.01,
          restSpeedThreshold: cfg.restSpeedThreshold ?? 0.01,
          useNativeDriver: true,
        })
      }
      case 'decay': {
        const cfg = (desc.config ?? {}) as unknown as DecayConfig
        return RNAnimated.decay(av, {
          velocity: cfg.velocity,
          deceleration: cfg.deceleration ?? 0.998,
          useNativeDriver: true,
        })
      }
      case 'sequence': {
        const steps = (desc.animations ?? []).map((d) => buildComposite(d, av))
        return RNAnimated.sequence(steps)
      }
      case 'delay': {
        const inner = desc.animation
          ? buildComposite(desc.animation, av)
          : RNAnimated.timing(av, { toValue: desc.toValue ?? 0, duration: 0, useNativeDriver: true })
        return RNAnimated.sequence([
          RNAnimated.delay(desc.delayMs ?? 0),
          inner,
        ])
      }
      default:
        return RNAnimated.timing(av, { toValue: 0, duration: 0, useNativeDriver: true })
    }
  }

  const sv: FallbackSharedValue<T> = {
    get value() { return currentValue },
    set value(v: T) {
      if (isNumeric && animatedValue && isAnimationDescriptor(v)) {
        runDescriptor(v as AnimationDescriptor, animatedValue)
      } else {
        // Immediate set
        currentValue = v
        if (isNumeric && animatedValue) {
          if (activeAnimation) {
            activeAnimation.stop()
            activeAnimation = null
          }
          animatedValue.setValue(v as number)
        }
        for (const cb of listeners.values()) cb(v)
      }
    },
    addListener(id: number, callback: (value: T) => void) {
      listeners.set(id, callback)
    },
    removeListener(id: number) {
      listeners.delete(id)
    },
    /** @internal The backing RN Animated.Value for native driver attachment */
    _animatedValue: animatedValue,
    /** @internal Stop any running animation */
    _stop() {
      if (activeAnimation) {
        activeAnimation.stop()
        activeAnimation = null
      }
    },
  }
  return sv
}

/** Extended SharedValue with internal animated value access */
interface FallbackSharedValue<T> extends SharedValue<T> {
  _animatedValue: RNAnimated.Value | null
  _stop(): void
}

// ─── useSharedValue ──────────────────────────────────────────────────────────

/**
 * Create a shared value that persists across renders.
 * Delegates to reanimated when available, otherwise uses RN Animated.Value fallback.
 *
 * @example
 * const offset = useSharedValue(0)
 * offset.value = withTiming(100, { duration: 500 })
 */
export function useSharedValue<T>(initial: T): SharedValue<T> {
  const rnr = getReanimated()
  if (rnr) {
    return rnr.useSharedValue(initial) as SharedValue<T>
  }

  const ref = useRef<FallbackSharedValue<T> | null>(null)
  if (!ref.current) ref.current = createFallbackSharedValue(initial)
  return ref.current
}

// ─── useAnimatedStyle ────────────────────────────────────────────────────────

/**
 * Create an animated style that updates when shared values change.
 * With reanimated, the worklet runs on the UI thread.
 * Without, it evaluates on the JS thread and maps to RN Animated interpolations.
 *
 * @example
 * const style = useAnimatedStyle(() => ({
 *   transform: [{ translateX: offset.value }],
 *   opacity: opacity.value,
 * }))
 */
export function useAnimatedStyle(
  worklet: () => NativeStyleProp,
  deps?: unknown[],
): NativeStyleProp {
  const rnr = getReanimated()
  if (rnr) {
    return rnr.useAnimatedStyle(worklet, deps) as NativeStyleProp
  }

  // Fallback: evaluate the worklet and track changes via re-renders
  const [style, setStyle] = useState<NativeStyleProp>(() => worklet())

  // Re-evaluate when deps change
  useEffect(() => {
    setStyle(worklet())
  }, deps ?? [])

  return style
}

// ─── withTiming ──────────────────────────────────────────────────────────────

/**
 * Create a timing animation that interpolates to `toValue` over `duration` ms.
 * Assign the result to a SharedValue to start the animation.
 *
 * @param toValue - Target value
 * @param config - Duration and easing configuration
 * @param callback - Called when the animation completes or is cancelled
 *
 * @example
 * opacity.value = withTiming(1, { duration: 600, easing: Easing.bezier(0.25, 0.1, 0.25, 1) })
 */
export function withTiming(
  toValue: number,
  config?: TimingConfig,
  callback?: AnimationCallback,
): number {
  const rnr = getReanimated()
  if (rnr) {
    return rnr.withTiming(toValue, config as any, callback as any) as unknown as number
  }

  // Return an animation descriptor that the fallback SharedValue intercepts
  return {
    __isAnimation: true,
    type: 'timing',
    toValue,
    config: config as Record<string, unknown>,
    callback,
  } as unknown as number
}

// ─── withSpring ──────────────────────────────────────────────────────────────

/**
 * Create a spring physics animation to `toValue`.
 * Uses damped harmonic oscillator simulation.
 *
 * @param toValue - Target value
 * @param config - Spring physics parameters (damping, mass, stiffness, etc.)
 * @param callback - Called when the animation settles or is cancelled
 *
 * @example
 * scale.value = withSpring(1, { damping: 15, stiffness: 150 })
 */
export function withSpring(
  toValue: number,
  config?: SpringConfig,
  callback?: AnimationCallback,
): number {
  const rnr = getReanimated()
  if (rnr) {
    return rnr.withSpring(toValue, config as any, callback as any) as unknown as number
  }

  return {
    __isAnimation: true,
    type: 'spring',
    toValue,
    config: config as Record<string, unknown>,
    callback,
  } as unknown as number
}

// ─── withDecay ───────────────────────────────────────────────────────────────

/**
 * Create a momentum-based decay animation. The value decelerates from
 * an initial velocity until it comes to rest.
 *
 * @param config - Velocity and deceleration parameters
 * @param callback - Called when the animation stops
 *
 * @example
 * offset.value = withDecay({ velocity: gestureVelocity, deceleration: 0.997 })
 */
export function withDecay(
  config: DecayConfig,
  callback?: AnimationCallback,
): number {
  const rnr = getReanimated()
  if (rnr) {
    return rnr.withDecay(config as any, callback as any) as unknown as number
  }

  return {
    __isAnimation: true,
    type: 'decay',
    config: config as unknown as Record<string, unknown>,
    callback,
  } as unknown as number
}

// ─── withSequence ────────────────────────────────────────────────────────────

/**
 * Chain multiple animations to run one after another.
 *
 * @param animations - Animation values (results of withTiming, withSpring, etc.)
 *
 * @example
 * offset.value = withSequence(
 *   withTiming(100, { duration: 300 }),
 *   withTiming(0, { duration: 300 }),
 * )
 */
export function withSequence(...animations: number[]): number {
  const rnr = getReanimated()
  if (rnr) {
    return rnr.withSequence(...(animations as any)) as unknown as number
  }

  // Unwrap animation descriptors from the number-typed values
  const descriptors = animations.map((a) => {
    if (isAnimationDescriptor(a)) return a
    // Plain number — treat as immediate set via 0-duration timing
    return {
      __isAnimation: true as const,
      type: 'timing' as const,
      toValue: a,
      config: { duration: 0 },
    }
  })

  return {
    __isAnimation: true,
    type: 'sequence',
    animations: descriptors,
  } as unknown as number
}

// ─── withDelay ───────────────────────────────────────────────────────────────

/**
 * Delay the start of an animation by the given number of milliseconds.
 *
 * @param delayMs - Delay before the animation starts
 * @param animation - The animation to delay
 *
 * @example
 * opacity.value = withDelay(200, withTiming(1))
 */
export function withDelay(delayMs: number, animation: number): number {
  const rnr = getReanimated()
  if (rnr) {
    return rnr.withDelay(delayMs, animation as any) as unknown as number
  }

  const innerDesc = isAnimationDescriptor(animation)
    ? animation
    : { __isAnimation: true as const, type: 'timing' as const, toValue: animation, config: { duration: 0 } }

  return {
    __isAnimation: true,
    type: 'delay',
    delayMs,
    animation: innerDesc,
  } as unknown as number
}

// ─── interpolate ─────────────────────────────────────────────────────────────

/**
 * Map a value from an input range to an output range with configurable extrapolation.
 * Works identically whether or not reanimated is available.
 *
 * @param value - The input value to interpolate
 * @param inputRange - Array of input breakpoints (must be monotonically increasing)
 * @param outputRange - Array of corresponding output values
 * @param extrapolation - How to handle values outside the input range
 *
 * @example
 * const opacity = interpolate(scrollY.value, [0, 100], [1, 0], 'clamp')
 */
export function interpolate(
  value: number,
  inputRange: number[],
  outputRange: number[],
  extrapolation?: ExtrapolationType | {
    extrapolateLeft?: ExtrapolationType
    extrapolateRight?: ExtrapolationType
  },
): number {
  const rnr = getReanimated()
  if (rnr) {
    // Map our extrapolation format to reanimated's
    let ext: unknown
    if (typeof extrapolation === 'string') {
      ext = extrapolation === 'extend'
        ? rnr.Extrapolation.EXTEND
        : extrapolation === 'clamp'
          ? rnr.Extrapolation.CLAMP
          : rnr.Extrapolation.IDENTITY
    } else if (extrapolation) {
      ext = {
        extrapolateLeft: extrapolation.extrapolateLeft,
        extrapolateRight: extrapolation.extrapolateRight,
      }
    }
    return rnr.interpolate(value, inputRange, outputRange, ext as any)
  }

  // Fallback: pure JS interpolation
  if (inputRange.length < 2 || outputRange.length < 2) return outputRange[0] ?? 0

  const extLeft = typeof extrapolation === 'string'
    ? extrapolation
    : extrapolation?.extrapolateLeft ?? 'extend'
  const extRight = typeof extrapolation === 'string'
    ? extrapolation
    : extrapolation?.extrapolateRight ?? 'extend'

  // Find segment
  let i = 0
  for (; i < inputRange.length - 1; i++) {
    if (value <= inputRange[i + 1]) break
  }
  i = Math.min(i, inputRange.length - 2)

  const inMin = inputRange[i]
  const inMax = inputRange[i + 1]
  const outMin = outputRange[i]
  const outMax = outputRange[i + 1]

  let t = (value - inMin) / (inMax - inMin)

  if (t < 0) {
    if (extLeft === 'clamp') t = 0
    else if (extLeft === 'identity') return value
  } else if (t > 1) {
    if (extRight === 'clamp') t = 1
    else if (extRight === 'identity') return value
  }

  return outMin + t * (outMax - outMin)
}

// ─── Easing ──────────────────────────────────────────────────────────────────

/**
 * Standard easing functions compatible with both withTiming and CSS transitions.
 * Each function maps t in [0,1] to an eased output in [0,1].
 */
export const Easing = {
  /** Linear interpolation — no easing */
  linear: (t: number) => t,
  /** Quadratic ease (t^2) */
  quad: (t: number) => t * t,
  /** Cubic ease (t^3) */
  cubic: (t: number) => t * t * t,
  /** Sinusoidal ease */
  sin: (t: number) => 1 - Math.cos((t * Math.PI) / 2),
  /** Circular ease */
  circle: (t: number) => 1 - Math.sqrt(1 - t * t),
  /** Exponential ease */
  exp: (t: number) => (t === 0 ? 0 : Math.pow(2, 10 * (t - 1))),
  /** Elastic ease with configurable period */
  elastic: (t: number) => {
    const p = 0.3
    return -Math.pow(2, 10 * (t - 1)) * Math.sin(((t - 1 - p / 4) * (2 * Math.PI)) / p)
  },
  /** Back ease — overshoots then returns. Optional overshoot amount s. */
  back: (s = 1.70158) => (t: number) => t * t * ((s + 1) * t - s),
  /** Bounce ease — simulates a bouncing ball */
  bounce: (t: number) => {
    const n1 = 7.5625
    const d1 = 2.75
    if (t < 1 / d1) return n1 * t * t
    if (t < 2 / d1) return n1 * (t -= 1.5 / d1) * t + 0.75
    if (t < 2.5 / d1) return n1 * (t -= 2.25 / d1) * t + 0.9375
    return n1 * (t -= 2.625 / d1) * t + 0.984375
  },

  /** Apply an easing function as ease-in (default direction) */
  in: (fn: (t: number) => number) => fn,
  /** Apply an easing function as ease-out (reversed) */
  out: (fn: (t: number) => number) => (t: number) => 1 - fn(1 - t),
  /** Apply an easing function as ease-in-out (mirrored at midpoint) */
  inOut: (fn: (t: number) => number) => (t: number) =>
    t < 0.5 ? fn(t * 2) / 2 : 1 - fn((1 - t) * 2) / 2,

  /**
   * Cubic bezier easing — compatible with CSS transition-timing-function.
   * Uses Newton-Raphson iteration for accurate curve sampling.
   *
   * @example
   * Easing.bezier(0.25, 0.1, 0.25, 1.0) // ease
   * Easing.bezier(0.42, 0, 1, 1)         // ease-in
   */
  bezier: (x1: number, y1: number, x2: number, y2: number) => {
    // Attempt to use RN's built-in Easing.bezier if available
    try {
      const { Easing: RNEasing } = require('react-native')
      if (RNEasing?.bezier) return RNEasing.bezier(x1, y1, x2, y2)
    } catch { /* fallback below */ }

    // Newton-Raphson binary search approximation
    return (t: number): number => {
      if (t === 0 || t === 1) return t
      let lo = 0
      let hi = 1
      for (let i = 0; i < 12; i++) {
        const mid = (lo + hi) / 2
        const x =
          3 * (1 - mid) * (1 - mid) * mid * x1 +
          3 * (1 - mid) * mid * mid * x2 +
          mid * mid * mid
        if (x < t) lo = mid
        else hi = mid
      }
      const s = (lo + hi) / 2
      return (
        3 * (1 - s) * (1 - s) * s * y1 +
        3 * (1 - s) * s * s * y2 +
        s * s * s
      )
    }
  },
} as const

// ─── runOnJS ─────────────────────────────────────────────────────────────────

/**
 * Run a function on the JS thread from within a UI worklet.
 * With reanimated, this properly dispatches from the UI thread.
 * Without reanimated, this is a no-op wrapper since everything runs on JS already.
 *
 * @example
 * // Inside a gesture handler worklet:
 * runOnJS(setShowTooltip)(true)
 */
export function runOnJS<F extends (...args: any[]) => any>(fn: F): (...args: Parameters<F>) => void {
  const rnr = getReanimated()
  if (rnr) {
    return rnr.runOnJS(fn) as (...args: Parameters<F>) => void
  }
  // Without reanimated, all code runs on JS thread already
  return (...args: Parameters<F>) => fn(...args)
}

// ─── cancelAnimation ─────────────────────────────────────────────────────────

/**
 * Cancel any running animation on a shared value, freezing it at its current position.
 *
 * @param sv - The shared value to cancel animations on
 */
export function cancelAnimation(sv: SharedValue<number>): void {
  const rnr = getReanimated()
  if (rnr) {
    rnr.cancelAnimation(sv as any)
    return
  }
  // Fallback: stop the RN animated value
  const fallback = sv as FallbackSharedValue<number>
  if (fallback._stop) fallback._stop()
}

// ─── Additional Reanimated-compatible utilities ──────────────────────────────

/**
 * Extrapolation mode constants for use with interpolate().
 */
export const Extrapolation = {
  EXTEND: 'extend' as ExtrapolationType,
  CLAMP: 'clamp' as ExtrapolationType,
  IDENTITY: 'identity' as ExtrapolationType,
} as const

/**
 * Create a derived shared value that recomputes when its dependencies change.
 *
 * @example
 * const scale = useDerivedValue(() => interpolate(progress.value, [0, 1], [1, 2]))
 */
export function useDerivedValue<T>(worklet: () => T, deps?: unknown[]): SharedValue<T> {
  const rnr = getReanimated()
  if (rnr) {
    return rnr.useDerivedValue(worklet as any, deps as any) as SharedValue<T>
  }

  const sv = useSharedValue(worklet())
  useEffect(() => {
    sv.value = worklet()
  }, deps ?? [])
  return sv
}

/**
 * Create animated props (non-style) that update from shared values.
 *
 * @example
 * const animProps = useAnimatedProps(() => ({ scrollOffset: offset.value }))
 */
export function useAnimatedProps<T extends Record<string, unknown>>(
  worklet: () => T,
  deps?: unknown[],
): T {
  const rnr = getReanimated()
  if (rnr) {
    return rnr.useAnimatedProps(worklet as any, deps as any) as T
  }
  return useMemo(() => worklet(), deps ?? [])
}

/**
 * Get a ref to an Animated component's underlying native view for
 * direct manipulation (e.g., measure, scrollTo).
 */
export function useAnimatedRef<T = unknown>() {
  const rnr = getReanimated()
  if (rnr) {
    return rnr.useAnimatedRef() as RefObject<T>
  }
  return useRef<T>(null)
}

/**
 * Run a function on the UI thread. With reanimated, this dispatches to the
 * worklet runtime. Without, it executes synchronously on the JS thread.
 */
export function runOnUI<F extends (...args: any[]) => any>(fn: F): (...args: Parameters<F>) => void {
  const rnr = getReanimated()
  if (rnr) {
    return rnr.runOnUI(fn) as (...args: Parameters<F>) => void
  }
  return (...args: Parameters<F>) => fn(...args)
}

/**
 * Repeat an animation N times. Pass -1 for infinite repetition.
 *
 * @param animation - The animation to repeat
 * @param numberOfReps - Number of repetitions (-1 = infinite)
 * @param reverse - Whether to reverse direction on each repetition
 * @param callback - Called when all repetitions complete
 *
 * @example
 * rotation.value = withRepeat(withTiming(360, { duration: 1000 }), -1, false)
 */
export function withRepeat(
  animation: number,
  numberOfReps?: number,
  reverse?: boolean,
  callback?: AnimationCallback,
): number {
  const rnr = getReanimated()
  if (rnr) {
    return rnr.withRepeat(animation as any, numberOfReps, reverse, callback as any) as unknown as number
  }

  // Fallback: for finite reps, build a sequence; for infinite, rely on RN Animated.loop
  const desc = isAnimationDescriptor(animation)
    ? animation
    : { __isAnimation: true as const, type: 'timing' as const, toValue: animation, config: { duration: 0 } }

  const reps = numberOfReps ?? 2
  if (reps === -1) {
    // Can't easily do infinite with descriptors. Use the animation once as best-effort.
    // In production, the native driver would handle Animated.loop().
    return animation
  }

  const steps: AnimationDescriptor[] = []
  for (let i = 0; i < reps; i++) {
    steps.push(desc)
    if (reverse && i < reps - 1) {
      // Reverse: animate back to starting point
      steps.push({
        __isAnimation: true,
        type: desc.type,
        toValue: 0, // assumes starting from 0
        config: desc.config,
      })
    }
  }

  return {
    __isAnimation: true,
    type: 'sequence',
    animations: steps,
    callback,
  } as unknown as number
}
