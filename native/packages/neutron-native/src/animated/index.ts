/**
 * '@neutron/native/animated' — Reanimated-compatible animation system.
 *
 * Architecture:
 *   1. When react-native-reanimated is installed, all APIs delegate to it
 *      for true worklet-based UI-thread animations via JSI.
 *   2. When reanimated is not available, a fallback uses React Native's
 *      built-in Animated API with useNativeDriver: true.
 *   3. Interpolation and easing are always pure JS (no native dependency).
 *
 * Usage:
 *   import {
 *     useSharedValue, useAnimatedStyle, withTiming, withSpring,
 *     interpolate, Easing, Animated,
 *   } from '@neutron/native/animated'
 *
 *   const opacity = useSharedValue(0)
 *   const style = useAnimatedStyle(() => ({ opacity: opacity.value }))
 *   useEffect(() => { opacity.value = withTiming(1) }, [])
 *   <Animated.View style={style}>...</Animated.View>
 */

// ─── Worklet-aware hooks and animation drivers ──────────────────────────────

export {
  // Hooks
  useSharedValue,
  useDerivedValue,
  useAnimatedStyle,
  useAnimatedProps,
  useAnimatedRef,

  // Animation drivers
  withTiming,
  withSpring,
  withDecay,
  withDelay,
  withSequence,
  withRepeat,
  cancelAnimation,

  // Thread bridging
  runOnUI,
  runOnJS,

  // Interpolation
  interpolate,
  Extrapolation,

  // Easing
  Easing,

  // Runtime detection
  hasReanimated,
} from './worklets.js'

// ─── Types ──────────────────────────────────────────────────────────────────

export type {
  SharedValue,
  AnimationConfig,
  TimingConfig,
  SpringConfig,
  DecayConfig,
  AnimatedStyleWorklet,
  AnimationCallback,
  ExtrapolationType,
} from './types.js'

// ─── Animated components ────────────────────────────────────────────────────

export { Animated } from './components.js'
