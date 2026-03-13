/**
 * Animation hooks — backward-compatible re-exports from worklets.ts.
 *
 * This file previously contained the entire animation implementation using
 * @preact/signals-core. It now delegates to worklets.ts which provides:
 *   1. Reanimated delegation when react-native-reanimated is installed
 *   2. RN Animated.Value fallback with useNativeDriver: true
 *   3. Pure JS interpolation and easing (always available)
 *
 * All public exports are preserved for backward compatibility.
 * New code should import directly from './worklets.js' or from
 * the '@neutron/native/animated' barrel export.
 */

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

  // Utility
  hasReanimated,
} from './worklets.js'
