/**
 * '@neutron/native/gesture' — React Native Gesture Handler compatible API.
 *
 * Architecture:
 *   1. When react-native-gesture-handler is installed, GestureDetector delegates
 *      to it for native gesture recognition with worklet-driven callbacks.
 *   2. When RNGH is not available, a PanResponder-based fallback provides
 *      JS-thread gesture recognition for all gesture types.
 *
 * Usage:
 *   import { GestureDetector, Gesture } from '@neutron/native/gesture'
 *
 *   const pan = Gesture.Pan()
 *     .onUpdate((e) => { offset.value = e.translationX })
 *     .onEnd(() => { offset.value = withSpring(0) })
 *
 *   <GestureDetector gesture={pan}>
 *     <Animated.View style={animStyle}>...</Animated.View>
 *   </GestureDetector>
 */

// ─── GestureDetector with native + PanResponder fallback ─────────────────────

export { GestureDetector, hasGestureHandler } from './handlers.js'

// ─── Gesture builders ────────────────────────────────────────────────────────

export { Gesture } from './gesture.js'
export type { ComposedGesture } from './gesture.js'

// ─── Types ──────────────────────────────────────────────────────────────────

export type {
  PanGesture,
  PinchGesture,
  RotationGesture,
  FlingGesture,
  TapGesture,
  LongPressGesture,
  GestureConfig,
  GestureEvent,
  PanGestureEvent,
  PinchGestureEvent,
  RotationGestureEvent,
  GestureCallback,
  GestureState,
} from './types.js'
