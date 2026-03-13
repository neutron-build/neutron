/**
 * Gesture handlers — React Native Gesture Handler compatible API with PanResponder fallback.
 *
 * When react-native-gesture-handler is installed, GestureDetector delegates to it
 * for native gesture recognition (UIGestureRecognizer on iOS, GestureDetectorCompat
 * on Android). When it is not available, a PanResponder-based fallback provides
 * equivalent (though less performant) gesture recognition on the JS thread.
 *
 * @example
 * import { GestureDetector, Gesture } from '@neutron/native/gesture'
 * import { useSharedValue, withSpring } from '@neutron/native/animated'
 *
 * const offset = useSharedValue(0)
 * const pan = Gesture.Pan()
 *   .onUpdate((e) => { offset.value = e.translationX })
 *   .onEnd(() => { offset.value = withSpring(0) })
 *
 * <GestureDetector gesture={pan}>
 *   <Animated.View style={animStyle}>
 *     <Text>Drag me</Text>
 *   </Animated.View>
 * </GestureDetector>
 */

import React, { type ReactNode, useMemo } from 'react'
import { View, PanResponder, type GestureResponderEvent, type PanResponderGestureState } from 'react-native'
import type {
  GestureConfig, GestureEvent, PanGestureEvent, PinchGestureEvent,
  RotationGestureEvent, GestureState,
  PanGesture, PinchGesture, RotationGesture, FlingGesture,
  TapGesture, LongPressGesture,
} from './types.js'
import type { ComposedGesture } from './gesture.js'

// ─── RNGH detection ──────────────────────────────────────────────────────────

let _rngh: any = null
let _rnghChecked = false

/**
 * Lazily attempt to load react-native-gesture-handler.
 * Returns the module if available, null otherwise.
 */
function getRNGH(): any {
  if (!_rnghChecked) {
    _rnghChecked = true
    try {
      _rngh = require('react-native-gesture-handler')
    } catch {
      _rngh = null
    }
  }
  return _rngh
}

/** Returns true if react-native-gesture-handler is available */
export function hasGestureHandler(): boolean {
  return getRNGH() !== null
}

// ─── Helper: resolve gesture config from builder or config ───────────────────

type GestureInput = GestureConfig | ComposedGesture | { build(): GestureConfig }

function resolveConfig(gesture: GestureInput): GestureConfig | ComposedGesture {
  if ('build' in gesture && typeof gesture.build === 'function') {
    return gesture.build()
  }
  return gesture as GestureConfig | ComposedGesture
}

function isComposed(g: GestureConfig | ComposedGesture): g is ComposedGesture {
  return 'gestures' in g && Array.isArray(g.gestures)
}

// ─── PanResponder Fallback ───────────────────────────────────────────────────

/**
 * Build a PanResponder that dispatches to the appropriate gesture callbacks.
 * This is used when react-native-gesture-handler is not installed.
 *
 * Limitations of the fallback:
 * - Only one gesture can be active at a time (no true simultaneous recognition)
 * - Pinch and rotation require two touches and are approximated
 * - No native-thread worklet execution
 * - Tap detection uses timing heuristics
 */
function buildPanResponderFromConfig(config: GestureConfig | ComposedGesture) {
  // Flatten composed gestures — in fallback mode, we process sequentially
  const gestures: GestureConfig[] = isComposed(config)
    ? config.gestures
    : [config]

  // Find the primary gesture for each type
  const panGesture = gestures.find((g) => g.type === 'pan') as PanGesture | undefined
  const tapGesture = gestures.find((g) => g.type === 'tap') as TapGesture | undefined
  const longPressGesture = gestures.find((g) => g.type === 'longPress') as LongPressGesture | undefined
  const flingGesture = gestures.find((g) => g.type === 'fling') as FlingGesture | undefined
  const pinchGesture = gestures.find((g) => g.type === 'pinch') as PinchGesture | undefined
  const rotationGesture = gestures.find((g) => g.type === 'rotation') as RotationGesture | undefined

  // State tracking
  let touchStartTime = 0
  // touchStartX/Y tracked via PanResponder's gestureState (gs.x0, gs.y0)
  let activatedGestureType: string | null = null
  let longPressTimer: ReturnType<typeof setTimeout> | null = null
  let tapCount = 0
  let lastTapTime = 0

  // Multi-touch tracking for pinch/rotation
  let initialPinchDistance = 0
  let initialRotationAngle = 0

  function makeBaseEvent(evt: GestureResponderEvent, gs: PanResponderGestureState): GestureEvent {
    return {
      state: 'active' as GestureState,
      absoluteX: evt.nativeEvent.pageX,
      absoluteY: evt.nativeEvent.pageY,
      x: evt.nativeEvent.locationX,
      y: evt.nativeEvent.locationY,
      numberOfPointers: gs.numberActiveTouches,
    }
  }

  function makePanEvent(evt: GestureResponderEvent, gs: PanResponderGestureState): PanGestureEvent {
    return {
      ...makeBaseEvent(evt, gs),
      translationX: gs.dx,
      translationY: gs.dy,
      velocityX: gs.vx * 1000, // PanResponder gives px/ms, we want px/s
      velocityY: gs.vy * 1000,
    }
  }

  function getDistance(touches: any[]): number {
    if (touches.length < 2) return 0
    const dx = touches[1].pageX - touches[0].pageX
    const dy = touches[1].pageY - touches[0].pageY
    return Math.sqrt(dx * dx + dy * dy)
  }

  function getAngle(touches: any[]): number {
    if (touches.length < 2) return 0
    return Math.atan2(
      touches[1].pageY - touches[0].pageY,
      touches[1].pageX - touches[0].pageX,
    )
  }

  function clearLongPress() {
    if (longPressTimer !== null) {
      clearTimeout(longPressTimer)
      longPressTimer = null
    }
  }

  return PanResponder.create({
    onStartShouldSetPanResponder: () => true,
    onMoveShouldSetPanResponder: (_evt, gs) => {
      // Only claim movement if we have a pan/pinch/rotation gesture configured
      if (panGesture || pinchGesture || rotationGesture) {
        const minDist = panGesture?.minDistance ?? 10
        return Math.abs(gs.dx) > minDist || Math.abs(gs.dy) > minDist || gs.numberActiveTouches >= 2
      }
      return false
    },
    onPanResponderGrant: (evt, gs) => {
      touchStartTime = Date.now()
      // Touch start coords available via gs.x0, gs.y0
      activatedGestureType = null

      // Multi-touch init
      const touches = (evt.nativeEvent as any).touches
      if (touches && touches.length >= 2) {
        initialPinchDistance = getDistance(touches)
        initialRotationAngle = getAngle(touches)
      }

      // Set up long press timer
      if (longPressGesture && longPressGesture.enabled !== false) {
        const minDuration = longPressGesture.minDuration ?? 500
        longPressTimer = setTimeout(() => {
          activatedGestureType = 'longPress'
          const base = makeBaseEvent(evt, gs)
          base.state = 'active'
          longPressGesture.onStart?.(base)
          longPressGesture.onUpdate?.(base)
        }, minDuration)
      }

      // Fire onBegin for all configured gestures
      const base = makeBaseEvent(evt, gs)
      base.state = 'began'
      for (const g of gestures) {
        if (g.enabled !== false) g.onBegin?.(base as any)
      }
    },
    onPanResponderMove: (evt, gs) => {
      clearLongPress()

      const touches = (evt.nativeEvent as any).touches
      const numTouches = gs.numberActiveTouches

      // Handle pinch (2+ fingers)
      if (numTouches >= 2 && pinchGesture && pinchGesture.enabled !== false && touches?.length >= 2) {
        if (activatedGestureType === null || activatedGestureType === 'pinch') {
          activatedGestureType = 'pinch'
          const currentDist = getDistance(touches)
          const scale = initialPinchDistance > 0 ? currentDist / initialPinchDistance : 1

          const pinchEvt: PinchGestureEvent = {
            ...makeBaseEvent(evt, gs),
            scale,
            velocity: 0, // Would need frame delta tracking for velocity
            focalX: (touches[0].pageX + touches[1].pageX) / 2,
            focalY: (touches[0].pageY + touches[1].pageY) / 2,
          }

          if (pinchGesture.onStart) {
            pinchGesture.onStart(pinchEvt)
          }
          pinchGesture.onUpdate?.(pinchEvt)
          return
        }
      }

      // Handle rotation (2+ fingers)
      if (numTouches >= 2 && rotationGesture && rotationGesture.enabled !== false && touches?.length >= 2) {
        if (activatedGestureType === null || activatedGestureType === 'rotation') {
          activatedGestureType = 'rotation'
          const currentAngle = getAngle(touches)
          const rotation = currentAngle - initialRotationAngle

          const rotEvt: RotationGestureEvent = {
            ...makeBaseEvent(evt, gs),
            rotation,
            velocity: 0,
            anchorX: (touches[0].pageX + touches[1].pageX) / 2,
            anchorY: (touches[0].pageY + touches[1].pageY) / 2,
          }

          rotationGesture.onUpdate?.(rotEvt)
          return
        }
      }

      // Handle pan (single or multi-finger drag)
      if (panGesture && panGesture.enabled !== false) {
        const minDist = panGesture.minDistance ?? 10
        const dist = Math.sqrt(gs.dx * gs.dx + gs.dy * gs.dy)

        if (dist >= minDist) {
          if (activatedGestureType === null) {
            activatedGestureType = 'pan'
            const startEvt = makePanEvent(evt, gs)
            startEvt.state = 'active'
            panGesture.onStart?.(startEvt)
          }
          panGesture.onUpdate?.(makePanEvent(evt, gs))
        }
      }
    },
    onPanResponderRelease: (evt, gs) => {
      clearLongPress()
      const elapsed = Date.now() - touchStartTime
      const dist = Math.sqrt(gs.dx * gs.dx + gs.dy * gs.dy)
      const base = makeBaseEvent(evt, gs)
      base.state = 'end'

      // Check for fling
      if (flingGesture && flingGesture.enabled !== false && activatedGestureType === null) {
        const speed = Math.sqrt(gs.vx * gs.vx + gs.vy * gs.vy)
        if (speed > 0.5 && dist > 50) {
          activatedGestureType = 'fling'
          flingGesture.onStart?.(base)
          flingGesture.onEnd?.(base)
          flingGesture.onFinalize?.(base)
          return
        }
      }

      // Check for tap
      if (tapGesture && tapGesture.enabled !== false && activatedGestureType === null) {
        const maxDuration = tapGesture.maxDuration ?? 300
        const maxDist = tapGesture.maxDistance ?? 10
        const requiredTaps = tapGesture.numberOfTaps ?? 1
        const maxDelay = tapGesture.maxDelay ?? 300

        if (elapsed < maxDuration && dist < maxDist) {
          const now = Date.now()
          if (now - lastTapTime < maxDelay) {
            tapCount++
          } else {
            tapCount = 1
          }
          lastTapTime = now

          if (tapCount >= requiredTaps) {
            tapCount = 0
            activatedGestureType = 'tap'
            tapGesture.onStart?.(base)
            tapGesture.onEnd?.(base)
            tapGesture.onFinalize?.(base)
            return
          }
        }
      }

      // Handle long press end
      if (activatedGestureType === 'longPress' && longPressGesture) {
        longPressGesture.onEnd?.(base)
        longPressGesture.onFinalize?.(base)
        return
      }

      // Handle pinch end
      if (activatedGestureType === 'pinch' && pinchGesture) {
        const touches = (evt.nativeEvent as any).touches ?? []
        const pinchEnd: PinchGestureEvent = {
          ...base,
          scale: touches.length >= 2
            ? getDistance(touches) / (initialPinchDistance || 1)
            : 1,
          velocity: 0,
          focalX: base.absoluteX,
          focalY: base.absoluteY,
        }
        pinchGesture.onEnd?.(pinchEnd)
        pinchGesture.onFinalize?.(pinchEnd)
        return
      }

      // Handle rotation end
      if (activatedGestureType === 'rotation' && rotationGesture) {
        const rotEnd: RotationGestureEvent = {
          ...base,
          rotation: 0,
          velocity: 0,
          anchorX: base.absoluteX,
          anchorY: base.absoluteY,
        }
        rotationGesture.onEnd?.(rotEnd)
        rotationGesture.onFinalize?.(rotEnd)
        return
      }

      // Handle pan end
      if (activatedGestureType === 'pan' && panGesture) {
        const panEnd = makePanEvent(evt, gs)
        panEnd.state = 'end'
        panGesture.onEnd?.(panEnd)
        panGesture.onFinalize?.(panEnd)
        return
      }

      // Finalize all gestures
      for (const g of gestures) {
        if (g.enabled !== false) g.onFinalize?.(base as any)
      }
    },
    onPanResponderTerminate: (evt, gs) => {
      clearLongPress()
      const base = makeBaseEvent(evt, gs)
      base.state = 'cancelled'
      for (const g of gestures) {
        if (g.enabled !== false) g.onFinalize?.(base as any)
      }
      activatedGestureType = null
    },
  })
}

// ─── RNGH Native Gesture Builder ─────────────────────────────────────────────

/**
 * Build a react-native-gesture-handler Gesture object from our config.
 * Maps our gesture config format to RNGH's fluent API.
 */
function buildRNGHGesture(config: GestureConfig): any {
  const rngh = getRNGH()
  if (!rngh) return null

  const RNGesture = rngh.Gesture
  let gesture: any

  switch (config.type) {
    case 'pan': {
      const panCfg = config as PanGesture
      gesture = RNGesture.Pan()
      if (panCfg.minDistance != null) gesture = gesture.minDistance(panCfg.minDistance)
      if (panCfg.minPointers != null) gesture = gesture.minPointers(panCfg.minPointers)
      if (panCfg.maxPointers != null) gesture = gesture.maxPointers(panCfg.maxPointers)
      if (panCfg.activeOffsetX != null) gesture = gesture.activeOffsetX(panCfg.activeOffsetX)
      if (panCfg.activeOffsetY != null) gesture = gesture.activeOffsetY(panCfg.activeOffsetY)
      if (panCfg.failOffsetX != null) gesture = gesture.failOffsetX(panCfg.failOffsetX)
      if (panCfg.failOffsetY != null) gesture = gesture.failOffsetY(panCfg.failOffsetY)
      if (panCfg.avgTouches != null) gesture = gesture.averageTouches(panCfg.avgTouches)
      break
    }
    case 'pinch':
      gesture = RNGesture.Pinch()
      break
    case 'rotation':
      gesture = RNGesture.Rotation()
      break
    case 'fling': {
      const flingCfg = config as FlingGesture
      gesture = RNGesture.Fling()
      if (flingCfg.direction != null) {
        const dirMap: Record<string, number> = {
          right: rngh.Directions?.RIGHT ?? 1,
          left: rngh.Directions?.LEFT ?? 2,
          up: rngh.Directions?.UP ?? 4,
          down: rngh.Directions?.DOWN ?? 8,
        }
        gesture = gesture.direction(dirMap[flingCfg.direction] ?? 0)
      }
      if (flingCfg.numberOfPointers != null) gesture = gesture.numberOfPointers(flingCfg.numberOfPointers)
      break
    }
    case 'tap': {
      const tapCfg = config as TapGesture
      gesture = RNGesture.Tap()
      if (tapCfg.numberOfTaps != null) gesture = gesture.numberOfTaps(tapCfg.numberOfTaps)
      if (tapCfg.maxDuration != null) gesture = gesture.maxDuration(tapCfg.maxDuration)
      if (tapCfg.maxDelay != null) gesture = gesture.maxDelay(tapCfg.maxDelay)
      if (tapCfg.maxDistance != null) gesture = gesture.maxDistance(tapCfg.maxDistance)
      break
    }
    case 'longPress': {
      const lpCfg = config as LongPressGesture
      gesture = RNGesture.LongPress()
      if (lpCfg.minDuration != null) gesture = gesture.minDuration(lpCfg.minDuration)
      if (lpCfg.maxDistance != null) gesture = gesture.maxDist(lpCfg.maxDistance)
      break
    }
    default:
      return null
  }

  // Attach common callbacks
  if (config.enabled === false) gesture = gesture.enabled(false)
  if (config.onBegin) gesture = gesture.onBegin(config.onBegin)
  if (config.onStart) gesture = gesture.onStart(config.onStart)
  if (config.onUpdate) gesture = gesture.onUpdate(config.onUpdate)
  if (config.onEnd) gesture = gesture.onEnd(config.onEnd)
  if (config.onFinalize) gesture = gesture.onFinalize(config.onFinalize)
  if (config.hitSlop != null) gesture = gesture.hitSlop(config.hitSlop)

  return gesture
}

/**
 * Build a composed RNGH gesture from our ComposedGesture config.
 */
function buildRNGHComposed(composed: ComposedGesture): any {
  const rngh = getRNGH()
  if (!rngh) return null

  const RNGesture = rngh.Gesture
  const builtGestures = composed.gestures.map(buildRNGHGesture).filter(Boolean)
  if (builtGestures.length === 0) return null

  switch (composed.type) {
    case 'simultaneous':
      return RNGesture.Simultaneous(...builtGestures)
    case 'exclusive':
      return RNGesture.Exclusive(...builtGestures)
    case 'race':
      return RNGesture.Race(...builtGestures)
    default:
      return builtGestures[0]
  }
}

// ─── GestureDetector (with native + fallback) ────────────────────────────────

interface GestureDetectorProps {
  /** Single gesture, composed gesture, or a gesture builder with .build() */
  gesture: GestureInput
  /** The child view to attach gesture recognition to */
  children: ReactNode
}

/**
 * GestureDetector attaches native gesture recognizers to its child view.
 *
 * When react-native-gesture-handler is installed, it delegates to RNGH's
 * GestureDetector for native UIGestureRecognizer (iOS) / GestureDetectorCompat
 * (Android) backed recognition with worklet callbacks on the UI thread.
 *
 * When RNGH is not available, it falls back to React Native's PanResponder
 * system, which provides JS-thread gesture recognition for pan, tap, long
 * press, fling, pinch, and rotation gestures.
 *
 * @example
 * const pan = Gesture.Pan()
 *   .onUpdate((e) => { offset.value = e.translationX })
 *   .onEnd(() => { offset.value = withSpring(0) })
 *
 * <GestureDetector gesture={pan}>
 *   <Animated.View style={animStyle}>
 *     <Text>Drag me</Text>
 *   </Animated.View>
 * </GestureDetector>
 */
export function GestureDetector({ gesture, children }: GestureDetectorProps) {
  const rngh = getRNGH()
  const resolved = useMemo(() => resolveConfig(gesture), [gesture])

  // ── Native RNGH path ──────────────────────────────────────────────────────
  if (rngh) {
    const nativeGesture = useMemo(() => {
      if (isComposed(resolved)) {
        return buildRNGHComposed(resolved)
      }
      return buildRNGHGesture(resolved as GestureConfig)
    }, [resolved])

    if (!nativeGesture) {
      // Gesture type not supported — render children as-is
      return React.createElement(View, { collapsable: false }, children)
    }

    const NativeDetector = rngh.GestureDetector
    return React.createElement(NativeDetector, { gesture: nativeGesture }, children)
  }

  // ── PanResponder fallback path ────────────────────────────────────────────
  const panResponder = useMemo(
    () => buildPanResponderFromConfig(resolved),
    [resolved],
  )

  return React.createElement(
    View,
    {
      collapsable: false,
      ...panResponder.panHandlers,
    },
    children,
  )
}

// ─── Re-export Gesture namespace with RNGH-aware builders ────────────────────

// We re-export from gesture.ts which has the builder implementations.
// The builders produce our config format, and GestureDetector above
// translates that to RNGH native gestures or PanResponder fallback.
export { Gesture } from './gesture.js'
export type { ComposedGesture } from './gesture.js'
