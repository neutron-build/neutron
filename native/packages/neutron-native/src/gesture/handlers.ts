/**
 * Gesture handlers — React Native Gesture Handler compatible API with PanResponder fallback.
 *
 * When react-native-gesture-handler is installed, GestureDetector delegates to it
 * for native gesture recognition (UIGestureRecognizer on iOS, GestureDetectorCompat
 * on Android). When it is not available, a PanResponder-based fallback provides
 * equivalent (though less performant) gesture recognition on the JS thread.
 *
 * @example
 * import { GestureDetector, Gesture } from '@neutron-build/native/gesture'
 * import { useSharedValue, withSpring } from '@neutron-build/native/animated'
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

import React, { type ReactNode, useEffect, useMemo } from 'react'
import { View, PanResponder, type GestureResponderEvent, type PanResponderGestureState } from 'react-native'
import type {
  GestureConfig, GestureEvent, PanGestureEvent, PinchGestureEvent,
  RotationGestureEvent, GestureState,
  PanGesture, PinchGesture, RotationGesture, FlingGesture,
  TapGesture, LongPressGesture,
} from './types.js'
import type { AnyGestureConfig, ComposedGesture } from './gesture.js'

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
 * DELIBERATELY LIMITED ADAPTER (NF-NR-06) — no native/worklet parity:
 * - One continuous gesture owns a touch sequence (first-activation-wins
 *   arbitration); `simultaneous` compositions that overlap two CONTINUOUS
 *   gestures throw instead of silently dropping one.
 * - Pinch re-baselines at pointer-count transitions and ACCUMULATES scale
 *   across finger lifts/replacements; end events RETAIN the last values.
 * - Pan activation honors minDistance/activeOffsetX/Y and fails on
 *   failOffsetX/Y before activation; fling honors its configured direction.
 * - A detector whose every gesture is disabled never claims a touch.
 * - Long-press timers are cleared on termination; `cleanup()` must be called
 *   from the owning component's effect teardown (GestureDetector does).
 */
function buildPanResponderFromConfig(config: GestureConfig | ComposedGesture) {
  const gestures: AnyGestureConfig[] = isComposed(config)
    ? config.gestures
    : [config]

  const panGesture = gestures.find((g) => g.type === 'pan') as PanGesture | undefined
  const tapGesture = gestures.find((g) => g.type === 'tap') as TapGesture | undefined
  const longPressGesture = gestures.find((g) => g.type === 'longPress') as LongPressGesture | undefined
  const flingGesture = gestures.find((g) => g.type === 'fling') as FlingGesture | undefined
  const pinchGesture = gestures.find((g) => g.type === 'pinch') as PinchGesture | undefined
  const rotationGesture = gestures.find((g) => g.type === 'rotation') as RotationGesture | undefined

  // Explicit unsupported case: simultaneous recognition of two continuous
  // gestures is not implementable on this adapter — refuse loudly.
  if (isComposed(config) && config.type === 'simultaneous') {
    const continuous = gestures.filter(g => g.type === 'pan' || g.type === 'pinch' || g.type === 'rotation')
    if (continuous.length >= 2) {
      throw new Error(
        'Gesture.Simultaneous over two continuous gestures (pan/pinch/rotation) requires react-native-gesture-handler; ' +
        'the PanResponder fallback arbitrates exactly one continuous gesture per touch sequence.',
      )
    }
  }

  const enabledGestures = gestures.filter(g => g.enabled !== false)
  const anyEnabled = () => enabledGestures.length > 0

  // ── per-touch-sequence state ──
  let touchStartTime = 0
  let activatedGestureType: string | null = null
  let longPressTimer: ReturnType<typeof setTimeout> | null = null
  let tapCount = 0
  let lastTapTime = 0

  // Pinch/rotation: baseline re-captured at every pointer-count transition;
  // accumulated values survive finger lifts and are RETAINED on end.
  let baselinePinchDistance = 0
  let baselineRotationAngle = 0
  let lastPointerCount = 0
  let accumulatedScale = 1
  let accumulatedRotation = 0
  let pinchStarted = false
  let rotationStarted = false

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

  /** Re-baseline pinch/rotation when the pointer count changes: a finger
   * arriving AFTER grant must not report a jump — the new pair becomes the
   * baseline and accumulation continues from the retained values. */
  function trackPointers(evt: GestureResponderEvent, gs: PanResponderGestureState): void {
    const touches = (evt.nativeEvent as any).touches
    const count = Math.min(gs.numberActiveTouches, touches?.length ?? gs.numberActiveTouches)
    if (count !== lastPointerCount) {
      lastPointerCount = count
      if (touches && touches.length >= 2) {
        baselinePinchDistance = getDistance(touches)
        baselineRotationAngle = getAngle(touches)
      }
    }
  }

  function panActivationEvent(_evt: GestureResponderEvent, gs: PanResponderGestureState): boolean {
    // Pan activation thresholds (NF-NR-06): minDistance OR an activeOffset
    // axis bound; failOffset bounds refuse activation outright. Offsets may
    // be a signed pair — a negative bound measures the negative direction.
    const bound = (value: number | [number, number] | undefined, delta: number): boolean => {
      if (value == null) return false
      if (typeof value === 'number') return Math.abs(delta) >= Math.abs(value)
      return value.some(b => b > 0 ? delta >= b : delta <= b)
    }
    const failed = (value: number | [number, number] | undefined, delta: number): boolean => {
      if (value == null) return false
      if (typeof value === 'number') return Math.abs(delta) > Math.abs(value)
      return value.some(b => b > 0 ? delta > b : delta < b)
    }
    if (failed(panGesture!.failOffsetX, gs.dx)) return false
    if (failed(panGesture!.failOffsetY, gs.dy)) return false
    const minDist = panGesture!.minDistance ?? 10
    const dist = Math.sqrt(gs.dx * gs.dx + gs.dy * gs.dy)
    if (dist >= minDist) return true
    if (bound(panGesture!.activeOffsetX, gs.dx)) return true
    if (bound(panGesture!.activeOffsetY, gs.dy)) return true
    return false
  }

  function flingDirectionMatches(gesture: FlingGesture, gs: PanResponderGestureState): boolean {
    const dir = gesture.direction
    if (!dir) return true
    switch (dir) {
      case 'right': return gs.vx > 0
      case 'left': return gs.vx < 0
      case 'up': return gs.vy < 0
      case 'down': return gs.vy > 0
      default: return true
    }
  }

  function resetSequence(): void {
    activatedGestureType = null
    pinchStarted = false
    rotationStarted = false
    lastPointerCount = 0
    // accumulatedScale/accumulatedRotation are RETAINED across sequences by
    // design: they represent the gesture's last known values (NF-NR-06).
  }

  const responder = PanResponder.create({
    // A detector whose every gesture is disabled must not claim any touch.
    onStartShouldSetPanResponder: () => anyEnabled(),
    onMoveShouldSetPanResponder: (_evt, gs) => {
      if (!anyEnabled()) return false
      if (panGesture || pinchGesture || rotationGesture) {
        const minDist = panGesture?.minDistance ?? 10
        return Math.abs(gs.dx) > minDist || Math.abs(gs.dy) > minDist || gs.numberActiveTouches >= 2
      }
      return false
    },
    onPanResponderGrant: (evt, gs) => {
      touchStartTime = Date.now()
      activatedGestureType = null
      pinchStarted = false
      rotationStarted = false
      lastPointerCount = 0
      trackPointers(evt, gs)

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

      const base = makeBaseEvent(evt, gs)
      base.state = 'began'
      for (const g of enabledGestures) {
        g.onBegin?.(base as any)
      }
    },
    onPanResponderMove: (evt, gs) => {
      // Movement cancels a pending (not yet fired) long press.
      if (activatedGestureType !== 'longPress') clearLongPress()

      trackPointers(evt, gs)
      const touches = (evt.nativeEvent as any).touches
      const numTouches = gs.numberActiveTouches

      // Pinch — baselines re-captured at pointer transitions, values accumulate.
      if (numTouches >= 2 && pinchGesture && pinchGesture.enabled !== false && touches?.length >= 2) {
        if (activatedGestureType === null || activatedGestureType === 'pinch') {
          activatedGestureType = 'pinch'
          const currentDist = getDistance(touches)
          if (baselinePinchDistance > 0) {
            accumulatedScale *= currentDist / baselinePinchDistance
          }
          baselinePinchDistance = currentDist

          const pinchEvt: PinchGestureEvent = {
            ...makeBaseEvent(evt, gs),
            scale: accumulatedScale,
            velocity: 0,
            focalX: (touches[0].pageX + touches[1].pageX) / 2,
            focalY: (touches[0].pageY + touches[1].pageY) / 2,
          }
          if (!pinchStarted) {
            pinchStarted = true  // exactly one start per sequence
            pinchGesture.onStart?.(pinchEvt)
          }
          pinchGesture.onUpdate?.(pinchEvt)
          return
        }
      }

      // Rotation — same transition/accumulation rules as pinch.
      if (numTouches >= 2 && rotationGesture && rotationGesture.enabled !== false && touches?.length >= 2) {
        if (activatedGestureType === null || activatedGestureType === 'rotation') {
          activatedGestureType = 'rotation'
          const currentAngle = getAngle(touches)
          accumulatedRotation += currentAngle - baselineRotationAngle
          baselineRotationAngle = currentAngle

          const rotEvt: RotationGestureEvent = {
            ...makeBaseEvent(evt, gs),
            rotation: accumulatedRotation,
            velocity: 0,
            anchorX: (touches[0].pageX + touches[1].pageX) / 2,
            anchorY: (touches[0].pageY + touches[1].pageY) / 2,
          }
          if (!rotationStarted) {
            rotationStarted = true
            rotationGesture.onStart?.(rotEvt)
          }
          rotationGesture.onUpdate?.(rotEvt)
          return
        }
      }

      // Pan — config-enforced activation, started exactly once.
      if (panGesture && panGesture.enabled !== false && (panGesture.maxPointers == null || numTouches <= panGesture.maxPointers)) {
        if (activatedGestureType === null && panActivationEvent(evt, gs)) {
          activatedGestureType = 'pan'
          const startEvt = makePanEvent(evt, gs)
          startEvt.state = 'active'
          panGesture.onStart?.(startEvt)
          panGesture.onUpdate?.(makePanEvent(evt, gs))
          return
        }
        if (activatedGestureType === 'pan') {
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

      // Fling — velocity threshold AND the configured direction.
      if (flingGesture && flingGesture.enabled !== false && activatedGestureType === null) {
        const speed = Math.sqrt(gs.vx * gs.vx + gs.vy * gs.vy)
        if (speed > 0.5 && dist > 50 && flingDirectionMatches(flingGesture, gs)) {
          activatedGestureType = 'fling'
          flingGesture.onStart?.(base)
          flingGesture.onEnd?.(base)
          flingGesture.onFinalize?.(base)
          resetSequence()
          return
        }
      }

      // Tap
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
            resetSequence()
            return
          }
        }
      }

      // Terminal states dispatch ONLY to the owning gesture, and pinch/
      // rotation end events RETAIN the accumulated values.
      if (activatedGestureType === 'longPress' && longPressGesture) {
        longPressGesture.onEnd?.(base)
        longPressGesture.onFinalize?.(base)
        resetSequence()
        return
      }

      if (activatedGestureType === 'pinch' && pinchGesture) {
        const pinchEnd: PinchGestureEvent = {
          ...base,
          scale: accumulatedScale,
          velocity: 0,
          focalX: base.absoluteX,
          focalY: base.absoluteY,
        }
        pinchGesture.onEnd?.(pinchEnd)
        pinchGesture.onFinalize?.(pinchEnd)
        resetSequence()
        return
      }

      if (activatedGestureType === 'rotation' && rotationGesture) {
        const rotEnd: RotationGestureEvent = {
          ...base,
          rotation: accumulatedRotation,
          velocity: 0,
          anchorX: base.absoluteX,
          anchorY: base.absoluteY,
        }
        rotationGesture.onEnd?.(rotEnd)
        rotationGesture.onFinalize?.(rotEnd)
        resetSequence()
        return
      }

      if (activatedGestureType === 'pan' && panGesture) {
        const panEnd = makePanEvent(evt, gs)
        panEnd.state = 'end'
        panGesture.onEnd?.(panEnd)
        panGesture.onFinalize?.(panEnd)
        resetSequence()
        return
      }

      for (const g of enabledGestures) {
        g.onFinalize?.(base as any)
      }
      resetSequence()
    },
    onPanResponderTerminate: (evt, gs) => {
      clearLongPress()
      const base = makeBaseEvent(evt, gs)
      base.state = 'cancelled'
      for (const g of enabledGestures) {
        g.onFinalize?.(base as any)
      }
      resetSequence()
    },
  })

  return {
    panHandlers: responder.panHandlers,
    /** Lifecycle cleanup for the owning component (NF-NR-06): clears any
     * pending long-press timer when the detector unmounts or is replaced. */
    cleanup: clearLongPress,
  }
}

// ─── RNGH Native Gesture Builder ─────────────────────────────────────────────

/**
 * Build a react-native-gesture-handler Gesture object from our config.
 * Maps our gesture config format to RNGH's fluent API.
 */
function buildRNGHGesture(config: AnyGestureConfig): any {
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
      // RNGH 2's builder method is `maxDistance` — `maxDist` does not exist
      // and calling it throws at build time (NF-NR-05).
      if (lpCfg.maxDistance != null) gesture = gesture.maxDistance(lpCfg.maxDistance)
      break
    }
    default:
      return null
  }

  // Interaction relations (NF-NR-05): stored by the builders, and now
  // actually forwarded through the provider's own composition graph.
  // Entries may arrive as builders or raw configs — resolve both.
  const resolveEntry = (entry: unknown): AnyGestureConfig | null => {
    const resolved = entry && typeof entry === 'object' && 'build' in entry && typeof (entry as { build?: unknown }).build === 'function'
      ? (entry as { build(): AnyGestureConfig }).build()
      : entry as AnyGestureConfig
    return resolved ?? null
  }
  if (config.simultaneousWith && config.simultaneousWith.length > 0) {
    const others = config.simultaneousWith
      .map(resolveEntry)
      .filter((entry): entry is AnyGestureConfig => entry !== null)
      .map(buildRNGHGesture)
      .filter(Boolean)
    if (others.length > 0) gesture = RNGesture.Simultaneous(gesture, ...others)
  }
  if (config.requireExternalFailure && config.requireExternalFailure.length > 0) {
    for (const external of config.requireExternalFailure) {
      const resolved = resolveEntry(external)
      if (!resolved) continue
      const required = buildRNGHGesture(resolved)
      if (required) gesture = gesture.requireToFail(required)
    }
  }

  // Attach common callbacks. Neutron's public contract is STRING gesture
  // states; RNGH events carry numeric State constants — normalize on the
  // boundary so the event shape is provider-independent (NF-NR-05).
  if (config.enabled === false) gesture = gesture.enabled(false)
  if (config.onBegin) gesture = gesture.onBegin((event: unknown) => config.onBegin!(normalizeRNGHEvent(event, rngh, 'began')))
  if (config.onStart) gesture = gesture.onStart((event: unknown) => config.onStart!(normalizeRNGHEvent(event, rngh, 'active')))
  if (config.onUpdate) gesture = gesture.onUpdate((event: unknown) => config.onUpdate!(normalizeRNGHEvent(event, rngh, 'active')))
  if (config.onEnd) gesture = gesture.onEnd((event: unknown, success: boolean) => config.onEnd!(normalizeRNGHEvent(event, rngh, success ? 'end' : 'cancelled')))
  if (config.onFinalize) gesture = gesture.onFinalize((event: unknown, success: boolean) => config.onFinalize!(normalizeRNGHEvent(event, rngh, success ? 'end' : 'cancelled')))
  if (config.hitSlop != null) gesture = gesture.hitSlop(config.hitSlop)

  return gesture
}

/**
 * Map an RNGH numeric State constant onto Neutron's string GestureState.
 * The mapping table covers the whole RNGH State enum; unknown values fall
 * back to the lifecycle phase implied by the callback that received them.
 */
const RNGH_STATE_TO_STRING: Record<number, GestureState> = {}
function normalizeRNGHEvent(event: any, rngh: any, phaseFallback: GestureState): any {
  const State = rngh?.State
  if (RNGH_STATE_TO_STRING[0] === undefined && State) {
    RNGH_STATE_TO_STRING[State.UNDETERMINED] = 'undetermined'
    RNGH_STATE_TO_STRING[State.BEGAN] = 'began'
    RNGH_STATE_TO_STRING[State.ACTIVE] = 'active'
    RNGH_STATE_TO_STRING[State.END] = 'end'
    RNGH_STATE_TO_STRING[State.CANCELLED] = 'cancelled'
    RNGH_STATE_TO_STRING[State.FAILED] = 'failed'
  }
  const native = event?.nativeEvent ?? event ?? {}
  const state: GestureState = RNGH_STATE_TO_STRING[native.state] ?? phaseFallback
  const normalized: GestureEvent = {
    state,
    absoluteX: native.absoluteX ?? 0,
    absoluteY: native.absoluteY ?? 0,
    x: native.x ?? 0,
    y: native.y ?? 0,
    numberOfPointers: native.numberOfPointers ?? 1,
  }
  // Enrich with pan/pinch/rotation fields when the provider supplied them.
  if (native.translationX != null || native.translationY != null) {
    return {
      ...normalized,
      translationX: native.translationX ?? 0,
      translationY: native.translationY ?? 0,
      velocityX: native.velocityX ?? 0,
      velocityY: native.velocityY ?? 0,
    } as PanGestureEvent
  }
  if (native.scale != null || native.rotation != null) {
    return {
      ...normalized,
      scale: native.scale ?? 1,
      rotation: native.rotation ?? 0,
      velocity: native.velocity ?? 0,
    } as unknown as GestureEvent
  }
  return normalized
}

/**
 * Build a composed RNGH gesture from our ComposedGesture config.
 */
function buildRNGHComposed(composed: ComposedGesture): any {
  const rngh = getRNGH()
  if (!rngh) return null

  const RNGesture = rngh.Gesture
  const builtGestures = composed.gestures
    .filter((g): g is AnyGestureConfig => g !== null && g !== undefined)
    .map(buildRNGHGesture)
    .filter(Boolean)
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

  // Long-press timers are tied to React lifecycle (NF-NR-06): unmount or
  // gesture replacement clears any pending timer.
  useEffect(() => {
    return () => { panResponder.cleanup() }
  }, [panResponder])

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
