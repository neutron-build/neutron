/**
 * Gesture builders — fluent API for constructing gesture configurations.
 *
 * @example
 * const pan = Gesture.Pan()
 *   .onUpdate((e) => { offset.value = e.translationX })
 *   .onEnd((e) => { offset.value = withSpring(0) })
 *   .minDistance(10)
 *
 * const composed = Gesture.Simultaneous(pan, pinch)
 */

import type {
  PanGesture, PinchGesture, RotationGesture, FlingGesture,
  TapGesture, LongPressGesture, GestureConfig, GestureEvent,
  PanGestureEvent, PinchGestureEvent, RotationGestureEvent,
  GestureCallback,
} from './types.js'

// ─── Builder base ────────────────────────────────────────────────────────────

class GestureBuilder<G extends GestureConfig<E>, E extends GestureEvent = GestureEvent> {
  protected config: G

  constructor(config: G) {
    this.config = config
  }

  /** Enable or disable the gesture */
  enabled(value: boolean): this {
    this.config.enabled = value
    return this
  }

  /** Called when the gesture is first recognized */
  onBegin(cb: GestureCallback<E>): this {
    this.config.onBegin = cb
    return this
  }

  /** Called when the gesture becomes active */
  onStart(cb: GestureCallback<E>): this {
    this.config.onStart = cb
    return this
  }

  /** Called on each frame while the gesture is active */
  onUpdate(cb: GestureCallback<E>): this {
    this.config.onUpdate = cb
    return this
  }

  /** Called when the gesture ends */
  onEnd(cb: GestureCallback<E>): this {
    this.config.onEnd = cb
    return this
  }

  /** Called when the gesture finalizes (end or cancel) */
  onFinalize(cb: GestureCallback<E>): this {
    this.config.onFinalize = cb
    return this
  }

  /** Allow this gesture to run simultaneously with another */
  simultaneousWith(...gestures: GestureConfig[]): this {
    this.config.simultaneousWith = gestures
    return this
  }

  /** Require another gesture to fail before this one activates */
  requireExternalFailure(...gestures: GestureConfig[]): this {
    this.config.requireExternalFailure = gestures
    return this
  }

  /** Set hit slop for larger touch targets */
  hitSlop(value: number | { top?: number; bottom?: number; left?: number; right?: number }): this {
    this.config.hitSlop = value
    return this
  }

  /** Get the built configuration */
  build(): G {
    return this.config
  }

  /** Allow using the builder directly as a config (auto-unwrap) */
  toJSON(): G {
    return this.config
  }
}

// ─── Pan ─────────────────────────────────────────────────────────────────────

class PanBuilder extends GestureBuilder<PanGesture, PanGestureEvent> {
  minDistance(d: number): this { this.config.minDistance = d; return this }
  minPointers(n: number): this { this.config.minPointers = n; return this }
  maxPointers(n: number): this { this.config.maxPointers = n; return this }
  activeOffsetX(v: number | [number, number]): this { this.config.activeOffsetX = v; return this }
  activeOffsetY(v: number | [number, number]): this { this.config.activeOffsetY = v; return this }
  failOffsetX(v: number | [number, number]): this { this.config.failOffsetX = v; return this }
  failOffsetY(v: number | [number, number]): this { this.config.failOffsetY = v; return this }
  avgTouches(v: boolean): this { this.config.avgTouches = v; return this }
}

// ─── Pinch ───────────────────────────────────────────────────────────────────

class PinchBuilder extends GestureBuilder<PinchGesture, PinchGestureEvent> {}

// ─── Rotation ────────────────────────────────────────────────────────────────

class RotationBuilder extends GestureBuilder<RotationGesture, RotationGestureEvent> {}

// ─── Fling ───────────────────────────────────────────────────────────────────

class FlingBuilder extends GestureBuilder<FlingGesture> {
  direction(d: 'left' | 'right' | 'up' | 'down'): this { this.config.direction = d; return this }
  numberOfPointers(n: number): this { this.config.numberOfPointers = n; return this }
}

// ─── Tap ─────────────────────────────────────────────────────────────────────

class TapBuilder extends GestureBuilder<TapGesture> {
  numberOfTaps(n: number): this { this.config.numberOfTaps = n; return this }
  maxDuration(ms: number): this { this.config.maxDuration = ms; return this }
  maxDelay(ms: number): this { this.config.maxDelay = ms; return this }
  maxDistance(d: number): this { this.config.maxDistance = d; return this }
}

// ─── Long Press ──────────────────────────────────────────────────────────────

class LongPressBuilder extends GestureBuilder<LongPressGesture> {
  minDuration(ms: number): this { this.config.minDuration = ms; return this }
  maxDistance(d: number): this { this.config.maxDistance = d; return this }
}

// ─── Composed gestures ───────────────────────────────────────────────────────

export interface ComposedGesture {
  type: 'simultaneous' | 'exclusive' | 'race'
  gestures: GestureConfig[]
}

// ─── Gesture namespace ───────────────────────────────────────────────────────

export const Gesture = {
  /** Create a pan (drag) gesture recognizer */
  Pan: () => new PanBuilder({ type: 'pan', enabled: true }),

  /** Create a pinch (scale) gesture recognizer */
  Pinch: () => new PinchBuilder({ type: 'pinch', enabled: true }),

  /** Create a rotation gesture recognizer */
  Rotation: () => new RotationBuilder({ type: 'rotation', enabled: true }),

  /** Create a fling (swipe) gesture recognizer */
  Fling: () => new FlingBuilder({ type: 'fling', enabled: true }),

  /** Create a tap gesture recognizer */
  Tap: () => new TapBuilder({ type: 'tap', enabled: true }),

  /** Create a long press gesture recognizer */
  LongPress: () => new LongPressBuilder({ type: 'longPress', enabled: true }),

  /** Allow multiple gestures to be recognized simultaneously */
  Simultaneous: (...gestures: (GestureConfig | GestureBuilder<GestureConfig>)[]): ComposedGesture => ({
    type: 'simultaneous',
    gestures: gestures.map(g => g instanceof GestureBuilder ? g.build() : g),
  }),

  /** Only the first matching gesture is recognized (priority order) */
  Exclusive: (...gestures: (GestureConfig | GestureBuilder<GestureConfig>)[]): ComposedGesture => ({
    type: 'exclusive',
    gestures: gestures.map(g => g instanceof GestureBuilder ? g.build() : g),
  }),

  /** First gesture to activate wins, others are cancelled */
  Race: (...gestures: (GestureConfig | GestureBuilder<GestureConfig>)[]): ComposedGesture => ({
    type: 'race',
    gestures: gestures.map(g => g instanceof GestureBuilder ? g.build() : g),
  }),
} as const
