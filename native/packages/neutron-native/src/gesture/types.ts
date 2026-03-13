/**
 * Gesture handler types.
 */

/** Gesture lifecycle state */
export type GestureState =
  | 'undetermined'
  | 'began'
  | 'active'
  | 'end'
  | 'cancelled'
  | 'failed'

/** Base gesture event */
export interface GestureEvent {
  state: GestureState
  /** Absolute X position on screen */
  absoluteX: number
  /** Absolute Y position on screen */
  absoluteY: number
  /** X relative to the gesture view */
  x: number
  /** Y relative to the gesture view */
  y: number
  /** Number of active pointers */
  numberOfPointers: number
}

/** Pan gesture event — includes translation and velocity */
export interface PanGestureEvent extends GestureEvent {
  /** Cumulative X translation since gesture began */
  translationX: number
  /** Cumulative Y translation since gesture began */
  translationY: number
  /** X velocity in px/s */
  velocityX: number
  /** Y velocity in px/s */
  velocityY: number
}

/** Pinch gesture event */
export interface PinchGestureEvent extends GestureEvent {
  /** Scale factor relative to initial pinch distance */
  scale: number
  /** Scale velocity */
  velocity: number
  /** Focal point X */
  focalX: number
  /** Focal point Y */
  focalY: number
}

/** Rotation gesture event */
export interface RotationGestureEvent extends GestureEvent {
  /** Rotation in radians since gesture began */
  rotation: number
  /** Angular velocity in rad/s */
  velocity: number
  /** Anchor point X */
  anchorX: number
  /** Anchor point Y */
  anchorY: number
}

/** Gesture callback type */
export type GestureCallback<E extends GestureEvent = GestureEvent> = (event: E) => void

/** Base gesture configuration */
export interface GestureConfig<E extends GestureEvent = GestureEvent> {
  type: string
  enabled: boolean
  onBegin?: GestureCallback<E>
  onUpdate?: GestureCallback<E>
  onEnd?: GestureCallback<E>
  onStart?: GestureCallback<E>
  onFinalize?: GestureCallback<E>
  simultaneousWith?: GestureConfig[]
  requireExternalFailure?: GestureConfig[]
  hitSlop?: number | { top?: number; bottom?: number; left?: number; right?: number }
}

/** Pan gesture — drag/swipe recognition */
export interface PanGesture extends GestureConfig<PanGestureEvent> {
  type: 'pan'
  minDistance?: number
  minPointers?: number
  maxPointers?: number
  activeOffsetX?: number | [number, number]
  activeOffsetY?: number | [number, number]
  failOffsetX?: number | [number, number]
  failOffsetY?: number | [number, number]
  avgTouches?: boolean
}

/** Pinch gesture — scale recognition */
export interface PinchGesture extends GestureConfig<PinchGestureEvent> {
  type: 'pinch'
}

/** Rotation gesture — two-finger rotation */
export interface RotationGesture extends GestureConfig<RotationGestureEvent> {
  type: 'rotation'
}

/** Fling gesture — fast swipe in a direction */
export interface FlingGesture extends GestureConfig<GestureEvent> {
  type: 'fling'
  direction?: 'left' | 'right' | 'up' | 'down'
  numberOfPointers?: number
}

/** Tap gesture — single or multi-tap */
export interface TapGesture extends GestureConfig<GestureEvent> {
  type: 'tap'
  numberOfTaps?: number
  maxDuration?: number
  maxDelay?: number
  maxDistance?: number
}

/** Long press gesture */
export interface LongPressGesture extends GestureConfig<GestureEvent> {
  type: 'longPress'
  minDuration?: number
  maxDistance?: number
}
