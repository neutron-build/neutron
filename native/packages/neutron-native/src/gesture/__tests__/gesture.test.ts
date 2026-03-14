/**
 * Tests for Gesture builders and composed gestures.
 */

import { Gesture } from '../gesture'

describe('Gesture.Pan', () => {
  it('creates a pan gesture with correct type', () => {
    const pan = Gesture.Pan()
    const config = pan.build()
    expect(config.type).toBe('pan')
    expect(config.enabled).toBe(true)
  })

  it('supports fluent chaining', () => {
    const cb = jest.fn()
    const pan = Gesture.Pan()
      .minDistance(10)
      .minPointers(1)
      .maxPointers(2)
      .activeOffsetX([-20, 20])
      .activeOffsetY(50)
      .failOffsetX(100)
      .failOffsetY([-100, 100])
      .avgTouches(true)
      .onBegin(cb)
      .onStart(cb)
      .onUpdate(cb)
      .onEnd(cb)
      .onFinalize(cb)
      .enabled(true)

    const config = pan.build()
    expect(config.minDistance).toBe(10)
    expect(config.minPointers).toBe(1)
    expect(config.maxPointers).toBe(2)
    expect(config.activeOffsetX).toEqual([-20, 20])
    expect(config.activeOffsetY).toBe(50)
    expect(config.avgTouches).toBe(true)
    expect(config.onBegin).toBe(cb)
    expect(config.onStart).toBe(cb)
    expect(config.onUpdate).toBe(cb)
    expect(config.onEnd).toBe(cb)
    expect(config.onFinalize).toBe(cb)
  })

  it('hitSlop accepts number', () => {
    const config = Gesture.Pan().hitSlop(20).build()
    expect(config.hitSlop).toBe(20)
  })

  it('hitSlop accepts object', () => {
    const config = Gesture.Pan().hitSlop({ top: 10, bottom: 20 }).build()
    expect(config.hitSlop).toEqual({ top: 10, bottom: 20 })
  })

  it('toJSON returns the config for serialization', () => {
    const pan = Gesture.Pan().minDistance(5)
    expect(pan.toJSON()).toEqual(pan.build())
  })
})

describe('Gesture.Tap', () => {
  it('creates a tap gesture with correct type', () => {
    const config = Gesture.Tap().build()
    expect(config.type).toBe('tap')
  })

  it('supports numberOfTaps, maxDuration, maxDelay, maxDistance', () => {
    const config = Gesture.Tap()
      .numberOfTaps(2)
      .maxDuration(500)
      .maxDelay(200)
      .maxDistance(10)
      .build()
    expect(config.numberOfTaps).toBe(2)
    expect(config.maxDuration).toBe(500)
    expect(config.maxDelay).toBe(200)
    expect(config.maxDistance).toBe(10)
  })
})

describe('Gesture.LongPress', () => {
  it('creates a long press with correct type', () => {
    const config = Gesture.LongPress().build()
    expect(config.type).toBe('longPress')
  })

  it('supports minDuration and maxDistance', () => {
    const config = Gesture.LongPress()
      .minDuration(800)
      .maxDistance(15)
      .build()
    expect(config.minDuration).toBe(800)
    expect(config.maxDistance).toBe(15)
  })
})

describe('Gesture.Fling', () => {
  it('creates a fling with correct type', () => {
    const config = Gesture.Fling().build()
    expect(config.type).toBe('fling')
  })

  it('supports direction and numberOfPointers', () => {
    const config = Gesture.Fling()
      .direction('left')
      .numberOfPointers(1)
      .build()
    expect(config.direction).toBe('left')
    expect(config.numberOfPointers).toBe(1)
  })
})

describe('Gesture.Pinch', () => {
  it('creates a pinch with correct type', () => {
    const config = Gesture.Pinch().build()
    expect(config.type).toBe('pinch')
  })
})

describe('Gesture.Rotation', () => {
  it('creates a rotation with correct type', () => {
    const config = Gesture.Rotation().build()
    expect(config.type).toBe('rotation')
  })
})

describe('Gesture.simultaneousWith', () => {
  it('sets simultaneousWith on the gesture', () => {
    const other = Gesture.Tap().build()
    const config = Gesture.Pan().simultaneousWith(other).build()
    expect(config.simultaneousWith).toEqual([other])
  })
})

describe('Gesture.requireExternalFailure', () => {
  it('sets requireExternalFailure on the gesture', () => {
    const other = Gesture.Tap().build()
    const config = Gesture.Pan().requireExternalFailure(other).build()
    expect(config.requireExternalFailure).toEqual([other])
  })
})

describe('Composed Gestures', () => {
  it('Simultaneous composes gestures', () => {
    const pan = Gesture.Pan()
    const pinch = Gesture.Pinch()
    const composed = Gesture.Simultaneous(pan, pinch)
    expect(composed.type).toBe('simultaneous')
    expect(composed.gestures).toHaveLength(2)
    expect(composed.gestures[0].type).toBe('pan')
    expect(composed.gestures[1].type).toBe('pinch')
  })

  it('Exclusive composes gestures with priority', () => {
    const tap = Gesture.Tap()
    const longPress = Gesture.LongPress()
    const composed = Gesture.Exclusive(tap, longPress)
    expect(composed.type).toBe('exclusive')
    expect(composed.gestures).toHaveLength(2)
  })

  it('Race composes gestures with race semantics', () => {
    const pan = Gesture.Pan()
    const fling = Gesture.Fling()
    const composed = Gesture.Race(pan, fling)
    expect(composed.type).toBe('race')
    expect(composed.gestures).toHaveLength(2)
  })

  it('accepts raw config objects as well as builders', () => {
    const rawConfig = { type: 'tap' as const, enabled: true }
    const composed = Gesture.Simultaneous(rawConfig, Gesture.Pan())
    expect(composed.gestures).toHaveLength(2)
    expect(composed.gestures[0]).toEqual(rawConfig)
  })
})

describe('Gesture.enabled', () => {
  it('can disable a gesture', () => {
    const config = Gesture.Pan().enabled(false).build()
    expect(config.enabled).toBe(false)
  })
})
