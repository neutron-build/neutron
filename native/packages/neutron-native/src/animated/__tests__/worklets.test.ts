/**
 * Tests for animation worklets — interpolate, Easing, animation descriptors, Extrapolation.
 */

describe('interpolate', () => {
  beforeEach(() => {
    jest.resetModules()
  })

  it('linearly interpolates between two ranges', () => {
    const { interpolate } = require('../worklets')
    expect(interpolate(50, [0, 100], [0, 1])).toBeCloseTo(0.5)
    expect(interpolate(0, [0, 100], [0, 1])).toBeCloseTo(0)
    expect(interpolate(100, [0, 100], [0, 1])).toBeCloseTo(1)
  })

  it('interpolates with multiple segments', () => {
    const { interpolate } = require('../worklets')
    expect(interpolate(50, [0, 50, 100], [0, 1, 0])).toBeCloseTo(1)
    expect(interpolate(75, [0, 50, 100], [0, 1, 0])).toBeCloseTo(0.5)
  })

  it('extends by default for out-of-range values', () => {
    const { interpolate } = require('../worklets')
    const result = interpolate(200, [0, 100], [0, 1])
    expect(result).toBeCloseTo(2)
  })

  it('clamps when extrapolation is "clamp"', () => {
    const { interpolate } = require('../worklets')
    expect(interpolate(200, [0, 100], [0, 1], 'clamp')).toBeCloseTo(1)
    expect(interpolate(-50, [0, 100], [0, 1], 'clamp')).toBeCloseTo(0)
  })

  it('returns input value when extrapolation is "identity"', () => {
    const { interpolate } = require('../worklets')
    expect(interpolate(200, [0, 100], [0, 1], 'identity')).toBe(200)
    expect(interpolate(-50, [0, 100], [0, 1], 'identity')).toBe(-50)
  })

  it('supports separate left/right extrapolation', () => {
    const { interpolate } = require('../worklets')
    const result = interpolate(-10, [0, 100], [0, 1], {
      extrapolateLeft: 'clamp',
      extrapolateRight: 'extend',
    })
    expect(result).toBeCloseTo(0) // clamped left

    const result2 = interpolate(200, [0, 100], [0, 1], {
      extrapolateLeft: 'clamp',
      extrapolateRight: 'clamp',
    })
    expect(result2).toBeCloseTo(1) // clamped right
  })

  it('returns first output when input/output range has < 2 entries', () => {
    const { interpolate } = require('../worklets')
    expect(interpolate(5, [0], [10])).toBe(10)
  })

  it('returns 0 when output range is empty', () => {
    const { interpolate } = require('../worklets')
    expect(interpolate(5, [], [])).toBe(0)
  })
})

describe('Easing', () => {
  beforeEach(() => {
    jest.resetModules()
  })

  it('linear returns t unchanged', () => {
    const { Easing } = require('../worklets')
    expect(Easing.linear(0)).toBe(0)
    expect(Easing.linear(0.5)).toBe(0.5)
    expect(Easing.linear(1)).toBe(1)
  })

  it('quad produces t^2', () => {
    const { Easing } = require('../worklets')
    expect(Easing.quad(0.5)).toBeCloseTo(0.25)
    expect(Easing.quad(1)).toBe(1)
  })

  it('cubic produces t^3', () => {
    const { Easing } = require('../worklets')
    expect(Easing.cubic(0.5)).toBeCloseTo(0.125)
  })

  it('sin produces sinusoidal output', () => {
    const { Easing } = require('../worklets')
    expect(Easing.sin(0)).toBeCloseTo(0)
    expect(Easing.sin(1)).toBeCloseTo(1)
  })

  it('circle produces circular output', () => {
    const { Easing } = require('../worklets')
    expect(Easing.circle(0)).toBeCloseTo(0)
    expect(Easing.circle(1)).toBeCloseTo(1) // 1 - sqrt(1-1) = 1 - 0 = 1
  })

  it('exp produces exponential output', () => {
    const { Easing } = require('../worklets')
    expect(Easing.exp(0)).toBe(0)
    expect(Easing.exp(1)).toBeCloseTo(1)
  })

  it('bounce produces bounded output in [0,1]', () => {
    const { Easing } = require('../worklets')
    for (const t of [0, 0.1, 0.25, 0.5, 0.75, 0.9, 1]) {
      const v = Easing.bounce(t)
      expect(v).toBeGreaterThanOrEqual(0)
      expect(v).toBeLessThanOrEqual(1.001)
    }
  })

  it('back factory returns an easing function with overshoot', () => {
    const { Easing } = require('../worklets')
    const backEase = Easing.back(1.70158)
    expect(typeof backEase).toBe('function')
    expect(backEase(0)).toBeCloseTo(0)
    // At t=0.5 the back ease goes negative (overshoot)
    expect(backEase(0.5)).toBeLessThan(0.5)
    expect(backEase(1)).toBeCloseTo(1)
  })

  it('Easing.in returns the same function', () => {
    const { Easing } = require('../worklets')
    const fn = (t: number) => t * t
    expect(Easing.in(fn)).toBe(fn)
  })

  it('Easing.out reverses the easing', () => {
    const { Easing } = require('../worklets')
    const fn = (t: number) => t * t
    const outFn = Easing.out(fn)
    expect(outFn(0)).toBeCloseTo(0)
    expect(outFn(1)).toBeCloseTo(1)
    expect(outFn(0.5)).toBeCloseTo(0.75) // 1 - (0.5)^2 = 0.75
  })

  it('Easing.inOut mirrors at midpoint', () => {
    const { Easing } = require('../worklets')
    const fn = (t: number) => t * t
    const inOutFn = Easing.inOut(fn)
    expect(inOutFn(0)).toBeCloseTo(0)
    expect(inOutFn(0.5)).toBeCloseTo(0.5)
    expect(inOutFn(1)).toBeCloseTo(1)
  })

  it('bezier returns a function', () => {
    const { Easing } = require('../worklets')
    const fn = Easing.bezier(0.25, 0.1, 0.25, 1.0)
    expect(typeof fn).toBe('function')
    expect(fn(0)).toBe(0)
    expect(fn(1)).toBe(1)
    const mid = fn(0.5)
    expect(mid).toBeGreaterThan(0)
    expect(mid).toBeLessThan(1)
  })
})

describe('Extrapolation constants', () => {
  it('exports correct values', () => {
    const { Extrapolation } = require('../worklets')
    expect(Extrapolation.EXTEND).toBe('extend')
    expect(Extrapolation.CLAMP).toBe('clamp')
    expect(Extrapolation.IDENTITY).toBe('identity')
  })
})

describe('withTiming (fallback)', () => {
  beforeEach(() => {
    jest.resetModules()
  })

  it('returns an animation descriptor', () => {
    const { withTiming } = require('../worklets')
    const result = withTiming(100, { duration: 300 })
    // The result is typed as number but is actually an animation descriptor
    expect((result as any).__isAnimation).toBe(true)
    expect((result as any).type).toBe('timing')
    expect((result as any).toValue).toBe(100)
  })
})

describe('withSpring (fallback)', () => {
  it('returns a spring animation descriptor', () => {
    const { withSpring } = require('../worklets')
    const result = withSpring(50, { damping: 15 })
    expect((result as any).__isAnimation).toBe(true)
    expect((result as any).type).toBe('spring')
    expect((result as any).toValue).toBe(50)
  })
})

describe('withDecay (fallback)', () => {
  it('returns a decay animation descriptor', () => {
    const { withDecay } = require('../worklets')
    const result = withDecay({ velocity: 500 })
    expect((result as any).__isAnimation).toBe(true)
    expect((result as any).type).toBe('decay')
  })
})

describe('withSequence (fallback)', () => {
  it('wraps multiple animations into a sequence descriptor', () => {
    const { withTiming, withSequence } = require('../worklets')
    const a1 = withTiming(100)
    const a2 = withTiming(0)
    const seq = withSequence(a1, a2)
    expect((seq as any).__isAnimation).toBe(true)
    expect((seq as any).type).toBe('sequence')
    expect((seq as any).animations).toHaveLength(2)
  })

  it('wraps plain numbers as 0-duration timing', () => {
    const { withSequence } = require('../worklets')
    const seq = withSequence(100, 200)
    expect((seq as any).__isAnimation).toBe(true)
    expect((seq as any).animations[0].type).toBe('timing')
    expect((seq as any).animations[0].toValue).toBe(100)
  })
})

describe('withDelay (fallback)', () => {
  it('wraps an animation with a delay', () => {
    const { withTiming, withDelay } = require('../worklets')
    const delayed = withDelay(500, withTiming(1))
    expect((delayed as any).__isAnimation).toBe(true)
    expect((delayed as any).type).toBe('delay')
    expect((delayed as any).delayMs).toBe(500)
  })
})

describe('withRepeat (fallback)', () => {
  it('creates a sequence for finite reps', () => {
    const { withTiming, withRepeat } = require('../worklets')
    const repeated = withRepeat(withTiming(100), 3)
    expect((repeated as any).__isAnimation).toBe(true)
    expect((repeated as any).type).toBe('sequence')
    expect((repeated as any).animations).toHaveLength(3)
  })

  it('adds reverse steps when reverse=true', () => {
    const { withTiming, withRepeat } = require('../worklets')
    const repeated = withRepeat(withTiming(100), 3, true)
    // 3 forward + 2 reverse = 5 steps
    expect((repeated as any).animations).toHaveLength(5)
  })

  it('returns the animation directly for infinite reps', () => {
    const { withTiming, withRepeat } = require('../worklets')
    const anim = withTiming(100)
    const repeated = withRepeat(anim, -1)
    // For infinite, just returns the animation as best-effort
    expect(repeated).toBe(anim)
  })
})

describe('runOnJS (fallback)', () => {
  it('returns a wrapper that calls the function directly', () => {
    const { runOnJS } = require('../worklets')
    const fn = jest.fn()
    const wrapped = runOnJS(fn)
    wrapped(42)
    expect(fn).toHaveBeenCalledWith(42)
  })
})

describe('runOnUI (fallback)', () => {
  it('returns a wrapper that calls the function directly', () => {
    const { runOnUI } = require('../worklets')
    const fn = jest.fn()
    const wrapped = runOnUI(fn)
    wrapped('hello')
    expect(fn).toHaveBeenCalledWith('hello')
  })
})

describe('hasReanimated', () => {
  it('returns false when react-native-reanimated is not installed', () => {
    const { hasReanimated } = require('../worklets')
    expect(hasReanimated()).toBe(false)
  })
})
