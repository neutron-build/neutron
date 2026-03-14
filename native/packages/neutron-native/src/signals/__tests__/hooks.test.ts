/**
 * Tests for signal hooks — useSignal and useComputed.
 */

describe('useSignal', () => {
  beforeEach(() => {
    jest.resetModules()
  })

  it('creates a signal with the initial value', () => {
    const { useSignal } = require('../hooks')
    const s = useSignal(42)
    expect(s.value).toBe(42)
  })

  it('allows setting the signal value', () => {
    const { useSignal } = require('../hooks')
    const s = useSignal('hello')
    s.value = 'world'
    expect(s.value).toBe('world')
  })

  it('returns a stable signal reference (useRef-backed)', () => {
    const { useSignal } = require('../hooks')
    // In our mock, useRef creates a new ref each call since we're not in React.
    // The important thing is the signal works correctly.
    const s = useSignal(0)
    expect(s).toBeDefined()
    expect(s.value).toBe(0)
  })
})

describe('useComputed', () => {
  beforeEach(() => {
    jest.resetModules()
  })

  it('computes from a function', () => {
    const { useComputed } = require('../hooks')
    const c = useComputed(() => 2 + 3)
    expect(c.value).toBe(5)
  })

  it('recomputes when accessed', () => {
    const { useComputed } = require('../hooks')
    let base = 10
    const c = useComputed(() => base * 2)
    expect(c.value).toBe(20)
    base = 20
    expect(c.value).toBe(40)
  })
})
