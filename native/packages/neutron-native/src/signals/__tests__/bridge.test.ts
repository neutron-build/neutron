/**
 * Tests for signals bridge — persistedSignal and createEventBus.
 */

describe('createEventBus', () => {
  it('emits to all subscribers', () => {
    const { createEventBus } = require('../bridge')
    const bus = createEventBus()
    const received: string[] = []
    bus.on((msg: string) => received.push(msg))
    bus.on((msg: string) => received.push(`copy:${msg}`))
    bus.emit('hello')
    expect(received).toEqual(['hello', 'copy:hello'])
  })

  it('returns unsubscribe function that removes the listener', () => {
    const { createEventBus } = require('../bridge')
    const bus = createEventBus()
    const received: number[] = []
    const unsub = bus.on((n: number) => received.push(n))
    bus.emit(1)
    unsub()
    bus.emit(2)
    expect(received).toEqual([1])
  })

  it('clear() removes all listeners', () => {
    const { createEventBus } = require('../bridge')
    const bus = createEventBus()
    const received: string[] = []
    bus.on((msg: string) => received.push(msg))
    bus.clear()
    bus.emit('nope')
    expect(received).toEqual([])
  })

  it('catches errors from one listener without breaking others', () => {
    const { createEventBus } = require('../bridge')
    const bus = createEventBus()
    const received: string[] = []
    bus.on(() => { throw new Error('bad listener') })
    bus.on((msg: string) => received.push(msg))
    bus.emit('still works')
    expect(received).toEqual(['still works'])
  })

  it('works with void emit', () => {
    const { createEventBus } = require('../bridge')
    const bus = createEventBus()
    let called = false
    bus.on(() => { called = true })
    bus.emit(undefined)
    expect(called).toBe(true)
  })

  it('supports multiple unsubscribes safely', () => {
    const { createEventBus } = require('../bridge')
    const bus = createEventBus()
    const received: number[] = []
    const unsub = bus.on((n: number) => received.push(n))
    unsub()
    unsub() // second call is a no-op
    bus.emit(1)
    expect(received).toEqual([])
  })
})

describe('persistedSignal', () => {
  const g = globalThis as any

  beforeEach(() => {
    delete g.AsyncStorage
    delete g.__neutronStorage
    jest.resetModules()
  })

  afterEach(() => {
    delete g.AsyncStorage
    delete g.__neutronStorage
  })

  it('creates a signal with the initial value when no storage', () => {
    const { persistedSignal } = require('../bridge')
    const s = persistedSignal('test-key', 42)
    expect(s.value).toBe(42)
  })

  it('creates a signal and restores from AsyncStorage', async () => {
    const store: Record<string, string> = { 'my-key': '"restored"' }
    g.AsyncStorage = {
      getItem: jest.fn(async (key: string) => store[key] ?? null),
      setItem: jest.fn(async () => {}),
      removeItem: jest.fn(async () => {}),
    }
    const { persistedSignal } = require('../bridge')
    const s = persistedSignal('my-key', 'default')
    // After the async restore resolves
    await new Promise(r => setTimeout(r, 10))
    expect(s.value).toBe('restored')
  })

  it('persists value changes to AsyncStorage', async () => {
    const store: Record<string, string> = {}
    g.AsyncStorage = {
      getItem: jest.fn(async () => null),
      setItem: jest.fn(async (key: string, value: string) => { store[key] = value }),
      removeItem: jest.fn(async () => {}),
    }
    const { persistedSignal } = require('../bridge')
    const s = persistedSignal('persist-test', 'initial')
    await new Promise(r => setTimeout(r, 10))
    s.value = 'updated'
    await new Promise(r => setTimeout(r, 10))
    expect(g.AsyncStorage.setItem).toHaveBeenCalledWith('persist-test', '"updated"')
  })

  it('calls removeItem when value is set to null', async () => {
    g.AsyncStorage = {
      getItem: jest.fn(async () => null),
      setItem: jest.fn(async () => {}),
      removeItem: jest.fn(async () => {}),
    }
    const { persistedSignal } = require('../bridge')
    const s = persistedSignal('nullable', 'something')
    await new Promise(r => setTimeout(r, 10))
    s.value = null
    await new Promise(r => setTimeout(r, 10))
    expect(g.AsyncStorage.removeItem).toHaveBeenCalledWith('nullable')
  })

  it('handles non-JSON stored values gracefully', async () => {
    g.AsyncStorage = {
      getItem: jest.fn(async () => 'plain-string-not-json'),
      setItem: jest.fn(async () => {}),
      removeItem: jest.fn(async () => {}),
    }
    const { persistedSignal } = require('../bridge')
    const s = persistedSignal('non-json', 'default')
    await new Promise(r => setTimeout(r, 10))
    // Should fall back to raw string
    expect(s.value).toBe('plain-string-not-json')
  })

  it('uses __neutronStorage as fallback', async () => {
    g.__neutronStorage = {
      getItem: jest.fn(async () => '100'),
      setItem: jest.fn(async () => {}),
      removeItem: jest.fn(async () => {}),
    }
    const { persistedSignal } = require('../bridge')
    const s = persistedSignal('fallback', 0)
    await new Promise(r => setTimeout(r, 10))
    expect(s.value).toBe(100)
  })

  it('provides a dispose method to unsubscribe', () => {
    g.AsyncStorage = {
      getItem: jest.fn(async () => null),
      setItem: jest.fn(async () => {}),
      removeItem: jest.fn(async () => {}),
    }
    const { persistedSignal } = require('../bridge')
    const s = persistedSignal('disposable', 'x')
    expect(typeof (s as any).dispose).toBe('function')
  })
})
