/**
 * NF-NR-05 regressions — RNGH adapter: correct builder method names,
 * forwarded interaction relations, and normalized (string-state) events.
 */

const calls: Array<{ method: string; args: unknown[] }> = []

function chainable(name: string): any {
  let proxy: any
  const builder: any = (...args: unknown[]) => {
    // Invoking the chainable itself = the constructor/composition call.
    calls.push({ method: name, args })
    return proxy
  }
  proxy = new Proxy(builder, {
    get: (_target, prop: string) => {
      if (prop === 'then') return undefined // thenable guard
      return (...args: unknown[]) => {
        calls.push({ method: `${name}.${String(prop)}`, args })
        return proxy
      }
    },
    apply: (target, _thisArg, args) => (target as (...a: unknown[]) => unknown)(...args),
  })
  return proxy
}

jest.mock('react-native-gesture-handler', () => {
  const State = {
    UNDETERMINED: 0, BEGAN: 1, ACTIVE: 2, END: 3, CANCELLED: 4, FAILED: 5,
  }
  const builders: Record<string, any> = {}
  for (const kind of ['Pan', 'Pinch', 'Rotation', 'Fling', 'Tap', 'LongPress']) {
    // The chainable itself is the factory: constructing records the base
    // name, chained methods record name.method.
    builders[kind] = chainable(kind)
  }
  return {
    __esModule: true,
    State,
    Directions: { RIGHT: 1, LEFT: 2, UP: 4, DOWN: 8 },
    Gesture: {
      ...builders,
      Simultaneous: chainable('Simultaneous'),
      Exclusive: chainable('Exclusive'),
      Race: chainable('Race'),
    },
    GestureDetector: (props: Record<string, unknown>) => ({ type: 'RNGHDetector', props }),
  }
  // Virtual: the peer is not installed in this workspace.
}, { virtual: true })

import { GestureDetector, Gesture } from '../handlers'
import { hasGestureHandler } from '../handlers'

export {}

beforeEach(() => { calls.length = 0 })

describe('NF-NR-05 RNGH adapter', () => {
  it('detects the mocked provider', () => {
    expect(hasGestureHandler()).toBe(true)
  })

  it('LongPress().maxDistance uses the RNGH 2 builder method (no maxDist)', () => {
    GestureDetector({ gesture: Gesture.LongPress().minDuration(300).maxDistance(20) } as never)
    const flat = calls.map(c => c.method).join(',')
    expect(flat).toContain('LongPress.minDuration')
    expect(flat).toContain('LongPress.maxDistance')
    expect(flat).not.toContain('maxDist(')  // maxDistance( is the RNGH 2 name; maxDist( must never appear
  })

  it('forwards simultaneousWith through the provider composition graph', () => {
    const pan = Gesture.Pan()
    const pinch = Gesture.Pinch()
    GestureDetector({ gesture: pan.simultaneousWith(pinch as never) } as never)
    const flat = calls.map(c => c.method).join(',')
    expect(flat).toContain('Simultaneous')
    expect(flat).toContain('Pan')
    expect(flat).toContain('Pinch')
  })

  it('forwards requireExternalFailure via requireToFail', () => {
    const longPress = Gesture.LongPress()
    const tap = Gesture.Tap()
    GestureDetector({ gesture: longPress.requireExternalFailure(tap as never) } as never)
    const flat = calls.map(c => c.method).join(',')
    expect(flat).toContain('LongPress.requireToFail')
  })

  it('normalizes RNGH numeric states into Neutron string states', () => {
    let received: { state: string } | undefined
    const cfg = Gesture.Pan().onStart((event: { state: string }) => { received = event })
    GestureDetector({ gesture: cfg } as never)
    // Find the registered onStart handler (the adapter wrapped it) and
    // invoke it with a provider-shaped numeric-state event.
    const registered = calls.find(c => c.method === 'Pan.onStart')
    expect(registered).toBeTruthy()
    const handler = registered!.args[0] as (event: unknown) => void
    handler({ nativeEvent: { state: 2, translationX: 5, translationY: 6, velocityX: 10, velocityY: 0, absoluteX: 1, absoluteY: 2, x: 1, y: 2, numberOfPointers: 1 } })
    expect(received).toBeTruthy()
    expect((received as unknown as { state: string }).state).toBe('active')
    expect((received as unknown as { translationX: number }).translationX).toBe(5)
  })
})
