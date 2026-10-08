/**
 * NF-NR-06 regressions — the deliberately limited PanResponder fallback:
 * disabled detectors never claim touches, pinch baselines follow pointer
 * transitions and accumulate, terminal events retain values and honor
 * ownership, composition overlaps refuse loudly, and configuration
 * (fling direction, pan fail offsets) is enforced.
 */

import { GestureDetector, Gesture } from '../handlers'

export {}

interface Touch { pageX: number; pageY: number }
interface Evt { nativeEvent: { touches: Touch[]; pageX: number; pageY: number; locationX: number; locationY: number } }
interface GS { numberActiveTouches: number; dx: number; dy: number; vx: number; vy: number; x0: number; y0: number }

function evt(touches: Touch[]): Evt {
  const last = touches[touches.length - 1] ?? { pageX: 0, pageY: 0 }
  return { nativeEvent: { touches, pageX: last.pageX, pageY: last.pageY, locationX: last.pageX, locationY: last.pageY } }
}
function gs(count: number, dx = 0, dy = 0, vx = 0, vy = 0): GS {
  return { numberActiveTouches: count, dx, dy, vx, vy, x0: 0, y0: 0 }
}

describe('NF-NR-06 PanResponder fallback limits', () => {
  it('a detector whose every gesture is disabled never claims a touch', () => {
    const element = (GestureDetector as any)({ gesture: Gesture.Tap().enabled(false) })
    const panHandlers = element.props
    expect(panHandlers.onStartShouldSetPanResponder()).toBe(false)
    expect(panHandlers.onMoveShouldSetPanResponder(evt([]), gs(1))).toBe(false)
  })

  it('throws on Simultaneous over two continuous gestures (no silent drop)', () => {
    expect(() => (GestureDetector as any)({ gesture: Gesture.Simultaneous(Gesture.Pan(), Gesture.Pinch()) })).toThrow(/react-native-gesture-handler/)
  })

  it('sequential finger placement reports increasing pinch scale with exactly one start', () => {
    const starts: number[] = []
    const updates: number[] = []
    const cfg = Gesture.Pinch()
      .onStart((e) => starts.push(e.scale))
      .onUpdate((e) => updates.push(e.scale))
      .onEnd((e) => updates.push(e.scale)) as any
    const element = (GestureDetector as any)({ gesture: cfg })
    const h = element.props
    // First finger down, then second arrives — grant at 1 finger, transition to 2.
    h.onPanResponderGrant(evt([{ pageX: 100, pageY: 100 }]), gs(1))
    const pair = (d: number): Touch[] => [{ pageX: 100, pageY: 100 }, { pageX: 100 + d, pageY: 100 }]
    h.onPanResponderMove(evt(pair(50)), gs(2))    // baseline re-captured at transition
    h.onPanResponderMove(evt(pair(75)), gs(2))    // scale 1.5
    h.onPanResponderMove(evt(pair(100)), gs(2))   // scale 2.0
    h.onPanResponderRelease(evt(pair(100)), gs(2))
    expect(starts).toHaveLength(1)
    const finalScale = updates[updates.length - 1]
    expect(finalScale).toBeGreaterThan(1.4)
    expect(finalScale).toBeLessThan(2.1)
  })

  it('pinch end RETAINS the accumulated scale', () => {
    const ends: number[] = []
    const cfg = Gesture.Pinch().onEnd((e) => ends.push(e.scale)) as any
    const h = (GestureDetector as any)({ gesture: cfg }).props
    h.onPanResponderGrant(evt([{ pageX: 100, pageY: 100 }]), gs(1))
    const pair = (d: number): Touch[] => [{ pageX: 100, pageY: 100 }, { pageX: 100 + d, pageY: 100 }]
    h.onPanResponderMove(evt(pair(50)), gs(2))
    h.onPanResponderMove(evt(pair(100)), gs(2))   // scale 2.0
    h.onPanResponderRelease(evt(pair(100)), gs(2))
    expect(ends).toHaveLength(1)
    expect(ends[0]).toBeGreaterThan(1.5)   // NOT reset to 1
  })

  it('fling honors its configured direction', () => {
    const ends: Array<Record<string, unknown>> = []
    const cfg = Gesture.Fling().direction('right').onEnd((e) => ends.push(e as unknown as Record<string, unknown>)) as any
    const h = (GestureDetector as any)({ gesture: cfg }).props
    // Fast LEFT swipe must not fire a right-fling.
    h.onPanResponderGrant(evt([{ pageX: 100, pageY: 100 }]), gs(1))
    h.onPanResponderRelease(evt([{ pageX: 0, pageY: 100 }]), gs(1, -120, 0, -3, 0))
    expect(ends).toHaveLength(0)
    // Fast RIGHT swipe fires.
    h.onPanResponderGrant(evt([{ pageX: 0, pageY: 100 }]), gs(1))
    h.onPanResponderRelease(evt([{ pageX: 120, pageY: 100 }]), gs(1, 120, 0, 3, 0))
    expect(ends).toHaveLength(1)
  })

  it('pan failOffset bounds refuse activation', () => {
    const starts: unknown[] = []
    const cfg = Gesture.Pan().failOffsetY(20).onStart((e) => starts.push(e)) as any
    const h = (GestureDetector as any)({ gesture: cfg }).props
    h.onPanResponderGrant(evt([{ pageX: 100, pageY: 100 }]), gs(1))
    h.onPanResponderMove(evt([{ pageX: 100, pageY: 130 }]), gs(1, 0, 30, 0, 2))
    expect(starts).toHaveLength(0)
  })
})
