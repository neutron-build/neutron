import { getModule, clearCache } from '../../registry.js'
import '../../modules/haptics.web.js'
let mod: any
beforeEach(() => {
  Object.defineProperty(globalThis, 'navigator', { configurable: true, value: {} })
  ;(globalThis as any).window = { addEventListener: jest.fn(), removeEventListener: jest.fn(), location: { hostname: 'example.test' } }
  ;(globalThis as any).document = {}
  clearCache()
  mod = getModule('NeutronHaptics')
  expect(mod).not.toBeNull()
})
afterEach(() => { jest.useRealTimers() })
it.each([['light', 10], ['medium', 20], ['heavy', 40]])('impact %s calls shipped vibration mapping', (style, duration) => {
  navigator.vibrate = jest.fn(); mod.impact(style); expect(navigator.vibrate).toHaveBeenCalledWith(duration)
})
it('notification uses the shipped warning pattern', () => { navigator.vibrate = jest.fn(); mod.notification('warning'); expect(navigator.vibrate).toHaveBeenCalledWith([0, 25, 50, 25]) })
it('availability reflects the actual provider', () => { expect(mod.isAvailable()).toBe(false); navigator.vibrate = jest.fn(); expect(mod.isAvailable()).toBe(true) })
