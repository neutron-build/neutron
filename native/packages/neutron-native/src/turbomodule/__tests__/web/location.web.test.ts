import { getModule, clearCache } from '../../registry.js'
import '../../modules/location.web.js'
let mod: any
beforeEach(() => {
  Object.defineProperty(globalThis, 'navigator', { configurable: true, value: {} })
  ;(globalThis as any).window = { addEventListener: jest.fn(), removeEventListener: jest.fn(), location: { hostname: 'example.test' } }
  ;(globalThis as any).document = {}
  clearCache()
  mod = getModule('NeutronLocation')
  expect(mod).not.toBeNull()
})
afterEach(() => { jest.useRealTimers() })
it('actual provider coordinates are normalized', async () => { (navigator as any).geolocation = { getCurrentPosition: jest.fn((cb: Function) => cb({ coords: { latitude: 1, longitude: 2, accuracy: 3 }, timestamp: 4 })) }; expect(await mod.getCurrentPosition()).toMatchObject({ ok: true, value: { latitude: 1, longitude: 2, timestamp: 4 } }) })
it('permission denial maps to failure', async () => { (navigator as any).geolocation = { getCurrentPosition: (_s: Function, fail: Function) => fail({ code: 1, message: 'denied' }) }; expect(await mod.getCurrentPosition()).toMatchObject({ ok: false, error: { code: 'PERMISSION_DENIED' } }) })
it('watch removal calls the actual provider id', () => { (navigator as any).geolocation = { watchPosition: jest.fn(() => 42), clearWatch: jest.fn() }; const subscription = mod.watchPosition(jest.fn()); subscription.remove(); expect(navigator.geolocation.clearWatch).toHaveBeenCalledWith(42) })
