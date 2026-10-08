import { getModule, clearCache } from '../../registry.js'
import '../../modules/device-info.web.js'
let mod: any
beforeEach(() => {
  Object.defineProperty(globalThis, 'navigator', { configurable: true, value: {} })
  ;(globalThis as any).window = { addEventListener: jest.fn(), removeEventListener: jest.fn(), location: { hostname: 'example.test' } }
  ;(globalThis as any).document = {}
  clearCache()
  mod = getModule('NeutronDeviceInfo')
  expect(mod).not.toBeNull()
})
afterEach(() => { jest.useRealTimers() })
it('actual browser info reports locale and browser version', () => { (navigator as any).userAgent = 'Mozilla Firefox/123.0'; (navigator as any).language = 'en-CA'; (navigator as any).platform = 'MacIntel'; (globalThis as any).screen = { width: 800, height: 600 }; expect(mod.getLocale()).toBe('en-CA'); expect(mod.getInfo()).toMatchObject({ brand: 'Firefox', model: 'MacIntel' }) })
it('unavailable battery returns explicit unknown level', async () => { expect(await mod.getBatteryLevel()).toBe(-1) })
