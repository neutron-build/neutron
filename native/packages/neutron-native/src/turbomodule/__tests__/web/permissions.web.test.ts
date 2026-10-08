import { getModule, clearCache } from '../../registry.js'
import '../../modules/permissions.web.js'
let mod: any
beforeEach(() => {
  Object.defineProperty(globalThis, 'navigator', { configurable: true, value: {} })
  ;(globalThis as any).window = { addEventListener: jest.fn(), removeEventListener: jest.fn(), location: { hostname: 'example.test' } }
  ;(globalThis as any).document = {}
  clearCache()
  mod = getModule('NeutronPermissions')
  expect(mod).not.toBeNull()
})
afterEach(() => { jest.useRealTimers() })
it.each(['granted', 'denied'])('maps %s from the actual permission query', async state => { (navigator as any).permissions = { query: jest.fn(async () => ({ state })) }; expect(await mod.check('camera')).toBe(state); expect(navigator.permissions.query).toHaveBeenCalledWith({ name: 'camera' }) })
it('actual camera request releases granted stream', async () => { const stop = jest.fn(); (navigator as any).mediaDevices = { getUserMedia: jest.fn(async () => ({ getTracks: () => [{ stop }] })) }; expect(await mod.request('camera')).toBe('granted'); expect(stop).toHaveBeenCalledTimes(1) })
it('unmapped permissions explicitly unavailable', async () => { expect(await mod.check('contacts')).toBe('unavailable') })
