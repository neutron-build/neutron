import { getModule, clearCache } from '../../registry.js'
import '../../modules/clipboard.web.js'
let mod: any
beforeEach(() => {
  Object.defineProperty(globalThis, 'navigator', { configurable: true, value: {} })
  ;(globalThis as any).window = { addEventListener: jest.fn(), removeEventListener: jest.fn(), location: { hostname: 'example.test' } }
  ;(globalThis as any).document = {}
  clearCache()
  mod = getModule('NeutronClipboard')
  expect(mod).not.toBeNull()
})
afterEach(() => { jest.useRealTimers() })
it('reads and writes through the real clipboard adapter', async () => {
  ;(navigator as any).clipboard = { readText: jest.fn(async () => 'actual text'), writeText: jest.fn(async () => {}) }
  expect(await mod.getString()).toBe('actual text'); expect(await mod.hasString()).toBe(true); mod.setString('new'); expect(navigator.clipboard.writeText).toHaveBeenCalledWith('new')
})
it('poll subscription is removed and emits actual content', async () => {
  jest.useFakeTimers(); (navigator as any).clipboard = { readText: jest.fn(async () => 'changed') }; const cb = jest.fn(); const subscription = mod.onChange(cb)
  await jest.advanceTimersByTimeAsync(2000); expect(cb).toHaveBeenCalledWith({ content: 'changed' }); subscription.remove(); cb.mockClear(); await jest.advanceTimersByTimeAsync(4000); expect(cb).not.toHaveBeenCalled(); expect(jest.getTimerCount()).toBe(0)
})
