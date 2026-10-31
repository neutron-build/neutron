import { getModule, clearCache } from '../../registry.js'
import '../../modules/async-storage.web.js'
let mod: any
beforeEach(() => {
  Object.defineProperty(globalThis, 'navigator', { configurable: true, value: {} })
  ;(globalThis as any).window = { addEventListener: jest.fn(), removeEventListener: jest.fn(), location: { hostname: 'example.test' } }
  ;(globalThis as any).document = {}
  clearCache()
  mod = getModule('NeutronAsyncStorage')
  expect(mod).not.toBeNull()
})
afterEach(() => { jest.useRealTimers() })
it('actual storage roundtrips and clears its own values', async () => {
  await mod.clear(); await mod.multiSet([['a', '1'], ['b', '2']]); expect(await mod.multiGet(['b', 'a'])).toEqual([['b', '2'], ['a', '1']])
  expect((await mod.getAllKeys()).sort()).toEqual(['a', 'b']); await mod.clear(); expect(await mod.getItem('a')).toBeNull()
})
it('removes only requested keys', async () => { await mod.clear(); await mod.setItem('a', '1'); await mod.setItem('b', '2'); await mod.removeItem('a'); expect(await mod.getItem('a')).toBeNull(); expect(await mod.getItem('b')).toBe('2') })
