import { listenerTarget } from '../fixtures/listener-target.js'
import { getModule, clearCache } from '../../registry.js'
import '../../modules/net-info.web.js'
let mod: any
let browser: ReturnType<typeof listenerTarget>
let connection: ReturnType<typeof listenerTarget>
beforeEach(() => {
  browser = listenerTarget(); connection = listenerTarget()
  Object.defineProperty(globalThis, 'navigator', { configurable: true, value: { connection } })
  ;(globalThis as any).window = { ...browser, location: { hostname: 'example.test' } }
  ;(globalThis as any).document = {}
  clearCache()
  mod = getModule('NeutronNetInfo')
  expect(mod).not.toBeNull()
})
afterEach(() => { jest.useRealTimers() })
it('uses actual online state', async () => { (navigator as any).onLine = false; expect(await mod.fetch()).toMatchObject({ isConnected: false, type: 'none' }); (navigator as any).onLine = true; expect(await mod.isConnected()).toBe(true) })
it('subscription cleanup removes the exact callbacks and stops browser/connection delivery', () => {
  const cb = jest.fn()
  const subscription = mod.addEventListener(cb)
  for (const event of ['online', 'offline']) browser.dispatch(event)
  connection.dispatch('change')
  expect(cb).toHaveBeenCalledTimes(3)
  subscription.remove()
  for (const event of ['online', 'offline']) {
    expect(browser.removeEventListener).toHaveBeenCalledWith(event, browser.registered(event))
    browser.dispatch(event)
  }
  expect(connection.removeEventListener).toHaveBeenCalledWith('change', connection.registered('change'))
  connection.dispatch('change')
  expect(browser.count()).toBe(0); expect(connection.count()).toBe(0)
  expect(cb).toHaveBeenCalledTimes(3)
})
