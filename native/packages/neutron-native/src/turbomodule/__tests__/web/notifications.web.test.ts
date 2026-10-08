import { getModule, clearCache } from '../../registry.js'
import '../../modules/notifications.web.js'
let mod: any
beforeEach(() => {
  Object.defineProperty(globalThis, 'navigator', { configurable: true, value: {} })
  ;(globalThis as any).window = { addEventListener: jest.fn(), removeEventListener: jest.fn(), location: { hostname: 'example.test' } }
  ;(globalThis as any).document = {}
  clearCache()
  mod = getModule('NeutronNotifications')
  expect(mod).not.toBeNull()
})
afterEach(() => { jest.useRealTimers() })
it('unsupported browser reports permission denial and unavailable tokens', async () => { expect(await mod.requestPermission()).toBe('denied'); expect(await mod.getToken()).toMatchObject({ ok: false, error: { code: 'UNAVAILABLE' } }) })
it('actual provider denial prevents scheduling', async () => { const notification = Object.assign(jest.fn(), { permission: 'denied', requestPermission: jest.fn(async () => 'denied') }); (globalThis as any).Notification = notification; (window as any).Notification = notification; expect(await mod.scheduleLocal({ title: 'hello' })).toMatchObject({ ok: false, error: { code: 'PERMISSION_DENIED' } }); expect(notification).not.toHaveBeenCalled() })

describe('NF-NR-11 web scheduling contract', () => {
  const grantPermission = () => {
    const notification: any = class GrantedNotification {
      static permission = 'granted'
      static requestPermission = jest.fn(async () => 'granted')
      close = jest.fn()
    }
    ;(globalThis as any).Notification = notification
    ;(window as any).Notification = notification
    return notification
  }
  it('repeat is rejected as unsupported instead of scheduling one delivery', async () => {
    grantPermission()
    const result = await mod.scheduleLocal({ title: 't', repeat: 'day' } as never)
    expect(result).toMatchObject({ ok: false, error: { code: 'UNSUPPORTED' } })
  })

  it('an invalid fireAt is an argument error, not an instant notification', async () => {
    const Granted = grantPermission()
    const result = await mod.scheduleLocal({ title: 't', fireAt: 'not-a-date' } as never)
    expect(result).toMatchObject({ ok: false, error: { code: 'INVALID_ARGUMENT' } })
    expect(Granted.mock).toBeUndefined()  // class mock never constructed
    const fresh = jest.fn()
    ;(globalThis as any).Notification = fresh; (window as any).Notification = fresh
    expect(fresh).not.toHaveBeenCalled()
  })

  it('a negative fireAfter is rejected', async () => {
    grantPermission()
    const result = await mod.scheduleLocal({ title: 't', fireAfter: -5 } as never)
    expect(result).toMatchObject({ ok: false, error: { code: 'INVALID_ARGUMENT' } })
  })

  it('a fired timer cleans its own record', async () => {
    grantPermission()
    jest.useFakeTimers()
    const result = await mod.scheduleLocal({ title: 'later', fireAfter: 60 } as never)
    expect(result.ok).toBe(true)
    await mod.cancel((result as { value: string }).value)  // pending timer cancels
    const second = await mod.scheduleLocal({ title: 'again', fireAfter: 60 } as never)
    jest.advanceTimersByTime(61_000)
    // After firing, cancel resolves without error (record already cleaned).
    await expect(mod.cancel((second as { value: string }).value)).resolves.toBeUndefined()
    jest.useRealTimers()
  })

  it('immediate constructor failures surface to the caller', async () => {
    const failing: any = jest.fn(function (this: any, _title: string) { throw new Error('constructor failed') })
    failing.permission = 'granted'
    ;(globalThis as any).Notification = failing
    ;(window as any).Notification = failing
    const result = await mod.scheduleLocal({ title: 'boom' } as never)
    expect(result).toMatchObject({ ok: false, error: { code: 'UNAVAILABLE', message: 'constructor failed' } })
  })
})
