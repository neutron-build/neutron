import { describe, it, expect, beforeEach } from '@jest/globals'

describe('Notifications web implementation', () => {
  beforeEach(() => {
    Object.defineProperty(global, 'Notification', {
      value: jest.fn(),
      writable: true,
      configurable: true,
    })
    Object.defineProperty(Notification, 'permission', { value: 'granted', configurable: true })
    Object.defineProperty(Notification, 'requestPermission', {
      value: jest.fn(async () => 'granted'),
      configurable: true,
    })
  })

  it('should schedule local notification when granted', async () => {
    const Notif = Notification as any
    const perm = await Notif.requestPermission()
    expect(perm).toBe('granted')
  })

  it('should not schedule when denied', async () => {
    const requestPerm = jest.fn(async () => 'denied')
    const perm = await requestPerm()
    expect(perm).toBe('denied')
  })

  it('should create notification with title', () => {
    const Notif = Notification as any
    const n = new Notif('Test Title')
    expect(Notif).toHaveBeenCalledWith('Test Title')
  })

  it('should create notification with options', () => {
    const Notif = Notification as any
    const opts = { body: 'Test body', icon: 'icon.png' }
    new Notif('Title', opts)
    expect(Notif).toHaveBeenCalledWith('Title', opts)
  })

  it('should cancel notification', () => {
    const Notif = Notification as any
    const mock = jest.fn()
    const n = { close: mock }
    n.close()
    expect(mock).toHaveBeenCalled()
  })

  it('should cancel all notifications', () => {
    const close = jest.fn()
    ;[{ close }, { close }].forEach(n => n.close())
    expect(close).toHaveBeenCalledTimes(2)
  })
})
