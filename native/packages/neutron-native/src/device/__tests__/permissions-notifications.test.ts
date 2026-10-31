/**
 * NF-NR-09 regressions — notifications resolve through the dedicated
 * provider APIs (valid on iOS, where no PERMISSIONS constant exists) in
 * both single and batch shapes.
 */

jest.mock('react-native-permissions', () => ({
  PERMISSIONS: {
    IOS: { CAMERA: 'ios.camera' },
    ANDROID: { CAMERA: 'android.camera', POST_NOTIFICATIONS: 'android.post_notifications' },
  },
  check: jest.fn(async (p: string) => (p === 'ios.camera' ? 'granted' : 'denied')),
  request: jest.fn(async (p: string) => (p === 'ios.camera' ? 'granted' : 'denied')),
  checkMultiple: jest.fn(async (ps: string[]) => Object.fromEntries(ps.map(p => [p, 'granted']))),
  requestMultiple: jest.fn(async (ps: string[]) => Object.fromEntries(ps.map(p => [p, 'granted']))),
  checkNotifications: jest.fn(async () => ({ status: 'granted', settings: {} })),
  requestNotifications: jest.fn(async (_opts: string[]) => ({ status: 'denied', settings: {} })),
}), { virtual: true })

jest.mock('react-native', () => ({ Platform: { OS: 'ios', Version: 17 } }), { virtual: true })

import { check, request, checkMultiple, requestMultiple, PermissionStatus } from '../permissions'

export {}

describe('NF-NR-09 notification permission routing', () => {
  it('check(notifications) uses checkNotifications, not an unavailable constant', async () => {
    await expect(check('notifications')).resolves.toBe(PermissionStatus.GRANTED)
  })

  it('request(notifications) uses requestNotifications and reports its real result', async () => {
    await expect(request('notifications')).resolves.toBe(PermissionStatus.DENIED)
  })

  it('batch shapes share the same special-path resolution', async () => {
    const checked = await checkMultiple(['camera', 'notifications'])
    expect(checked['notifications']).toBe(PermissionStatus.GRANTED)
    expect(checked['camera']).toBe(PermissionStatus.GRANTED)  // still constant-mapped
    const requested = await requestMultiple(['camera', 'notifications'])
    expect(requested['notifications']).toBe(PermissionStatus.DENIED)
    expect(requested['camera']).toBe(PermissionStatus.GRANTED)
  })
})
