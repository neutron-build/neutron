/**
 * NF-NR-08 regressions — camera and location adapters never manufacture
 * permission success; provider denial reaches the caller; explicit
 * cancellation stays distinguishable from failure.
 */

jest.mock('react-native-image-picker', () => ({
  launchCamera: jest.fn(),
}), { virtual: true })

jest.mock('@react-native-community/geolocation', () => {
  const api: Record<string, unknown> = {
    requestAuthorization: jest.fn((onSuccess: (s: boolean) => void, onError: () => void) => {
      api.__respond = (answer: boolean | Error) => {
        if (answer === true) onSuccess(true)
        else if (answer === false) onSuccess(false)
        else onError()
      }
    }),
    getCurrentPosition: jest.fn(),
    clearWatch: jest.fn(),
    watchPosition: jest.fn(),
    __esModule: true,
  }
  return api
}, { virtual: true })

jest.mock('react-native', () => ({
  Platform: { OS: 'android' },
  PermissionsAndroid: {
    PERMISSIONS: { CAMERA: 'android.permission.CAMERA' },
    RESULTS: { GRANTED: 'granted', DENIED: 'denied', NEVER_ASK_AGAIN: 'never_ask_again' },
    check: jest.fn(async () => false),
    request: jest.fn(async () => 'denied'),
  },
}), { virtual: true })

import { requestCameraPermission, takePicture } from '../camera'
import { requestLocationPermission } from '../location'
// @ts-expect-error virtual mock internals
import picker from 'react-native-image-picker'
// @ts-expect-error virtual mock internals
import * as geo from '@react-native-community/geolocation'

export {}

describe('NF-NR-08 camera adapter', () => {
  it('never reports granted without an actual grant (Android denial)', async () => {
    await expect(requestCameraPermission()).resolves.toBe('denied')
  })

  it('an image-picker permission errorCode rejects with a typed error', async () => {
    (picker.launchCamera as jest.Mock).mockResolvedValueOnce({ errorCode: 'permission', errorMessage: 'denied' })
    await expect(takePicture()).rejects.toThrow(/permission/i)
  })

  it('other launch failures reject with the errorCode; didCancel stays null', async () => {
    ;(picker.launchCamera as jest.Mock).mockResolvedValueOnce({ errorCode: 'camera_unavailable' })
    await expect(takePicture()).rejects.toThrow(/camera_unavailable/)
    ;(picker.launchCamera as jest.Mock).mockResolvedValueOnce({ didCancel: true })
    await expect(takePicture()).resolves.toBeNull()
  })
})

describe('NF-NR-08 location adapter (community geolocation)', () => {
  it('the permission promise stays pending until the real callback answers', async () => {
    let pending = true
    const promise = requestLocationPermission().then(status => { pending = false; return status })
    await Promise.resolve()
    expect(pending).toBe(true)  // no fabricated early 'granted'
    ;(geo as any).__respond(true)
    await expect(promise).resolves.toBe('granted')
  })

  it('a provider denial reaches the caller as denied', async () => {
    const promise = requestLocationPermission()
    ;(geo as any).__respond(false)
    await expect(promise).resolves.toBe('denied')
  })

  it('an authorization error reaches the caller as denied', async () => {
    const promise = requestLocationPermission()
    ;(geo as any).__respond(new Error('blocked'))
    await expect(promise).resolves.toBe('denied')
  })
})
