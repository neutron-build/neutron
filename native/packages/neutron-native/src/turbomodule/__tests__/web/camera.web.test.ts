import { getModule, clearCache } from '../../registry.js'
import '../../modules/camera.web.js'
let mod: any
beforeEach(() => {
  Object.defineProperty(globalThis, 'navigator', { configurable: true, value: {} })
  ;(globalThis as any).window = { addEventListener: jest.fn(), removeEventListener: jest.fn(), location: { hostname: 'example.test' } }
  ;(globalThis as any).document = {}
  clearCache()
  mod = getModule('NeutronCamera')
  expect(mod).not.toBeNull()
})
afterEach(() => { jest.useRealTimers() })
it('captures through actual getUserMedia and stops acquired tracks', async () => {
  const stop = jest.fn(); const stream = { getTracks: () => [{ stop }] }
  const play = jest.fn(async () => {}); const drawImage = jest.fn()
  const video = { srcObject: null, setAttribute: jest.fn(), play, videoWidth: 32, videoHeight: 24, readyState: 2, addEventListener: jest.fn(), removeEventListener: jest.fn() }
  const canvas = { getContext: () => ({ drawImage }), toDataURL: () => 'data:image/jpeg;base64,YQ==' }
  ;(navigator as any).mediaDevices = { getUserMedia: jest.fn(async () => stream) }
  ;(document as any).createElement = jest.fn((name: string) => name === 'video' ? video : canvas)
  ;(globalThis as any).requestAnimationFrame = (cb: Function) => cb()
  const result = await mod.capture({ facing: 'front' })
  expect(result).toMatchObject({ ok: true, value: { width: 32, height: 24, type: 'photo' } })
  expect(navigator.mediaDevices.getUserMedia).toHaveBeenCalledWith(expect.objectContaining({ video: expect.objectContaining({ facingMode: 'user' }) }))
  expect(drawImage).toHaveBeenCalledWith(video, 0, 0); expect(stop).toHaveBeenCalledTimes(1)
})
it('maps actual acquisition permission failure', async () => {
  ;(navigator as any).mediaDevices = { getUserMedia: jest.fn(async () => { const e = new Error('denied'); e.name = 'NotAllowedError'; throw e }) }
  expect(await mod.capture()).toMatchObject({ ok: false, error: { code: 'PERMISSION_DENIED' } })
})
it.each(['granted', 'denied'])('checks actual %s permission', async state => {
  ;(navigator as any).permissions = { query: jest.fn(async () => ({ state })) }
  expect(await mod.checkPermission()).toBe(state); expect(navigator.permissions.query).toHaveBeenCalledWith({ name: 'camera' })
})

describe('NF-NR-10 camera.web lifecycle', () => {
  it('a rejected play() still stops every acquired track and detaches the stream', async () => {
    const stop = jest.fn(); const stream = { getTracks: () => [{ stop }, { stop }] }
    const video = { srcObject: { marker: 1 }, setAttribute: jest.fn(), play: jest.fn(async () => { throw new Error('aborted') }), addEventListener: jest.fn(), removeEventListener: jest.fn() }
    ;(globalThis as any).document = { createElement: jest.fn(() => video) }
    ;(navigator as any).mediaDevices = { getUserMedia: jest.fn(async () => stream) }
    const result = await mod.capture()
    expect(result.ok).toBe(false)
    expect(stop).toHaveBeenCalledTimes(2)
    expect(video.srcObject).toBeNull()
  })

  it('video capture is explicitly unsupported, never a silent photo', async () => {
    const getUserMedia = jest.fn()
    ;(navigator as any).mediaDevices = { getUserMedia }
    const result = await mod.capture({ mediaType: 'video' } as never)
    expect(result).toMatchObject({ ok: false, error: { code: 'UNSUPPORTED' } })
    expect(getUserMedia).not.toHaveBeenCalled()  // refused before opening the camera
  })

  it('gallery object URLs are caller-owned and revocable', async () => {
    const { revokeGalleryUris } = await import('../../modules/camera.web.js')
    const revoke = jest.fn()
    ;(URL as any).revokeObjectURL = revoke
    revokeGalleryUris([{ uri: 'blob:one' }, { uri: 'data:image/jpeg;base64,x' }, { uri: 'blob:two' }])
    expect(revoke).toHaveBeenCalledTimes(2)
  })
})
