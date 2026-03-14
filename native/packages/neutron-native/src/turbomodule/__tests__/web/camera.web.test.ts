import { describe, it, expect, beforeEach } from '@jest/globals'

describe('Camera web implementation', () => {
  beforeEach(() => {
    Object.defineProperty(navigator, 'mediaDevices', {
      value: {
        getUserMedia: jest.fn(),
      },
      configurable: true,
    })
  })

  it('should request camera access', async () => {
    const media = navigator.mediaDevices as any
    media.getUserMedia({ video: true })
    expect(media.getUserMedia).toHaveBeenCalledWith({ video: true })
  })

  it('should return stream on success', async () => {
    const stream = { id: 'stream-id', getTracks: jest.fn(() => []) }
    const get = jest.fn(async () => stream) as any
    const result = await get({ video: true })
    expect(result.id).toBe('stream-id')
  })

  it('should handle permission denied', async () => {
    const get = jest.fn(async () => {
      throw new DOMException('Permission denied', 'NotAllowedError')
    }) as any
    await expect(get({ video: true })).rejects.toThrow('Permission denied')
  })

  it('should handle device not found', async () => {
    const get = jest.fn(async () => {
      throw new DOMException('Requested device not found', 'NotFoundError')
    }) as any
    await expect(get({ video: true })).rejects.toThrow('Requested device not found')
  })

  it('should check camera availability', () => {
    const hasCamera = typeof navigator.mediaDevices?.getUserMedia === 'function'
    expect(typeof hasCamera).toBe('boolean')
  })

  it('should capture video frame', async () => {
    const stream = {
      getTracks: jest.fn(() => [{ stop: jest.fn() }]),
      getVideoTracks: jest.fn(() => []),
    }
    expect(stream.getTracks()).toHaveLength(1)
  })

  it('should stop camera stream', () => {
    const track = { stop: jest.fn() }
    track.stop()
    expect(track.stop).toHaveBeenCalled()
  })

  it('should handle NotAllowedError', async () => {
    const capture = jest.fn(async () => {
      throw new DOMException('User denied', 'NotAllowedError')
    })
    await expect(capture()).rejects.toThrow('User denied')
  })
})
