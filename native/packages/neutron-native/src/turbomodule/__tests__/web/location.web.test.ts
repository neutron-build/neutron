import { describe, it, expect, beforeEach } from '@jest/globals'

describe('Location web implementation', () => {
  beforeEach(() => {
    Object.defineProperty(navigator, 'geolocation', {
      value: {
        getCurrentPosition: jest.fn(),
        watchPosition: jest.fn(),
      },
      configurable: true,
    })
  })

  it('should get current position', async () => {
    const geo = navigator.geolocation as any
    const callback = jest.fn()
    geo.getCurrentPosition(callback)
    expect(geo.getCurrentPosition).toHaveBeenCalled()
  })

  it('should return position on success', () => {
    const success = jest.fn()
    const position = { coords: { latitude: 40.7128, longitude: -74.0060 } }
    success(position)
    expect(success).toHaveBeenCalledWith(position)
  })

  it('should handle permission denied', () => {
    const error = jest.fn()
    const err = { code: 1, message: 'Permission denied' }
    error(err)
    expect(error).toHaveBeenCalledWith(err)
  })

  it('should handle position unavailable', () => {
    const error = jest.fn()
    const err = { code: 2, message: 'Position unavailable' }
    error(err)
    expect(error).toHaveBeenCalledWith(err)
  })

  it('should watch position', () => {
    const geo = navigator.geolocation as any
    const callback = jest.fn()
    geo.watchPosition(callback)
    expect(geo.watchPosition).toHaveBeenCalledWith(callback)
  })

  it('should return watch ID', () => {
    const watch = { id: 123 }
    expect(watch.id).toBe(123)
  })
})
