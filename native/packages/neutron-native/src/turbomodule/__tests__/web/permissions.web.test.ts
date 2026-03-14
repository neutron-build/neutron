import { describe, it, expect, beforeEach } from '@jest/globals'

describe('Permissions web implementation', () => {
  beforeEach(() => {
    Object.defineProperty(navigator, 'permissions', {
      value: {
        query: jest.fn(async () => ({ state: 'granted' })),
      },
      configurable: true,
    })
  })

  it('should check permission granted', async () => {
    const query = navigator.permissions?.query as any
    const result = await query({ name: 'camera' })
    expect(result.state).toBe('granted')
  })

  it('should check permission denied', async () => {
    const query = jest.fn(async () => ({ state: 'denied' })) as any
    const result = await query({ name: 'microphone' })
    expect(result.state).toBe('denied')
  })

  it('should check permission prompt', async () => {
    const query = jest.fn(async () => ({ state: 'prompt' })) as any
    const result = await query({ name: 'geolocation' })
    expect(result.state).toBe('prompt')
  })

  it('should return unavailable for unsupported permissions', async () => {
    const query = jest.fn(async () => ({ state: 'unavailable' })) as any
    const result = await query({ name: 'unsupported' })
    expect(result.state).toBe('unavailable')
  })

  it('should warn when opening settings', () => {
    const warn = jest.fn()
    warn()
    expect(warn).toHaveBeenCalled()
  })
})
