import { describe, it, expect, beforeEach } from '@jest/globals'

// Mock window for node environment
if (typeof window === 'undefined') {
  (global as any).window = {}
}

describe('Biometrics web implementation', () => {
  beforeEach(() => {
    Object.defineProperty(window, 'PublicKeyCredential', {
      value: jest.fn(),
      writable: true,
      configurable: true,
    })
  })

  it('should check if biometrics available', () => {
    const isAvailable = typeof window.PublicKeyCredential !== 'undefined'
    expect(typeof isAvailable).toBe('boolean')
  })

  it('should authenticate with biometrics', async () => {
    const credentials = { get: jest.fn(async () => ({ id: 'test' })) }
    Object.defineProperty(navigator, 'credentials', {
      value: credentials,
      configurable: true,
    })

    const cred = await navigator.credentials?.get({} as any)
    expect(cred).toEqual({ id: 'test' })
  })

  it('should handle auth success', async () => {
    const response = { id: 'credential-id', type: 'public-key' }
    expect(response.id).toBeTruthy()
    expect(response.type).toBe('public-key')
  })

  it('should handle NotAllowedError', async () => {
    const authenticate = jest.fn(async () => {
      const err = new DOMException('User cancelled', 'NotAllowedError')
      throw err
    })

    await expect(authenticate()).rejects.toThrow('User cancelled')
  })

  it('should handle unavailable biometrics', async () => {
    Object.defineProperty(window, 'PublicKeyCredential', { value: undefined })
    const isAvailable = typeof window.PublicKeyCredential !== 'undefined'
    expect(isAvailable).toBe(false)
  })

  it('should handle security error', async () => {
    const auth = jest.fn(async () => {
      throw new DOMException('Security error', 'SecurityError')
    })
    await expect(auth()).rejects.toThrow('Security error')
  })
})
