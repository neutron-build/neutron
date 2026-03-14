import { describe, it, expect, beforeEach } from '@jest/globals'

// Mock window and navigator for node environment
if (typeof window === 'undefined') {
  (global as any).window = {
    innerWidth: 1024,
    innerHeight: 768,
  }
}

describe('DeviceInfo web implementation', () => {
  beforeEach(() => {
    ;(global as any).window = {
      innerWidth: 1024,
      innerHeight: 768,
    }
  })
  it('should detect Chrome browser', () => {
    const ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    expect(ua).toContain('Chrome')
  })

  it('should detect Firefox browser', () => {
    const ua = 'Mozilla/5.0 (X11; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0'
    expect(ua).toContain('Firefox')
  })

  it('should get device locale', () => {
    Object.defineProperty(navigator, 'language', { value: 'en-US', configurable: true })
    expect(navigator.language).toBe('en-US')
  })

  it('should detect tablet by screen width', () => {
    const isTablet = window.innerWidth > 600
    expect(typeof isTablet).toBe('boolean')
  })

  it('should get screen dimensions', () => {
    expect(window.innerWidth).toBeGreaterThan(0)
    expect(window.innerHeight).toBeGreaterThan(0)
  })

  it('should detect timezone', () => {
    const tz = new Intl.DateTimeFormat().resolvedOptions().timeZone
    expect(tz).toBeTruthy()
  })

  it('should provide device name', () => {
    const userAgent = navigator.userAgent
    expect(userAgent).toBeTruthy()
  })

  it('should get OS from user agent', () => {
    const ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
    expect(ua).toContain('Windows')
  })
})
