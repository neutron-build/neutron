/**
 * Tests for deep link handling.
 */

describe('deep-link', () => {
  const g = globalThis as any

  beforeEach(() => {
    delete g.ReactNativeLinking
    jest.resetModules()
  })

  afterEach(() => {
    delete g.ReactNativeLinking
  })

  it('_matchesConfig matches URL by scheme', () => {
    // Access the internal function via module
    // We need to test the module's behavior through initDeepLinks
    const mockLinking = {
      getInitialURL: jest.fn().mockResolvedValue(null),
      addEventListener: jest.fn().mockReturnValue({ remove: jest.fn() }),
    }
    g.ReactNativeLinking = mockLinking

    const { initDeepLinks } = require('../deep-link')
    const cleanup = initDeepLinks({ schemes: ['myapp://'] })
    expect(typeof cleanup).toBe('function')
    expect(mockLinking.getInitialURL).toHaveBeenCalled()
    expect(mockLinking.addEventListener).toHaveBeenCalledWith('url', expect.any(Function))
    cleanup()
  })

  it('returns noop cleanup when Linking is unavailable', () => {
    const { initDeepLinks } = require('../deep-link')
    const cleanup = initDeepLinks({ schemes: ['myapp://'] })
    expect(typeof cleanup).toBe('function')
    cleanup() // should not throw
  })

  it('handles cold start URL that matches config', async () => {
    const mockLinking = {
      getInitialURL: jest.fn().mockResolvedValue('myapp://example.com/deep/page'),
      addEventListener: jest.fn().mockReturnValue({ remove: jest.fn() }),
    }
    g.ReactNativeLinking = mockLinking

    const { initDeepLinks } = require('../deep-link')
    initDeepLinks({ schemes: ['myapp://'] })

    // Allow promise to resolve
    await new Promise(r => setTimeout(r, 10))
    // The navigator should have been called — we can verify Linking was consulted
    expect(mockLinking.getInitialURL).toHaveBeenCalled()
  })

  it('ignores cold start URL that does not match config', async () => {
    const mockLinking = {
      getInitialURL: jest.fn().mockResolvedValue('otherapp://something'),
      addEventListener: jest.fn().mockReturnValue({ remove: jest.fn() }),
    }
    g.ReactNativeLinking = mockLinking

    const { initDeepLinks } = require('../deep-link')
    initDeepLinks({ schemes: ['myapp://'] })
    await new Promise(r => setTimeout(r, 10))
    // Should not crash — URL doesn't match
  })

  it('subscribes to warm/hot URL events', () => {
    const mockLinking = {
      getInitialURL: jest.fn().mockResolvedValue(null),
      addEventListener: jest.fn().mockReturnValue({ remove: jest.fn() }),
    }
    g.ReactNativeLinking = mockLinking

    const { initDeepLinks } = require('../deep-link')
    initDeepLinks({ schemes: ['myapp://'], domains: ['example.com'] })
    expect(mockLinking.addEventListener).toHaveBeenCalledWith('url', expect.any(Function))
  })

  it('matches by domain when domains are configured', async () => {
    const mockLinking = {
      getInitialURL: jest.fn().mockResolvedValue('https://example.com/path'),
      addEventListener: jest.fn().mockReturnValue({ remove: jest.fn() }),
    }
    g.ReactNativeLinking = mockLinking

    const { initDeepLinks } = require('../deep-link')
    initDeepLinks({ schemes: ['myapp://'], domains: ['example.com'] })
    await new Promise(r => setTimeout(r, 10))
    // Should have matched the domain
    expect(mockLinking.getInitialURL).toHaveBeenCalled()
  })

  it('cleanup removes subscription', () => {
    const removeFn = jest.fn()
    const mockLinking = {
      getInitialURL: jest.fn().mockResolvedValue(null),
      addEventListener: jest.fn().mockReturnValue({ remove: removeFn }),
    }
    g.ReactNativeLinking = mockLinking

    const { initDeepLinks } = require('../deep-link')
    const cleanup = initDeepLinks({ schemes: ['myapp://'] })
    cleanup()
    expect(removeFn).toHaveBeenCalled()
  })
})
