/**
 * Tests for Platform detection module.
 */

describe('Platform', () => {
  const g = globalThis as any

  beforeEach(() => {
    // Clean up any globals
    delete g.Platform
    delete g.__hermes__
    jest.resetModules()
  })

  afterEach(() => {
    delete g.Platform
    delete g.__hermes__
  })

  it('detects ios from globalThis.Platform.OS', () => {
    g.Platform = { OS: 'ios', Version: '18.0' }
    const { Platform } = require('../index')
    expect(Platform.OS).toBe('ios')
    expect(Platform.isIOS).toBe(true)
    expect(Platform.isAndroid).toBe(false)
    expect(Platform.isWeb).toBe(false)
    expect(Platform.isNative).toBe(true)
  })

  it('detects android from globalThis.Platform.OS', () => {
    g.Platform = { OS: 'android', Version: 33 }
    const { Platform } = require('../index')
    expect(Platform.OS).toBe('android')
    expect(Platform.isAndroid).toBe(true)
    expect(Platform.isIOS).toBe(false)
    expect(Platform.isNative).toBe(true)
  })

  it('detects ios from __hermes__ + iPhone user agent', () => {
    g.__hermes__ = true
    g.navigator = { userAgent: 'Mozilla/5.0 (iPhone; CPU iPhone OS 18_0)' }
    const { Platform } = require('../index')
    expect(Platform.OS).toBe('ios')
    delete g.navigator
  })

  it('detects ios from __hermes__ + iPad user agent', () => {
    g.__hermes__ = true
    g.navigator = { userAgent: 'Mozilla/5.0 (iPad; CPU OS 18_0)' }
    const { Platform } = require('../index')
    expect(Platform.OS).toBe('ios')
    delete g.navigator
  })

  it('defaults to android from __hermes__ without matching user agent', () => {
    g.__hermes__ = true
    g.navigator = { userAgent: 'SomeOtherAgent' }
    const { Platform } = require('../index')
    expect(Platform.OS).toBe('android')
    delete g.navigator
  })

  it('falls back to android when nothing else matches', () => {
    // No Platform, no __hermes__, no document/window
    const { Platform } = require('../index')
    // In test environment, document and window may be undefined -> android fallback
    expect(['android', 'web']).toContain(Platform.OS)
  })

  it('select() returns value for current platform', () => {
    g.Platform = { OS: 'ios', Version: '18.0' }
    const { Platform } = require('../index')
    const result = Platform.select({ ios: 'SF Pro', android: 'Roboto', default: 'sans-serif' })
    expect(result).toBe('SF Pro')
  })

  it('select() falls back to default when platform not specified', () => {
    g.Platform = { OS: 'macos', Version: '14.0' }
    const { Platform } = require('../index')
    const result = Platform.select({ ios: 'SF Pro', default: 'system-ui' })
    expect(result).toBe('system-ui')
  })

  it('reads Version from globalThis.Platform', () => {
    g.Platform = { OS: 'ios', Version: '18.2' }
    const { Platform } = require('../index')
    expect(Platform.Version).toBe('18.2')
  })

  it('defaults Version to 0 when not set', () => {
    // No global Platform
    const { Platform } = require('../index')
    expect(Platform.Version).toBe(0)
  })
})
