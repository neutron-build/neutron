/**
 * Tests for TurboModule registry.
 */

describe('TurboModule Registry', () => {
  beforeEach(() => {
    jest.resetModules()
    const g = globalThis as any
    delete g.__turboModuleProxy
    delete g.TurboModuleRegistry
    // Make sure we're not in web environment for most tests
    delete (globalThis as any).document
  })

  afterEach(() => {
    const g = globalThis as any
    delete g.__turboModuleProxy
    delete g.TurboModuleRegistry
  })

  it('registerModule and getModule with a factory', () => {
    const registry = require('../registry')
    const mockModule = { __turboModuleType: 'test' as any }
    registry.registerModule('TestModule', () => mockModule)
    const result = registry.getModule('TestModule')
    expect(result).toBe(mockModule)
  })

  it('getModule returns null for unknown modules', () => {
    const registry = require('../registry')
    expect(registry.getModule('NonExistent')).toBeNull()
  })

  it('requireModule throws for unknown modules', () => {
    const registry = require('../registry')
    expect(() => registry.requireModule('Missing')).toThrow(
      '[neutron-native] TurboModule "Missing" is not available'
    )
  })

  it('caches modules after first resolution', () => {
    const registry = require('../registry')
    let callCount = 0
    registry.registerModule('Counted', () => {
      callCount++
      return { __turboModuleType: 'counted' as any }
    })
    registry.getModule('Counted')
    registry.getModule('Counted')
    expect(callCount).toBe(1)
  })

  it('clearCache forces re-resolution', () => {
    const registry = require('../registry')
    let callCount = 0
    registry.registerModule('Reclear', () => {
      callCount++
      return { __turboModuleType: 'reclear' as any }
    })
    registry.getModule('Reclear')
    registry.clearCache()
    registry.getModule('Reclear')
    expect(callCount).toBe(2)
  })

  it('hasModule returns true for registered modules', () => {
    const registry = require('../registry')
    registry.registerModule('Present', () => ({ __turboModuleType: 'p' as any }))
    expect(registry.hasModule('Present')).toBe(true)
    expect(registry.hasModule('Absent')).toBe(false)
  })

  it('listModules lists all registered module names', () => {
    const registry = require('../registry')
    registry.clearCache()
    registry.registerModule('ModA', () => ({ __turboModuleType: 'a' as any }))
    registry.registerModule('ModB', () => ({ __turboModuleType: 'b' as any }))
    const list = registry.listModules()
    expect(list).toContain('ModA')
    expect(list).toContain('ModB')
  })

  it('prioritizes native JSI registry over JS factory', () => {
    const g = globalThis as any
    const nativeModule = { __turboModuleType: 'native' as any }
    g.__turboModuleProxy = {
      get: (name: string) => name === 'NativeMod' ? nativeModule : null,
    }

    const registry = require('../registry')
    registry.registerModule('NativeMod', () => ({ __turboModuleType: 'js' as any }))
    const result = registry.getModule('NativeMod')
    expect(result).toBe(nativeModule)
  })

  it('registerWebModule provides web fallback', () => {
    // Simulate web environment
    const g = globalThis as any
    const origDoc = g.document
    const origWin = g.window
    const origNav = g.navigator
    g.document = {}
    g.window = {}
    g.navigator = { product: 'Gecko' }

    jest.resetModules()
    const registry = require('../registry')
    const webMod = { __turboModuleType: 'web' as any }
    registry.registerWebModule('WebMod', () => webMod)
    expect(registry.getModule('WebMod')).toBe(webMod)

    // Restore
    if (origDoc === undefined) delete g.document
    else g.document = origDoc
    if (origWin === undefined) delete g.window
    else g.window = origWin
    if (origNav === undefined) delete g.navigator
    else g.navigator = origNav
  })

  it('isWeb returns false in non-browser environment', () => {
    const registry = require('../registry')
    // In test env without document/window, should be false
    const g = globalThis as any
    const origDoc = g.document
    const origWin = g.window
    delete g.document
    delete g.window
    jest.resetModules()
    const reg2 = require('../registry')
    expect(reg2.isWeb()).toBe(false)
    // Restore
    if (origDoc !== undefined) g.document = origDoc
    if (origWin !== undefined) g.window = origWin
  })

  it('hasModule checks native registry too', () => {
    const g = globalThis as any
    g.__turboModuleProxy = {
      get: (name: string) => name === 'NativeOnly' ? { __turboModuleType: 'x' as any } : null,
    }
    const registry = require('../registry')
    expect(registry.hasModule('NativeOnly')).toBe(true)
  })
})
