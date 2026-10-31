/**
 * Tests for Capabilities detection module.
 */

describe('Capabilities', () => {
  const g = globalThis as any
  const originalEnv = process.env.NODE_ENV

  beforeEach(() => {
    delete g.HermesInternal
    delete g.__hermes__
    delete g.nativeFabricUIManager
    delete g.__fbRCTBridgeEnabled
    delete g.nativeCallSyncHook
    delete g.__DEV__
    delete g.__turboModuleProxy
    jest.resetModules()
  })

  afterEach(() => {
    delete g.HermesInternal
    delete g.__hermes__
    delete g.nativeFabricUIManager
    delete g.__fbRCTBridgeEnabled
    delete g.nativeCallSyncHook
    delete g.__DEV__
    delete g.__turboModuleProxy
    process.env.NODE_ENV = originalEnv
  })

  it('detects hermes from the documented HermesInternal marker (NF-NR-15)', () => {
    g.HermesInternal = { isHermes: true }
    const { Capabilities } = require('../capabilities')
    expect(Capabilities.hermes).toBe(true)
    delete g.HermesInternal
  })

  it('the undocumented __hermes__ global is not the marker', () => {
    g.__hermes__ = true
    const { Capabilities } = require('../capabilities')
    expect(Capabilities.hermes).toBe(false)
  })

  it('hermes is false when HermesInternal is absent', () => {
    const { Capabilities } = require('../capabilities')
    expect(Capabilities.hermes).toBe(false)
  })

  it('detects fabric from nativeFabricUIManager', () => {
    g.nativeFabricUIManager = {}
    const { Capabilities } = require('../capabilities')
    expect(Capabilities.fabric).toBe(true)
  })

  it('__fbRCTBridgeEnabled === false alone is NOT fabric (diagnostic probes only, NF-NR-15)', () => {
    g.__fbRCTBridgeEnabled = false
    const { Capabilities } = require('../capabilities')
    expect(Capabilities.fabric).toBe(false)
  })

  it('fabric is false when neither marker exists', () => {
    const { Capabilities } = require('../capabilities')
    expect(Capabilities.fabric).toBe(false)
  })

  it('detects jsi from nativeCallSyncHook', () => {
    g.nativeCallSyncHook = () => {}
    const { Capabilities } = require('../capabilities')
    expect(Capabilities.jsi).toBe(true)
  })

  it('jsi is false without nativeCallSyncHook', () => {
    const { Capabilities } = require('../capabilities')
    expect(Capabilities.jsi).toBe(false)
  })

  it('dev is true when __DEV__ is set', () => {
    g.__DEV__ = true
    const { Capabilities } = require('../capabilities')
    expect(Capabilities.dev).toBe(true)
  })

  it('dev is true when NODE_ENV is development', () => {
    process.env.NODE_ENV = 'development'
    const { Capabilities } = require('../capabilities')
    expect(Capabilities.dev).toBe(true)
  })

  it('test is true when NODE_ENV is test', () => {
    process.env.NODE_ENV = 'test'
    const { Capabilities } = require('../capabilities')
    expect(Capabilities.test).toBe(true)
  })

  it('detects turboModules from __turboModuleProxy', () => {
    g.__turboModuleProxy = () => {}
    const { Capabilities } = require('../capabilities')
    expect(Capabilities.turboModules).toBe(true)
  })

  it('turboModules is false without proxy', () => {
    const { Capabilities } = require('../capabilities')
    expect(Capabilities.turboModules).toBe(false)
  })
})
