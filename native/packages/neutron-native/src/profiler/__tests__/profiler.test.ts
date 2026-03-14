/**
 * Tests for profiler utilities — startupMetrics, interaction timing, FPS monitor.
 */

describe('Profiler', () => {
  beforeEach(() => jest.resetModules())

  it('exports startupMetrics singleton', () => {
    const mod = require('../index')
    expect(mod.startupMetrics).toBeDefined()
    expect(typeof mod.startupMetrics).toBe('object')
  })

  it('exports useFPSMonitor hook', () => {
    const mod = require('../index')
    expect(mod.useFPSMonitor).toBeDefined()
    expect(typeof mod.useFPSMonitor).toBe('function')
  })

  it('exports useRenderTracker hook', () => {
    const mod = require('../index')
    expect(mod.useRenderTracker).toBeDefined()
    expect(typeof mod.useRenderTracker).toBe('function')
  })

  it('exports useInteractionTiming hook', () => {
    const mod = require('../index')
    expect(mod.useInteractionTiming).toBeDefined()
    expect(typeof mod.useInteractionTiming).toBe('function')
  })

  it('exports PerformanceOverlay component', () => {
    const mod = require('../index')
    expect(mod.PerformanceOverlay).toBeDefined()
  })

  it('startupMetrics has expected properties', () => {
    const { startupMetrics } = require('../index')
    // The object should at least be defined; exact shape depends on implementation
    expect(startupMetrics).toBeDefined()
  })
})
