/**
 * Tests for accessibility utilities.
 */

describe('Accessibility', () => {
  beforeEach(() => jest.resetModules())

  it('exports announceForAccessibility', () => {
    const mod = require('../index')
    expect(mod.announceForAccessibility).toBeDefined()
    expect(typeof mod.announceForAccessibility).toBe('function')
  })

  it('announceForAccessibility does not throw', () => {
    const { announceForAccessibility } = require('../index')
    expect(() => announceForAccessibility('Test announcement')).not.toThrow()
  })

  it('exports setAccessibilityFocus', () => {
    const mod = require('../index')
    expect(mod.setAccessibilityFocus).toBeDefined()
    expect(typeof mod.setAccessibilityFocus).toBe('function')
  })

  it('exports AccessibleView component', () => {
    const mod = require('../index')
    expect(mod.AccessibleView).toBeDefined()
  })

  it('exports LiveRegion component', () => {
    const mod = require('../index')
    expect(mod.LiveRegion).toBeDefined()
  })

  it('exports FocusTrap component', () => {
    const mod = require('../index')
    expect(mod.FocusTrap).toBeDefined()
  })

  it('exports AccessibilityOrder component', () => {
    const mod = require('../index')
    expect(mod.AccessibilityOrder).toBeDefined()
  })

  it('exports useAccessibility hook', () => {
    const mod = require('../index')
    expect(mod.useAccessibility).toBeDefined()
    expect(typeof mod.useAccessibility).toBe('function')
  })

  it('exports useReducedMotion hook', () => {
    const mod = require('../index')
    expect(mod.useReducedMotion).toBeDefined()
    expect(typeof mod.useReducedMotion).toBe('function')
  })

  it('exports useScreenReader hook', () => {
    const mod = require('../index')
    expect(mod.useScreenReader).toBeDefined()
    expect(typeof mod.useScreenReader).toBe('function')
  })

  it('useScreenReader returns a boolean', () => {
    const { useScreenReader } = require('../index')
    const result = useScreenReader()
    expect(typeof result).toBe('boolean')
  })

  it('useReducedMotion returns a boolean', () => {
    const { useReducedMotion } = require('../index')
    const result = useReducedMotion()
    expect(typeof result).toBe('boolean')
  })
})
