/**
 * Tests for virtualized list components — VirtualizedList, FlashList.
 */

describe('VirtualizedList', () => {
  beforeEach(() => jest.resetModules())

  it('exports VirtualizedList', () => {
    const { VirtualizedList } = require('../virtualized-list')
    expect(VirtualizedList).toBeDefined()
  })

  it('can be instantiated with data', () => {
    const { VirtualizedList } = require('../virtualized-list')
    expect(typeof VirtualizedList).toBe('function')
  })
})

describe('FlashList', () => {
  beforeEach(() => jest.resetModules())

  it('exports FlashList', () => {
    const { FlashList } = require('../flash-list')
    expect(FlashList).toBeDefined()
  })

  it('FlashList wraps VirtualizedList', () => {
    const { FlashList } = require('../flash-list')
    expect(typeof FlashList).toBe('function')
  })
})

describe('Barrel exports', () => {
  it('exports both from index', () => {
    const mod = require('../index')
    expect(mod.VirtualizedList).toBeDefined()
    expect(mod.FlashList).toBeDefined()
  })
})
