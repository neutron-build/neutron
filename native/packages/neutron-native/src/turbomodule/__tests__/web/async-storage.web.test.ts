import { describe, it, expect, beforeEach } from '@jest/globals'

describe('AsyncStorage web implementation', () => {
  let storage: Map<string, string>

  beforeEach(() => {
    storage = new Map()
  })

  it('should get item', async () => {
    storage.set('key1', 'value1')
    const value = storage.get('key1')
    expect(value).toBe('value1')
  })

  it('should return null for missing key', async () => {
    const value = storage.get('nonexistent')
    expect(value).toBeUndefined()
  })

  it('should set item', async () => {
    storage.set('key1', 'value1')
    expect(storage.get('key1')).toBe('value1')
  })

  it('should remove item', async () => {
    storage.set('key1', 'value1')
    storage.delete('key1')
    expect(storage.get('key1')).toBeUndefined()
  })

  it('should get multiple items', async () => {
    storage.set('key1', 'value1')
    storage.set('key2', 'value2')
    const keys = ['key1', 'key2']
    const values = keys.map(k => storage.get(k))
    expect(values).toEqual(['value1', 'value2'])
  })

  it('should set multiple items', async () => {
    const items = [['key1', 'value1'], ['key2', 'value2']]
    items.forEach(([k, v]) => storage.set(k, v))
    expect(storage.size).toBe(2)
    expect(storage.get('key1')).toBe('value1')
    expect(storage.get('key2')).toBe('value2')
  })

  it('should get all keys', async () => {
    storage.set('key1', 'value1')
    storage.set('key2', 'value2')
    storage.set('key3', 'value3')
    const keys = Array.from(storage.keys())
    expect(keys).toHaveLength(3)
    expect(keys).toContain('key1')
    expect(keys).toContain('key2')
    expect(keys).toContain('key3')
  })

  it('should clear all items', async () => {
    storage.set('key1', 'value1')
    storage.set('key2', 'value2')
    storage.clear()
    expect(storage.size).toBe(0)
    expect(storage.get('key1')).toBeUndefined()
  })

  it('should handle unicode values', async () => {
    const value = '你好世界 مرحبا العالم'
    storage.set('unicode', value)
    expect(storage.get('unicode')).toBe(value)
  })

  it('should handle large values', async () => {
    const largeValue = 'x'.repeat(10000)
    storage.set('large', largeValue)
    expect(storage.get('large')).toBe(largeValue)
  })
})
