/**
 * Tests for device API modules.
 * Each module lazily loads peer dependencies and falls back to stubs.
 */

describe('Device module barrel exports', () => {
  beforeEach(() => jest.resetModules())

  it('exports camera functions', () => {
    const mod = require('../camera')
    expect(mod.takePicture).toBeDefined()
    expect(mod.pickFromGallery).toBeDefined()
    expect(mod.isCameraAvailable).toBeDefined()
    expect(mod.requestCameraPermission).toBeDefined()
  })

  it('exports haptics functions', () => {
    const mod = require('../haptics')
    expect(mod.impact).toBeDefined()
    expect(mod.notification).toBeDefined()
    expect(mod.selection).toBeDefined()
    expect(mod.vibrate).toBeDefined()
    expect(mod.cancel).toBeDefined()
    expect(mod.isAvailable).toBeDefined()
  })

  it('exports clipboard functions', () => {
    const mod = require('../clipboard')
    expect(mod.getString).toBeDefined()
    expect(mod.setString).toBeDefined()
    expect(mod.hasString).toBeDefined()
  })

  it('exports async-storage functions', () => {
    const mod = require('../async-storage')
    expect(mod.getItem).toBeDefined()
    expect(mod.setItem).toBeDefined()
    expect(mod.removeItem).toBeDefined()
    expect(mod.getAllKeys).toBeDefined()
    expect(mod.clear).toBeDefined()
  })

  it('exports permissions functions', () => {
    const mod = require('../permissions')
    expect(mod.check).toBeDefined()
    expect(mod.request).toBeDefined()
    expect(mod.openSettings).toBeDefined()
  })
})

describe('Device async-storage (in-memory fallback)', () => {
  // Each test gets a fresh module to avoid shared state
  it('setItem and getItem round-trip', async () => {
    jest.resetModules()
    const storage = require('../async-storage')
    await storage.setItem('test-key', 'test-value')
    const result = await storage.getItem('test-key')
    expect(result).toBe('test-value')
  })

  it('getItem returns null for missing keys', async () => {
    jest.resetModules()
    const storage = require('../async-storage')
    const result = await storage.getItem('nonexistent')
    expect(result).toBeNull()
  })

  it('removeItem deletes a key', async () => {
    jest.resetModules()
    const storage = require('../async-storage')
    await storage.setItem('to-delete', 'value')
    await storage.removeItem('to-delete')
    const result = await storage.getItem('to-delete')
    expect(result).toBeNull()
  })

  it('clear removes all keys', async () => {
    jest.resetModules()
    const storage = require('../async-storage')
    await storage.setItem('a', '1')
    await storage.setItem('b', '2')
    await storage.clear()
    expect(await storage.getItem('a')).toBeNull()
    expect(await storage.getItem('b')).toBeNull()
  })

  it('getAllKeys returns stored keys', async () => {
    jest.resetModules()
    const storage = require('../async-storage')
    await storage.setItem('k1', 'v1')
    await storage.setItem('k2', 'v2')
    const keys = await storage.getAllKeys()
    expect(keys).toContain('k1')
    expect(keys).toContain('k2')
  })

  it('multiGet returns multiple values', async () => {
    jest.resetModules()
    const storage = require('../async-storage')
    await storage.setItem('mg1', 'val1')
    await storage.setItem('mg2', 'val2')
    const results = await storage.multiGet(['mg1', 'mg2', 'mg3'])
    expect(results).toEqual([
      ['mg1', 'val1'],
      ['mg2', 'val2'],
      ['mg3', null],
    ])
  })

  it('multiSet sets multiple values', async () => {
    jest.resetModules()
    const storage = require('../async-storage')
    await storage.multiSet([['ms1', 'v1'], ['ms2', 'v2']])
    expect(await storage.getItem('ms1')).toBe('v1')
    expect(await storage.getItem('ms2')).toBe('v2')
  })

  it('multiRemove removes multiple keys', async () => {
    jest.resetModules()
    const storage = require('../async-storage')
    await storage.setItem('mr1', 'v1')
    await storage.setItem('mr2', 'v2')
    await storage.multiRemove(['mr1', 'mr2'])
    expect(await storage.getItem('mr1')).toBeNull()
    expect(await storage.getItem('mr2')).toBeNull()
  })

  it('mergeItem merges JSON objects', async () => {
    jest.resetModules()
    const storage = require('../async-storage')
    await storage.setItem('merge', JSON.stringify({ a: 1 }))
    await storage.mergeItem('merge', JSON.stringify({ b: 2 }))
    const result = JSON.parse(await storage.getItem('merge'))
    expect(result).toEqual({ a: 1, b: 2 })
  })

  it('mergeItem creates key if it does not exist', async () => {
    jest.resetModules()
    const storage = require('../async-storage')
    await storage.mergeItem('new-merge', JSON.stringify({ x: 99 }))
    const result = JSON.parse(await storage.getItem('new-merge'))
    expect(result).toEqual({ x: 99 })
  })
})

describe('Device haptics (fallback)', () => {
  beforeEach(() => jest.resetModules())

  it('impact does not throw', async () => {
    const { impact } = require('../haptics')
    await expect(impact('medium')).resolves.not.toThrow()
  })

  it('notification does not throw', async () => {
    const { notification } = require('../haptics')
    await expect(notification('success')).resolves.not.toThrow()
  })

  it('selection does not throw', async () => {
    const { selection } = require('../haptics')
    await expect(selection()).resolves.not.toThrow()
  })

  it('vibrate delegates to Vibration', () => {
    const { vibrate } = require('../haptics')
    expect(() => vibrate()).not.toThrow()
  })

  it('cancel does not throw', () => {
    const { cancel } = require('../haptics')
    expect(() => cancel()).not.toThrow()
  })
})

describe('Device permissions (fallback)', () => {
  beforeEach(() => jest.resetModules())

  it('check returns a status', async () => {
    const { check } = require('../permissions')
    const result = await check('camera')
    expect(result).toBeDefined()
  })

  it('request returns a status', async () => {
    const { request } = require('../permissions')
    const result = await request('camera')
    expect(result).toBeDefined()
  })
})
