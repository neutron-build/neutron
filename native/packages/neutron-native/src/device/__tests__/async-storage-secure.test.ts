/**
 * NF-NR-07 regressions — SecureStore fallback: bulk listing/clear stay
 * consistent with what was actually persisted, and clear never claims
 * success while persistent keys remain.
 */

jest.mock('expo-secure-store', () => {
  const store = new Map<string, string>()
  return {
    __esModule: true,
    getItemAsync: jest.fn(async (k: string) => store.get(k) ?? null),
    setItemAsync: jest.fn(async (k: string, v: string) => { store.set(k, v) }),
    deleteItemAsync: jest.fn(async (k: string) => { store.delete(k) }),
    __store: store,
  }
}, { virtual: true })

// No AsyncStorage: the SecureStore backend is selected.
jest.mock('@react-native-async-storage/async-storage', () => {
  throw new Error('backend disabled for this suite')
}, { virtual: true })

import { setItem, getItem, getAllKeys, clear, removeItem, importLegacySecureKeys, IncompleteClearError } from '../async-storage'
// @ts-expect-error virtual mock internals
import * as secureStoreModule from 'expo-secure-store'

export {}

const backend: Map<string, string> = (secureStoreModule as any).__store

describe('NF-NR-07 SecureStore index consistency', () => {
  beforeEach(() => {
    backend.clear()
  })

  it('write/getAllKeys/clear are consistent through the same backend', async () => {
    await setItem('auth.token', 'secret')
    await setItem('prefs.theme', 'dark')
    expect(await getItem('auth.token')).toBe('secret')
    expect(await getAllKeys()).toEqual(['auth.token', 'prefs.theme'])
    await clear()
    expect(await getAllKeys()).toEqual([])
    // The clear actually removed the persisted values, not a memory shadow.
    expect(backend.has('auth.token')).toBe(false)
    expect(await getItem('auth.token')).toBeNull()
  })

  it('a fresh module instance (new process equivalent) cannot read cleared keys', async () => {
    await setItem('session', 'data')
    await clear()
    // Simulate a restart: drop all in-module state by reading through the
    // backend directly.
    expect(backend.has('session')).toBe(false)
    expect(await getItem('session')).toBeNull()
  })

  it('partial delete failures reject with remaining-key information', async () => {
    await setItem('a', '1')
    await setItem('b', '2')
    const del = (secureStoreModule as any).deleteItemAsync as jest.Mock
    del.mockImplementationOnce(async (k: string) => {
      if (k === 'a') throw new Error('keychain locked')
      backend.delete(k)
    })
    const failure: Promise<void> = clear()
    await expect(failure).rejects.toBeInstanceOf(IncompleteClearError)
    await expect(failure).rejects.toThrow(/1 persistent key/)
    // The failed key stays indexed for a retry; the successful one is gone.
    expect(await getAllKeys()).toEqual(['a'])
    expect(backend.has('b')).toBe(false)
  })

  it('removeItem keeps the index accurate', async () => {
    await setItem('x', '1')
    await setItem('y', '2')
    await removeItem('x')
    expect(await getAllKeys()).toEqual(['y'])
  })

  it('legacy unindexed keys merge explicitly', async () => {
    backend.set('legacy.key', 'old')  // written before the index existed
    expect(await getAllKeys()).toEqual([])
    await importLegacySecureKeys(['legacy.key'])
    expect(await getAllKeys()).toEqual(['legacy.key'])
    await clear()
    expect(backend.has('legacy.key')).toBe(false)
  })
})
