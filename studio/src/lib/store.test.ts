import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import {
  connections, activeConnection, connectionLoading, connectionError,
  features, isNucleus, schema, schemaLoading,
  tabs, activeTabId, activeTab, openTab, closeTab,
  pendingChanges, pendingCount, addPending, removePending, revertLast, clearPending,
  theme, toggleTheme,
  paletteOpen, paletteQuery, openPalette, closePalette,
  toasts, toast,
} from './store'
import type { Tab, PendingChange } from './types'

describe('store — connection state', () => {
  beforeEach(() => {
    connections.value = []
    activeConnection.value = null
    connectionLoading.value = false
    connectionError.value = null
  })

  it('should start with empty connections', () => {
    expect(connections.value).toEqual([])
    expect(activeConnection.value).toBeNull()
  })

  it('should set and read active connection', () => {
    const conn = { id: 'c1', name: 'Test', url: 'pg://test', isNucleus: false }
    activeConnection.value = conn
    expect(activeConnection.value).toEqual(conn)
  })

  it('should track loading and error state', () => {
    connectionLoading.value = true
    expect(connectionLoading.value).toBe(true)

    connectionError.value = 'Failed to connect'
    expect(connectionError.value).toBe('Failed to connect')
  })
})

describe('store — features', () => {
  beforeEach(() => {
    features.value = { isNucleus: false, version: '', models: [] }
  })

  it('should compute isNucleus from features signal', () => {
    expect(isNucleus.value).toBe(false)
    features.value = { isNucleus: true, version: '0.1.0', models: ['sql', 'kv'] }
    expect(isNucleus.value).toBe(true)
  })
})

describe('store — schema', () => {
  beforeEach(() => {
    schema.value = null
    schemaLoading.value = false
  })

  it('should start with null schema', () => {
    expect(schema.value).toBeNull()
  })
})

describe('store — tabs', () => {
  beforeEach(() => {
    tabs.value = []
    activeTabId.value = null
  })

  it('should start with no tabs', () => {
    expect(tabs.value).toEqual([])
    expect(activeTab.value).toBeNull()
  })

  it('should open a new tab', () => {
    const tab: Tab = { id: 't1', kind: 'sql-browser', label: 'users', objectSchema: 'public', objectName: 'users' }
    openTab(tab)
    expect(tabs.value.length).toBe(1)
    expect(activeTabId.value).toBe('t1')
    expect(activeTab.value).toEqual(tab)
  })

  it('should not duplicate tabs with same kind, objectSchema, objectName', () => {
    const tab1: Tab = { id: 't1', kind: 'sql-browser', label: 'users', objectSchema: 'public', objectName: 'users' }
    const tab2: Tab = { id: 't2', kind: 'sql-browser', label: 'users', objectSchema: 'public', objectName: 'users' }
    openTab(tab1)
    openTab(tab2)
    expect(tabs.value.length).toBe(1)
    // Should activate the existing tab
    expect(activeTabId.value).toBe('t1')
  })

  it('should allow tabs with different objectName', () => {
    const tab1: Tab = { id: 't1', kind: 'sql-browser', label: 'users', objectSchema: 'public', objectName: 'users' }
    const tab2: Tab = { id: 't2', kind: 'sql-browser', label: 'posts', objectSchema: 'public', objectName: 'posts' }
    openTab(tab1)
    openTab(tab2)
    expect(tabs.value.length).toBe(2)
    expect(activeTabId.value).toBe('t2')
  })

  it('should close a tab', () => {
    const tab1: Tab = { id: 't1', kind: 'sql-browser', label: 'users', objectSchema: 'public', objectName: 'users' }
    const tab2: Tab = { id: 't2', kind: 'kv', label: 'cache', objectName: 'cache' }
    openTab(tab1)
    openTab(tab2)
    expect(tabs.value.length).toBe(2)
    expect(activeTabId.value).toBe('t2')

    closeTab('t2')
    expect(tabs.value.length).toBe(1)
    // Should activate the previous tab
    expect(activeTabId.value).toBe('t1')
  })

  it('should handle closing the last tab', () => {
    const tab: Tab = { id: 't1', kind: 'sql-browser', label: 'users' }
    openTab(tab)
    closeTab('t1')
    expect(tabs.value.length).toBe(0)
    expect(activeTabId.value).toBeNull()
  })

  it('should not change activeTabId when closing a non-active tab', () => {
    const tab1: Tab = { id: 't1', kind: 'sql-browser', label: 'users' }
    const tab2: Tab = { id: 't2', kind: 'kv', label: 'cache', objectName: 'cache' }
    const tab3: Tab = { id: 't3', kind: 'vector', label: 'embeddings', objectName: 'embeddings' }
    openTab(tab1)
    openTab(tab2)
    openTab(tab3)

    // t3 is active, close t1
    closeTab('t1')
    expect(activeTabId.value).toBe('t3')
    expect(tabs.value.length).toBe(2)
  })
})

describe('store — pending changes', () => {
  beforeEach(() => {
    pendingChanges.value = []
  })

  it('should start with zero pending changes', () => {
    expect(pendingCount.value).toBe(0)
  })

  it('should add pending changes', () => {
    const change: PendingChange = {
      id: 'p1',
      model: 'sql',
      label: 'test change',
      sql: "UPDATE x SET y = 1",
      revert: () => {},
    }
    addPending(change)
    expect(pendingCount.value).toBe(1)
    expect(pendingChanges.value[0].id).toBe('p1')
  })

  it('should remove a pending change by id', () => {
    addPending({ id: 'p1', model: 'sql', label: 'a', sql: 'a', revert: () => {} })
    addPending({ id: 'p2', model: 'sql', label: 'b', sql: 'b', revert: () => {} })
    expect(pendingCount.value).toBe(2)

    removePending('p1')
    expect(pendingCount.value).toBe(1)
    expect(pendingChanges.value[0].id).toBe('p2')
  })

  it('should revert the last pending change', () => {
    let revertCalled = false
    addPending({ id: 'p1', model: 'sql', label: 'a', sql: 'a', revert: () => {} })
    addPending({ id: 'p2', model: 'sql', label: 'b', sql: 'b', revert: () => { revertCalled = true } })

    revertLast()
    expect(revertCalled).toBe(true)
    expect(pendingCount.value).toBe(1)
    expect(pendingChanges.value[0].id).toBe('p1')
  })

  it('should do nothing when reverting with no changes', () => {
    revertLast()
    expect(pendingCount.value).toBe(0)
  })

  it('should clear all pending changes', () => {
    addPending({ id: 'p1', model: 'sql', label: 'a', sql: 'a', revert: () => {} })
    addPending({ id: 'p2', model: 'sql', label: 'b', sql: 'b', revert: () => {} })
    clearPending()
    expect(pendingCount.value).toBe(0)
  })
})

describe('store — theme', () => {
  beforeEach(() => {
    theme.value = 'dark'
  })

  it('should toggle theme from dark to light', () => {
    expect(theme.value).toBe('dark')
    toggleTheme()
    expect(theme.value).toBe('light')
  })

  it('should toggle theme from light to dark', () => {
    theme.value = 'light'
    toggleTheme()
    expect(theme.value).toBe('dark')
  })
})

describe('store — command palette', () => {
  beforeEach(() => {
    paletteOpen.value = false
    paletteQuery.value = ''
  })

  it('should open palette and clear query', () => {
    paletteQuery.value = 'old search'
    openPalette()
    expect(paletteOpen.value).toBe(true)
    expect(paletteQuery.value).toBe('')
  })

  it('should close palette', () => {
    openPalette()
    closePalette()
    expect(paletteOpen.value).toBe(false)
  })
})

describe('store — toast notifications', () => {
  beforeEach(() => {
    toasts.value = []
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('should add a toast', () => {
    toast('success', 'Changes saved')
    expect(toasts.value.length).toBe(1)
    expect(toasts.value[0].kind).toBe('success')
    expect(toasts.value[0].message).toBe('Changes saved')
  })

  it('should add multiple toasts', () => {
    toast('info', 'Info message')
    toast('error', 'Error message')
    expect(toasts.value.length).toBe(2)
  })

  it('should auto-remove toast after 4 seconds', () => {
    toast('success', 'Will disappear')
    expect(toasts.value.length).toBe(1)

    vi.advanceTimersByTime(4000)
    expect(toasts.value.length).toBe(0)
  })

  it('should not remove other toasts when one expires', () => {
    toast('info', 'First')
    vi.advanceTimersByTime(2000)
    toast('info', 'Second')
    expect(toasts.value.length).toBe(2)

    // First toast disappears at t=4000
    vi.advanceTimersByTime(2000)
    expect(toasts.value.length).toBe(1)
    expect(toasts.value[0].message).toBe('Second')
  })
})
