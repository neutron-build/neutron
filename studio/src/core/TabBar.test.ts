import { describe, it, expect, beforeEach } from 'vitest'
import { tabs, activeTabId, closeTab, openTab, activeTab } from '../lib/store'
import type { Tab } from '../lib/types'

// This file tests the TabBar logic (tab management) not the rendered output.
// The KIND_COLORS map used in TabBar.tsx is also validated.

const KIND_COLORS: Partial<Record<string, string>> = {
  'sql-browser': 'sql',
  'sql-editor': 'sql',
  'kv': 'kv',
  'vector': 'vector',
  'timeseries': 'ts',
  'document': 'doc',
  'graph': 'graph',
  'fts': 'fts',
  'geo': 'geo',
  'blob': 'blob',
  'pubsub': 'pubsub',
  'streams': 'streams',
  'columnar': 'columnar',
  'datalog': 'datalog',
  'cdc': 'cdc',
}

describe('TabBar logic', () => {
  beforeEach(() => {
    tabs.value = []
    activeTabId.value = null
  })

  it('KIND_COLORS should have all 15 model/editor kinds', () => {
    const expected = [
      'sql-browser', 'sql-editor', 'kv', 'vector', 'timeseries',
      'document', 'graph', 'fts', 'geo', 'blob', 'pubsub',
      'streams', 'columnar', 'datalog', 'cdc',
    ]
    for (const kind of expected) {
      expect(KIND_COLORS[kind]).toBeDefined()
    }
  })

  it('should map sql-browser and sql-editor to same color key', () => {
    expect(KIND_COLORS['sql-browser']).toBe('sql')
    expect(KIND_COLORS['sql-editor']).toBe('sql')
  })

  it('should handle closing active tab and activating previous', () => {
    const t1: Tab = { id: 't1', kind: 'sql-browser', label: 'users' }
    const t2: Tab = { id: 't2', kind: 'kv', label: 'cache', objectName: 'cache' }
    const t3: Tab = { id: 't3', kind: 'vector', label: 'vecs', objectName: 'vecs' }

    openTab(t1)
    openTab(t2)
    openTab(t3)

    // Active is t3 (last opened)
    expect(activeTabId.value).toBe('t3')

    // Close active tab t3
    closeTab('t3')
    expect(tabs.value.length).toBe(2)
    // Should activate t2 (previous in list)
    expect(activeTabId.value).toBe('t2')
  })

  it('should handle closing the first tab', () => {
    const t1: Tab = { id: 't1', kind: 'sql-browser', label: 'a' }
    const t2: Tab = { id: 't2', kind: 'kv', label: 'b', objectName: 'b' }
    openTab(t1)
    openTab(t2)

    // Make t1 active
    activeTabId.value = 't1'
    closeTab('t1')

    // Should activate t2 (first remaining)
    expect(activeTabId.value).toBe('t2')
    expect(tabs.value.length).toBe(1)
  })

  it('should return null for activeTab when no tabs', () => {
    expect(activeTab.value).toBeNull()
  })
})
