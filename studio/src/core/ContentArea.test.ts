import { describe, it, expect, beforeEach } from 'vitest'
import { tabs, activeTabId, openTab, activeTab } from '../lib/store'
import type { Tab, TabKind } from '../lib/types'

// Tests the tab-to-module routing logic used by ContentArea.

const ALL_KINDS: TabKind[] = [
  'sql-browser', 'sql-editor', 'schema-designer',
  'kv', 'vector', 'timeseries', 'document', 'graph',
  'fts', 'geo', 'blob', 'pubsub', 'streams',
  'columnar', 'datalog', 'cdc', 'connection-manager',
]

describe('ContentArea routing logic', () => {
  beforeEach(() => {
    tabs.value = []
    activeTabId.value = null
  })

  it('should return null active tab when no tabs exist', () => {
    expect(activeTab.value).toBeNull()
  })

  it('should correctly resolve active tab', () => {
    const tab: Tab = { id: 't1', kind: 'sql-browser', label: 'users', objectSchema: 'public', objectName: 'users' }
    openTab(tab)
    expect(activeTab.value).toEqual(tab)
    expect(activeTab.value!.kind).toBe('sql-browser')
  })

  it('should support all 17 tab kinds', () => {
    expect(ALL_KINDS.length).toBe(17)
    for (const kind of ALL_KINDS) {
      const tab: Tab = { id: `tab-${kind}`, kind, label: kind }
      tabs.value = [tab]
      activeTabId.value = tab.id
      expect(activeTab.value?.kind).toBe(kind)
    }
  })

  it('should keep the correct active tab when multiple tabs exist', () => {
    openTab({ id: 't1', kind: 'sql-browser', label: 'users', objectSchema: 'public', objectName: 'users' })
    openTab({ id: 't2', kind: 'kv', label: 'cache', objectName: 'cache' })
    openTab({ id: 't3', kind: 'vector', label: 'vecs', objectName: 'vecs' })

    // Last opened is active
    expect(activeTab.value?.kind).toBe('vector')

    // Switch to first
    activeTabId.value = 't1'
    expect(activeTab.value?.kind).toBe('sql-browser')
    expect(activeTab.value?.objectName).toBe('users')
  })

  it('should handle tab switch correctly after closing', () => {
    openTab({ id: 't1', kind: 'sql-browser', label: 'a', objectSchema: 'public', objectName: 'a' })
    openTab({ id: 't2', kind: 'graph', label: 'b', objectName: 'b' })
    openTab({ id: 't3', kind: 'fts', label: 'c', objectName: 'c' })

    // Close middle tab (non-active) -- should not affect active
    tabs.value = tabs.value.filter(t => t.id !== 't2')
    expect(activeTab.value?.kind).toBe('fts')
  })
})
