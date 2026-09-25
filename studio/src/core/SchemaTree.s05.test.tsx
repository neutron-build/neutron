import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen, cleanup, fireEvent } from '@testing-library/preact'

// S05 schema navigation: the tree lists views alongside tables, search
// filters across schemas, the refresh button drives the REAL refresh
// (updating the schema signal), and deep links round-trip through the
// router. Only the fetch boundary (lib/api) is mocked; backend behavior is
// covered by the Go E2E leg.

vi.mock('../lib/api', async (importOriginal) => {
  const orig = await importOriginal<typeof import('../lib/api')>()
  return {
    ...orig,
    api: {
      ...orig.api,
      schema: vi.fn(),
    },
  }
})

import { api } from '../lib/api'
import { schema, features, activeConnection, refreshSchema, openTab, tabs, activeTabId } from '../lib/store'
import { parseDeepLink, serializeDeepLink } from '../lib/router'
import { SchemaTree } from './SchemaTree'
import type { Schema } from '../lib/types'

const schemaMock = vi.mocked(api.schema)

function baseSchema(): Schema {
  return {
    sql: [
      { schema: 'public', name: 'users', columns: [] },
      { schema: 'public', name: 'orders', columns: [] },
      { schema: 'analytics', name: 'events', columns: [] },
    ],
    views: [
      { schema: 'public', name: 'active_users' },
      { schema: 'analytics', name: 'MixedCase View' },
    ],
    kv: [], vector: [], timeseries: [], document: [], graph: [], fts: [],
    geo: [], blob: [], pubsub: [], streams: [], columnar: [], datalog: null, cdc: false,
  }
}

beforeEach(() => {
  cleanup()
  schema.value = baseSchema()
  features.value = { isNucleus: false, version: '', models: ['sql'] }
  activeConnection.value = { id: 'c1', name: 'test', url: 'postgres://x', isNucleus: false }
  tabs.value = []
  activeTabId.value = null
  window.location.hash = ''
})

afterEach(() => {
  cleanup()
  vi.clearAllMocks()
})

describe('SchemaTree navigation (S05)', () => {
  it('lists tables and views, and marks views', () => {
    render(<SchemaTree />)
    expect(screen.getByText('users')).toBeTruthy()
    expect(screen.getByText('orders')).toBeTruthy()
    expect(screen.getByText('events')).toBeTruthy()
    expect(screen.getByText('active_users')).toBeTruthy()
    expect(screen.getByText('MixedCase View')).toBeTruthy()
    // Views carry the view badge; tables carry an inspect button instead.
    const viewItem = screen.getByText('active_users').closest('button')!
    expect(viewItem.querySelector('[title*="view (browsed read-only)"]')).toBeTruthy()
    expect(screen.getByLabelText('Inspect structure of users')).toBeTruthy()
    expect(screen.queryByLabelText('Inspect structure of active_users')).toBeNull()
  })

  it('search filters tables and views by name across schemas', () => {
    render(<SchemaTree />)
    const search = screen.getByLabelText('Search schema objects') as HTMLInputElement
    fireEvent.input(search, { target: { value: 'ACTIVE' } })
    expect(screen.getByText('active_users')).toBeTruthy()
    expect(screen.getByText('1 match(es)')).toBeTruthy() // active_users + MixedCase View
    expect(screen.queryByText('users')).toBeNull()
    expect(screen.queryByText('orders')).toBeNull()

    fireEvent.input(search, { target: { value: 'analytics.' } })
    expect(screen.getByText('events')).toBeTruthy()
    expect(screen.getByText('MixedCase View')).toBeTruthy()
    expect(screen.queryByText('active_users')).toBeNull()

    fireEvent.input(search, { target: { value: 'zzz-nothing' } })
    expect(screen.getByText(/No objects match/)).toBeTruthy()
  })

  it('clicking a view opens a read-only browse tab for it', () => {
    render(<SchemaTree />)
    fireEvent.click(screen.getByText('MixedCase View'))
    expect(tabs.value[0].kind).toBe('sql-browser')
    expect(tabs.value[0].objectSchema).toBe('analytics')
    expect(tabs.value[0].objectName).toBe('MixedCase View')
  })

  it('the inspect button opens the structure inspector tab', () => {
    render(<SchemaTree />)
    fireEvent.click(screen.getByLabelText('Inspect structure of users'))
    expect(tabs.value[0].kind).toBe('schema-inspector')
    expect(tabs.value[0].objectName).toBe('users')
  })

  it('refresh re-fetches the live catalog and updates the signal', async () => {
    const next = baseSchema()
    next.sql = [{ schema: 'public', name: 'renamed_table', columns: [] }]
    schemaMock.mockResolvedValueOnce(next)
    render(<SchemaTree />)
    await refreshSchema('c1')
    expect(schemaMock).toHaveBeenCalledWith('c1')
    expect(screen.getByText('renamed_table')).toBeTruthy()
    expect(screen.queryByText('orders')).toBeNull()
  })

  it('openTab deep-links browsable tabs into the URL', () => {
    openTab({ id: '', kind: 'sql-browser', label: 'x', objectSchema: 'analytics', objectName: 'MixedCase View' })
    expect(window.location.hash).toBe('#/c/c1/t/analytics/MixedCase%20View')
    // Filtered FK-follow tabs are contexts, not addresses.
    window.location.hash = ''
    openTab({ id: '', kind: 'sql-browser', label: 'x', objectSchema: 'public', objectName: 'users', match: [{ column: 'id', value: 1 }] })
    expect(window.location.hash).toBe('')
  })
})

describe('deep-link router (S05)', () => {
  it('round-trips every browsable surface', () => {
    const cases: Array<{ hash: string; kind: string; schema?: string; name?: string }> = [
      { hash: '#/c/c1/t/public/users', kind: 'sql-browser', schema: 'public', name: 'users' },
      { hash: '#/c/c1/t/analytics/MixedCase%20View', kind: 'sql-browser', schema: 'analytics', name: 'MixedCase View' },
      { hash: '#/c/c1/designer/public/users', kind: 'schema-designer', schema: 'public', name: 'users' },
      { hash: '#/c/c1/designer', kind: 'schema-designer' },
      { hash: '#/c/c1/inspect/public/users', kind: 'schema-inspector', schema: 'public', name: 'users' },
      { hash: '#/c/c1/diagnostics', kind: 'diagnostics' },
      { hash: '#/c/c1/diagnostics/public/users', kind: 'diagnostics', schema: 'public', name: 'users' },
      { hash: '#/c/c1/sql', kind: 'sql-editor' },
    ]
    for (const c of cases) {
      const link = parseDeepLink(c.hash)
      expect(link, c.hash).not.toBeNull()
      expect(link!.connectionId, c.hash).toBe('c1')
      expect(link!.tab.kind, c.hash).toBe(c.kind)
      expect(link!.tab.objectSchema, c.hash).toBe(c.schema)
      expect(link!.tab.objectName, c.hash).toBe(c.name)
      if (c.kind !== 'sql-editor' && c.schema === undefined) continue
      // Serialize matches the original hash for the named forms.
      if (c.name) {
        expect(serializeDeepLink({ id: 'x', ...link!.tab }, 'c1'), c.hash).toBe(c.hash)
      }
    }
  })

  it('refuses non-link and truncated hashes', () => {
    expect(parseDeepLink('')).toBeNull()
    expect(parseDeepLink('#/other')).toBeNull()
    expect(parseDeepLink('#/c/c1')).toBeNull()
    expect(parseDeepLink('#/c/c1/t/public')).toBeNull()
    expect(parseDeepLink('#/c/c1/unknown/x/y')).toBeNull()
  })
})
