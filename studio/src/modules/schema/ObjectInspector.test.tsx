import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen, cleanup, fireEvent, waitFor } from '@testing-library/preact'

// S05 object detail: the structure inspector renders the shared v2
// introspection truth (columns, constraints, indexes, FK edges both ways,
// view definitions) and states honest 404s for vanished relations. Only the
// fetch boundary is mocked; backend behavior is covered by the Go E2E leg.

vi.mock('../../lib/api', async (importOriginal) => {
  const orig = await importOriginal<typeof import('../../lib/api')>()
  return {
    ...orig,
    api: {
      ...orig.api,
      schemaObject: vi.fn(),
    },
  }
})

import { api, ApiError } from '../../lib/api'
import { activeConnection, tabs } from '../../lib/store'
import { ObjectInspector } from './ObjectInspector'
import type { SchemaObjectDetail } from '../../lib/types'

const detailMock = vi.mocked(api.schemaObject)

beforeEach(() => {
  cleanup()
  activeConnection.value = { id: 'c1', name: 'test', url: 'postgres://x', isNucleus: false }
  tabs.value = []
})

afterEach(() => {
  cleanup()
  vi.clearAllMocks()
})

const tableDetail: SchemaObjectDetail = {
  kind: 'table', schema: 'public', name: 'orders', source: 'introspection-v2',
  documentSHA256: 'abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890',
  table: {
    columns: [
      { name: 'id', type: 'integer', notNull: true, isPrimaryKey: true },
      { name: 'total', type: 'numeric(12,4)', notNull: false, isPrimaryKey: false, default: { kind: 'literal', sql: '0' } },
      { name: 'upper', type: 'integer', notNull: true, isPrimaryKey: false, generated: { expression: 'id * 2' } },
    ],
    constraints: [
      { name: 'orders_pkey', type: 'primary-key', columns: ['id'] },
      { name: 'orders_total_positive', type: 'check', expression: 'total >= 0' },
      { name: 'orders_user_id_fkey', type: 'foreign-key', columns: ['user_id'], references: { table: 'public.users', columns: ['id'], onDelete: 'cascade' } },
    ],
    indexes: [
      { name: 'orders_note_idx', unique: false, method: 'btree', key: [{ column: 'note' }] },
      { name: 'orders_email_uidx', unique: true, method: 'btree', key: [{ column: 'email' }], where: 'email is not null' },
    ],
    references: [{ constraint: 'orders_user_id_fkey', schema: 'public', name: 'users', columns: ['user_id'], refColumns: ['id'] }],
    referencedBy: [{ constraint: 'shipments_line_fkey', schema: 'crm.eu', name: 'Ship Lines', columns: ['line_order', 'line_no'], refColumns: ['id', 'line'] }],
  },
}

describe('ObjectInspector (S05)', () => {
  it('renders columns, constraints, indexes and both FK directions', async () => {
    detailMock.mockResolvedValueOnce(tableDetail)
    render(<ObjectInspector schema="public" table="orders" />)
    await waitFor(() => screen.getByText('Constraints (3)'))
    expect(screen.getByText('orders_pkey')).toBeTruthy()
    expect(screen.getByText('total >= 0')).toBeTruthy()
    expect(document.body.textContent).toContain('public.users(id)')
    expect(screen.getByText('orders_note_idx')).toBeTruthy()
    expect(screen.getByText('UNIQUE')).toBeTruthy()
    expect(screen.getByText('where email is not null')).toBeTruthy()
    expect(screen.getByText('generated: id * 2')).toBeTruthy()
    // PK marker on the column row and the canonical document identity.
    expect(screen.getByTitle(/canonical document SHA-256/).textContent).toContain('abcdef123456')
    // Relationships carry both ordered tuples.
    expect(screen.getByText('orders_user_id_fkey: (user_id) → users(id)')).toBeTruthy()
    expect(screen.getByText('shipments_line_fkey: crm.eu.Ship Lines(line_order, line_no) → (id, line)')).toBeTruthy()
    // Navigation addresses the other table by schema and name separately,
    // so a dotted schema and a name with spaces survive.
    fireEvent.click(screen.getByRole('button', { name: 'Inspect crm.eu.Ship Lines' }))
    expect(tabs.value.some(t => t.kind === 'schema-inspector' && t.objectSchema === 'crm.eu' && t.objectName === 'Ship Lines')).toBe(true)
    fireEvent.click(screen.getByRole('button', { name: 'Inspect users' }))
    expect(tabs.value.some(t => t.kind === 'schema-inspector' && t.objectSchema === 'public' && t.objectName === 'users')).toBe(true)
  })

  it('renders a view definition', async () => {
    detailMock.mockResolvedValueOnce({
      kind: 'view', schema: 'public', name: 'active_users', source: 'introspection-v2',
      documentSHA256: '0123456789abcdef',
      view: { definition: 'SELECT id, email FROM users WHERE seen_at IS NOT NULL' },
    })
    render(<ObjectInspector schema="public" table="active_users" />)
    await waitFor(() => screen.getByText('SELECT id, email FROM users WHERE seen_at IS NOT NULL'))
    expect(screen.getByText('view')).toBeTruthy()
  })

  it('reports opaque inventory instead of pretending editability', async () => {
    detailMock.mockResolvedValueOnce({
      kind: 'opaque', schema: 'public', name: ' ext_tbl', source: 'introspection-v2', documentSHA256: 'x',
      opaque: { opaqueKind: 'extension-table', reason: 'owned by extension pg_stat_statements', owner: 'pg_stat_statements' },
    })
    render(<ObjectInspector schema="public" table=" ext_tbl" />)
    await waitFor(() => screen.getByText(/inventoried, never planned/))
    expect(screen.getByText(/owned by extension/)).toBeTruthy()
  })

  it('surfaces an honest 404 for a dropped relation', async () => {
    detailMock.mockRejectedValueOnce(new ApiError(404, 'no table or view public.gone', { state: undefined }))
    render(<ObjectInspector schema="public" table="gone" />)
    const alert = await screen.findByRole('alert')
    expect(alert.textContent).toContain('gone from the live catalog')
    expect(alert.textContent).toContain('Refresh the schema tree')
  })
})
