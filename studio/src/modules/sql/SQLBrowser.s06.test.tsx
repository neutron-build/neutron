import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from '@testing-library/preact'
import { activeConnection, schema, toasts, clearStaged } from '../../lib/store'
import { SQLBrowser } from './SQLBrowser'
import type { Schema, SqlTable, TableMeta } from '../../lib/types'

// S06 surface tests for the table view: the streamed-export ticket flow,
// the import entry point and bounded page sizes. Only the fetch boundary is
// mocked; the dialog/grid behaviors are covered by their own suites.

vi.mock('../../lib/api', async (importOriginal) => {
  const orig = await importOriginal<typeof import('../../lib/api')>()
  return {
    ...orig,
    api: {
      tableData: vi.fn(),
      tableMeta: vi.fn(),
      tableFKs: vi.fn().mockResolvedValue({ fks: [] }),
      tableExport: vi.fn(),
    },
  }
})

import { api } from '../../lib/api'

const tableData = vi.mocked(api.tableData)
const tableMeta = vi.mocked(api.tableMeta)
const tableExport = vi.mocked(api.tableExport)

function sqlTable(): SqlTable {
  return { schema: 'public', name: 'imp', columns: [] }
}
function fullSchema(): Schema {
  return {
    sql: [sqlTable()], kv: [], vector: [], timeseries: [], document: [], graph: [],
    fts: [], geo: [], blob: [], pubsub: [], streams: [], columnar: [], datalog: null, cdc: false,
  }
}
function meta(): TableMeta {
  return {
    exists: true, keyColumns: ['id'], versioned: true, readOnly: false, canDelete: true, binding: 'e1:16390',
    columns: [
      { name: 'id', type: 'int4', tag: null, nullable: false, isKey: true, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: false, readOnlyReason: 'key', insertable: true },
      { name: 'note', type: 'text', tag: null, nullable: true, isKey: false, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: true, insertable: true },
    ],
  }
}

beforeEach(() => {
  clearStaged()
  toasts.value = []
  activeConnection.value = { id: 'c1', name: 'local', url: 'postgres://localhost/x', isNucleus: false }
  schema.value = fullSchema()
  tableData.mockReset()
  tableMeta.mockReset().mockResolvedValue(meta())
  tableExport.mockReset().mockResolvedValue({
    ticket: 't-1', url: '/api/table/v2/export/download?ticket=t-1',
    filename: 'public.imp.csv', format: 'csv', expiresIn: 120,
  })
  tableData.mockResolvedValue({
    columns: ['id', 'note'], rows: [[1, 'a']], rowCount: 1, filterCount: 1, totalCount: 1,
    keyColumns: ['id'], versions: ['1'], versioned: true, binding: 'e1:16390',
  })
})
afterEach(() => {
  cleanup()
  clearStaged()
})

describe('SQLBrowser S06: streamed export', () => {
  it('Export requests a single-use ticket for the table, then downloads its URL', async () => {
    render(<SQLBrowser schema="public" table="imp" />)

    await waitFor(() => expect(tableData).toHaveBeenCalled())
    fireEvent.click(screen.getByRole('button', { name: 'Export' }))
    await waitFor(() => expect(tableExport).toHaveBeenCalledTimes(1))
    expect(tableExport.mock.calls[0][0]).toMatchObject({ connectionId: 'c1', schema: 'public', table: 'imp', format: 'csv' })
    // the ticket landed as a user-visible toast, not a silent failure
    await waitFor(() => expect(toasts.value.some(t => t.kind === 'info' && t.message.includes('public.imp.csv'))).toBe(true))
    expect(toasts.value.some(t => t.kind === 'error')).toBe(false)
  })

  it('the format selector changes what is requested', async () => {
    render(<SQLBrowser schema="public" table="imp" />)
    await waitFor(() => expect(tableData).toHaveBeenCalled())
    fireEvent.change(screen.getByLabelText('Export format'), { target: { value: 'ndjson' } })
    fireEvent.click(screen.getByRole('button', { name: 'Export' }))
    await waitFor(() => expect(tableExport).toHaveBeenCalled())
    expect(tableExport.mock.calls[0][0].format).toBe('ndjson')
  })

  it('a refused export surfaces its error, not a download', async () => {
    tableExport.mockRejectedValue(new Error('not connected'))
    render(<SQLBrowser schema="public" table="imp" />)
    await waitFor(() => expect(tableData).toHaveBeenCalled())
    fireEvent.click(screen.getByRole('button', { name: 'Export' }))
    await waitFor(() => expect(toasts.value.some(t => t.kind === 'error' && t.message.includes('Export failed'))).toBe(true))
  })
})

describe('SQLBrowser S06: import entry and page sizes', () => {
  it('the Import button opens the import dialog on an editable table', async () => {
    render(<SQLBrowser schema="public" table="imp" />)
    await waitFor(() => expect(tableData).toHaveBeenCalled())
    fireEvent.click(screen.getByRole('button', { name: 'Import…' }))
    await waitFor(() => expect(screen.getByRole('dialog')).toBeTruthy())
    expect(screen.getByText(/Import into public\.imp/)).toBeTruthy()
  })

  it('rows-per-page offers bounded sizes and reloads on change', async () => {
    render(<SQLBrowser schema="public" table="imp" />)
    await waitFor(() => expect(tableData).toHaveBeenCalledTimes(1))
    const sel = screen.getByLabelText('rows per page')
    expect(Array.from(sel.querySelectorAll('option')).map(o => o.value)).toEqual(['100', '200', '500', '1000'])
    fireEvent.change(sel, { target: { value: '500' } })
    await waitFor(() => expect(tableData).toHaveBeenCalledTimes(2))
    expect(tableData.mock.calls[1][3]).toBe(500)
  })

  it('read-only tables get no Import button', async () => {
    tableMeta.mockResolvedValue({ ...meta(), readOnly: true, readOnlyReason: 'no primary key' })
    render(<SQLBrowser schema="public" table="imp" />)
    await waitFor(() => expect(screen.queryByRole('button', { name: 'Import…' })).toBeNull())
  })
})
