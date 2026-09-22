import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from '@testing-library/preact'
import { activeConnection, schema, toasts } from '../../lib/store'
import type { Schema, SqlTable, QueryResult } from '../../lib/types'
import { SQLBrowser } from './SQLBrowser'

// Rendered SQLBrowser tests for the B04 containment contract, driven through
// the real component tree (SQLBrowser -> DataGrid). Only the fetch boundary
// (lib/api) is mocked; backend behavior is covered by the Go E2E leg.

vi.mock('../../lib/api', () => ({
  api: {
    tableData: vi.fn(),
    tableFKs: vi.fn().mockResolvedValue({ fks: [] }),
    tableUpdate: vi.fn(),
  },
}))

import { api } from '../../lib/api'

const tableUpdate = vi.mocked(api.tableUpdate)
const tableData = vi.mocked(api.tableData)

function sqlTable(t: Partial<SqlTable>): SqlTable {
  return { schema: 'public', name: 'memo', columns: [], ...t }
}

function fullSchema(tables: SqlTable[]): Schema {
  return {
    sql: tables,
    kv: [], vector: [], timeseries: [], document: [], graph: [],
    fts: [], geo: [], blob: [], pubsub: [], streams: [], columnar: [],
    datalog: null, cdc: false,
  }
}

function result(rows: unknown[][]): QueryResult {
  return { columns: ['id', 'body'], rows, rowCount: rows.length, duration: 0 }
}

function cellAt(row: number, col: string): HTMLElement {
  const headerCells = Array.from(document.querySelectorAll('th'))
  const colIdx = headerCells.findIndex(th => th.textContent?.includes(col))
  const trs = Array.from(document.querySelectorAll('tbody tr'))
  return trs[row].children[colIdx] as HTMLElement
}


function editorInput(): HTMLInputElement {
  const el = document.querySelector('input.cellInput')
  if (!el) throw new Error('cell editor input not found')
  return el as HTMLInputElement
}

beforeEach(() => {
  vi.clearAllMocks()
  toasts.value = []
  activeConnection.value = { id: 'c1', name: 'local', url: 'postgres://x', isNucleus: false }
  tableFKsMock()
})

afterEach(cleanup)

function tableFKsMock() {
  return (api.tableFKs as unknown as ReturnType<typeof vi.fn>).mockResolvedValue({ fks: [] })
}

describe('SQLBrowser single-column PK editing (rendered flow)', () => {
  it('commits an empty string distinctly from NULL through the real UI', async () => {
    schema.value = fullSchema([sqlTable({
      columns: [
        { name: 'id', type: 'int4', nullable: false, isPrimaryKey: true },
        { name: 'body', type: 'text', nullable: true, isPrimaryKey: false },
      ],
    })])
    tableData.mockResolvedValue(result([[1, 'hello']]))

    const { container } = render(<SQLBrowser schema="public" table="memo" />)
    await waitFor(() => expect(cellAt(0, 'body').textContent).toBe('hello'))

    tableUpdate.mockResolvedValue({ rowsAffected: 1 })

    // 1. clear the text and commit -> empty string, isNull false
    fireEvent.dblClick(cellAt(0, 'body'))
    fireEvent.input(editorInput(), { target: { value: '' } })
    fireEvent.keyDown(editorInput(), { key: 'Enter' })
    await waitFor(() => expect(tableUpdate).toHaveBeenCalledTimes(1))
    expect(tableUpdate).toHaveBeenCalledWith(expect.objectContaining({
      connectionId: 'c1', schema: 'public', table: 'memo',
      pkColumn: 'id', pkValue: 1, column: 'body', value: '', isNull: false,
    }))

    // 2. explicit NULL via the checkbox -> isNull true, no value
    tableData.mockResolvedValue(result([[1, 'hello']]))
    fireEvent.dblClick(cellAt(0, 'body'))
    fireEvent.click(screen.getByRole('checkbox'))
    fireEvent.keyDown(editorInput(), { key: 'Enter' })
    await waitFor(() => expect(tableUpdate).toHaveBeenCalledTimes(2))
    expect(tableUpdate).toHaveBeenLastCalledWith(expect.objectContaining({
      column: 'body', value: undefined, isNull: true,
    }))
    expect(container.textContent).not.toBeNull()
  })

  it('honors error responses: no success toast, no silent acceptance', async () => {
    schema.value = fullSchema([sqlTable({
      columns: [
        { name: 'id', type: 'int4', nullable: false, isPrimaryKey: true },
        { name: 'body', type: 'text', nullable: true, isPrimaryKey: false },
      ],
    })])
    tableData.mockResolvedValue(result([[1, 'hello']]))

    render(<SQLBrowser schema="public" table="memo" />)
    await waitFor(() => expect(cellAt(0, 'body').textContent).toBe('hello'))

    tableUpdate.mockResolvedValue({ rowsAffected: 0, error: 'update matched no row for id = 1: the row is stale, deleted, or not visible to this connection' })
    fireEvent.dblClick(cellAt(0, 'body'))
    fireEvent.input(editorInput(), { target: { value: 'x' } })
    fireEvent.keyDown(editorInput(), { key: 'Enter' })

    await waitFor(() => expect(tableUpdate).toHaveBeenCalledTimes(1))
    await waitFor(() => expect(toasts.value.some(t => t.kind === 'error')).toBe(true))
    expect(toasts.value.some(t => t.kind === 'success')).toBe(false)
  })

  it('honors rowsAffected != 1 as a failure even without an error string', async () => {
    schema.value = fullSchema([sqlTable({
      columns: [
        { name: 'id', type: 'int4', nullable: false, isPrimaryKey: true },
        { name: 'body', type: 'text', nullable: true, isPrimaryKey: false },
      ],
    })])
    tableData.mockResolvedValue(result([[1, 'hello']]))

    render(<SQLBrowser schema="public" table="memo" />)
    await waitFor(() => expect(cellAt(0, 'body').textContent).toBe('hello'))

    tableUpdate.mockResolvedValue({ rowsAffected: 2 })
    fireEvent.dblClick(cellAt(0, 'body'))
    fireEvent.input(editorInput(), { target: { value: 'x' } })
    fireEvent.keyDown(editorInput(), { key: 'Enter' })

    await waitFor(() => expect(toasts.value.some(t => t.kind === 'error' && t.message.includes('exactly 1 row'))).toBe(true))
    expect(toasts.value.some(t => t.kind === 'success')).toBe(false)
  })
})

describe('SQLBrowser composite and no-key tables are read-only', () => {
  it('composite PK: shows the explanation, offers no editor, never calls the API', async () => {
    schema.value = fullSchema([sqlTable({
      name: 'docs',
      columns: [
        { name: 'tenant_id', type: 'int4', nullable: false, isPrimaryKey: true },
        { name: 'id', type: 'int4', nullable: false, isPrimaryKey: true },
        { name: 'payload', type: 'text', nullable: false, isPrimaryKey: false },
      ],
    })])
    tableData.mockResolvedValue({
      columns: ['tenant_id', 'id', 'payload'], rows: [[1, 1, 'alpha']], rowCount: 1, duration: 0,
    })

    render(<SQLBrowser schema="public" table="docs" />)
    await waitFor(() => expect(screen.getByRole('note')).toBeDefined())
    expect(screen.getByRole('note').textContent).toContain('composite primary key (tenant_id, id)')
    expect(screen.queryByText(/double-click a cell to edit/)).toBeNull()

    fireEvent.dblClick(cellAt(0, 'payload'))
    expect(document.querySelector('input.cellInput')).toBeNull()
    expect(tableUpdate).not.toHaveBeenCalled()
  })

  it('no-key table: read-only note and no editor', async () => {
    schema.value = fullSchema([sqlTable({
      name: 'keyless',
      columns: [
        { name: 'tag', type: 'text', nullable: false, isPrimaryKey: false },
        { name: 'note', type: 'text', nullable: false, isPrimaryKey: false },
      ],
    })])
    tableData.mockResolvedValue({
      columns: ['tag', 'note'], rows: [['a', 'keepme']], rowCount: 1, duration: 0,
    })

    render(<SQLBrowser schema="public" table="keyless" />)
    await waitFor(() => expect(screen.getByRole('note')).toBeDefined())
    expect(screen.getByRole('note').textContent).toContain('has no primary key')

    fireEvent.dblClick(cellAt(0, 'note'))
    expect(document.querySelector('input.cellInput')).toBeNull()
    expect(tableUpdate).not.toHaveBeenCalled()
  })
})
