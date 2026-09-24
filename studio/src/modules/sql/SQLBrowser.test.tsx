import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from '@testing-library/preact'
import { activeConnection, schema, toasts } from '../../lib/store'
import { _setSessionTokenForTests, ApiError } from '../../lib/api'
import type { Schema, SqlTable, QueryResult, TableMeta } from '../../lib/types'
import { SQLBrowser } from './SQLBrowser'

// Rendered SQLBrowser tests for the S01 typed row-identity protocol, driven
// through the real component tree (SQLBrowser -> DataGrid). Only the fetch
// boundary (lib/api) is mocked; backend behavior is covered by the Go E2E leg.

vi.mock('../../lib/api', async (importOriginal) => {
  const orig = await importOriginal<typeof import('../../lib/api')>()
  return {
    ...orig,
    api: {
      tableData: vi.fn(),
      tableMeta: vi.fn(),
      tableFKs: vi.fn().mockResolvedValue({ fks: [] }),
      tableUpdateV2: vi.fn(),
    },
  }
})

import { api } from '../../lib/api'

const tableUpdateV2 = vi.mocked(api.tableUpdateV2)
const tableData = vi.mocked(api.tableData)
const tableMeta = vi.mocked(api.tableMeta)

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

function keyedResult(rows: unknown[][], versions: string[]): QueryResult {
  return { ...result(rows), keyColumns: ['id'], versions, versioned: true }
}

function singleKeyMeta(t: Partial<TableMeta>): TableMeta {
  return {
    exists: true,
    keyColumns: ['id'],
    versioned: true,
    readOnly: false,
    columns: [
      { name: 'id', type: 'int4', tag: null, nullable: false, isKey: true, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: false, readOnlyReason: 'key column is read-only (it addresses the row)' },
      { name: 'body', type: 'text', tag: null, nullable: true, isKey: false, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: true },
    ],
    ...t,
  }
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
  _setSessionTokenForTests('test-session-token')
  toasts.value = []
  activeConnection.value = { id: 'c1', name: 'local', url: 'postgres://x', isNucleus: false }
  tableMeta.mockResolvedValue(singleKeyMeta({}))
  ;(api.tableFKs as unknown as ReturnType<typeof vi.fn>).mockResolvedValue({ fks: [] })
})

afterEach(() => {
  cleanup()
  _setSessionTokenForTests(null)
})

describe('SQLBrowser versioned identity editing (rendered flow)', () => {
  it('commits edits addressed by the full-key identity and version', async () => {
    schema.value = fullSchema([sqlTable({
      columns: [
        { name: 'id', type: 'int4', nullable: false, isPrimaryKey: true },
        { name: 'body', type: 'text', nullable: true, isPrimaryKey: false },
      ],
    })])
    tableData.mockResolvedValue(keyedResult([[1, 'hello']], ['777']))

    render(<SQLBrowser schema="public" table="memo" />)
    await waitFor(() => expect(cellAt(0, 'body').textContent).toBe('hello'))

    tableUpdateV2.mockResolvedValue({ rowsAffected: 1, version: '778' })
    tableData.mockResolvedValue(keyedResult([[1, 'world']], ['778']))

    fireEvent.dblClick(cellAt(0, 'body'))
    fireEvent.input(editorInput(), { target: { value: 'world' } })
    fireEvent.keyDown(editorInput(), { key: 'Enter' })

    await waitFor(() => expect(tableUpdateV2).toHaveBeenCalledTimes(1))
    expect(tableUpdateV2).toHaveBeenCalledWith(expect.objectContaining({
      connectionId: 'c1', schema: 'public', table: 'memo',
      key: [{ column: 'id', value: 1 }], version: '777',
      column: 'body', value: 'world', isNull: false,
    }))
  })

  it('commits an empty string distinctly from NULL through the real UI', async () => {
    schema.value = fullSchema([sqlTable({
      columns: [
        { name: 'id', type: 'int4', nullable: false, isPrimaryKey: true },
        { name: 'body', type: 'text', nullable: true, isPrimaryKey: false },
      ],
    })])
    tableData.mockResolvedValue(keyedResult([[1, 'hello']], ['100']))
    tableUpdateV2.mockResolvedValue({ rowsAffected: 1, version: '101' })

    render(<SQLBrowser schema="public" table="memo" />)
    await waitFor(() => expect(cellAt(0, 'body').textContent).toBe('hello'))

    // 1. clear the text and commit -> empty string, isNull false
    fireEvent.dblClick(cellAt(0, 'body'))
    fireEvent.input(editorInput(), { target: { value: '' } })
    fireEvent.keyDown(editorInput(), { key: 'Enter' })
    await waitFor(() => expect(tableUpdateV2).toHaveBeenCalledTimes(1))
    expect(tableUpdateV2).toHaveBeenLastCalledWith(expect.objectContaining({
      column: 'body', value: '', isNull: false,
    }))

    // 2. explicit NULL via the checkbox -> isNull true, no value
    tableData.mockResolvedValue(keyedResult([[1, 'hello']], ['101']))
    fireEvent.dblClick(cellAt(0, 'body'))
    fireEvent.click(screen.getByRole('checkbox'))
    fireEvent.keyDown(editorInput(), { key: 'Enter' })
    await waitFor(() => expect(tableUpdateV2).toHaveBeenCalledTimes(2))
    expect(tableUpdateV2).toHaveBeenLastCalledWith(expect.objectContaining({
      column: 'body', value: undefined, isNull: true,
    }))
  })

  it('surfaces a stale-version conflict explicitly and reloads', async () => {
    schema.value = fullSchema([sqlTable({
      columns: [
        { name: 'id', type: 'int4', nullable: false, isPrimaryKey: true },
        { name: 'body', type: 'text', nullable: true, isPrimaryKey: false },
      ],
    })])
    tableData.mockResolvedValue(keyedResult([[1, 'hello']], ['55']))

    render(<SQLBrowser schema="public" table="memo" />)
    await waitFor(() => expect(cellAt(0, 'body').textContent).toBe('hello'))

    // The server refuses with the explicit conflict state (409 shape).
    tableUpdateV2.mockRejectedValue(new ApiError(409, 'update refused: row changed since it was read (current row version 99)', { conflict: true, currentVersion: '99' }))
    const reloadSpy = tableData.mockResolvedValue(keyedResult([[1, 'fresh']], ['99']))

    fireEvent.dblClick(cellAt(0, 'body'))
    fireEvent.input(editorInput(), { target: { value: 'stale-edit' } })
    fireEvent.keyDown(editorInput(), { key: 'Enter' })

    await waitFor(() => expect(tableUpdateV2).toHaveBeenCalledTimes(1))
    await waitFor(() => expect(toasts.value.some(t => t.kind === 'error' && t.message.includes('Edit conflict'))).toBe(true))
    expect(toasts.value.some(t => t.kind === 'success')).toBe(false)
    // The conflict triggered a reload so the user sees current values.
    await waitFor(() => expect(cellAt(0, 'body').textContent).toBe('fresh'))
    expect(reloadSpy).toBeTruthy()
  })

  it('surfaces a missing row distinctly and reloads', async () => {
    schema.value = fullSchema([sqlTable({
      columns: [
        { name: 'id', type: 'int4', nullable: false, isPrimaryKey: true },
        { name: 'body', type: 'text', nullable: true, isPrimaryKey: false },
      ],
    })])
    tableData.mockResolvedValue(keyedResult([[1, 'hello']], ['55']))

    render(<SQLBrowser schema="public" table="memo" />)
    await waitFor(() => expect(cellAt(0, 'body').textContent).toBe('hello'))

    tableUpdateV2.mockRejectedValue(new ApiError(409, 'update matched no row for this key: the row is stale, deleted, or not visible to this connection', { missing: true }))
    tableData.mockResolvedValue({ columns: ['id', 'body'], rows: [], rowCount: 0, duration: 0, keyColumns: ['id'], versions: [], versioned: true })

    fireEvent.dblClick(cellAt(0, 'body'))
    fireEvent.input(editorInput(), { target: { value: 'x' } })
    fireEvent.keyDown(editorInput(), { key: 'Enter' })

    await waitFor(() => expect(toasts.value.some(t => t.kind === 'error' && t.message.includes('Row is gone'))).toBe(true))
    await waitFor(() => expect(tableData).toHaveBeenCalledTimes(2))
  })

  it('refuses to edit when the connection has switched (lost-window semantics)', async () => {
    schema.value = fullSchema([sqlTable({
      columns: [
        { name: 'id', type: 'int4', nullable: false, isPrimaryKey: true },
        { name: 'body', type: 'text', nullable: true, isPrimaryKey: false },
      ],
    })])
    tableData.mockResolvedValue(keyedResult([[1, 'hello']], ['55']))

    render(<SQLBrowser schema="public" table="memo" />)
    await waitFor(() => expect(cellAt(0, 'body').textContent).toBe('hello'))

    // User switches the active connection; the view stays bound to c1.
    activeConnection.value = { id: 'c2', name: 'other', url: 'postgres://y', isNucleus: false }

    fireEvent.dblClick(cellAt(0, 'body'))
    fireEvent.input(editorInput(), { target: { value: 'smuggled' } })
    fireEvent.keyDown(editorInput(), { key: 'Enter' })

    await waitFor(() => expect(toasts.value.some(t => t.kind === 'error' && t.message.includes('bound to connection'))).toBe(true))
    expect(tableUpdateV2).not.toHaveBeenCalled()
  })

  it('sends tagged key cells for bigint keys', async () => {
    schema.value = fullSchema([sqlTable({
      columns: [
        { name: 'id', type: 'int8', nullable: false, isPrimaryKey: true },
        { name: 'body', type: 'text', nullable: true, isPrimaryKey: false },
      ],
    })])
    tableMeta.mockResolvedValue(singleKeyMeta({
      keyColumns: ['id'],
      columns: [
        { name: 'id', type: 'int8', tag: 'int8', nullable: false, isKey: true, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: false, readOnlyReason: 'key column is read-only (it addresses the row)' },
        { name: 'body', type: 'text', tag: null, nullable: true, isKey: false, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: true },
      ],
    }))
    // The read returns a tagged int8 cell; the UI decodes it to a bigint
    // (lib/api decodeRows) and re-tags it exactly on the way back.
    tableData.mockResolvedValue(keyedResult([[[{ t: 'int8', v: '9007199254740993' }], 'hello']], ['9']))
    // decodeRows is real (only fetch is mocked) — run it through api? No:
    // tableData IS mocked, so decode the row the way api would.
    const decoded = keyedResult([[[{ t: 'int8', v: '9007199254740993' }], 'hello']], ['9'])
    // simulate the decode layer
    ;(decoded.rows[0] as unknown[])[0] = 9007199254740993n
    tableData.mockResolvedValue(decoded)

    render(<SQLBrowser schema="public" table="memo" />)
    await waitFor(() => expect(cellAt(0, 'body').textContent).toBe('hello'))

    tableUpdateV2.mockResolvedValue({ rowsAffected: 1, version: '10' })
    fireEvent.dblClick(cellAt(0, 'body'))
    fireEvent.input(editorInput(), { target: { value: 'big' } })
    fireEvent.keyDown(editorInput(), { key: 'Enter' })

    await waitFor(() => expect(tableUpdateV2).toHaveBeenCalledTimes(1))
    const key = tableUpdateV2.mock.calls[0][0].key
    expect(key).toEqual([{ column: 'id', value: { t: 'int8', v: '9007199254740993' } }])
  })
})

describe('SQLBrowser read-only states are authoritative', () => {
  it('composite PK with versions is editable (full-tuple identity)', async () => {
    schema.value = fullSchema([sqlTable({
      name: 'docs',
      columns: [
        { name: 'tenant_id', type: 'int4', nullable: false, isPrimaryKey: true },
        { name: 'id', type: 'int4', nullable: false, isPrimaryKey: true },
        { name: 'payload', type: 'text', nullable: false, isPrimaryKey: false },
      ],
    })])
    tableMeta.mockResolvedValue(singleKeyMeta({
      keyColumns: ['tenant_id', 'id'],
      columns: [
        { name: 'tenant_id', type: 'int4', tag: null, nullable: false, isKey: true, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: false, readOnlyReason: 'key column is read-only (it addresses the row)' },
        { name: 'id', type: 'int4', tag: null, nullable: false, isKey: true, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: false, readOnlyReason: 'key column is read-only (it addresses the row)' },
        { name: 'payload', type: 'text', tag: null, nullable: false, isKey: false, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: true },
      ],
    }))
    tableData.mockResolvedValue({
      columns: ['tenant_id', 'id', 'payload'], rows: [[1, 1, 'alpha']], rowCount: 1, duration: 0,
      keyColumns: ['tenant_id', 'id'], versions: ['42'], versioned: true,
    })

    render(<SQLBrowser schema="public" table="docs" />)
    await waitFor(() => expect(screen.queryByRole('note')).toBeNull())
    expect(screen.getByText(/double-click a cell to edit/)).toBeDefined()

    tableUpdateV2.mockResolvedValue({ rowsAffected: 1, version: '43' })
    fireEvent.dblClick(cellAt(0, 'payload'))
    fireEvent.input(editorInput(), { target: { value: 'renamed' } })
    fireEvent.keyDown(editorInput(), { key: 'Enter' })

    await waitFor(() => expect(tableUpdateV2).toHaveBeenCalledWith(expect.objectContaining({
      key: [{ column: 'tenant_id', value: 1 }, { column: 'id', value: 1 }],
      version: '42',
      column: 'payload', value: 'renamed',
    })))
  })

  it('no-key table: meta-driven read-only note and no editor', async () => {
    schema.value = fullSchema([sqlTable({
      name: 'keyless',
      columns: [
        { name: 'tag', type: 'text', nullable: false, isPrimaryKey: false },
        { name: 'note', type: 'text', nullable: false, isPrimaryKey: false },
      ],
    })])
    tableMeta.mockResolvedValue(singleKeyMeta({
      exists: true, keyColumns: [], versioned: true, readOnly: true,
      readOnlyReason: 'has no primary key — rows cannot be identified, so the table is read-only',
      columns: [],
    }))
    tableData.mockResolvedValue({
      columns: ['tag', 'note'], rows: [['a', 'keepme']], rowCount: 1, duration: 0,
      keyColumns: [], versions: [], versioned: true,
    })

    render(<SQLBrowser schema="public" table="keyless" />)
    await waitFor(() => expect(screen.getByRole('note')).toBeDefined())
    expect(screen.getByRole('note').textContent).toContain('has no primary key')

    fireEvent.dblClick(cellAt(0, 'note'))
    expect(document.querySelector('input.cellInput')).toBeNull()
    expect(tableUpdateV2).not.toHaveBeenCalled()
  })

  it('unversioned table (no xmin): read-only with the server reason', async () => {
    schema.value = fullSchema([sqlTable({ name: 'nucleus_t', columns: [] })])
    tableMeta.mockResolvedValue(singleKeyMeta({
      exists: true, keyColumns: ['k'], versioned: false, readOnly: true,
      readOnlyReason: 'does not expose row versions on this connection (no xmin) — editing is disabled to prevent stale overwrites',
      columns: [],
    }))
    tableData.mockResolvedValue({
      columns: ['k'], rows: [[1]], rowCount: 1, duration: 0,
      keyColumns: ['k'], versioned: false,
    })

    render(<SQLBrowser schema="public" table="nucleus_t" />)
    await waitFor(() => expect(screen.getByRole('note').textContent).toContain('no xmin'))
    expect(screen.queryByText(/double-click a cell to edit/)).toBeNull()
  })
})
