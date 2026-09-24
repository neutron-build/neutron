import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from '@testing-library/preact'
import {
  activeConnection, schema, openTab, toast, toasts, tabs, stagedEdits, clearStaged,
  failedEditFocus, bindingActive,
} from '../../lib/store'
import { _setSessionTokenForTests } from '../../lib/api'
import type { Schema, SqlTable, QueryResult, TableMeta } from '../../lib/types'
import { SQLBrowser } from './SQLBrowser'

// Rendered SQLBrowser tests for the S01 typed row-identity protocol and the
// S03 data editor, driven through the real component tree (SQLBrowser ->
// DataGrid -> TypedEditor). Only the fetch boundary (lib/api) is mocked;
// backend behavior is covered by the Go E2E leg.

vi.mock('../../lib/api', async (importOriginal) => {
  const orig = await importOriginal<typeof import('../../lib/api')>()
  return {
    ...orig,
    api: {
      tableData: vi.fn(),
      tableMeta: vi.fn(),
      tableFKs: vi.fn().mockResolvedValue({ fks: [] }),
    },
  }
})

import { api } from '../../lib/api'

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
  return { ...result(rows), keyColumns: ['id'], versions, versioned: true, binding: 'e1:16385' }
}

function singleKeyMeta(t: Partial<TableMeta>): TableMeta {
  return {
    exists: true,
    keyColumns: ['id'],
    versioned: true,
    readOnly: false,
    canDelete: true,
    columns: [
      { name: 'id', type: 'int4', tag: null, nullable: false, isKey: true, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: false, readOnlyReason: 'key column is read-only (it addresses the row)', insertable: true },
      { name: 'body', type: 'text', tag: null, nullable: true, isKey: false, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: true, insertable: true },
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
  const el = document.querySelector('input[aria-label$=" value"]')
  if (!el) throw new Error('cell editor input not found')
  return el as HTMLInputElement
}

function stateSelect(): HTMLSelectElement {
  const el = document.querySelector('select[aria-label$=" value state"]')
  if (!el) throw new Error('value-state select not found')
  return el as HTMLSelectElement
}

/** The last staged operation (staged through the real UI). */
function lastStaged() {
  const edits = stagedEdits.value
  if (edits.length === 0) throw new Error('nothing staged')
  return edits[edits.length - 1]
}

const memoSchema = () => {
  schema.value = fullSchema([sqlTable({
    columns: [
      { name: 'id', type: 'int4', nullable: false, isPrimaryKey: true },
      { name: 'body', type: 'text', nullable: true, isPrimaryKey: false },
    ],
  })])
}

beforeEach(() => {
  vi.clearAllMocks()
  _setSessionTokenForTests('test-session-token')
  toasts.value = []
  tabs.value = []
  clearStaged()
  failedEditFocus.value = null
  activeConnection.value = { id: 'c1', name: 'local', url: 'postgres://x', isNucleus: false }
  tableMeta.mockResolvedValue(singleKeyMeta({}))
  ;(api.tableFKs as unknown as ReturnType<typeof vi.fn>).mockResolvedValue({ fks: [] })
})

afterEach(() => {
  cleanup()
  _setSessionTokenForTests(null)
})

describe('SQLBrowser versioned identity editing (staged draft flow)', () => {
  it('stages edits addressed by the full-key identity and version', async () => {
    memoSchema()
    tableData.mockResolvedValue(keyedResult([[1, 'hello']], ['777']))

    render(<SQLBrowser schema="public" table="memo" />)
    await waitFor(() => expect(cellAt(0, 'body').textContent).toBe('hello'))

    fireEvent.dblClick(cellAt(0, 'body'))
    fireEvent.input(editorInput(), { target: { value: 'world' } })
    fireEvent.keyDown(editorInput(), { key: 'Enter' })

    const staged = lastStaged()
    expect(staged.connectionId).toBe('c1')
    expect(staged.operation).toEqual({
      op: 'update', schema: 'public', table: 'memo', binding: 'e1:16385',
      key: [{ column: 'id', value: 1 }], version: '777',
      column: 'body', value: 'world',
    })
    // staging sends nothing over the wire — the draft commits as one batch
    expect(tableData).toHaveBeenCalledTimes(1)
  })

  it('commits an empty string distinctly from NULL through the real UI', async () => {
    memoSchema()
    tableData.mockResolvedValue(keyedResult([[1, 'hello']], ['100']))

    render(<SQLBrowser schema="public" table="memo" />)
    await waitFor(() => expect(cellAt(0, 'body').textContent).toBe('hello'))

    // 1. clear the text and stage -> empty string value
    fireEvent.dblClick(cellAt(0, 'body'))
    fireEvent.input(editorInput(), { target: { value: '' } })
    fireEvent.keyDown(editorInput(), { key: 'Enter' })
    expect(lastStaged().operation).toEqual(expect.objectContaining({
      column: 'body', value: '',
    }))
    expect((lastStaged().operation as Record<string, unknown>).isNull).toBeUndefined()

    // 2. explicit NULL via the state control
    fireEvent.dblClick(cellAt(0, 'body'))
    fireEvent.change(stateSelect(), { target: { value: 'null' } })
    fireEvent.keyDown(stateSelect(), { key: 'Enter' })
    expect(lastStaged().operation).toEqual(expect.objectContaining({
      column: 'body', isNull: true,
    }))
    expect((lastStaged().operation as Record<string, unknown>).value).toBeUndefined()
  })

  it('staged edits overlay their committed cells and survive reloads by key', async () => {
    memoSchema()
    tableData.mockResolvedValue(keyedResult([[1, 'hello'], [2, 'there']], ['10', '11']))

    render(<SQLBrowser schema="public" table="memo" />)
    await waitFor(() => expect(cellAt(0, 'body').textContent).toBe('hello'))

    fireEvent.dblClick(cellAt(0, 'body'))
    fireEvent.input(editorInput(), { target: { value: 'staged-value' } })
    fireEvent.keyDown(editorInput(), { key: 'Enter' })

    await waitFor(() => expect(cellAt(0, 'body').textContent).toBe('staged-value'))
    expect(cellAt(0, 'body').querySelector('span[title^="staged (not committed)"]')).not.toBeNull()

    // a reload (refresh) re-reads; the draft re-attaches to its row by key
    fireEvent.click(screen.getByTitle('Refresh'))
    await waitFor(() => expect(tableData).toHaveBeenCalledTimes(2))
    await waitFor(() => expect(cellAt(0, 'body').textContent).toBe('staged-value'))
    expect(cellAt(1, 'body').textContent).toBe('there')
  })

  it('staged edits survive re-sorting and follow their row, never the index', async () => {
    memoSchema()
    tableData.mockResolvedValue(keyedResult([[1, 'hello'], [2, 'there']], ['10', '11']))

    render(<SQLBrowser schema="public" table="memo" />)
    await waitFor(() => expect(cellAt(0, 'body').textContent).toBe('hello'))

    fireEvent.dblClick(cellAt(0, 'body'))
    fireEvent.input(editorInput(), { target: { value: 'sorted-draft' } })
    fireEvent.keyDown(editorInput(), { key: 'Enter' })
    await waitFor(() => expect(cellAt(0, 'body').textContent).toBe('sorted-draft'))

    // Sort by id: the server returns the rows reversed.
    tableData.mockResolvedValue(keyedResult([[2, 'there'], [1, 'hello']], ['11', '10']))
    const headers0 = Array.from(document.querySelectorAll('th'))
    fireEvent.click(headers0.find(th => th.textContent?.includes('id'))!)

    await waitFor(() => expect(tableData).toHaveBeenCalledTimes(2))
    expect(tableData.mock.calls[1][7]).toEqual([{ column: 'id', dir: 'asc' }])
    // the draft moved WITH its row (id=1 is now row 1), untouched row 0
    await waitFor(() => expect(cellAt(1, 'body').textContent).toBe('sorted-draft'))
    expect(cellAt(0, 'body').textContent).toBe('there')
    expect(lastStaged().operation).toEqual(expect.objectContaining({
      key: [{ column: 'id', value: 1 }], version: '10', value: 'sorted-draft',
    }))
  })

  it('refuses to stage when the connection has switched (lost-window semantics)', async () => {
    memoSchema()
    tableData.mockResolvedValue(keyedResult([[1, 'hello']], ['55']))

    render(<SQLBrowser schema="public" table="memo" />)
    await waitFor(() => expect(cellAt(0, 'body').textContent).toBe('hello'))

    // User switches the active connection; the view stays bound to c1.
    activeConnection.value = { id: 'c2', name: 'other', url: 'postgres://y', isNucleus: false }
    await waitFor(() => expect(screen.getByRole('note').textContent).toContain('bound to connection c1'))

    fireEvent.dblClick(cellAt(0, 'body'))
    fireEvent.input(editorInput(), { target: { value: 'smuggled' } })
    fireEvent.keyDown(editorInput(), { key: 'Enter' })

    await waitFor(() => expect(toasts.value.some(t => t.kind === 'error' && t.message.includes('bound to connection'))).toBe(true))
    expect(stagedEdits.value).toEqual([])
    expect(bindingActive({ connectionId: 'c1', schema: 'public', table: 'memo' })).toBe(false)
  })

  it('a failed commit focuses the first offending row in the grid', async () => {
    memoSchema()
    tableData.mockResolvedValue(keyedResult([[1, 'hello'], [2, 'there']], ['10', '11']))

    render(<SQLBrowser schema="public" table="memo" />)
    await waitFor(() => expect(cellAt(0, 'body').textContent).toBe('hello'))

    // row 2's edit is the offending one (operations[1])
    fireEvent.dblClick(cellAt(1, 'body'))
    fireEvent.input(editorInput(), { target: { value: 'bad' } })
    fireEvent.keyDown(editorInput(), { key: 'Enter' })
    await waitFor(() => expect(stagedEdits.value.length).toBe(1))

    failedEditFocus.value = { editId: stagedEdits.value[0].id, reason: 'operations[0]: update refused' }
    await waitFor(() => expect(document.querySelector('tr.trFocus')).not.toBeNull())
    const focused = document.querySelector('tr.trFocus') as HTMLElement
    expect(focused.getAttribute('data-row-index')).toBe('1')
    expect(cellAt(1, 'body').className).toContain('tdFocus')

    failedEditFocus.value = null
    // focus handed back clears the highlight
  })

  it('edits to tagged columns stage as tagged wire cells, not bare text', async () => {
    schema.value = fullSchema([sqlTable({
      columns: [
        { name: 'id', type: 'int4', nullable: false, isPrimaryKey: true },
        { name: 'amount', type: 'numeric', nullable: true, isPrimaryKey: false },
        { name: 'seen_at', type: 'timestamptz', nullable: true, isPrimaryKey: false },
      ],
    })])
    tableMeta.mockResolvedValue(singleKeyMeta({
      columns: [
        { name: 'id', type: 'int4', tag: null, nullable: false, isKey: true, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: false, readOnlyReason: 'key column is read-only (it addresses the row)', insertable: true },
        { name: 'amount', type: 'numeric', tag: 'numeric', nullable: true, isKey: false, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: true, insertable: true },
        { name: 'seen_at', type: 'timestamptz', tag: 'timestamptz', nullable: true, isKey: false, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: true, insertable: true },
      ],
    }))
    // Decoded cells: numeric/timestamptz arrive from lib/wire as strings.
    tableData.mockResolvedValue({
      columns: ['id', 'amount', 'seen_at'], rows: [[1, '9.9000', '2026-01-02T03:04:05.000001Z']], rowCount: 1, duration: 0,
      keyColumns: ['id'], versions: ['5'], versioned: true, binding: 'e1:1',
    })

    render(<SQLBrowser schema="public" table="memo" />)
    await waitFor(() => expect(cellAt(0, 'amount').textContent).toBe('9.9000'))

    fireEvent.dblClick(cellAt(0, 'amount'))
    fireEvent.input(editorInput(), { target: { value: '12.3450' } })
    fireEvent.keyDown(editorInput(), { key: 'Enter' })

    await waitFor(() => expect(stagedEdits.value.length).toBe(1))
    expect(lastStaged().operation).toEqual(expect.objectContaining({
      column: 'amount', value: { t: 'numeric', v: '12.3450' },
    }))

    // A temporal edit is likewise a tagged cell.
    fireEvent.dblClick(cellAt(0, 'seen_at'))
    fireEvent.input(editorInput(), { target: { value: '2026-01-02T03:04:05.000002Z' } })
    fireEvent.keyDown(editorInput(), { key: 'Enter' })
    await waitFor(() => expect(stagedEdits.value.length).toBe(2))
    expect(lastStaged().operation).toEqual(expect.objectContaining({
      column: 'seen_at', value: { t: 'timestamptz', v: '2026-01-02T03:04:05.000002Z' },
    }))
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
        { name: 'id', type: 'int8', tag: 'int8', nullable: false, isKey: true, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: false, readOnlyReason: 'key column is read-only (it addresses the row)', insertable: true },
        { name: 'body', type: 'text', tag: null, nullable: true, isKey: false, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: true, insertable: true },
      ],
    }))
    // The read returns a tagged int8 cell; the UI decodes it to a bigint
    // (lib/api decodeRows) and re-tags it exactly on the way back.
    const decoded = keyedResult([[[{ t: 'int8', v: '9007199254740993' }], 'hello']], ['9'])
    // simulate the decode layer (tableData is mocked)
    ;(decoded.rows[0] as unknown[])[0] = 9007199254740993n
    tableData.mockResolvedValue(decoded)

    render(<SQLBrowser schema="public" table="memo" />)
    await waitFor(() => expect(cellAt(0, 'body').textContent).toBe('hello'))

    fireEvent.dblClick(cellAt(0, 'body'))
    fireEvent.input(editorInput(), { target: { value: 'big' } })
    fireEvent.keyDown(editorInput(), { key: 'Enter' })

    await waitFor(() => expect(stagedEdits.value.length).toBe(1))
    const key = (lastStaged().operation as { key: { column: string; value: unknown }[] }).key
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
        { name: 'tenant_id', type: 'int4', tag: null, nullable: false, isKey: true, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: false, readOnlyReason: 'key column is read-only (it addresses the row)', insertable: true },
        { name: 'id', type: 'int4', tag: null, nullable: false, isKey: true, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: false, readOnlyReason: 'key column is read-only (it addresses the row)', insertable: true },
        { name: 'payload', type: 'text', tag: null, nullable: false, isKey: false, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: true, insertable: true },
      ],
    }))
    tableData.mockResolvedValue({
      columns: ['tenant_id', 'id', 'payload'], rows: [[1, 1, 'alpha']], rowCount: 1, duration: 0,
      keyColumns: ['tenant_id', 'id'], versions: ['42'], versioned: true, binding: 'e1:16390',
    })

    render(<SQLBrowser schema="public" table="docs" />)
    await waitFor(() => expect(screen.queryByRole('note')).toBeNull())
    expect(screen.getByText(/stage an edit/)).toBeDefined()

    fireEvent.dblClick(cellAt(0, 'payload'))
    fireEvent.input(editorInput(), { target: { value: 'renamed' } })
    fireEvent.keyDown(editorInput(), { key: 'Enter' })

    await waitFor(() => expect(stagedEdits.value.length).toBe(1))
    expect(lastStaged().operation).toEqual(expect.objectContaining({
      key: [{ column: 'tenant_id', value: 1 }, { column: 'id', value: 1 }],
      version: '42',
      column: 'payload', value: 'renamed',
    }))
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
    expect(document.querySelector('select[aria-label$=" value state"]')).toBeNull()
    expect(stagedEdits.value).toEqual([])
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
    expect(screen.queryByText(/stage an edit/)).toBeNull()
  })
})

describe('SQLBrowser S03: insert, delete, filters, sorts, counts', () => {
  function editorsMeta(): TableMeta {
    return singleKeyMeta({
      columns: [
        { name: 'id', type: 'int4', tag: null, nullable: false, isKey: true, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: false, readOnlyReason: 'key column is read-only (it addresses the row)', insertable: true },
        { name: 'body', type: 'text', tag: null, nullable: false, isKey: false, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: true, insertable: true },
        { name: 'note', type: 'text', tag: null, nullable: true, isKey: false, generated: false, identity: false, hasDefault: true, autoAssigned: false, editable: true, insertable: true },
        { name: 'flag', type: 'boolean', tag: null, nullable: false, isKey: false, generated: false, identity: false, hasDefault: true, autoAssigned: false, editable: true, insertable: true },
        { name: 'amount', type: 'numeric', tag: 'numeric', nullable: true, isKey: false, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: true, insertable: true },
        { name: 'doc', type: 'jsonb', tag: null, nullable: true, isKey: false, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: true, insertable: true },
      ],
    })
  }
  function editorsSchema() {
    schema.value = fullSchema([sqlTable({
      name: 'editors',
      columns: [
        { name: 'id', type: 'int4', nullable: false, isPrimaryKey: true },
        { name: 'body', type: 'text', nullable: false, isPrimaryKey: false },
        { name: 'note', type: 'text', nullable: true, isPrimaryKey: false },
        { name: 'flag', type: 'boolean', nullable: false, isPrimaryKey: false },
        { name: 'amount', type: 'numeric', nullable: true, isPrimaryKey: false },
        { name: 'doc', type: 'jsonb', nullable: true, isPrimaryKey: false },
      ],
    })])
  }

  it('the insert form stages a typed insert with NULL, DEFAULT and tagged values distinct', async () => {
    editorsSchema()
    tableMeta.mockResolvedValue(editorsMeta())
    tableData.mockResolvedValue(keyedResult([[1, 'hello', 'n', true, '1.0', null]], ['1']))

    render(<SQLBrowser schema="public" table="editors" />)
    await waitFor(() => expect(cellAt(0, 'body').textContent).toBe('hello'))

    fireEvent.click(screen.getByTitle(/Stage a new row/))

    // id + body are required (NOT NULL, no default): fill them
    const idInput = screen.getByLabelText('id value') as HTMLInputElement
    fireEvent.input(idInput, { target: { value: '10' } })
    const bodyInput = screen.getByLabelText('body value') as HTMLInputElement
    fireEvent.input(bodyInput, { target: { value: '' } }) // explicit empty string
    // note: switch to NULL
    fireEvent.change(screen.getByLabelText('note value state'), { target: { value: 'null' } })
    // flag: boolean select -> true
    fireEvent.change(screen.getByLabelText('flag boolean value'), { target: { value: 'true' } })
    // amount: tagged numeric text
    fireEvent.input(screen.getByLabelText('amount value'), { target: { value: '12.3450' } })
    // doc: JSON text; note stays DEFAULT (omitted) — wait, note was set NULL above; 'doc' default omitted
    fireEvent.input(screen.getByLabelText('doc JSON text'), { target: { value: '{"a":1}' } })

    fireEvent.click(screen.getByText('Stage insert'))

    await waitFor(() => expect(stagedEdits.value.length).toBe(1))
    expect(lastStaged().operation).toEqual({
      op: 'insert', schema: 'public', table: 'editors', binding: 'e1:16385',
      values: {
        id: '10',
        body: '',
        note: null,
        flag: true,
        amount: { t: 'numeric', v: '12.3450' },
        doc: '{"a":1}',
      },
    })
    // form closes after staging
    expect(screen.queryByText('Stage insert')).toBeNull()
  })

  it('the insert form refuses a required column left non-value', async () => {
    editorsSchema()
    tableMeta.mockResolvedValue(editorsMeta())
    tableData.mockResolvedValue(keyedResult([[1, 'hello']], ['1']))

    render(<SQLBrowser schema="public" table="editors" />)
    await waitFor(() => expect(cellAt(0, 'body').textContent).toBe('hello'))

    fireEvent.click(screen.getByTitle(/Stage a new row/))
    // body is required but set to DEFAULT
    fireEvent.change(screen.getByLabelText('body value state'), { target: { value: 'default' } })
    fireEvent.click(screen.getByText('Stage insert'))

    await waitFor(() => expect(toasts.value.some(t => t.kind === 'error' && t.message.includes('body'))).toBe(true))
    expect(stagedEdits.value).toEqual([])
    // the form stays open with the error surfaced on the field
    expect(screen.getByRole('alert').textContent).toContain('required column')
  })

  it('the insert form refuses invalid JSON with column context', async () => {
    editorsSchema()
    tableMeta.mockResolvedValue(editorsMeta())
    tableData.mockResolvedValue(keyedResult([[1, 'hello']], ['1']))

    render(<SQLBrowser schema="public" table="editors" />)
    await waitFor(() => expect(cellAt(0, 'body').textContent).toBe('hello'))

    fireEvent.click(screen.getByTitle(/Stage a new row/))
    fireEvent.input(screen.getByLabelText('id value'), { target: { value: '11' } })
    fireEvent.input(screen.getByLabelText('body value'), { target: { value: 'x' } })
    fireEvent.input(screen.getByLabelText('doc JSON text'), { target: { value: '{not json' } })
    fireEvent.click(screen.getByText('Stage insert'))

    await waitFor(() => expect(toasts.value.some(t => t.kind === 'error' && t.message.includes('doc'))).toBe(true))
    expect(stagedEdits.value).toEqual([])
  })

  it('the row delete button stages a delete with identity and version', async () => {
    memoSchema()
    tableMeta.mockResolvedValue(singleKeyMeta({}))
    tableData.mockResolvedValue(keyedResult([[1, 'hello'], [2, 'bye']], ['10', '11']))

    render(<SQLBrowser schema="public" table="memo" />)
    await waitFor(() => expect(cellAt(0, 'body').textContent).toBe('hello'))

    fireEvent.click(screen.getAllByTitle('Stage row delete')[0])
    await waitFor(() => expect(stagedEdits.value.length).toBe(1))
    expect(lastStaged().operation).toEqual({
      op: 'delete', schema: 'public', table: 'memo', binding: 'e1:16385',
      key: [{ column: 'id', value: 1 }], version: '10',
    })
    // the row renders struck-through until commit or discard
    expect((document.querySelector('tr[data-row-index="0"]') as HTMLElement).className).toContain('trStagedDelete')
  })

  it('multiple filters AND: the read carries the filters array', async () => {
    memoSchema()
    tableData.mockResolvedValue(keyedResult([], []))

    render(<SQLBrowser schema="public" table="memo" />)
    await waitFor(() => expect(tableData).toHaveBeenCalledTimes(1))

    fireEvent.click(screen.getByTitle('Add an ANDed filter'))
    fireEvent.change(screen.getByLabelText('Filter column 1'), { target: { value: 'body' } })
    fireEvent.change(screen.getByLabelText('Filter operator 1'), { target: { value: 'like' } })
    fireEvent.input(screen.getByLabelText('Filter value 1'), { target: { value: '%x%' } })
    fireEvent.click(screen.getByTitle('Add an ANDed filter'))
    fireEvent.change(screen.getByLabelText('Filter column 2'), { target: { value: 'id' } })
    fireEvent.change(screen.getByLabelText('Filter operator 2'), { target: { value: 'gt' } })
    fireEvent.input(screen.getByLabelText('Filter value 2'), { target: { value: '5' } })
    fireEvent.click(screen.getByText('Apply'))

    await waitFor(() => expect(tableData).toHaveBeenCalledTimes(2))
    expect(tableData.mock.calls[1][5]).toEqual([
      { column: 'body', op: 'like', value: '%x%' },
      { column: 'id', op: 'gt', value: '5' },
    ])
  })

  it('an incomplete filter row refuses to apply', async () => {
    memoSchema()
    tableData.mockResolvedValue(keyedResult([], []))

    render(<SQLBrowser schema="public" table="memo" />)
    await waitFor(() => expect(tableData).toHaveBeenCalledTimes(1))

    fireEvent.click(screen.getByTitle('Add an ANDed filter'))
    fireEvent.click(screen.getByText('Apply'))
    await waitFor(() => expect(toasts.value.some(t => t.kind === 'error' && t.message.includes('column'))).toBe(true))
    expect(tableData).toHaveBeenCalledTimes(1)
  })

  it('shift-click builds a multi-column sort; plain click resets to primary', async () => {
    memoSchema()
    tableData.mockResolvedValue(keyedResult([], []))

    render(<SQLBrowser schema="public" table="memo" />)
    await waitFor(() => expect(tableData).toHaveBeenCalledTimes(1))

    const headers = Array.from(document.querySelectorAll('th'))
    const idTh = headers.find(th => th.textContent?.includes('id'))!
    const bodyTh = headers.find(th => th.textContent?.includes('body'))!

    // plain click: primary asc
    fireEvent.click(idTh)
    await waitFor(() => expect(tableData).toHaveBeenCalledTimes(2))
    expect(tableData.mock.calls[1][7]).toEqual([{ column: 'id', dir: 'asc' }])
    await waitFor(() => expect(Array.from(document.querySelectorAll('th')).find(th => th.textContent?.includes('id'))!.textContent).toContain('↑'))

    // shift-click body: multi-sort id asc, body asc
    fireEvent.click(bodyTh, { shiftKey: true })
    await waitFor(() => expect(tableData).toHaveBeenCalledTimes(3))
    expect(tableData.mock.calls[2][7]).toEqual([
      { column: 'id', dir: 'asc' }, { column: 'body', dir: 'asc' },
    ])
    await waitFor(() => expect(Array.from(document.querySelectorAll('th')).find(th => th.textContent?.includes('body'))!.textContent).toContain('↑2'))

    // plain click body: body becomes the single primary
    fireEvent.click(bodyTh)
    await waitFor(() => expect(tableData).toHaveBeenCalledTimes(4))
    expect(tableData.mock.calls[3][7]).toEqual([{ column: 'body', dir: 'asc' }])
  })

  it('row counts are separated: fetched vs filtered vs total vs staged', async () => {
    memoSchema()
    tableData.mockResolvedValue({
      ...keyedResult([[1, 'a'], [2, 'b']], ['10', '11']),
      filterCount: 42, totalCount: 1000,
    })

    render(<SQLBrowser schema="public" table="memo" />)
    await waitFor(() => expect(screen.getByText(/42 filtered/)).toBeDefined())
    expect(screen.getByText(/1,000 total/)).toBeDefined()

    fireEvent.dblClick(cellAt(0, 'body'))
    fireEvent.input(editorInput(), { target: { value: 'z' } })
    fireEvent.keyDown(editorInput(), { key: 'Enter' })
    await waitFor(() => expect(screen.getByText(/1 staged/)).toBeDefined())
  })
})

describe('SQLBrowser S01 binding, read-level state and composite FK follow', () => {
  function docsMeta(): TableMeta {
    return {
      exists: true, binding: 'e1:20', keyColumns: ['tenant_id', 'order_no'], versioned: true, readOnly: false, canDelete: true,
      columns: [
        { name: 'tenant_id', type: 'int4', tag: null, nullable: false, isKey: true, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: false, readOnlyReason: 'key column is read-only (it addresses the row)', insertable: true },
        { name: 'order_no', type: 'int8', tag: 'int8', nullable: false, isKey: true, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: false, readOnlyReason: 'key column is read-only (it addresses the row)', insertable: true },
        { name: 'label', type: 'text', tag: null, nullable: true, isKey: false, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: true, insertable: true },
      ],
    }
  }
  function ordersResult(rows: unknown[][]): QueryResult {
    return {
      columns: ['tenant_id', 'order_no', 'label'], rows, rowCount: rows.length, duration: 0,
      keyColumns: ['tenant_id', 'order_no'], versions: rows.map(() => '10'), versioned: true, binding: 'e1:20',
    }
  }
  function ordersSchema() {
    schema.value = fullSchema([sqlTable({
      name: 'orders',
      columns: [
        { name: 'tenant_id', type: 'int4', nullable: false, isPrimaryKey: true },
        { name: 'order_no', type: 'int8', nullable: false, isPrimaryKey: true },
        { name: 'label', type: 'text', nullable: true, isPrimaryKey: false },
      ],
    })])
  }

  it('composite FK follow opens the target filtered by the WHOLE tuple, exactly', async () => {
    ordersSchema()
    tableMeta.mockResolvedValue(docsMeta())
    ;(api.tableFKs as unknown as ReturnType<typeof vi.fn>).mockResolvedValue({
      fks: [{ name: 'orders_doc_fk', columns: ['tenant_id', 'order_no'], refSchema: 'public', refTable: 'docs', refColumns: ['tenant_id', 'id'], composite: true }],
    })
    tableData.mockResolvedValue(ordersResult([[1, 9007199254740993n, 'o1']]))
    tabs.value = []

    render(<SQLBrowser schema="public" table="orders" />)
    await waitFor(() => expect(cellAt(0, 'label').textContent).toBe('o1'))
    // Both tuple components render as follow links; either follows the tuple.
    await waitFor(() => expect(cellAt(0, 'order_no').querySelector('button')).not.toBeNull())
    expect(cellAt(0, 'order_no').textContent).toBe('9007199254740993')
    fireEvent.click(cellAt(0, 'order_no').querySelector('button')!)

    const opened = tabs.value[tabs.value.length - 1]
    expect(opened.objectName).toBe('docs')
    expect(opened.filter).toBeUndefined()
    expect(opened.match).toEqual([
      { column: 'tenant_id', value: 1 },
      { column: 'id', value: { t: 'int8', v: '9007199254740993' } },
    ])
  })

  it('a composite FK with a NULL component offers no follow link', async () => {
    ordersSchema()
    tableMeta.mockResolvedValue(docsMeta())
    ;(api.tableFKs as unknown as ReturnType<typeof vi.fn>).mockResolvedValue({
      fks: [{ name: 'fk', columns: ['tenant_id', 'label'], refSchema: 'public', refTable: 'docs', refColumns: ['tenant_id', 'payload'], composite: true }],
    })
    tableData.mockResolvedValue(ordersResult([[1, 5n, null]]))

    render(<SQLBrowser schema="public" table="orders" />)
    await waitFor(() => expect(cellAt(0, 'order_no').textContent).toBe('5'))
    await new Promise(r => setTimeout(r, 0))
    expect(cellAt(0, 'tenant_id').querySelector('button.fkLink')).toBeNull()
  })

  it('an initial match (FK follow target) is passed to the read', async () => {
    ordersSchema()
    tableMeta.mockResolvedValue(docsMeta())
    tableData.mockResolvedValue(ordersResult([]))
    const match = [{ column: 'tenant_id', value: 1 }, { column: 'order_no', value: { t: 'int8', v: '2' } }]

    render(<SQLBrowser schema="public" table="orders" initialMatch={match} />)
    await waitFor(() => expect(tableData).toHaveBeenCalled())
    expect(tableData.mock.calls[0][8]).toEqual(match)
  })

  it('a read-only table read wins even if metadata says editable', async () => {
    ordersSchema()
    tableMeta.mockResolvedValue(docsMeta())
    tableData.mockResolvedValue({
      ...ordersResult([[1, 7n, 'o1']]),
      readOnly: true, readOnlyReason: 'public.orders has key column "x" of type float8, which Studio cannot yet compare exactly',
    })

    render(<SQLBrowser schema="public" table="orders" />)
    await waitFor(() => expect(screen.getByRole('note').textContent).toContain('cannot yet compare exactly'))
    fireEvent.dblClick(cellAt(0, 'label'))
    expect(document.querySelector('select[aria-label$=" value state"]')).toBeNull()
    expect(stagedEdits.value).toEqual([])
  })
})
