import { describe, it, expect, vi, afterEach } from 'vitest'
import { render, screen, fireEvent, cleanup } from '@testing-library/preact'
import { DataGrid, type StagedRowState } from './DataGrid'
import type { CellEdit, QueryResult, TableMetaColumn } from '../lib/types'

// Rendered-component tests for the S03 data editor: staged edits (empty
// string / NULL / value never coerce), typed-editor keyboard flows
// (Enter/Escape/Tab), staged overlays and delete buttons, and editability
// from the AUTHORITATIVE server metadata (key/generated/identity columns
// read-only), not from client-side key guesses.

function gridResult(): QueryResult {
  return {
    columns: ['id', 'name', 'total'],
    rows: [[1, 'Alice', 84], [2, null, null]],
    rowCount: 2,
    duration: 0,
  }
}

function metaColumns(): TableMetaColumn[] {
  return [
    { name: 'id', type: 'int4', tag: null, nullable: false, isKey: true, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: false, readOnlyReason: 'key column is read-only (it addresses the row)' },
    { name: 'name', type: 'text', tag: null, nullable: true, isKey: false, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: true },
    { name: 'total', type: 'int4', tag: null, nullable: true, isKey: false, generated: false, identity: false, hasDefault: false, autoAssigned: false, editable: true },
  ]
}

function cellAt(row: number, col: string): HTMLElement {
  const headerCells = Array.from(document.querySelectorAll('th'))
  const colIdx = headerCells.findIndex(th => th.textContent?.includes(col))
  const rows = Array.from(document.querySelectorAll('tbody tr'))
  return rows[row].children[colIdx] as HTMLElement
}

/** The typed editor's text input for the open cell. */
function editorInput(): HTMLInputElement {
  const el = document.querySelector('input[aria-label$=" value"]')
  if (!el) throw new Error('typed editor input not found')
  return el as HTMLInputElement
}

/** The typed editor's three-way state select ("name value state"). */
function stateSelect(): HTMLSelectElement {
  const el = document.querySelector('select[aria-label$=" value state"]')
  if (!el) throw new Error('value-state select not found')
  return el as HTMLSelectElement
}

afterEach(cleanup)

describe('DataGrid staged editing: empty string, NULL and value stay distinct', () => {
  it('committing cleared text stages an empty string, not null', () => {
    const onStageUpdate = vi.fn()
    render(<DataGrid result={gridResult()} columns={metaColumns()} onStageUpdate={onStageUpdate} />)

    fireEvent.dblClick(cellAt(0, 'name'))
    fireEvent.input(editorInput(), { target: { value: '' } })
    fireEvent.keyDown(editorInput(), { key: 'Enter' })

    expect(onStageUpdate).toHaveBeenCalledWith(0, 'name', { kind: 'value', text: '' })
    const edit = onStageUpdate.mock.calls[0][2] as CellEdit
    expect(edit.kind).toBe('value')
  })

  it('committing text stages the text', () => {
    const onStageUpdate = vi.fn()
    render(<DataGrid result={gridResult()} columns={metaColumns()} onStageUpdate={onStageUpdate} />)

    fireEvent.dblClick(cellAt(0, 'name'))
    fireEvent.input(editorInput(), { target: { value: 'Bob' } })
    fireEvent.keyDown(editorInput(), { key: 'Enter' })

    expect(onStageUpdate).toHaveBeenCalledWith(0, 'name', { kind: 'value', text: 'Bob' })
  })

  it('unchanged text is a no-op (nothing staged)', () => {
    const onStageUpdate = vi.fn()
    render(<DataGrid result={gridResult()} columns={metaColumns()} onStageUpdate={onStageUpdate} />)

    fireEvent.dblClick(cellAt(0, 'name'))
    fireEvent.keyDown(editorInput(), { key: 'Enter' })

    expect(onStageUpdate).not.toHaveBeenCalled()
  })

  it('a NULL cell opens in the NULL state; switching to a value stages text', () => {
    const onStageUpdate = vi.fn()
    render(<DataGrid result={gridResult()} columns={metaColumns()} onStageUpdate={onStageUpdate} />)

    fireEvent.click(cellAt(1, 'name').querySelector('button')!)
    expect(stateSelect().value).toBe('null')
    expect((editorInput() as HTMLInputElement).disabled).toBe(true)

    fireEvent.change(stateSelect(), { target: { value: 'value' } })
    fireEvent.input(editorInput(), { target: { value: 'filled' } })
    fireEvent.keyDown(editorInput(), { key: 'Enter' })

    expect(onStageUpdate).toHaveBeenCalledWith(1, 'name', { kind: 'value', text: 'filled' })
  })

  it('choosing NULL stages an explicit SQL NULL', () => {
    const onStageUpdate = vi.fn()
    render(<DataGrid result={gridResult()} columns={metaColumns()} onStageUpdate={onStageUpdate} />)

    fireEvent.dblClick(cellAt(0, 'name'))
    fireEvent.change(stateSelect(), { target: { value: 'null' } })
    fireEvent.keyDown(stateSelect(), { key: 'Enter' })

    expect(onStageUpdate).toHaveBeenCalledWith(0, 'name', { kind: 'null' })
  })

  it('Escape cancels without staging', () => {
    const onStageUpdate = vi.fn()
    render(<DataGrid result={gridResult()} columns={metaColumns()} onStageUpdate={onStageUpdate} />)

    fireEvent.dblClick(cellAt(0, 'name'))
    fireEvent.input(editorInput(), { target: { value: 'changed' } })
    fireEvent.keyDown(editorInput(), { key: 'Escape' })

    expect(onStageUpdate).not.toHaveBeenCalled()
    expect(document.querySelector('select[aria-label$=" value state"]')).toBeNull()
  })
})

describe('DataGrid keyboard flow: Tab stages and moves to the next editable cell', () => {
  it('Tab stages the edit and opens the next editable column', () => {
    const onStageUpdate = vi.fn()
    render(<DataGrid result={gridResult()} columns={metaColumns()} onStageUpdate={onStageUpdate} />)

    fireEvent.dblClick(cellAt(0, 'name'))
    fireEvent.input(editorInput(), { target: { value: 'tabbed' } })
    fireEvent.keyDown(editorInput(), { key: 'Tab' })

    // staged the previous edit...
    expect(onStageUpdate).toHaveBeenCalledWith(0, 'name', { kind: 'value', text: 'tabbed' })
    // ...and moved the editor to 'total' (id is a read-only key column)
    expect(editorInput().getAttribute('aria-label')).toBe('total value')
  })

  it('Tab on the last editable column closes the editor', () => {
    const onStageUpdate = vi.fn()
    render(<DataGrid result={gridResult()} columns={metaColumns()} onStageUpdate={onStageUpdate} />)

    fireEvent.dblClick(cellAt(0, 'total'))
    fireEvent.keyDown(editorInput(), { key: 'Tab' })

    expect(onStageUpdate).not.toHaveBeenCalled() // unchanged value
    expect(document.querySelector('select[aria-label$=" value state"]')).toBeNull()
  })
})

describe('DataGrid staged overlay and delete button', () => {
  it('staged updates render as highlighted staged values over the committed cell', () => {
    const staged = new Map<number, StagedRowState>([
      [0, { updates: { name: { editId: 'e1', edit: { kind: 'value', text: 'staged-name' } } } }],
    ])
    render(<DataGrid result={gridResult()} columns={metaColumns()} onStageUpdate={vi.fn()} stagedRows={staged} />)

    const cell = cellAt(0, 'name')
    expect(cell.textContent).toBe('staged-name')
    expect(cell.querySelector('[title^="staged (not committed)"]')).not.toBeNull()
    // the untouched row renders its committed value
    expect(cellAt(1, 'name').textContent).toBe('NULL')
  })

  it('a staged NULL renders as an explicit NULL overlay', () => {
    const staged = new Map<number, StagedRowState>([
      [0, { updates: { name: { editId: 'e1', edit: { kind: 'null' } } } }],
    ])
    render(<DataGrid result={gridResult()} columns={metaColumns()} onStageUpdate={vi.fn()} stagedRows={staged} />)
    expect(cellAt(0, 'name').textContent).toBe('NULL')
  })

  it('the row delete button stages a delete and disables on an already-staged row', () => {
    const onStageDelete = vi.fn()
    const { rerender } = render(<DataGrid result={gridResult()} columns={metaColumns()} onStageDelete={onStageDelete} canDelete />)

    const del = screen.getAllByTitle('Stage row delete')[0] as HTMLButtonElement
    fireEvent.click(del)
    expect(onStageDelete).toHaveBeenCalledWith(0)

    const staged = new Map<number, StagedRowState>([[0, { updates: {}, deleteId: 'd1' }]])
    rerender(<DataGrid result={gridResult()} columns={metaColumns()} onStageDelete={onStageDelete} canDelete stagedRows={staged} />)
    // row 0's button changed its title (delete staged) and is disabled
    const stagedDel = screen.getByTitle('Delete staged — commit or discard it below') as HTMLButtonElement
    expect(stagedDel.disabled).toBe(true)
    fireEvent.click(stagedDel)
    expect(onStageDelete).toHaveBeenCalledTimes(1)
    // row 1's button is unaffected
    expect((screen.getByTitle('Stage row delete') as HTMLButtonElement).disabled).toBe(false)
  })

  it('no delete column without a delete handler or privilege', () => {
    render(<DataGrid result={gridResult()} columns={metaColumns()} onStageDelete={undefined} canDelete={false} />)
    expect(screen.queryAllByTitle('Stage row delete')).toEqual([])
  })
})

describe('DataGrid multi-sort headers and focus target', () => {
  it('header clicks report additive vs primary; marks show order', () => {
    const onSort = vi.fn()
    render(
      <DataGrid
        result={gridResult()}
        onSort={onSort}
        sorts={[{ column: 'name', dir: 'asc' }, { column: 'id', dir: 'desc' }]}
      />,
    )
    const headers = Array.from(document.querySelectorAll('th'))
    const nameTh = headers.find(th => th.textContent?.includes('name'))!
    expect(nameTh.textContent).toContain('↑1')
    const idTh = headers.find(th => th.textContent?.includes('id'))!
    expect(idTh.textContent).toContain('↓2')

    fireEvent.click(nameTh)
    expect(onSort).toHaveBeenCalledWith('name', false)
    fireEvent.click(idTh, { shiftKey: true })
    expect(onSort).toHaveBeenCalledWith('id', true)
  })

  it('a focus cell highlights its row and cell until the parent clears it', () => {
    const { rerender } = render(
      <DataGrid result={gridResult()} columns={metaColumns()} focusCell={{ rowIndex: 1, column: 'name' }} />,
    )
    const row = document.querySelector('tr[data-row-index="1"]') as HTMLElement
    expect(row.className).toContain('trFocus')
    const cell = cellAt(1, 'name')
    expect(cell.className).toContain('tdFocus')

    rerender(<DataGrid result={gridResult()} columns={metaColumns()} focusCell={null} />)
    expect(document.querySelector('tr[data-row-index="1"]')!.className).not.toContain('trFocus')
  })
})

describe('DataGrid edit affordances come from authoritative metadata', () => {
  it('offers no editing without column metadata', () => {
    const onStageUpdate = vi.fn()
    render(<DataGrid result={gridResult()} onStageUpdate={onStageUpdate} />)

    expect(screen.queryByText(/stage an edit/)).toBeNull()
    fireEvent.dblClick(cellAt(0, 'name'))
    expect(document.querySelector('select[aria-label$=" value state"]')).toBeNull()
    expect(onStageUpdate).not.toHaveBeenCalled()

    // NULL cells render as plain text, not as a set-value button
    const nullCell = cellAt(1, 'name')
    expect(nullCell.querySelector('button')).toBeNull()
    expect(nullCell.textContent).toBe('NULL')
  })

  it('the key column is read-only even when editing is enabled', () => {
    const onStageUpdate = vi.fn()
    render(<DataGrid result={gridResult()} columns={metaColumns()} onStageUpdate={onStageUpdate} />)

    const idCell = cellAt(0, 'id')
    expect(idCell.getAttribute('title')).toBe('key column is read-only (it addresses the row)')
    fireEvent.dblClick(idCell)
    expect(document.querySelector('select[aria-label$=" value state"]')).toBeNull()

    // non-key cells still open the editor
    fireEvent.dblClick(cellAt(0, 'name'))
    expect(stateSelect()).toBeDefined()
  })

  it('a column missing from the metadata is read-only (fail-safe)', () => {
    const onStageUpdate = vi.fn()
    const partial = metaColumns().filter(c => c.name !== 'name')
    render(<DataGrid result={gridResult()} columns={partial} onStageUpdate={onStageUpdate} />)

    fireEvent.dblClick(cellAt(0, 'name'))
    expect(document.querySelector('select[aria-label$=" value state"]')).toBeNull()
    expect(onStageUpdate).not.toHaveBeenCalled()
  })

  it('generated non-key columns are read-only (B04-L2 carry)', () => {
    const onStageUpdate = vi.fn()
    const withGenerated = metaColumns().map(c =>
      c.name === 'total' ? { ...c, editable: false, generated: true, readOnlyReason: 'generated column (computed by the database) is read-only' } : c)
    render(<DataGrid result={gridResult()} columns={withGenerated} onStageUpdate={onStageUpdate} />)

    const genCell = cellAt(0, 'total')
    expect(genCell.getAttribute('title')).toContain('generated')
    fireEvent.dblClick(genCell)
    expect(document.querySelector('select[aria-label$=" value state"]')).toBeNull()
    expect(onStageUpdate).not.toHaveBeenCalled()
  })
})

describe('DataGrid renders decoded lossless values exactly', () => {
  it('bigint keeps every digit, bytea shows \\x hex, JSON objects show JSON text', () => {
    const result: QueryResult = {
      columns: ['id', 'blob', 'doc'],
      rows: [[9223372036854775807n, new Uint8Array([0x00, 0xff, 0x10]), { a: 1 }]],
      rowCount: 1,
      duration: 0,
    }
    render(<DataGrid result={result} />)
    expect(cellAt(0, 'id').textContent).toBe('9223372036854775807')
    expect(cellAt(0, 'blob').textContent).toBe('\\x00ff10')
    expect(cellAt(0, 'doc').textContent).toBe('{"a":1}')
  })
})
