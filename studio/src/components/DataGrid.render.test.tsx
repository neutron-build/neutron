import { describe, it, expect, vi, afterEach } from 'vitest'
import { render, screen, fireEvent, cleanup } from '@testing-library/preact'
import { DataGrid } from './DataGrid'
import type { QueryResult, TableMetaColumn } from '../lib/types'

// Rendered-component tests for the S01 edit containment: empty string and
// SQL NULL are distinct controls, and editability comes from the
// AUTHORITATIVE server metadata (key/generated/identity columns read-only),
// not from client-side key guesses.

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
    { name: 'total', type: 'int4', tag: null, nullable: true, isKey: false, generated: true, identity: false, hasDefault: false, autoAssigned: false, editable: false, readOnlyReason: 'generated column (computed by the database) is read-only' },
  ]
}

function cellAt(row: number, col: string): HTMLElement {
  const headerCells = Array.from(document.querySelectorAll('th'))
  const colIdx = headerCells.findIndex(th => th.textContent?.includes(col))
  const rows = Array.from(document.querySelectorAll('tbody tr'))
  return rows[row].children[colIdx] as HTMLElement
}

afterEach(cleanup)

describe('DataGrid editing: empty string vs NULL stay distinct', () => {
  it('committing cleared text sends an empty string, not null', () => {
    const onCommitEdit = vi.fn()
    render(<DataGrid result={gridResult()} columns={metaColumns()} onCommitEdit={onCommitEdit} />)

    fireEvent.dblClick(cellAt(0, 'name'))
    const input = screen.getByRole('textbox') as HTMLInputElement
    fireEvent.input(input, { target: { value: '' } })
    fireEvent.keyDown(input, { key: 'Enter' })

    expect(onCommitEdit).toHaveBeenCalledWith(0, 'name', '')
    expect(onCommitEdit.mock.calls[0][2]).not.toBe(null)
  })

  it('committing text sends the text', () => {
    const onCommitEdit = vi.fn()
    render(<DataGrid result={gridResult()} columns={metaColumns()} onCommitEdit={onCommitEdit} />)

    fireEvent.dblClick(cellAt(0, 'name'))
    const input = screen.getByRole('textbox') as HTMLInputElement
    fireEvent.input(input, { target: { value: 'Bob' } })
    fireEvent.keyDown(input, { key: 'Enter' })

    expect(onCommitEdit).toHaveBeenCalledWith(0, 'name', 'Bob')
  })

  it('unchanged text is a no-op (no commit)', () => {
    const onCommitEdit = vi.fn()
    render(<DataGrid result={gridResult()} columns={metaColumns()} onCommitEdit={onCommitEdit} />)

    fireEvent.dblClick(cellAt(0, 'name'))
    fireEvent.keyDown(screen.getByRole('textbox'), { key: 'Enter' })

    expect(onCommitEdit).not.toHaveBeenCalled()
  })

  it('a NULL cell opens with the NULL control checked and Enter is a no-op', () => {
    const onCommitEdit = vi.fn()
    render(<DataGrid result={gridResult()} columns={metaColumns()} onCommitEdit={onCommitEdit} />)

    fireEvent.click(cellAt(1, 'name').querySelector('button')!)
    const nullToggle = screen.getByRole('checkbox') as HTMLInputElement
    expect(nullToggle.checked).toBe(true)
    const input = screen.getByRole('textbox') as HTMLInputElement
    expect(input.disabled).toBe(true)

    fireEvent.keyDown(input, { key: 'Enter' })
    expect(onCommitEdit).not.toHaveBeenCalled()
  })

  it('unchecking NULL on a NULL cell commits an explicit empty string', () => {
    const onCommitEdit = vi.fn()
    render(<DataGrid result={gridResult()} columns={metaColumns()} onCommitEdit={onCommitEdit} />)

    fireEvent.click(cellAt(1, 'name').querySelector('button')!)
    const nullToggle = screen.getByRole('checkbox')
    fireEvent.click(nullToggle)
    fireEvent.keyDown(screen.getByRole('textbox'), { key: 'Enter' })

    expect(onCommitEdit).toHaveBeenCalledWith(1, 'name', '')
  })

  it('checking the NULL control commits SQL null', () => {
    const onCommitEdit = vi.fn()
    render(<DataGrid result={gridResult()} columns={metaColumns()} onCommitEdit={onCommitEdit} />)

    fireEvent.dblClick(cellAt(0, 'name'))
    const nullToggle = screen.getByRole('checkbox')
    fireEvent.click(nullToggle)
    fireEvent.keyDown(screen.getByRole('textbox'), { key: 'Enter' })

    expect(onCommitEdit).toHaveBeenCalledWith(0, 'name', null)
  })

  it('Escape cancels without committing', () => {
    const onCommitEdit = vi.fn()
    render(<DataGrid result={gridResult()} columns={metaColumns()} onCommitEdit={onCommitEdit} />)

    fireEvent.dblClick(cellAt(0, 'name'))
    const input = screen.getByRole('textbox') as HTMLInputElement
    fireEvent.input(input, { target: { value: 'changed' } })
    fireEvent.keyDown(input, { key: 'Escape' })

    expect(onCommitEdit).not.toHaveBeenCalled()
    expect(screen.queryByRole('textbox')).toBeNull()
  })
})

describe('DataGrid edit affordances come from authoritative metadata', () => {
  it('offers no editing without column metadata', () => {
    const onCommitEdit = vi.fn()
    render(<DataGrid result={gridResult()} onCommitEdit={onCommitEdit} />)

    expect(screen.queryByText(/double-click a cell to edit/)).toBeNull()
    fireEvent.dblClick(cellAt(0, 'name'))
    expect(screen.queryByRole('textbox')).toBeNull()
    expect(onCommitEdit).not.toHaveBeenCalled()

    // NULL cells render as plain text, not as a set-value button
    const nullCell = cellAt(1, 'name')
    expect(nullCell.querySelector('button')).toBeNull()
    expect(nullCell.textContent).toBe('NULL')
  })

  it('the key column is read-only even when editing is enabled', () => {
    const onCommitEdit = vi.fn()
    render(<DataGrid result={gridResult()} columns={metaColumns()} onCommitEdit={onCommitEdit} />)

    const idCell = cellAt(0, 'id')
    expect(idCell.getAttribute('title')).toBe('key column is read-only (it addresses the row)')
    fireEvent.dblClick(idCell)
    expect(screen.queryByRole('textbox')).toBeNull()

    // non-key cells still open the editor
    fireEvent.dblClick(cellAt(0, 'name'))
    expect(screen.getByRole('textbox')).toBeDefined()
  })

  it('generated non-key columns are read-only (B04-L2 carry)', () => {
    const onCommitEdit = vi.fn()
    render(<DataGrid result={gridResult()} columns={metaColumns()} onCommitEdit={onCommitEdit} />)

    const genCell = cellAt(0, 'total')
    expect(genCell.getAttribute('title')).toContain('generated')
    fireEvent.dblClick(genCell)
    expect(screen.queryByRole('textbox')).toBeNull()
    expect(onCommitEdit).not.toHaveBeenCalled()
  })

  it('a column missing from the metadata is read-only (fail-safe)', () => {
    const onCommitEdit = vi.fn()
    const partial = metaColumns().filter(c => c.name !== 'name')
    render(<DataGrid result={gridResult()} columns={partial} onCommitEdit={onCommitEdit} />)

    fireEvent.dblClick(cellAt(0, 'name'))
    expect(screen.queryByRole('textbox')).toBeNull()
    expect(onCommitEdit).not.toHaveBeenCalled()
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
